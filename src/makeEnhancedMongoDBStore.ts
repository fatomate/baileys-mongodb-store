import { MongoClient, Collection } from 'mongodb'
import { proto } from 'baileys'
import type { 
    BaileysEventEmitter, 
    Chat, 
    ConnectionState, 
    Contact, 
    GroupMetadata, 
    PresenceData, 
    WAMessageCursor 
} from 'baileys'
import { jidNormalizedUser, updateMessageWithReceipt, updateMessageWithReaction, toNumber } from 'baileys'
import type { Label } from 'baileys/lib/Types/Label'
import type { LabelAssociation } from 'baileys/lib/Types/LabelAssociation'
import type { 
    EnhancedMongoDBStoreConfig, 
    EnhancedMongoDBStore,
    EventStorageConfig,
    EventMetrics,
    CollectionTTLConfig
} from './types-enhanced'
import NodeCache from 'node-cache'
import PQueue from 'p-queue'

const DEFAULT_TTL_DAYS = 30
const DEFAULT_EVENT_CONFIG: EventStorageConfig = {
    enabled: true,
    useBatch: false
}

// Event metrics storage
const eventMetricsMap = new Map<string, EventMetrics>()

interface MongoCollections {
    chats: Collection<Chat & { instanceId: string; updatedAt: Date }>
    contacts: Collection<Contact & { instanceId: string; updatedAt: Date }>
    messages: Collection<proto.IWebMessageInfo & { instanceId: string; jid: string; updatedAt: Date }>
    groupMetadata: Collection<GroupMetadata & { instanceId: string; updatedAt: Date }>
    state: Collection<ConnectionState & { instanceId: string; updatedAt: Date }>
    presences: Collection<{ instanceId: string; id: string; presences: { [participant: string]: PresenceData }; updatedAt: Date }>
    labels: Collection<Label & { instanceId: string; updatedAt: Date }>
    labelAssociations: Collection<LabelAssociation & { instanceId: string; updatedAt: Date }>
}

// Cache for Binary conversions
const binaryConversionCache = new NodeCache({ stdTTL: 300, checkperiod: 60 })

// Helper function to convert MongoDB Binary objects to Buffers
const convertBinaryToBuffer = (obj: any): any => {
    try {
        if (!obj || typeof obj !== 'object') return obj
        
        if (obj._bsontype === 'Binary') {
            if (obj.buffer instanceof Buffer) {
                return Buffer.from(obj.buffer)
            } else if (obj.buffer instanceof ArrayBuffer) {
                return Buffer.from(obj.buffer)
            } else if (obj.buffer instanceof Uint8Array) {
                return Buffer.from(obj.buffer)
            } else if (obj.buffer) {
                return Buffer.from(obj.buffer)
            }
        }
        
        if (obj.type === 'Buffer' && Array.isArray(obj.data)) {
            return Buffer.from(obj.data)
        }
        
        if (Array.isArray(obj)) {
            return obj.map(item => convertBinaryToBuffer(item))
        }
        
        const result: any = {}
        for (const key in obj) {
            if (Object.prototype.hasOwnProperty.call(obj, key)) {
                result[key] = convertBinaryToBuffer(obj[key])
            }
        }
        return result
    } catch (error) {
        console.error('Error converting binary to buffer:', error)
        return obj
    }
}

export const makeEnhancedMongoDBStore = async (config: EnhancedMongoDBStoreConfig): Promise<EnhancedMongoDBStore> => {
    const {
        uri,
        database: dbName,
        instanceId,
        ttlDays = DEFAULT_TTL_DAYS,
        collectionTTL = {},
        events = {},
        storeAllByDefault = true,
        collectionPrefix = 'baileys_',
        logLevel = 'none',
        enableMetrics = false,
        hooks = {}
    } = config
    
    // Helper functions for conditional logging
    const log = (...args: any[]) => {
        if (logLevel === 'all') {
            console.log(...args)
        }
    }

    // MongoDB connection
    const client: MongoClient = new MongoClient(uri, {
        maxPoolSize: 100,
        minPoolSize: 10,
        maxIdleTimeMS: 30000,
        writeConcern: { w: 1, j: false }
    })
    await client.connect()
    const db = client.db(dbName)
    
    // Get collections
    const getCollections = (): MongoCollections => {
        if (!db) {
            throw new Error('Database not initialized')
        }
        return {
            chats: db.collection(`${collectionPrefix}chats`),
            contacts: db.collection(`${collectionPrefix}contacts`),
            messages: db.collection(`${collectionPrefix}messages`),
            groupMetadata: db.collection(`${collectionPrefix}groupMetadata`),
            state: db.collection(`${collectionPrefix}state`),
            presences: db.collection(`${collectionPrefix}presences`),
            labels: db.collection(`${collectionPrefix}labels`),
            labelAssociations: db.collection(`${collectionPrefix}labelAssociations`)
        }
    }
    
    const collections = getCollections()
    
    // Queue configuration
    const QUEUE_CONCURRENCY = 50
    const pMessageQueue = new PQueue({ concurrency: QUEUE_CONCURRENCY })
    const generalQueue = new PQueue({ concurrency: QUEUE_CONCURRENCY })
    
    // Event configuration management
    const eventConfigs = new Map<string, EventStorageConfig>()
    
    // Initialize event configs from user configuration
    Object.keys(events).forEach(eventType => {
        eventConfigs.set(eventType, { ...DEFAULT_EVENT_CONFIG, ...events[eventType] })
    })
    
    // Helper to get event configuration
    const getEventConfig = (eventType: string): EventStorageConfig => {
        if (eventConfigs.has(eventType)) {
            return eventConfigs.get(eventType)!
        }
        
        // Check if we have a config in the initial events object
        if (events[eventType]) {
            const config = { ...DEFAULT_EVENT_CONFIG, ...events[eventType] }
            eventConfigs.set(eventType, config)
            return config
        }
        
        // Return default based on storeAllByDefault setting
        return {
            enabled: storeAllByDefault,
            ttlDays: ttlDays,
            useBatch: false
        }
    }
    
    // Helper to check if event should be stored
    const shouldStoreEvent = async (eventType: string, data: any): Promise<boolean> => {
        const config = getEventConfig(eventType)
        
        if (!config.enabled) {
            if (enableMetrics) updateEventMetrics(eventType, 'skipped')
            return false
        }
        
        // Apply custom filter if provided
        if (config.filter && !config.filter(data)) {
            if (enableMetrics) updateEventMetrics(eventType, 'skipped')
            return false
        }
        
        // Apply hook if provided
        if (hooks.beforeStore) {
            const shouldStore = await hooks.beforeStore(eventType, data)
            if (!shouldStore) {
                if (enableMetrics) updateEventMetrics(eventType, 'skipped')
                return false
            }
        }
        
        return true
    }
    
    // Helper to get TTL for a collection based on event type
    const getTTLForCollection = (collectionName: keyof CollectionTTLConfig, eventType?: string): number => {
        // First check if event has specific TTL
        if (eventType) {
            const eventConfig = getEventConfig(eventType)
            if (eventConfig.ttlDays !== undefined) {
                return eventConfig.ttlDays
            }
        }
        
        // Then check collection-specific TTL
        if (collectionTTL[collectionName] !== undefined) {
            return collectionTTL[collectionName]!
        }
        
        // Finally use global TTL
        return ttlDays
    }
    
    // Update event metrics
    const updateEventMetrics = (eventType: string, action: 'received' | 'stored' | 'skipped' | 'error') => {
        if (!enableMetrics) return
        
        let metrics = eventMetricsMap.get(eventType)
        if (!metrics) {
            metrics = {
                eventType,
                totalReceived: 0,
                totalStored: 0,
                totalSkipped: 0,
                totalErrors: 0
            }
            eventMetricsMap.set(eventType, metrics)
        }
        
        switch (action) {
            case 'received':
                metrics.totalReceived++
                break
            case 'stored':
                metrics.totalStored++
                metrics.lastProcessedAt = new Date()
                break
            case 'skipped':
                metrics.totalSkipped++
                break
            case 'error':
                metrics.totalErrors++
                break
        }
    }
    
    // Create indexes with custom TTL
    const createIndexes = async () => {
        // Create indexes for each collection with appropriate TTL
        const indexPromises: Promise<void>[] = []
        
        // Chats indexes
        const chatsTTL = getTTLForCollection('chats') * 24 * 60 * 60
        indexPromises.push(
            collections.chats.createIndex({ instanceId: 1, id: 1 }, { unique: true }).then(() => {}),
            collections.chats.createIndex({ updatedAt: 1 }, { expireAfterSeconds: chatsTTL }).then(() => {})
        )
        
        // Contacts indexes
        const contactsTTL = getTTLForCollection('contacts') * 24 * 60 * 60
        indexPromises.push(
            collections.contacts.createIndex({ instanceId: 1, id: 1 }, { unique: true }).then(() => {}),
            collections.contacts.createIndex({ updatedAt: 1 }, { expireAfterSeconds: contactsTTL }).then(() => {})
        )
        
        // Messages indexes
        const messagesTTL = getTTLForCollection('messages') * 24 * 60 * 60
        indexPromises.push(
            collections.messages.createIndex({ instanceId: 1, jid: 1, 'key.id': 1 }, { unique: true }).then(() => {}),
            collections.messages.createIndex({ instanceId: 1, jid: 1, messageTimestamp: -1 }).then(() => {}),
            collections.messages.createIndex({ updatedAt: 1 }, { expireAfterSeconds: messagesTTL }).then(() => {})
        )
        
        // Group metadata indexes
        const groupsTTL = getTTLForCollection('groupMetadata') * 24 * 60 * 60
        indexPromises.push(
            collections.groupMetadata.createIndex({ instanceId: 1, id: 1 }, { unique: true }).then(() => {}),
            collections.groupMetadata.createIndex({ updatedAt: 1 }, { expireAfterSeconds: groupsTTL }).then(() => {})
        )
        
        // State indexes
        const stateTTL = getTTLForCollection('state') * 24 * 60 * 60
        indexPromises.push(
            collections.state.createIndex({ instanceId: 1 }, { unique: true }).then(() => {}),
            collections.state.createIndex({ updatedAt: 1 }, { expireAfterSeconds: stateTTL }).then(() => {})
        )
        
        // Presences indexes
        const presencesTTL = getTTLForCollection('presences') * 24 * 60 * 60
        indexPromises.push(
            collections.presences.createIndex({ instanceId: 1, id: 1 }, { unique: true }).then(() => {}),
            collections.presences.createIndex({ updatedAt: 1 }, { expireAfterSeconds: presencesTTL }).then(() => {})
        )
        
        // Labels indexes
        const labelsTTL = getTTLForCollection('labels') * 24 * 60 * 60
        indexPromises.push(
            collections.labels.createIndex({ instanceId: 1, id: 1 }, { unique: true }).then(() => {}),
            collections.labels.createIndex({ updatedAt: 1 }, { expireAfterSeconds: labelsTTL }).then(() => {})
        )
        
        // Label associations indexes
        const labelAssocTTL = getTTLForCollection('labelAssociations') * 24 * 60 * 60
        indexPromises.push(
            collections.labelAssociations.createIndex({ instanceId: 1, chatId: 1, labelId: 1 }, { unique: true }).then(() => {}),
            collections.labelAssociations.createIndex({ updatedAt: 1 }, { expireAfterSeconds: labelAssocTTL }).then(() => {})
        )
        
        await Promise.all(indexPromises)
        log(`✅ Indexes created with custom TTL settings for instance ${instanceId}`)
    }
    
    // Initialize indexes
    await createIndexes()
    
    const storeImpl: EnhancedMongoDBStore = {
        instanceId,
        
        getEventConfig(eventType: string): EventStorageConfig {
            return getEventConfig(eventType)
        },
        
        updateEventConfig(eventType: string, config: EventStorageConfig): void {
            const currentConfig = getEventConfig(eventType)
            eventConfigs.set(eventType, { ...currentConfig, ...config })
            log(`Updated event config for ${eventType}:`, eventConfigs.get(eventType))
        },
        
        getEventMetrics(eventType?: string): EventMetrics | EventMetrics[] {
            if (eventType) {
                return eventMetricsMap.get(eventType) || {
                    eventType,
                    totalReceived: 0,
                    totalStored: 0,
                    totalSkipped: 0,
                    totalErrors: 0
                }
            }
            return Array.from(eventMetricsMap.values())
        },
        
        resetEventMetrics(eventType?: string): void {
            if (eventType) {
                eventMetricsMap.delete(eventType)
            } else {
                eventMetricsMap.clear()
            }
        },

        async getChats(): Promise<Chat[]> {
            const chats = await collections.chats
                .find({ instanceId })
                .sort({ conversationTimestamp: -1 })
                .toArray()
            
            return chats.map(({ _id, instanceId: _instanceId, updatedAt: _updatedAt, ...chat }) => chat as Chat)
        },

        async getChat(jid: string): Promise<Chat | null> {
            const chat = await collections.chats.findOne({ instanceId, id: jid })
            if (!chat) return null
            
            const { _id, instanceId: _instanceId, updatedAt, ...chatData } = chat
            return chatData as Chat
        },

        async upsertChats(...chats: Chat[]): Promise<void> {
            if (chats.length === 0) return
            
            const bulkOps = chats.map(chat => ({
                replaceOne: {
                    filter: { instanceId, id: chat.id },
                    replacement: { ...chat, instanceId, updatedAt: new Date() },
                    upsert: true
                }
            }))
            
            await collections.chats.bulkWrite(bulkOps)
        },

        async updateChat(jid: string, update: Partial<Chat>): Promise<boolean> {
            const result = await collections.chats.updateOne(
                { instanceId, id: jid },
                { $set: { ...update, updatedAt: new Date() } }
            )
            
            return result.modifiedCount > 0
        },

        async deleteChats(jids: string[]): Promise<void> {
            await collections.chats.deleteMany({
                instanceId,
                id: { $in: jids }
            })
        },

        async getContacts(): Promise<{ [id: string]: Contact }> {
            const contacts = await collections.contacts
                .find({ instanceId })
                .toArray()
            
            const contactsMap: { [id: string]: Contact } = {}
            for (const contact of contacts) {
                const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...contactData } = contact
                contactsMap[contact.id] = contactData as Contact
            }
            
            return contactsMap
        },

        async getContact(jid: string): Promise<Contact | null> {
            const contact = await collections.contacts.findOne({ instanceId, id: jid })
            if (!contact) return null
            
            const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...contactData } = contact
            return contactData as Contact
        },

        async upsertContacts(contacts: Contact[]): Promise<void> {
            if (contacts.length === 0) return
            
            const bulkOps = contacts.map(contact => ({
                replaceOne: {
                    filter: { instanceId, id: contact.id },
                    replacement: { ...contact, instanceId, updatedAt: new Date() },
                    upsert: true
                }
            }))
            
            await collections.contacts.bulkWrite(bulkOps, { ordered: false })
        },

        async getMessages(jid: string): Promise<proto.IWebMessageInfo[]> {
            const messages = await collections.messages
                .find({ instanceId, jid })
                .sort({ messageTimestamp: -1 })
                .toArray()
            
            return messages.map(({ _id, instanceId: _instanceId, jid: _jid, updatedAt: _updatedAt, ...msg }) => 
                convertBinaryToBuffer(msg))
        },

        async getMessage(jid: string, id: string): Promise<proto.IWebMessageInfo | null> {
            const cacheKey = `msg_${instanceId}_${jid}_${id}`
            const cached = binaryConversionCache.get<proto.IWebMessageInfo>(cacheKey)
            if (cached) return cached
            
            const message = await collections.messages.findOne({
                instanceId,
                jid,
                'key.id': id
            })
            
            if (!message) return null
            
            const { _id, instanceId: _instanceId, jid: _jid, updatedAt: _updatedAt, ...msg } = message
            const converted = convertBinaryToBuffer(msg)
            
            binaryConversionCache.set(cacheKey, converted)
            
            return converted
        },

        async upsertMessage(jid: string, message: proto.IWebMessageInfo, useBatch: boolean = false): Promise<void> {
            const cacheKey = `msg_${instanceId}_${jid}_${message.key?.id}`
            binaryConversionCache.del(cacheKey)
            
            await pMessageQueue.add(async () => {
                await collections.messages.replaceOne(
                    {
                        instanceId,
                        jid,
                        'key.id': message.key?.id
                    },
                    {
                        ...message,
                        instanceId,
                        jid,
                        updatedAt: new Date()
                    },
                    { upsert: true }
                )
            })
        },

        async updateMessage(jid: string, id: string, update: Partial<proto.IWebMessageInfo>): Promise<boolean> {
            const cacheKey = `msg_${instanceId}_${jid}_${id}`
            binaryConversionCache.del(cacheKey)
            
            const result = await collections.messages.updateOne(
                {
                    instanceId,
                    jid,
                    'key.id': id
                },
                {
                    $set: { ...update, updatedAt: new Date() }
                }
            )
            
            return result.modifiedCount > 0
        },

        async deleteMessages(jid: string, ids?: string[]): Promise<void> {
            if (ids && ids.length > 0) {
                ids.forEach(id => {
                    const cacheKey = `msg_${instanceId}_${jid}_${id}`
                    binaryConversionCache.del(cacheKey)
                })
            } else {
                const keys = binaryConversionCache.keys()
                keys.forEach(key => {
                    if (key.startsWith(`msg_${instanceId}_${jid}_`)) {
                        binaryConversionCache.del(key)
                    }
                })
            }
            
            const filter: any = { instanceId, jid }
            
            if (ids && ids.length > 0) {
                filter['key.id'] = { $in: ids }
            }
            
            await collections.messages.deleteMany(filter)
        },

        async getGroupMetadata(jid: string): Promise<GroupMetadata | null> {
            const metadata = await collections.groupMetadata.findOne({ instanceId, id: jid })
            if (!metadata) return null
            
            const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...metadataData } = metadata
            return metadataData as GroupMetadata
        },
        
        async getAllGroupMetadata(): Promise<GroupMetadata[]> {
            const groups = await collections.groupMetadata.find({ instanceId }).toArray()
            return groups.map(({ _id, instanceId: _instanceId, updatedAt: _updatedAt, ...metadata }) => metadata as GroupMetadata)
        },

        async upsertGroupMetadata(jid: string, metadata: GroupMetadata): Promise<void> {
            if (!metadata.id) {
                metadata.id = jid
            }
            
            await collections.groupMetadata.replaceOne(
                { instanceId, id: metadata.id },
                { ...metadata, instanceId, updatedAt: new Date() },
                { upsert: true }
            )
        },

        async getState(): Promise<ConnectionState> {
            const state = await collections.state.findOne({ instanceId })
            if (!state) return { connection: 'close' }
            
            const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...stateData } = state
            return stateData as ConnectionState
        },

        async updateState(update: Partial<ConnectionState>): Promise<void> {
            await collections.state.updateOne(
                { instanceId },
                { 
                    $set: { ...update, instanceId, updatedAt: new Date() }
                },
                { upsert: true }
            )
        },

        async getPresences(): Promise<{ [id: string]: { [participant: string]: PresenceData } }> {
            const presences = await collections.presences
                .find({ instanceId })
                .toArray()
            
            const presencesMap: { [id: string]: { [participant: string]: PresenceData } } = {}
            for (const presence of presences) {
                presencesMap[presence.id] = presence.presences
            }
            
            return presencesMap
        },

        async updatePresence(id: string, presences: { [participant: string]: PresenceData }): Promise<void> {
            await collections.presences.updateOne(
                { instanceId, id },
                {
                    $set: { presences, updatedAt: new Date() }
                },
                { upsert: true }
            )
        },

        async getLabels(): Promise<{ [id: string]: Label }> {
            const labels = await collections.labels
                .find({ instanceId })
                .toArray()
            
            const labelsMap: { [id: string]: Label } = {}
            for (const label of labels) {
                const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...labelData } = label
                labelsMap[label.id] = labelData as Label
            }
            
            return labelsMap
        },

        async upsertLabel(id: string, label: Label): Promise<void> {
            await collections.labels.replaceOne(
                { instanceId, id },
                { ...label, instanceId, updatedAt: new Date() },
                { upsert: true }
            )
        },

        async deleteLabel(id: string): Promise<void> {
            await collections.labels.deleteOne({ instanceId, id })
        },

        async getLabelAssociations(): Promise<LabelAssociation[]> {
            const associations = await collections.labelAssociations
                .find({ instanceId })
                .toArray()
            
            return associations.map(({ _id, instanceId: _instanceId, updatedAt: _updatedAt, ...assoc }) => assoc as LabelAssociation)
        },

        async getChatLabels(chatId: string): Promise<LabelAssociation[]> {
            const associations = await collections.labelAssociations
                .find({ instanceId, chatId })
                .toArray()
            
            return associations.map(({ _id, instanceId: _instanceId, updatedAt: _updatedAt, ...assoc }) => assoc as LabelAssociation)
        },

        async getMessageLabels(messageId: string): Promise<string[]> {
            const associations = await collections.labelAssociations
                .find({ instanceId, messageId })
                .toArray()
            
            return associations.map(assoc => assoc.labelId)
        },

        async upsertLabelAssociation(association: LabelAssociation): Promise<void> {
            const filter: any = {
                instanceId,
                chatId: association.chatId,
                labelId: association.labelId
            }
            
            if ('messageId' in association && association.messageId) {
                filter.messageId = association.messageId
            }
            
            await collections.labelAssociations.replaceOne(
                filter,
                {
                    ...association,
                    instanceId,
                    updatedAt: new Date()
                },
                { upsert: true }
            )
        },

        async deleteLabelAssociation(association: LabelAssociation): Promise<void> {
            const filter: any = {
                instanceId,
                chatId: association.chatId,
                labelId: association.labelId
            }
            
            if ('messageId' in association && association.messageId) {
                filter.messageId = association.messageId
            }
            
            await collections.labelAssociations.deleteOne(filter)
        },

        bind(ev: BaileysEventEmitter): void {
            log(`[${instanceId}] store.bind() called - setting up event listeners with selective storage`)
            
            // Connection update
            ev.on('connection.update', async update => {
                if (enableMetrics) updateEventMetrics('connection.update', 'received')
                if (await shouldStoreEvent('connection.update', update)) {
                    await storeImpl.updateState(update)
                    if (enableMetrics) updateEventMetrics('connection.update', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('connection.update', update)
                }
            })

            // Messaging history
            ev.on('messaging-history.set', async ({ chats: newChats, contacts: newContacts, messages: newMessages, isLatest }) => {
                if (enableMetrics) updateEventMetrics('messaging-history.set', 'received')
                if (await shouldStoreEvent('messaging-history.set', { chats: newChats, contacts: newContacts, messages: newMessages, isLatest })) {
                    if (isLatest) {
                        await storeImpl.clearAll()
                    }
                    
                    const promises: Promise<void>[] = []
                    
                    if (newChats?.length) {
                        promises.push(generalQueue.add(async () => {
                            await storeImpl.upsertChats(...newChats)
                        }))
                    }
                    
                    if (newContacts?.length) {
                        promises.push(generalQueue.add(async () => {
                            await storeImpl.upsertContacts(newContacts)
                        }))
                    }
                    
                    if (newMessages?.length) {
                        for (const msg of newMessages) {
                            const jid = msg.key.remoteJid!
                            await storeImpl.upsertMessage(jid, msg, true)
                        }
                    }
                    
                    await Promise.all(promises)
                    if (enableMetrics) updateEventMetrics('messaging-history.set', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('messaging-history.set', { chats: newChats, contacts: newContacts, messages: newMessages, isLatest })
                }
            })

            // Contacts events
            ev.on('contacts.upsert', async contacts => {
                if (enableMetrics) updateEventMetrics('contacts.upsert', 'received')
                if (await shouldStoreEvent('contacts.upsert', contacts)) {
                    await storeImpl.upsertContacts(contacts)
                    if (enableMetrics) updateEventMetrics('contacts.upsert', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('contacts.upsert', contacts)
                }
            })

            ev.on('contacts.update', async updates => {
                if (enableMetrics) updateEventMetrics('contacts.update', 'received')
                if (await shouldStoreEvent('contacts.update', updates)) {
                    for (const update of updates) {
                        const contact = await storeImpl.getContact(update.id!)
                        if (contact) {
                            Object.assign(contact, update)
                            await storeImpl.upsertContacts([contact])
                        }
                    }
                    if (enableMetrics) updateEventMetrics('contacts.update', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('contacts.update', updates)
                }
            })

            // Chats events
            ev.on('chats.upsert', async newChats => {
                if (enableMetrics) updateEventMetrics('chats.upsert', 'received')
                if (await shouldStoreEvent('chats.upsert', newChats)) {
                    await storeImpl.upsertChats(...newChats)
                    if (enableMetrics) updateEventMetrics('chats.upsert', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('chats.upsert', newChats)
                }
            })

            ev.on('chats.update', async updates => {
                if (enableMetrics) updateEventMetrics('chats.update', 'received')
                if (await shouldStoreEvent('chats.update', updates)) {
                    for (const update of updates) {
                        await storeImpl.updateChat(update.id!, update)
                    }
                    if (enableMetrics) updateEventMetrics('chats.update', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('chats.update', updates)
                }
            })

            ev.on('chats.delete', async deletions => {
                if (enableMetrics) updateEventMetrics('chats.delete', 'received')
                if (await shouldStoreEvent('chats.delete', deletions)) {
                    await storeImpl.deleteChats(deletions)
                    if (enableMetrics) updateEventMetrics('chats.delete', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('chats.delete', deletions)
                }
            })

            // Labels events
            ev.on('labels.edit', async (label) => {
                if (enableMetrics) updateEventMetrics('labels.edit', 'received')
                if (await shouldStoreEvent('labels.edit', label)) {
                    if (label.deleted) {
                        await storeImpl.deleteLabel(label.id)
                        await collections.labelAssociations.deleteMany({
                            instanceId,
                            labelId: label.id
                        })
                    } else {
                        await storeImpl.upsertLabel(label.id, label)
                    }
                    if (enableMetrics) updateEventMetrics('labels.edit', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('labels.edit', label)
                }
            })

            ev.on('labels.association', async ({ type, association }) => {
                if (enableMetrics) updateEventMetrics('labels.association', 'received')
                if (await shouldStoreEvent('labels.association', { type, association })) {
                    if (type === 'add') {
                        await storeImpl.upsertLabelAssociation(association)
                    } else if (type === 'remove') {
                        await storeImpl.deleteLabelAssociation(association)
                    }
                    if (enableMetrics) updateEventMetrics('labels.association', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('labels.association', { type, association })
                }
            })

            // Presence events
            ev.on('presence.update', async ({ id, presences: update }) => {
                if (enableMetrics) updateEventMetrics('presence.update', 'received')
                if (await shouldStoreEvent('presence.update', { id, presences: update })) {
                    const existing = (await storeImpl.getPresences())[id] || {}
                    Object.assign(existing, update)
                    await storeImpl.updatePresence(id, existing)
                    if (enableMetrics) updateEventMetrics('presence.update', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('presence.update', { id, presences: update })
                }
            })

            // Messages events
            ev.on('messages.upsert', async ({ messages: newMessages, type }) => {
                if (enableMetrics) updateEventMetrics('messages.upsert', 'received')
                
                const eventConfig = getEventConfig('messages.upsert')
                
                for (const msg of newMessages) {
                    // Apply transform if provided
                    let messageToStore = msg
                    if (eventConfig.transform) {
                        messageToStore = eventConfig.transform(msg)
                    }
                    
                    if (await shouldStoreEvent('messages.upsert', messageToStore)) {
                        const jid = jidNormalizedUser(msg.key.remoteJid!)
                        await storeImpl.upsertMessage(jid, messageToStore, eventConfig.useBatch)
                        
                        if (type === 'notify' && !(await storeImpl.getChat(jid))) {
                            await storeImpl.upsertChats({
                                id: jid,
                                conversationTimestamp: toNumber(msg.messageTimestamp),
                                unreadCount: 1
                            } as Chat)
                        }
                    }
                }
                
                if (enableMetrics) updateEventMetrics('messages.upsert', 'stored')
                if (hooks.afterStore) await hooks.afterStore('messages.upsert', { messages: newMessages, type })
            })

            ev.on('messages.update', async updates => {
                if (enableMetrics) updateEventMetrics('messages.update', 'received')
                if (await shouldStoreEvent('messages.update', updates)) {
                    for (const { update, key } of updates) {
                        const jid = jidNormalizedUser(key.remoteJid!)
                        await storeImpl.updateMessage(jid, key.id!, update)
                    }
                    if (enableMetrics) updateEventMetrics('messages.update', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('messages.update', updates)
                }
            })

            ev.on('messages.delete', async item => {
                if (enableMetrics) updateEventMetrics('messages.delete', 'received')
                if (await shouldStoreEvent('messages.delete', item)) {
                    if ('all' in item) {
                        await storeImpl.deleteMessages(item.jid)
                    } else {
                        const jid = item.keys[0].remoteJid!
                        const ids = item.keys.map(k => k.id!)
                        await storeImpl.deleteMessages(jid, ids)
                    }
                    if (enableMetrics) updateEventMetrics('messages.delete', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('messages.delete', item)
                }
            })

            // Groups events
            ev.on('groups.update', async updates => {
                if (enableMetrics) updateEventMetrics('groups.update', 'received')
                if (await shouldStoreEvent('groups.update', updates)) {
                    for (const update of updates) {
                        if (update.participants && Array.isArray(update.participants)) {
                            await storeImpl.upsertGroupMetadata(update.id!, update as GroupMetadata)
                        } else {
                            const existingMetadata = await storeImpl.getGroupMetadata(update.id!)
                            if (existingMetadata) {
                                Object.assign(existingMetadata, update)
                                await storeImpl.upsertGroupMetadata(update.id!, existingMetadata)
                            } else {
                                const newMetadata: GroupMetadata = {
                                    id: update.id!,
                                    subject: update.subject || '',
                                    participants: [],
                                    ...update
                                } as GroupMetadata
                                await storeImpl.upsertGroupMetadata(update.id!, newMetadata)
                            }
                        }
                    }
                    if (enableMetrics) updateEventMetrics('groups.update', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('groups.update', updates)
                }
            })

            ev.on('groups.upsert', async groups => {
                if (enableMetrics) updateEventMetrics('groups.upsert', 'received')
                if (await shouldStoreEvent('groups.upsert', groups)) {
                    for (const group of groups) {
                        await storeImpl.upsertGroupMetadata(group.id, group)
                    }
                    if (enableMetrics) updateEventMetrics('groups.upsert', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('groups.upsert', groups)
                }
            })

            ev.on('group-participants.update', async ({ id, participants, action }) => {
                if (enableMetrics) updateEventMetrics('group-participants.update', 'received')
                if (await shouldStoreEvent('group-participants.update', { id, participants, action })) {
                    const metadata = await storeImpl.getGroupMetadata(id)
                    if (metadata) {
                        switch (action) {
                            case 'add':
                                metadata.participants.push(...participants.map(id => ({ 
                                    id, 
                                    isAdmin: false, 
                                    isSuperAdmin: false 
                                })))
                                break
                            case 'demote':
                            case 'promote':
                                for (const participant of metadata.participants) {
                                    if (participants.includes(participant.id)) {
                                        participant.isAdmin = action === 'promote'
                                    }
                                }
                                break
                            case 'remove':
                                metadata.participants = metadata.participants.filter(p => !participants.includes(p.id))
                                break
                        }
                        await storeImpl.upsertGroupMetadata(id, metadata)
                    }
                    if (enableMetrics) updateEventMetrics('group-participants.update', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('group-participants.update', { id, participants, action })
                }
            })

            // Message receipts
            ev.on('message-receipt.update', async updates => {
                if (enableMetrics) updateEventMetrics('message-receipt.update', 'received')
                if (await shouldStoreEvent('message-receipt.update', updates)) {
                    for (const { key, receipt } of updates) {
                        const msg = await storeImpl.getMessage(key.remoteJid!, key.id!)
                        if (msg) {
                            updateMessageWithReceipt(msg, receipt)
                            await storeImpl.updateMessage(key.remoteJid!, key.id!, msg)
                        }
                    }
                    if (enableMetrics) updateEventMetrics('message-receipt.update', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('message-receipt.update', updates)
                }
            })

            // Message reactions
            ev.on('messages.reaction', async reactions => {
                if (enableMetrics) updateEventMetrics('messages.reaction', 'received')
                if (await shouldStoreEvent('messages.reaction', reactions)) {
                    for (const { key, reaction } of reactions) {
                        const msg = await storeImpl.getMessage(key.remoteJid!, key.id!)
                        if (msg) {
                            updateMessageWithReaction(msg, reaction)
                            await storeImpl.updateMessage(key.remoteJid!, key.id!, msg)
                        }
                    }
                    if (enableMetrics) updateEventMetrics('messages.reaction', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('messages.reaction', reactions)
                }
            })
        },

        async loadMessages(jid: string, count: number, cursor: WAMessageCursor): Promise<proto.IWebMessageInfo[]> {
            const mode = !cursor || 'before' in cursor ? 'before' : 'after'
            const cursorKey = cursor ? ('before' in cursor ? cursor.before : cursor.after) : undefined
            
            let messages: proto.IWebMessageInfo[] = []
            
            if (mode === 'before') {
                const query: any = { instanceId, jid }
                
                if (cursorKey) {
                    const cursorMsg = await storeImpl.getMessage(jid, cursorKey.id!)
                    if (cursorMsg) {
                        query.messageTimestamp = { $lt: cursorMsg.messageTimestamp }
                    }
                }
                
                messages = await collections.messages
                    .find(query)
                    .sort({ messageTimestamp: -1 })
                    .limit(count)
                    .toArray()
                    .then(msgs => msgs.map(({ _id, instanceId: _instanceId, jid: _jid, updatedAt: _updatedAt, ...msg }) => convertBinaryToBuffer(msg)))
            }
            
            return messages
        },

        async loadMessage(jid: string, id: string): Promise<proto.IWebMessageInfo | undefined> {
            const msg = await storeImpl.getMessage(jid, id)
            return msg || undefined
        },

        async mostRecentMessage(jid: string): Promise<proto.IWebMessageInfo | undefined> {
            const messages = await storeImpl.getMessages(jid)
            return messages[0]
        },

        async clearAll(): Promise<void> {
            const keys = binaryConversionCache.keys()
            keys.forEach(key => {
                if (key.startsWith(`msg_${instanceId}_`)) {
                    binaryConversionCache.del(key)
                }
            })
            
            await Promise.all([
                collections.chats.deleteMany({ instanceId }),
                collections.contacts.deleteMany({ instanceId }),
                collections.messages.deleteMany({ instanceId }),
                collections.groupMetadata.deleteMany({ instanceId }),
                collections.presences.deleteMany({ instanceId }),
                collections.labels.deleteMany({ instanceId }),
                collections.labelAssociations.deleteMany({ instanceId })
            ])
        },

        getPerformanceStats() {
            const eventMetrics: { [key: string]: EventMetrics } = {}
            eventMetricsMap.forEach((value, key) => {
                eventMetrics[key] = value
            })
            
            return {
                messagesProcessed: 0,
                labelsProcessed: 0,
                batchesProcessed: 0,
                errors: 0,
                lastResetTime: new Date(),
                uptime: 0,
                eventMetrics: enableMetrics ? eventMetrics : undefined
            }
        },
        
        async flushLabelAssociations(): Promise<void> {
            // Placeholder for batch processing
            log('Flushing label associations')
        },
        
        resetPerformanceStats(): void {
            eventMetricsMap.clear()
        },
        
        async recreateIndexes(): Promise<{ created: number; failed: number; details: string[] }> {
            try {
                await createIndexes()
                return { created: 16, failed: 0, details: ['All indexes recreated successfully'] }
            } catch (error) {
                return { created: 0, failed: 16, details: [`Index recreation failed: ${error}`] }
            }
        },

        async getIndexStatus(): Promise<{ collection: string; indexes: any[] }[]> {
            const collectionNames = ['chats', 'contacts', 'messages', 'groupMetadata', 'state', 'presences', 'labels', 'labelAssociations']
            const indexStatus = []
            
            for (const collName of collectionNames) {
                try {
                    const collection = collections[collName as keyof MongoCollections]
                    const indexes = await collection.listIndexes().toArray()
                    indexStatus.push({
                        collection: `${collectionPrefix}${collName}`,
                        indexes: indexes.map(idx => ({
                            name: idx.name,
                            key: idx.key,
                            unique: idx.unique,
                            expireAfterSeconds: idx.expireAfterSeconds
                        }))
                    })
                } catch (error) {
                    indexStatus.push({
                        collection: `${collectionPrefix}${collName}`,
                        indexes: [],
                        error: (error as Error).message
                    })
                }
            }
            
            return indexStatus
        },

        async close(): Promise<void> {
            await pMessageQueue.onIdle()
            await generalQueue.onIdle()
            
            const keys = binaryConversionCache.keys()
            keys.forEach(key => {
                if (key.startsWith(`msg_${instanceId}_`)) {
                    binaryConversionCache.del(key)
                }
            })
            
            if (client) {
                await client.close()
            }
        }
    }

    return storeImpl
}