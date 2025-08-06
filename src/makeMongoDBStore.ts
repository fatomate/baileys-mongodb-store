import { MongoClient, Db, Collection } from 'mongodb'
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
import type { MongoDBStoreConfig, MongoDBStore } from './types'
import NodeCache from 'node-cache'
import PQueue from 'p-queue'

const DEFAULT_TTL_DAYS = 30

interface ActiveConnection {
    client: MongoClient
    database: string
    instanceId: string
    collectionPrefix: string
}

let activeConnections: ActiveConnection[] = []

// Cache for Binary conversions (TTL: 5 minutes, check period: 60 seconds)
const binaryConversionCache = new NodeCache({ stdTTL: 300, checkperiod: 60 })

// Queue configuration for concurrent operations
const QUEUE_CONCURRENCY = 50 // Process up to 50 operations concurrently
const BATCH_SIZE = 100 // Batch size for bulk operations
const BATCH_DELAY = 50 // Delay in ms between batches to avoid overwhelming MongoDB

// Batch accumulator for label associations
interface BatchAccumulator<T> {
    items: T[]
    timer: NodeJS.Timeout | null
    processing: boolean
}

// Performance tracking
interface PerformanceMetrics {
    messagesProcessed: number
    labelsProcessed: number
    batchesProcessed: number
    errors: number
    lastResetTime: Date
}

const performanceMetrics: PerformanceMetrics = {
    messagesProcessed: 0,
    labelsProcessed: 0,
    batchesProcessed: 0,
    errors: 0,
    lastResetTime: new Date()
}

// Helper function to convert MongoDB Binary objects to Buffers with error handling
const convertBinaryToBuffer = (obj: any): any => {
    try {
        if (!obj || typeof obj !== 'object') return obj
        
        // Handle Binary objects
        if (obj.buffer && obj._bsontype === 'Binary') {
            return Buffer.from(obj.buffer)
        }
        
        // Handle arrays
        if (Array.isArray(obj)) {
            return obj.map(item => convertBinaryToBuffer(item))
        }
        
        // Handle nested objects
        const result: any = {}
        for (const key in obj) {
            if (Object.prototype.hasOwnProperty.call(obj, key)) {
                result[key] = convertBinaryToBuffer(obj[key])
            }
        }
        return result
    } catch (error) {
        console.error('Error converting Binary to Buffer:', error)
        return obj // Return original object if conversion fails
    }
}


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

export const makeMongoDBStore = async (config: MongoDBStoreConfig): Promise<MongoDBStore> => {
    const {
        uri,
        database: dbName,
        instanceId,
        ttlDays = DEFAULT_TTL_DAYS,
        collectionPrefix = 'baileys_'
    } = config

    const client = new MongoClient(uri, {
        // Optimize connection pool for high concurrency
        maxPoolSize: 100,
        minPoolSize: 10,
        maxIdleTimeMS: 30000,
        // Write concern for better performance
        writeConcern: { w: 1, j: false }
    })
    await client.connect()
    activeConnections.push({
        client,
        database: dbName,
        instanceId,
        collectionPrefix
    })
    
    const db: Db = client.db(dbName)
    
    // Create queues for different operation types
    const messageQueue = new PQueue({ concurrency: QUEUE_CONCURRENCY })
    const labelQueue = new PQueue({ concurrency: QUEUE_CONCURRENCY })
    const generalQueue = new PQueue({ concurrency: QUEUE_CONCURRENCY })
    
    // Batch accumulators
    const labelAssociationBatch: BatchAccumulator<LabelAssociation> = {
        items: [],
        timer: null,
        processing: false
    }
    
    const messageBatch: BatchAccumulator<proto.IWebMessageInfo & { jid: string }> = {
        items: [],
        timer: null,
        processing: false
    }
    
    // Create collections with TTL indexes
    const collections: MongoCollections = {
        chats: db.collection(`${collectionPrefix}chats`),
        contacts: db.collection(`${collectionPrefix}contacts`),
        messages: db.collection(`${collectionPrefix}messages`),
        groupMetadata: db.collection(`${collectionPrefix}groupMetadata`),
        state: db.collection(`${collectionPrefix}state`),
        presences: db.collection(`${collectionPrefix}presences`),
        labels: db.collection(`${collectionPrefix}labels`),
        labelAssociations: db.collection(`${collectionPrefix}labelAssociations`)
    }

    // Batch processing functions
    const processBatchedLabelAssociations = async () => {
        if (labelAssociationBatch.processing || labelAssociationBatch.items.length === 0) return
        
        labelAssociationBatch.processing = true
        const itemsToProcess = [...labelAssociationBatch.items]
        labelAssociationBatch.items = []
        
        try {
            const bulkOps = itemsToProcess.map(association => ({
                replaceOne: {
                    filter: {
                        instanceId,
                        chatId: association.chatId,
                        labelId: association.labelId,
                        messageId: 'messageId' in association ? association.messageId : ''
                    },
                    replacement: {
                        ...association,
                        instanceId,
                        updatedAt: new Date()
                    },
                    upsert: true
                }
            }))
            
            // Process in chunks to avoid overwhelming MongoDB
            for (let i = 0; i < bulkOps.length; i += BATCH_SIZE) {
                const chunk = bulkOps.slice(i, i + BATCH_SIZE)
                await collections.labelAssociations.bulkWrite(chunk, { ordered: false })
                
                // Small delay between chunks
                if (i + BATCH_SIZE < bulkOps.length) {
                    await new Promise(resolve => setTimeout(resolve, BATCH_DELAY))
                }
            }
            
            performanceMetrics.labelsProcessed += itemsToProcess.length
            performanceMetrics.batchesProcessed++
        } catch (error) {
            console.error('Error processing label associations batch:', error)
            performanceMetrics.errors++
        } finally {
            labelAssociationBatch.processing = false
        }
    }
    
    const processBatchedMessages = async () => {
        if (messageBatch.processing || messageBatch.items.length === 0) return
        
        messageBatch.processing = true
        const itemsToProcess = [...messageBatch.items]
        messageBatch.items = []
        
        try {
            const bulkOps = itemsToProcess.map(({ jid, ...message }) => ({
                replaceOne: {
                    filter: {
                        instanceId,
                        jid,
                        'key.id': message.key?.id
                    },
                    replacement: {
                        ...message,
                        instanceId,
                        jid,
                        updatedAt: new Date()
                    },
                    upsert: true
                }
            }))
            
            // Process in chunks
            for (let i = 0; i < bulkOps.length; i += BATCH_SIZE) {
                const chunk = bulkOps.slice(i, i + BATCH_SIZE)
                await collections.messages.bulkWrite(chunk, { ordered: false })
                
                // Clear cache for these messages
                chunk.forEach(op => {
                    const msgId = op.replaceOne.filter['key.id']
                    const msgJid = op.replaceOne.filter.jid
                    const cacheKey = `msg_${instanceId}_${msgJid}_${msgId}`
                    binaryConversionCache.del(cacheKey)
                })
                
                if (i + BATCH_SIZE < bulkOps.length) {
                    await new Promise(resolve => setTimeout(resolve, BATCH_DELAY))
                }
            }
            
            performanceMetrics.messagesProcessed += itemsToProcess.length
            performanceMetrics.batchesProcessed++
        } catch (error) {
            console.error('Error processing messages batch:', error)
            performanceMetrics.errors++
        } finally {
            messageBatch.processing = false
        }
    }
    
    // Schedule batch processing
    const scheduleLabelBatch = () => {
        if (labelAssociationBatch.timer) {
            clearTimeout(labelAssociationBatch.timer)
        }
        labelAssociationBatch.timer = setTimeout(() => {
            processBatchedLabelAssociations()
        }, 100) // Process after 100ms of inactivity
    }
    
    const scheduleMessageBatch = () => {
        if (messageBatch.timer) {
            clearTimeout(messageBatch.timer)
        }
        messageBatch.timer = setTimeout(() => {
            processBatchedMessages()
        }, 100)
    }
    
    // Create indexes
    const createIndexes = async () => {
        // TTL indexes for automatic expiration
        const ttlSeconds = ttlDays * 24 * 60 * 60
        
        await Promise.all([
            // Chats indexes
            collections.chats.createIndex({ instanceId: 1, id: 1 }, { unique: true }),
            collections.chats.createIndex({ updatedAt: 1 }, { expireAfterSeconds: ttlSeconds }),
            
            // Contacts indexes
            collections.contacts.createIndex({ instanceId: 1, id: 1 }, { unique: true }),
            collections.contacts.createIndex({ updatedAt: 1 }, { expireAfterSeconds: ttlSeconds }),
            
            // Messages indexes
            collections.messages.createIndex({ instanceId: 1, jid: 1, 'key.id': 1 }, { unique: true }),
            collections.messages.createIndex({ instanceId: 1, jid: 1, messageTimestamp: -1 }),
            collections.messages.createIndex({ updatedAt: 1 }, { expireAfterSeconds: ttlSeconds }),
            
            // Group metadata indexes
            collections.groupMetadata.createIndex({ instanceId: 1, id: 1 }, { unique: true }),
            collections.groupMetadata.createIndex({ updatedAt: 1 }, { expireAfterSeconds: ttlSeconds }),
            
            // State indexes
            collections.state.createIndex({ instanceId: 1 }, { unique: true }),
            
            // Presences indexes
            collections.presences.createIndex({ instanceId: 1, id: 1 }, { unique: true }),
            collections.presences.createIndex({ updatedAt: 1 }, { expireAfterSeconds: ttlSeconds }),
            
            // Labels indexes
            collections.labels.createIndex({ instanceId: 1, id: 1 }, { unique: true }),
            collections.labels.createIndex({ updatedAt: 1 }, { expireAfterSeconds: ttlSeconds }),
            
            // Label associations indexes
            collections.labelAssociations.createIndex({ instanceId: 1, chatId: 1, labelId: 1, messageId: 1 }, { unique: true }),
            collections.labelAssociations.createIndex({ instanceId: 1, chatId: 1 }),
            collections.labelAssociations.createIndex({ instanceId: 1, messageId: 1 }),
            collections.labelAssociations.createIndex({ updatedAt: 1 }, { expireAfterSeconds: ttlSeconds })
        ])
        
        // MongoDB indexes created successfully
    }
    
    await createIndexes()

    const store: MongoDBStore = {
        instanceId,

        async getChats(): Promise<Chat[]> {
            const chats = await collections.chats
                .find({ instanceId })
                .sort({ conversationTimestamp: -1 })
                .toArray()
            
            return chats.map(({ _id: _1, instanceId: _2, updatedAt: _3, ...chat }) => chat as Chat)
        },

        async getChat(jid: string): Promise<Chat | null> {
            const chat = await collections.chats.findOne({ instanceId, id: jid })
            if (!chat) return null
            
            const { _id: _1, instanceId: _2, updatedAt: _3, ...chatData } = chat
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
                { 
                    $set: { ...update, updatedAt: new Date() }
                }
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
                const { _id: _1, instanceId: _2, updatedAt: _3, ...contactData } = contact
                contactsMap[contact.id] = contactData as Contact
            }
            
            return contactsMap
        },

        async getContact(jid: string): Promise<Contact | null> {
            const contact = await collections.contacts.findOne({ instanceId, id: jid })
            if (!contact) return null
            
            const { _id: _1, instanceId: _2, updatedAt: _3, ...contactData } = contact
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
            
            // Process in chunks for large contact lists
            for (let i = 0; i < bulkOps.length; i += BATCH_SIZE) {
                const chunk = bulkOps.slice(i, i + BATCH_SIZE)
                await generalQueue.add(async () => {
                    await collections.contacts.bulkWrite(chunk, { ordered: false })
                })
                
                // Small delay between chunks for very large imports
                if (i + BATCH_SIZE < bulkOps.length && bulkOps.length > 1000) {
                    await new Promise(resolve => setTimeout(resolve, BATCH_DELAY))
                }
            }
        },

        async getMessages(jid: string): Promise<proto.IWebMessageInfo[]> {
            const messages = await collections.messages
                .find({ instanceId, jid })
                .sort({ messageTimestamp: -1 })
                .toArray()
            
            // Convert Binary objects and preserve messageContextInfo
            return messages.map(({ _id: _1, instanceId: _2, jid: _3, updatedAt: _4, ...msg }) => convertBinaryToBuffer(msg))
        },

        async getMessage(jid: string, id: string): Promise<proto.IWebMessageInfo | null> {
            // Check cache first
            const cacheKey = `msg_${instanceId}_${jid}_${id}`
            const cached = binaryConversionCache.get<proto.IWebMessageInfo>(cacheKey)
            if (cached) return cached
            
            const message = await collections.messages.findOne({
                instanceId,
                jid,
                'key.id': id
            })
            
            if (!message) return null
            
            const { _id: _1, instanceId: _2, jid: _3, updatedAt: _4, ...msg } = message
            // Convert all MongoDB Binary objects to Buffers and preserve messageContextInfo
            const converted = convertBinaryToBuffer(msg)
            
            // Cache the converted message
            binaryConversionCache.set(cacheKey, converted)
            
            return converted
        },

        async upsertMessage(jid: string, message: proto.IWebMessageInfo, useBatch: boolean = false): Promise<void> {
            if (useBatch) {
                // Add to batch for processing
                messageBatch.items.push({ ...message, jid })
                scheduleMessageBatch()
                
                // Force process if batch is full
                if (messageBatch.items.length >= BATCH_SIZE) {
                    await processBatchedMessages()
                }
            } else {
                // Invalidate cache for this message
                const cacheKey = `msg_${instanceId}_${jid}_${message.key?.id}`
                binaryConversionCache.del(cacheKey)
                
                await messageQueue.add(async () => {
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
            }
        },

        async updateMessage(jid: string, id: string, update: Partial<proto.IWebMessageInfo>): Promise<boolean> {
            // Invalidate cache for this message
            const cacheKey = `msg_${instanceId}_${jid}_${id}`
            binaryConversionCache.del(cacheKey)
            
            // Ensure Binary objects remain as Buffers during updates
            const processedUpdate = convertBinaryToBuffer(update)
            
            const result = await collections.messages.updateOne(
                {
                    instanceId,
                    jid,
                    'key.id': id
                },
                {
                    $set: { ...processedUpdate, updatedAt: new Date() }
                }
            )
            
            return result.modifiedCount > 0
        },

        async deleteMessages(jid: string, ids?: string[]): Promise<void> {
            // Clear cache for deleted messages
            if (ids && ids.length > 0) {
                ids.forEach(id => {
                    const cacheKey = `msg_${instanceId}_${jid}_${id}`
                    binaryConversionCache.del(cacheKey)
                })
            } else {
                // Clear all cache entries for this jid if deleting all messages
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
            
            const { _id: _1, instanceId: _2, updatedAt: _3, ...metadataData } = metadata
            return metadataData as GroupMetadata
        },

        async upsertGroupMetadata(jid: string, metadata: GroupMetadata): Promise<void> {
            await collections.groupMetadata.replaceOne(
                { instanceId, id: jid },
                { ...metadata, instanceId, updatedAt: new Date() },
                { upsert: true }
            )
        },

        async getState(): Promise<ConnectionState> {
            const state = await collections.state.findOne({ instanceId })
            if (!state) return { connection: 'close' }
            
            const { _id: _1, instanceId: _2, updatedAt: _3, ...stateData } = state
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
                const { _id: _1, instanceId: _2, updatedAt: _3, ...labelData } = label
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
            
            return associations.map(({ _id: _1, instanceId: _2, updatedAt: _3, ...assoc }) => assoc as LabelAssociation)
        },

        async getChatLabels(chatId: string): Promise<LabelAssociation[]> {
            const associations = await collections.labelAssociations
                .find({ instanceId, chatId })
                .toArray()
            
            return associations.map(({ _id: _1, instanceId: _2, updatedAt: _3, ...assoc }) => assoc as LabelAssociation)
        },

        async getMessageLabels(messageId: string): Promise<string[]> {
            const associations = await collections.labelAssociations
                .find({ instanceId, messageId })
                .toArray()
            
            return associations.map(assoc => assoc.labelId)
        },

        async upsertLabelAssociation(association: LabelAssociation): Promise<void> {
            // Always use batching for label associations as they come in bulk
            labelAssociationBatch.items.push(association)
            scheduleLabelBatch()
            
            // Force process if batch is full
            if (labelAssociationBatch.items.length >= BATCH_SIZE) {
                await processBatchedLabelAssociations()
            }
        },

        async deleteLabelAssociation(association: LabelAssociation): Promise<void> {
            await collections.labelAssociations.deleteOne({
                instanceId,
                chatId: association.chatId,
                labelId: association.labelId,
                messageId: 'messageId' in association ? association.messageId : ''
            })
        },

        bind(ev: BaileysEventEmitter): void {
            ev.on('connection.update', async update => {
                await store.updateState(update)
            })

            ev.on('messaging-history.set', async ({ chats: newChats, contacts: newContacts, messages: newMessages, isLatest }) => {
                if (isLatest) {
                    await store.clearAll()
                }
                
                // Process in parallel with proper queue management
                const promises: Promise<void>[] = []
                
                if (newChats?.length) {
                    promises.push(generalQueue.add(async () => {
                        await store.upsertChats(...newChats)
                    }))
                }
                
                if (newContacts?.length) {
                    promises.push(generalQueue.add(async () => {
                        await store.upsertContacts(newContacts)
                    }))
                }
                
                if (newMessages?.length) {
                    // Use batch processing for messages
                    for (const msg of newMessages) {
                        const jid = msg.key.remoteJid!
                        await store.upsertMessage(jid, msg, true) // Use batch mode
                    }
                    // Force process any remaining messages
                    await processBatchedMessages()
                }
                
                await Promise.all(promises)
            })

            ev.on('contacts.upsert', async contacts => {
                await store.upsertContacts(contacts)
            })

            ev.on('contacts.update', async updates => {
                for (const update of updates) {
                    const contact = await store.getContact(update.id!)
                    if (contact) {
                        Object.assign(contact, update)
                        await store.upsertContacts([contact])
                    }
                }
            })

            ev.on('chats.upsert', async newChats => {
                await store.upsertChats(...newChats)
            })

            ev.on('chats.update', async updates => {
                for (const update of updates) {
                    await store.updateChat(update.id!, update)
                }
            })

            ev.on('labels.edit', async (label) => {
                if (label.deleted) {
                    await store.deleteLabel(label.id)
                } else {
                    await store.upsertLabel(label.id, label)
                }
            })

            ev.on('labels.association', async ({ type, association }) => {
                if (type === 'add') {
                    await store.upsertLabelAssociation(association)
                } else if (type === 'remove') {
                    await store.deleteLabelAssociation(association)
                }
            })

            ev.on('presence.update', async ({ id, presences: update }) => {
                const existing = (await store.getPresences())[id] || {}
                Object.assign(existing, update)
                await store.updatePresence(id, existing)
            })

            ev.on('chats.delete', async deletions => {
                await store.deleteChats(deletions)
            })

            ev.on('messages.upsert', async ({ messages: newMessages, type }) => {
                for (const msg of newMessages) {
                    const jid = jidNormalizedUser(msg.key.remoteJid!)
                    await store.upsertMessage(jid, msg)
                    
                    if (type === 'notify' && !(await store.getChat(jid))) {
                        await store.upsertChats({
                            id: jid,
                            conversationTimestamp: toNumber(msg.messageTimestamp),
                            unreadCount: 1
                        } as Chat)
                    }
                }
            })

            ev.on('messages.update', async updates => {
                for (const { update, key } of updates) {
                    const jid = jidNormalizedUser(key.remoteJid!)
                    await store.updateMessage(jid, key.id!, update)
                }
            })

            ev.on('messages.delete', async item => {
                if ('all' in item) {
                    await store.deleteMessages(item.jid)
                } else {
                    const jid = item.keys[0].remoteJid!
                    const ids = item.keys.map(k => k.id!)
                    await store.deleteMessages(jid, ids)
                }
            })

            ev.on('groups.update', async updates => {
                for (const update of updates) {
                    const metadata = await store.getGroupMetadata(update.id!)
                    if (metadata) {
                        Object.assign(metadata, update)
                        await store.upsertGroupMetadata(update.id!, metadata)
                    }
                }
            })

            ev.on('group-participants.update', async ({ id, participants, action }) => {
                const metadata = await store.getGroupMetadata(id)
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
                    await store.upsertGroupMetadata(id, metadata)
                }
            })

            ev.on('message-receipt.update', async updates => {
                for (const { key, receipt } of updates) {
                    const msg = await store.getMessage(key.remoteJid!, key.id!)
                    if (msg) {
                        updateMessageWithReceipt(msg, receipt)
                        await store.updateMessage(key.remoteJid!, key.id!, msg)
                    }
                }
            })

            ev.on('messages.reaction', async reactions => {
                for (const { key, reaction } of reactions) {
                    const msg = await store.getMessage(key.remoteJid!, key.id!)
                    if (msg) {
                        updateMessageWithReaction(msg, reaction)
                        await store.updateMessage(key.remoteJid!, key.id!, msg)
                    }
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
                    const cursorMsg = await store.getMessage(jid, cursorKey.id!)
                    if (cursorMsg) {
                        query.messageTimestamp = { $lt: cursorMsg.messageTimestamp }
                    }
                }
                
                messages = await collections.messages
                    .find(query)
                    .sort({ messageTimestamp: -1 })
                    .limit(count)
                    .toArray()
                    .then(msgs => msgs.map(({ _id: _1, instanceId: _2, jid: _3, updatedAt: _4, ...msg }) => convertBinaryToBuffer(msg)))
            }
            
            return messages
        },

        async loadMessage(jid: string, id: string): Promise<proto.IWebMessageInfo | undefined> {
            const msg = await store.getMessage(jid, id)
            return msg || undefined
        },

        async mostRecentMessage(jid: string): Promise<proto.IWebMessageInfo | undefined> {
            const messages = await store.getMessages(jid)
            return messages[0]
        },

        async clearAll(): Promise<void> {
            // Clear all cache entries for this instance
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

        getPerformanceStats(): PerformanceMetrics & { uptime: number } {
            const uptime = Date.now() - performanceMetrics.lastResetTime.getTime()
            return {
                ...performanceMetrics,
                uptime
            }
        },
        
        resetPerformanceStats(): void {
            performanceMetrics.messagesProcessed = 0
            performanceMetrics.labelsProcessed = 0
            performanceMetrics.batchesProcessed = 0
            performanceMetrics.errors = 0
            performanceMetrics.lastResetTime = new Date()
        },
        
        async close(): Promise<void> {
            // Process any remaining batches
            await processBatchedLabelAssociations()
            await processBatchedMessages()
            
            // Clear all timers
            if (labelAssociationBatch.timer) {
                clearTimeout(labelAssociationBatch.timer)
            }
            if (messageBatch.timer) {
                clearTimeout(messageBatch.timer)
            }
            
            // Wait for all queues to finish
            await Promise.all([
                messageQueue.onIdle(),
                labelQueue.onIdle(),
                generalQueue.onIdle()
            ])
            
            // Clear all cache entries for this instance
            const keys = binaryConversionCache.keys()
            keys.forEach(key => {
                if (key.startsWith(`msg_${instanceId}_`)) {
                    binaryConversionCache.del(key)
                }
            })
            
            await client.close()
            activeConnections = activeConnections.filter(c => c.client !== client)
        }
    }

    return store
}

/**
 * Cleanup MongoDB store data for a specific instance
 * @param instanceId - The instance ID to cleanup. If not provided, closes all connections.
 * @param deleteData - Whether to delete all data for the instance (default: false)
 */
export const cleanupMongoDBStore = async (instanceId?: string, deleteData: boolean = false): Promise<void> => {
    if (!instanceId) {
        // Just close all connections
        for (const conn of activeConnections) {
            try {
                await conn.client.close()
            } catch (error) {
                console.error('Error closing MongoDB connection:', error)
            }
        }
        activeConnections = []
        return
    }

    // Find connections for the specific instance
    const instanceConnections = activeConnections.filter(c => c.instanceId === instanceId)
    
    if (instanceConnections.length === 0) {
        console.warn(`No active connections found for instance: ${instanceId}`)
        return
    }

    for (const conn of instanceConnections) {
        try {
            if (deleteData) {
                // Delete all data for this instance
                const db = conn.client.db(conn.database)
                const collections = [
                    `${conn.collectionPrefix}chats`,
                    `${conn.collectionPrefix}contacts`,
                    `${conn.collectionPrefix}messages`,
                    `${conn.collectionPrefix}groupMetadata`,
                    `${conn.collectionPrefix}state`,
                    `${conn.collectionPrefix}presences`,
                    `${conn.collectionPrefix}labels`,
                    `${conn.collectionPrefix}labelAssociations`
                ]

                for (const collName of collections) {
                    try {
                        await db.collection(collName).deleteMany({ instanceId })
                    } catch (error) {
                        console.error(`Error deleting data from ${collName}:`, error)
                    }
                }
                console.log(`Deleted all data for instance: ${instanceId}`)
            }

            // Close the connection
            await conn.client.close()
            
        } catch (error) {
            console.error(`Error cleaning up instance ${instanceId}:`, error)
        }
    }

    // Remove from active connections
    activeConnections = activeConnections.filter(c => c.instanceId !== instanceId)
}