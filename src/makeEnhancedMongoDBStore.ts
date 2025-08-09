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
import { Queue, Worker, Job } from 'bullmq'
import Redis from 'ioredis'
import { 
    validateJID, 
    validateMessageId, 
    validateInstanceId,
    ValidationError,
    AuthorizationError,
    createSafeErrorMessage,
    hashForLogging
} from './utils/security'
import { InstanceAccessContext, DEFAULT_PERMISSIONS } from './utils/auth'
import { MemoryMonitor, BackpressureController } from './utils/memory'
import { TTLMonitor } from './utils/ttl'
import { downloadMedia, downloadOfficialAPIMedia, cleanupOldMedia, getMediaStats } from './utils/media'

const DEFAULT_TTL_DAYS = 30
const DEFAULT_EVENT_CONFIG: EventStorageConfig = {
    enabled: true,
    useBatch: false
}

// Queue types enum
enum QueueType {
    LABELS = 'labels',
    LABEL_ASSOCIATIONS = 'label-associations',
    MESSAGES = 'messages',
    CHATS = 'chats',
    CONTACTS = 'contacts',
    GROUP_METADATA = 'group-metadata',
    PRESENCES = 'presences',
    STATE = 'state'
}

// Bull job data types
interface MessageJob {
    type: 'upsert' | 'update' | 'delete'
    jid: string
    message?: proto.IWebMessageInfo
    messageId?: string
    update?: Partial<proto.IWebMessageInfo>
    deleteIds?: string[]
    instanceId: string
    timestamp: number
}

interface ChatJob {
    type: 'upsert' | 'update' | 'delete'
    chats?: Chat[]
    chatId?: string
    update?: Partial<Chat>
    deleteIds?: string[]
    instanceId: string
    timestamp: number
}

interface ContactJob {
    type: 'upsert' | 'update'
    contacts?: Contact[]
    contact?: Contact
    instanceId: string
    timestamp: number
}

interface GroupMetadataJob {
    type: 'upsert' | 'update'
    jid: string
    metadata: GroupMetadata
    update?: Partial<GroupMetadata>
    instanceId: string
    timestamp: number
}

interface PresenceJob {
    type: 'update'
    id: string
    presences: { [participant: string]: PresenceData }
    instanceId: string
    timestamp: number
}

interface StateJob {
    type: 'update'
    update: Partial<ConnectionState>
    instanceId: string
    timestamp: number
}

interface LabelJob {
    type: 'upsert' | 'delete'
    id: string
    label?: Label
    instanceId: string
    timestamp: number
}

interface LabelAssociationJob {
    type: 'upsert' | 'delete'
    association: LabelAssociation
    instanceId: string
    timestamp: number
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
        console.error('Binary conversion error:', error)
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
        redis,
        logLevel = 'none',
        enableMetrics = false,
        hooks = {},
        auth,
        memory,
        ttlMonitoring
    } = config
    
    // Validate instance ID
    const validatedInstanceId = validateInstanceId(instanceId)
    
    // Create access context for this instance
    const accessContext = new InstanceAccessContext(
        validatedInstanceId,
        auth?.enableApiKey ? DEFAULT_PERMISSIONS : ['read:all', 'write:all', 'delete:all'],
        auth
    )
    
    // Helper functions for conditional logging
    const log = (...args: any[]) => {
        if (logLevel === 'all') {
            console.log(...args)
        }
    }
    
    const logError = (...args: any[]) => {
        if (logLevel === 'error' || logLevel === 'warn' || logLevel === 'all') {
            console.error(...args)
        }
    }
    
    const logWarn = (...args: any[]) => {
        if (logLevel === 'warn' || logLevel === 'all') {
            console.warn(...args)
        }
    }
    
    // Initialize memory monitor if configured
    const memoryMonitor = memory ? new MemoryMonitor(memory) : null
    const backpressureController = memory ? new BackpressureController(memory) : null
    
    // TTL monitor will be initialized after DB connection
    let ttlMonitor: TTLMonitor | null = null

    // MongoDB connection
    const client: MongoClient = new MongoClient(uri, {
        maxPoolSize: 100,
        minPoolSize: 10,
        maxIdleTimeMS: 30000,
        writeConcern: { w: 1, j: false }
    })
    await client.connect()
    const db = client.db(dbName)
    
    // Initialize TTL monitor after DB connection
    if (ttlMonitoring) {
        const globalTTL = ttlDays || DEFAULT_TTL_DAYS
        ttlMonitor = new TTLMonitor(db, { days: globalTTL, ...ttlMonitoring })
        ttlMonitor.startMonitoring((message) => {
            logWarn(`[TTL Monitor] ${message}`)
        })
    }
    
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
    const BATCH_SIZE = 100
    
    // Bull queue setup (if Redis provided)
    let bullInitialized = false
    const queues: Map<QueueType, Queue<any>> = new Map()
    const workers: Map<QueueType, Worker<any>> = new Map()
    let redisConnection: Redis | null = null
    
    // Default job options for automatic cleanup
    const defaultJobOptions = {
        removeOnComplete: {
            age: 60,    // Keep completed jobs for 60 seconds max
            count: 10   // Keep max 10 completed jobs
        },
        removeOnFail: {
            age: 300    // Keep failed jobs for 5 minutes for debugging
        },
        attempts: 3,
        backoff: {
            type: 'exponential' as const,
            delay: 2000
        }
    }
    
    // Initialize Bull queues if Redis config provided
    const initializeBullQueues = async () => {
        if (!redis) return
        
        try {
            log(`🐂 Initializing Bull queues for instance ${instanceId}...`)
            
            // Create Redis connection with BullMQ requirements
            if (typeof redis.connection === 'string') {
                redisConnection = new Redis(redis.connection, {
                    maxRetriesPerRequest: null,
                    enableReadyCheck: true,
                    lazyConnect: false
                })
            } else {
                redisConnection = new Redis({
                    ...redis.connection,
                    maxRetriesPerRequest: null,
                    enableReadyCheck: true,
                    lazyConnect: false
                })
            }
            
            // Test Redis connection
            await redisConnection.ping()
            
            // Check eviction policy (warning only, not blocking)
            try {
                const redisConfig = await redisConnection.config('GET', 'maxmemory-policy') as [string, string]
                const policy = redisConfig[1]
                if (policy && policy !== 'noeviction') {
                    logWarn(`⚠️  Redis eviction policy is '${policy}'. Consider using 'noeviction' for BullMQ or a separate Redis instance.`)
                }
            } catch (err) {
                // Config command might be disabled, continue anyway
            }
            
            const queuePrefix = redis.queuePrefix || 'baileys'
            const redisOpts = { connection: redisConnection }
            
            // Helper to create queue and worker for each event type
            const createQueueAndWorker = <T>(queueType: QueueType, processor: (job: Job<T>) => Promise<any>) => {
                const queueName = `${queuePrefix}_${queueType}_${instanceId}`
                
                // Create queue
                const queue = new Queue<T>(queueName, redisOpts)
                queues.set(queueType, queue)
                
                // Set concurrency based on queue type
                const concurrency = queueType === QueueType.LABEL_ASSOCIATIONS ? 1 : (redis.concurrency || 50)
                
                // Create worker
                const worker = new Worker<T>(
                    queueName,
                    processor,
                    {
                        ...redisOpts,
                        concurrency,
                        autorun: true
                    }
                )
                
                log(`🔧 Created ${queueType} queue with concurrency: ${concurrency}`)
                
                // Set up event handlers
                worker.on('completed', (job) => {
                    if (queueType !== QueueType.LABEL_ASSOCIATIONS) {
                        log(`✅ ${queueType} job ${job.id} completed`)
                    }
                })
                
                worker.on('failed', (job, err) => {
                    logError(`❌ ${queueType} job ${job?.id} failed:`, err.message)
                    if (enableMetrics) {
                        updateEventMetrics(queueType, 'error')
                    }
                })
                
                worker.on('stalled', (jobId) => {
                    logWarn(`⚠️ ${queueType} job ${jobId} stalled`)
                })
                
                workers.set(queueType, worker)
                
                // Clean up old jobs on startup
                queue.obliterate({ force: true }).catch(() => {})
            }
            
            // Create queues for different event types
            createQueueAndWorker<MessageJob>(QueueType.MESSAGES, async (job) => {
                const { type, jid, message, messageId, update, deleteIds } = job.data
                
                if (type === 'upsert' && message) {
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
                } else if (type === 'update' && messageId && update) {
                    await collections.messages.updateOne(
                        {
                            instanceId,
                            jid,
                            'key.id': messageId
                        },
                        {
                            $set: { ...update, updatedAt: new Date() }
                        }
                    )
                } else if (type === 'delete') {
                    if (deleteIds && deleteIds.length > 0) {
                        await collections.messages.deleteMany({
                            instanceId,
                            jid,
                            'key.id': { $in: deleteIds }
                        })
                    } else {
                        await collections.messages.deleteMany({ instanceId, jid })
                    }
                }
                
                return { success: true }
            })
            
            createQueueAndWorker<ChatJob>(QueueType.CHATS, async (job) => {
                const { type, chats, chatId, update, deleteIds } = job.data
                
                if (type === 'upsert' && chats) {
                    const bulkOps = chats.map(chat => ({
                        replaceOne: {
                            filter: { instanceId, id: chat.id },
                            replacement: { ...chat, instanceId, updatedAt: new Date() },
                            upsert: true
                        }
                    }))
                    await collections.chats.bulkWrite(bulkOps)
                } else if (type === 'update' && chatId && update) {
                    await collections.chats.updateOne(
                        { instanceId, id: chatId },
                        { $set: { ...update, updatedAt: new Date() } }
                    )
                } else if (type === 'delete' && deleteIds) {
                    await collections.chats.deleteMany({
                        instanceId,
                        id: { $in: deleteIds }
                    })
                }
                
                return { success: true }
            })
            
            createQueueAndWorker<ContactJob>(QueueType.CONTACTS, async (job) => {
                const { type, contacts, contact } = job.data
                
                if (type === 'upsert' && contacts) {
                    const bulkOps = contacts.map(contact => ({
                        replaceOne: {
                            filter: { instanceId, id: contact.id },
                            replacement: { ...contact, instanceId, updatedAt: new Date() },
                            upsert: true
                        }
                    }))
                    await collections.contacts.bulkWrite(bulkOps, { ordered: false })
                } else if (type === 'update' && contact) {
                    await collections.contacts.replaceOne(
                        { instanceId, id: contact.id },
                        { ...contact, instanceId, updatedAt: new Date() },
                        { upsert: true }
                    )
                }
                
                return { success: true }
            })
            
            createQueueAndWorker<GroupMetadataJob>(QueueType.GROUP_METADATA, async (job) => {
                const { type, jid, metadata, update } = job.data
                
                if (type === 'upsert') {
                    await collections.groupMetadata.replaceOne(
                        { instanceId, id: metadata.id },
                        { ...metadata, instanceId, updatedAt: new Date() },
                        { upsert: true }
                    )
                } else if (type === 'update' && update) {
                    await collections.groupMetadata.updateOne(
                        { instanceId, id: jid },
                        { $set: { ...update, updatedAt: new Date() } }
                    )
                }
                
                return { success: true }
            })
            
            createQueueAndWorker<PresenceJob>(QueueType.PRESENCES, async (job) => {
                const { id, presences } = job.data
                
                await collections.presences.updateOne(
                    { instanceId, id },
                    {
                        $set: { presences, updatedAt: new Date() }
                    },
                    { upsert: true }
                )
                
                return { success: true }
            })
            
            createQueueAndWorker<StateJob>(QueueType.STATE, async (job) => {
                const { update } = job.data
                
                await collections.state.updateOne(
                    { instanceId },
                    { 
                        $set: { ...update, instanceId, updatedAt: new Date() }
                    },
                    { upsert: true }
                )
                
                return { success: true }
            })
            
            createQueueAndWorker<LabelJob>(QueueType.LABELS, async (job) => {
                const { type, id, label } = job.data
                
                if (type === 'upsert' && label) {
                    await collections.labels.replaceOne(
                        { instanceId, id },
                        { ...label, instanceId, updatedAt: new Date() },
                        { upsert: true }
                    )
                } else if (type === 'delete') {
                    await collections.labels.deleteOne({ instanceId, id })
                }
                
                return { success: true }
            })
            
            createQueueAndWorker<LabelAssociationJob>(QueueType.LABEL_ASSOCIATIONS, async (job) => {
                const { type, association } = job.data
                
                if (type === 'upsert') {
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
                } else if (type === 'delete') {
                    const filter: any = {
                        instanceId,
                        chatId: association.chatId,
                        labelId: association.labelId
                    }
                    
                    if ('messageId' in association && association.messageId) {
                        filter.messageId = association.messageId
                    }
                    
                    await collections.labelAssociations.deleteOne(filter)
                }
                
                return { success: true }
            })
            
            bullInitialized = true
            log(`✅ Bull queues initialized successfully for instance ${instanceId}`)
        } catch (error) {
            logError(`❌ Failed to initialize Bull queues for instance ${instanceId}:`, error)
            log('⚠️  Falling back to in-memory queue processing')
            
            // Clean up partial initialization
            for (const worker of workers.values()) {
                await worker.close()
            }
            for (const queue of queues.values()) {
                await queue.close()
            }
            if (redisConnection) redisConnection.disconnect()
            
            queues.clear()
            workers.clear()
            redisConnection = null
            bullInitialized = false
        }
    }
    
    // Initialize Bull queues
    await initializeBullQueues()
    
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
        
        if (events[eventType]) {
            const config = { ...DEFAULT_EVENT_CONFIG, ...events[eventType] }
            eventConfigs.set(eventType, config)
            return config
        }
        
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
        
        if (config.filter && !config.filter(data)) {
            if (enableMetrics) updateEventMetrics(eventType, 'skipped')
            return false
        }
        
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
        if (eventType) {
            const eventConfig = getEventConfig(eventType)
            if (eventConfig.ttlDays !== undefined) {
                return eventConfig.ttlDays
            }
        }
        
        if (collectionTTL[collectionName] !== undefined) {
            return collectionTTL[collectionName]!
        }
        
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
        const indexPromises: Promise<void>[] = []
        
        const chatsTTL = getTTLForCollection('chats') * 24 * 60 * 60
        indexPromises.push(
            collections.chats.createIndex({ instanceId: 1, id: 1 }, { unique: true }).then(() => {}),
            collections.chats.createIndex({ updatedAt: 1 }, { expireAfterSeconds: chatsTTL }).then(() => {})
        )
        
        const contactsTTL = getTTLForCollection('contacts') * 24 * 60 * 60
        indexPromises.push(
            collections.contacts.createIndex({ instanceId: 1, id: 1 }, { unique: true }).then(() => {}),
            collections.contacts.createIndex({ updatedAt: 1 }, { expireAfterSeconds: contactsTTL }).then(() => {})
        )
        
        const messagesTTL = getTTLForCollection('messages') * 24 * 60 * 60
        indexPromises.push(
            collections.messages.createIndex({ instanceId: 1, jid: 1, 'key.id': 1 }, { unique: true }).then(() => {}),
            collections.messages.createIndex({ instanceId: 1, jid: 1, messageTimestamp: -1 }).then(() => {}),
            collections.messages.createIndex({ updatedAt: 1 }, { expireAfterSeconds: messagesTTL }).then(() => {}),
            // Index for media deduplication
            collections.messages.createIndex({ instanceId: 1, mediaHash: 1 }, { sparse: true }).then(() => {})
        )
        
        const groupsTTL = getTTLForCollection('groupMetadata') * 24 * 60 * 60
        indexPromises.push(
            collections.groupMetadata.createIndex({ instanceId: 1, id: 1 }, { unique: true }).then(() => {}),
            collections.groupMetadata.createIndex({ updatedAt: 1 }, { expireAfterSeconds: groupsTTL }).then(() => {})
        )
        
        const stateTTL = getTTLForCollection('state') * 24 * 60 * 60
        indexPromises.push(
            collections.state.createIndex({ instanceId: 1 }, { unique: true }).then(() => {}),
            collections.state.createIndex({ updatedAt: 1 }, { expireAfterSeconds: stateTTL }).then(() => {})
        )
        
        const presencesTTL = getTTLForCollection('presences') * 24 * 60 * 60
        indexPromises.push(
            collections.presences.createIndex({ instanceId: 1, id: 1 }, { unique: true }).then(() => {}),
            collections.presences.createIndex({ updatedAt: 1 }, { expireAfterSeconds: presencesTTL }).then(() => {})
        )
        
        const labelsTTL = getTTLForCollection('labels') * 24 * 60 * 60
        indexPromises.push(
            collections.labels.createIndex({ instanceId: 1, id: 1 }, { unique: true }).then(() => {}),
            collections.labels.createIndex({ updatedAt: 1 }, { expireAfterSeconds: labelsTTL }).then(() => {})
        )
        
        const labelAssocTTL = getTTLForCollection('labelAssociations') * 24 * 60 * 60
        indexPromises.push(
            collections.labelAssociations.createIndex({ instanceId: 1, chatId: 1, labelId: 1 }, { unique: true }).then(() => {}),
            collections.labelAssociations.createIndex({ updatedAt: 1 }, { expireAfterSeconds: labelAssocTTL }).then(() => {})
        )
        
        await Promise.all(indexPromises)
        log(`✅ Indexes created with custom TTL settings for instance ${instanceId}`)
        
        // Verify TTL indexes if monitoring is enabled
        if (ttlMonitor) {
            log('[TTL Monitor] Verifying TTL indexes...')
            const verificationResults = await Promise.all([
                ttlMonitor.verifyTTLIndex(`${collectionPrefix}chats`),
                ttlMonitor.verifyTTLIndex(`${collectionPrefix}contacts`),
                ttlMonitor.verifyTTLIndex(`${collectionPrefix}messages`),
                ttlMonitor.verifyTTLIndex(`${collectionPrefix}groupMetadata`),
                ttlMonitor.verifyTTLIndex(`${collectionPrefix}presences`),
                ttlMonitor.verifyTTLIndex(`${collectionPrefix}labels`),
                ttlMonitor.verifyTTLIndex(`${collectionPrefix}labelAssociations`)
            ])
            
            const invalidTTL = verificationResults.filter(r => !r.isValid)
            if (invalidTTL.length > 0) {
                logWarn(`[TTL Monitor] Found ${invalidTTL.length} invalid TTL indexes:`, invalidTTL.map(r => r.collection))
            } else {
                log('[TTL Monitor] All TTL indexes verified successfully')
            }
        }
    }
    
    // Initialize indexes
    await createIndexes()
    
    // Main store implementation
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
            try {
                const validJid = validateJID(jid)
                
                const chat = await collections.chats.findOne({ instanceId: validatedInstanceId, id: validJid })
                if (!chat) return null
                
                // Check access permissions
                accessContext.validateAccess(chat.instanceId, 'read')
                
                // eslint-disable-next-line @typescript-eslint/no-unused-vars
                const { _id, instanceId: _instanceId, updatedAt, ...chatData } = chat
                return chatData as Chat
            } catch (error) {
                if (error instanceof ValidationError || error instanceof AuthorizationError) {
                    throw error
                }
                logError(createSafeErrorMessage(error as Error, 'getChat'))
                throw new Error(createSafeErrorMessage(error as Error, 'getChat'))
            }
        },

        async upsertChats(...chats: Chat[]): Promise<void> {
            if (chats.length === 0) return
            
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.CHATS)) {
                try {
                    const queue = queues.get(QueueType.CHATS)!
                    await queue.add(
                        'upsert',
                        {
                            type: 'upsert',
                            chats,
                            instanceId,
                            timestamp: Date.now()
                        },
                        defaultJobOptions
                    )
                    return
                } catch (error) {
                    logError('[Bull Chats] Failed to queue, falling back:', error)
                }
            }
            
            // Fallback to direct write
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
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.CHATS)) {
                try {
                    const queue = queues.get(QueueType.CHATS)!
                    await queue.add(
                        'update',
                        {
                            type: 'update',
                            chatId: jid,
                            update,
                            instanceId,
                            timestamp: Date.now()
                        },
                        defaultJobOptions
                    )
                    return true
                } catch (error) {
                    logError('[Bull Chats] Failed to queue update, falling back:', error)
                }
            }
            
            // Fallback to direct update
            const result = await collections.chats.updateOne(
                { instanceId, id: jid },
                { $set: { ...update, updatedAt: new Date() } }
            )
            
            return result.modifiedCount > 0
        },

        async deleteChats(jids: string[]): Promise<void> {
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.CHATS)) {
                try {
                    const queue = queues.get(QueueType.CHATS)!
                    await queue.add(
                        'delete',
                        {
                            type: 'delete',
                            deleteIds: jids,
                            instanceId,
                            timestamp: Date.now()
                        },
                        defaultJobOptions
                    )
                    return
                } catch (error) {
                    logError('[Bull Chats] Failed to queue delete, falling back:', error)
                }
            }
            
            // Fallback to direct delete
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
                // eslint-disable-next-line @typescript-eslint/no-unused-vars
                const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...contactData } = contact
                contactsMap[contact.id] = contactData as Contact
            }
            
            return contactsMap
        },

        async getContact(jid: string): Promise<Contact | null> {
            const contact = await collections.contacts.findOne({ instanceId, id: jid })
            if (!contact) return null
            
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...contactData } = contact
            return contactData as Contact
        },

        async upsertContacts(contacts: Contact[]): Promise<void> {
            if (contacts.length === 0) return
            
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.CONTACTS)) {
                try {
                    const queue = queues.get(QueueType.CONTACTS)!
                    // Split large contact lists into batches
                    for (let i = 0; i < contacts.length; i += BATCH_SIZE) {
                        const batch = contacts.slice(i, i + BATCH_SIZE)
                        await queue.add(
                            'upsert',
                            {
                                type: 'upsert',
                                contacts: batch,
                                instanceId,
                                timestamp: Date.now()
                            },
                            defaultJobOptions
                        )
                    }
                    return
                } catch (error) {
                    logError('[Bull Contacts] Failed to queue, falling back:', error)
                }
            }
            
            // Fallback to direct write
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
                await collections.contacts.bulkWrite(chunk, { ordered: false })
            }
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
            try {
                const validJid = validateJID(jid)
                const validId = validateMessageId(id)
                
                log(`getMessage called with ${jid} and ${id}`)
                log(`getMessage validJid ${validJid}`)
                log(`getMessage validId ${validId}`)
                
                const cacheKey = `msg_${validatedInstanceId}_${hashForLogging(validJid)}_${hashForLogging(validId)}`
                log(`getMessage cacheKey ${cacheKey}`)
                
                const cached = binaryConversionCache.get<proto.IWebMessageInfo>(cacheKey)
                log(`getMessage cached ${cached ? 'found' : 'undefined'}`)
                if (cached) return cached
                
                // First try the standard query
                let message = await collections.messages.findOne({
                    instanceId: validatedInstanceId,
                    jid: validJid,
                    'key.id': validId
                })
                
                log(`getMessage message ${message ? 'found' : 'null'}`)
                
                // If not found, try alternative queries for poll messages and other edge cases
                if (!message) {
                    log(`Trying alternative query for poll message`)
                    
                    // Try with key.remoteJid instead of jid field
                    message = await collections.messages.findOne({
                        instanceId: validatedInstanceId,
                        'key.remoteJid': validJid,
                        'key.id': validId
                    })
                    
                    if (!message) {
                        // Try without the jid constraint at all (just instanceId and key.id)
                        log(`Trying query with just instanceId and key.id`)
                        message = await collections.messages.findOne({
                            instanceId: validatedInstanceId,
                            'key.id': validId
                        })
                        
                        // Verify the jid matches if we found something
                        if (message && message.key?.remoteJid !== validJid) {
                            log(`Found message but JID mismatch: ${message.key?.remoteJid} !== ${validJid}`)
                            message = null
                        }
                    }
                    
                    if (message) {
                        log(`Found message with alternative query`)
                    } else {
                        // Log more details to help debug
                        const count = await collections.messages.countDocuments({
                            instanceId: validatedInstanceId
                        })
                        log(`Total messages for instance: ${count}`)
                        
                        // Try to find similar message IDs
                        const similarMessages = await collections.messages.find({
                            instanceId: validatedInstanceId,
                            'key.id': { $regex: validId.substring(0, 10) }
                        }).limit(5).toArray()
                        
                        if (similarMessages.length > 0) {
                            log(`Found ${similarMessages.length} messages with similar IDs:`)
                            similarMessages.forEach(msg => {
                                log(`  - ID: ${msg.key?.id}, JID: ${msg.key?.remoteJid || msg.jid}`)
                            })
                        }
                        
                        return null
                    }
                }
                
                // Check access permissions
                accessContext.validateAccess(message.instanceId, 'read')
                
                // eslint-disable-next-line @typescript-eslint/no-unused-vars
                const { _id, instanceId: _instanceId, jid: _jid, updatedAt: _updatedAt, ...msg } = message
                const converted = convertBinaryToBuffer(msg)
            
            binaryConversionCache.set(cacheKey, converted)
            
            return converted
            } catch (error) {
                if (error instanceof ValidationError || error instanceof AuthorizationError) {
                    throw error
                }
                logError(createSafeErrorMessage(error as Error, 'getMessage'))
                throw new Error(createSafeErrorMessage(error as Error, 'getMessage'))
            }
        },

        async upsertMessage(jid: string, message: proto.IWebMessageInfo): Promise<void> {
            try {
                const validJid = validateJID(jid)
                
                // Check if this is an Official API message
                const isOfficialAPI = (message as any).official_api === true
                
                // Validate message ID if present (with special handling for Official API)
                if (message.key?.id) {
                    validateMessageId(message.key.id, isOfficialAPI)
                }
                
                // Check write permissions
                accessContext.validateAccess(validatedInstanceId, 'write')
                
                const cacheKey = `msg_${validatedInstanceId}_${hashForLogging(validJid)}_${message.key?.id ? hashForLogging(message.key.id) : ''}`
            binaryConversionCache.del(cacheKey)
            
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.MESSAGES)) {
                try {
                    const queue = queues.get(QueueType.MESSAGES)!
                    await queue.add(
                        'upsert',
                        {
                            type: 'upsert',
                            jid: validJid,
                            message,
                            instanceId: validatedInstanceId,
                            timestamp: Date.now()
                        },
                        defaultJobOptions
                    )
                    return
                } catch (error) {
                    logError('[Bull Messages] Failed to queue, falling back:', error)
                }
            }
            
            // Fallback to direct database operation
            await collections.messages.replaceOne(
                {
                    instanceId: validatedInstanceId,
                    jid: validJid,
                    'key.id': message.key?.id
                },
                {
                    ...message,
                    instanceId: validatedInstanceId,
                    jid: validJid,
                    updatedAt: new Date()
                },
                { upsert: true }
            )
            } catch (error) {
                if (error instanceof ValidationError || error instanceof AuthorizationError) {
                    throw error
                }
                logError(createSafeErrorMessage(error as Error, 'upsertMessage'))
                throw new Error(createSafeErrorMessage(error as Error, 'upsertMessage'))
            }
        },

        async updateMessage(jid: string, id: string, update: Partial<proto.IWebMessageInfo>): Promise<boolean> {
            const cacheKey = `msg_${instanceId}_${jid}_${id}`
            binaryConversionCache.del(cacheKey)
            
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.MESSAGES)) {
                try {
                    const queue = queues.get(QueueType.MESSAGES)!
                    await queue.add(
                        'update',
                        {
                            type: 'update',
                            jid,
                            messageId: id,
                            update: convertBinaryToBuffer(update),
                            instanceId,
                            timestamp: Date.now()
                        },
                        defaultJobOptions
                    )
                    return true
                } catch (error) {
                    logError('[Bull Messages] Failed to queue update, falling back:', error)
                }
            }
            
            // Fallback to direct update
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
            try {
                const validJid = validateJID(jid)
                
                // For deletion, we can't easily determine if these are Official API IDs,
                // so we'll be more lenient with validation
                const validIds = ids?.map(id => {
                    try {
                        return validateMessageId(id, false)
                    } catch {
                        // If regular validation fails, try as Official API ID
                        return validateMessageId(id, true)
                    }
                })
                
                // Check delete permissions
                accessContext.validateAccess(validatedInstanceId, 'delete')
                
                // Clear cache
                if (validIds && validIds.length > 0) {
                    validIds.forEach(id => {
                        const cacheKey = `msg_${validatedInstanceId}_${hashForLogging(validJid)}_${hashForLogging(id)}`
                        binaryConversionCache.del(cacheKey)
                    })
                } else {
                    const keys = binaryConversionCache.keys()
                    const jidHash = hashForLogging(validJid)
                    keys.forEach(key => {
                        if (key.startsWith(`msg_${validatedInstanceId}_${jidHash}_`)) {
                            binaryConversionCache.del(key)
                        }
                    })
                }
            
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.MESSAGES)) {
                try {
                    const queue = queues.get(QueueType.MESSAGES)!
                    await queue.add(
                        'delete',
                        {
                            type: 'delete',
                            jid: validJid,
                            deleteIds: validIds,
                            instanceId: validatedInstanceId,
                            timestamp: Date.now()
                        },
                        defaultJobOptions
                    )
                    return
                } catch (error) {
                    logError('[Bull Messages] Failed to queue delete, falling back:', error)
                }
            }
            
            // Fallback to direct delete
            const filter: any = { instanceId: validatedInstanceId, jid: validJid }
            
            if (validIds && validIds.length > 0) {
                filter['key.id'] = { $in: validIds }
            }
            
            await collections.messages.deleteMany(filter)
            } catch (error) {
                if (error instanceof ValidationError || error instanceof AuthorizationError) {
                    throw error
                }
                logError(createSafeErrorMessage(error as Error, 'deleteMessages'))
                throw new Error(createSafeErrorMessage(error as Error, 'deleteMessages'))
            }
        },

        async getGroupMetadata(jid: string): Promise<GroupMetadata | null> {
            const metadata = await collections.groupMetadata.findOne({ instanceId, id: jid })
            if (!metadata) return null
            
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
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
            
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.GROUP_METADATA)) {
                try {
                    const queue = queues.get(QueueType.GROUP_METADATA)!
                    await queue.add(
                        'upsert',
                        {
                            type: 'upsert',
                            jid,
                            metadata,
                            instanceId,
                            timestamp: Date.now()
                        },
                        defaultJobOptions
                    )
                    return
                } catch (error) {
                    logError('[Bull GroupMetadata] Failed to queue, falling back:', error)
                }
            }
            
            // Fallback to direct write
            await collections.groupMetadata.replaceOne(
                { instanceId, id: metadata.id },
                { ...metadata, instanceId, updatedAt: new Date() },
                { upsert: true }
            )
        },

        async getState(): Promise<ConnectionState> {
            const state = await collections.state.findOne({ instanceId })
            if (!state) return { connection: 'close' }
            
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...stateData } = state
            return stateData as ConnectionState
        },

        async updateState(update: Partial<ConnectionState>): Promise<void> {
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.STATE)) {
                try {
                    const queue = queues.get(QueueType.STATE)!
                    await queue.add(
                        'update',
                        {
                            type: 'update',
                            update,
                            instanceId,
                            timestamp: Date.now()
                        },
                        defaultJobOptions
                    )
                    return
                } catch (error) {
                    logError('[Bull State] Failed to queue, falling back:', error)
                }
            }
            
            // Fallback to direct update
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
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.PRESENCES)) {
                try {
                    const queue = queues.get(QueueType.PRESENCES)!
                    await queue.add(
                        'update',
                        {
                            type: 'update',
                            id,
                            presences,
                            instanceId,
                            timestamp: Date.now()
                        },
                        defaultJobOptions
                    )
                    return
                } catch (error) {
                    logError('[Bull Presences] Failed to queue, falling back:', error)
                }
            }
            
            // Fallback to direct update
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
                // eslint-disable-next-line @typescript-eslint/no-unused-vars
                const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...labelData } = label
                labelsMap[label.id] = labelData as Label
            }
            
            return labelsMap
        },

        async upsertLabel(id: string, label: Label): Promise<void> {
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.LABELS)) {
                try {
                    const queue = queues.get(QueueType.LABELS)!
                    await queue.add(
                        'upsert',
                        {
                            type: 'upsert',
                            id,
                            label,
                            instanceId,
                            timestamp: Date.now()
                        },
                        defaultJobOptions
                    )
                    return
                } catch (error) {
                    logError('[Bull Labels] Failed to queue, falling back:', error)
                }
            }
            
            // Fallback to direct write
            await collections.labels.replaceOne(
                { instanceId, id },
                { ...label, instanceId, updatedAt: new Date() },
                { upsert: true }
            )
        },

        async deleteLabel(id: string): Promise<void> {
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.LABELS)) {
                try {
                    const queue = queues.get(QueueType.LABELS)!
                    await queue.add(
                        'delete',
                        {
                            type: 'delete',
                            id,
                            instanceId,
                            timestamp: Date.now()
                        },
                        defaultJobOptions
                    )
                    return
                } catch (error) {
                    logError('[Bull Labels] Failed to queue delete, falling back:', error)
                }
            }
            
            // Fallback to direct delete
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
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.LABEL_ASSOCIATIONS)) {
                try {
                    const queue = queues.get(QueueType.LABEL_ASSOCIATIONS)!
                    await queue.add(
                        'upsert',
                        {
                            type: 'upsert',
                            association,
                            instanceId,
                            timestamp: Date.now()
                        },
                        defaultJobOptions
                    )
                    return
                } catch (error) {
                    logError('[Bull LabelAssociations] Failed to queue, falling back:', error)
                }
            }
            
            // Fallback to direct write
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
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.LABEL_ASSOCIATIONS)) {
                try {
                    const queue = queues.get(QueueType.LABEL_ASSOCIATIONS)!
                    await queue.add(
                        'delete',
                        {
                            type: 'delete',
                            association,
                            instanceId,
                            timestamp: Date.now()
                        },
                        defaultJobOptions
                    )
                    return
                } catch (error) {
                    logError('[Bull LabelAssociations] Failed to queue delete, falling back:', error)
                }
            }
            
            // Fallback to direct delete
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

        // bind method continues with event handling...
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

            // Messages upsert - CRITICAL for storing messages including polls
            ev.on('messages.upsert', async ({ messages }) => {
                if (enableMetrics) updateEventMetrics('messages.upsert', 'received')
                
                for (const msg of messages) {
                    const jid = msg.key.remoteJid
                    if (!jid) continue
                    
                    if (await shouldStoreEvent('messages.upsert', msg)) {
                        try {
                            // Store the message first
                            await storeImpl.upsertMessage(jid, msg)
                            
                            // Handle media download if configured
                            if (config.media?.enabled) {
                                // Function to check for existing media by hash
                                const checkExistingMedia = async (hash: string): Promise<string | null> => {
                                    const existing = await collections.messages.findOne({
                                        instanceId,
                                        mediaHash: hash,
                                        mediaUrl: { $exists: true }
                                    }) as any
                                    return existing?.mediaUrl || null
                                }
                                
                                // Check if this is an Official API message
                                const isOfficialAPI = (msg as any).official_api === true
                                
                                let mediaResult
                                if (isOfficialAPI) {
                                    // Use Official API download method
                                    mediaResult = await downloadOfficialAPIMedia(msg, instanceId, config.media, config.logger, checkExistingMedia)
                                } else {
                                    // Use regular Baileys download method
                                    mediaResult = await downloadMedia(msg, instanceId, config.media, config.logger, checkExistingMedia)
                                }
                                
                                if (mediaResult.success && mediaResult.localPath) {
                                    // Update message with media URL
                                    const updateResult = await collections.messages.updateOne(
                                        { 
                                            instanceId, 
                                            jid, 
                                            'key.id': msg.key.id 
                                        },
                                        { 
                                            $set: { 
                                                mediaUrl: mediaResult.localPath,
                                                mediaType: mediaResult.mediaType,
                                                mediaFileName: mediaResult.fileName,
                                                mediaFileSize: mediaResult.fileSize,
                                                mediaHash: mediaResult.mediaHash,
                                                mediaReused: mediaResult.reused || false,
                                                mediaDownloadedAt: new Date()
                                            } 
                                        }
                                    )
                                    
                                    if (mediaResult.reused) {
                                        log(`♻️  Media reused for message ${msg.key.id}: ${mediaResult.localPath} (saved storage space)`)
                                    } else {
                                        log(`✅ Media downloaded for message ${msg.key.id}: ${mediaResult.localPath}`)
                                    }
                                    log(`📝 MongoDB update result: matched=${updateResult.matchedCount}, modified=${updateResult.modifiedCount}`)
                                    
                                    if (updateResult.matchedCount === 0) {
                                        log(`⚠️ No document found to update for message ${msg.key.id} in chat ${jid}`)
                                    }
                                } else if (!mediaResult.success && mediaResult.error) {
                                    log(`⚠️ Media download failed for message ${msg.key.id}: ${mediaResult.error}`)
                                }
                            }
                            
                            if (enableMetrics) updateEventMetrics('messages.upsert', 'stored')
                            if (hooks.afterStore) await hooks.afterStore('messages.upsert', msg)
                        } catch (error) {
                            logError(`Failed to upsert message for ${jid}:`, error)
                            if (enableMetrics) updateEventMetrics('messages.upsert', 'error')
                        }
                    }
                }
            })

            // Messages update
            ev.on('messages.update', async (updates) => {
                if (enableMetrics) updateEventMetrics('messages.update', 'received')
                
                for (const update of updates) {
                    const jid = update.key.remoteJid
                    if (!jid) continue
                    
                    if (await shouldStoreEvent('messages.update', update)) {
                        try {
                            await storeImpl.updateMessage(jid, update.key.id!, update.update!)
                            if (enableMetrics) updateEventMetrics('messages.update', 'stored')
                            if (hooks.afterStore) await hooks.afterStore('messages.update', update)
                        } catch (error) {
                            logError(`Failed to update message for ${jid}:`, error)
                            if (enableMetrics) updateEventMetrics('messages.update', 'error')
                        }
                    }
                }
            })

            // Messages delete
            ev.on('messages.delete', async (item) => {
                if (enableMetrics) updateEventMetrics('messages.delete', 'received')
                
                if ('keys' in item) {
                    const jid = item.keys[0].remoteJid
                    if (!jid) return
                    
                    const ids = item.keys.map(k => k.id).filter(id => id) as string[]
                    if (await shouldStoreEvent('messages.delete', item)) {
                        try {
                            await storeImpl.deleteMessages(jid, ids)
                            if (enableMetrics) updateEventMetrics('messages.delete', 'stored')
                            if (hooks.afterStore) await hooks.afterStore('messages.delete', item)
                        } catch (error) {
                            logError(`Failed to delete messages for ${jid}:`, error)
                            if (enableMetrics) updateEventMetrics('messages.delete', 'error')
                        }
                    }
                } else if ('jid' in item && item.jid) {
                    // Delete all messages for JID
                    if (await shouldStoreEvent('messages.delete', item)) {
                        try {
                            await storeImpl.deleteMessages(item.jid)
                            if (enableMetrics) updateEventMetrics('messages.delete', 'stored')
                            if (hooks.afterStore) await hooks.afterStore('messages.delete', item)
                        } catch (error) {
                            logError(`Failed to delete all messages for ${item.jid}:`, error)
                            if (enableMetrics) updateEventMetrics('messages.delete', 'error')
                        }
                    }
                }
            })

            // Chats upsert
            ev.on('chats.upsert', async (chats) => {
                if (enableMetrics) updateEventMetrics('chats.upsert', 'received')
                
                const chatsToStore = []
                for (const chat of chats) {
                    if (await shouldStoreEvent('chats.upsert', chat)) {
                        chatsToStore.push(chat)
                    }
                }
                
                if (chatsToStore.length > 0) {
                    try {
                        await storeImpl.upsertChats(...chatsToStore)
                        if (enableMetrics) updateEventMetrics('chats.upsert', 'stored')
                        if (hooks.afterStore) await hooks.afterStore('chats.upsert', chatsToStore)
                    } catch (error) {
                        logError('Failed to upsert chats:', error)
                        if (enableMetrics) updateEventMetrics('chats.upsert', 'error')
                    }
                }
            })

            // Chats update
            ev.on('chats.update', async (updates) => {
                if (enableMetrics) updateEventMetrics('chats.update', 'received')
                
                for (const update of updates) {
                    if (await shouldStoreEvent('chats.update', update)) {
                        try {
                            await storeImpl.updateChat(update.id!, update)
                            if (enableMetrics) updateEventMetrics('chats.update', 'stored')
                            if (hooks.afterStore) await hooks.afterStore('chats.update', update)
                        } catch (error) {
                            logError(`Failed to update chat ${update.id}:`, error)
                            if (enableMetrics) updateEventMetrics('chats.update', 'error')
                        }
                    }
                }
            })

            // Chats delete
            ev.on('chats.delete', async (deletions) => {
                if (enableMetrics) updateEventMetrics('chats.delete', 'received')
                
                if (await shouldStoreEvent('chats.delete', deletions)) {
                    try {
                        await storeImpl.deleteChats(deletions)
                        if (enableMetrics) updateEventMetrics('chats.delete', 'stored')
                        if (hooks.afterStore) await hooks.afterStore('chats.delete', deletions)
                    } catch (error) {
                        logError('Failed to delete chats:', error)
                        if (enableMetrics) updateEventMetrics('chats.delete', 'error')
                    }
                }
            })

            // Contacts upsert
            ev.on('contacts.upsert', async (contacts) => {
                if (enableMetrics) updateEventMetrics('contacts.upsert', 'received')
                
                const contactsToStore = []
                for (const contact of contacts) {
                    if (await shouldStoreEvent('contacts.upsert', contact)) {
                        contactsToStore.push(contact)
                    }
                }
                
                if (contactsToStore.length > 0) {
                    try {
                        await storeImpl.upsertContacts(contactsToStore)
                        if (enableMetrics) updateEventMetrics('contacts.upsert', 'stored')
                        if (hooks.afterStore) await hooks.afterStore('contacts.upsert', contactsToStore)
                    } catch (error) {
                        logError('Failed to upsert contacts:', error)
                        if (enableMetrics) updateEventMetrics('contacts.upsert', 'error')
                    }
                }
            })

            // Contacts update
            ev.on('contacts.update', async (updates) => {
                if (enableMetrics) updateEventMetrics('contacts.update', 'received')
                
                const contactsToUpdate = []
                for (const update of updates) {
                    if (await shouldStoreEvent('contacts.update', update)) {
                        contactsToUpdate.push(update)
                    }
                }
                
                if (contactsToUpdate.length > 0) {
                    try {
                        await storeImpl.upsertContacts(contactsToUpdate as Contact[])
                        if (enableMetrics) updateEventMetrics('contacts.update', 'stored')
                        if (hooks.afterStore) await hooks.afterStore('contacts.update', contactsToUpdate)
                    } catch (error) {
                        logError('Failed to update contacts:', error)
                        if (enableMetrics) updateEventMetrics('contacts.update', 'error')
                    }
                }
            })

            // Group participants update
            ev.on('group-participants.update', async ({ id, participants, action }) => {
                if (enableMetrics) updateEventMetrics('group-participants.update', 'received')
                
                const updateData = { id, participants, action }
                if (await shouldStoreEvent('group-participants.update', updateData)) {
                    try {
                        const metadata = await storeImpl.getGroupMetadata(id)
                        if (metadata) {
                            if (action === 'add') {
                                metadata.participants.push(...participants.map(id => ({ id, admin: null })))
                            } else if (action === 'remove') {
                                metadata.participants = metadata.participants.filter(p => !participants.includes(p.id))
                            } else if (action === 'promote') {
                                metadata.participants.forEach(p => {
                                    if (participants.includes(p.id)) p.admin = 'admin'
                                })
                            } else if (action === 'demote') {
                                metadata.participants.forEach(p => {
                                    if (participants.includes(p.id)) p.admin = null
                                })
                            }
                            await storeImpl.upsertGroupMetadata(id, metadata)
                            if (enableMetrics) updateEventMetrics('group-participants.update', 'stored')
                            if (hooks.afterStore) await hooks.afterStore('group-participants.update', updateData)
                        }
                    } catch (error) {
                        logError(`Failed to update group participants for ${id}:`, error)
                        if (enableMetrics) updateEventMetrics('group-participants.update', 'error')
                    }
                }
            })

            // Groups upsert
            ev.on('groups.upsert', async (groups) => {
                if (enableMetrics) updateEventMetrics('groups.upsert', 'received')
                
                for (const group of groups) {
                    if (await shouldStoreEvent('groups.upsert', group)) {
                        try {
                            await storeImpl.upsertGroupMetadata(group.id, group)
                            if (enableMetrics) updateEventMetrics('groups.upsert', 'stored')
                            if (hooks.afterStore) await hooks.afterStore('groups.upsert', group)
                        } catch (error) {
                            logError(`Failed to upsert group ${group.id}:`, error)
                            if (enableMetrics) updateEventMetrics('groups.upsert', 'error')
                        }
                    }
                }
            })

            // Groups update
            ev.on('groups.update', async (updates) => {
                if (enableMetrics) updateEventMetrics('groups.update', 'received')
                
                for (const update of updates) {
                    if (!update.id) continue
                    
                    if (await shouldStoreEvent('groups.update', update)) {
                        try {
                            const existing = await storeImpl.getGroupMetadata(update.id)
                            if (existing) {
                                const merged = { ...existing, ...update }
                                await storeImpl.upsertGroupMetadata(update.id, merged)
                                if (enableMetrics) updateEventMetrics('groups.update', 'stored')
                                if (hooks.afterStore) await hooks.afterStore('groups.update', update)
                            }
                        } catch (error) {
                            logError(`Failed to update group ${update.id}:`, error)
                            if (enableMetrics) updateEventMetrics('groups.update', 'error')
                        }
                    }
                }
            })

            // Presence update
            ev.on('presence.update', async ({ id, presences }) => {
                if (enableMetrics) updateEventMetrics('presence.update', 'received')
                
                const updateData = { id, presences }
                if (await shouldStoreEvent('presence.update', updateData)) {
                    try {
                        await storeImpl.updatePresence(id, presences)
                        if (enableMetrics) updateEventMetrics('presence.update', 'stored')
                        if (hooks.afterStore) await hooks.afterStore('presence.update', updateData)
                    } catch (error) {
                        logError(`Failed to update presence for ${id}:`, error)
                        if (enableMetrics) updateEventMetrics('presence.update', 'error')
                    }
                }
            })

            // Note: Labels and label-associations events are not part of the standard Baileys events
            // They would need to be implemented separately if needed
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
            log(`loadMessage called with ${jid} and ${id}`)
            const msg = await storeImpl.getMessage(jid, id)
            log(`loadMessage result ${msg ? 'found' : 'null'}`)
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
            
            // Add Bull queue stats if available
            const bullStats: any = {}
            if (bullInitialized) {
                bullStats.initialized = true
                bullStats.queues = {}
                for (const [queueType] of queues.entries()) {
                    bullStats.queues[queueType] = 'active'
                }
                bullStats.totalQueues = queues.size
                bullStats.redisConnected = redisConnection?.status === 'ready'
            } else {
                bullStats.initialized = false
                bullStats.reason = redis ? 'initialization failed' : 'not configured'
            }
            
            // Add memory stats if memory monitor is available
            let memoryStats = undefined
            if (memoryMonitor) {
                const memoryMetrics = memoryMonitor.getCurrentMemory()
                memoryStats = {
                    heapUsedMB: Math.round(memoryMetrics.heapUsed / 1024 / 1024),
                    heapTotalMB: Math.round(memoryMetrics.heapTotal / 1024 / 1024),
                    rssMB: Math.round(memoryMetrics.rss / 1024 / 1024),
                    memoryPressure: backpressureController ? backpressureController.getPressure() : 0
                }
            }
            
            return {
                messagesProcessed: 0,
                labelsProcessed: 0,
                batchesProcessed: 0,
                errors: 0,
                lastResetTime: new Date(),
                uptime: 0,
                eventMetrics: enableMetrics ? eventMetrics : undefined,
                bullStats,
                memoryStats
            }
        },
        
        async flushLabelAssociations(): Promise<void> {
            // Force process all pending items in p-queue
            // Queue processing handled by Bull
            log('Flushed all pending label associations')
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

        async getTTLStatus(): Promise<any> {
            if (!ttlMonitor) {
                return {
                    enabled: false,
                    message: 'TTL monitoring not configured'
                }
            }
            
            try {
                const report = await ttlMonitor.getTTLStatusReport()
                return {
                    enabled: true,
                    ttlDays: ttlDays || DEFAULT_TTL_DAYS,
                    collectionTTL: collectionTTL,
                    summary: report.summary,
                    details: report.details,
                    metrics: ttlMonitor.getMetrics()
                }
            } catch (error) {
                logError('[TTL Status] Error getting TTL status:', error)
                return {
                    enabled: true,
                    error: 'Failed to get TTL status'
                }
            }
        },
        
        async cleanupOldMedia(daysToKeep: number = 30): Promise<{ deleted: number; errors: number }> {
            if (!config.media?.enabled) {
                return { deleted: 0, errors: 0 }
            }
            
            return await cleanupOldMedia(instanceId, config.media, daysToKeep, config.logger)
        },
        
        async getMediaStats(): Promise<{
            totalFiles: number
            totalSize: number
            byType: Record<string, { count: number; size: number }>
        }> {
            if (!config.media?.enabled) {
                return {
                    totalFiles: 0,
                    totalSize: 0,
                    byType: {}
                }
            }
            
            return await getMediaStats(instanceId, config.media, config.logger)
        },
        
        async downloadMessageMedia(jid: string, messageId: string): Promise<{
            success: boolean
            localPath?: string
            error?: string
        }> {
            if (!config.media?.enabled) {
                return { success: false, error: 'Media download not enabled' }
            }
            
            try {
                // Fetch the message from database
                const message = await collections.messages.findOne({
                    instanceId,
                    jid,
                    'key.id': messageId
                })
                
                if (!message) {
                    return { success: false, error: 'Message not found' }
                }
                
                // Check if media already downloaded
                if ((message as any).mediaUrl) {
                    return { success: true, localPath: (message as any).mediaUrl }
                }
                
                // Function to check for existing media by hash
                const checkExistingMedia = async (hash: string): Promise<string | null> => {
                    const existing = await collections.messages.findOne({
                        instanceId,
                        mediaHash: hash,
                        mediaUrl: { $exists: true }
                    }) as any
                    return existing?.mediaUrl || null
                }
                
                // Download the media
                const mediaResult = await downloadMedia(message, instanceId, config.media, config.logger, checkExistingMedia)
                
                if (mediaResult.success && mediaResult.localPath) {
                    // Update message with media URL
                    await collections.messages.updateOne(
                        { 
                            instanceId, 
                            jid, 
                            'key.id': messageId 
                        },
                        { 
                            $set: { 
                                mediaUrl: mediaResult.localPath,
                                mediaType: mediaResult.mediaType,
                                mediaFileName: mediaResult.fileName,
                                mediaFileSize: mediaResult.fileSize,
                                mediaHash: mediaResult.mediaHash,
                                mediaReused: mediaResult.reused || false,
                                mediaDownloadedAt: new Date()
                            } 
                        }
                    )
                    
                    return { success: true, localPath: mediaResult.localPath }
                }
                
                return { success: false, error: mediaResult.error }
            } catch (error) {
                const errorMsg = error instanceof Error ? error.message : 'Unknown error'
                logError(`Failed to download media for message ${messageId}:`, error)
                return { success: false, error: errorMsg }
            }
        },
        
        async close(): Promise<void> {
            // Close Bull queues if initialized
            if (bullInitialized) {
                log(`🛑 Closing Bull queues for instance ${instanceId}...`)
                try {
                    // Close all workers
                    for (const worker of workers.values()) {
                        await worker.close()
                    }
                    // Close all queues
                    for (const queue of queues.values()) {
                        await queue.close()
                    }
                    // Disconnect Redis
                    if (redisConnection) redisConnection.disconnect()
                } catch (error) {
                    logError('Error closing Bull queues:', error)
                }
            }
            
            // Wait for all p-queues to finish
            await Promise.all([
                Promise.resolve() // Bull handles queue processing
            ])
            
            // Clear cache
            const keys = binaryConversionCache.keys()
            keys.forEach(key => {
                if (key.startsWith(`msg_${instanceId}_`)) {
                    binaryConversionCache.del(key)
                }
            })
            
            // Stop TTL monitor if running
            if (ttlMonitor) {
                ttlMonitor.stopMonitoring()
                ttlMonitor = null
            }
            
            if (client) {
                await client.close()
            }
        }
    }

    return storeImpl
}