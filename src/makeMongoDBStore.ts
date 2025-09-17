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
import { MemoryMonitor, BackpressureController, MemoryAwareBatchProcessor, calculateOptimalBatchSize } from './utils/memory'
import { ConnectionManager, getConnectionManager } from './utils/connectionManager'
// @ts-ignore - Type is used in annotations
import type { ConnectionConfig } from './types/connection'
import { TTLMonitor } from './utils/ttl'
import { LidHandler } from './utils/lidHandler'
import { retryWithBackoff, isRetryableError, RetryOptions } from './utils/connectionRetry'
import { shouldCreateIndexes, IndexSpec, clearCollectionCache } from './utils/collectionHelper'
import { batchCreateIndexes, recreateIndexes } from './utils/indexHelper'

const DEFAULT_TTL_DAYS = 30

interface ActiveConnection {
    client: MongoClient
    database: string
    instanceId: string
    collectionPrefix: string
    uri?: string
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
    totalReceived?: number  // Track total items received
    totalProcessed?: number // Track total items processed
    pendingPromises?: Array<{ resolve: () => void; reject: (error: any) => void }> // Track pending promises
}

// Bull job data types
interface LabelAssociationJob {
    type: 'upsert' | 'delete'
    association: LabelAssociation
    instanceId: string
    timestamp: number
    operationId?: string
}

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
        
        // Handle MongoDB Binary objects
        if (obj._bsontype === 'Binary') {
            // For Binary objects, use the buffer property directly
            if (obj.buffer instanceof Buffer) {
                return Buffer.from(obj.buffer)
            } else if (obj.buffer instanceof ArrayBuffer) {
                return Buffer.from(obj.buffer)
            } else if (obj.buffer instanceof Uint8Array) {
                return Buffer.from(obj.buffer)
            } else if (obj.buffer) {
                // Fallback for other buffer types
                return Buffer.from(obj.buffer)
            }
        }
        
        // Handle direct Buffer-like objects that might have been serialized
        if (obj.type === 'Buffer' && Array.isArray(obj.data)) {
            return Buffer.from(obj.data)
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
        console.error('Binary conversion error:', error)
        return obj
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
        collectionPrefix = 'baileys_',
        redis,
        logLevel = 'none',
        auth,
        memory,
        ttlMonitoring,
        lidHandler: lidHandlerConfig,
        connectionConfig,
        useSharedConnections = true,
        indexManagement
    } = config
    
    // Configure smart index management with defaults
    const indexConfig = {
        skipExistingCollectionIndexes: indexManagement?.skipExistingCollectionIndexes ?? true,
        forceRecreateIndexes: indexManagement?.forceRecreateIndexes ?? false,
        enableIndexHealthLogging: indexManagement?.enableIndexHealthLogging ?? true,
        indexCreationTimeout: indexManagement?.indexCreationTimeout ?? 30000
    }
    
    // Validate instance ID
    const validatedInstanceId = validateInstanceId(instanceId)
    
    // Create access context for this instance
    const accessContext = new InstanceAccessContext(
        validatedInstanceId,
        auth?.enableApiKey ? DEFAULT_PERMISSIONS : ['read:all', 'write:all', 'delete:all'],
        auth
    )
    
    // Helper functions for conditional logging based on log level
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
    
    // LID handler will be initialized after DB connection
    let lidHandler: LidHandler | null = null

    let client: MongoClient
    let db: Db
    let isConnected = false
    let isConnecting = false
    let connectionError: Error | null = null
    let reconnectAttempts = 0
    const MAX_RECONNECT_ATTEMPTS = 5
    const RECONNECT_DELAY_BASE = 1000 // 1 second base delay
    let connectionManager: ConnectionManager | null = null
    let isUsingSharedConnection = false
    
    // Bull queue instances - one per event type
    const queues: Map<QueueType, Queue<any>> = new Map()
    const workers: Map<QueueType, Worker<any>> = new Map()
    let redisConnection: Redis | null = null
    let bullInitialized = false
    
    // Default job options for automatic cleanup - remove immediately
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
    
    // Health check variables
    let healthCheckInterval: NodeJS.Timeout | null = null
    let lastHealthCheck = Date.now()
    const HEALTH_CHECK_INTERVAL = 30000 // Check every 30 seconds
    const QUEUE_STALE_THRESHOLD = 60000 // Consider stale after 60 seconds
    
    // Initialize connection
    const initializeConnection = async (): Promise<void> => {
        try {
            // Check if we should use shared connections
            if (useSharedConnections) {
                // Use ConnectionManager for shared connections
                connectionManager = getConnectionManager({
                    logLevel: logLevel as any,
                    enableMetrics: true
                })
                
                const connection = await connectionManager.registerInstance({
                    instanceId: validatedInstanceId,
                    uri,
                    database: dbName,
                    config: connectionConfig
                })
                
                client = connection.client
                db = connection.db
                isUsingSharedConnection = true
                
                // Track activity on operations
                log(`Using shared connection for instance ${instanceId}`)
            } else {
                // Use dedicated connection (original behavior)
                client = new MongoClient(uri, {
                    // Optimize connection pool for high concurrency
                    maxPoolSize: connectionConfig?.maxPoolSize ?? 100,
                    minPoolSize: connectionConfig?.minPoolSize ?? 10,
                    maxIdleTimeMS: 30000,
                    // Write concern for better performance
                    writeConcern: { w: 1, j: false }
                })
                await client.connect()
                db = client.db(dbName)
                
                // Update active connections for dedicated connections
                activeConnections = activeConnections.filter(c => c.instanceId !== instanceId)
                activeConnections.push({
                    client,
                    database: dbName,
                    instanceId,
                    collectionPrefix
                })
            }
            
            isConnected = true
            isConnecting = false
            connectionError = null
            reconnectAttempts = 0
            
            // Initialize TTL monitor after DB connection (do not start yet)
            if (ttlMonitoring && !ttlMonitor) {
                const ttlManagedCollections = ['chats', 'messages', 'presences']
                ttlMonitor = new TTLMonitor(db, { 
                    days: ttlDays, 
                    ...ttlMonitoring,
                    collectionPrefix,
                    collectionsToCheck: ttlManagedCollections,
                    // Silence non-critical TTL warnings unless verbose logging
                    onlyCriticalAlerts: logLevel !== 'all'
                })
            }
            
            // Initialize LID handler after DB connection
            if (lidHandlerConfig && !lidHandler) {
                lidHandler = new LidHandler(validatedInstanceId, lidHandlerConfig)
                await lidHandler.initialize(db, collectionPrefix)
                log(`[LID Handler] Initialized for instance ${validatedInstanceId}`)
            }
            
            log(`MongoDB connected successfully for instance ${instanceId}`)
        } catch (error) {
            isConnected = false
            isConnecting = false
            connectionError = error as Error
            logError(`MongoDB connection failed for instance ${instanceId}:`, error)
            throw error
        }
    }
    
    // Ensure connection is active before operations
    const ensureConnection = async (): Promise<void> => {
        if (isConnected && client) {
            try {
                // Quick ping to check if connection is actually alive
                await client.db(dbName).command({ ping: 1 })
                return
            } catch {
                isConnected = false
            }
        }
        
        if (isConnecting) {
            // Wait for ongoing connection attempt
            let waitAttempts = 0
            while (isConnecting && waitAttempts < 50) {
                await new Promise(resolve => setTimeout(resolve, 100))
                waitAttempts++
            }
            if (isConnected) return
        }
        
        if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
            throw new Error(`Failed to reconnect after ${MAX_RECONNECT_ATTEMPTS} attempts: ${connectionError?.message}`)
        }
        
        // Attempt reconnection with exponential backoff
        isConnecting = true
        const delay = RECONNECT_DELAY_BASE * Math.pow(2, Math.min(reconnectAttempts, 5))
        
        if (reconnectAttempts > 0) {
            log(`Attempting to reconnect (attempt ${reconnectAttempts + 1}/${MAX_RECONNECT_ATTEMPTS}) after ${delay}ms...`)
            await new Promise(resolve => setTimeout(resolve, delay))
        }
        
        reconnectAttempts++
        
        try {
            await initializeConnection()
        } catch (error) {
            isConnecting = false
            throw error
        }
    }
    
    // Initial connection
    await initializeConnection()
    
    // Create queues for different operation types
    const pMessageQueue = new PQueue({ concurrency: QUEUE_CONCURRENCY })
    const pLabelQueue = new PQueue({ concurrency: QUEUE_CONCURRENCY })
    const generalQueue = new PQueue({ concurrency: QUEUE_CONCURRENCY })
    
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
                const config = await redisConnection.config('GET', 'maxmemory-policy') as [string, string]
                const policy = config[1]
                if (policy && policy !== 'noeviction') {
                    logWarn(`⚠️  Redis eviction policy is '${policy}'. Consider using 'noeviction' for BullMQ or a separate Redis instance.`)
                    logWarn(`   Current settings will work but jobs may be lost if Redis memory fills up.`)
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
                // Label associations and CONTACTS MUST have concurrency of 1 to maintain order and avoid races
                const concurrency = (queueType === QueueType.LABEL_ASSOCIATIONS || queueType === QueueType.CONTACTS)
                    ? 1
                    : (redis.concurrency || 50)
                
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
                
                // Increase max listeners to prevent warnings
                if (typeof (worker as any).setMaxListeners === 'function') {
                    (worker as any).setMaxListeners(20)
                }
                
                log(`🔧 Created ${queueType} queue with concurrency: ${concurrency}`)
                
                // Store event handler functions for cleanup
                const completedHandler = (job: Job<T>) => {
                    // Only log for label associations in debug mode
                    if (queueType !== QueueType.LABEL_ASSOCIATIONS) {
                        log(`✅ ${queueType} job ${job.id} completed`)
                    }
                }
                
                const failedHandler = (job: Job<T> | undefined, err: Error) => {
                    logError(`❌ ${queueType} job ${job?.id} failed:`, err.message)
                    performanceMetrics.errors++
                }
                
                const stalledHandler = (jobId: string) => {
                    logWarn(`⚠️ ${queueType} job ${jobId} stalled`)
                }
                
                // Set up event handlers
                ;(worker as any).on('completed', completedHandler)
                ;(worker as any).on('failed', failedHandler)
                ;(worker as any).on('stalled', stalledHandler)
                
                // Store handlers for cleanup
                (worker as any).__eventHandlers = {
                    completed: completedHandler,
                    failed: failedHandler,
                    stalled: stalledHandler
                }
                
                workers.set(queueType, worker)
                
                // Clean up old jobs on startup
                queue.obliterate({ force: true }).catch(() => {})
                
                // Queue and worker created successfully
            }
            
            // Create queue for LABEL ASSOCIATIONS
            createQueueAndWorker<LabelAssociationJob>(QueueType.LABEL_ASSOCIATIONS, async (job) => {
                const { type, association, timestamp, operationId } = job.data
                const jobId = job.id
                const messageId = 'messageId' in association ? association.messageId : undefined
                const associationId = `${association.labelId}-${association.chatId}${messageId ? `-${messageId}` : ''}`
                
                log(`[Label Queue] Processing ${type} job ${jobId} (op: ${operationId}) for ${associationId}`)
                
                try {
                    if (type === 'upsert') {
                        // Include type field to properly differentiate label associations
                        const filter: any = {
                            instanceId,
                            type: association.type, // Include type field (label_jid or label_message)
                            chatId: association.chatId,
                            labelId: association.labelId
                        }
                        
                        // Only include messageId in filter if it's a message label
                        if (association.type === 'label_message' && 'messageId' in association && association.messageId) {
                            filter.messageId = association.messageId
                        }
                        
                        // Debug logging to understand what's being stored
                        log(`[Label Queue] Processing ${type} - Type: ${association.type}, ChatId: ${association.chatId}, LabelId: ${association.labelId}`)
                        log(`[Label Queue] Filter: ${JSON.stringify(filter)}`)
                        log(`[Label Queue] Document: ${JSON.stringify({...association, instanceId, updatedAt: new Date()})}`)
                        
                        const result = await collections.labelAssociations.replaceOne(
                            filter,
                            {
                                ...association,
                                instanceId,
                                updatedAt: new Date()
                            },
                            { upsert: true }
                        )
                        
                        // Validate operation succeeded
                        if (result.upsertedCount === 0 && result.modifiedCount === 0) {
                            throw new Error(`[Label Queue] Failed to upsert label association for ${associationId} - no documents affected`)
                        }
                        
                        log(`[Label Queue] ✅ Completed upsert for ${associationId} - upserted: ${result.upsertedCount}, modified: ${result.modifiedCount}`)
                        
                        // Show what was actually stored
                        if (result.upsertedCount > 0) {
                            log(`[Label Queue] New document created with ID: ${result.upsertedId}`)
                        }
                        
                        // Track the add operation for automation AFTER successful DB operation
                        await trackLabelOperation('add', association)
                    } else if (type === 'delete') {
                        // Include type field for proper matching
                        const filter: any = {
                            instanceId,
                            type: association.type, // Include type field
                            chatId: association.chatId,
                            labelId: association.labelId
                        }
                        
                        // Only add messageId to filter if it's a message label
                        if (association.type === 'label_message' && 'messageId' in association && association.messageId) {
                            filter.messageId = association.messageId
                        }
                        
                        // Debug logging
                        log(`[Label Queue] Processing delete - Type: ${association.type}, ChatId: ${association.chatId}, LabelId: ${association.labelId}`)
                        log(`[Label Queue] Delete filter: ${JSON.stringify(filter)}`)
                        
                        const result = await collections.labelAssociations.deleteOne(filter)
                        
                        if (result.deletedCount === 0) {
                            logWarn(`[Label Queue] ⚠️ No document found to delete for ${associationId}`)
                            // For delete operations, warn but don't fail since the association might already be deleted
                        } else {
                            log(`[Label Queue] ✅ Deleted association for ${associationId}`)
                        }
                        
                        // Track the remove operation for automation AFTER successful DB operation
                        await trackLabelOperation('remove', association)
                    }
                    
                    performanceMetrics.labelsProcessed++
                    const processingTime = Date.now() - (timestamp || 0)
                    log(`[Label Queue] Job ${jobId} completed in ${processingTime}ms`)
                    
                    return { success: true, processingTime, timestamp: Date.now() }
                } catch (error) {
                    logError(`[Label Queue] ❌ Failed ${type} job ${jobId} for ${associationId}:`, error)
                    throw error
                }
            })
            
            // Create queue for LABELS
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
            
            // Create queue for MESSAGES
            createQueueAndWorker<MessageJob>(QueueType.MESSAGES, async (job) => {
                const { type, jid, message, messageId, update, deleteIds } = job.data
                
                if (type === 'upsert' && message) {
                    // Skip protocol messages that shouldn't be stored as regular messages
                    if (message.message?.protocolMessage) {
                        const protoType = message.message.protocolMessage.type
                        
                        // Handle REVOKE messages - update the revoked message instead of storing the revoke message
                        if (protoType === proto.Message.ProtocolMessage.Type.REVOKE && message.message.protocolMessage.key) {
                            const revokedKey = message.message.protocolMessage.key
                            log(`🔄 [Bull Queue REVOKE] Processing revoke message for ${revokedKey.id} in chat ${revokedKey.remoteJid || jid}`)
                            
                            try {
                                // Update the revoked message to mark it as deleted/revoked
                                const updateResult = await collections.messages.updateOne(
                                    {
                                        instanceId,
                                        jid: revokedKey.remoteJid || jid,
                                        'key.id': revokedKey.id
                                    },
                                    {
                                        $set: {
                                            'message.protocolMessage': message.message.protocolMessage,
                                            revoked: true,
                                            revokedAt: new Date(),
                                            revokedBy: message.key.fromMe ? 'me' : message.key.participant || message.key.remoteJid
                                        }
                                    }
                                )
                                
                                if (updateResult.matchedCount > 0) {
                                    log(`✅ [Bull Queue REVOKE] Successfully marked message ${revokedKey.id} as revoked`)
                                } else {
                                    log(`⚠️ [Bull Queue REVOKE] Message ${revokedKey.id} not found to revoke`)
                                }
                            } catch (error) {
                                logError(`[Bull Queue REVOKE] Failed to process revoke for message ${revokedKey.id}:`, error)
                            }
                            
                            // Skip storing the REVOKE message itself
                            return { success: true }
                        }
                        
                        // Skip other protocol message types that shouldn't be stored
                        const skipTypes = [
                            proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION,
                            proto.Message.ProtocolMessage.Type.APP_STATE_SYNC_KEY_SHARE,
                            proto.Message.ProtocolMessage.Type.INITIAL_SECURITY_NOTIFICATION_SETTING_SYNC,
                            proto.Message.ProtocolMessage.Type.APP_STATE_SYNC_KEY_REQUEST
                        ]
                        
                        if (protoType && skipTypes.includes(protoType)) {
                            log(`⏭️ [Bull Queue Protocol] Skipping protocol message of type ${protoType} for instance ${instanceId}`)
                            return { success: true }
                        }
                    }
                    
                    try {
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
                    } catch (error: any) {
                        // Handle duplicate key errors gracefully
                        if (error.code === 11000 || error.message?.includes('duplicate key')) {
                            log(`⚠️ [Bull Queue] Duplicate key error for message ${message.key?.id} in chat ${jid} - message already exists`)
                            // Try to update instead of replace
                            try {
                                await collections.messages.updateOne(
                                    {
                                        instanceId,
                                        jid,
                                        'key.id': message.key?.id
                                    },
                                    {
                                        $set: {
                                            ...message,
                                            updatedAt: new Date()
                                        }
                                    }
                                )
                                log(`✅ [Bull Queue] Successfully updated existing message ${message.key?.id} after duplicate key error`)
                            } catch (updateError) {
                                logError(`[Bull Queue] Failed to update message after duplicate key error:`, updateError)
                                throw updateError
                            }
                        } else {
                            throw error
                        }
                    }
                    performanceMetrics.messagesProcessed++
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
            
            // Create queue for CHATS
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
            
            // Create queue for CONTACTS
            createQueueAndWorker<ContactJob>(QueueType.CONTACTS, async (job) => {
                const { type, contacts, contact } = job.data
                
                if (type === 'upsert' && contacts) {
                    const bulkOps = contacts.map(contact => {
                        const { notify, ...rest } = (contact as any) || {}
                        return {
                            updateOne: {
                                filter: { instanceId, id: contact.id },
                                update: {
                                    $set: { ...rest, instanceId, updatedAt: new Date() },
                                    $setOnInsert: {
                                        instanceId,
                                        id: contact.id,
                                        ...(notify !== undefined ? { notify } : {})
                                    }
                                },
                                upsert: true
                            }
                        }
                    })
                    await collections.contacts.bulkWrite(bulkOps, { ordered: false })
                } else if (type === 'update' && contact) {
                const { notify, id: _ignoredId, instanceId: _ignoredInstanceId, ...rest } = (contact as any) || {}
                    await collections.contacts.updateOne(
                        { instanceId, id: contact.id },
                        {
                        $set: { ...rest, updatedAt: new Date() },
                            $setOnInsert: {
                                instanceId,
                                id: contact.id,
                                ...(notify !== undefined ? { notify } : {})
                            }
                        },
                        { upsert: true }
                    )
                }
                
                return { success: true }
            })
            
            // Create queue for GROUP METADATA
            createQueueAndWorker<GroupMetadataJob>(QueueType.GROUP_METADATA, async (job) => {
                const { type, jid, metadata, update } = job.data
                console.log(`[${instanceId}] Bull worker processing job ${job.id} for group metadata`)
                
                if (type === 'upsert') {
                    console.log(`[${instanceId}] Processing upsert for group ${metadata.id}`)
                    config.logger?.debug({ instanceId, groupId: metadata.id }, 'Processing group metadata upsert job')
                    try {
                        const result = await collections.groupMetadata.replaceOne(
                            { instanceId, id: metadata.id },
                            { ...metadata, instanceId, updatedAt: new Date() },
                            { upsert: true }
                        )
                        console.log(`[${instanceId}] Bull worker result for group ${metadata.id}: upserted=${result.upsertedCount}, modified=${result.modifiedCount}`)
                        config.logger?.info({ instanceId, groupId: metadata.id, upserted: result.upsertedCount, modified: result.modifiedCount }, 'Group metadata processed by queue')
                    } catch (error) {
                        console.error(`[${instanceId}] Bull worker failed for group ${metadata.id}:`, error)
                        throw error
                    }
                } else if (type === 'update' && update) {
                    console.log(`[${instanceId}] Processing update for group ${jid}`)
                    await collections.groupMetadata.updateOne(
                        { instanceId, id: jid },
                        { $set: { ...update, updatedAt: new Date() } }
                    )
                }
                
                return { success: true }
            })
            
            // Create queue for PRESENCES
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
            
            // Create queue for STATE
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
            
            // Set up health check and auto-restart
            const checkQueuesHealth = async () => {
                try {
                    // Check Redis connection
                    if (redisConnection?.status !== 'ready') {
                        logError('⚠️ Redis connection lost, attempting to reconnect...')
                        await restartQueues()
                        return
                    }
                    
                    // Check each queue for stuck jobs
                    for (const [queueType, queue] of queues.entries()) {
                        try {
                            const counts = await queue.getJobCounts()
                            const worker = workers.get(queueType)
                            
                            // Check for stuck jobs (waiting too long)
                            if (counts.waiting > 100) {
                                logWarn(`⚠️ ${queueType} queue has ${counts.waiting} waiting jobs`)
                            }
                            
                            // Check if worker is running
                            if (worker && !worker.isRunning()) {
                                logError(`❌ ${queueType} worker stopped, restarting...`)
                                await worker.run()
                            }
                            
                            // Check for stale active jobs
                            const activeJobs = await queue.getActive()
                            const now = Date.now()
                            for (const job of activeJobs) {
                                const processingTime = now - job.processedOn!
                                if (processingTime > QUEUE_STALE_THRESHOLD) {
                                    logWarn(`⚠️ Stale job detected in ${queueType}: ${job.id} (${processingTime}ms)`)
                                    // Move stale job back to waiting
                                    // BullMQ v5 requires a lock token as the second argument
                                    const token = (job as any).token as string | undefined
                                    if (typeof token === 'string') {
                                        await job.moveToFailed(new Error('Job stale, moving to failed'), token, false)
                                    } else {
                                        // If token is unavailable, skip moving to failed to avoid runtime errors
                                        logWarn(`Skipping moveToFailed for job ${job.id}: missing lock token`)
                                    }
                                }
                            }
                        } catch (error) {
                            logError(`Health check failed for ${queueType}:`, error)
                        }
                    }
                    
                    lastHealthCheck = Date.now()
                } catch (error) {
                    logError('Queue health check error:', error)
                    // Try to restart if health check completely fails
                    if (Date.now() - lastHealthCheck > QUEUE_STALE_THRESHOLD * 2) {
                        await restartQueues()
                    }
                }
            }
            
            // Function to restart queues
            const restartQueues = async () => {
                log('🔄 Restarting Bull queues...')
                
                try {
                    // Close existing workers and queues
                    for (const worker of workers.values()) {
                        await worker.close()
                    }
                    for (const queue of queues.values()) {
                        await queue.close()
                    }
                    
                    // Clear maps
                    workers.clear()
                    queues.clear()
                    
                    // Reconnect Redis if needed
                    if (redisConnection && redisConnection.status !== 'ready') {
                        redisConnection.disconnect()
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
                        await redisConnection.ping()
                    }
                    
                    // Recreate all queues and workers
                    await initializeBullQueues()
                    
                    log('✅ Queues restarted successfully')
                } catch (error) {
                    logError('❌ Failed to restart queues:', error)
                    bullInitialized = false
                }
            }
            
            // Start health check interval
            healthCheckInterval = setInterval(checkQueuesHealth, HEALTH_CHECK_INTERVAL)
            
            // Also set up minimal cleanup for any jobs that slip through
            setInterval(async () => {
                for (const [, queue] of queues.entries()) {
                    try {
                        // Clean any completed/failed jobs older than 10 seconds (safety net)
                        await queue.clean(10000, 1000, 'completed')
                        await queue.clean(10000, 1000, 'failed')
                    } catch (error) {
                        // Ignore cleanup errors
                    }
                }
            }, 60000) // Clean every minute as safety net
            
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
    
    // Helper function to track label operations in Redis cache for automation  
    const trackLabelOperation = async (operation: 'add' | 'remove', association: LabelAssociation) => {
        try {
            if (!redisConnection) {
                log('[Label Operations] Redis not available, skipping cache tracking')
                return
            }

            const normalizedChatId = association.chatId.replace('@s.whatsapp.net', '').replace('@lid', '')
            const hashKey = `labelsAssociation:${instanceId}`
            
            // Get existing data from Redis
            const existingData = await redisConnection.hget(hashKey, normalizedChatId)
            const metadata = existingData ? JSON.parse(existingData) : {
                chatId: association.chatId,
                addLabelIds: [],
                removeLabelIds: []
            }
            
            // Update metadata based on operation type (like waziper.js)
            if (operation === 'add') {
                metadata.removeLabelIds = metadata.removeLabelIds.filter((id: string) => id !== association.labelId)
                if (!metadata.addLabelIds.includes(association.labelId)) {
                    metadata.addLabelIds.push(association.labelId)
                }
            } else {
                metadata.addLabelIds = metadata.addLabelIds.filter((id: string) => id !== association.labelId)
                if (!metadata.removeLabelIds.includes(association.labelId)) {
                    metadata.removeLabelIds.push(association.labelId)
                }
            }
            
            // Store updated data in Redis with TTL
            const pipeline = redisConnection.pipeline()
            pipeline.hset(hashKey, normalizedChatId, JSON.stringify(metadata))
            pipeline.expire(hashKey, 48 * 60 * 60) // 48 hours TTL for safety
            await pipeline.exec()
            
            log(`[Label Operations] Tracked ${operation} operation in Redis for chat ${association.chatId}, label ${association.labelId}`)
        } catch (error) {
            logError(`[Label Operations] Failed to track ${operation} operation:`, error)
        }
    }
    
    // Memory-aware batch processors
    let labelAssociationProcessor: MemoryAwareBatchProcessor<LabelAssociation> | null = null
    let messageProcessor: MemoryAwareBatchProcessor<proto.IWebMessageInfo & { jid: string }> | null = null
    
    // Legacy batch accumulators for backward compatibility
    const labelAssociationBatch: BatchAccumulator<LabelAssociation> = {
        items: [],
        timer: null,
        processing: false,
        totalReceived: 0,
        totalProcessed: 0,
        pendingPromises: []
    }
    
    const messageBatch: BatchAccumulator<proto.IWebMessageInfo & { jid: string }> = {
        items: [],
        timer: null,
        processing: false
    }
    
    // Create collections getter that ensures connection
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
    
    // Initialize collections
    let collections = getCollections()
    
    // Initialize memory-aware batch processors if memory config is provided
    if (memory) {
        // Label association processor
        labelAssociationProcessor = new MemoryAwareBatchProcessor<LabelAssociation>(
            async (items) => {
                const bulkOps = items.map(association => {
                    const filter: any = {
                        instanceId: validatedInstanceId,
                        type: association.type, // Include type field for proper matching
                        chatId: association.chatId,
                        labelId: association.labelId
                    }
                    
                    if ('messageId' in association && association.messageId) {
                        filter.messageId = association.messageId
                    }
                    
                    return {
                        replaceOne: {
                            filter,
                            replacement: {
                                ...association,
                                instanceId: validatedInstanceId,
                                updatedAt: new Date()
                            },
                            upsert: true
                        }
                    }
                })
                
                // Calculate optimal batch size based on memory pressure
                const optimalBatchSize = backpressureController 
                    ? calculateOptimalBatchSize(BATCH_SIZE, backpressureController.getPressure())
                    : BATCH_SIZE
                
                // Process in chunks
                for (let i = 0; i < bulkOps.length; i += optimalBatchSize) {
                    const chunk = bulkOps.slice(i, i + optimalBatchSize)
                    await withConnection(() => collections.labelAssociations.bulkWrite(chunk, { ordered: false }))
                    
                    if (i + optimalBatchSize < bulkOps.length) {
                        await new Promise(resolve => setTimeout(resolve, BATCH_DELAY))
                    }
                }
                
                performanceMetrics.labelsProcessed += items.length
                performanceMetrics.batchesProcessed++
                log(`[Memory-Aware Batch] Processed ${items.length} label associations`)
            },
            memory
        )
        
        // Message processor
        messageProcessor = new MemoryAwareBatchProcessor<proto.IWebMessageInfo & { jid: string }>(
            async (items) => {
                const bulkOps = items.map(({ jid, ...message }) => ({
                    replaceOne: {
                        filter: {
                            instanceId: validatedInstanceId,
                            jid,
                            'key.id': message.key.id
                        },
                        replacement: {
                            ...message,
                            instanceId: validatedInstanceId,
                            jid,
                            updatedAt: new Date()
                        },
                        upsert: true
                    }
                }))
                
                const optimalBatchSize = backpressureController 
                    ? calculateOptimalBatchSize(BATCH_SIZE, backpressureController.getPressure())
                    : BATCH_SIZE
                
                for (let i = 0; i < bulkOps.length; i += optimalBatchSize) {
                    const chunk = bulkOps.slice(i, i + optimalBatchSize)
                    await withConnection(() => collections.messages.bulkWrite(chunk, { ordered: false }))
                    
                    if (i + optimalBatchSize < bulkOps.length) {
                        await new Promise(resolve => setTimeout(resolve, BATCH_DELAY))
                    }
                }
                
                performanceMetrics.messagesProcessed += items.length
                log(`[Memory-Aware Batch] Processed ${items.length} messages`)
            },
            memory
        )
    }
    
    // Enhanced wrapper for MongoDB operations with retry logic
    const withConnection = async <T>(
        operation: () => Promise<T>,
        retryOptions?: Partial<RetryOptions>
    ): Promise<T> => {
        const startTime = Date.now()
        
        // Define retry options with defaults
        const options: RetryOptions = {
            maxAttempts: retryOptions?.maxAttempts ?? 3,
            initialDelay: retryOptions?.initialDelay ?? 100,
            maxDelay: retryOptions?.maxDelay ?? 5000,
            factor: retryOptions?.factor ?? 2,
            jitter: retryOptions?.jitter ?? true,
            shouldRetry: (error: any) => {
                // Use the default retry logic from connectionRetry which includes session errors
                return isRetryableError(error)
            }
        }
        
        const result = await retryWithBackoff(
            async () => {
                await ensureConnection()
                // Refresh collections after reconnection
                collections = getCollections()
                
                // Track activity for connection manager
                if (isUsingSharedConnection && connectionManager) {
                    connectionManager.recordActivity(validatedInstanceId)
                }
                
                return await operation()
            },
            options,
            (attempt, error, delay) => {
                logWarn(`[withConnection] Retry attempt ${attempt} for instance ${validatedInstanceId} after error: ${error.message}. Waiting ${delay}ms...`)
            }
        )
        
        if (!result.success) {
            logError(`[withConnection] Operation failed after ${result.attempts} attempts:`, result.error)
            throw result.error
        }
        
        // Record response time for successful operations
        if (isUsingSharedConnection && connectionManager) {
            const responseTime = Date.now() - startTime
            connectionManager.recordActivity(validatedInstanceId, responseTime)
        }
        
        return result.result as T
    }

    // Batch processing functions
    const processBatchedLabelAssociations = async () => {
        // Prevent concurrent processing
        if (labelAssociationBatch.processing) {
            log(`[Label Batch] Skipping - already processing`)
            return
        }
        
        // Check if there are items to process
        if (labelAssociationBatch.items.length === 0) {
            return
        }
        
        labelAssociationBatch.processing = true
        
        // Clear the timer immediately to prevent duplicate processing
        if (labelAssociationBatch.timer) {
            clearTimeout(labelAssociationBatch.timer)
            labelAssociationBatch.timer = null
        }
        
        // Take all items atomically
        const itemsToProcess = labelAssociationBatch.items.splice(0)
        const pendingPromises = labelAssociationBatch.pendingPromises?.splice(0, itemsToProcess.length) || []
        log(`[Label Batch] Processing ${itemsToProcess.length} label associations`)
        
        // Add protection against memory leaks from excessive batch accumulation
        if (itemsToProcess.length > BATCH_SIZE * 10) {
            logWarn(`Batch size exceeded for instance ${instanceId} (${itemsToProcess.length} items), processing first ${BATCH_SIZE * 10} items`)
            itemsToProcess.splice(BATCH_SIZE * 10)
        }
        
        try {
            const bulkOps = itemsToProcess.map(association => {
                // Build filter based on whether it's a message or contact label
                const filter: any = {
                    instanceId,
                    type: association.type, // Include type field for proper matching
                    chatId: association.chatId,
                    labelId: association.labelId
                }
                
                // Only include messageId for message labels
                if ('messageId' in association && association.messageId) {
                    filter.messageId = association.messageId
                }
                
                return {
                    replaceOne: {
                        filter,
                        replacement: {
                            ...association,
                            instanceId,
                            updatedAt: new Date()
                        },
                        upsert: true
                    }
                }
            })
            
            // Process in chunks to avoid overwhelming MongoDB
            for (let i = 0; i < bulkOps.length; i += BATCH_SIZE) {
                const chunk = bulkOps.slice(i, i + BATCH_SIZE)
                await withConnection(() => collections.labelAssociations.bulkWrite(chunk, { ordered: false }))
                
                // Small delay between chunks
                if (i + BATCH_SIZE < bulkOps.length) {
                    await new Promise(resolve => setTimeout(resolve, BATCH_DELAY))
                }
            }
            
            performanceMetrics.labelsProcessed += itemsToProcess.length
            performanceMetrics.batchesProcessed++
            labelAssociationBatch.totalProcessed = (labelAssociationBatch.totalProcessed || 0) + itemsToProcess.length
            
            // Resolve all pending promises for this batch
            pendingPromises.forEach(p => p.resolve())
            
            console.log(`[Label Batch] Successfully processed ${itemsToProcess.length} label associations (total processed: ${labelAssociationBatch.totalProcessed}/${labelAssociationBatch.totalReceived})`)
        } catch (error) {
            console.error('Error processing label associations batch:', error)
            performanceMetrics.errors++
            
            // Reject all pending promises for this batch
            pendingPromises.forEach(p => p.reject(error))
            
            // Re-add failed items to the queue for retry
            labelAssociationBatch.items.unshift(...itemsToProcess)
            console.log(`[Label Batch] Re-queued ${itemsToProcess.length} items after error`)
        } finally {
            labelAssociationBatch.processing = false
            // Check if more items accumulated during processing
            if (labelAssociationBatch.items.length > 0) {
                console.log(`[Label Batch] ${labelAssociationBatch.items.length} new items accumulated, scheduling next batch`)
                scheduleLabelBatch()
            }
        }
    }
    
    const processBatchedMessages = async () => {
        if (messageBatch.processing || messageBatch.items.length === 0) return
        
        messageBatch.processing = true
        const itemsToProcess = [...messageBatch.items]
        messageBatch.items = []
        
        // Add protection against memory leaks from excessive batch accumulation
        if (itemsToProcess.length > BATCH_SIZE * 10) {
            console.warn(`Message batch size exceeded for instance ${instanceId} (${itemsToProcess.length} items), processing first ${BATCH_SIZE * 10} items`)
            itemsToProcess.splice(BATCH_SIZE * 10)
        }
        
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
                await withConnection(() => collections.messages.bulkWrite(chunk, { ordered: false }))
                
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
    
    // Schedule batch processing with deduplication
    const scheduleLabelBatch = () => {
        // Don't schedule if already processing
        if (labelAssociationBatch.processing) {
            return
        }
        
        // Clear existing timer
        if (labelAssociationBatch.timer) {
            clearTimeout(labelAssociationBatch.timer)
        }
        
        // Set new timer
        labelAssociationBatch.timer = setTimeout(() => {
            labelAssociationBatch.timer = null
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
    
    // Smart index creation with collection existence checking
    const createIndexes = async () => {
        const ttlSeconds = ttlDays * 24 * 60 * 60
        
        // Migration: Drop existing TTL indexes for collections that should persist indefinitely
        const dropObsoleteTTLIndexes = async () => {
            try {
                // Drop TTL indexes for groupMetadata if they exist
                await collections.groupMetadata.dropIndex('updatedAt_1').catch(() => {
                    // Index might not exist, ignore error
                })
                
                // Drop TTL indexes for labelAssociations if they exist
                await collections.labelAssociations.dropIndex('updatedAt_1').catch(() => {
                    // Index might not exist, ignore error
                })
                
                // Drop TTL indexes for labels if they exist
                await collections.labels.dropIndex('updatedAt_1').catch(() => {
                    // Index might not exist, ignore error
                })
                
                log('🔄 Migrated: Removed TTL indexes from groupMetadata, labelAssociations, and labels')
            } catch (error) {
                // Migration is best-effort, continue even if it fails
                log('⚠️  TTL index migration completed (some indexes may not have existed)')
            }
        }
        
        // Run migration before creating new indexes
        await dropObsoleteTTLIndexes()
        
        // Define all indexes by collection with standardized format
        const indexDefinitions: Record<string, IndexSpec[]> = {
            chats: [
                { name: 'chats_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } },
                { name: 'chats_ttl', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds } }
            ],
                    contacts: [
                        { name: 'contacts_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } },
                        { name: 'contacts_lid_lookup', spec: { instanceId: 1, lid: 1 }, options: { unique: true, partialFilterExpression: { lid: { $type: 'string' } } } }
                    ],
            messages: [
                { name: 'messages_primary', spec: { instanceId: 1, jid: 1, 'key.id': 1 }, options: { unique: true } },
                { name: 'messages_query', spec: { instanceId: 1, jid: 1, messageTimestamp: -1 }, options: {} },
                { name: 'messages_keyid', spec: { instanceId: 1, 'key.id': 1 }, options: {} },
                { name: 'messages_ttl', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds } }
            ],
            groupMetadata: [
                { name: 'groups_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } }
                // No TTL - persists indefinitely
            ],
            state: [
                { name: 'state_primary', spec: { instanceId: 1 }, options: { unique: true } }
            ],
            presences: [
                { name: 'presences_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } },
                { name: 'presences_ttl', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds } }
            ],
            labels: [
                { name: 'labels_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } }
                // No TTL - persists indefinitely
            ],
            labelAssociations: [
                { name: 'label_assoc_primary', spec: { instanceId: 1, type: 1, chatId: 1, labelId: 1 }, options: { unique: true } },
                { name: 'label_assoc_chat', spec: { instanceId: 1, chatId: 1 }, options: {} },
                { name: 'label_assoc_message', spec: { instanceId: 1, messageId: 1 }, options: {} }
                // No TTL - persists indefinitely
            ]
        }
        
        // Smart index creation logic with configuration support
        const createResults: Array<{ collection: string, created: number, skipped: boolean, details: string[] }> = []
        let totalCreated = 0
        let totalSkipped = 0
        
        if (indexConfig.enableIndexHealthLogging) {
            console.log(`🔧 Smart index management for instance ${instanceId}...`)
            console.log(`   Settings: skipExisting=${indexConfig.skipExistingCollectionIndexes}, forceRecreate=${indexConfig.forceRecreateIndexes}`)
        }
        
        // Process each collection
        for (const [collectionName, requiredIndexes] of Object.entries(indexDefinitions)) {
            try {
                const collection = collections[collectionName as keyof MongoCollections]
                
                // Handle force recreation mode
                if (indexConfig.forceRecreateIndexes) {
                    if (indexConfig.enableIndexHealthLogging) {
                        console.log(`🔄 Collection ${collectionName}: Force recreating all ${requiredIndexes.length} indexes`)
                    }
                    
                    // Force recreate: drop and recreate all indexes with timeout applied
                    const timedIndexes = requiredIndexes.map(idx => ({
                        ...idx,
                        options: { ...idx.options, maxTimeMS: indexConfig.indexCreationTimeout }
                    }))
                    const batchResult = await withConnection(() => 
                        recreateIndexes(collection, timedIndexes)
                    )
                    
                    createResults.push({
                        collection: collectionName,
                        created: batchResult.successful,
                        skipped: false,
                        details: batchResult.details
                    })
                    totalCreated += batchResult.successful
                } else if (indexConfig.skipExistingCollectionIndexes) {
                    // Smart mode: check what indexes are needed
                    const checkResult = await withConnection(() => shouldCreateIndexes(collection as any, requiredIndexes))
                    
                    if (checkResult.missingIndexes.length === 0) {
                        if (indexConfig.enableIndexHealthLogging) {
                            console.log(`✅ Collection ${collectionName}: All ${checkResult.requiredCount} indexes exist, skipping creation`)
                        }
                        createResults.push({
                            collection: collectionName,
                            created: 0,
                            skipped: true,
                            details: [`All ${checkResult.requiredCount} indexes already exist`]
                        })
                        totalSkipped += checkResult.requiredCount
                    } else {
                        const missingCount = checkResult.missingIndexes.length
                        const action = checkResult.collectionExists ? 'Creating missing' : 'Creating all'
                        if (indexConfig.enableIndexHealthLogging) {
                            console.log(`🔨 Collection ${collectionName}: ${action} ${missingCount} indexes`)
                        }
                        
                        // Create missing indexes using batch operation with timeout applied
                        const timedMissingIndexes = checkResult.missingIndexes.map(idx => ({
                            ...idx,
                            options: { ...idx.options, maxTimeMS: indexConfig.indexCreationTimeout }
                        }))
                        const batchResult = await batchCreateIndexes(collection, timedMissingIndexes, withConnection)
                        
                        createResults.push({
                            collection: collectionName,
                            created: batchResult.successful,
                            skipped: false,
                            details: batchResult.details
                        })
                        
                        totalCreated += batchResult.successful
                        
                        // Critical indexes must succeed (unique and primary key indexes)
                        const createdNames = new Set(
                            batchResult.details
                                .filter(d => d.startsWith('✅ Created index: '))
                                .map(d => d.replace('✅ Created index: ', '').trim())
                        )
                        const criticalIndexes = checkResult.missingIndexes.filter(idx => 
                            idx.options?.unique || /primary|unique/i.test(idx.name)
                        )
                        const failedCriticalIndexes = criticalIndexes.filter(idx => !createdNames.has(idx.name))
                        
                        if (batchResult.failed > 0) {
                            if (failedCriticalIndexes.length > 0) {
                                throw new Error(`Critical indexes failed for ${collectionName}: ${failedCriticalIndexes.map(idx => idx.name).join(', ')}`)
                            } else {
                                console.warn(`⚠️ Some non-critical indexes failed for ${collectionName}: ${batchResult.failed} failures`)
                            }
                        }
                    }
                } else {
                    // Legacy mode: create all indexes without smart checking
                    if (indexConfig.enableIndexHealthLogging) {
                        console.log(`🔧 Collection ${collectionName}: Creating all ${requiredIndexes.length} indexes (legacy mode)`)
                    }
                    
                    // Apply timeout to all indexes for legacy mode
                    const timedIndexes = requiredIndexes.map(idx => ({
                        ...idx,
                        options: { ...idx.options, maxTimeMS: indexConfig.indexCreationTimeout }
                    }))
                    const batchResult = await batchCreateIndexes(collection, timedIndexes, withConnection)
                    
                    createResults.push({
                        collection: collectionName,
                        created: batchResult.successful,
                        skipped: false,
                        details: batchResult.details
                    })
                    totalCreated += batchResult.successful
                }
            } catch (error) {
                console.error(`❌ Failed to process indexes for collection ${collectionName}:`, error)
                throw error // Re-throw to halt initialization if critical
            }
        }
        
        // Summary logging
        const totalRequired = Object.values(indexDefinitions).reduce((sum, indexes) => sum + indexes.length, 0)
        if (indexConfig.enableIndexHealthLogging) {
            console.log(`✅ Smart index management completed for instance ${instanceId}:`)
            console.log(`   📊 Total indexes: ${totalRequired} required`)
            console.log(`   🔨 Created: ${totalCreated}`)
            console.log(`   ⏭️  Skipped (existing): ${totalSkipped}`)
            console.log(`   📈 Efficiency: ${Math.round((totalSkipped / totalRequired) * 100)}% reduction in index operations`)
        }
        
        // Detailed logging if any indexes were created
        if (totalCreated > 0) {
            if (indexConfig.enableIndexHealthLogging) {
                console.log('📋 Index creation details:')
                createResults.forEach(result => {
                    if (result.created > 0) {
                        console.log(`   ${result.collection}: ${result.created} created`)
                    }
                })
            }
            
            // Clear collection cache since new collections may have been created
            clearCollectionCache()
            if (indexConfig.enableIndexHealthLogging) {
                console.log('🧹 Cleared collection cache after index creation')
            }
        }
        
        // Verify TTL indexes if monitoring is enabled and TTL indexes were created
        if (ttlMonitor !== null && totalCreated > 0) {
            const ttlCollections = ['chats', 'contacts', 'messages', 'presences']
            const createdTTLCollections = createResults
                .filter(r => r.created > 0 && ttlCollections.includes(r.collection))
                .map(r => r.collection)
            
            if (createdTTLCollections.length > 0) {
                log('[TTL Monitor] Verifying newly created TTL indexes...')
                const verificationPromises = createdTTLCollections.map(collectionName => 
                    ttlMonitor!.verifyTTLIndex(`${collectionPrefix}${collectionName}`)
                )
                
                const verificationResults = await Promise.all(verificationPromises)
                const invalidTTL = verificationResults.filter(r => !r.isValid)
                
                if (invalidTTL.length > 0) {
                    console.warn(`[TTL Monitor] Found ${invalidTTL.length} invalid TTL indexes:`, invalidTTL.map(r => r.collection))
                } else {
                    log('[TTL Monitor] ✅ All newly created TTL indexes verified successfully')
                }
            } else {
                log('[TTL Monitor] No new TTL indexes to verify')
            }
        }
    }
    
    // Check and fix labelAssociations index if needed
    const checkAndFixLabelAssociationsIndex = async () => {
        try {
            const existingIndexes = await collections.labelAssociations.indexes()
            
            // Look for the old problematic index that includes messageId
            const oldIndex = existingIndexes.find(idx => {
                const keys = Object.keys(idx.key || {})
                return keys.includes('instanceId') && 
                       keys.includes('chatId') && 
                       keys.includes('labelId') && 
                       keys.includes('messageId') && 
                       keys.length === 4
            })
            
            if (oldIndex) {
                console.log(`🔧 Found old labelAssociations index with messageId field, updating...`)
                
                // Drop the old index
                const indexName = oldIndex.name || 'instanceId_1_chatId_1_labelId_1_messageId_1'
                try {
                    await collections.labelAssociations.dropIndex(indexName)
                    console.log(`✅ Dropped old index: ${indexName}`)
                } catch (dropError) {
                    console.warn(`⚠️  Failed to drop old index ${indexName}:`, dropError)
                }
            }
        } catch (error) {
            console.warn('⚠️  Could not check existing labelAssociations indexes:', error)
        }
    }
    
    // Fix old index before creating new ones
    await checkAndFixLabelAssociationsIndex()
    
    // Initialize indexes - critical indexes must succeed
    await createIndexes()

    // Start TTL monitor after indexes are ensured
    if (ttlMonitor !== null) {
        const monitor = ttlMonitor as TTLMonitor
        monitor.startMonitoring((message: string) => {
            logWarn(`[TTL Monitor] ${message}`)
        })
    }
    
    // Create a proxy to automatically wrap all async methods with connection checking
    const createStoreProxy = (target: any): MongoDBStore => {
        return new Proxy(target, {
            get(obj, prop) {
                const value = obj[prop]
                if (typeof value === 'function' && prop !== 'bind' && prop !== 'close') {
                    return async (...args: any[]) => {
                        // Special handling for methods that already use withConnection
                        const methodsWithConnection = new Set(['getChats', 'getChat', 'updateState'])
                        if (methodsWithConnection.has(prop as string)) {
                            return value.apply(obj, args)
                        }
                        // Wrap other async methods
                        return withConnection(() => value.apply(obj, args))
                    }
                }
                return value
            }
        }) as MongoDBStore
    }

    const storeImpl = {
        instanceId,

        async getChats(): Promise<Chat[]> {
            return withConnection(async () => {
                const chats = await collections.chats
                    .find({ instanceId })
                    .sort({ conversationTimestamp: -1 })
                    .toArray()
                
                return chats.map(({ _id, instanceId: _instanceId, updatedAt: _updatedAt, ...chat }) => chat as Chat)
            })
        },

        async getChat(jid: string): Promise<Chat | null> {
            try {
                const validJid = validateJID(jid)
                
                return withConnection(async () => {
                    const chat = await collections.chats.findOne({ instanceId: validatedInstanceId, id: validJid })
                    if (!chat) return null
                    
                    // Check access permissions
                    accessContext.validateAccess(chat.instanceId, 'read')
                    
                    // eslint-disable-next-line @typescript-eslint/no-unused-vars
                    const { _id, instanceId: _instanceId, updatedAt, ...chatData } = chat
                    return chatData as Chat
                })
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
                { 
                    $set: { ...update, updatedAt: new Date() }
                }
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
            const bulkOps = contacts.map(contact => {
                const { notify, id: _ignoredId, instanceId: _ignoredInstanceId, ...rest } = (contact as any) || {}
                return {
                    updateOne: {
                        filter: { instanceId, id: contact.id },
                        update: {
                            $set: { ...rest, updatedAt: new Date() },
                            $setOnInsert: {
                                instanceId,
                                id: contact.id,
                                ...(notify !== undefined ? { notify } : {})
                            }
                        },
                        upsert: true
                    }
                }
            })
            
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
            // Normalize JID if it's a LID
            let normalizedJid = jid
            if (lidHandler) {
                normalizedJid = await lidHandler.normalizeJid(jid) || jid
            }
            
            const messages = await collections.messages
                .find({ instanceId, jid: normalizedJid })
                .sort({ messageTimestamp: -1 })
                .toArray()
            
            // Convert Binary objects and preserve messageContextInfo
            return messages.map(({ _id, instanceId: _instanceId, jid: _jid, updatedAt: _updatedAt, ...msg }) => {
                const converted = convertBinaryToBuffer(msg)
                
                // Special handling for messageSecret in poll messages
                if (msg.message?.messageContextInfo?.messageSecret) {
                    const secret = msg.message.messageContextInfo.messageSecret as any
                    if (secret._bsontype === 'Binary' && secret.buffer) {
                        converted.message.messageContextInfo.messageSecret = Buffer.from(secret.buffer)
                    } else if (secret.type === 'Buffer' && Array.isArray(secret.data)) {
                        converted.message.messageContextInfo.messageSecret = Buffer.from(secret.data)
                    } else if (!Buffer.isBuffer(secret)) {
                        try {
                            converted.message.messageContextInfo.messageSecret = Buffer.from(secret)
                        } catch (e) {
                            console.error('Failed to convert messageSecret to Buffer:', e)
                        }
                    }
                }
                
                return converted
            })
        },

        async getMessage(jid: string, id: string): Promise<proto.IWebMessageInfo | null> {
            const startTime = Date.now()
            try {
                // Normalize JID if it's a LID
                let normalizedJid = jid
                if (lidHandler) {
                    normalizedJid = await lidHandler.normalizeJid(jid) || jid
                }
                
                const validJid = validateJID(normalizedJid)
                const validId = validateMessageId(id)
                
                // Check cache first
                const cacheKey = `msg_${validatedInstanceId}_${hashForLogging(validJid)}_${hashForLogging(validId)}`
                const cached = binaryConversionCache.get<proto.IWebMessageInfo>(cacheKey)
                if (cached) {
                    const elapsed = Date.now() - startTime
                    if (elapsed > 10) {
                        log(`getMessage cache hit in ${elapsed}ms`)
                    }
                    return cached
                }
                
                // Primary query using jid
                let message = await collections.messages.findOne({
                    instanceId: validatedInstanceId,
                    jid: validJid,
                    'key.id': validId
                })
                
                // Fallback query using key.remoteJid (for poll messages and edge cases)
                if (!message) {
                    const fallbackStart = Date.now()
                    log(`[getMessage] Primary query failed for jid: ${validJid}, id: ${validId}. Trying fallback with key.remoteJid`)
                    message = await collections.messages.findOne({
                        instanceId: validatedInstanceId,
                        'key.remoteJid': validJid,
                        'key.id': validId
                    })
                    
                    const fallbackTime = Date.now() - fallbackStart
                    if (message) {
                        log(`[getMessage] ✅ Found message using fallback query with key.remoteJid in ${fallbackTime}ms`)
                    } else if (fallbackTime > 100) {
                        logWarn(`[getMessage] Slow fallback query: ${fallbackTime}ms`)
                    }
                }
                
                if (!message) {
                    const totalTime = Date.now() - startTime
                    if (totalTime > 100) {
                        logWarn(`[getMessage] Message not found after ${totalTime}ms - ID: ${validId}, JID: ${validJid}`)
                    }
                    return null
                }
                
                // Check access permissions
                accessContext.validateAccess(message.instanceId, 'read')
                
                // eslint-disable-next-line @typescript-eslint/no-unused-vars
                const { _id, instanceId: _instanceId, jid: _jid, updatedAt: _updatedAt, ...msg } = message
            // Convert all MongoDB Binary objects to Buffers and preserve messageContextInfo
            const converted = convertBinaryToBuffer(msg)
            
            // Special handling for messageSecret in poll messages
            if (msg.message?.messageContextInfo?.messageSecret) {
                const secret = msg.message.messageContextInfo.messageSecret as any
                if (secret._bsontype === 'Binary' && secret.buffer) {
                    // Ensure the messageSecret is properly converted to a Buffer
                    converted.message.messageContextInfo.messageSecret = Buffer.from(secret.buffer)
                } else if (secret.type === 'Buffer' && Array.isArray(secret.data)) {
                    converted.message.messageContextInfo.messageSecret = Buffer.from(secret.data)
                } else if (!Buffer.isBuffer(secret)) {
                    // Try to convert to Buffer if it's not already
                    try {
                        converted.message.messageContextInfo.messageSecret = Buffer.from(secret)
                    } catch (e) {
                        logError('Failed to convert messageSecret')
                    }
                }
            }
            
            // Cache the converted message
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

        async upsertMessage(jid: string, message: proto.IWebMessageInfo, useBatch: boolean = false): Promise<void> {
            try {
                // Handle protocol messages before validation
                if (message.message?.protocolMessage) {
                    const protoType = message.message.protocolMessage.type
                    
                    // Handle REVOKE messages - update the revoked message instead of storing the revoke message
                    if (protoType === proto.Message.ProtocolMessage.Type.REVOKE && message.message.protocolMessage.key) {
                        const revokedKey = message.message.protocolMessage.key
                        log(`🔄 [Direct REVOKE] Processing revoke message for ${revokedKey.id} in chat ${revokedKey.remoteJid || jid}`)
                        
                        try {
                            // Update the revoked message to mark it as deleted/revoked
                            const updateResult = await collections.messages.updateOne(
                                {
                                    instanceId: validatedInstanceId,
                                    jid: revokedKey.remoteJid || jid,
                                    'key.id': revokedKey.id
                                },
                                {
                                    $set: {
                                        'message.protocolMessage': message.message.protocolMessage,
                                        revoked: true,
                                        revokedAt: new Date(),
                                        revokedBy: message.key.fromMe ? 'me' : message.key.participant || message.key.remoteJid
                                    }
                                }
                            )
                            
                            if (updateResult.matchedCount > 0) {
                                log(`✅ [Direct REVOKE] Successfully marked message ${revokedKey.id} as revoked`)
                            } else {
                                log(`⚠️ [Direct REVOKE] Message ${revokedKey.id} not found to revoke`)
                            }
                        } catch (error) {
                            logError(`[Direct REVOKE] Failed to process revoke for message ${revokedKey.id}:`, error)
                        }
                        
                        // Skip storing the REVOKE message itself
                        return
                    }
                    
                    // Skip other protocol message types that shouldn't be stored
                    const skipTypes = [
                        proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION,
                        proto.Message.ProtocolMessage.Type.APP_STATE_SYNC_KEY_SHARE,
                        proto.Message.ProtocolMessage.Type.INITIAL_SECURITY_NOTIFICATION_SETTING_SYNC,
                        proto.Message.ProtocolMessage.Type.APP_STATE_SYNC_KEY_REQUEST
                    ]
                    
                    if (protoType && skipTypes.includes(protoType)) {
                        log(`⏭️ [Direct Protocol] Skipping protocol message of type ${protoType} for instance ${validatedInstanceId}`)
                        return
                    }
                }
                
                const validJid = validateJID(jid)
                
                // Validate message ID if present
                if (message.key?.id) {
                    validateMessageId(message.key.id)
                }
                
                // Check write permissions
                accessContext.validateAccess(validatedInstanceId, 'write')
                
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
                    
                    // Invalidate cache
                    const cacheKey = `msg_${validatedInstanceId}_${hashForLogging(validJid)}_${message.key?.id ? hashForLogging(message.key.id) : ''}`
                    binaryConversionCache.del(cacheKey)
                    return
                } catch (error) {
                    logError('[Bull Messages] Failed to queue, falling back:', error)
                }
            }
            
            // Fallback to existing batch/direct processing
            if (useBatch) {
                // Add to batch for processing
                messageBatch.items.push({ ...message, jid: validJid })
                scheduleMessageBatch()
                
                // Force process if batch is full
                if (messageBatch.items.length >= BATCH_SIZE) {
                    await processBatchedMessages()
                }
            } else {
                // Invalidate cache for this message
                const cacheKey = `msg_${validatedInstanceId}_${hashForLogging(validJid)}_${message.key?.id ? hashForLogging(message.key.id) : ''}`
                binaryConversionCache.del(cacheKey)
                
                await pMessageQueue.add(async () => {
                    try {
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
                    } catch (error: any) {
                        // Handle duplicate key errors gracefully
                        if (error.code === 11000 || error.message?.includes('duplicate key')) {
                            log(`⚠️ [Direct] Duplicate key error for message ${message.key?.id} in chat ${validJid} - message already exists`)
                            // Try to update instead of replace
                            try {
                                await collections.messages.updateOne(
                                    {
                                        instanceId: validatedInstanceId,
                                        jid: validJid,
                                        'key.id': message.key?.id
                                    },
                                    {
                                        $set: {
                                            ...message,
                                            updatedAt: new Date()
                                        }
                                    }
                                )
                                log(`✅ [Direct] Successfully updated existing message ${message.key?.id} after duplicate key error`)
                            } catch (updateError) {
                                logError(`[Direct] Failed to update message after duplicate key error:`, updateError)
                                throw updateError
                            }
                        } else {
                            throw error
                        }
                    }
                })
            }
            } catch (error) {
                if (error instanceof ValidationError || error instanceof AuthorizationError) {
                    throw error
                }
                logError(createSafeErrorMessage(error as Error, 'upsertMessage'))
                throw new Error(createSafeErrorMessage(error as Error, 'upsertMessage'))
            }
        },

        async updateMessage(jid: string, id: string, update: Partial<proto.IWebMessageInfo>): Promise<boolean> {
            // Invalidate cache for this message
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
            try {
                const validJid = validateJID(jid)
                
                // Validate message IDs if provided
                const validIds = ids?.map(id => validateMessageId(id))
                
                // Check delete permissions
                accessContext.validateAccess(validatedInstanceId, 'delete')
                
                // Clear cache for deleted messages
                if (validIds && validIds.length > 0) {
                    validIds.forEach(id => {
                        const cacheKey = `msg_${validatedInstanceId}_${hashForLogging(validJid)}_${hashForLogging(id)}`
                        binaryConversionCache.del(cacheKey)
                    })
                } else {
                    // Clear all cache entries for this jid if deleting all messages
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
            // Ensure metadata has an id field
            if (!metadata.id) {
                config.logger?.error({ instanceId, jid, metadata }, 'GroupMetadata missing id field')
                throw new Error(`GroupMetadata missing id field for jid: ${jid}`)
            }
            
            console.log(`[${instanceId}] upsertGroupMetadata called for group ${metadata.id}`)
            console.log(`[${instanceId}] Bull initialized: ${bullInitialized}, Has queue: ${queues.has(QueueType.GROUP_METADATA)}`)
            config.logger?.debug({ instanceId, groupId: metadata.id, jid }, 'Upserting group metadata')
            
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.GROUP_METADATA)) {
                try {
                    console.log(`[${instanceId}] Using Bull queue for group ${metadata.id}`)
                    const queue = queues.get(QueueType.GROUP_METADATA)!
                    const job = await queue.add(
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
                    console.log(`[${instanceId}] Job queued with ID: ${job.id} for group ${metadata.id}`)
                    config.logger?.debug({ instanceId, groupId: metadata.id }, 'Group metadata queued for processing')
                    return
                } catch (error) {
                    console.error(`[${instanceId}] Bull queue failed for group ${metadata.id}:`, error)
                    logError('[Bull GroupMetadata] Failed to queue, falling back:', error)
                }
            }
            
            // Fallback to direct write
            console.log(`[${instanceId}] Using direct MongoDB write for group ${metadata.id}`)
            try {
                const result = await collections.groupMetadata.replaceOne(
                    { instanceId, id: metadata.id },
                    { ...metadata, instanceId, updatedAt: new Date() },
                    { upsert: true }
                )
                console.log(`[${instanceId}] Direct write result for group ${metadata.id}: upserted=${result.upsertedCount}, modified=${result.modifiedCount}`)
                config.logger?.info({ instanceId, groupId: metadata.id, upserted: result.upsertedCount, modified: result.modifiedCount }, 'Group metadata saved directly to MongoDB')
            } catch (error) {
                console.error(`[${instanceId}] Direct MongoDB write failed for group ${metadata.id}:`, error)
                throw error
            }
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
            await withConnection(() => collections.state.updateOne(
                { instanceId },
                { 
                    $set: { ...update, instanceId, updatedAt: new Date() }
                },
                { upsert: true }
            ))
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
            // If Bull queue is available, use it
            if (bullInitialized && queues.has(QueueType.LABEL_ASSOCIATIONS)) {
                try {
                    const queue = queues.get(QueueType.LABEL_ASSOCIATIONS)!
                    
                    // Generate unique job ID for deduplication
                    const messageId = 'messageId' in association ? association.messageId : undefined
                    const jobId = `${association.labelId}-${association.chatId}${messageId ? `-${messageId}` : ''}`
                    const uniqueJobId = `upsert-${jobId}`
                    
                    // Check for existing conflicting jobs and remove only truly conflicting ones
                    const existingJobs = await queue.getJobs(['waiting', 'delayed'])
                    const conflictingJobs = existingJobs.filter((job: Job<LabelAssociationJob>) => {
                        const jobData = job.data as LabelAssociationJob
                        // Skip jobs without association data (like cleanup jobs)
                        if (!jobData || !jobData.association) return false
                        const jobMessageId = 'messageId' in jobData.association ? jobData.association.messageId : undefined
                        
                        // Only consider it conflicting if:
                        // 1. Same labelId, chatId, and type
                        // 2. Same messageId (or both are undefined)
                        // 3. Same operation type (both upsert or both delete)
                        return jobData.association.labelId === association.labelId &&
                               jobData.association.chatId === association.chatId &&
                               jobData.association.type === association.type &&
                               jobData.type === 'upsert' && // Current is upsert, only conflict with other upserts
                               ((!messageId && !jobMessageId) || (messageId === jobMessageId))
                    })
                    
                    // Remove truly conflicting jobs (only recent duplicates)
                    let removedCount = 0
                    for (const conflictingJob of conflictingJobs as Job<LabelAssociationJob>[]) {
                        // Only remove if the job is very recent (within last 10 seconds) to avoid removing legitimate queued operations
                        const jobAge = Date.now() - (conflictingJob.opts?.timestamp || conflictingJob.processedOn || Date.now())
                        if (jobAge < 10000) { // 10 seconds
                            await conflictingJob.remove()
                            removedCount++
                            log(`[Bull Label] Removed recent duplicate job ${conflictingJob.id} (age: ${jobAge}ms) for ${jobId}`)
                        }
                    }
                    
                    if (conflictingJobs.length > 0) {
                        log(`[Bull Label] Found ${conflictingJobs.length} potential conflicts, removed ${removedCount} recent duplicates for ${jobId}`)
                    }
                    
                    // Add the new job with unique ID
                    const job = await queue.add(
                        uniqueJobId,
                        {
                            type: 'upsert',
                            association,
                            instanceId,
                            timestamp: Date.now(),
                            operationId: `${Date.now()}-${Math.random().toString(36).substring(2, 11)}`
                        },
                        {
                            ...defaultJobOptions,
                            jobId: uniqueJobId
                        }
                    )
                    
                    labelAssociationBatch.totalReceived = (labelAssociationBatch.totalReceived || 0) + 1
                    log(`[Bull Label] Queued upsert job ${job.id} for ${jobId}`)
                    return
                } catch (error) {
                    logError('[Bull Label] Failed to queue job, falling back to in-memory:', error)
                    // Fall through to in-memory processing
                }
            }
            
            // Use memory-aware processor if available
            if (labelAssociationProcessor) {
                labelAssociationBatch.totalReceived = (labelAssociationBatch.totalReceived || 0) + 1
                log(`[Memory-Aware] Adding label association to processor - chatId: ${association.chatId}, labelId: ${association.labelId}`)
                return labelAssociationProcessor.add([association])
            }
            
            // Fallback to in-memory batch processing
            return new Promise((resolve, reject) => {
                // Track total received
                labelAssociationBatch.totalReceived = (labelAssociationBatch.totalReceived || 0) + 1
                
                // Add to batch queue
                labelAssociationBatch.items.push(association)
                labelAssociationBatch.pendingPromises?.push({ resolve, reject })
                
                const currentBatchSize = labelAssociationBatch.items.length
                const totalReceived = labelAssociationBatch.totalReceived
                const totalProcessed = labelAssociationBatch.totalProcessed || 0
                
                log(`[Label Association] #${totalReceived} Added to batch (queue: ${currentBatchSize}, received: ${totalReceived}, processed: ${totalProcessed}) - chatId: ${association.chatId}, labelId: ${association.labelId}, messageId: ${(association as any).messageId || 'none'}`)
                
                // Process immediately if batch is full
                if (currentBatchSize >= BATCH_SIZE) {
                    log(`[Label Association] Batch full (${currentBatchSize}/${BATCH_SIZE}), processing immediately`)
                    // Cancel any pending timer before processing
                    if (labelAssociationBatch.timer) {
                        clearTimeout(labelAssociationBatch.timer)
                        labelAssociationBatch.timer = null
                    }
                    processBatchedLabelAssociations().catch(error => {
                        logError('[Label Association] Error in batch processing:', error)
                    })
                } else {
                    // Schedule batch processing after timeout
                    scheduleLabelBatch()
                }
            })
        },

        async deleteLabelAssociation(association: LabelAssociation): Promise<void> {
            // If Bull queue is available, use it
            if (bullInitialized && queues.has(QueueType.LABEL_ASSOCIATIONS)) {
                try {
                    const queue = queues.get(QueueType.LABEL_ASSOCIATIONS)!
                    
                    // Generate unique job ID for deduplication
                    const messageId = 'messageId' in association ? association.messageId : undefined
                    const jobId = `${association.labelId}-${association.chatId}${messageId ? `-${messageId}` : ''}`
                    const uniqueJobId = `delete-${jobId}`
                    
                    // Check for existing conflicting jobs and remove them
                    const existingJobs = await queue.getJobs(['waiting', 'delayed'])
                    const conflictingJobs = existingJobs.filter(job => {
                        const jobData = job.data as LabelAssociationJob
                        const jobMessageId = 'messageId' in jobData.association ? jobData.association.messageId : undefined
                        return jobData.association.labelId === association.labelId &&
                               jobData.association.chatId === association.chatId &&
                               ((!messageId && !jobMessageId) ||
                                (messageId === jobMessageId))
                    })
                    
                    // Remove conflicting jobs
                    for (const conflictingJob of conflictingJobs) {
                        await conflictingJob.remove()
                        log(`[Bull Label] Removed conflicting job ${conflictingJob.id} for ${jobId}`)
                    }
                    
                    // Add the new job with unique ID
                    const job = await queue.add(
                        uniqueJobId,
                        {
                            type: 'delete',
                            association,
                            instanceId,
                            timestamp: Date.now(),
                            operationId: `${Date.now()}-${Math.random().toString(36).substring(2, 11)}`
                        },
                        {
                            ...defaultJobOptions,
                            jobId: uniqueJobId
                        }
                    )
                    
                    log(`[Bull Label] Queued delete job ${job.id} for ${jobId}`)
                    return
                } catch (error) {
                    logError('[Bull Label] Failed to queue delete job, falling back to direct deletion:', error)
                    // Fall through to direct deletion
                }
            }
            
            // Direct deletion (fallback)
            const filter: any = {
                instanceId,
                chatId: association.chatId,
                labelId: association.labelId
            }
            
            // Only add messageId to filter if it exists in the association
            if ('messageId' in association && association.messageId) {
                filter.messageId = association.messageId
            }
            
            const result = await collections.labelAssociations.deleteOne(filter)
            
            if (result.deletedCount === 0) {
                logWarn(`[Direct Delete] Warning: No label association found to delete - chatId: ${association.chatId}, labelId: ${association.labelId}, messageId: ${(association as any).messageId || 'none'}`)
            }
        },

        bind(ev: BaileysEventEmitter): void {
            console.log(`[${instanceId}] store.bind() called - setting up event listeners`)
            
            ev.on('connection.update', async update => {
                await store.updateState(update)
            })

            ev.on('messaging-history.set', async ({ chats: newChats, contacts: newContacts, messages: newMessages, isLatest }) => {
                // Clear all data if isLatest is true and clearAllOnHistorySync is enabled
                if (isLatest && config.clearAllOnHistorySync) {
                    console.log(`[${instanceId}] Clearing all data before syncing latest history (isLatest=true, clearAllOnHistorySync=true)`)
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
                    // Also delete all associations for this label
                    const deleteResult = await collections.labelAssociations.deleteMany({
                        instanceId,
                        labelId: label.id
                    })
                    if (deleteResult.deletedCount > 0) {
                        log(`Deleted ${deleteResult.deletedCount} label associations for deleted label ${label.id}`)
                    }
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
                
                // For label associations, check if we should flush periodically
                const stats = store.getPerformanceStats()
                if (stats.labelStats && stats.labelStats.totalReceived % 50 === 0 && stats.labelStats.totalReceived > 0) {
                    log(`[Label Event] Periodic status - received: ${stats.labelStats.totalReceived}, processed: ${stats.labelStats.totalProcessed}, queued: ${stats.labelStats.currentQueueSize}`)
                    // If too many are queued, force a flush
                    if (stats.labelStats.currentQueueSize > BATCH_SIZE * 2) {
                        log('[Label Event] Queue backlog detected, forcing flush')
                        store.flushLabelAssociations().catch(err => logError('[Label Event] Flush error:', err))
                    }
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
                    let jid = jidNormalizedUser(msg.key.remoteJid!)
                    
                    // Process LID if handler is available
                    if (lidHandler) {
                        const { normalizedJid, lidInfo } = await lidHandler.processMessage(msg)
                        
                        // Use normalized JID (phone number) for storage
                        jid = jidNormalizedUser(normalizedJid)
                        
                        // Store LID info in the message for reference
                        if (lidInfo.lid || lidInfo.phoneNumber) {
                            (msg as any).lidMapping = {
                                lid: lidInfo.lid,
                                phoneNumber: lidInfo.phoneNumber,
                                originalJid: msg.key.remoteJid
                            }
                        }
                        
                        // Log LID mapping if discovered
                        if (lidInfo.mappingStored) {
                            log(`[LID Handler] Discovered mapping: ${lidInfo.lid} -> ${lidInfo.phoneNumber}`)
                        }
                    }

                    // Persist pushName to contacts.notify if available and not from me,
                    // without overriding existing notify or name
                    try {
                        const pushName = (msg as any)?.pushName
                        if (pushName && !msg.key.fromMe && jid && jid.endsWith('@s.whatsapp.net')) {
                            // Route via CONTACTS queue to serialize writes
                            if (bullInitialized && queues.has(QueueType.CONTACTS)) {
                                const queue = queues.get(QueueType.CONTACTS)!
                                await queue.add(
                                    'update',
                                    {
                                        type: 'update',
                                        contact: { id: jid, notify: pushName },
                                        instanceId,
                                        timestamp: Date.now()
                                    },
                                    defaultJobOptions
                                )
                            } else {
                                const filter: any = {
                                    instanceId,
                                    id: jid,
                                    $or: [
                                        { notify: { $exists: false } },
                                        { notify: { $in: [null, ''] } }
                                    ]
                                }
                                try {
                                    await collections.contacts.updateOne(
                                        filter,
                                        {
                                            $set: { notify: pushName, updatedAt: new Date() },
                                            $setOnInsert: { instanceId, id: jid }
                                        },
                                        { upsert: true }
                                    )
                                } catch (e: any) {
                                    if (e?.code === 11000) {
                                        await collections.contacts.updateOne(
                                            filter,
                                            { $set: { notify: pushName, updatedAt: new Date() } },
                                            { upsert: false }
                                        )
                                    } else {
                                        throw e
                                    }
                                }
                            }
                        }
                    } catch (err) {
                        log(`Failed to persist pushName for ${jid}: ${String(err)}`)
                    }
                    
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
                console.log(`[${instanceId}] groups.update event received with ${updates.length} updates`)
                config.logger?.info({ instanceId, count: updates.length }, 'Processing groups.update event')
                for (const update of updates) {
                    // Check if this is a complete metadata or just an update
                    if (update.participants && Array.isArray(update.participants)) {
                        // This is complete metadata, save it directly
                        config.logger?.debug({ instanceId, groupId: update.id }, 'Saving complete group metadata from groups.update')
                        await store.upsertGroupMetadata(update.id!, update as GroupMetadata)
                    } else {
                        // This is a partial update, merge with existing metadata if available
                        const existingMetadata = await store.getGroupMetadata(update.id!)
                        if (existingMetadata) {
                            config.logger?.debug({ instanceId, groupId: update.id }, 'Merging group update with existing metadata')
                            Object.assign(existingMetadata, update)
                            await store.upsertGroupMetadata(update.id!, existingMetadata)
                        } else {
                            // No existing metadata, but we have an update
                            // Create a minimal metadata object with the update
                            config.logger?.debug({ instanceId, groupId: update.id }, 'Creating new group metadata from update')
                            const newMetadata: GroupMetadata = {
                                id: update.id!,
                                subject: update.subject || '',
                                participants: [],
                                ...update
                            } as GroupMetadata
                            await store.upsertGroupMetadata(update.id!, newMetadata)
                        }
                    }
                }
            })

            ev.on('groups.upsert', async groups => {
                console.log(`[${instanceId}] groups.upsert event received with ${groups.length} groups`)
                config.logger?.info({ instanceId, count: groups.length }, 'Processing groups.upsert event')
                for (const group of groups) {
                    await store.upsertGroupMetadata(group.id, group)
                    config.logger?.debug({ instanceId, groupId: group.id }, 'Group metadata upserted')
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
                    .then(msgs => msgs.map(({ _id, instanceId: _instanceId, jid: _jid, updatedAt: _updatedAt, ...msg }) => convertBinaryToBuffer(msg)))
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
            
            // Note: Group metadata is excluded from clearAll() as it's persistent data
            await Promise.all([
                collections.chats.deleteMany({ instanceId }),
                collections.contacts.deleteMany({ instanceId }),
                collections.messages.deleteMany({ instanceId }),
                collections.presences.deleteMany({ instanceId }),
                collections.labels.deleteMany({ instanceId }),
                collections.labelAssociations.deleteMany({ instanceId })
                // Removed: collections.groupMetadata.deleteMany({ instanceId })
            ])
        },

        getPerformanceStats(): PerformanceMetrics & { uptime: number; labelStats?: any; bullStats?: any; memoryStats?: any } {
            const uptime = Date.now() - performanceMetrics.lastResetTime.getTime()
            const stats: any = {
                ...performanceMetrics,
                uptime,
                labelStats: {
                    totalReceived: labelAssociationBatch.totalReceived || 0,
                    totalProcessed: labelAssociationBatch.totalProcessed || 0,
                    currentQueueSize: labelAssociationBatch.items.length,
                    isProcessing: labelAssociationBatch.processing
                }
            }
            
            // Add memory stats if memory monitor is available
            if (memoryMonitor) {
                const memoryMetrics = memoryMonitor.getCurrentMemory()
                const batchMetrics = labelAssociationProcessor?.getMetrics()
                stats.memoryStats = {
                    heapUsedMB: Math.round(memoryMetrics.heapUsed / 1024 / 1024),
                    heapTotalMB: Math.round(memoryMetrics.heapTotal / 1024 / 1024),
                    rssMB: Math.round(memoryMetrics.rss / 1024 / 1024),
                    memoryPressure: backpressureController ? backpressureController.getPressure() : 0,
                    batchMetrics: batchMetrics || undefined
                }
            }
            
            // Add Bull queue stats if available
            if (bullInitialized) {
                const queueStats: any = {}
                for (const [queueType] of queues.entries()) {
                    queueStats[queueType] = 'active'
                }
                
                stats.bullStats = {
                    initialized: true,
                    queues: queueStats,
                    totalQueues: queues.size,
                    redisConnected: redisConnection?.status === 'ready'
                }
            } else {
                stats.bullStats = {
                    initialized: false,
                    reason: redis ? 'initialization failed' : 'not configured'
                }
            }
            
            return stats
        },
        
        async flushLabelAssociations(): Promise<void> {
            // Use memory-aware processor if available
            if (labelAssociationProcessor) {
                log(`[Memory-Aware Flush] Forcing flush of label associations`)
                await labelAssociationProcessor.flush()
                const metrics = labelAssociationProcessor.getMetrics()
                log(`[Memory-Aware Flush] Complete. Processed: ${metrics.itemsProcessed} items in ${metrics.batchesProcessed} batches`)
                return
            }
            
            log(`[Label Flush] Forcing flush of ${labelAssociationBatch.items.length} pending label associations`)
            
            // Cancel any pending timers
            if (labelAssociationBatch.timer) {
                clearTimeout(labelAssociationBatch.timer)
                labelAssociationBatch.timer = null
            }
            
            // Wait for any current processing to complete
            while (labelAssociationBatch.processing) {
                log('[Label Flush] Waiting for current batch to complete...')
                await new Promise(resolve => setTimeout(resolve, 50))
            }
            
            // Process all remaining items
            while (labelAssociationBatch.items.length > 0) {
                await processBatchedLabelAssociations()
                // Small delay to ensure processing completes
                await new Promise(resolve => setTimeout(resolve, 10))
            }
            
            log(`[Label Flush] Flush complete. Total processed: ${labelAssociationBatch.totalProcessed}/${labelAssociationBatch.totalReceived}`)
        },
        
        resetPerformanceStats(): void {
            performanceMetrics.messagesProcessed = 0
            performanceMetrics.labelsProcessed = 0
            performanceMetrics.batchesProcessed = 0
            performanceMetrics.errors = 0
            performanceMetrics.lastResetTime = new Date()
        },
        
        async recreateIndexes(): Promise<{ created: number; failed: number; details: string[] }> {
            return withConnection(async () => {
                let totalCreated = 0
                let totalFailed = 0
                const allDetails: string[] = []
                
                // Define index definitions (same as in createIndexes)
                const ttlSeconds = ttlDays * 24 * 60 * 60
                const indexDefinitions: Record<string, IndexSpec[]> = {
                    chats: [
                        { name: 'chats_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } },
                        { name: 'chats_ttl', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds } }
                    ],
                    contacts: [
                        { name: 'contacts_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } },
                        { name: 'contacts_lid_lookup', spec: { instanceId: 1, lid: 1 }, options: { unique: true, partialFilterExpression: { lid: { $type: 'string' } } } }
                    ],
                    messages: [
                        { name: 'messages_primary', spec: { instanceId: 1, jid: 1, 'key.id': 1 }, options: { unique: true } },
                        { name: 'messages_jid_timestamp', spec: { instanceId: 1, jid: 1, messageTimestamp: -1 }, options: {} },
                        { name: 'messages_ttl', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds } }
                    ],
                    groupMetadata: [
                        { name: 'groupMetadata_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } }
                    ],
                    state: [
                        { name: 'state_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } }
                    ],
                    presences: [
                        { name: 'presences_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } },
                        { name: 'presences_ttl', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds } }
                    ],
                    labels: [
                        { name: 'labels_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } }
                    ],
                    labelAssociations: [
                        { name: 'labelAssociations_primary', spec: { instanceId: 1, chatId: 1, messageId: 1, labelId: 1 }, options: { unique: true } },
                        { name: 'labelAssociations_chatId_labelId', spec: { instanceId: 1, chatId: 1, labelId: 1 }, options: {} },
                        { name: 'labelAssociations_messageId_labelId', spec: { instanceId: 1, messageId: 1, labelId: 1 }, options: {} }
                    ]
                }
                
                // Process each collection
                for (const [collectionName, requiredIndexes] of Object.entries(indexDefinitions)) {
                    try {
                        const collection = collections[collectionName as keyof MongoCollections]
                        
                        // Apply timeout to indexes
                        const timedIndexes = requiredIndexes.map(idx => ({
                            ...idx,
                            options: { ...idx.options, maxTimeMS: indexConfig.indexCreationTimeout }
                        }))
                        
                        const result = await recreateIndexes(collection, timedIndexes)
                        totalCreated += result.successful
                        totalFailed += result.failed
                        
                        result.details.forEach(detail => {
                            allDetails.push(`${collectionName}: ${detail}`)
                        })
                    } catch (error) {
                        const errorMsg = `${collectionName}: Recreation failed - ${error}`
                        allDetails.push(errorMsg)
                        totalFailed += 1
                    }
                }
                
                return { created: totalCreated, failed: totalFailed, details: allDetails }
            })
        },

        async getIndexStatus(): Promise<{ collection: string; indexes: any[] }[]> {
            return withConnection(async () => {
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
            })
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
                    ttlDays: ttlDays,
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
        
        getConnectionMetrics(): any {
            if (!isUsingSharedConnection || !connectionManager) {
                return {
                    type: 'dedicated',
                    poolSize: connectionConfig?.maxPoolSize ?? 100,
                    instanceId: validatedInstanceId
                }
            }
            
            const instanceMetrics = connectionManager.getInstanceMetrics(validatedInstanceId)
            const globalMetrics = connectionManager.getMetrics()
            
            return {
                type: 'shared',
                instanceId: validatedInstanceId,
                tier: instanceMetrics?.tier,
                operationsPerMinute: instanceMetrics?.operationsPerMinute,
                avgResponseTime: instanceMetrics?.avgResponseTime,
                global: {
                    totalConnections: globalMetrics.totalConnections,
                    totalInstances: globalMetrics.instances.total,
                    poolDistribution: globalMetrics.pools,
                    utilizationRate: globalMetrics.utilizationRate
                }
            }
        },
        
        async close(): Promise<void> {
            // Mark as disconnected
            isConnected = false
            reconnectAttempts = 0
            
            // Flush all pending label associations first
            if (store.flushLabelAssociations) {
                await store.flushLabelAssociations()
            } else {
                // Fallback to old method
                await processBatchedLabelAssociations()
            }
            
            // Process any remaining message batches
            await processBatchedMessages()
            
            // Clear all timers
            if (labelAssociationBatch.timer) {
                clearTimeout(labelAssociationBatch.timer)
            }
            if (messageBatch.timer) {
                clearTimeout(messageBatch.timer)
            }
            
            // Close Bull queues if initialized
            if (bullInitialized) {
                console.log(`🛑 Closing Bull queues for instance ${instanceId}...`)
                try {
                    // Clear health check interval
                    if (healthCheckInterval) {
                        clearInterval(healthCheckInterval)
                        healthCheckInterval = null
                    }
                    
                    // Close all workers and remove event listeners
                    for (const worker of workers.values()) {
                        // Remove event listeners if they exist
                        if ((worker as any).__eventHandlers) {
                            const handlers = (worker as any).__eventHandlers
                            worker.removeListener('completed', handlers.completed)
                            worker.removeListener('failed', handlers.failed)
                            worker.removeListener('stalled', handlers.stalled)
                            delete (worker as any).__eventHandlers
                        }
                        await worker.close()
                    }
                    // Close all queues
                    for (const queue of queues.values()) {
                        await queue.close()
                    }
                    // Disconnect Redis
                    if (redisConnection) redisConnection.disconnect()
                } catch (error) {
                    console.error('Error closing Bull queues:', error)
                }
            }
            
            // Wait for all p-queues to finish
            await Promise.all([
                pMessageQueue.onIdle(),
                pLabelQueue.onIdle(),
                generalQueue.onIdle()
            ])
            
            // Clear all cache entries for this instance
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
            
            // Clear LID handler cache if running
            if (lidHandler) {
                lidHandler.clearCache()
                lidHandler = null
            }
            
            // Flush and clean up memory-aware processors
            if (labelAssociationProcessor) {
                await labelAssociationProcessor.flush()
                labelAssociationProcessor = null
            }
            if (messageProcessor) {
                await messageProcessor.flush()
                messageProcessor = null
            }
            
            // Handle connection cleanup based on connection type
            if (isUsingSharedConnection && connectionManager) {
                // Unregister from connection manager
                await connectionManager.unregisterInstance(validatedInstanceId)
                connectionManager = null
            } else if (client && !isUsingSharedConnection) {
                // Close dedicated connection
                await client.close()
                activeConnections = activeConnections.filter(c => c.instanceId !== instanceId)
            }
        }
    }

    // Return the proxied store
    const store = createStoreProxy(storeImpl)
    return store
}

/**
 * Cleanup MongoDB store data for a specific instance
 * @param instanceId - The instance ID to cleanup. If not provided, closes all connections.
 * @param deleteData - Whether to delete all data for the instance (default: false)
 * @param mongoUri - MongoDB connection URI (required if no active connections)
 * @param database - Database name (required if no active connections)
 * @param collectionPrefix - Collection prefix to use (default: 'baileys_')
 */
export const cleanupMongoDBStore = async (instanceId?: string, deleteData: boolean = false, mongoUri?: string, database?: string, collectionPrefix: string = 'baileys_'): Promise<void> => {
    if (!instanceId) {
        // Just close all connections
        const connections = [...activeConnections]
        activeConnections = []
        
        for (const conn of connections) {
            try {
                // Check if client is still connected before closing
                if (conn.client) {
                    try {
                        // Try to ping the database to check if connection is alive
                        await conn.client.db(conn.database).command({ ping: 1 })
                        await conn.client.close()
                    } catch {
                        // Connection already dead, just ensure client is closed
                        try {
                            await conn.client.close()
                        } catch {
                            // Client already closed, ignore
                        }
                    }
                }
            } catch (error) {
                console.error('Error closing MongoDB connection:', error)
            }
        }
        return
    }

    // Validate and normalize the instance ID
    const validatedInstanceId = validateInstanceId(instanceId)
    
    // Find connections for the specific instance
    let instanceConnections = activeConnections.filter(c => c.instanceId === validatedInstanceId)
    
    // If no active connections and deleteData is requested, create a temporary connection
    if (instanceConnections.length === 0 && deleteData && mongoUri && database) {
        console.log(`No active connection found for instance ${instanceId}, creating temporary connection for cleanup`)
        try {
            const tempClient = new MongoClient(mongoUri)
            await tempClient.connect()
            
            instanceConnections = [{
                instanceId: validatedInstanceId,
                client: tempClient,
                database,
                collectionPrefix,
                uri: mongoUri
            }]
            
            // Don't add to activeConnections since this is temporary
        } catch (error) {
            console.error(`Failed to create temporary connection for cleanup of instance ${instanceId}:`, error)
            return
        }
    } else if (instanceConnections.length === 0) {
        console.warn(`No active connections found for instance: ${instanceId} and no connection info provided`)
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
                    `${conn.collectionPrefix}labelAssociations`,
                    `${conn.collectionPrefix}lidMappings`
                ]

                let totalDeleted = 0
                for (const collName of collections) {
                    try {
                        const result = await db.collection(collName).deleteMany({ instanceId: validatedInstanceId })
                        if (result.deletedCount > 0) {
                            console.log(`Deleted ${result.deletedCount} documents from ${collName}`)
                            totalDeleted += result.deletedCount
                        }
                    } catch (error) {
                        console.error(`Error deleting data from ${collName}:`, error)
                    }
                }
                console.log(`Deleted all data for instance: ${instanceId} (${totalDeleted} total documents)`)
            }

            // Close the connection
            await conn.client.close()
            
        } catch (error) {
            console.error(`Error cleaning up instance ${instanceId}:`, error)
        }
    }

    // Remove from active connections
    activeConnections = activeConnections.filter(c => c.instanceId !== validatedInstanceId)
}
