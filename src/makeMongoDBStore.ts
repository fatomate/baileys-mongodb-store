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
        collectionPrefix = 'baileys_',
        redis
    } = config

    let client: MongoClient
    let db: Db
    let isConnected = false
    let isConnecting = false
    let connectionError: Error | null = null
    let reconnectAttempts = 0
    const MAX_RECONNECT_ATTEMPTS = 5
    const RECONNECT_DELAY_BASE = 1000 // 1 second base delay
    
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
            client = new MongoClient(uri, {
                // Optimize connection pool for high concurrency
                maxPoolSize: 100,
                minPoolSize: 10,
                maxIdleTimeMS: 30000,
                // Write concern for better performance
                writeConcern: { w: 1, j: false }
            })
            await client.connect()
            db = client.db(dbName)
            isConnected = true
            isConnecting = false
            connectionError = null
            reconnectAttempts = 0
            
            // Update active connections
            activeConnections = activeConnections.filter(c => c.instanceId !== instanceId)
            activeConnections.push({
                client,
                database: dbName,
                instanceId,
                collectionPrefix
            })
            
            console.log(`MongoDB connected successfully for instance ${instanceId}`)
        } catch (error) {
            isConnected = false
            isConnecting = false
            connectionError = error as Error
            console.error(`MongoDB connection failed for instance ${instanceId}:`, error)
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
            console.log(`Attempting to reconnect (attempt ${reconnectAttempts + 1}/${MAX_RECONNECT_ATTEMPTS}) after ${delay}ms...`)
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
            console.log(`🐂 Initializing Bull queues for instance ${instanceId}...`)
            
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
                    console.warn(`⚠️  Redis eviction policy is '${policy}'. Consider using 'noeviction' for BullMQ or a separate Redis instance.`)
                    console.warn(`   Current settings will work but jobs may be lost if Redis memory fills up.`)
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
                // Label associations MUST have concurrency of 1 to maintain order
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
                
                console.log(`🔧 Created ${queueType} queue with concurrency: ${concurrency}`)
                
                // Set up event handlers
                worker.on('completed', (job) => {
                    // Only log for label associations in debug mode
                    if (queueType !== QueueType.LABEL_ASSOCIATIONS) {
                        console.log(`✅ ${queueType} job ${job.id} completed`)
                    }
                })
                
                worker.on('failed', (job, err) => {
                    console.error(`❌ ${queueType} job ${job?.id} failed:`, err.message)
                    performanceMetrics.errors++
                })
                
                worker.on('stalled', (jobId) => {
                    console.warn(`⚠️ ${queueType} job ${jobId} stalled`)
                })
                
                workers.set(queueType, worker)
                
                // Clean up old jobs on startup
                queue.obliterate({ force: true }).catch(() => {})
                
                // Queue and worker created successfully
            }
            
            // Create queue for LABEL ASSOCIATIONS
            createQueueAndWorker<LabelAssociationJob>(QueueType.LABEL_ASSOCIATIONS, async (job) => {
                const { type, association } = job.data
                
                if (type === 'upsert') {
                    await collections.labelAssociations.replaceOne(
                        {
                            instanceId,
                            chatId: association.chatId,
                            labelId: association.labelId,
                            messageId: 'messageId' in association ? association.messageId : ''
                        },
                        {
                            ...association,
                            instanceId,
                            updatedAt: new Date()
                        },
                        { upsert: true }
                    )
                } else if (type === 'delete') {
                    await collections.labelAssociations.deleteOne({
                        instanceId,
                        chatId: association.chatId,
                        labelId: association.labelId,
                        messageId: 'messageId' in association ? association.messageId : ''
                    })
                }
                
                performanceMetrics.labelsProcessed++
                return { success: true }
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
            
            // Create queue for GROUP METADATA
            createQueueAndWorker<GroupMetadataJob>(QueueType.GROUP_METADATA, async (job) => {
                const { type, jid, metadata, update } = job.data
                
                if (type === 'upsert') {
                    await collections.groupMetadata.replaceOne(
                        { instanceId, id: jid },
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
                        console.error('⚠️ Redis connection lost, attempting to reconnect...')
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
                                console.warn(`⚠️ ${queueType} queue has ${counts.waiting} waiting jobs`)
                            }
                            
                            // Check if worker is running
                            if (worker && !worker.isRunning()) {
                                console.error(`❌ ${queueType} worker stopped, restarting...`)
                                await worker.run()
                            }
                            
                            // Check for stale active jobs
                            const activeJobs = await queue.getActive()
                            const now = Date.now()
                            for (const job of activeJobs) {
                                const processingTime = now - job.processedOn!
                                if (processingTime > QUEUE_STALE_THRESHOLD) {
                                    console.warn(`⚠️ Stale job detected in ${queueType}: ${job.id} (${processingTime}ms)`)
                                    // Move stale job back to waiting
                                    await job.moveToFailed(new Error('Job stale, moving to failed'), false)
                                }
                            }
                        } catch (error) {
                            console.error(`Health check failed for ${queueType}:`, error)
                        }
                    }
                    
                    lastHealthCheck = Date.now()
                } catch (error) {
                    console.error('Queue health check error:', error)
                    // Try to restart if health check completely fails
                    if (Date.now() - lastHealthCheck > QUEUE_STALE_THRESHOLD * 2) {
                        await restartQueues()
                    }
                }
            }
            
            // Function to restart queues
            const restartQueues = async () => {
                console.log('🔄 Restarting Bull queues...')
                
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
                    
                    console.log('✅ Queues restarted successfully')
                } catch (error) {
                    console.error('❌ Failed to restart queues:', error)
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
            console.log(`✅ Bull queues initialized successfully for instance ${instanceId}`)
        } catch (error) {
            console.error(`❌ Failed to initialize Bull queues for instance ${instanceId}:`, error)
            console.log('⚠️  Falling back to in-memory queue processing')
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
    
    // Batch accumulators with tracking
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
    
    // Wrapper for MongoDB operations with automatic reconnection
    const withConnection = async <T>(operation: () => Promise<T>): Promise<T> => {
        try {
            await ensureConnection()
            // Refresh collections after reconnection
            collections = getCollections()
            return await operation()
        } catch (error: any) {
            // If it's a connection error, reset and try once more
            if (error.message?.includes('Client must be connected') || 
                error.message?.includes('Topology is closed') ||
                error.code === 'ECONNREFUSED') {
                console.log(`Connection error detected for instance ${instanceId}, attempting reconnection...`)
                isConnected = false
                await ensureConnection()
                collections = getCollections()
                return await operation()
            }
            throw error
        }
    }

    // Batch processing functions
    const processBatchedLabelAssociations = async () => {
        // Prevent concurrent processing
        if (labelAssociationBatch.processing) {
            console.log(`[Label Batch] Skipping - already processing`)
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
        console.log(`[Label Batch] Processing ${itemsToProcess.length} label associations`)
        
        // Add protection against memory leaks from excessive batch accumulation
        if (itemsToProcess.length > BATCH_SIZE * 10) {
            console.warn(`Batch size exceeded for instance ${instanceId} (${itemsToProcess.length} items), processing first ${BATCH_SIZE * 10} items`)
            itemsToProcess.splice(BATCH_SIZE * 10)
        }
        
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
    
    // Enhanced index creation with retry logic and categorization
    const createIndexes = async () => {
        const ttlSeconds = ttlDays * 24 * 60 * 60
        
        // Define indexes by priority - critical indexes must succeed
        const criticalIndexes = [
            // Primary lookup indexes - essential for query performance
            { collection: 'chats', spec: { instanceId: 1, id: 1 }, options: { unique: true }, name: 'chats_primary' },
            { collection: 'contacts', spec: { instanceId: 1, id: 1 }, options: { unique: true }, name: 'contacts_primary' },
            { collection: 'messages', spec: { instanceId: 1, jid: 1, 'key.id': 1 }, options: { unique: true }, name: 'messages_primary' },
            { collection: 'messages', spec: { instanceId: 1, jid: 1, messageTimestamp: -1 }, options: {}, name: 'messages_query' },
            { collection: 'groupMetadata', spec: { instanceId: 1, id: 1 }, options: { unique: true }, name: 'groups_primary' },
            { collection: 'state', spec: { instanceId: 1 }, options: { unique: true }, name: 'state_primary' },
            { collection: 'presences', spec: { instanceId: 1, id: 1 }, options: { unique: true }, name: 'presences_primary' },
            { collection: 'labels', spec: { instanceId: 1, id: 1 }, options: { unique: true }, name: 'labels_primary' },
            { collection: 'labelAssociations', spec: { instanceId: 1, chatId: 1, labelId: 1, messageId: 1 }, options: { unique: true }, name: 'label_assoc_primary' }
        ]
        
        const optimizationIndexes = [
            // Performance optimization indexes - improve speed but not essential
            { collection: 'labelAssociations', spec: { instanceId: 1, chatId: 1 }, options: {}, name: 'label_assoc_chat' },
            { collection: 'labelAssociations', spec: { instanceId: 1, messageId: 1 }, options: {}, name: 'label_assoc_message' }
        ]
        
        const ttlIndexes = [
            // TTL indexes for automatic cleanup - can be recreated later if needed
            { collection: 'chats', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds }, name: 'chats_ttl' },
            { collection: 'contacts', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds }, name: 'contacts_ttl' },
            { collection: 'messages', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds }, name: 'messages_ttl' },
            { collection: 'groupMetadata', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds }, name: 'groups_ttl' },
            { collection: 'presences', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds }, name: 'presences_ttl' },
            { collection: 'labels', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds }, name: 'labels_ttl' },
            { collection: 'labelAssociations', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds }, name: 'label_assoc_ttl' }
        ]
        
        const createIndexWithRetry = async (indexDef: any, maxRetries = 3): Promise<{ success: boolean; error?: Error }> => {
            const { collection, spec, options, name } = indexDef
            
            for (let attempt = 1; attempt <= maxRetries; attempt++) {
                try {
                    await withConnection(() => collections[collection as keyof MongoCollections].createIndex(spec, options))
                    console.log(`✅ Index created: ${name} (attempt ${attempt})`)
                    return { success: true }
                } catch (error) {
                    const delay = Math.min(1000 * Math.pow(2, attempt - 1), 5000) // Max 5s delay
                    console.warn(`❌ Index creation failed: ${name} (attempt ${attempt}/${maxRetries}):`, error)
                    
                    if (attempt < maxRetries) {
                        console.log(`⏳ Retrying ${name} in ${delay}ms...`)
                        await new Promise(resolve => setTimeout(resolve, delay))
                    } else {
                        return { success: false, error: error as Error }
                    }
                }
            }
            return { success: false }
        }
        
        // Create critical indexes first - these MUST succeed
        console.log(`🔧 Creating critical indexes for instance ${instanceId}...`)
        const criticalResults = await Promise.allSettled(
            criticalIndexes.map(idx => createIndexWithRetry(idx, 5)) // More retries for critical indexes
        )
        
        const failedCritical = criticalResults
            .map((result, i) => ({ result, index: criticalIndexes[i] }))
            .filter(({ result }) => result.status === 'rejected' || (result.status === 'fulfilled' && !result.value.success))
        
        if (failedCritical.length > 0) {
            const errorDetails = failedCritical.map(({ index }) => index.name).join(', ')
            throw new Error(`Critical indexes failed to create: ${errorDetails}. Query performance will be severely impacted. Please check MongoDB permissions and server status.`)
        }
        
        // Create optimization indexes - failures are acceptable but logged
        console.log(`⚡ Creating optimization indexes for instance ${instanceId}...`)
        const optimizationResults = await Promise.allSettled(
            optimizationIndexes.map(idx => createIndexWithRetry(idx, 2))
        )
        
        const failedOptimization = optimizationResults
            .map((result, i) => ({ result, index: optimizationIndexes[i] }))
            .filter(({ result }) => result.status === 'rejected' || (result.status === 'fulfilled' && !result.value.success))
        
        if (failedOptimization.length > 0) {
            console.warn(`⚠️  Some optimization indexes failed: ${failedOptimization.map(({ index }) => index.name).join(', ')}`)
        }
        
        // Create TTL indexes - failures are logged but don't block operation
        console.log(`🗑️  Creating TTL indexes for instance ${instanceId}...`)
        const ttlResults = await Promise.allSettled(
            ttlIndexes.map(idx => createIndexWithRetry(idx, 2))
        )
        
        const failedTTL = ttlResults
            .map((result, i) => ({ result, index: ttlIndexes[i] }))
            .filter(({ result }) => result.status === 'rejected' || (result.status === 'fulfilled' && !result.value.success))
        
        if (failedTTL.length > 0) {
            console.warn(`⚠️  Some TTL indexes failed: ${failedTTL.map(({ index }) => index.name).join(', ')} - automatic data cleanup may not work`)
        }
        
        const totalCreated = criticalIndexes.length + optimizationIndexes.length + ttlIndexes.length - failedOptimization.length - failedTTL.length
        console.log(`✅ Index creation completed for instance ${instanceId}: ${totalCreated}/${criticalIndexes.length + optimizationIndexes.length + ttlIndexes.length} indexes created`)
    }
    
    // Initialize indexes - critical indexes must succeed
    await createIndexes()
    
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
            return withConnection(async () => {
                const chat = await collections.chats.findOne({ instanceId, id: jid })
                if (!chat) return null
                
                // eslint-disable-next-line @typescript-eslint/no-unused-vars
                const { _id, instanceId: _instanceId, updatedAt, ...chatData } = chat
                return chatData as Chat
            })
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
                    console.error('[Bull Chats] Failed to queue, falling back:', error)
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
                    console.error('[Bull Chats] Failed to queue update, falling back:', error)
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
                    console.error('[Bull Chats] Failed to queue delete, falling back:', error)
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
                    console.error('[Bull Contacts] Failed to queue, falling back:', error)
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
            return messages.map(({ _id, instanceId: _instanceId, jid: _jid, updatedAt: _updatedAt, ...msg }) => convertBinaryToBuffer(msg))
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
            
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { _id, instanceId: _instanceId, jid: _jid, updatedAt: _updatedAt, ...msg } = message
            // Convert all MongoDB Binary objects to Buffers and preserve messageContextInfo
            const converted = convertBinaryToBuffer(msg)
            
            // Cache the converted message
            binaryConversionCache.set(cacheKey, converted)
            
            return converted
        },

        async upsertMessage(jid: string, message: proto.IWebMessageInfo, useBatch: boolean = false): Promise<void> {
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.MESSAGES)) {
                try {
                    const queue = queues.get(QueueType.MESSAGES)!
                    await queue.add(
                        'upsert',
                        {
                            type: 'upsert',
                            jid,
                            message,
                            instanceId,
                            timestamp: Date.now()
                        },
                        defaultJobOptions
                    )
                    
                    // Invalidate cache
                    const cacheKey = `msg_${instanceId}_${jid}_${message.key?.id}`
                    binaryConversionCache.del(cacheKey)
                    return
                } catch (error) {
                    console.error('[Bull Messages] Failed to queue, falling back:', error)
                }
            }
            
            // Fallback to existing batch/direct processing
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
                    console.error('[Bull Messages] Failed to queue update, falling back:', error)
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
            
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.MESSAGES)) {
                try {
                    const queue = queues.get(QueueType.MESSAGES)!
                    await queue.add(
                        'delete',
                        {
                            type: 'delete',
                            jid,
                            deleteIds: ids,
                            instanceId,
                            timestamp: Date.now()
                        },
                        defaultJobOptions
                    )
                    return
                } catch (error) {
                    console.error('[Bull Messages] Failed to queue delete, falling back:', error)
                }
            }
            
            // Fallback to direct delete
            const filter: any = { instanceId, jid }
            
            if (ids && ids.length > 0) {
                filter['key.id'] = { $in: ids }
            }
            
            await collections.messages.deleteMany(filter)
        },

        async getGroupMetadata(jid: string): Promise<GroupMetadata | null> {
            const metadata = await collections.groupMetadata.findOne({ instanceId, id: jid })
            if (!metadata) return null
            
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...metadataData } = metadata
            return metadataData as GroupMetadata
        },

        async upsertGroupMetadata(jid: string, metadata: GroupMetadata): Promise<void> {
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
                    console.error('[Bull GroupMetadata] Failed to queue, falling back:', error)
                }
            }
            
            // Fallback to direct write
            await collections.groupMetadata.replaceOne(
                { instanceId, id: jid },
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
                    console.error('[Bull State] Failed to queue, falling back:', error)
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
                    console.error('[Bull Presences] Failed to queue, falling back:', error)
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
                    console.error('[Bull Labels] Failed to queue, falling back:', error)
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
                    console.error('[Bull Labels] Failed to queue delete, falling back:', error)
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
                    const job = await queue.add(
                        'upsert',
                        {
                            type: 'upsert',
                            association,
                            instanceId,
                            timestamp: Date.now()
                        },
                        defaultJobOptions
                    )
                    
                    labelAssociationBatch.totalReceived = (labelAssociationBatch.totalReceived || 0) + 1
                    console.log(`[Bull Label] Job ${job.id} queued - chatId: ${association.chatId}, labelId: ${association.labelId}`)
                    return
                } catch (error) {
                    console.error('[Bull Label] Failed to queue job, falling back to in-memory:', error)
                    // Fall through to in-memory processing
                }
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
                
                console.log(`[Label Association] #${totalReceived} Added to batch (queue: ${currentBatchSize}, received: ${totalReceived}, processed: ${totalProcessed}) - chatId: ${association.chatId}, labelId: ${association.labelId}, messageId: ${(association as any).messageId || 'none'}`)
                
                // Process immediately if batch is full
                if (currentBatchSize >= BATCH_SIZE) {
                    console.log(`[Label Association] Batch full (${currentBatchSize}/${BATCH_SIZE}), processing immediately`)
                    // Cancel any pending timer before processing
                    if (labelAssociationBatch.timer) {
                        clearTimeout(labelAssociationBatch.timer)
                        labelAssociationBatch.timer = null
                    }
                    processBatchedLabelAssociations().catch(error => {
                        console.error('[Label Association] Error in batch processing:', error)
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
                    const job = await queue.add(
                        'delete',
                        {
                            type: 'delete',
                            association,
                            instanceId,
                            timestamp: Date.now()
                        },
                        defaultJobOptions
                    )
                    
                    console.log(`[Bull Label] Delete job ${job.id} queued - chatId: ${association.chatId}, labelId: ${association.labelId}`)
                    return
                } catch (error) {
                    console.error('[Bull Label] Failed to queue delete job, falling back to direct deletion:', error)
                    // Fall through to direct deletion
                }
            }
            
            // Direct deletion (fallback)
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
                
                // For label associations, check if we should flush periodically
                const stats = store.getPerformanceStats()
                if (stats.labelStats && stats.labelStats.totalReceived % 50 === 0 && stats.labelStats.totalReceived > 0) {
                    console.log(`[Label Event] Periodic status - received: ${stats.labelStats.totalReceived}, processed: ${stats.labelStats.totalProcessed}, queued: ${stats.labelStats.currentQueueSize}`)
                    // If too many are queued, force a flush
                    if (stats.labelStats.currentQueueSize > BATCH_SIZE * 2) {
                        console.log('[Label Event] Queue backlog detected, forcing flush')
                        store.flushLabelAssociations().catch(err => console.error('[Label Event] Flush error:', err))
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

        getPerformanceStats(): PerformanceMetrics & { uptime: number; labelStats?: any; bullStats?: any } {
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
            console.log(`[Label Flush] Forcing flush of ${labelAssociationBatch.items.length} pending label associations`)
            
            // Cancel any pending timers
            if (labelAssociationBatch.timer) {
                clearTimeout(labelAssociationBatch.timer)
                labelAssociationBatch.timer = null
            }
            
            // Wait for any current processing to complete
            while (labelAssociationBatch.processing) {
                console.log('[Label Flush] Waiting for current batch to complete...')
                await new Promise(resolve => setTimeout(resolve, 50))
            }
            
            // Process all remaining items
            while (labelAssociationBatch.items.length > 0) {
                await processBatchedLabelAssociations()
                // Small delay to ensure processing completes
                await new Promise(resolve => setTimeout(resolve, 10))
            }
            
            console.log(`[Label Flush] Flush complete. Total processed: ${labelAssociationBatch.totalProcessed}/${labelAssociationBatch.totalReceived}`)
        },
        
        resetPerformanceStats(): void {
            performanceMetrics.messagesProcessed = 0
            performanceMetrics.labelsProcessed = 0
            performanceMetrics.batchesProcessed = 0
            performanceMetrics.errors = 0
            performanceMetrics.lastResetTime = new Date()
        },
        
        async recreateIndexes(): Promise<{ created: number; failed: number; details: string[] }> {
            const results: string[] = []
            try {
                await createIndexes()
                const totalIndexes = 18 // Total number of indexes we try to create
                results.push(`Index recreation completed successfully`)
                return { created: totalIndexes, failed: 0, details: results }
            } catch (error) {
                results.push(`Index recreation failed: ${error}`)
                throw error
            }
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
            
            if (client) {
                await client.close()
            }
            activeConnections = activeConnections.filter(c => c.instanceId !== instanceId)
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
 */
export const cleanupMongoDBStore = async (instanceId?: string, deleteData: boolean = false): Promise<void> => {
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