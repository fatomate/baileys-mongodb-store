import { MongoClient, Collection } from 'mongodb'
import { proto, getAggregateVotesInPollMessage, updateMessageWithReceipt, updateMessageWithReaction } from 'baileys'
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
import { LabelAssociationType } from 'baileys/lib/Types/LabelAssociation'
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
    safeValidateJID,
    safeValidateMessageId, 
    validateInstanceId,
    ValidationError,
    AuthorizationError,
    createSafeErrorMessage,
    hashForLogging
} from './utils/security'
import { InstanceAccessContext, DEFAULT_PERMISSIONS } from './utils/auth'
import { MemoryMonitor, BackpressureController } from './utils/memory'
import { TTLMonitor } from './utils/ttl'
import { downloadMedia, downloadOfficialAPIMedia, cleanupOldMedia, getMediaStats, extractMediaInfo } from './utils/media'
import { LidHandler } from './utils/lidHandler'
import { areJidsEquivalent, isLidAndPhonePair } from './utils/jidUtils'

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
    operationId?: string
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

// Helper function to resolve quoted messages
const resolveQuotedMessage = async (
    message: proto.IWebMessageInfo,
    jid: string,
    collections: MongoCollections,
    instanceId: string,
    log: (...args: any[]) => void
): Promise<proto.IWebMessageInfo | null> => {
    try {
        const extendedText = message.message?.extendedTextMessage
        if (!extendedText?.contextInfo?.stanzaId) {
            return null
        }
        
        const stanzaId = extendedText.contextInfo.stanzaId
        const participant = extendedText.contextInfo.participant || jid
        
        log(`🔍 Resolving quoted message with stanzaId: ${stanzaId} from participant: ${participant}`)
        
        // Fetch the quoted message from the database
        const quotedMsg = await collections.messages.findOne({
            instanceId,
            'key.id': stanzaId,
            $or: [
                { jid: participant },
                { 'key.remoteJid': participant },
                { jid: jid },
                { 'key.remoteJid': jid }
            ]
        }) as any
        
        if (!quotedMsg) {
            log(`⚠️ Quoted message not found for stanzaId: ${stanzaId}`)
            return null
        }
        
        // Extract the relevant message content
        const quotedMessage: any = {}
        
        // Handle different message types - only extract essential fields
        if (quotedMsg.message?.conversation) {
            quotedMessage.conversation = quotedMsg.message.conversation
        } else if (quotedMsg.message?.extendedTextMessage?.text) {
            quotedMessage.conversation = quotedMsg.message.extendedTextMessage.text
        } else if (quotedMsg.message?.imageMessage) {
            // Only extract essential fields to avoid Long/BigInt serialization issues
            quotedMessage.imageMessage = {
                url: quotedMsg.mediaUrl || quotedMsg.message.imageMessage.url,
                caption: quotedMsg.message.imageMessage.caption,
                mimetype: quotedMsg.message.imageMessage.mimetype,
                jpegThumbnail: quotedMsg.message.imageMessage.jpegThumbnail,
                // Keep media key fields for download capability
                mediaKey: quotedMsg.message.imageMessage.mediaKey,
                fileEncSha256: quotedMsg.message.imageMessage.fileEncSha256,
                directPath: quotedMsg.message.imageMessage.directPath,
                fileSha256: quotedMsg.message.imageMessage.fileSha256
            }
        } else if (quotedMsg.message?.videoMessage) {
            quotedMessage.videoMessage = {
                url: quotedMsg.mediaUrl || quotedMsg.message.videoMessage.url,
                caption: quotedMsg.message.videoMessage.caption,
                mimetype: quotedMsg.message.videoMessage.mimetype,
                jpegThumbnail: quotedMsg.message.videoMessage.jpegThumbnail,
                // Keep media key fields for download capability
                mediaKey: quotedMsg.message.videoMessage.mediaKey,
                fileEncSha256: quotedMsg.message.videoMessage.fileEncSha256,
                directPath: quotedMsg.message.videoMessage.directPath,
                fileSha256: quotedMsg.message.videoMessage.fileSha256
            }
        } else if (quotedMsg.message?.audioMessage) {
            quotedMessage.audioMessage = {
                url: quotedMsg.mediaUrl || quotedMsg.message.audioMessage.url,
                mimetype: quotedMsg.message.audioMessage.mimetype,
                ptt: quotedMsg.message.audioMessage.ptt, // voice note flag
                seconds: quotedMsg.message.audioMessage.seconds, // duration
                // Keep media key fields for download capability
                mediaKey: quotedMsg.message.audioMessage.mediaKey,
                fileEncSha256: quotedMsg.message.audioMessage.fileEncSha256,
                directPath: quotedMsg.message.audioMessage.directPath,
                fileSha256: quotedMsg.message.audioMessage.fileSha256
            }
        } else if (quotedMsg.message?.documentMessage) {
            quotedMessage.documentMessage = {
                url: quotedMsg.mediaUrl || quotedMsg.message.documentMessage.url,
                title: quotedMsg.message.documentMessage.title,
                fileName: quotedMsg.message.documentMessage.fileName,
                mimetype: quotedMsg.message.documentMessage.mimetype,
                jpegThumbnail: quotedMsg.message.documentMessage.jpegThumbnail,
                // Keep media key fields for download capability
                mediaKey: quotedMsg.message.documentMessage.mediaKey,
                fileEncSha256: quotedMsg.message.documentMessage.fileEncSha256,
                directPath: quotedMsg.message.documentMessage.directPath,
                fileSha256: quotedMsg.message.documentMessage.fileSha256
            }
        } else if (quotedMsg.message?.documentWithCaptionMessage) {
            // Handle document with caption
            const docMsg = quotedMsg.message.documentWithCaptionMessage.message?.documentMessage
            if (docMsg) {
                quotedMessage.documentWithCaptionMessage = {
                    message: {
                        documentMessage: {
                            url: quotedMsg.mediaUrl || docMsg.url,
                            title: docMsg.title,
                            fileName: docMsg.fileName,
                            mimetype: docMsg.mimetype,
                            jpegThumbnail: docMsg.jpegThumbnail,
                            // Keep media key fields for download capability
                            mediaKey: docMsg.mediaKey,
                            fileEncSha256: docMsg.fileEncSha256,
                            directPath: docMsg.directPath,
                            fileSha256: docMsg.fileSha256
                        }
                    },
                    caption: quotedMsg.message.documentWithCaptionMessage.caption
                }
            }
        } else if (quotedMsg.message?.stickerMessage) {
            quotedMessage.stickerMessage = {
                url: quotedMsg.mediaUrl || quotedMsg.message.stickerMessage.url,
                mimetype: quotedMsg.message.stickerMessage.mimetype,
                isAnimated: quotedMsg.message.stickerMessage.isAnimated,
                // Keep media key fields for download capability
                mediaKey: quotedMsg.message.stickerMessage.mediaKey,
                fileEncSha256: quotedMsg.message.stickerMessage.fileEncSha256,
                directPath: quotedMsg.message.stickerMessage.directPath,
                fileSha256: quotedMsg.message.stickerMessage.fileSha256
            }
        } else {
            // For any other message type, copy the entire message object
            Object.assign(quotedMessage, quotedMsg.message)
        }
        
        // Add messageContextInfo if present
        if (!quotedMessage.messageContextInfo) {
            quotedMessage.messageContextInfo = {}
        }
        
        log(`✅ Resolved quoted message type: ${Object.keys(quotedMessage)[0]}`)
        
        // Return the quoted message in the expected format
        return {
            key: quotedMsg.key,
            message: quotedMessage,
            messageTimestamp: quotedMsg.messageTimestamp
        } as proto.IWebMessageInfo
    } catch (error) {
        log(`❌ Error resolving quoted message: ${error instanceof Error ? error.message : 'Unknown error'}`)
        return null
    }
}

// Helper function to decrypt poll votes
const decryptPollVote = async (
    message: proto.IWebMessageInfo,
    collections: MongoCollections,
    instanceId: string,
    meId: string | undefined,
    log: (...args: any[]) => void
): Promise<any> => {
    try {
        // Check if this is a poll vote message
        if (!message.message?.pollUpdateMessage) {
            return null
        }
        
        const pollUpdate = message.message.pollUpdateMessage
        const pollKey = pollUpdate.pollCreationMessageKey
        
        if (!pollKey?.remoteJid || !pollKey?.id) {
            log(`⚠️ Poll vote message missing poll creation key`)
            return null
        }
        
        log(`🗳️ Decrypting poll vote for poll message: ${pollKey.id}`)
        
        // Fetch the original poll creation message
        const originalPoll = await collections.messages.findOne({
            instanceId,
            'key.id': pollKey.id,
            $or: [
                { jid: pollKey.remoteJid },
                { 'key.remoteJid': pollKey.remoteJid }
            ]
        }) as any
        
        if (!originalPoll) {
            log(`⚠️ Original poll message not found for ID: ${pollKey.id}`)
            return null
        }
        
        // Check if poll creation message exists
        if (!originalPoll.message?.pollCreationMessage && 
            !originalPoll.message?.pollCreationMessageV2 && 
            !originalPoll.message?.pollCreationMessageV3) {
            log(`⚠️ Missing poll creation message (v1/v2/v3) for poll: ${pollKey.id}`)
            return null
        }
        
        // Get the poll encryption key from the original message
        let pollEncKey: any = originalPoll.message?.messageContextInfo?.messageSecret
        
        if (!pollEncKey) {
            log(`⚠️ No encryption key found for poll: ${pollKey.id}`)
            return null
        }
        
        // Convert MongoDB Binary object to Buffer if needed
        if (pollEncKey) {
            if (pollEncKey.buffer && pollEncKey._bsontype === 'Binary') {
                // MongoDB Binary object - extract the actual buffer
                pollEncKey = Buffer.from(pollEncKey.buffer)
            } else if (pollEncKey.type === 'Buffer' && Array.isArray(pollEncKey.data)) {
                // JSON-serialized Buffer format (most common from MongoDB)
                const tempBuffer = Buffer.from(pollEncKey.data)
                // Check if this is actually a base64 string stored as bytes
                const asString = tempBuffer.toString('ascii')
                if (asString.match(/^[A-Za-z0-9+/]+=*$/)) {
                    // It's a base64 string, decode it
                    pollEncKey = Buffer.from(asString, 'base64')
                } else {
                    // It's raw binary data
                    pollEncKey = tempBuffer
                }
            } else if (pollEncKey.data && Array.isArray(pollEncKey.data)) {
                // Sometimes stored as an array of bytes
                pollEncKey = Buffer.from(pollEncKey.data)
            } else if (typeof pollEncKey === 'string') {
                // If stored as base64 string
                pollEncKey = Buffer.from(pollEncKey, 'base64')
            } else if (Buffer.isBuffer(pollEncKey)) {
                // Already a Buffer, use as is
            } else {
                log(`⚠️ Unknown encryption key format for poll: ${pollKey.id}`)
                return null
            }
        }
        
        // Determine the poll creator JID
        const pollCreatorJid = originalPoll.key?.participant || 
                               (originalPoll.key?.fromMe ? originalPoll.key?.remoteJid : pollKey.participant) || 
                               originalPoll.key?.remoteJid
        
        // Decrypt the poll vote using Baileys function
        const decryptedVotes = getAggregateVotesInPollMessage({
            message: message.message,
            pollEncKey,
            meId: meId || undefined
        } as any)
        
        // Get poll options for reference
        const pollMessage = originalPoll.message?.pollCreationMessage || 
                          originalPoll.message?.pollCreationMessageV2 || 
                          originalPoll.message?.pollCreationMessageV3
        const pollOptions = pollMessage?.options || []
        
        // Map selected option names
        const selectedOptions = decryptedVotes.map((vote: any) => {
            const optionName = vote.name || vote
            const option = pollOptions.find((opt: any) => opt.optionName === optionName)
            return {
                name: optionName,
                // Include option index if found
                index: option ? pollOptions.indexOf(option) : -1
            }
        })
        
        log(`✅ Decrypted poll vote: ${selectedOptions.map((o: any) => o.name).join(', ')}`)
        
        // Extract vote names from decrypted votes
        const voteNames = decryptedVotes.map((vote: any) => vote.name || vote)
        
        return {
            pollMessageId: pollKey.id,
            pollCreatorJid,
            votes: voteNames, // Array of selected option names
            votesDetailed: selectedOptions, // Array with option names and indices
            voterJid: message.key?.participant || message.key?.remoteJid,
            timestamp: message.messageTimestamp,
            pollQuestion: pollMessage?.name || 'Unknown Poll'
        }
    } catch (error) {
        log(`❌ Error decrypting poll vote: ${error instanceof Error ? error.message : 'Unknown error'}`)
        return null
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
        ttlMonitoring,
        meId,
        lidHandler: lidHandlerConfig
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
    
    // LID handler will be initialized after DB connection
    let lidHandler: LidHandler | null = null

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
    
    // Initialize LID handler after DB connection
    if (lidHandlerConfig) {
        lidHandler = new LidHandler(validatedInstanceId, lidHandlerConfig)
        await lidHandler.initialize(db, collectionPrefix)
        log(`[LID Handler] Initialized for instance ${validatedInstanceId}`)
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
                const { type, message, messageId, update, deleteIds, jid } = job.data
                
                if (type === 'upsert' && message) {
                    // Note: JID normalization is done in upsertMessage before queuing
                    // The jid here is already normalized through LID handler
                    
                    // Resolve quoted message if present
                    if (message.message?.extendedTextMessage?.contextInfo?.stanzaId && 
                        (!message.message.extendedTextMessage.contextInfo.quotedMessage || 
                         Object.keys(message.message.extendedTextMessage.contextInfo.quotedMessage).length === 0)) {
                        
                        const quotedMsg = await resolveQuotedMessage(message, jid, collections, instanceId, log)
                        if (quotedMsg && quotedMsg.message) {
                            // Update the message with the resolved quoted content
                            message.message.extendedTextMessage.contextInfo.quotedMessage = quotedMsg.message
                            log(`✅ [Bull Queue] Updated message with resolved quoted content for ${message.key?.id}`)
                        }
                    }
                    
                    // Decrypt poll vote if present
                    let pollVoteDecrypted = null
                    if (message.message?.pollUpdateMessage) {
                        pollVoteDecrypted = await decryptPollVote(message, collections, instanceId, meId, log)
                        if (pollVoteDecrypted) {
                            log(`✅ [Bull Queue] Decrypted poll vote for ${message.key?.id}`)
                        }
                    }
                    
                    // Check if this is a MESSAGE_EDIT
                    let preservedTimestamp = null
                    if (message.message?.protocolMessage?.type === proto.Message.ProtocolMessage.Type.MESSAGE_EDIT && message.message.protocolMessage.key) {
                        const editTargetKey = message.message.protocolMessage.key
                        log(`🔄 [Bull Queue] Detected MESSAGE_EDIT for message ${editTargetKey.id}`)
                        
                        // Fetch the original message to preserve its timestamp
                        const originalMessage = await collections.messages.findOne({
                            instanceId,
                            jid,
                            'key.id': editTargetKey.id
                        })
                        
                        if (originalMessage && originalMessage.messageTimestamp) {
                            preservedTimestamp = originalMessage.messageTimestamp
                            log(`⏰ [Bull Queue] Preserving original messageTimestamp: ${preservedTimestamp} for edited message ${editTargetKey.id}`)
                        }
                    }
                    
                    await collections.messages.replaceOne(
                        {
                            instanceId,
                            jid,
                            'key.id': message.key?.id
                        },
                        {
                            ...message,
                            // Preserve original timestamp for MESSAGE_EDIT, otherwise use the message's timestamp
                            ...(preservedTimestamp && { messageTimestamp: preservedTimestamp }),
                            instanceId,
                            jid,
                            ...(pollVoteDecrypted && { pollVoteDecrypted }),
                            updatedAt: new Date()
                        },
                        { upsert: true }
                    )
                    
                    // Handle media download for Official API messages in Bull queue
                    const isOfficialAPI = (message as any).official_api === true
                    if (config.media?.enabled && isOfficialAPI) {
                        log(`🔍 [Bull Queue] Checking for Official API media in message ${message.key?.id}`)
                        const mediaInfo = extractMediaInfo(message)
                        log(`📋 [Bull Queue] Media extraction result: ${mediaInfo ? `Found ${mediaInfo.type} media` : 'No media found'}`)
                        
                        if (mediaInfo) {
                            const mediaMessage = mediaInfo.message as any
                            log(`🆔 [Bull Queue] Media ID: ${mediaMessage.id}, Type: ${mediaInfo.type}, Mimetype: ${mediaInfo.mimetype}`)
                            
                            // Function to check for existing media by hash
                            const checkExistingMedia = async (hash: string): Promise<string | null> => {
                                const existing = await collections.messages.findOne({
                                    instanceId,
                                    mediaHash: hash,
                                    mediaUrl: { $exists: true }
                                }) as any
                                return existing?.mediaUrl || null
                            }
                            
                            try {
                                const mediaResult = await downloadOfficialAPIMedia(message, instanceId, config.media, config.logger, checkExistingMedia)
                                
                                if (mediaResult.success && mediaResult.localPath) {
                                    await collections.messages.updateOne(
                                        { 
                                            instanceId, 
                                            jid, 
                                            'key.id': message.key?.id 
                                        },
                                        { 
                                            $set: { 
                                                mediaUrl: mediaResult.localPath,
                                                mediaType: mediaResult.mediaType,
                                                mediaHash: mediaResult.mediaHash
                                            } 
                                        }
                                    )
                                    log(`✅ [Bull Queue] Official API media downloaded successfully: ${mediaResult.localPath}`)
                                } else {
                                    log(`❌ [Bull Queue] Failed to download Official API media: ${mediaResult.error}`)
                                }
                            } catch (error) {
                                log(`❌ [Bull Queue] Error downloading Official API media: ${error instanceof Error ? error.message : 'Unknown error'}`)
                            }
                        } else {
                            log(`⚠️ [Bull Queue] No media info extracted from Official API message ${message.key?.id}`)
                        }
                    }
                } else if (type === 'update' && messageId && update) {
                    // For message updates, preserve existing quoted message structure AND messageTimestamp for edits
                    const existingMsg = await collections.messages.findOne({
                        instanceId,
                        jid,
                        'key.id': messageId
                    }) as any
                    
                    if (existingMsg) {
                        // Check if this is a MESSAGE_EDIT by looking for editedMessage field
                        const isMessageEdit = !!(update.message?.editedMessage || (update as any).editedMessage)
                        const originalTimestamp = existingMsg.messageTimestamp
                        
                        if (isMessageEdit) {
                            log(`🔄 [Bull Queue Update] Detected MESSAGE_EDIT for ${messageId}`)
                            log(`⏰ [Bull Queue Update] Original timestamp: ${originalTimestamp}, New timestamp in update: ${update.messageTimestamp}`)
                        }
                        
                        // Prepare the update object, preserving original timestamp for edits
                        const finalUpdate = { ...update }
                        if (isMessageEdit && originalTimestamp) {
                            finalUpdate.messageTimestamp = originalTimestamp
                            log(`✅ [Bull Queue Update] Preserved original messageTimestamp: ${originalTimestamp}`)
                        }
                        
                        if (existingMsg.message?.extendedTextMessage?.contextInfo?.quotedMessage) {
                            // Deep merge to preserve quoted message
                            const mergedMessage = {
                                ...existingMsg.message,
                                ...update.message
                            }
                            
                            // Ensure quoted message is preserved
                            if (update.message?.extendedTextMessage && 
                                existingMsg.message.extendedTextMessage.contextInfo?.quotedMessage &&
                                mergedMessage.extendedTextMessage) {
                                if (!mergedMessage.extendedTextMessage.contextInfo) {
                                    mergedMessage.extendedTextMessage.contextInfo = existingMsg.message.extendedTextMessage.contextInfo
                                } else if (!mergedMessage.extendedTextMessage.contextInfo.quotedMessage) {
                                    mergedMessage.extendedTextMessage.contextInfo.quotedMessage = 
                                        existingMsg.message.extendedTextMessage.contextInfo.quotedMessage
                                    mergedMessage.extendedTextMessage.contextInfo.stanzaId = 
                                        existingMsg.message.extendedTextMessage.contextInfo.stanzaId
                                    mergedMessage.extendedTextMessage.contextInfo.participant = 
                                        existingMsg.message.extendedTextMessage.contextInfo.participant
                                }
                            }
                            
                            await collections.messages.updateOne(
                                {
                                    instanceId,
                                    jid,
                                    'key.id': messageId
                                },
                                {
                                    $set: { 
                                        ...finalUpdate,
                                        message: mergedMessage,
                                        updatedAt: new Date() 
                                    }
                                }
                            )
                        } else {
                            // No existing quoted message, proceed with update
                            await collections.messages.updateOne(
                                {
                                    instanceId,
                                    jid,
                                    'key.id': messageId
                                },
                                {
                                    $set: { ...finalUpdate, updatedAt: new Date() }
                                }
                            )
                        }
                    } else {
                        // No existing message, just apply the update
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
                    }
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
                // Handle cleanup job
                if (job.name === 'label-cache-cleanup') {
                    log('[Label Cache Cleanup] Running scheduled cleanup job')
                    await cleanupLabelCache()
                    return { success: true }
                }
                
                // Handle regular label association jobs
                const { type, association, timestamp, operationId } = job.data
                const jobId = job.id
                const messageId = 'messageId' in association ? association.messageId : undefined
                const associationId = `${association.labelId}-${association.chatId}${messageId ? `-${messageId}` : ''}`
                
                log(`[Label Queue] Processing ${type} job ${jobId} (op: ${operationId}) for ${associationId}`)
                
                try {
                    if (type === 'upsert') {
                        const filter: any = {
                            instanceId,
                            type: association.type, // Include type field to differentiate label_jid vs label_message
                            chatId: association.chatId,
                            labelId: association.labelId
                        }
                        
                        // Only add messageId for message labels
                        if (association.type === 'label_message' && 'messageId' in association && association.messageId) {
                            filter.messageId = association.messageId
                        }
                        
                        // Debug logging to understand what's being stored
                        log(`[Label Queue] Processing ${type} - Type: ${association.type}, ChatId: ${association.chatId}, LabelId: ${association.labelId}`)
                        log(`[Label Queue] Filter: ${JSON.stringify(filter)}`)
                        log(`[Label Queue] Document to upsert: ${JSON.stringify({...association, instanceId, updatedAt: new Date()})}`)
                        
                        // Enhanced debug logging - check existing documents before operation
                        const existingDocs = await collections.labelAssociations.find({
                            instanceId,
                            chatId: association.chatId,
                            labelId: association.labelId
                        }).toArray()
                        
                        if (existingDocs.length > 0) {
                            log(`[Label Queue] Found ${existingDocs.length} existing docs for ${association.chatId}/${association.labelId}:`)
                            existingDocs.forEach((doc, index) => {
                                log(`[Label Queue]   Doc ${index + 1}: type=${doc.type}, messageId=${(doc as any).messageId || 'none'}`)
                            })
                        } else {
                            log(`[Label Queue] No existing documents found for ${association.chatId}/${association.labelId}`)
                        }
                        
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
                        
                        // Enhanced debug - verify final state
                        const finalDocs = await collections.labelAssociations.find({
                            instanceId,
                            chatId: association.chatId,
                            labelId: association.labelId
                        }).toArray()
                        
                        log(`[Label Queue] Final state: ${finalDocs.length} docs exist for ${association.chatId}/${association.labelId}`)
                        finalDocs.forEach((doc, index) => {
                            log(`[Label Queue]   Final Doc ${index + 1}: type=${doc.type}, messageId=${(doc as any).messageId || 'none'}, id=${doc._id}`)
                        })
                        
                        // Track the add operation for automation AFTER successful DB operation
                        await trackLabelOperation('add', association)
                    } else if (type === 'delete') {
                        const filter: any = {
                            instanceId,
                            type: association.type, // Include type field for proper matching
                            chatId: association.chatId,
                            labelId: association.labelId
                        }
                        
                        // Only add messageId for message labels
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
                            
                            // Track the remove operation for automation AFTER successful DB operation
                            await trackLabelOperation('remove', association)
                        }
                    }
                    
                    const processingTime = Date.now() - (timestamp || 0)
                    log(`[Label Queue] Job ${jobId} completed in ${processingTime}ms`)
                    
                    return { success: true, processingTime, timestamp: Date.now() }
                } catch (error) {
                    logError(`[Label Queue] ❌ Failed ${type} job ${jobId} for ${associationId}:`, error)
                    throw error
                }
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
    
    // Daily cleanup function for Redis label cache
    const cleanupLabelCache = async () => {
        try {
            if (!redisConnection) {
                log('[Label Cache Cleanup] Redis not available, skipping cleanup')
                return
            }

            log('[Label Cache Cleanup] Starting daily cleanup at 3AM')
            const startTime = Date.now()
            let cleanedCount = 0
            let totalCount = 0

            const hashKey = `labelsAssociation:${instanceId}`
            const allFields = await redisConnection.hgetall(hashKey)
            
            for (const [chatId, dataStr] of Object.entries(allFields)) {
                totalCount++
                try {
                    const metadata = JSON.parse(dataStr)
                    
                    // Check if all labels in this chat are already in MongoDB
                    let allSynced = true
                    
                    // Check add labels
                    for (const labelId of metadata.addLabelIds || []) {
                        const exists = await collections.labelAssociations.findOne({
                            instanceId,
                            chatId: metadata.chatId,
                            labelId,
                            type: LabelAssociationType.Chat
                        })
                        if (!exists) {
                            allSynced = false
                            break
                        }
                    }
                    
                    // If all add labels are synced and no remove labels pending, clean up
                    if (allSynced && (!metadata.removeLabelIds || metadata.removeLabelIds.length === 0)) {
                        await redisConnection.hdel(hashKey, chatId)
                        cleanedCount++
                        log(`[Label Cache Cleanup] Cleaned cache for chat ${chatId}`)
                    }
                } catch (error) {
                    logError(`[Label Cache Cleanup] Error processing chat ${chatId}:`, error)
                }
            }
            
            const duration = Date.now() - startTime
            log(`[Label Cache Cleanup] Completed - Cleaned ${cleanedCount}/${totalCount} entries in ${duration}ms`)
        } catch (error) {
            logError('[Label Cache Cleanup] Failed to run cleanup:', error)
        }
    }
    
    // Schedule daily cleanup at 3AM using Bull repeatable job
    if (redisConnection && queues.has(QueueType.LABEL_ASSOCIATIONS)) {
        const labelQueue = queues.get(QueueType.LABEL_ASSOCIATIONS)!
        
        // Add repeatable job for daily cleanup at 3AM UTC
        await labelQueue.add(
            'label-cache-cleanup',
            { instanceId },
            {
                repeat: {
                    pattern: '0 3 * * *', // Cron pattern for 3AM daily
                    tz: 'UTC'
                },
                removeOnComplete: true,
                removeOnFail: false
            }
        )
        
        // Process cleanup jobs
        workers.get(QueueType.LABEL_ASSOCIATIONS)?.on('completed', (job: Job) => {
            if (job.name === 'label-cache-cleanup') {
                log('[Label Cache Cleanup] Daily cleanup job completed')
            }
        })
        
        // Add handler for cleanup job in the existing worker
        const existingProcessor = workers.get(QueueType.LABEL_ASSOCIATIONS)
        if (existingProcessor) {
            // The worker already processes label jobs, we need to handle cleanup in the same processor
            // This will be handled in the existing processor logic
        }
        
        log('[Label Cache Cleanup] Scheduled daily cleanup at 3AM UTC using Bull repeatable job')
    }
    
    // Helper function to track label operations in Redis cache for automation
    const trackLabelOperation = async (operation: 'add' | 'remove', association: LabelAssociation) => {
        try {
            if (!redisConnection) {
                log('[Label Operations] Redis not available, skipping cache tracking')
                // Still trigger automation if hook is provided
                if (hooks?.onLabelOperation) {
                    await hooks.onLabelOperation(instanceId, operation, association.chatId, association.labelId)
                }
                return
            }

            const normalizedChatId = association.chatId.replace('@s.whatsapp.net', '').replace('@lid', '')
            const hashKey = `labelsAssociation:${instanceId}`
            
            // Get existing data from Redis
            const existingData = await redisConnection.hget(hashKey, normalizedChatId)
            let metadata = existingData ? JSON.parse(existingData) : {
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
            
            // Trigger automation processing if hook is provided
            if (hooks?.onLabelOperation) {
                await hooks.onLabelOperation(instanceId, operation, association.chatId, association.labelId)
            }
        } catch (error) {
            logError(`[Label Operations] Failed to track ${operation} operation:`, error)
        }
    }
    
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
        
        // No TTL for groupMetadata - data persists indefinitely
        // Drop existing TTL index if it exists (migration from older versions)
        indexPromises.push(
            collections.groupMetadata.dropIndex('updatedAt_1').catch(() => {
                // Index might not exist, ignore error
            }).then(() => {
                // Create unique index without TTL
                return collections.groupMetadata.createIndex({ instanceId: 1, id: 1 }, { unique: true })
            }).then(() => {})
            // TTL index removed - groupMetadata will persist until explicitly deleted
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
        
        // No TTL for labelAssociations - data persists indefinitely
        // Drop existing TTL index if it exists (migration from older versions)
        indexPromises.push(
            collections.labelAssociations.dropIndex('updatedAt_1').catch(() => {
                // Index might not exist, ignore error
            }).then(() => {
                // Create unique index without TTL
                return collections.labelAssociations.createIndex({ instanceId: 1, type: 1, chatId: 1, labelId: 1 }, { unique: true })
            }).then(() => {})
            // TTL index removed - labelAssociations will persist until explicitly deleted
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
                // groupMetadata - removed from TTL verification (no TTL)
                ttlMonitor.verifyTTLIndex(`${collectionPrefix}presences`),
                ttlMonitor.verifyTTLIndex(`${collectionPrefix}labels`)
                // labelAssociations - removed from TTL verification (no TTL)
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
            
            // Normalize chat IDs through LID handler if available
            const normalizedChats = await Promise.all(chats.map(async (chat) => {
                if (lidHandler && chat.id) {
                    const normalizedId = await lidHandler.normalizeJid(chat.id) || chat.id
                    return { ...chat, id: normalizedId }
                }
                return chat
            }))
            
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.CHATS)) {
                try {
                    const queue = queues.get(QueueType.CHATS)!
                    await queue.add(
                        'upsert',
                        {
                            type: 'upsert',
                            chats: normalizedChats,
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
            const bulkOps = normalizedChats.map(chat => ({
                replaceOne: {
                    filter: { instanceId, id: chat.id },
                    replacement: { ...chat, instanceId, updatedAt: new Date() },
                    upsert: true
                }
            }))
            
            await collections.chats.bulkWrite(bulkOps)
        },

        async updateChat(jid: string, update: Partial<Chat>): Promise<boolean> {
            // Normalize JID through LID handler if available
            let normalizedJid = jid
            if (lidHandler) {
                normalizedJid = await lidHandler.normalizeJid(jid) || jid
            }
            
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.CHATS)) {
                try {
                    const queue = queues.get(QueueType.CHATS)!
                    await queue.add(
                        'update',
                        {
                            type: 'update',
                            chatId: normalizedJid,
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
                { instanceId, id: normalizedJid },
                { $set: { ...update, updatedAt: new Date() } }
            )
            
            return result.modifiedCount > 0
        },

        async deleteChats(jids: string[]): Promise<void> {
            // Normalize JIDs through LID handler if available
            const normalizedJids = await Promise.all(jids.map(async (jid) => {
                if (lidHandler) {
                    return await lidHandler.normalizeJid(jid) || jid
                }
                return jid
            }))
            
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.CHATS)) {
                try {
                    const queue = queues.get(QueueType.CHATS)!
                    await queue.add(
                        'delete',
                        {
                            type: 'delete',
                            deleteIds: normalizedJids,
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
                id: { $in: normalizedJids }
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
                const validJid = safeValidateJID(jid)
                const validId = safeValidateMessageId(id)
                
                const cacheKey = `msg_${validatedInstanceId}_${hashForLogging(validJid)}_${hashForLogging(validId)}`
                
                const cached = binaryConversionCache.get<proto.IWebMessageInfo>(cacheKey)
                if (cached) return cached
                
                // First try the standard query
                let message = await collections.messages.findOne({
                    instanceId: validatedInstanceId,
                    jid: validJid,
                    'key.id': validId
                })
                
                // If not found, try alternative queries for poll messages and other edge cases
                if (!message) {
                    
                    // Try with key.remoteJid instead of jid field
                    message = await collections.messages.findOne({
                        instanceId: validatedInstanceId,
                        'key.remoteJid': validJid,
                        'key.id': validId
                    })
                    
                    if (!message) {
                        // Try without the jid constraint at all (just instanceId and key.id)
                        message = await collections.messages.findOne({
                            instanceId: validatedInstanceId,
                            'key.id': validId
                        })
                        
                        // Check if the JID mismatch is acceptable
                        if (message && message.key?.remoteJid !== validJid) {
                            const foundJid = message.key?.remoteJid || message.jid
                            
                            // Check if JIDs are equivalent (same JID with different format)
                            if (areJidsEquivalent(foundJid, validJid)) {
                                // JIDs are equivalent, accept the message
                            }
                            // Check if this is a LID-phone pair
                            else if (isLidAndPhonePair(foundJid, validJid)) {
                                log(`Discovered LID-phone pair: ${foundJid} <-> ${validJid}`)
                                // Store the discovered mapping if we have a LID handler
                                if (lidHandler) {
                                    const stored = await lidHandler.storeDiscoveredMapping(foundJid, validJid)
                                    
                                    // If mapping was successfully stored, update existing messages with LID format
                                    if (stored) {
                                        // Determine which one is the LID and which is the phone
                                        const lidJid = lidHandler.isLidFormat(foundJid) ? foundJid : validJid
                                        const phoneJid = lidHandler.isLidFormat(foundJid) ? validJid : foundJid
                                        
                                        log(`Updating existing messages from LID ${lidJid} to phone ${phoneJid}`)
                                        
                                        // Update messages that have the LID as remoteJid
                                        // Only update remoteJid and jid fields, leave senderPn and senderLid as-is
                                        try {
                                            const updateResult = await collections.messages.updateMany(
                                                {
                                                    instanceId: validatedInstanceId,
                                                    'key.remoteJid': lidJid
                                                },
                                                {
                                                    $set: {
                                                        'key.remoteJid': phoneJid,
                                                        jid: phoneJid,
                                                        'lidMapping.resolved': true,
                                                        'lidMapping.resolvedAt': new Date()
                                                    }
                                                }
                                            )
                                            
                                            if (updateResult.modifiedCount > 0) {
                                                log(`Updated ${updateResult.modifiedCount} messages from LID to phone number format`)
                                            }
                                        } catch (updateError) {
                                            logError(`Failed to update messages with new LID mapping:`, updateError)
                                        }
                                    }
                                }
                            }
                            // Otherwise, reject the message
                            else {
                                log(`JIDs are not related, rejecting message`)
                                message = null
                            }
                        }
                    }

                    // After all attempts, if still not found, log debug and return null
                    if (!message) {
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
                // Deep clone the message to ensure we work with a mutable copy
                // This prevents issues with Baileys' object references and multiple event emissions
                const clonedMessage = JSON.parse(JSON.stringify(message))
                
                // Normalize JID through LID handler if available
                // Note: Message object LID processing is done in the messages.upsert event handler
                let normalizedJid = jid
                if (lidHandler) {
                    normalizedJid = await lidHandler.normalizeJid(jid) || jid
                }
                const validJid = safeValidateJID(normalizedJid)
                
                // Check if this is an Official API message
                const isOfficialAPI = (clonedMessage as any).official_api === true
                
                // Validate message ID if present (with special handling for Official API)
                if (clonedMessage.key?.id) {
                    safeValidateMessageId(clonedMessage.key.id, isOfficialAPI)
                }
                
                // Check write permissions
                accessContext.validateAccess(validatedInstanceId, 'write')
                
                const cacheKey = `msg_${validatedInstanceId}_${hashForLogging(validJid)}_${clonedMessage.key?.id ? hashForLogging(clonedMessage.key.id) : ''}`
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
                            message: clonedMessage,
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
            
            // Resolve quoted message if present
            if (clonedMessage.message?.extendedTextMessage?.contextInfo?.stanzaId && 
                (!clonedMessage.message.extendedTextMessage.contextInfo.quotedMessage || 
                 Object.keys(clonedMessage.message.extendedTextMessage.contextInfo.quotedMessage).length === 0)) {
                
                const quotedMsg = await resolveQuotedMessage(clonedMessage, validJid, collections, validatedInstanceId, log)
                if (quotedMsg && quotedMsg.message) {
                    // Update the cloned message with the resolved quoted content
                    clonedMessage.message.extendedTextMessage.contextInfo.quotedMessage = quotedMsg.message
                    log(`✅ Updated message with resolved quoted content for ${clonedMessage.key?.id}`)
                }
            }
            
            // Decrypt poll vote if present
            let pollVoteDecrypted = null
            if (clonedMessage.message?.pollUpdateMessage) {
                pollVoteDecrypted = await decryptPollVote(clonedMessage, collections, validatedInstanceId, meId, log)
                if (pollVoteDecrypted) {
                    log(`✅ Decrypted poll vote for ${clonedMessage.key?.id}`)
                }
            }
            
            // Check if this is a MESSAGE_EDIT
            let preservedTimestamp = null
            if (clonedMessage.message?.protocolMessage?.type === proto.Message.ProtocolMessage.Type.MESSAGE_EDIT && clonedMessage.message.protocolMessage.key) {
                const editTargetKey = clonedMessage.message.protocolMessage.key
                log(`🔄 [Direct] Detected MESSAGE_EDIT for message ${editTargetKey.id}`)
                
                // Fetch the original message to preserve its timestamp
                const originalMessage = await collections.messages.findOne({
                    instanceId: validatedInstanceId,
                    jid: validJid,
                    'key.id': editTargetKey.id
                })
                
                if (originalMessage && originalMessage.messageTimestamp) {
                    preservedTimestamp = originalMessage.messageTimestamp
                    log(`⏰ [Direct] Preserving original messageTimestamp: ${preservedTimestamp} for edited message ${editTargetKey.id}`)
                }
            }
            
            // Fallback to direct database operation
            await collections.messages.replaceOne(
                {
                    instanceId: validatedInstanceId,
                    jid: validJid,
                    'key.id': clonedMessage.key?.id
                },
                {
                    ...clonedMessage,
                    // Preserve original timestamp for MESSAGE_EDIT, otherwise use the message's timestamp
                    ...(preservedTimestamp && { messageTimestamp: preservedTimestamp }),
                    instanceId: validatedInstanceId,
                    jid: validJid,
                    ...(pollVoteDecrypted && { pollVoteDecrypted }),
                    updatedAt: new Date()
                },
                { upsert: true }
            )
            
            // Handle media download for Official API messages
            if (config.media?.enabled && isOfficialAPI) {
                log(`🔍 Checking for Official API media in message ${clonedMessage.key?.id}`)
                const mediaInfo = extractMediaInfo(clonedMessage)
                log(`📋 Media extraction result: ${mediaInfo ? `Found ${mediaInfo.type} media` : 'No media found'}`)
                
                if (mediaInfo) {
                    const mediaMessage = mediaInfo.message as any
                    log(`🆔 Media ID: ${mediaMessage.id}, Type: ${mediaInfo.type}, Mimetype: ${mediaInfo.mimetype}`)
                    
                    config.logger?.info({
                        messageId: clonedMessage.key?.id,
                        jid: validJid,
                        mediaType: mediaInfo.type,
                        mediaId: mediaMessage.id,
                        isOfficialAPI
                    }, '📥 Triggering Official API media download from upsertMessage')
                    
                    // Function to check for existing media by hash
                    const checkExistingMedia = async (hash: string): Promise<string | null> => {
                        const existing = await collections.messages.findOne({
                            instanceId: validatedInstanceId,
                            mediaHash: hash,
                            mediaUrl: { $exists: true }
                        }) as any
                        return existing?.mediaUrl || null
                    }
                    
                    // Download media asynchronously
                    downloadOfficialAPIMedia(clonedMessage, validatedInstanceId, config.media, config.logger, checkExistingMedia)
                        .then(async (mediaResult) => {
                            if (mediaResult.success && mediaResult.localPath) {
                                // Update message with media URL
                                await collections.messages.updateOne(
                                    { 
                                        instanceId: validatedInstanceId, 
                                        jid: validJid, 
                                        'key.id': clonedMessage.key?.id 
                                    },
                                    { 
                                        $set: { 
                                            mediaUrl: mediaResult.localPath,
                                            mediaType: mediaResult.mediaType,
                                            mediaHash: mediaResult.mediaHash
                                        } 
                                    }
                                )
                                log(`✅ Official API media downloaded successfully: ${mediaResult.localPath}`)
                                config.logger?.info({
                                    messageId: clonedMessage.key?.id,
                                    mediaUrl: mediaResult.localPath
                                }, '✅ Official API media downloaded and URL updated')
                            } else {
                                log(`❌ Failed to download Official API media: ${mediaResult.error}`)
                                config.logger?.warn({
                                    messageId: clonedMessage.key?.id,
                                    error: mediaResult.error
                                }, '❌ Failed to download Official API media')
                            }
                        })
                        .catch(error => {
                            log(`❌ Error downloading Official API media: ${error instanceof Error ? error.message : 'Unknown error'}`)
                            config.logger?.error({
                                messageId: clonedMessage.key?.id,
                                error: error instanceof Error ? error.message : 'Unknown error'
                            }, '❌ Error downloading Official API media')
                        })
                } else {
                    log(`⚠️ No media info extracted from Official API message ${clonedMessage.key?.id}`)
                }
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
            // Normalize JID through LID handler if available
            let normalizedJid = jid
            if (lidHandler) {
                normalizedJid = await lidHandler.normalizeJid(jid) || jid
            }
            
            const cacheKey = `msg_${instanceId}_${normalizedJid}_${id}`
            binaryConversionCache.del(cacheKey)
            
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.MESSAGES)) {
                try {
                    const queue = queues.get(QueueType.MESSAGES)!
                    await queue.add(
                        'update',
                        {
                            type: 'update',
                            jid: normalizedJid,
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
            
            // Fallback to direct update - preserve quoted message structure AND messageTimestamp for edits
            const existingMsg = await collections.messages.findOne({
                instanceId,
                jid: normalizedJid,
                'key.id': id
            }) as any
            
            let finalUpdate = update
            
            if (existingMsg) {
                // Check if this is a MESSAGE_EDIT by looking for editedMessage field
                const isMessageEdit = !!(update.message?.editedMessage || (update as any).editedMessage)
                const originalTimestamp = existingMsg.messageTimestamp
                
                if (isMessageEdit) {
                    log(`🔄 [updateMessage] Detected MESSAGE_EDIT for ${id}`)
                    log(`⏰ [updateMessage] Original timestamp: ${originalTimestamp}, New timestamp in update: ${update.messageTimestamp}`)
                }
                
                // Preserve original timestamp for edits
                if (isMessageEdit && originalTimestamp) {
                    finalUpdate = { ...update }
                    finalUpdate.messageTimestamp = originalTimestamp
                    log(`✅ [updateMessage] Preserved original messageTimestamp: ${originalTimestamp}`)
                }
                
                // If existing message has quoted message and update has message content, preserve quoted structure
                if (existingMsg.message?.extendedTextMessage?.contextInfo?.quotedMessage && 
                    update.message?.extendedTextMessage) {
                    
                    const mergedMessage = {
                        ...existingMsg.message,
                        ...update.message
                    }
                    
                    // Preserve quoted message if not in update
                    if (!mergedMessage.extendedTextMessage.contextInfo || 
                        !mergedMessage.extendedTextMessage.contextInfo.quotedMessage) {
                        mergedMessage.extendedTextMessage.contextInfo = {
                            ...existingMsg.message.extendedTextMessage.contextInfo,
                            ...(mergedMessage.extendedTextMessage.contextInfo || {})
                        }
                    }
                    
                    finalUpdate = {
                        ...finalUpdate,
                        message: mergedMessage
                    }
                }
            }
            
            const result = await collections.messages.updateOne(
                {
                    instanceId,
                    jid: normalizedJid,
                    'key.id': id
                },
                {
                    $set: { ...finalUpdate, updatedAt: new Date() }
                }
            )
            
            return result.modifiedCount > 0
        },

        async deleteMessages(jid: string, ids?: string[]): Promise<void> {
            try {
                // Normalize JID through LID handler if available
                let normalizedJid = jid
                if (lidHandler) {
                    normalizedJid = await lidHandler.normalizeJid(jid) || jid
                }
                const validJid = safeValidateJID(normalizedJid)
                
                // For deletion, use safe validation for message IDs
                const validIds = ids?.map(id => safeValidateMessageId(id, false))
                
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
                    
                    // Generate unique job ID for deduplication
                    const messageId = 'messageId' in association ? association.messageId : undefined
                    const jobId = `${association.labelId}-${association.chatId}${messageId ? `-${messageId}` : ''}`
                    const uniqueJobId = `upsert-${jobId}`
                    
                    // Check for existing conflicting jobs and remove only truly conflicting ones
                    const existingJobs = await queue.getJobs(['waiting', 'delayed'])
                    const conflictingJobs = existingJobs.filter(job => {
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
                    for (const conflictingJob of conflictingJobs) {
                        // Only remove if the job is very recent (within last 10 seconds) to avoid removing legitimate queued operations
                        // Fix: Get timestamp from job.data.timestamp, not job.opts.timestamp
                        const jobTimestamp = (conflictingJob.data as LabelAssociationJob)?.timestamp || 
                                           conflictingJob.processedOn || 
                                           conflictingJob.timestamp || 
                                           0
                        const jobAge = jobTimestamp > 0 ? Date.now() - jobTimestamp : Number.MAX_SAFE_INTEGER
                        if (jobAge < 10000) { // 10 seconds
                            await conflictingJob.remove()
                            removedCount++
                            log(`[Bull LabelAssociations] Removed recent duplicate job ${conflictingJob.id} (age: ${jobAge}ms) for ${jobId}`)
                        }
                    }
                    
                    if (conflictingJobs.length > 0) {
                        log(`[Bull LabelAssociations] Found ${conflictingJobs.length} potential conflicts, removed ${removedCount} recent duplicates for ${jobId}`)
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
                    
                    log(`[Bull LabelAssociations] Queued upsert job ${job.id} for ${jobId}`)
                    return
                } catch (error) {
                    logError('[Bull LabelAssociations] Failed to queue, falling back to direct write:', error)
                    // Continue to direct write fallback
                }
            }
            
            // Fallback to direct write with comprehensive error handling
            const filter: any = {
                instanceId,
                type: association.type, // CRITICAL FIX: Include type field to differentiate label_jid vs label_message
                chatId: association.chatId,
                labelId: association.labelId
            }
            
            // Only add messageId for message labels
            if (association.type === 'label_message' && 'messageId' in association && association.messageId) {
                filter.messageId = association.messageId
            }
            
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
                const errorMsg = `[Direct] Failed to upsert label association - chatId: ${association.chatId}, labelId: ${association.labelId}, type: ${association.type}`
                logError(errorMsg)
                throw new Error(errorMsg)
            }
            
            log(`[Direct] ✅ Label association upserted - upserted: ${result.upsertedCount}, modified: ${result.modifiedCount}, type: ${association.type}`)
        },

        async deleteLabelAssociation(association: LabelAssociation): Promise<void> {
            // Use Bull queue if available
            if (bullInitialized && queues.has(QueueType.LABEL_ASSOCIATIONS)) {
                try {
                    const queue = queues.get(QueueType.LABEL_ASSOCIATIONS)!
                    
                    // Generate unique job ID for deduplication
                    const messageId = 'messageId' in association ? association.messageId : undefined
                    const jobId = `${association.labelId}-${association.chatId}${messageId ? `-${messageId}` : ''}`
                    const uniqueJobId = `delete-${jobId}`
                    
                    // Check for existing conflicting jobs and remove only truly conflicting ones
                    const existingJobs = await queue.getJobs(['waiting', 'delayed'])
                    const conflictingJobs = existingJobs.filter(job => {
                        const jobData = job.data as LabelAssociationJob
                        // Skip jobs without association data (like cleanup jobs)
                        if (!jobData || !jobData.association) return false
                        const jobMessageId = 'messageId' in jobData.association ? jobData.association.messageId : undefined
                        
                        // Only consider it conflicting if:
                        // 1. Same labelId, chatId, and type
                        // 2. Same messageId (or both are undefined)
                        // 3. Same operation type (both delete or both upsert)
                        return jobData.association.labelId === association.labelId &&
                               jobData.association.chatId === association.chatId &&
                               jobData.association.type === association.type &&
                               jobData.type === 'delete' && // Current is delete, only conflict with other deletes
                               ((!messageId && !jobMessageId) || (messageId === jobMessageId))
                    })
                    
                    // Remove truly conflicting jobs (only recent duplicates)
                    let removedCount = 0
                    for (const conflictingJob of conflictingJobs) {
                        // Only remove if the job is very recent (within last 10 seconds) to avoid removing legitimate queued operations
                        // Fix: Get timestamp from job.data.timestamp, not job.opts.timestamp
                        const jobTimestamp = (conflictingJob.data as LabelAssociationJob)?.timestamp || 
                                           conflictingJob.processedOn || 
                                           conflictingJob.timestamp || 
                                           0
                        const jobAge = jobTimestamp > 0 ? Date.now() - jobTimestamp : Number.MAX_SAFE_INTEGER
                        if (jobAge < 10000) { // 10 seconds
                            await conflictingJob.remove()
                            removedCount++
                            log(`[Bull LabelAssociations] Removed recent duplicate job ${conflictingJob.id} (age: ${jobAge}ms) for ${jobId}`)
                        }
                    }
                    
                    if (conflictingJobs.length > 0) {
                        log(`[Bull LabelAssociations] Found ${conflictingJobs.length} potential conflicts, removed ${removedCount} recent duplicates for ${jobId}`)
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
                    
                    log(`[Bull LabelAssociations] Queued delete job ${job.id} for ${jobId}`)
                    return
                } catch (error) {
                    logError('[Bull LabelAssociations] Failed to queue delete, falling back to direct delete:', error)
                    // Continue to direct delete fallback
                }
            }
            
            // Fallback to direct delete with comprehensive error handling
            const filter: any = {
                instanceId,
                type: association.type, // CRITICAL FIX: Include type field for proper matching
                chatId: association.chatId,
                labelId: association.labelId
            }
            
            // Only add messageId for message labels
            if (association.type === 'label_message' && 'messageId' in association && association.messageId) {
                filter.messageId = association.messageId
            }
            
            const result = await collections.labelAssociations.deleteOne(filter)
            
            if (result.deletedCount === 0) {
                logWarn(`[Direct] Warning: No label association found to delete - chatId: ${association.chatId}, labelId: ${association.labelId}, type: ${association.type}`)
            } else {
                log(`[Direct] ✅ Label association deleted - type: ${association.type}`)
            }
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
                    let jid = msg.key.remoteJid
                    if (!jid) continue
                    
                    if (await shouldStoreEvent('messages.upsert', msg)) {
                        try {
                            // Enhanced LID handling with complete pattern support
                            if (lidHandler) {
                                const isFromMe = msg.key.fromMe || false
                                const remoteJid = msg.key.remoteJid
                                const senderLid = (msg.key as any)?.senderLid
                                const senderPn = (msg.key as any)?.senderPn
                                
                                // Pattern 1: FromMe=false with LID remoteJid and phone number in senderPn
                                if (!isFromMe && remoteJid && lidHandler.isLidFormat(remoteJid) && senderPn && !lidHandler.isLidFormat(senderPn)) {
                                    log(`[LID] Pattern 1: FromMe=false, LID remoteJid with phone in senderPn`)
                                    log(`[LID] Discovering: ${remoteJid} -> ${senderPn}`)
                                    
                                    // Store the mapping
                                    await lidHandler.storeLidMapping(remoteJid, senderPn)
                                    
                                    // Update message to use phone number
                                    msg.key.remoteJid = senderPn
                                    jid = senderPn
                                    
                                    // Update existing messages with this LID
                                    await lidHandler.updateExistingMessages(remoteJid, senderPn)
                                }
                                // Pattern 2: FromMe=true with LID remoteJid (both remoteJid and senderPn are LID)
                                else if (isFromMe && remoteJid && lidHandler.isLidFormat(remoteJid)) {
                                    log(`[LID] Pattern 2: FromMe=true, LID remoteJid (need reverse lookup)`)
                                    
                                    // First check if we already have a mapping
                                    let phoneNumber = await lidHandler.getPhoneNumberFromLid(remoteJid)
                                    
                                    // If no mapping, try reverse lookup from previous messages
                                    if (!phoneNumber) {
                                        log(`[LID] No cached mapping, attempting reverse lookup for ${remoteJid}`)
                                        phoneNumber = await lidHandler.reversePhoneLookupFromMessages(remoteJid)
                                        
                                        if (phoneNumber) {
                                            log(`[LID] Reverse lookup found: ${remoteJid} -> ${phoneNumber}`)
                                            await lidHandler.storeLidMapping(remoteJid, phoneNumber)
                                            await lidHandler.updateExistingMessages(remoteJid, phoneNumber)
                                        }
                                    }
                                    
                                    // Update message if we found the phone number
                                    if (phoneNumber) {
                                        log(`[LID] Normalizing fromMe message: ${remoteJid} -> ${phoneNumber}`)
                                        msg.key.remoteJid = phoneNumber
                                        jid = phoneNumber
                                    } else {
                                        log(`[LID] Warning: Could not resolve LID ${remoteJid} for fromMe message`)
                                    }
                                }
                                // Pattern 3: FromMe=false with only senderLid (no phone yet)
                                else if (!isFromMe && senderLid && lidHandler.isLidFormat(senderLid) && !senderPn) {
                                    log(`[LID] Pattern 3: FromMe=false, only senderLid (waiting for phone discovery)`)
                                    
                                    // Check if we have a cached mapping
                                    const phoneNumber = await lidHandler.getPhoneNumberFromLid(senderLid)
                                    if (phoneNumber && remoteJid && lidHandler.isLidFormat(remoteJid)) {
                                        log(`[LID] Using cached mapping: ${remoteJid} -> ${phoneNumber}`)
                                        msg.key.remoteJid = phoneNumber
                                        jid = phoneNumber
                                    }
                                }
                                // Pattern 4: Standard normalization for any remaining LID formats
                                else if (remoteJid && lidHandler.isLidFormat(remoteJid)) {
                                    const phoneNumber = await lidHandler.getPhoneNumberFromLid(remoteJid)
                                    if (phoneNumber) {
                                        log(`[LID] Standard normalization: ${remoteJid} -> ${phoneNumber}`)
                                        msg.key.remoteJid = phoneNumber
                                        jid = phoneNumber
                                    }
                                }
                                
                                // Store debug info in message
                                if ((remoteJid && lidHandler.isLidFormat(remoteJid)) || senderLid || (senderPn && lidHandler.isLidFormat(senderPn))) {
                                    (msg as any).lidDebug = {
                                        originalRemoteJid: remoteJid,
                                        senderLid,
                                        senderPn,
                                        fromMe: isFromMe,
                                        normalized: msg.key.remoteJid !== remoteJid,
                                        timestamp: new Date().toISOString()
                                    }
                                }
                            }
                            
                            // Resolve quoted message before storing if present
                            if (msg.message?.extendedTextMessage?.contextInfo?.stanzaId && 
                                (!msg.message.extendedTextMessage.contextInfo.quotedMessage || 
                                 Object.keys(msg.message.extendedTextMessage.contextInfo.quotedMessage).length === 0) &&
                                jid) {
                                
                                const quotedMsg = await resolveQuotedMessage(msg, jid, collections, instanceId, log)
                                if (quotedMsg && quotedMsg.message) {
                                    // Update the message with the resolved quoted content
                                    msg.message.extendedTextMessage.contextInfo.quotedMessage = quotedMsg.message
                                    log(`✅ [Event Handler] Updated message with resolved quoted content for ${msg.key?.id}`)
                                }
                            }
                            
                            // Decrypt poll vote before storing if present
                            if (msg.message?.pollUpdateMessage) {
                                const pollVoteDecrypted = await decryptPollVote(msg, collections, instanceId, meId, log)
                                if (pollVoteDecrypted) {
                                    // Add decrypted poll vote data to the message
                                    (msg as any).pollVoteDecrypted = pollVoteDecrypted
                                    log(`✅ [Event Handler] Decrypted poll vote for ${msg.key?.id}`)
                                }
                            }
                            
                            // Store the message with normalized JID
                            if (jid) {
                                await storeImpl.upsertMessage(jid, msg)
                            } else {
                                log(`[Warning] Skipping message storage - no valid JID for message ${msg.key?.id}`)
                            }
                            
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
                                const isOfficialAPI = !!(msg as any).official_api
                                
                                // Debug logging for Official API media detection
                                if (config.logger) {
                                    config.logger.info({
                                        messageId: msg.key?.id,
                                        officialApiFlag: (msg as any).official_api,
                                        isOfficialAPI,
                                        hasMedia: !!extractMediaInfo(msg),
                                        messageType: msg.message ? Object.keys(msg.message)[0] : 'unknown'
                                    }, '🔍 DEBUG: Media download detection')
                                }
                                
                                let mediaResult
                                if (isOfficialAPI) {
                                    config.logger?.info({ 
                                        messageId: msg.key?.id,
                                        jid
                                    }, '📥 Attempting Official API media download')
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
                                            jid: jid || undefined, 
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
                    let jid = update.key.remoteJid
                    if (!jid) continue
                    
                    // Normalize JID through LID handler if available
                    if (lidHandler) {
                        jid = await lidHandler.normalizeJid(jid) || jid
                    }
                    
                    if (await shouldStoreEvent('messages.update', update)) {
                        try {
                            // For edited messages, we need to preserve the quoted message structure
                            // First, fetch the existing message to preserve fields not in the update
                            const existingMessage = await storeImpl.getMessage(jid, update.key.id!)
                            
                            if (existingMessage) {
                                // Check if this is a MESSAGE_EDIT by looking for editedMessage field
                                const isMessageEdit = !!(update.update?.message?.editedMessage || (update.update as any)?.editedMessage)
                                
                                // Preserve original messageTimestamp for edits
                                const originalTimestamp = existingMessage.messageTimestamp
                                
                                if (isMessageEdit) {
                                    log(`🔄 [messages.update] Detected MESSAGE_EDIT for ${update.key.id}`)
                                    log(`⏰ [messages.update] Original timestamp: ${originalTimestamp}, New timestamp in update: ${update.update?.messageTimestamp}`)
                                }
                                
                                // Deep merge the update with existing message to preserve quoted messages
                                const mergedUpdate = {
                                    ...existingMessage,
                                    ...update.update,
                                    message: {
                                        ...existingMessage.message,
                                        ...update.update?.message
                                    }
                                }
                                
                                // CRITICAL: Preserve original messageTimestamp for MESSAGE_EDIT
                                if (isMessageEdit && originalTimestamp) {
                                    mergedUpdate.messageTimestamp = originalTimestamp
                                    log(`✅ [messages.update] Preserved original messageTimestamp: ${originalTimestamp}`)
                                }
                                
                                // Preserve quoted message structure if it exists
                                if (existingMessage.message?.extendedTextMessage?.contextInfo?.quotedMessage) {
                                    if (!mergedUpdate.message) mergedUpdate.message = {}
                                    if (!mergedUpdate.message.extendedTextMessage) {
                                        mergedUpdate.message.extendedTextMessage = existingMessage.message.extendedTextMessage
                                    } else if (!mergedUpdate.message.extendedTextMessage.contextInfo) {
                                        mergedUpdate.message.extendedTextMessage.contextInfo = existingMessage.message.extendedTextMessage.contextInfo
                                    } else if (!mergedUpdate.message.extendedTextMessage.contextInfo.quotedMessage) {
                                        mergedUpdate.message.extendedTextMessage.contextInfo.quotedMessage = 
                                            existingMessage.message.extendedTextMessage.contextInfo.quotedMessage
                                        mergedUpdate.message.extendedTextMessage.contextInfo.stanzaId = 
                                            existingMessage.message.extendedTextMessage.contextInfo.stanzaId
                                        mergedUpdate.message.extendedTextMessage.contextInfo.participant = 
                                            existingMessage.message.extendedTextMessage.contextInfo.participant
                                    }
                                }
                                
                                await storeImpl.updateMessage(jid, update.key.id!, mergedUpdate)
                                log(`✅ Updated message ${update.key.id} preserving quoted message structure${isMessageEdit ? ' and original timestamp' : ''}`)
                            } else {
                                // If no existing message found, just apply the update
                                await storeImpl.updateMessage(jid, update.key.id!, update.update!)
                            }
                            
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
                    let jid = item.keys[0].remoteJid
                    if (!jid) return
                    
                    // Normalize JID through LID handler if available
                    if (lidHandler) {
                        jid = await lidHandler.normalizeJid(jid) || jid
                    }
                    
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
                    let jid = item.jid
                    
                    // Normalize JID through LID handler if available
                    if (lidHandler) {
                        jid = await lidHandler.normalizeJid(jid) || jid
                    }
                    
                    if (await shouldStoreEvent('messages.delete', item)) {
                        try {
                            await storeImpl.deleteMessages(jid)
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

            // Messaging history sync
            ev.on('messaging-history.set', async ({ chats: newChats, contacts: newContacts, messages: newMessages, isLatest }) => {
                if (enableMetrics) updateEventMetrics('messaging-history.set', 'received')
                
                const historyData = { chats: newChats, contacts: newContacts, messages: newMessages, isLatest }
                if (await shouldStoreEvent('messaging-history.set', historyData)) {
                    try {
                        if (isLatest) {
                            // Clear existing data when syncing latest history
                            await storeImpl.clearAll()
                            log(`[${instanceId}] Cleared all data for latest history sync`)
                        }
                        
                        // Process in parallel
                        const promises: Promise<void>[] = []
                        
                        if (newChats?.length) {
                            promises.push((async () => {
                                await storeImpl.upsertChats(...newChats)
                                log(`[${instanceId}] Synced ${newChats.length} chats from history`)
                            })())
                        }
                        
                        if (newContacts?.length) {
                            promises.push((async () => {
                                await storeImpl.upsertContacts(newContacts)
                                log(`[${instanceId}] Synced ${newContacts.length} contacts from history`)
                            })())
                        }
                        
                        if (newMessages?.length) {
                            // Process messages in batches
                            for (const msg of newMessages) {
                                let jid = msg.key.remoteJid
                                if (!jid) continue
                                
                                // Process LID if handler is available
                                if (lidHandler) {
                                    const { normalizedJid, lidInfo } = await lidHandler.processMessage(msg)
                                    
                                    // Update the message's remoteJid to use normalized (phone number) format
                                    if (normalizedJid && normalizedJid !== msg.key.remoteJid) {
                                        log(`[History LID Handler] Normalizing JID: ${msg.key.remoteJid} -> ${normalizedJid}`)
                                        msg.key.remoteJid = normalizedJid
                                        jid = normalizedJid
                                    }
                                    
                                    // Store LID info in the message for reference
                                    if (lidInfo.lid || lidInfo.phoneNumber) {
                                        (msg as any).lidMapping = {
                                            lid: lidInfo.lid,
                                            phoneNumber: lidInfo.phoneNumber,
                                            originalJid: msg.key.remoteJid,
                                            mappingStored: lidInfo.mappingStored
                                        }
                                    }
                                }
                                
                                await storeImpl.upsertMessage(jid, msg)
                            }
                            log(`[${instanceId}] Synced ${newMessages.length} messages from history`)
                        }
                        
                        await Promise.all(promises)
                        
                        if (enableMetrics) updateEventMetrics('messaging-history.set', 'stored')
                        if (hooks.afterStore) await hooks.afterStore('messaging-history.set', historyData)
                    } catch (error) {
                        logError('Failed to process messaging history:', error)
                        if (enableMetrics) updateEventMetrics('messaging-history.set', 'error')
                    }
                }
            })

            // Labels edit
            ev.on('labels.edit', async (label) => {
                if (enableMetrics) updateEventMetrics('labels.edit', 'received')
                
                if (await shouldStoreEvent('labels.edit', label)) {
                    try {
                        if (label.deleted) {
                            await storeImpl.deleteLabel(label.id)
                            // Also delete all associations for this label
                            const deleteResult = await collections.labelAssociations.deleteMany({
                                instanceId,
                                labelId: label.id
                            })
                            if (deleteResult.deletedCount > 0) {
                                log(`[${instanceId}] Deleted ${deleteResult.deletedCount} label associations for deleted label ${label.id}`)
                            }
                        } else {
                            await storeImpl.upsertLabel(label.id, label)
                        }
                        
                        if (enableMetrics) updateEventMetrics('labels.edit', 'stored')
                        if (hooks.afterStore) await hooks.afterStore('labels.edit', label)
                    } catch (error) {
                        logError(`Failed to process label edit for ${label.id}:`, error)
                        if (enableMetrics) updateEventMetrics('labels.edit', 'error')
                    }
                }
            })

            // Labels association
            ev.on('labels.association', async ({ type, association }) => {
                if (enableMetrics) updateEventMetrics('labels.association', 'received')
                
                // Enhanced debug logging for label association events
                const messageId = 'messageId' in association ? association.messageId : undefined
                log(`[Event Handler] labels.association received - Type: ${type}, Association: ${association.type}, ChatId: ${association.chatId}, LabelId: ${association.labelId}, MessageId: ${messageId || 'none'}`)
                
                const associationData = { type, association }
                if (await shouldStoreEvent('labels.association', associationData)) {
                    try {
                        log(`[Event Handler] Processing ${type} operation for ${association.chatId}/${association.labelId}`)
                        
                        if (type === 'add') {
                            await storeImpl.upsertLabelAssociation(association)
                            log(`[Event Handler] ✅ Add operation completed for ${association.chatId}/${association.labelId}`)
                        } else if (type === 'remove') {
                            await storeImpl.deleteLabelAssociation(association)
                            log(`[Event Handler] ✅ Remove operation completed for ${association.chatId}/${association.labelId}`)
                        }
                        
                        if (enableMetrics) updateEventMetrics('labels.association', 'stored')
                        if (hooks.afterStore) await hooks.afterStore('labels.association', associationData)
                    } catch (error) {
                        logError(`[Event Handler] Failed to process ${type} label association for ${association.chatId}/${association.labelId}:`, error)
                        if (enableMetrics) updateEventMetrics('labels.association', 'error')
                    }
                } else {
                    log(`[Event Handler] Skipping storage for ${type} label association (filtered out)`)
                }
            })

            // Message receipt update
            ev.on('message-receipt.update', async (updates) => {
                if (enableMetrics) updateEventMetrics('message-receipt.update', 'received')
                
                for (const { key, receipt } of updates) {
                    if (await shouldStoreEvent('message-receipt.update', { key, receipt })) {
                        try {
                            let jid = key.remoteJid
                            if (!jid) continue
                            
                            // Normalize JID through LID handler if available
                            if (lidHandler) {
                                jid = await lidHandler.normalizeJid(jid) || jid
                            }
                            
                            const msg = await storeImpl.getMessage(jid, key.id!)
                            if (msg) {
                                updateMessageWithReceipt(msg, receipt)
                                await storeImpl.updateMessage(jid, key.id!, msg)
                                if (enableMetrics) updateEventMetrics('message-receipt.update', 'stored')
                                if (hooks.afterStore) await hooks.afterStore('message-receipt.update', { key, receipt })
                            }
                        } catch (error) {
                            logError(`Failed to update message receipt for ${key.remoteJid}:`, error)
                            if (enableMetrics) updateEventMetrics('message-receipt.update', 'error')
                        }
                    }
                }
            })

            // Messages reaction
            ev.on('messages.reaction', async (reactions) => {
                if (enableMetrics) updateEventMetrics('messages.reaction', 'received')
                
                for (const { key, reaction } of reactions) {
                    if (await shouldStoreEvent('messages.reaction', { key, reaction })) {
                        try {
                            let jid = key.remoteJid
                            if (!jid) continue
                            
                            // Normalize JID through LID handler if available
                            if (lidHandler) {
                                jid = await lidHandler.normalizeJid(jid) || jid
                            }
                            
                            const msg = await storeImpl.getMessage(jid, key.id!)
                            if (msg) {
                                updateMessageWithReaction(msg, reaction)
                                await storeImpl.updateMessage(jid, key.id!, msg)
                                if (enableMetrics) updateEventMetrics('messages.reaction', 'stored')
                                if (hooks.afterStore) await hooks.afterStore('messages.reaction', { key, reaction })
                            }
                        } catch (error) {
                            logError(`Failed to update message reaction for ${key.remoteJid}:`, error)
                            if (enableMetrics) updateEventMetrics('messages.reaction', 'error')
                        }
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
            
            // Note: Labels and label associations are excluded from clearAll()
            // They should persist across history syncs to maintain label integrity
            await Promise.all([
                collections.chats.deleteMany({ instanceId }),
                collections.contacts.deleteMany({ instanceId }),
                collections.messages.deleteMany({ instanceId }),
                collections.groupMetadata.deleteMany({ instanceId }),
                collections.presences.deleteMany({ instanceId })
                // Removed: collections.labels.deleteMany({ instanceId })
                // Removed: collections.labelAssociations.deleteMany({ instanceId })
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
            // Remove repeatable job for cleanup if it exists
            if (queues.has(QueueType.LABEL_ASSOCIATIONS)) {
                const labelQueue = queues.get(QueueType.LABEL_ASSOCIATIONS)!
                try {
                    await labelQueue.removeRepeatableByKey('label-cache-cleanup')
                    log('[Label Cache Cleanup] Removed scheduled cleanup job')
                } catch (error) {
                    // Ignore if job doesn't exist
                }
            }
            
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