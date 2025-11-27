import { MongoClient, Collection, Db, ClientSession } from 'mongodb'
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
    hashForLogging,
    safeNormalizeJid
} from './utils/security'
import { InstanceAccessContext, DEFAULT_PERMISSIONS } from './utils/auth'
import { MemoryMonitor, BackpressureController } from './utils/memory'
import { TTLMonitor } from './utils/ttl'
import { downloadMedia, downloadOfficialAPIMedia, cleanupOldMedia, getMediaStats, extractMediaInfo } from './utils/media'
import { LidHandler } from './utils/lidHandler'
import type { LidMapping } from './utils/lidHandler'
import { areJidsEquivalent, isLidAndPhonePair } from './utils/jidUtils'
import { ConnectionManager, getConnectionManager } from './utils/connectionManager'
import { retryWithBackoff, isRetryableError, RetryOptions } from './utils/connectionRetry'
import { ConnectionHealthMonitor } from './utils/connectionHealth'
import { safeDropIndex, batchCreateIndexes, recreateIndexes } from './utils/indexHelper'
import { shouldCreateIndexes, IndexSpec, clearCollectionCache } from './utils/collectionHelper'
// @ts-ignore - Types are used in annotations only
import type { ConnectionConfig, ConnectionManagerConfig } from './types/connection'
import { EventEmitter } from 'events'
import { SharedQueueManager, JobType, SharedQueueManagerConfig } from './utils/sharedQueueManager'

// Declare Node.js globals if not available in tsconfig
declare global {
    function setImmediate(callback: (...args: any[]) => void): NodeJS.Immediate
    function clearImmediate(handle: NodeJS.Immediate): void
    function setTimeout(callback: (...args: any[]) => void, ms: number, ...args: any[]): NodeJS.Timeout
    function clearTimeout(handle: NodeJS.Timeout): void
}

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
    STATE = 'state',
    PROFILE_PICTURES = 'profile-pictures'
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

interface ProfilePictureJob {
    contactId: string
    instanceId: string
    timestamp: number
    retryCount?: number
    lastError?: string
}

// Event metrics storage
const eventMetricsMap = new Map<string, EventMetrics>()

// LID Resolution Metrics
interface LidResolutionMetrics {
    operationType: string
    totalResolved: number
    totalErrors: number
    lastProcessedAt?: Date
}

const lidResolutionMetricsMap = new Map<string, LidResolutionMetrics>()

interface MongoCollections {
    chats: Collection<Chat & { instanceId: string; updatedAt: Date }>
    contacts: Collection<Contact & { instanceId: string; updatedAt: Date }>
    messages: Collection<proto.IWebMessageInfo & { instanceId: string; jid: string; updatedAt: Date }>
    groupMetadata: Collection<GroupMetadata & { instanceId: string; updatedAt: Date }>
    state: Collection<ConnectionState & { instanceId: string; updatedAt: Date }>
    presences: Collection<{ instanceId: string; id: string; presences: { [participant: string]: PresenceData }; updatedAt: Date }>
    labels: Collection<Label & { instanceId: string; updatedAt: Date }>
    labelAssociations: Collection<LabelAssociation & { instanceId: string; updatedAt: Date }>
    lidMappings: Collection<LidMapping>
}

// Cache for Binary conversions
const binaryConversionCache = new NodeCache({ stdTTL: 300, checkperiod: 60 })
// Negative cache for not-found lookups to avoid repeated slow fallbacks
const notFoundCache = new NodeCache({ stdTTL: 60, checkperiod: 30, useClones: false })

// Deduplication caches to prevent duplicate processing/log spam
const processedEditCache = new NodeCache({ stdTTL: 10, checkperiod: 30 })
const logThrottleCache = new NodeCache({ stdTTL: 2, checkperiod: 5 })

// Helper to rate-limit repeated logs for the same key
const shouldLogOnce = (key: string, ttlSeconds: number = 2): boolean => {
    if (logThrottleCache.get(key)) {
        return false
    }
    logThrottleCache.set(key, true, ttlSeconds)
    return true
}

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
    log: (...args: any[]) => void,
    withConnection: <T>(operation: () => Promise<T>) => Promise<T>
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
        const quotedMsg = await withConnection(async () =>
            collections.messages.findOne({
                instanceId,
                'key.id': stanzaId,
                $or: [
                    { jid: participant },
                    { 'key.remoteJid': participant },
                    { jid: jid },
                    { 'key.remoteJid': jid }
                ]
            })
        ) as any
        
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
    log: (...args: any[]) => void,
    withConnection: <T>(operation: () => Promise<T>) => Promise<T>
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
        const originalPoll = await withConnection(async () =>
            collections.messages.findOne({
                instanceId,
                'key.id': pollKey.id,
                ...(pollKey.remoteJid && {
                    $or: [
                        { jid: pollKey.remoteJid },
                        { 'key.remoteJid': pollKey.remoteJid }
                    ]
                })
            })
        ) as any
        
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
export const makeEnhancedMongoDBStore = async (config: EnhancedMongoDBStoreConfig & {
    lidConfig?: {
        enabled: boolean;
        requestDelay?: number;
        retryAttempts?: number;
        maxConcurrentLookups?: number;
        negativeCacheTTL?: number;
        lookupsEnabled?: boolean;
        preferReverseLookupFirst?: boolean;
        contactsQueryMaxTimeMS?: number;
        dynamicNegativeBackoff?: boolean;
        minNegativeCacheTTL?: number;
        maxNegativeCacheTTL?: number;
        proactiveHistoryResolution?: boolean;
    }
}): Promise<EnhancedMongoDBStore> => {
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
        lidHandler: lidHandlerConfig,
        connectionConfig,
        connectionManager: connectionManagerOverrides,
        useSharedConnections = true,
        profilePictureConfig,
        indexManagement,
        lidConfig
    } = config

    // Set default values for lidConfig (safer CPU-friendly defaults)
    const defaultLidConfig = {
        enabled: true,
        requestDelay: 500,
        retryAttempts: 3,
        maxConcurrentLookups: 10,
        negativeCacheTTL: 300,
        lookupsEnabled: true,
        preferReverseLookupFirst: true,
        contactsQueryMaxTimeMS: 500,
        dynamicNegativeBackoff: true,
        minNegativeCacheTTL: 300,
        maxNegativeCacheTTL: 3600,
        proactiveHistoryResolution: false
    }
    const finalLidConfig = { ...defaultLidConfig, ...lidConfig }


    // Configure smart index management with defaults
    const indexConfig = {
        skipExistingCollectionIndexes: indexManagement?.skipExistingCollectionIndexes ?? true,
        forceRecreateIndexes: indexManagement?.forceRecreateIndexes ?? false,
        enableIndexHealthLogging: indexManagement?.enableIndexHealthLogging ?? true,
        indexCreationTimeout: indexManagement?.indexCreationTimeout ?? null
    }

    const addIndexTimeout = <T extends IndexSpec>(index: T): T => {
        if (!indexConfig.indexCreationTimeout || indexConfig.indexCreationTimeout <= 0) {
            return index
        }

        const baseOptions = index.options ? { ...index.options } : {}

        return {
            ...index,
            options: {
                ...baseOptions,
                maxTimeMS: indexConfig.indexCreationTimeout
            }
        } as T
    }
    
    // Socket can be set later using setSock() method
    let sock = config.sock || null
    
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

    const waitForBackpressure = async (reason: string) => {
        if (!backpressureController) {
            return
        }

        while (backpressureController.shouldPause()) {
            if (shouldLogOnce(`backpressure-${reason}`)) {
                logWarn(`[${instanceId}] Memory backpressure engaged${reason ? ` during ${reason}` : ''}; pausing operations`) 
            }
            await new Promise(resolve => setTimeout(resolve, 200))
        }
    }

    if (backpressureController) {
        backpressureController.onPause(() => {
            logWarn(`[${instanceId}] Memory pressure high, throttling new work`)
        })

        backpressureController.onResume(() => {
            log(`[${instanceId}] Memory pressure normalized; resuming work`)
        })
    }
    
    // TTL monitor will be initialized after DB connection
    let ttlMonitor: TTLMonitor | null = null
    
    // LID handler will be initialized after DB connection
    let lidHandler: LidHandler | null = null

    // Redis connections
    let redisConnection: Redis | null = null
    let lidRedisClient: Redis | null = null

    // MongoDB connection
    let client: MongoClient
    let db: Db
    let connectionManager: ConnectionManager | null = null
    let isUsingSharedConnection = false
    let currentSharedPoolId: string | null = null
    let reconnectAttempts = 0
    
    // Connection state management
    enum MongoConnectionState {
        DISCONNECTED = 'disconnected',
        CONNECTING = 'connecting',
        CONNECTED = 'connected',
        RECONNECTING = 'reconnecting',
        FAILED = 'failed'
    }
    
    let mongoConnectionState = MongoConnectionState.DISCONNECTED
    const connectionStateEmitter = new EventEmitter()
    // Fix EventEmitter memory leak warning by setting reasonable limit
    connectionStateEmitter.setMaxListeners(50)

    const dedicatedMaxPoolSize = connectionConfig?.maxPoolSize ?? 20
    const dedicatedMinPoolSize = Math.min(connectionConfig?.minPoolSize ?? 4, dedicatedMaxPoolSize)
    const connectionCheckInterval = connectionManagerOverrides?.monitoringInterval ?? 60000
    let lastSuccessfulPing = 0

    const markConnectionStale = (reason: string, error?: unknown) => {
        const err = error instanceof Error ? error : new Error(reason)
        lastSuccessfulPing = 0
        mongoConnectionState = MongoConnectionState.DISCONNECTED
        if (isUsingSharedConnection) {
            currentSharedPoolId = null
        }
        connectionStateEmitter.emit('disconnected', err)
        if (shouldLogOnce(`stale-connection-${reason}`)) {
            logWarn(`[${instanceId}] Connection marked stale: ${reason}`)
        }
    }

    // Track if proactive LID resolution has been done for this instance
    let historyLidResolutionDone = false
    
    // Initialize health monitor
    const healthMonitor = new ConnectionHealthMonitor({
        checkInterval: 30000, // 30 seconds
        unhealthyThreshold: 3,
        healthyThreshold: 2
    })
    
    // Helper function to track activity for shared connections
    const trackActivity = (responseTime?: number) => {
        if (isUsingSharedConnection && connectionManager) {
            connectionManager.recordActivity(validatedInstanceId, responseTime)
        }
    }

    const hasErrorLabel = (error: any, label: string): boolean => {
        if (!error) {
            return false
        }
        if (Array.isArray(error.errorLabels) && error.errorLabels.includes(label)) {
            return true
        }
        if (error.errorLabelSet instanceof Set && error.errorLabelSet.has(label)) {
            return true
        }
        return false
    }

    const isTransientTransactionError = (error: any): boolean => {
        if (!error) {
            return false
        }
        const message = typeof error.message === 'string' ? error.message : ''
        return hasErrorLabel(error, 'TransientTransactionError')
            || error.code === 251
            || error.codeName === 'NoSuchTransaction'
            || /no such transaction/i.test(message)
    }
    
    const sharedConnectionManagerConfig: ConnectionManagerConfig | undefined = useSharedConnections
        ? {
            ...connectionManagerOverrides,
            maxTotalConnections: connectionManagerOverrides?.maxTotalConnections ?? 900,
            logLevel: connectionManagerOverrides?.logLevel ?? (logLevel === 'all' ? 'info' : 'none'),
            enableMetrics: connectionManagerOverrides?.enableMetrics ?? enableMetrics ?? false
        }
        : undefined

    // Check if we should use shared connections
    if (useSharedConnections) {
        try {
            // Use ConnectionManager for shared connections
            connectionManager = getConnectionManager(sharedConnectionManagerConfig)
            
            const connection = await connectionManager.registerInstance({
                instanceId: validatedInstanceId,
                uri,
                database: dbName,
                config: connectionConfig
            })
            
            client = connection.client
            db = connection.db
            isUsingSharedConnection = true
            currentSharedPoolId = connection.poolId
            mongoConnectionState = MongoConnectionState.CONNECTED
            lastSuccessfulPing = Date.now()

            log(`Using shared connection for instance ${instanceId}`)
        } catch (error) {
            logWarn(`Failed to use shared connection for instance ${instanceId}, falling back to dedicated connection:`, error)
            
            // Fallback to dedicated connection on error
            client = new MongoClient(uri, {
                maxPoolSize: dedicatedMaxPoolSize,
                minPoolSize: dedicatedMinPoolSize,
                maxIdleTimeMS: 30000,
                writeConcern: { w: 1, j: false }
            })
            await client.connect()
            db = client.db(dbName)
            isUsingSharedConnection = false
            connectionManager = null
            currentSharedPoolId = null
            lastSuccessfulPing = Date.now()
        }
    } else {
        // Use dedicated connection (original behavior)
        client = new MongoClient(uri, {
            maxPoolSize: dedicatedMaxPoolSize,
            minPoolSize: dedicatedMinPoolSize,
            maxIdleTimeMS: 30000,
            writeConcern: { w: 1, j: false }
        })
        await client.connect()
        db = client.db(dbName)
        mongoConnectionState = MongoConnectionState.CONNECTED
        currentSharedPoolId = null
        lastSuccessfulPing = Date.now()

        // Start health monitoring
        healthMonitor.startMonitoring(db)
    }
    
    // Initialize TTL monitor after DB connection (do not start yet)
    if (ttlMonitoring) {
        const globalTTL = ttlDays || DEFAULT_TTL_DAYS
        // Disable TTL monitoring for contacts (no TTL for contacts)
        const ttlManagedCollections = ['chats', 'messages', 'state', 'presences']
        ttlMonitor = new TTLMonitor(db, { 
            days: globalTTL, 
            // Safer defaults: require explicit enableMonitoring and use daily checks by default
            enableMonitoring: ttlMonitoring.enableMonitoring === true,
            checkIntervalMinutes: ttlMonitoring.checkIntervalMinutes ?? 1440,
            ...ttlMonitoring,
            collectionPrefix,
            collectionsToCheck: ttlManagedCollections,
            // Silence non-critical TTL warnings unless verbose logging
            onlyCriticalAlerts: ttlMonitoring.onlyCriticalAlerts ?? (logLevel !== 'all')
        })
        // Start will be called after indexes are created
    }
    
    // LID handler will be initialized after ensureConnection is declared
    
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
            labelAssociations: db.collection(`${collectionPrefix}labelAssociations`),
            lidMappings: db.collection(`${collectionPrefix}lidMappings`)
        }
    }
    
    let collections = getCollections()
    
    // Ensure connection is active before operations with enhanced error handling
    const ensureConnection = async (): Promise<void> => {
        if (isUsingSharedConnection && connectionManager && mongoConnectionState === MongoConnectionState.CONNECTED) {
            const poolState = connectionManager.getInstancePoolState(validatedInstanceId)
            if (poolState.pendingMigration && poolState.pendingMigration.toPoolId !== currentSharedPoolId) {
                markConnectionStale('shared pool migration pending')
            } else if (poolState.currentPoolId && currentSharedPoolId && poolState.currentPoolId !== currentSharedPoolId) {
                markConnectionStale('shared pool mismatch')
            }
        }

        // Quick check if already connected
        if (mongoConnectionState === MongoConnectionState.CONNECTED && client) {
            const now = Date.now()
            if (now - lastSuccessfulPing < connectionCheckInterval) {
                return
            }

            try {
                await db.admin().ping()
                lastSuccessfulPing = now
                return
            } catch (error) {
                log(`Connection check failed for instance ${validatedInstanceId}: ${error}`)
                markConnectionStale('ping failure', error)
            }
        }
        
        // If already connecting or reconnecting, wait for it
        if (mongoConnectionState === MongoConnectionState.CONNECTING || mongoConnectionState === MongoConnectionState.RECONNECTING) {
            return new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    connectionStateEmitter.off('connected', onConnected)
                    connectionStateEmitter.off('failed', onFailed)
                    reject(new Error('Connection timeout'))
                }, 30000) // 30 second timeout
                
                const onConnected = () => {
                    clearTimeout(timeout)
                    resolve()
                }
                
                const onFailed = (error: Error) => {
                    clearTimeout(timeout)
                    reject(error)
                }
                
                // Add listeners without removing all - the off() calls above handle cleanup
                connectionStateEmitter.once('connected', onConnected)
                connectionStateEmitter.once('failed', onFailed)
            })
        }
        
        // Mark as connecting/reconnecting
        mongoConnectionState = reconnectAttempts > 0 ? MongoConnectionState.RECONNECTING : MongoConnectionState.CONNECTING
        
        try {
            // For shared connections, we need to re-register
            if (useSharedConnections && connectionManager) {
                try {
                    const connection = await connectionManager.registerInstance({
                        instanceId: validatedInstanceId,
                        uri,
                        database: dbName,
                        config: connectionConfig
                    })
                    
                    client = connection.client
                    db = connection.db
                    isUsingSharedConnection = true
                    currentSharedPoolId = connection.poolId
                    
                    // Refresh collections after reconnection
                    collections = getCollections()
                    
                    // Re-initialize LID handler if needed with retry
                    if (lidHandler) {
                        const reinitResult = await retryWithBackoff(
                            () => lidHandler!.initialize(db, collectionPrefix),
                            { maxAttempts: 3, initialDelay: 500, maxDelay: 5000 },
                            (attempt, error, delay) => {
                                log(`[LID Handler] Retry reconnection attempt ${attempt} after error: ${error.message}. Waiting ${delay}ms...`)
                            }
                        )
                        if (!reinitResult.success) {
                            logWarn(`[LID Handler] Failed to re-initialize after reconnection: ${reinitResult.error}`)
                        }
                    }
                    
                    reconnectAttempts = 0
                    mongoConnectionState = MongoConnectionState.CONNECTED
                    connectionStateEmitter.emit('connected')
                    healthMonitor.updateConnectionState('connected')
                    lastSuccessfulPing = Date.now()
                    log(`Reconnected to MongoDB (shared) for instance ${validatedInstanceId}`)
                } catch (error) {
                    // Fall back to dedicated connection if shared fails
                    logWarn(`Failed to reconnect with shared connection, falling back to dedicated:`, error)
                    
                    client = new MongoClient(uri, {
                        maxPoolSize: dedicatedMaxPoolSize,
                        minPoolSize: dedicatedMinPoolSize,
                        maxIdleTimeMS: 30000,
                        writeConcern: { w: 1, j: false }
                    })
                    await client.connect()
                    db = client.db(dbName)
                    isUsingSharedConnection = false
                    connectionManager = null
                    currentSharedPoolId = null
                    
                    // Refresh collections after reconnection
                    collections = getCollections()
                    
                    // Re-initialize LID handler if needed with retry
                    if (lidHandler) {
                        const reinitResult = await retryWithBackoff(
                            () => lidHandler!.initialize(db, collectionPrefix),
                            { maxAttempts: 3, initialDelay: 500, maxDelay: 5000 },
                            (attempt, error, delay) => {
                                log(`[LID Handler] Retry reconnection attempt ${attempt} after error: ${error.message}. Waiting ${delay}ms...`)
                            }
                        )
                        if (!reinitResult.success) {
                            logWarn(`[LID Handler] Failed to re-initialize after reconnection: ${reinitResult.error}`)
                        }
                    }
                    
                    reconnectAttempts = 0
                    mongoConnectionState = MongoConnectionState.CONNECTED
                    connectionStateEmitter.emit('connected')
                    healthMonitor.updateConnectionState('connected')
                    lastSuccessfulPing = Date.now()
                    log(`Reconnected to MongoDB (dedicated) for instance ${validatedInstanceId}`)
                }
            } else {
                // For dedicated connections, reconnect directly
                if (client) {
                    try {
                        await client.close()
                    } catch (error) {
                        // Ignore close errors
                    }
                }
                
                client = new MongoClient(uri, {
                    maxPoolSize: dedicatedMaxPoolSize,
                    minPoolSize: dedicatedMinPoolSize,
                    maxIdleTimeMS: 30000,
                    writeConcern: { w: 1, j: false }
                })
                await client.connect()
                db = client.db(dbName)
                currentSharedPoolId = null

                // Refresh collections after reconnection
                collections = getCollections()
                
                // Re-initialize LID handler if needed
                if (lidHandler) {
                    await lidHandler.initialize(db, collectionPrefix)
                }
                
                reconnectAttempts = 0
                mongoConnectionState = MongoConnectionState.CONNECTED
                connectionStateEmitter.emit('connected')
                healthMonitor.updateConnectionState('connected')
                lastSuccessfulPing = Date.now()
                log(`Reconnected to MongoDB (dedicated) for instance ${validatedInstanceId}`)
            }
        } catch (error) {
            reconnectAttempts++
            mongoConnectionState = MongoConnectionState.FAILED
            connectionStateEmitter.emit('failed', error)
            healthMonitor.updateConnectionState('failed')
            healthMonitor.recordFailure()
            logError(`Failed to reconnect to MongoDB for instance ${validatedInstanceId}:`, error)
            throw new Error(`Failed to connect to MongoDB: ${(error as Error).message}`)
        } finally {
            // Connection state is managed by enum now
        }
    }
    
    // Initialize LID handler after ensureConnection is available
    if (lidHandlerConfig) {
        // Prepare Redis for LID cache: reuse bull Redis if present, else create a light client from config
        let redisForCache: Redis | null = null
        if (redisConnection) {
            redisForCache = redisConnection
        } else if (redis?.connection) {
            try {
                if (typeof redis.connection === 'string') {
                    redisForCache = new Redis(redis.connection, {
                        maxRetriesPerRequest: null,
                        enableReadyCheck: true,
                        lazyConnect: false
                    })
                } else {
                    redisForCache = new Redis({
                        ...redis.connection,
                        maxRetriesPerRequest: null,
                        enableReadyCheck: true,
                        lazyConnect: false
                    })
                }
                await redisForCache.ping().catch(() => {})
                lidRedisClient = redisForCache
            } catch (_err) {
                redisForCache = null
                lidRedisClient = null
            }
        }
        // Ensure LidHandler uses store's connection lifecycle and skip index creation by default
        lidHandler = new LidHandler(validatedInstanceId, {
            skipIndexCreation: true,
            maxConcurrentLookups: lidHandlerConfig.maxConcurrentLookups ?? finalLidConfig.maxConcurrentLookups,
            negativeCacheTTL: lidHandlerConfig.negativeCacheTTL ?? finalLidConfig.negativeCacheTTL,
            lookupsEnabled: lidHandlerConfig.lookupsEnabled ?? finalLidConfig.lookupsEnabled,
            preferReverseLookupFirst: lidHandlerConfig.preferReverseLookupFirst ?? finalLidConfig.preferReverseLookupFirst,
            contactsQueryMaxTimeMS: lidHandlerConfig.contactsQueryMaxTimeMS ?? finalLidConfig.contactsQueryMaxTimeMS,
            dynamicNegativeBackoff: lidHandlerConfig.dynamicNegativeBackoff ?? finalLidConfig.dynamicNegativeBackoff,
            minNegativeCacheTTL: lidHandlerConfig.minNegativeCacheTTL ?? finalLidConfig.minNegativeCacheTTL,
            maxNegativeCacheTTL: lidHandlerConfig.maxNegativeCacheTTL ?? finalLidConfig.maxNegativeCacheTTL,
            cacheTTLSeconds: (lidHandlerConfig as any).cacheTTLSeconds ?? (finalLidConfig as any).cacheTTLSeconds ?? (3 * 24 * 60 * 60),
            // Prefer reusing existing Redis connection; fallback to lightweight cache client
            redisClient: (redisConnection as any) || (lidRedisClient as any) || undefined,
            ...lidHandlerConfig,
            ensureConnection: ensureConnection
        })
        const initResult = await retryWithBackoff(
            () => lidHandler!.initialize(db, collectionPrefix),
            {
                maxAttempts: 3,
                initialDelay: 500,
                maxDelay: 5000,
                factor: 2,
                jitter: true
            },
            (attempt, error, delay) => {
                logWarn(`[LID Handler] Retry attempt ${attempt} for initialization after error: ${error.message}. Waiting ${delay}ms...`)
            }
        )
        
        if (initResult.success) {
            log(`[LID Handler] Initialized for instance ${validatedInstanceId}`)
        } else {
            logError(`[LID Handler] Failed to initialize after ${initResult.attempts} attempts:`, initResult.error)
            // Don't throw - LID handler is optional
        }
    }
    
    // Enhanced wrapper for operations with retry logic
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
                // Use the default retry logic from connectionRetry
                return isRetryableError(error)
            }
        }
        
        const isConnectionClosedError = (error: any): boolean => {
            if (!error) {
                return false
            }
            const message = typeof error.message === 'string' ? error.message : ''
            return error.name === 'MongoNotConnectedError'
                || error.name === 'MongoExpiredSessionError'
                || error.name === 'MongoPoolClosedError'
                || isTransientTransactionError(error)
                || /session has ended/i.test(message)
                || /closed connection pool/i.test(message)
                || /client was closed/i.test(message)
        }

        const result = await retryWithBackoff(
            async () => {
                await ensureConnection()
                const endPoolOperation = isUsingSharedConnection && connectionManager
                    ? connectionManager.beginInstanceOperation(validatedInstanceId)
                    : null
                try {
                    await waitForBackpressure('db-operation')
                    return await operation()
                } finally {
                    if (endPoolOperation) {
                        endPoolOperation()
                    }
                }
            },
            options,
            (attempt, error, delay) => {
                if (isConnectionClosedError(error)) {
                    markConnectionStale('retry detected closed session', error)
                }
                const retryLogKey = `retry-${validatedInstanceId}-${error?.name || 'unknown'}`
                if (shouldLogOnce(retryLogKey, 5)) {
                    const retryMessage = error?.message ?? 'Unknown error'
                    logWarn(`[withConnection] Retry attempt ${attempt} for instance ${validatedInstanceId} after error: ${retryMessage}. Waiting ${delay}ms...`)
                }
            }
        )

        if (!result.success) {
            if (isConnectionClosedError(result.error)) {
                markConnectionStale('retry budget exhausted', result.error)
            }
            logError(`[withConnection] Operation failed after ${result.attempts} attempts:`, result.error)
            healthMonitor.recordFailure()
            throw result.error
        }
        
        // Track success metrics
        const responseTime = Date.now() - startTime
        trackActivity(responseTime)
        healthMonitor.recordSuccess(responseTime)
        lastSuccessfulPing = Date.now()

        return result.result as T
    }
    
    // Queue configuration
    const BATCH_SIZE = 100
    
    // Bull queue setup (if Redis provided)
    let bullInitialized = false
    const queues: Map<QueueType, Queue<any>> = new Map()
    const workers: Map<QueueType, Worker<any>> = new Map()
    let sharedQueueManager: SharedQueueManager | null = null
    const useSharedQueues = redis?.useSharedQueues !== false // Default to true if Redis is provided
    
    // Track background tasks for cleanup
    let profilePictureFetchHandle: NodeJS.Immediate | null = null
    let isClosing = false
    
    // Operation locking for clearAll to prevent race conditions
    let clearAllInProgress = false
    const pendingOperations = new Set<Promise<any>>()
    const pendingOperationsMetadata = new Map<Promise<any>, { startTime: number, description?: string }>()
    let historyDebounceTimer: NodeJS.Timeout | null = null
    const pendingHistoryData: any[] = []
    let staleOperationCleanupTimer: NodeJS.Timeout | null = null
    
    // Helper function to track operations with metadata
    const trackOperation = (promise: Promise<any>, description?: string) => {
        pendingOperations.add(promise)
        pendingOperationsMetadata.set(promise, {
            startTime: Date.now(),
            description
        })
        
        // Clean up when promise completes
        promise.finally(() => {
            pendingOperations.delete(promise)
            pendingOperationsMetadata.delete(promise)
        })
        
        return promise
    }
    
    // Periodic cleanup of stale operations
    const cleanupStaleOperations = () => {
        const now = Date.now()
        const staleThreshold = config.staleOperationThreshold || 5 * 60 * 1000 // Default 5 minutes
        let cleaned = 0
        
        for (const [promise, metadata] of pendingOperationsMetadata.entries()) {
            if (now - metadata.startTime > staleThreshold) {
                pendingOperations.delete(promise)
                pendingOperationsMetadata.delete(promise)
                cleaned++
                logWarn(`[${instanceId}] Cleaned stale operation: ${metadata.description || 'unknown'} (age: ${Math.round((now - metadata.startTime) / 1000)}s)`)
            }
        }
        
        if (cleaned > 0) {
            log(`[${instanceId}] Cleaned ${cleaned} stale operations`)
        }
    }
    
    // Start periodic cleanup
    staleOperationCleanupTimer = setInterval(() => {
        cleanupStaleOperations()
    }, config.staleOperationCleanupInterval || 2 * 60 * 1000) // Default 2 minutes
    
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
    
    // Helper function to queue jobs for both shared and per-instance modes
    // @ts-ignore - Function is used conditionally
    const queueJob = async (jobType: JobType, queueType: QueueType, data: any, priority: number = 5): Promise<boolean> => {
        try {
            if (sharedQueueManager && useSharedQueues) {
                // Use shared queue manager
                await sharedQueueManager.addJob(jobType, data, validatedInstanceId, priority)
                log(`✅ Job queued via shared queue: ${jobType}`)
                return true
            } else if (bullInitialized && queues.has(queueType)) {
                // Use per-instance queue
                const queue = queues.get(queueType)!
                await queue.add(
                    jobType,
                    {
                        ...data,
                        instanceId: validatedInstanceId,
                        timestamp: Date.now()
                    },
                    defaultJobOptions
                )
                log(`✅ Job queued via per-instance queue: ${jobType}`)
                return true
            }
            return false
        } catch (error) {
            logError(`❌ Failed to queue job ${jobType}:`, error)
            return false
        }
    }
    // Register processors for shared queue manager
    const registerSharedQueueProcessors = async () => {
        if (!sharedQueueManager) return

        const ownership = await sharedQueueManager.claimInstanceOwnership(validatedInstanceId)
        if (!ownership.owned && ownership.ownerId && ownership.ownerId !== sharedQueueManager.getWorkerId()) {
            logWarn(`[${instanceId}] Shared queue ownership currently held by ${ownership.ownerId}. Jobs will be deferred until ownership changes.`)
        } else if (ownership.owned) {
            log(`[${instanceId}] Shared queue ownership claimed by worker ${sharedQueueManager.getWorkerId()}`)
        }
        
        // Messages processor - use instance-specific registration to fix singleton issue
        sharedQueueManager.registerInstanceProcessor(validatedInstanceId, JobType.MESSAGES, async (job) => {
            const { data, instanceId: jobInstanceId } = job.data
            
            // Only process jobs for this instance
            if (jobInstanceId !== validatedInstanceId) {
                return { skipped: true, reason: 'Different instance' }
            }
            
            const { type, message, messageId, update, deleteIds, jid } = data as MessageJob
            
            trackActivity() // Track request
            
            if (type === 'upsert' && message) {
                // [Message processing logic - same as existing]
                // Skip protocol messages that shouldn't be stored
                if (message.message?.protocolMessage) {
                    const protoType = message.message.protocolMessage.type
                    
                    // Handle REVOKE messages
                    if (protoType === proto.Message.ProtocolMessage.Type.REVOKE && message.message.protocolMessage.key) {
                        const revokedKey = message.message.protocolMessage.key
                        const outerChatJid = message.key.remoteJid || jid
                        
                        // Prefer outer chat JID and normalize via LID if available
                        let targetJid = outerChatJid
                        if (lidHandler && targetJid) {
                            try {
                                const normalized = await (lidHandler as any).normalizeJid(targetJid)
                                if (normalized) targetJid = normalized
                            } catch {}
                        }
                        
                        const baseSet: any = {
                            'message.protocolMessage': message.message?.protocolMessage,
                            revoked: true,
                            revokedAt: new Date(),
                            revokedBy: message.key.fromMe ? 'me' : message.key.participant || message.key.remoteJid,
                            messageStubType: 1
                        }
                        
                        let updateResult = await withConnection(async () =>
                            collections.messages.updateOne(
                                { instanceId: validatedInstanceId, jid: targetJid, 'key.id': revokedKey.id },
                                { $set: baseSet }
                            )
                        )
                        
                        if (updateResult.matchedCount === 0 && targetJid !== outerChatJid) {
                            updateResult = await withConnection(async () =>
                                collections.messages.updateOne(
                                    { instanceId: validatedInstanceId, jid: outerChatJid, 'key.id': revokedKey.id },
                                    { $set: baseSet }
                                )
                            )
                        }
                        
                        if (updateResult.matchedCount === 0) {
                            await withConnection(async () =>
                                collections.messages.updateOne(
                                    { instanceId: validatedInstanceId, 'key.id': revokedKey.id },
                                    { $set: baseSet }
                                )
                            )
                        }
                        return { success: true, type: 'revoke' }
                    }
                    
                    // Skip other protocol messages
                    const skipTypes = [
                        proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION,
                        proto.Message.ProtocolMessage.Type.APP_STATE_SYNC_KEY_SHARE,
                        proto.Message.ProtocolMessage.Type.INITIAL_SECURITY_NOTIFICATION_SETTING_SYNC,
                        proto.Message.ProtocolMessage.Type.APP_STATE_SYNC_KEY_REQUEST
                    ]
                    
                    if (protoType && skipTypes.includes(protoType)) {
                        return { success: true, skipped: true }
                    }
                }
                
                // Store the message
                await withConnection(async () =>
                    collections.messages.replaceOne(
                        {
                            instanceId: validatedInstanceId,
                            jid,
                            'key.id': message.key?.id
                        },
                        {
                            ...message,
                            instanceId: validatedInstanceId,
                            jid,
                            updatedAt: new Date()
                        },
                        { upsert: true }
                    )
                )
                
                return { success: true, type: 'upsert' }
            } else if (type === 'update' && update) {
                // Update message
                await withConnection(async () =>
                    collections.messages.updateOne(
                        {
                            instanceId: validatedInstanceId,
                            jid,
                            'key.id': messageId
                        },
                        { $set: { ...update, updatedAt: new Date() } }
                    )
                )
                return { success: true, type: 'update' }
            } else if (type === 'delete' && deleteIds) {
                // Mark messages as revoked instead of deleting
                await withConnection(async () =>
                    collections.messages.updateMany(
                        {
                            instanceId: validatedInstanceId,
                            jid,
                            'key.id': { $in: deleteIds }
                        },
                        {
                            $set: {
                                revoked: true,
                                revokedAt: new Date(),
                                updatedAt: new Date()
                            }
                        }
                    )
                )
                return { success: true, type: 'delete', count: deleteIds.length, marked: true }
            }
            
            return { success: false, error: 'Unknown message job type' }
        })
        
        // Contacts processor - use instance-specific registration
        sharedQueueManager.registerInstanceProcessor(validatedInstanceId, JobType.CONTACTS, async (job) => {
            const { data } = job.data
            
            const { type, contacts, contact } = data as ContactJob
            
            trackActivity()
            
            if (type === 'upsert' && contacts) {
                // Fetch existing contacts to preserve names and profile pictures
                const ids = contacts.map(c => c.id)
                const existingContacts: any[] = []
                for (let i = 0; i < ids.length; i += 1000) {  // Reduced from 5000 to optimize $in performance
                    const idChunk = ids.slice(i, i + 1000)
                    const chunk = await withConnection(async () =>
                        collections.contacts.find(
                            { instanceId: validatedInstanceId, id: { $in: idChunk } },
                            { projection: { id: 1, name: 1, profilePic: 1, profilePicUpdatedAt: 1, notify: 1, ...(lidConfig?.enabled ? { lid: 1 } : {}) } }
                        ).toArray()
                    ) as any[]
                    existingContacts.push(...chunk)
                }

                const existingDataMap = new Map(
                    existingContacts.map(c => [c.id, {
                        name: c.name,
                        profilePic: c.profilePic,
                        profilePicUpdatedAt: c.profilePicUpdatedAt,
                        notify: c.notify,
                        ...(lidConfig?.enabled ? { lid: c.lid } : {})
                    }])
                )

                // Bulk upsert without overriding user-saved notify
                const bulkOps = contacts.map((contact: Contact) => {
                    // Handle null name: fallback to notify or verifiedName, or skip if both are null
                    if (contact.name === null) {
                        contact.name = contact.notify || contact.verifiedName || undefined;
                    }

                    const { notify, id: _ignoredId, instanceId: _ignoredInstanceId, ...rest } = (contact as any) || {}
                    const existing = existingDataMap.get(contact.id)

                    // Always include updatedAt
                    const setData: any = {
                        ...rest,
                        updatedAt: new Date()
                    }

                    // Preserve existing name if new name is null/undefined/empty
                    if (existing?.name && (!setData.name || setData.name.trim() === '')) {
                        setData.name = existing.name
                    }

                    // Preserve existing profile picture data if it exists
                    if (existing?.profilePic) {
                        setData.profilePic = existing.profilePic
                        setData.profilePicUpdatedAt = existing.profilePicUpdatedAt
                    }

                    // Remove name from setData if it's null/undefined/empty after preservation
                    if (setData.name == null || setData.name.trim() === '') {
                        delete setData.name
                    }

                    // Base update with $set (always present with updatedAt)
                    const update: any = {
                        $set: setData,
                        $setOnInsert: {
                            instanceId,
                            id: contact.id,
                            ...(notify !== undefined ? { notify } : {})
                        }
                    }

                    // Add $unset if needed to clean up existing null name
                    if (existing && existing.name === null && !setData.name) {
                        update.$unset = { name: 1 }
                    }

                    return {
                        updateOne: {
                            filter: { instanceId, id: contact.id },
                            update,
                            upsert: true
                        }
                    }
                })

                await withConnection(async () =>
                    collections.contacts.bulkWrite(bulkOps)
                )

                return { success: true, count: contacts.length }
            } else if (type === 'update' && contact) {
                // Fetch existing contact to preserve name and profile picture
                const existingContact = await withConnection(async () =>
                    collections.contacts.findOne(
                        { instanceId: validatedInstanceId, id: contact.id },
                        { projection: { name: 1, profilePic: 1, profilePicUpdatedAt: 1, notify: 1, ...(lidConfig?.enabled ? { lid: 1 } : {}) } }
                    )
                ) as any

                const { notify, id: _ignoredId, instanceId: _ignoredInstanceId, ...rest } = (contact as any) || {}

                // Always include updatedAt
                const setData: any = {
                    ...rest,
                    updatedAt: new Date()
                }

                // Preserve existing name if new name is null/undefined/empty
                if (existingContact?.name && (!setData.name || setData.name.trim() === '')) {
                    setData.name = existingContact.name
                }

                // Preserve existing profile picture data if it exists
                if (existingContact?.profilePic) {
                    setData.profilePic = existingContact.profilePic
                    setData.profilePicUpdatedAt = existingContact.profilePicUpdatedAt
                }

                await withConnection(async () =>
                    collections.contacts.updateOne(
                        { instanceId: validatedInstanceId, id: contact.id },
                        {
                            $set: setData,
                            $setOnInsert: {
                                instanceId,
                                id: contact.id,
                                ...(notify !== undefined ? { notify } : {})
                            }
                        },
                        { upsert: true }
                    )
                )

                return { success: true }
            }
            
            return { success: false, error: 'Unknown contact job type' }
        })
        
        // Chats processor - use instance-specific registration
        sharedQueueManager.registerInstanceProcessor(validatedInstanceId, JobType.CHATS, async (job) => {
            const { data } = job.data
            
            const { type, chats, chatId, update, deleteIds } = data as ChatJob
            
            trackActivity()
            
            if (type === 'upsert' && chats) {
                const bulkOps = chats.map((chat: Chat) => ({
                    replaceOne: {
                        filter: { instanceId: validatedInstanceId, id: chat.id },
                        replacement: {
                            ...chat,
                            instanceId: validatedInstanceId,
                            updatedAt: new Date()
                        },
                        upsert: true
                    }
                }))
                
                await withConnection(async () =>
                    collections.chats.bulkWrite(bulkOps)
                )
                
                return { success: true, count: chats.length }
            } else if (type === 'update' && update && chatId) {
                await withConnection(async () =>
                    collections.chats.updateOne(
                        { instanceId: validatedInstanceId, id: chatId },
                        { $set: { ...update, updatedAt: new Date() } }
                    )
                )
                
                return { success: true }
            } else if (type === 'delete' && deleteIds) {
                await withConnection(async () =>
                    collections.chats.deleteMany({
                        instanceId: validatedInstanceId,
                        id: { $in: deleteIds }
                    })
                )
                
                return { success: true, count: deleteIds.length }
            }
            
            return { success: false, error: 'Unknown chat job type' }
        })
        
        // Group metadata processor - use instance-specific registration
        sharedQueueManager.registerInstanceProcessor(validatedInstanceId, JobType.GROUP_METADATA, async (job) => {
            const { data } = job.data
            
            const { type, jid, metadata, update } = data as GroupMetadataJob
            
            trackActivity()
            
            if (type === 'upsert' && metadata) {
                await withConnection(async () =>
                    collections.groupMetadata.replaceOne(
                        { instanceId: validatedInstanceId, id: jid },
                        {
                            ...metadata,
                            instanceId: validatedInstanceId,
                            updatedAt: new Date()
                        },
                        { upsert: true }
                    )
                )
                
                return { success: true }
            } else if (type === 'update' && update) {
                await withConnection(async () =>
                    collections.groupMetadata.updateOne(
                        { instanceId: validatedInstanceId, id: jid },
                        { $set: { ...update, updatedAt: new Date() } }
                    )
                )
                
                return { success: true }
            }
            
            return { success: false, error: 'Unknown group metadata job type' }
        })
        
        // Profile pictures processor - use instance-specific registration
        sharedQueueManager.registerInstanceProcessor(validatedInstanceId, JobType.PROFILE_PICTURES, async (job) => {
            const { data } = job.data
            
            const { contactId, retryCount = 0 } = data as ProfilePictureJob
            
            if (!sock || !profilePictureConfig?.enabled) {
                return { success: false, error: 'Profile picture fetching not configured' }
            }
            
            const maxRetries = profilePictureConfig.retryAttempts || 3
            const requestDelay = profilePictureConfig.requestDelay || 500
            
            // Add delay between requests to avoid rate limiting
            if (requestDelay > 0) {
                await new Promise(resolve => setTimeout(resolve, requestDelay))
            }
            
            try {
                // Fetch profile picture URL using Baileys sock
                const profilePictureUrl = await sock.profilePictureUrl(contactId)
                
                if (profilePictureUrl) {
                    await withConnection(async () =>
                        collections.contacts.updateOne(
                            { instanceId: validatedInstanceId, id: contactId },
                            { 
                                $set: { 
                                    profilePic: profilePictureUrl,
                                    profilePicUpdatedAt: new Date(),
                                    updatedAt: new Date()
                                } 
                            }
                        )
                    )
                    
                    return { success: true, profilePictureUrl }
                } else {
                    return { success: true, profilePictureUrl: null }
                }
            } catch (error: any) {
                // Handle privacy errors silently
                const isPrivacyError = error.message?.includes('privacy') || 
                                      error.message?.includes('401') ||
                                      error.message?.includes('not authorized')
                
                if (isPrivacyError) {
                    return { success: true, privacyRestricted: true }
                }
                
                // For other errors, retry if attempts remaining
                if (retryCount < maxRetries - 1) {
                    // Re-queue with incremented retry count
                    await sharedQueueManager!.addJob(
                        JobType.PROFILE_PICTURES,
                        {
                            ...data,
                            retryCount: retryCount + 1,
                            lastError: error.message
                        },
                        validatedInstanceId,
                        3 // Lower priority for retries
                    )
                    return { success: true, requeued: true }
                }
                
                throw error
            }
        })
        
        // Media download processor - use instance-specific registration
        sharedQueueManager.registerInstanceProcessor(validatedInstanceId, JobType.MEDIA_DOWNLOAD, async (job) => {
            const { data } = job.data
            
            const { message } = data
            
            if (!config.media?.enabled) {
                return { success: false, error: 'Media download not configured' }
            }
            
            try {
                const attemptNumber = (job.attemptsMade ?? 0) + 1
                
                // Check for existing media by hash
                const checkExistingMedia = async (fileHash: string) => {
                    const existingMessage = await withConnection(async () =>
                        collections.messages.findOne({
                            'mediaInfo.fileHash': fileHash
                        })
                    )
                    
                    return (existingMessage as any)?.mediaUrl || null
                }
                
                // Determine if this is Official API media
                const isOfficialAPI = (message as any).official_api === true
                
                let mediaResult
                if (isOfficialAPI) {
                    mediaResult = await downloadOfficialAPIMedia(
                        message,
                        validatedInstanceId,
                        config.media,
                        config.logger,
                        checkExistingMedia,
                        {
                            attempt: attemptNumber
                        }
                    )
                } else {
                    mediaResult = await downloadMedia(
                        message,
                        validatedInstanceId,
                        config.media,
                        config.logger,
                        checkExistingMedia
                    )
                }
                
                if (mediaResult.success && mediaResult.localPath) {
                    // Update message with media URL
                    const mediaUpdate: Record<string, any> = {
                        mediaUrl: mediaResult.localPath,
                        mediaDownloadedAt: new Date()
                    }
                    if (mediaResult.mediaType) mediaUpdate.mediaType = mediaResult.mediaType
                    if (mediaResult.fileName) mediaUpdate.mediaFileName = mediaResult.fileName
                    if (typeof mediaResult.fileSize === 'number') mediaUpdate.mediaFileSize = mediaResult.fileSize
                    if (mediaResult.mediaHash) mediaUpdate.mediaHash = mediaResult.mediaHash
                    if (typeof mediaResult.reused !== 'undefined') mediaUpdate.mediaReused = mediaResult.reused
                    
                    await withConnection(async () =>
                        collections.messages.updateOne(
                            {
                                instanceId: validatedInstanceId,
                                'key.id': message.key?.id
                            },
                            {
                                $set: mediaUpdate
                            }
                        )
                    )
                    
                    return { success: true, mediaUrl: mediaResult.localPath }
                } else {
                    const errorMessage = mediaResult.error || 'Download failed'
                    logWarn(`[Media Download Queue] Attempt ${attemptNumber} failed for ${message.key?.id}: ${errorMessage}`)
                    throw new Error(errorMessage)
                }
            } catch (error: any) {
                logError(`❌ [Media Download Queue] Failed to download media:`, error)
                throw error
            }
        })
        
        // State processor - use instance-specific registration
        sharedQueueManager.registerInstanceProcessor(validatedInstanceId, JobType.STATE, async (job) => {
            const { data } = job.data
            
            const { update } = data as StateJob
            
            trackActivity()
            
            await withConnection(async () =>
                collections.state.replaceOne(
                    { instanceId: validatedInstanceId },
                    {
                        ...update,
                        instanceId: validatedInstanceId,
                        updatedAt: new Date()
                    } as any,
                    { upsert: true }
                )
            )
            
            return { success: true }
        })
        
        // Presences processor - use instance-specific registration
        sharedQueueManager.registerInstanceProcessor(validatedInstanceId, JobType.PRESENCES, async (job) => {
            const { data } = job.data
            
            const { id, presences } = data as PresenceJob
            
            trackActivity()
            
            await withConnection(async () =>
                collections.presences.replaceOne(
                    { instanceId: validatedInstanceId, id },
                    {
                        instanceId: validatedInstanceId,
                        id,
                        presences,
                        updatedAt: new Date()
                    },
                    { upsert: true }
                )
            )
            
            return { success: true }
        })
        
        // Labels processor - use instance-specific registration
        sharedQueueManager.registerInstanceProcessor(validatedInstanceId, JobType.LABELS, async (job) => {
            const { data } = job.data
            
            const { type, id, label } = data as LabelJob
            
            trackActivity()
            
            if (type === 'upsert' && label) {
                await withConnection(async () =>
                    collections.labels.replaceOne(
                        { instanceId: validatedInstanceId, id },
                        {
                            ...label,
                            instanceId: validatedInstanceId,
                            updatedAt: new Date()
                        },
                        { upsert: true }
                    )
                )
                
                return { success: true }
            } else if (type === 'delete') {
                await withConnection(async () =>
                    collections.labels.deleteOne({
                        instanceId: validatedInstanceId,
                        id
                    })
                )
                
                return { success: true }
            }
            
            return { success: false, error: 'Unknown label job type' }
        })
        
        // Label associations processor - use instance-specific registration
        sharedQueueManager.registerInstanceProcessor(validatedInstanceId, JobType.LABEL_ASSOCIATIONS, async (job) => {
            const { data } = job.data
            
            const { type, association } = data as LabelAssociationJob
            
            trackActivity()
            
            // const associationId = `${association.labelId}_${association.chatId || (association as any).messageId}`
            
            if (type === 'upsert') {
                await withConnection(async () =>
                    collections.labelAssociations.replaceOne(
                        {
                            instanceId: validatedInstanceId,
                            labelId: association.labelId,
                            chatId: association.chatId,
                            messageId: (association as any).messageId
                        },
                        {
                            ...association,
                            instanceId: validatedInstanceId,
                            updatedAt: new Date()
                        },
                        { upsert: true }
                    )
                )
                
                return { success: true }
            } else if (type === 'delete') {
                await withConnection(async () =>
                    collections.labelAssociations.deleteOne({
                        instanceId: validatedInstanceId,
                        labelId: association.labelId,
                        chatId: association.chatId,
                        messageId: (association as any).messageId
                    })
                )
                
                return { success: true }
            }
            
            return { success: false, error: 'Unknown label association job type' }
        })
        
        log(`✅ Registered all shared queue processors for instance ${instanceId}`)
    }
    // Initialize Bull queues if Redis config provided
    const initializeBullQueues = async () => {
        if (!redis) return
        
        try {
            // Check if we should use shared queues
            if (useSharedQueues) {
                log(`🚀 Initializing shared queue manager for instance ${instanceId}...`)
                
                // Configure shared queue manager
                const sharedConfig: SharedQueueManagerConfig = {
                    redis: {
                        connection: redis.connection
                    },
                    queueConcurrency: redis.queueConcurrency,
                    enableMetrics: enableMetrics,
                    logLevel: logLevel as any
                }
                
                // Get or create singleton instance
                sharedQueueManager = SharedQueueManager.getInstance(sharedConfig)
                
                // Register processors for shared queues
                await registerSharedQueueProcessors()
                
                bullInitialized = true
                log(`✅ Shared queue manager initialized successfully for instance ${instanceId}`)
                return
            }
            
            // Fall back to per-instance queues (original implementation)
            log(`🐂 Initializing per-instance Bull queues for instance ${instanceId}...`)
            
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
            
            // Set reasonable listener limit instead of unlimited
            redisConnection.setMaxListeners(30)
            
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
                let concurrency: number
                if (queueType === QueueType.LABEL_ASSOCIATIONS || queueType === QueueType.CONTACTS) {
                    concurrency = 1
                } else if (queueType === QueueType.PROFILE_PICTURES) {
                    concurrency = profilePictureConfig?.maxConcurrent || 5
                } else {
                    concurrency = redis.concurrency || 50
                }
                
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
                    if (queueType !== QueueType.LABEL_ASSOCIATIONS) {
                        log(`✅ ${queueType} job ${job.id} completed`)
                    }
                }
                
                const failedHandler = (job: Job<T> | undefined, err: Error) => {
                    logError(`❌ ${queueType} job ${job?.id} failed:`, err.message)
                    if (enableMetrics) {
                        updateEventMetrics(queueType, 'error')
                    }
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
            }
            
            // Create queues for different event types
            createQueueAndWorker<MessageJob>(QueueType.MESSAGES, async (job) => {
                const jobStartTime = Date.now()
                trackActivity() // Track request
                const { type, message, messageId, update, deleteIds, jid } = job.data
                
                if (type === 'upsert' && message) {
                    // Skip protocol messages that shouldn't be stored as regular messages
                    if (message.message?.protocolMessage) {
                        const protoType = message.message.protocolMessage.type
                        
                        // Handle REVOKE messages - update the revoked message instead of storing the revoke message
                        if (protoType === proto.Message.ProtocolMessage.Type.REVOKE && message.message.protocolMessage.key) {
                            const revokedKey = message.message.protocolMessage.key
                            const outerChatJid = message.key.remoteJid || jid
                            
                            // Prefer outer chat JID and normalize via LID if available
                            let targetJid = outerChatJid
                            if (lidHandler && targetJid) {
                                try {
                                    const normalized = await (lidHandler as any).normalizeJid(targetJid)
                                    if (normalized) targetJid = normalized
                                } catch {}
                            }
                            
                            try {
                                const baseSet: any = {
                                    'message.protocolMessage': message.message?.protocolMessage,
                                    revoked: true,
                                    revokedAt: new Date(),
                                    revokedBy: message.key.fromMe ? 'me' : message.key.participant || message.key.remoteJid,
                                    messageStubType: 1
                                }
                                
                                let updateResult = await withConnection(async () =>
                                    collections.messages.updateOne(
                                        { instanceId, jid: targetJid, 'key.id': revokedKey.id },
                                        { $set: baseSet }
                                    )
                                )
                                
                                if (updateResult.matchedCount === 0 && targetJid !== outerChatJid) {
                                    updateResult = await withConnection(async () =>
                                        collections.messages.updateOne(
                                            { instanceId, jid: outerChatJid, 'key.id': revokedKey.id },
                                            { $set: baseSet }
                                        )
                                    )
                                }
                                
                                if (updateResult.matchedCount === 0) {
                                    updateResult = await withConnection(async () =>
                                        collections.messages.updateOne(
                                            { instanceId, 'key.id': revokedKey.id },
                                            { $set: baseSet }
                                        )
                                    )
                                }
                                
                                if (updateResult.matchedCount > 0) {
                                    log(`✅ [Bull Queue REVOKE] Successfully marked message ${revokedKey.id} as revoked`)
                                } else {
                                    log(`⚠️ [Bull Queue REVOKE] Message ${revokedKey.id} not found to revoke`)
                                }
                            } catch (error) {
                                logError(`[Bull Queue REVOKE] Failed to process revoke for message ${revokedKey.id}:`, error)
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
                            log(`⏭️ [Bull Queue Protocol] Skipping protocol message of type ${protoType} for instance ${instanceId}`)
                            return
                        }
                    }
                    
                    // Note: JID normalization is done in upsertMessage before queuing
                    // The jid here is already normalized through LID handler
                    
                    // Resolve quoted message if present
                    if (message.message?.extendedTextMessage?.contextInfo?.stanzaId && 
                        (!message.message.extendedTextMessage.contextInfo.quotedMessage || 
                         Object.keys(message.message.extendedTextMessage.contextInfo.quotedMessage).length === 0)) {
                        
                        const quotedMsg = await resolveQuotedMessage(message, jid, collections, instanceId, log, withConnection)
                        if (quotedMsg && quotedMsg.message) {
                            // Update the message with the resolved quoted content
                            message.message.extendedTextMessage.contextInfo.quotedMessage = quotedMsg.message
                            log(`✅ [Bull Queue] Updated message with resolved quoted content for ${message.key?.id}`)
                        }
                    }
                    
                    // Decrypt poll vote if present
                    let pollVoteDecrypted = null
                    if (message.message?.pollUpdateMessage) {
                        pollVoteDecrypted = await decryptPollVote(message, collections, instanceId, meId, log, withConnection)
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
                        const originalMessage = await withConnection(async () =>
                            collections.messages.findOne({
                                instanceId,
                                jid,
                                'key.id': editTargetKey.id
                            })
                        )
                        
                        if (originalMessage && originalMessage.messageTimestamp) {
                            preservedTimestamp = originalMessage.messageTimestamp
                            log(`⏰ [Bull Queue] Preserving original messageTimestamp: ${preservedTimestamp} for edited message ${editTargetKey.id}`)
                        }
                    }
                    
                    try {
                        await withConnection(async () =>
                            collections.messages.replaceOne(
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
                        )
                    } catch (error: any) {
                        // Handle duplicate key errors gracefully
                        if (error.code === 11000 || error.message?.includes('duplicate key')) {
                            log(`⚠️ [Bull Queue] Duplicate key error for message ${message.key?.id} in chat ${jid} - message already exists`)
                            // Try to update instead of replace
                            try {
                                await withConnection(async () =>
                                    collections.messages.updateOne(
                                        {
                                            instanceId,
                                            jid,
                                            'key.id': message.key?.id
                                        },
                                        {
                                            $set: {
                                                ...message,
                                                ...(preservedTimestamp && { messageTimestamp: preservedTimestamp }),
                                                ...(pollVoteDecrypted && { pollVoteDecrypted }),
                                                updatedAt: new Date()
                                            }
                                        }
                                    )
                                )
                                log(`✅ [Bull Queue] Successfully updated existing message ${message.key?.id} after duplicate key error`)
                            } catch (updateError) {
                                logError(`[Bull Queue] Failed to update message after duplicate key error:`, updateError)
                            }
                        } else {
                            throw error
                        }
                    }
                    
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
                                const existing = await withConnection(async () =>
                                    collections.messages.findOne({
                                        instanceId,
                                        mediaHash: hash,
                                        mediaUrl: { $exists: true }
                                    })
                                ) as any
                                return existing?.mediaUrl || null
                            }
                            
                            try {
                                const mediaResult = await downloadOfficialAPIMedia(message, instanceId, config.media, config.logger, checkExistingMedia)
                                
                                if (mediaResult.success && mediaResult.localPath) {
                                    await withConnection(async () =>
                                        collections.messages.updateOne(
                                            { 
                                                instanceId: validatedInstanceId, 
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
                    const existingMsg = await withConnection(async () =>
                        collections.messages.findOne({
                            instanceId,
                            jid,
                            'key.id': messageId
                        })
                    ) as any
                    
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
                            
                            await withConnection(async () =>
                                collections.messages.updateOne(
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
                            )
                        } else {
                            // No existing quoted message, proceed with update
                            await withConnection(async () =>
                                collections.messages.updateOne(
                                    {
                                        instanceId,
                                        jid,
                                        'key.id': messageId
                                    },
                                    {
                                        $set: { ...finalUpdate, updatedAt: new Date() }
                                    }
                                )
                            )
                        }
                    } else {
                        // No existing message, just apply the update
                        await withConnection(async () =>
                            collections.messages.updateOne(
                                {
                                    instanceId,
                                    jid,
                                    'key.id': messageId
                                },
                                {
                                    $set: { ...update, updatedAt: new Date() }
                                }
                            )
                        )
                    }
                } else if (type === 'delete') {
                    // Mark messages as revoked/deleted instead of deleting
                    if (deleteIds && deleteIds.length > 0) {
                        await withConnection(async () =>
                            collections.messages.updateMany(
                                { instanceId, jid, 'key.id': { $in: deleteIds } },
                                { $set: { revoked: true, revokedAt: new Date(), updatedAt: new Date() } }
                            )
                        )
                    } else {
                        await withConnection(async () =>
                            collections.messages.updateMany(
                                { instanceId, jid },
                                { $set: { deleted: true, deletedAt: new Date(), updatedAt: new Date() } }
                            )
                        )
                    }
                }
                
                trackActivity(Date.now() - jobStartTime) // Track response time
                return { success: true }
            })
            
            createQueueAndWorker<ChatJob>(QueueType.CHATS, async (job) => {
                const jobStartTime = Date.now()
                trackActivity() // Track request
                const { type, chats, chatId, update, deleteIds } = job.data
                
                if (type === 'upsert' && chats) {
                    const bulkOps = chats.map((chat: Chat) => ({
                        replaceOne: {
                            filter: { instanceId, id: chat.id },
                            replacement: { ...chat, instanceId, updatedAt: new Date() },
                            upsert: true
                        }
                    }))
                    await withConnection(async () =>
                        collections.chats.bulkWrite(bulkOps)
                    )
                } else if (type === 'update' && chatId && update) {
                    await withConnection(async () =>
                        collections.chats.updateOne(
                            { instanceId, id: chatId },
                            { $set: { ...update, updatedAt: new Date() } }
                        )
                    )
                } else if (type === 'delete' && deleteIds) {
                    await withConnection(async () =>
                        collections.chats.deleteMany({
                            instanceId,
                            id: { $in: deleteIds }
                        })
                    )
                }
                
                trackActivity(Date.now() - jobStartTime) // Track response time
                return { success: true }
            })
            
            createQueueAndWorker<ContactJob>(QueueType.CONTACTS, async (job) => {
                const { type, contacts, contact } = job.data
                const jobStartTime = Date.now()
                trackActivity() // Track request
                
                if (type === 'upsert' && contacts) {
                    // Fetch existing contacts to preserve names and profile pictures
                    const ids = contacts.map(c => c.id)
                    const existingContacts: any[] = []
                    for (let i = 0; i < ids.length; i += 1000) {  // Reduced from 5000 to optimize $in performance
                        const idChunk = ids.slice(i, i + 1000)
                        const chunk = await withConnection(async () =>
                            collections.contacts.find(
                                { instanceId, id: { $in: idChunk } },
                                { projection: { id: 1, name: 1, profilePic: 1, profilePicUpdatedAt: 1, notify: 1, ...(lidConfig?.enabled ? { lid: 1 } : {}) } }
                            ).toArray()
                        ) as any[]
                        existingContacts.push(...chunk)
                    }

                    const existingDataMap = new Map(
                        existingContacts.map(c => [c.id, {
                            name: c.name,
                            profilePic: c.profilePic,
                            profilePicUpdatedAt: c.profilePicUpdatedAt,
                            notify: c.notify,
                            ...(lidConfig?.enabled ? { lid: c.lid } : {})
                        }])
                    )

                    const bulkOps = contacts.map(contact => {
                        // Handle null name: fallback to notify or verifiedName, or skip if both are null
                        if (contact.name === null) {
                            contact.name = contact.notify || contact.verifiedName || undefined;
                        }

                        const { notify, id: _ignoredId, instanceId: _ignoredInstanceId, ...rest } = (contact as any) || {}
                        const existing = existingDataMap.get(contact.id)

                        // Always include updatedAt
                        const setData: any = {
                            ...rest,
                            updatedAt: new Date()
                        }

                        // Preserve existing name if new name is null/undefined/empty
                        if (existing?.name && (!setData.name || setData.name.trim() === '')) {
                            setData.name = existing.name
                        }

                        // Preserve existing profile picture data if it exists
                        if (existing?.profilePic) {
                            setData.profilePic = existing.profilePic
                            setData.profilePicUpdatedAt = existing.profilePicUpdatedAt
                        }

                        // Remove name from setData if it's null/undefined/empty after preservation
                        if (setData.name == null || setData.name.trim() === '') {
                            delete setData.name
                        }

                        // Base update with $set (always present with updatedAt)
                        const update: any = {
                            $set: setData,
                            $setOnInsert: {
                                instanceId,
                                id: contact.id,
                                ...(notify !== undefined ? { notify } : {})
                            }
                        }

                        // Add $unset if needed to clean up existing null name
                        if (existing && existing.name === null && !setData.name) {
                            update.$unset = { name: 1 }
                        }

                        return {
                            updateOne: {
                                filter: { instanceId, id: contact.id },
                                update,
                                upsert: true
                            }
                        }
                    });

                    await withConnection(async () =>
                        collections.contacts.bulkWrite(bulkOps, { ordered: false })
                    )
                    
                    // Queue profile picture retrieval for contacts if enabled
                    if (sock && profilePictureConfig?.enabled && (sharedQueueManager || queues.has(QueueType.PROFILE_PICTURES))) {
                        const refreshIntervalDays = profilePictureConfig.refreshIntervalDays || 7
                        const refreshIntervalMs = refreshIntervalDays * 24 * 60 * 60 * 1000
                        
                        for (const contact of contacts) {
                            // Only fetch profile pictures for user JIDs (@s.whatsapp.net)
                            // Skip groups (@g.us) and LIDs (@lid)
                            if (!contact.id.endsWith('@s.whatsapp.net')) {
                                log(`📸 [Contacts Queue] Skipping profile picture for ${contact.id} (not a user JID)`)
                                continue
                            }
                            
                            try {
                                // Check if we need to fetch/refresh the profile picture
                                const existingContact = await withConnection(async () =>
                                    collections.contacts.findOne({ instanceId, id: contact.id })
                                ) as any
                                
                                let shouldFetchProfilePic = false
                                
                                if (!existingContact?.profilePic) {
                                    // No profile picture, fetch it
                                    shouldFetchProfilePic = true
                                    log(`📸 [Contacts Queue] Queuing profile picture fetch for ${contact.id} (no existing picture)`)
                                } else if (existingContact.profilePicUpdatedAt) {
                                    // Check if profile picture is stale
                                    const lastUpdated = new Date(existingContact.profilePicUpdatedAt).getTime()
                                    const now = Date.now()
                                    if (now - lastUpdated > refreshIntervalMs) {
                                        shouldFetchProfilePic = true
                                        log(`📸 [Contacts Queue] Queuing profile picture refresh for ${contact.id} (last updated ${refreshIntervalDays}+ days ago)`)
                                    }
                                } else {
                                    // Has profile pic but no update timestamp, refresh it
                                    shouldFetchProfilePic = true
                                    log(`📸 [Contacts Queue] Queuing profile picture refresh for ${contact.id} (no update timestamp)`)
                                }
                                
                                if (shouldFetchProfilePic) {
                                    // Queue the profile picture fetch job
                                    const profilePicQueue = queues.get(QueueType.PROFILE_PICTURES)!
                                    await profilePicQueue.add(
                                        `profile-pic-${contact.id}`,
                                        {
                                            contactId: contact.id,
                                            instanceId,
                                            timestamp: Date.now()
                                        } as ProfilePictureJob,
                                        {
                                            ...defaultJobOptions,
                                            delay: Math.random() * 1000 // Random delay up to 1 second to spread out requests
                                        }
                                    )
                                }
                            } catch (error) {
                                logWarn(`⚠️ [Contacts Queue] Failed to queue profile picture fetch for ${contact.id}:`, error)
                            }
                        }
                    }
                    
                    trackActivity(Date.now() - jobStartTime) // Track response time
                } else if (type === 'update' && contact) {
                    // Fetch existing contact to preserve name and profile picture
                    const existingContact = await withConnection(async () =>
                        collections.contacts.findOne(
                            { instanceId: validatedInstanceId, id: contact.id },
                            { projection: { name: 1, profilePic: 1, profilePicUpdatedAt: 1, notify: 1, ...(lidConfig?.enabled ? { lid: 1 } : {}) } }
                        )
                    ) as any

                    const { notify, id: _ignoredId, instanceId: _ignoredInstanceId, ...rest } = (contact as any) || {}

                    // Always include updatedAt
                    const setData: any = {
                        ...rest,
                        updatedAt: new Date()
                    }

                    // Preserve existing name if new name is null/undefined/empty
                    if (existingContact?.name && (!setData.name || setData.name.trim() === '')) {
                        setData.name = existingContact.name
                    }

                    // Preserve existing profile picture data if it exists
                    if (existingContact?.profilePic) {
                        setData.profilePic = existingContact.profilePic
                        setData.profilePicUpdatedAt = existingContact.profilePicUpdatedAt
                    }

                    await withConnection(async () =>
                        collections.contacts.updateOne(
                            { instanceId: validatedInstanceId, id: contact.id },
                            {
                                $set: setData,
                                $setOnInsert: {
                                    instanceId,
                                    id: contact.id,
                                    ...(notify !== undefined ? { notify } : {})
                                }
                            },
                            { upsert: true }
                        )
                    )
                    
                    // Queue profile picture retrieval for single contact update if enabled
                    if (sock && profilePictureConfig?.enabled && queues.has(QueueType.PROFILE_PICTURES)) {
                        // Only fetch profile pictures for user JIDs (@s.whatsapp.net)
                        // Skip groups (@g.us) and LIDs (@lid)
                        if (!contact.id.endsWith('@s.whatsapp.net')) {
                            log(`📸 [Contacts Queue] Skipping profile picture for ${contact.id} (not a user JID)`)
                            return { success: true }
                        }
                        
                        const profilePicQueue = queues.get(QueueType.PROFILE_PICTURES)!
                        const refreshIntervalDays = profilePictureConfig.refreshIntervalDays || 7
                        const refreshIntervalMs = refreshIntervalDays * 24 * 60 * 60 * 1000
                        
                        try {
                            const existingContact = await withConnection(async () =>
                                collections.contacts.findOne({ instanceId, id: contact.id })
                            ) as any
                            
                            let shouldFetchProfilePic = false
                            
                            if (!existingContact?.profilePic) {
                                shouldFetchProfilePic = true
                            } else if (existingContact.profilePicUpdatedAt) {
                                const lastUpdated = new Date(existingContact.profilePicUpdatedAt).getTime()
                                const now = Date.now()
                                if (now - lastUpdated > refreshIntervalMs) {
                                    shouldFetchProfilePic = true
                                }
                            } else {
                                shouldFetchProfilePic = true
                            }
                            
                            if (shouldFetchProfilePic) {
                                await profilePicQueue.add(
                                    `profile-pic-${contact.id}`,
                                    {
                                        contactId: contact.id,
                                        instanceId,
                                        timestamp: Date.now()
                                    } as ProfilePictureJob,
                                    defaultJobOptions
                                )
                                log(`📸 [Contacts Queue] Queued profile picture fetch for ${contact.id}`)
                            }
                        } catch (error) {
                            logWarn(`⚠️ [Contacts Queue] Failed to queue profile picture fetch for ${contact.id}:`, error)
                        }
                    }
                }
                
                return { success: true }
            })
            createQueueAndWorker<GroupMetadataJob>(QueueType.GROUP_METADATA, async (job) => {
                const { type, jid, metadata, update } = job.data
                
                if (type === 'upsert') {
                    await withConnection(async () =>
                        collections.groupMetadata.replaceOne(
                            { instanceId, id: metadata.id },
                            { ...metadata, instanceId, updatedAt: new Date() },
                            { upsert: true }
                        )
                    )
                } else if (type === 'update' && update) {
                    await withConnection(async () =>
                        collections.groupMetadata.updateOne(
                            { instanceId, id: jid },
                            { $set: { ...update, updatedAt: new Date() } }
                        )
                    )
                }
                
                return { success: true }
            })
            
            createQueueAndWorker<PresenceJob>(QueueType.PRESENCES, async (job) => {
                const { id, presences } = job.data
                
                await withConnection(async () =>
                    collections.presences.updateOne(
                        { instanceId, id },
                        {
                            $set: { presences, updatedAt: new Date() }
                        },
                        { upsert: true }
                    )
                )
                
                return { success: true }
            })
            
            createQueueAndWorker<StateJob>(QueueType.STATE, async (job) => {
                const { update } = job.data
                
                await withConnection(async () =>
                    collections.state.updateOne(
                        { instanceId },
                        { 
                            $set: { ...update, instanceId, updatedAt: new Date() }
                        },
                        { upsert: true }
                    )
                )
                
                return { success: true }
            })
            
            createQueueAndWorker<LabelJob>(QueueType.LABELS, async (job) => {
                const { type, id, label } = job.data
                
                if (type === 'upsert' && label) {
                    await withConnection(async () =>
                        collections.labels.replaceOne(
                            { instanceId, id },
                            { ...label, instanceId, updatedAt: new Date() },
                            { upsert: true }
                        )
                    )
                } else if (type === 'delete') {
                    await withConnection(async () =>
                        collections.labels.deleteOne({
                            instanceId,
                            id
                        })
                    )
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
                        const existingDocs = await withConnection(async () =>
                            collections.labelAssociations.find({
                                instanceId,
                                chatId: association.chatId,
                                labelId: association.labelId
                            }).toArray()
                        )
                        
                        if (existingDocs.length > 0) {
                            log(`[Label Queue] Found ${existingDocs.length} existing docs for ${association.chatId}/${association.labelId}:`)
                            existingDocs.forEach((doc, index) => {
                                log(`[Label Queue]   Doc ${index + 1}: type=${doc.type}, messageId=${(doc as any).messageId || 'none'}`)
                            })
                        } else {
                            log(`[Label Queue] No existing documents found for ${association.chatId}/${association.labelId}`)
                        }
                        
                        const result = await withConnection(async () =>
                            collections.labelAssociations.replaceOne(
                                filter,
                                {
                                    ...association,
                                    instanceId,
                                    updatedAt: new Date()
                                },
                                { upsert: true }
                            )
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
                        const finalDocs = await withConnection(async () =>
                            collections.labelAssociations.find({
                                instanceId,
                                chatId: association.chatId,
                                labelId: association.labelId
                            }).toArray()
                        )
                        
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
                        
                        const result = await withConnection(async () =>
                            collections.labelAssociations.deleteOne(filter)
                        )
                        
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
            
            // Profile picture queue (only if sock and config are provided)
            if (sock && profilePictureConfig?.enabled) {
                createQueueAndWorker<ProfilePictureJob>(QueueType.PROFILE_PICTURES, async (job) => {
                    const { contactId, retryCount = 0 } = job.data
                    const maxRetries = profilePictureConfig.retryAttempts || 3
                    const requestDelay = profilePictureConfig.requestDelay || 500
                    
                    // Only fetch profile pictures for user JIDs (@s.whatsapp.net)
                    // Skip groups (@g.us) and LIDs (@lid)
                    if (!contactId.endsWith('@s.whatsapp.net')) {
                        log(`📸 [Profile Picture Queue] Skipping profile picture for ${contactId} (not a user JID)`)
                        return { success: true }
                    }
                    
                    // Add delay between requests to avoid rate limiting
                    if (requestDelay > 0) {
                        await new Promise(resolve => setTimeout(resolve, requestDelay))
                    }
                    
                    try {
                        log(`📸 [Profile Picture Queue] Fetching profile picture for ${contactId}`)
                        
                        // Check if socket is available
                        if (!sock) {
                            log(`⚠️ [Profile Picture Queue] Socket not available, skipping profile picture for ${contactId}`)
                            return
                        }
                        
                        // Fetch profile picture URL using Baileys sock
                        const profilePictureUrl = await sock.profilePictureUrl(contactId)
                        
                        if (profilePictureUrl) {
                            // Update contact with profile picture
                            await withConnection(async () =>
                                collections.contacts.updateOne(
                                    { instanceId, id: contactId },
                                    { 
                                        $set: { 
                                            profilePic: profilePictureUrl,
                                            profilePicUpdatedAt: new Date(),
                                            updatedAt: new Date()
                                        } 
                                    }
                                )
                            )
                            
                            log(`✅ [Profile Picture Queue] Updated profile picture for ${contactId}`)
                            return { success: true, profilePictureUrl }
                        } else {
                            log(`⚠️ [Profile Picture Queue] No profile picture available for ${contactId}`)
                            return { success: true, profilePictureUrl: null }
                        }
                    } catch (error: any) {
                        // Handle privacy errors silently unless configured to log them
                        const isPrivacyError = error.message?.includes('privacy') || 
                                              error.message?.includes('401') ||
                                              error.message?.includes('not authorized') ||
                                              error.message?.includes('ProfilePictureUrl')
                        
                        if (isPrivacyError) {
                            if (profilePictureConfig.logPrivacyErrors) {
                                log(`🔒 [Profile Picture Queue] Privacy restricted for ${contactId}`)
                            }
                            // Mark as successful to avoid retries for privacy errors
                            return { success: true, privacyRestricted: true }
                        }
                        
                        // For other errors, retry if attempts remaining
                        if (retryCount < maxRetries - 1) {
                            logWarn(`⚠️ [Profile Picture Queue] Failed to fetch profile picture for ${contactId}, retry ${retryCount + 1}/${maxRetries}: ${error.message}`)
                            // Re-queue with incremented retry count
                            await queues.get(QueueType.PROFILE_PICTURES)?.add(
                                `profile-pic-${contactId}`,
                                {
                                    ...job.data,
                                    retryCount: retryCount + 1,
                                    lastError: error.message
                                },
                                {
                                    delay: (retryCount + 1) * 2000, // Exponential backoff
                                    ...defaultJobOptions
                                }
                            )
                        } else {
                            logError(`❌ [Profile Picture Queue] Failed to fetch profile picture for ${contactId} after ${maxRetries} attempts:`, error.message)
                        }
                        
                        throw error
                    }
                })
                
                log(`📸 Profile picture queue initialized with concurrency: ${profilePictureConfig.maxConcurrent || 5}`)
            }
            
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
                        const exists = await withConnection(async () =>
                            collections.labelAssociations.findOne({
                                instanceId,
                                chatId: metadata.chatId,
                                labelId,
                                type: LabelAssociationType.Chat
                            })
                        )
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
        // Tightened sensible defaults for enhanced store
        switch (collectionName) {
            case 'presences':
                // Presence updates are highly ephemeral
                return 1
            case 'state':
                // Connection state can be short-lived
                return Math.min(ttlDays, 7)
            case 'messages':
            case 'chats':
            case 'contacts':
            default:
                return ttlDays
        }
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

    const updateLidResolutionMetrics = (operationType: string, action: 'resolved' | 'error') => {
        if (!enableMetrics) return

        let metrics = lidResolutionMetricsMap.get(operationType)
        if (!metrics) {
            metrics = {
                operationType,
                totalResolved: 0,
                totalErrors: 0
            }
            lidResolutionMetricsMap.set(operationType, metrics)
        }

        switch (action) {
            case 'resolved':
                metrics.totalResolved++
                metrics.lastProcessedAt = new Date()
                break
            case 'error':
                metrics.totalErrors++
                break
        }
    }
    // Smart index creation with collection existence checking and custom TTL
    const createIndexes = async () => {
        // Migration: Drop obsolete TTL indexes from collections that should persist indefinitely
        const migrationPromises = [
            withConnection(async () => safeDropIndex(collections.groupMetadata, 'updatedAt_1', { silent: true })).catch(() => {}),
            withConnection(async () => safeDropIndex(collections.labels, 'updatedAt_1', { silent: true })).catch(() => {}),
            withConnection(async () => safeDropIndex(collections.labelAssociations, 'updatedAt_1', { silent: true })).catch(() => {})
        ]
        
        await Promise.all(migrationPromises)
        log('🔄 Migration completed: Removed obsolete TTL indexes')
        
        // Define all indexes by collection with standardized format
        const indexDefinitions: Record<string, IndexSpec[]> = {
            chats: [
                { name: 'chats_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } },
                { name: 'chats_ttl', spec: { updatedAt: 1 }, options: { expireAfterSeconds: getTTLForCollection('chats') * 24 * 60 * 60 } }
            ],
            contacts: [
                { name: 'contacts_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } },
                { name: 'contacts_lid_lookup', spec: { instanceId: 1, lid: 1 }, options: { unique: true, partialFilterExpression: { lid: { $type: 'string' } } } },
                // NEW: Composite index for efficient $in queries with projection fields
                { name: 'contacts_batch_lookup', spec: { instanceId: 1, id: 1, name: 1, profilePic: 1, profilePicUpdatedAt: 1, notify: 1, lid: 1 }, options: {} },
                // NEW: Covering index for lid->id lookups to avoid document fetch
                { name: 'contacts_lid_id_cover', spec: { instanceId: 1, lid: 1, id: 1 }, options: { partialFilterExpression: { lid: { $type: 'string' } } } }
            ],
            messages: [
                { name: 'messages_primary', spec: { instanceId: 1, jid: 1, 'key.id': 1 }, options: { unique: true } },
                { name: 'messages_jid_timestamp', spec: { instanceId: 1, jid: 1, messageTimestamp: -1 }, options: {} },
                { name: 'messages_ttl', spec: { updatedAt: 1 }, options: { expireAfterSeconds: getTTLForCollection('messages') * 24 * 60 * 60 } },
                // Ensure compound index exists for manual per-instance cleanup
                { name: 'messages_instance_updatedAt', spec: { instanceId: 1, updatedAt: 1 }, options: {} },
                { name: 'messages_media_dedup', spec: { instanceId: 1, mediaHash: 1 }, options: { sparse: true } },
                { name: 'messages_remote_fallback', spec: { instanceId: 1, 'key.remoteJid': 1, 'key.id': 1 }, options: {} },
                { name: 'messages_keyid_direct', spec: { instanceId: 1, 'key.id': 1 }, options: {} },
                // Supports reverse lookup for LID discovery when only senderLid is present on incoming messages
                { name: 'messages_senderLid_lookup', spec: { instanceId: 1, 'key.fromMe': 1, 'key.senderLid': 1 }, options: {} },
                // Index for LID resolution tracking
                { name: 'messages_lid_resolution', spec: { instanceId: 1, 'lidMapping.resolved': 1 }, options: { sparse: true } },
                // Index for media deduplication by fileHash
                { name: 'messages_media_fileHash', spec: { 'mediaInfo.fileHash': 1 }, options: { sparse: true } }
            ],
            groupMetadata: [
                { name: 'groups_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } }
                // No TTL - persists indefinitely
            ],
            state: [
                { name: 'state_primary', spec: { instanceId: 1 }, options: { unique: true } },
                { name: 'state_ttl', spec: { updatedAt: 1 }, options: { expireAfterSeconds: getTTLForCollection('state') * 24 * 60 * 60 } }
            ],
            presences: [
                { name: 'presences_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } },
                { name: 'presences_ttl', spec: { updatedAt: 1 }, options: { expireAfterSeconds: getTTLForCollection('presences') * 24 * 60 * 60 } }
            ],
            labels: [
                { name: 'labels_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } }
                // No TTL - persists indefinitely
            ],
            labelAssociations: [
                { name: 'labelAssociations_primary', spec: { instanceId: 1, chatId: 1, messageId: 1, labelId: 1 }, options: { unique: true } },
                { name: 'labelAssociations_chatId_labelId', spec: { instanceId: 1, chatId: 1, labelId: 1 }, options: {} },
                { name: 'labelAssociations_messageId_labelId', spec: { instanceId: 1, messageId: 1, labelId: 1 }, options: {} },
                // Index for LID resolution tracking
                { name: 'labelAssociations_lid_resolution', spec: { instanceId: 1, 'lidMapping.resolved': 1 }, options: { sparse: true } }
                // No TTL - persists indefinitely
            ],
            // Include LidHandler's collection in smart index management
            lidMappings: [
                { name: 'lidMappings_primary', spec: { instanceId: 1, lid: 1 }, options: { unique: true } },
                { name: 'lidMappings_phone_lookup', spec: { instanceId: 1, phoneNumber: 1 }, options: {} }
            ]
        }
        
        // Smart index creation logic with configuration support
        const createResults: Array<{ collection: string, created: number, skipped: boolean, details: string[] }> = []
        let totalCreated = 0
        let totalSkipped = 0
        
        if (indexConfig.enableIndexHealthLogging) {
            log(`🔧 Smart index management for enhanced instance ${instanceId}...`)
            log(`   Settings: skipExisting=${indexConfig.skipExistingCollectionIndexes}, forceRecreate=${indexConfig.forceRecreateIndexes}`)
        }
        
        // Process each collection
        for (const [collectionName, requiredIndexes] of Object.entries(indexDefinitions)) {
            try {
                const collection = collections[collectionName as keyof typeof collections]
                
                // Handle force recreation mode
                if (indexConfig.forceRecreateIndexes) {
                    if (indexConfig.enableIndexHealthLogging) {
                        log(`🔄 Collection ${collectionName}: Force recreating all ${requiredIndexes.length} indexes`)
                    }
                    
                    // Force recreate: drop and recreate all indexes with timeout applied
                    const timedIndexes = requiredIndexes.map(addIndexTimeout)
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
                            log(`✅ Collection ${collectionName}: All ${checkResult.requiredCount} indexes exist, skipping creation`)
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
                            log(`🔨 Collection ${collectionName}: ${action} ${missingCount} indexes`)
                        }
                        
                        // Create missing indexes using batch operation with timeout applied
                        const timedMissingIndexes = checkResult.missingIndexes.map(addIndexTimeout)
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
                        log(`🔧 Collection ${collectionName}: Creating all ${requiredIndexes.length} indexes (legacy mode)`)
                    }
                    
                    // Apply timeout to all indexes for legacy mode
                    const timedIndexes = requiredIndexes.map(addIndexTimeout)
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
            log(`✅ Smart enhanced index management completed for instance ${instanceId}:`)
            log(`   📊 Total indexes: ${totalRequired} required`)
            log(`   🔨 Created: ${totalCreated}`)
            log(`   ⏭️  Skipped (existing): ${totalSkipped}`)
            log(`   📈 Efficiency: ${Math.round((totalSkipped / totalRequired) * 100)}% reduction in index operations`)
        }
        
        // Detailed logging if any indexes were created
        if (totalCreated > 0) {
            if (indexConfig.enableIndexHealthLogging) {
                log('📋 Enhanced index creation details:')
                createResults.forEach(result => {
                    if (result.created > 0) {
                        log(`   ${result.collection}: ${result.created} created`)
                    }
                })
            }
            
            // Clear collection cache since new collections may have been created
            clearCollectionCache()
            if (indexConfig.enableIndexHealthLogging) {
                log('🧹 Cleared collection cache after index creation')
            }
        }
        
        // Verify TTL indexes if monitoring is enabled and TTL indexes were created
        if (ttlMonitor !== null && totalCreated > 0) {
            const ttlCollections = ['chats', 'contacts', 'messages', 'state', 'presences']
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
                    logWarn(`[TTL Monitor] Found ${invalidTTL.length} invalid TTL indexes:`, invalidTTL.map(r => r.collection))
                } else {
                    log('[TTL Monitor] ✅ All newly created TTL indexes verified successfully')
                }
            } else {
                log('[TTL Monitor] No new TTL indexes to verify')
            }
        }
    }
    
    // Initialize indexes
    await createIndexes()
    
    // Start TTL monitor after indexes are ensured (only if explicitly enabled)
    if (ttlMonitor && ttlMonitoring?.enableMonitoring === true) {
        ttlMonitor.startMonitoring((message: string) => {
            logWarn(`[TTL Monitor] ${message}`)
        })
    }
    
    // Track binding state to prevent duplicate bindings
    let isBound = false
    
    // Track current event emitter and handlers for proper cleanup
    let currentEventEmitter: BaileysEventEmitter | null = null
    const eventHandlers = new Map<string, (...args: any[]) => Promise<void>>()
    
    // LID resolution helper function
    const performProactiveLidResolutionForLabelAssociations = async (
        lidMappings: Array<{ lid: string | undefined; id: string }>
    ): Promise<number> => {
        let totalLabelAssociationsUpdated = 0
        const batchSize = 50

        for (let i = 0; i < lidMappings.length; i += batchSize) {
            const batch = lidMappings.slice(i, i + batchSize)
            const batchPromises = batch.map(async (mapping: any) => {
                const lid = mapping.lid
                const phoneNumber = mapping.id

                if (!lid || !lidHandler!.isLidFormat(lid) || lidHandler!.isLidFormat(phoneNumber)) {
                    return 0 // Skip invalid mappings
                }

                try {
                    // Update label associations where chatId equals the LID
                    const updateResult = await withConnection(async () =>
                        collections.labelAssociations.updateMany(
                            {
                                instanceId: validatedInstanceId,
                                chatId: lid,
                                type: 'label_jid' as any // Only update chat-based associations
                            },
                            {
                                $set: {
                                    chatId: phoneNumber,
                                    'lidMapping.resolved': true,
                                    'lidMapping.resolvedAt': new Date(),
                                    'lidMapping.originalLid': lid,
                                    updatedAt: new Date()
                                }
                            }
                        )
                    )

                    if (updateResult.modifiedCount > 0) {
                        log(`[${instanceId}] Updated ${updateResult.modifiedCount} label associations: ${lid} -> ${phoneNumber}`)
                        if (enableMetrics) {
                            updateLidResolutionMetrics('proactive-label-associations', 'resolved')
                        }
                    }

                    return updateResult.modifiedCount
                } catch (error) {
                    logError(`[${instanceId}] Error updating label associations for LID ${lid}:`, error)
                    if (enableMetrics) {
                        updateLidResolutionMetrics('proactive-label-associations', 'error')
                    }
                    return 0
                }
            })

            const batchResults = await Promise.all(batchPromises)
            totalLabelAssociationsUpdated += batchResults.reduce((sum, count) => sum + count, 0)

            // Small delay between batches to prevent overwhelming
            if (i + batchSize < lidMappings.length) {
                await new Promise(resolve => setTimeout(resolve, 100))
            }
        }

        return totalLabelAssociationsUpdated
    }
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

        getLidResolutionMetrics(operationType?: string): LidResolutionMetrics | LidResolutionMetrics[] {
            if (operationType) {
                return lidResolutionMetricsMap.get(operationType) || {
                    operationType,
                    totalResolved: 0,
                    totalErrors: 0
                }
            }
            return Array.from(lidResolutionMetricsMap.values())
        },

        resetLidResolutionMetrics(operationType?: string): void {
            if (operationType) {
                lidResolutionMetricsMap.delete(operationType)
            } else {
                lidResolutionMetricsMap.clear()
            }
        },

        async getChats(): Promise<Chat[]> {
            const startTime = Date.now()
            trackActivity() // Track request
            
            const chats = await withConnection(async () =>
                collections.chats
                    .find({ instanceId })
                    .sort({ conversationTimestamp: -1 })
                    .toArray()
            )
            
            trackActivity(Date.now() - startTime) // Track response time
            return chats.map(({ _id, instanceId: _instanceId, updatedAt: _updatedAt, ...chat }) => chat as Chat)
        },

        async getChat(jid: string): Promise<Chat | null> {
            try {
                const validJid = validateJID(jid)
                
                const chat = await withConnection(async () =>
                    collections.chats.findOne({ instanceId: validatedInstanceId, id: validJid })
                )
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
            
            const startTime = Date.now()
            trackActivity() // Track request
            
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
                    trackActivity(Date.now() - startTime) // Track response time
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
            
            await withConnection(async () =>
                collections.chats.bulkWrite(bulkOps)
            )
            trackActivity(Date.now() - startTime) // Track response time
        },

        async updateChat(jid: string, update: Partial<Chat>): Promise<boolean> {
            const startTime = Date.now()
            trackActivity() // Track request
            
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
            const result = await withConnection(async () =>
                collections.chats.updateOne(
                    { instanceId, id: normalizedJid },
                    { $set: { ...update, updatedAt: new Date() } }
                )
            )
            
            trackActivity(Date.now() - startTime) // Track response time
            return result.modifiedCount > 0
        },

        async deleteChats(jids: string[]): Promise<void> {
            const startTime = Date.now()
            trackActivity() // Track request
            
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
            await withConnection(async () =>
                collections.chats.deleteMany({
                    instanceId,
                    id: { $in: normalizedJids }
                })
            )
            trackActivity(Date.now() - startTime) // Track response time
        },

        async getContacts(): Promise<{ [id: string]: Contact }> {
            const contacts = await withConnection(async () =>
                collections.contacts
                    .find({ instanceId })
                    .toArray()
            )
            
            const contactsMap: { [id: string]: Contact } = {}
            for (const contact of contacts) {
                // eslint-disable-next-line @typescript-eslint/no-unused-vars
                const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...contactData } = contact
                contactsMap[contact.id] = contactData as Contact
            }
            
            return contactsMap
        },

        async getContact(jid: string): Promise<Contact | null> {
            const contact = await withConnection(async () =>
                collections.contacts.findOne({ instanceId, id: jid })
            )
            if (!contact) return null
            
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...contactData } = contact
            return contactData as Contact
        },

        async upsertContacts(contacts: Contact[]): Promise<void> {
            if (contacts.length === 0) return
            
            const startTime = Date.now()
            
            log(`📝 [Contacts] Saving ${contacts.length} contacts to database`)
            
            // Fetch existing contacts to preserve profile picture data, existing notify, and name (chunked + projected)
            const ids = contacts.map(c => c.id)
            const existingContacts: any[] = []
            const idChunkSize = 1000  // Reduced from 5000 to optimize $in performance
            for (let i = 0; i < ids.length; i += idChunkSize) {
                const idChunk = ids.slice(i, i + idChunkSize)
                const chunk = await withConnection(async () =>
                    collections.contacts.find(
                        { instanceId, id: { $in: idChunk } },
                        { projection: { id: 1, name: 1, profilePic: 1, profilePicUpdatedAt: 1, notify: 1, ...(lidConfig?.enabled ? { lid: 1 } : {}) } }
                    ).toArray()
                ) as any[]
                existingContacts.push(...chunk)
            }

            // Create a map of existing contact data to preserve
            const existingDataMap = new Map(
                existingContacts.map(c => [c.id, {
                    name: c.name,
                    profilePic: c.profilePic,
                    profilePicUpdatedAt: c.profilePicUpdatedAt,
                    notify: c.notify,
                    ...(lidConfig?.enabled ? { lid: c.lid } : {})
                }])
            )
            
            // IMPORTANT: Save contacts directly to ensure data persistence
            // This bypasses the broken SharedQueueManager that causes processor conflicts
            const bulkOps = contacts.map(contact => {
                // Handle null name: fallback to notify or verifiedName, or skip if both are null
                if (contact.name === null) {
                    contact.name = contact.notify || contact.verifiedName || undefined;
                }

                const { notify, id: _ignoredId, instanceId: _ignoredInstanceId, ...rest } = (contact as any) || {}
                const existing = existingDataMap.get(contact.id)

                // Always include updatedAt
                const setData: any = {
                    ...rest,
                    updatedAt: new Date()
                }

                // Preserve existing name if new name is null/undefined/empty
                if (existing?.name && (!setData.name || setData.name.trim() === '')) {
                    setData.name = existing.name
                }

                // Preserve existing profile picture data if it exists
                if (existing?.profilePic) {
                    setData.profilePic = existing.profilePic
                    setData.profilePicUpdatedAt = existing.profilePicUpdatedAt
                }

                // Remove name from setData if it's null/undefined/empty after preservation
                if (setData.name == null || setData.name.trim() === '') {
                    delete setData.name
                }

                // Base update with $set (always present with updatedAt)
                const update: any = {
                    $set: setData,
                    $setOnInsert: {
                        instanceId,
                        id: contact.id,
                        ...(notify !== undefined ? { notify } : {})
                    }
                }

                // Add $unset if needed to clean up existing null name
                if (existing && existing.name === null && !setData.name) {
                    update.$unset = { name: 1 }
                }

                return {
                    updateOne: {
                        filter: { instanceId: validatedInstanceId, id: contact.id },
                        update,
                        upsert: true
                    }
                }
            });
            
            // Process in chunks for large contact lists
            for (let i = 0; i < bulkOps.length; i += BATCH_SIZE) {
                const chunk = bulkOps.slice(i, i + BATCH_SIZE)
                try {
                    await withConnection(async () =>
                        collections.contacts.bulkWrite(chunk, { ordered: false })
                    )
                } catch (e: any) {
                    // Handle duplicate and path conflict errors by retrying without upsert
                    if (e?.code === 11000 || e?.code === 40 || String(e?.message || '').includes("conflict at 'id'")) {
                        const fallbackOps = chunk.map(op => {
                            const u = (op as any).updateOne
                            return {
                                updateOne: {
                                    filter: u.filter,
                                    update: {
                                        $set: {
                                            ...(u.update?.$set || {}),
                                            updatedAt: new Date()  // Ensure atomic operator
                                        }
                                    },
                                    upsert: false
                                }
                            }
                        })
                        await withConnection(async () =>
                            collections.contacts.bulkWrite(fallbackOps as any, { ordered: false })
                        )
                    } else {
                        throw e
                    }
                }
            }
            
            log(`✅ [Contacts] Saved ${contacts.length} contacts to database`)
            trackActivity(Date.now() - startTime) // Track response time
            
            // Fetch profile pictures asynchronously in the background
            // This ensures contacts are saved immediately while profile pictures are fetched later
            if (sock && profilePictureConfig?.enabled) {
                // Clear any existing fetch operation
                if (profilePictureFetchHandle) {
                    clearImmediate(profilePictureFetchHandle)
                    profilePictureFetchHandle = null
                }
                
                // Use setImmediate to run in background without blocking the event loop
                profilePictureFetchHandle = setImmediate(async () => {
                    // Track this operation
                    const profileFetchPromise = (async () => {
                        // Check if we're closing or clearAll is in progress
                        if (isClosing || clearAllInProgress) {
                            log(`⚠️ [Contacts] Skipping profile picture fetch - store is ${isClosing ? 'closing' : 'clearing'}`)
                            return
                        }
                        log(`📸 [Contacts] Starting profile picture fetch for ${contacts.length} contacts`)
                    
                    const requestDelay = profilePictureConfig.requestDelay || 500
                    const maxRetries = profilePictureConfig.retryAttempts || 3
                    const refreshIntervalDays = profilePictureConfig.refreshIntervalDays || 7
                    const refreshIntervalMs = refreshIntervalDays * 24 * 60 * 60 * 1000
                    
                    for (const contact of contacts) {
                        // Only fetch profile pictures for user JIDs (@s.whatsapp.net)
                        // Skip groups (@g.us) and LIDs (@lid)
                        if (!contact.id.endsWith('@s.whatsapp.net')) {
                            continue
                        }
                        
                        try {
                            // Check if we need to fetch profile picture for this contact
                            // Use the existingDataMap we already fetched to avoid redundant queries
                            const existingData = existingDataMap.get(contact.id)
                            
                            const shouldFetch = !existingData?.profilePic || 
                                                !existingData?.profilePicUpdatedAt ||
                                                (Date.now() - new Date(existingData.profilePicUpdatedAt).getTime() > refreshIntervalMs)
                            
                            if (!shouldFetch) {
                                log(`⏭️ [Contacts] Skipping profile picture for ${contact.id} (recently updated)`)
                                continue
                            }
                            
                            // Rate limiting: add delay between requests
                            if (requestDelay > 0) {
                                await new Promise(resolve => setTimeout(resolve, requestDelay))
                            }
                            
                            log(`📸 [Contacts] Fetching profile picture for ${contact.id} (${existingData?.profilePic ? 'outdated' : 'no existing picture'})`)
                            
                            let attempts = 0
                            let profilePictureUrl: string | null = null
                            
                            while (attempts < maxRetries && !profilePictureUrl) {
                                try {
                                    // Check if store is closing or sock is null
                                    if (isClosing || !sock) {
                                        log(`⚠️ [Contacts] Stopping profile fetch - ${isClosing ? 'closing' : 'no socket'}`)
                                        break
                                    }
                                    
                                    profilePictureUrl = await sock.profilePictureUrl(contact.id).catch(() => null) as string | null
                                    
                                    if (profilePictureUrl) {
                                        // Update contact with profile picture URL
                                        await withConnection(async () =>
                                            collections.contacts.updateOne(
                                                { instanceId, id: contact.id },
                                                { 
                                                    $set: { 
                                                        profilePic: profilePictureUrl,
                                                        profilePicUpdatedAt: new Date(),
                                                        updatedAt: new Date()
                                                    } 
                                                }
                                            )
                                        )
                                        log(`✅ [Contacts] Updated profile picture for ${contact.id}`)
                                        break
                                    }
                                } catch (error: any) {
                                    // Handle privacy errors silently
                                    if (error?.message?.includes('privacy') || 
                                        error?.message?.includes('401') ||
                                        error?.message?.includes('not authorized')) {
                                        log(`🔒 [Contacts] Profile picture private for ${contact.id}`)
                                        // Mark as checked to avoid repeated attempts
                                        await withConnection(async () =>
                                            collections.contacts.updateOne(
                                                { instanceId, id: contact.id },
                                                { 
                                                    $set: { 
                                                        profilePicUpdatedAt: new Date(),
                                                        updatedAt: new Date()
                                                    } 
                                                }
                                            )
                                        )
                                        break
                                    }
                                    
                                    attempts++
                                    if (attempts < maxRetries) {
                                        log(`⚠️ [Contacts] Retry ${attempts}/${maxRetries} for ${contact.id}: ${error?.message}`)
                                        await new Promise(resolve => setTimeout(resolve, requestDelay * 2))
                                    }
                                }
                            }
                            
                            if (!profilePictureUrl && attempts >= maxRetries) {
                                log(`❌ [Contacts] Failed to fetch profile picture for ${contact.id} after ${maxRetries} attempts`)
                            }
                        } catch (error) {
                            logError(`❌ [Contacts] Error processing profile picture for ${contact.id}:`, error)
                        }
                    }
                    
                    log(`✅ [Contacts] Profile picture fetch completed for ${contacts.length} contacts`)
                    })() // Close and execute the async function
                    
                    // Track this operation with metadata
                    trackOperation(profileFetchPromise, `Profile picture fetch for ${contacts.length} contacts`)
                })
            }

            // Fetch LIDs asynchronously in the background
            if (finalLidConfig?.enabled && sock) {
                setImmediate(async () => {
                    const lidFetchPromise = (async () => {
                        if (isClosing || clearAllInProgress) {
                            log(`⚠️ [Contacts] Skipping LID fetch - store is ${isClosing ? 'closing' : 'clearing'}`)
                            return
                        }
                        log(`📍 [Contacts] Starting LID fetch for ${contacts.length} contacts`)

                        const requestDelay = finalLidConfig.requestDelay || 500 // Increased default delay for rate limiting
                        const maxRetries = finalLidConfig.retryAttempts || 3

                        for (const contact of contacts) {
                            // Only fetch LIDs for user JIDs (@s.whatsapp.net)
                            // Skip groups (@g.us) and LIDs (@lid)
                            if (!contact.id.endsWith('@s.whatsapp.net') || contact.id.includes('@lid') || contact.id.endsWith('@g.us')) {
                                continue
                            }

                            // Check if we need to fetch LID for this contact
                            const existingData = existingDataMap.get(contact.id)

                            if (existingData?.lid) {
                                log(`⏭️ [Contacts] Skipping LID for ${contact.id} (already has LID)`)
                                continue
                            }

                            // Rate limiting: add delay between requests
                            if (requestDelay > 0) {
                                await new Promise(resolve => setTimeout(resolve, requestDelay))
                            }

                            log(`📍 [Contacts] Fetching LID for ${contact.id}`)

                            let attempts = 0
                            let lid: string | undefined

                            while (attempts < maxRetries && !lid) {
                                try {
                                    // Check if store is closing or sock is null
                                    if (isClosing || !sock) {
                                        log(`⚠️ [Contacts] Stopping LID fetch - ${isClosing ? 'closing' : 'no socket'}`)
                                        break
                                    }

                                    const result = await sock.onWhatsApp(contact.id) as Array<{ jid: string; exists: boolean; lid?: string }> | undefined

                                    if (result && result.length > 0 && result[0].exists && result[0].lid) {
                                        lid = result[0].lid
                                        // Update contact with LID
                                        await withConnection(async () =>
                                            collections.contacts.updateOne(
                                                { instanceId, id: contact.id },
                                                {
                                                    $set: {
                                                        lid,
                                                        updatedAt: new Date()
                                                    }
                                                }
                                            )
                                        )

                                        log(`✅ [Contacts] Updated LID for ${contact.id}`)
                                        break
                                    }
                                } catch (error: any) {
                                    attempts++
                                    if (attempts < maxRetries) {
                                        log(`⚠️ [Contacts] Retry ${attempts}/${maxRetries} for ${contact.id}: ${error?.message}`)
                                        await new Promise(resolve => setTimeout(resolve, requestDelay * 2))
                                    }
                                }
                            }

                            if (!lid && attempts >= maxRetries) {
                                log(`❌ [Contacts] Failed to fetch LID for ${contact.id} after ${maxRetries} attempts`)
                            }
                        }

                        log(`✅ [Contacts] LID fetch completed for ${contacts.length} contacts`)
                    })()

                    // Track this operation with metadata
                    trackOperation(lidFetchPromise, `LID fetch for ${contacts.length} contacts`)
                })
            }
        },

        async getMessages(jid: string): Promise<proto.IWebMessageInfo[]> {
            const messages = await withConnection(async () =>
                collections.messages
                    .find({ instanceId, jid })
                    .sort({ messageTimestamp: -1 })
                    .toArray()
            )
            
            return messages.map(({ _id, instanceId: _instanceId, jid: _jid, updatedAt: _updatedAt, ...msg }) => 
                convertBinaryToBuffer(msg))
        },

        async getMessage(jid: string, id: string): Promise<proto.IWebMessageInfo | null> {
            try {
                const startTime = Date.now()
                trackActivity() // Track request
                
                // Normalize out any :XX suffixes for safe comparison/lookup
                const validJid = safeValidateJID(safeNormalizeJid(jid))
                
                // Fast-exit for placeholder IDs to avoid unnecessary DB queries
                if (typeof id === 'string' && id.startsWith('PLACEHOLDER_')) {
                    const nfKey1 = `nf_${validatedInstanceId}_${hashForLogging(validJid)}_${hashForLogging(id)}`
                    const nfKey2 = `nf_${validatedInstanceId}_${hashForLogging(id)}`
                    notFoundCache.set(nfKey1, true)
                    notFoundCache.set(nfKey2, true)
                    return null
                }
                
                const validId = safeValidateMessageId(id)
                
                // Early JID normalization: try normalized variant first when available
                const candidateJids: string[] = [validJid]
                if (lidHandler) {
                    try {
                        const normalized = await lidHandler.normalizeJid(validJid)
                        if (normalized && normalized !== validJid) {
                            candidateJids.unshift(normalized)
                        }
                    } catch {}
                }
                
                const cacheKey = `msg_${validatedInstanceId}_${hashForLogging(validJid)}_${hashForLogging(validId)}`
                
                const cached = binaryConversionCache.get<proto.IWebMessageInfo>(cacheKey)
                if (cached) {
                    trackActivity(Date.now() - startTime) // Track cache hit
                    return cached
                }
                
                // Negative cache: avoid repeated slow lookups for recent misses
                const nfKey1 = `nf_${validatedInstanceId}_${hashForLogging(validJid)}_${hashForLogging(validId)}`
                const nfKey2 = `nf_${validatedInstanceId}_${hashForLogging(validId)}`
                if (notFoundCache.get(nfKey1) || notFoundCache.get(nfKey2)) {
                    return null
                }
                
                // First try the standard query, with hint and time budget; try normalized JID first
                let message: any = null
                for (const tryJid of candidateJids) {
                    message = await withConnection(async () => 
                        collections.messages.findOne({
                            instanceId: validatedInstanceId,
                            jid: tryJid,
                            'key.id': validId
                        }, {
                            // @ts-ignore - hint & maxTimeMS supported by driver
                            hint: { instanceId: 1, jid: 1, 'key.id': 1 },
                            maxTimeMS: 150
                        } as any)
                    )
                    if (message) break
                }
                
                // If not found, try alternative queries for poll messages and other edge cases
                if (!message) {
                    
                    // Try with key.remoteJid instead of jid field (common for poll messages)
                    log(`[getMessage] Primary query failed, trying fallback with key.remoteJid for ${validJid}/${validId}`)
                    message = await withConnection(async () => {
                        const cursor = collections.messages.find({
                            instanceId: validatedInstanceId,
                            'key.remoteJid': validJid,
                            'key.id': validId
                        }).limit(1)
                        try { cursor.hint({ instanceId: 1, 'key.remoteJid': 1, 'key.id': 1 }) } catch {}
                        try {
                            const arr = await cursor.maxTimeMS(150).toArray()
                            return arr[0] || null
                        } catch {
                            const arr = await cursor.toArray()
                            return arr[0] || null
                        }
                    })
                    
                    if (message) {
                        log(`[getMessage] Found message using key.remoteJid fallback for ${validJid}/${validId}`)
                        // Read-repair: ensure future primary lookups hit the primary index
                        try {
                            await withConnection(async () =>
                                collections.messages.updateOne(
                                    { instanceId: validatedInstanceId, 'key.id': validId },
                                    { $set: { jid: validJid, 'key.remoteJid': validJid } }
                                )
                            )
                        } catch {}
                    }
                }
                    
                // If still not found, try without the jid constraint at all (just instanceId and key.id)
                // This handles edge cases like LID/phone number mismatches
                if (!message) {
                    const queryStart = Date.now()
                    message = await withConnection(async () => {
                        // Use the dedicated key.id index to avoid inefficient index selection
                        // This prevents MongoDB from choosing the mediaHash index incorrectly
                        const cursor = collections.messages.find({
                            instanceId: validatedInstanceId,
                            'key.id': validId
                        }).limit(1)
                        
                        // Try to use hint if available (MongoDB 4.4+)
                        try {
                            cursor.hint({ instanceId: 1, 'key.id': 1 })
                        } catch (e) {
                            // Hint not supported, continue without it
                        }
                        
                        try {
                            const results = await cursor.maxTimeMS(200).toArray()
                            return results[0] || null
                        } catch {
                            const results = await cursor.toArray()
                            return results[0] || null
                        }
                    })
                    const queryTime = Date.now() - queryStart
                    if (queryTime > 100) {
                        logWarn(`[getMessage] Slow query detected: ${queryTime}ms for key.id lookup (${validId})`)
                    }
                    
                    // Check if the JID mismatch is acceptable, and attempt read-repair when resolvable
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
                                            const updateResult = await withConnection(async () =>
                                                collections.messages.updateMany(
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
                                            )
                                            
                                            if (updateResult.modifiedCount > 0) {
                                                log(`Updated ${updateResult.modifiedCount} messages from LID to phone number format`)
                                            }
                                        } catch (updateError) {
                                            logError(`Failed to update messages with new LID mapping:`, updateError)
                                        }
                                        
                                        // Read-repair for the current message document
                                        try {
                                            await withConnection(async () =>
                                                collections.messages.updateOne(
                                                    {
                                                        instanceId: validatedInstanceId,
                                                        'key.id': validId
                                                    },
                                                    {
                                                        $set: {
                                                            jid: phoneJid,
                                                            'key.remoteJid': phoneJid,
                                                            'lidMapping.resolved': true,
                                                            'lidMapping.resolvedAt': new Date()
                                                        }
                                                    }
                                                )
                                            )
                                        } catch {}
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

                    // After all attempts, if still not found, return null
                    if (!message) {
                        // Only log basic info, avoid expensive debug queries
                        log(`Message not found - ID: ${validId}, JID: ${validJid}`)
                        notFoundCache.set(nfKey1, true)
                        notFoundCache.set(nfKey2, true)
                        return null
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
                
                // Handle protocol messages before any other processing
                if (clonedMessage.message?.protocolMessage) {
                    const protoType = clonedMessage.message.protocolMessage.type
                    
                    // Handle REVOKE messages - update the revoked message instead of storing the revoke message
                    if (protoType === proto.Message.ProtocolMessage.Type.REVOKE && clonedMessage.message.protocolMessage.key) {
                        const revokedKey = clonedMessage.message.protocolMessage.key
                        const outerChatJid = clonedMessage.key.remoteJid || jid
                        
                        // Prefer outer chat JID; normalize if LID
                        let targetJid = outerChatJid
                        if (lidHandler && targetJid) {
                            try {
                                const normalized = await (lidHandler as any).normalizeJid(targetJid)
                                if (normalized) targetJid = normalized
                            } catch {}
                        }
                        
                        log(`🔄 [Direct REVOKE] Processing revoke message for ${revokedKey.id} in chat ${targetJid}`)
                        
                        try {
                            const baseSet: any = {
                                'message.protocolMessage': clonedMessage.message.protocolMessage,
                                revoked: true,
                                revokedAt: new Date(),
                                revokedBy: clonedMessage.key.fromMe ? 'me' : clonedMessage.key.participant || clonedMessage.key.remoteJid,
                                messageStubType: 1
                            }
                            
                            let updateResult = await withConnection(async () =>
                                collections.messages.updateOne(
                                    { instanceId: validatedInstanceId, jid: targetJid, 'key.id': revokedKey.id },
                                    { $set: baseSet }
                                )
                            )
                            
                            if (updateResult.matchedCount === 0 && targetJid !== outerChatJid) {
                                updateResult = await withConnection(async () =>
                                    collections.messages.updateOne(
                                        { instanceId: validatedInstanceId, jid: outerChatJid, 'key.id': revokedKey.id },
                                        { $set: baseSet }
                                    )
                                )
                            }
                            
                            if (updateResult.matchedCount === 0) {
                                updateResult = await withConnection(async () =>
                                    collections.messages.updateOne(
                                        { instanceId: validatedInstanceId, 'key.id': revokedKey.id },
                                        { $set: baseSet }
                                    )
                                )
                            }
                            
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
                
                const quotedMsg = await resolveQuotedMessage(clonedMessage, validJid, collections, validatedInstanceId, log, withConnection)
                if (quotedMsg && quotedMsg.message) {
                    // Update the cloned message with the resolved quoted content
                    clonedMessage.message.extendedTextMessage.contextInfo.quotedMessage = quotedMsg.message
                    log(`✅ Updated message with resolved quoted content for ${clonedMessage.key?.id}`)
                }
            }
            
            // Decrypt poll vote if present
            let pollVoteDecrypted = null
            if (clonedMessage.message?.pollUpdateMessage) {
                pollVoteDecrypted = await decryptPollVote(clonedMessage, collections, validatedInstanceId, meId, log, withConnection)
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
                const originalMessage = await withConnection(async () =>
                    collections.messages.findOne({
                        instanceId: validatedInstanceId,
                        jid: validJid,
                        'key.id': editTargetKey.id
                    })
                )
                
                if (originalMessage && originalMessage.messageTimestamp) {
                    preservedTimestamp = originalMessage.messageTimestamp
                    log(`⏰ [Direct] Preserving original messageTimestamp: ${preservedTimestamp} for edited message ${editTargetKey.id}`)
                }
            }
            
            // Fallback to direct database operation
            try {
                await withConnection(async () =>
                    collections.messages.replaceOne(
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
                )
            } catch (error: any) {
                // Handle duplicate key errors gracefully
                if (error.code === 11000 || error.message?.includes('duplicate key')) {
                    log(`⚠️ [Direct] Duplicate key error for message ${clonedMessage.key?.id} in chat ${validJid} - message already exists`)
                    // Try to update instead of replace
                    try {
                        await withConnection(async () =>
                            collections.messages.updateOne(
                                {
                                    instanceId: validatedInstanceId,
                                    jid: validJid,
                                    'key.id': clonedMessage.key?.id
                                },
                                {
                                    $set: {
                                        ...clonedMessage,
                                        ...(preservedTimestamp && { messageTimestamp: preservedTimestamp }),
                                        ...(pollVoteDecrypted && { pollVoteDecrypted }),
                                        updatedAt: new Date()
                                    }
                                }
                            )
                        )
                        log(`✅ [Direct] Successfully updated existing message ${clonedMessage.key?.id} after duplicate key error`)
                    } catch (updateError) {
                        logError(`[Direct] Failed to update message after duplicate key error:`, updateError)
                        throw updateError
                    }
                } else {
                    throw error
                }
            }
            
            // Handle media download for Official API messages
            if (config.media?.enabled && isOfficialAPI) {
                log(`🔍 Checking for Official API media in message ${clonedMessage.key?.id}`)
                const mediaInfo = extractMediaInfo(clonedMessage)
                log(`📋 Media extraction result: ${mediaInfo ? `Found ${mediaInfo.type} media` : 'No media found'}`)
                
                if (mediaInfo) {
                    const mediaMessage = mediaInfo.message as any
                    log(`🆔 Media ID: ${mediaMessage.id}, Type: ${mediaInfo.type}, Mimetype: ${mediaInfo.mimetype}`)
                    
                    // Queue media download using shared queue manager
                    if (sharedQueueManager && useSharedQueues) {
                        try {
                            const queueAttempts = config.media?.maxRetries ? Math.max(config.media.maxRetries, 3) : 5
                            const backoffDelay = config.media?.retryDelay ?? 1000
                            await sharedQueueManager.addJob(
                                JobType.MEDIA_DOWNLOAD,
                                {
                                    message: clonedMessage,
                                    mediaInfo,
                                    jid: validJid
                                },
                                validatedInstanceId,
                                5, // Medium priority
                                {
                                    attempts: queueAttempts,
                                    backoff: { type: 'exponential', delay: backoffDelay }
                                }
                            )
                            log(`✅ Media download queued for message ${clonedMessage.key?.id}`)
                            config.logger?.info({
                                messageId: clonedMessage.key?.id,
                                jid: validJid,
                                mediaType: mediaInfo.type,
                                mediaId: mediaMessage.id,
                                isOfficialAPI
                            }, '📥 Media download queued via shared queue')
                        } catch (error) {
                            logError(`❌ Failed to queue media download, falling back to inline:`, error)
                            // Fall back to inline download
                            performInlineMediaDownload()
                        }
                    } else if (bullInitialized && queues.has(QueueType.MESSAGES)) {
                        // Queue via per-instance queue (for backward compatibility)
                        // Media download will be handled in the message queue processor
                        log(`📥 Media download will be handled by message queue processor`)
                    } else {
                        // Fall back to inline download
                        performInlineMediaDownload()
                    }
                    
                    // Helper function for inline media download (fallback)
                    async function performInlineMediaDownload() {
                        // Check for existing media by hash
                        const checkExistingMedia = async (hash: string): Promise<string | null> => {
                            const existing = await withConnection(async () =>
                                collections.messages.findOne({
                                    instanceId: validatedInstanceId,
                                    mediaHash: hash,
                                    mediaUrl: { $exists: true }
                                })
                            ) as any
                            return existing?.mediaUrl || null
                        }
                        
                        // Download media asynchronously
                        downloadOfficialAPIMedia(clonedMessage, validatedInstanceId, config.media!, config.logger, checkExistingMedia)
                            .then(async (mediaResult) => {
                                if (mediaResult.success && mediaResult.localPath) {
                                    // Update message with media URL
                                    await withConnection(async () =>
                                        collections.messages.updateOne(
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
                    }
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
            const existingMsg = await withConnection(async () =>
                collections.messages.findOne({
                    instanceId,
                    jid: normalizedJid,
                    'key.id': id
                })
            ) as any
            
            let finalUpdate = update
            
            if (existingMsg) {
                // Check if this is a MESSAGE_EDIT by looking for editedMessage field
                const isMessageEdit = !!(update.message?.editedMessage || (update as any).editedMessage)
                const originalTimestamp = existingMsg.messageTimestamp
                
                if (isMessageEdit) {
                    if (shouldLogOnce(`um_det_${id}`, 5)) {
                        log(`🔄 [updateMessage] Detected MESSAGE_EDIT for ${id}`)
                    }
                    if (shouldLogOnce(`um_ts_${id}`, 5)) {
                        log(`⏰ [updateMessage] Original timestamp: ${originalTimestamp}, New timestamp in update: ${update.messageTimestamp}`)
                    }
                }
                
                // Preserve original timestamp for edits
                if (isMessageEdit && originalTimestamp) {
                    finalUpdate = { ...update }
                    finalUpdate.messageTimestamp = originalTimestamp
                    if (shouldLogOnce(`um_pres_${id}`, 5)) {
                        log(`✅ [updateMessage] Preserved original messageTimestamp: ${originalTimestamp}`)
                    }
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
            
            const result = await withConnection(async () =>
                collections.messages.updateOne(
                    {
                        instanceId,
                        jid: normalizedJid,
                    'key.id': id
                },
                {
                    $set: { ...finalUpdate, updatedAt: new Date() }
                }
                )
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
            
            // Fallback to marking as revoked/deleted instead of deleting
            if (validIds && validIds.length > 0) {
                await withConnection(async () =>
                    collections.messages.updateMany(
                        { instanceId: validatedInstanceId, jid: validJid, 'key.id': { $in: validIds } },
                        { $set: { revoked: true, revokedAt: new Date(), updatedAt: new Date() } }
                    )
                )
            } else {
                await withConnection(async () =>
                    collections.messages.updateMany(
                        { instanceId: validatedInstanceId, jid: validJid },
                        { $set: { deleted: true, deletedAt: new Date(), updatedAt: new Date() } }
                    )
                )
            }
            } catch (error) {
                if (error instanceof ValidationError || error instanceof AuthorizationError) {
                    throw error
                }
                logError(createSafeErrorMessage(error as Error, 'deleteMessages'))
                throw new Error(createSafeErrorMessage(error as Error, 'deleteMessages'))
            }
        },

        async getGroupMetadata(jid: string): Promise<GroupMetadata | null> {
            const metadata = await withConnection(async () =>
                collections.groupMetadata.findOne({ instanceId, id: jid })
            )
            if (!metadata) return null
            
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...metadataData } = metadata
            return metadataData as GroupMetadata
        },
        
        async getAllGroupMetadata(): Promise<GroupMetadata[]> {
            const groups = await withConnection(async () =>
                collections.groupMetadata.find({ instanceId }).toArray()
            )
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
            await withConnection(async () =>
                collections.groupMetadata.replaceOne(
                    { instanceId, id: metadata.id },
                    { ...metadata, instanceId, updatedAt: new Date() },
                    { upsert: true }
                )
            )
        },

        async getState(): Promise<ConnectionState> {
            const state = await withConnection(async () =>
                collections.state.findOne({ instanceId })
            )
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
            await withConnection(async () =>
                collections.state.updateOne(
                    { instanceId },
                    { 
                        $set: { ...update, instanceId, updatedAt: new Date() }
                    },
                    { upsert: true }
                )
            )
        },

        async getPresences(): Promise<{ [id: string]: { [participant: string]: PresenceData } }> {
            const presences = await withConnection(async () =>
                collections.presences
                    .find({ instanceId })
                    .toArray()
            )
            
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
            await withConnection(async () =>
                collections.presences.updateOne(
                    { instanceId, id },
                    {
                        $set: { presences, updatedAt: new Date() }
                    },
                    { upsert: true }
                )
            )
        },

        async getLabels(): Promise<{ [id: string]: Label }> {
            const labels = await withConnection(async () =>
                collections.labels
                    .find({ instanceId })
                    .toArray()
            )
            
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
            await withConnection(async () =>
                collections.labels.replaceOne(
                    { instanceId, id },
                    { ...label, instanceId, updatedAt: new Date() },
                    { upsert: true }
                )
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
            await withConnection(async () =>
                collections.labels.deleteOne({ instanceId, id })
            )
        },

        async getLabelAssociations(): Promise<LabelAssociation[]> {
            const associations = await withConnection(async () =>
                collections.labelAssociations
                    .find({ instanceId })
                    .toArray()
            )
            
            return associations.map(({ _id, instanceId: _instanceId, updatedAt: _updatedAt, ...assoc }) => assoc as LabelAssociation)
        },

        async getChatLabels(chatId: string): Promise<LabelAssociation[]> {
            const associations = await withConnection(async () =>
                collections.labelAssociations
                    .find({ instanceId, chatId })
                    .toArray()
            )
            
            return associations.map(({ _id, instanceId: _instanceId, updatedAt: _updatedAt, ...assoc }) => assoc as LabelAssociation)
        },

        async getMessageLabels(messageId: string): Promise<string[]> {
            const associations = await withConnection(async () =>
                collections.labelAssociations
                    .find({ instanceId, messageId })
                    .toArray()
            )
            
            return associations.map(assoc => assoc.labelId)
        },

        async upsertLabelAssociation(association: LabelAssociation): Promise<void> {
            // Real-time LID resolution for chatId
            if (lidHandler && lidHandler.isLidFormat(association.chatId)) {
                const originalLid = association.chatId
                const resolvedChatId = await lidHandler.normalizeJid(association.chatId)
                if (resolvedChatId && resolvedChatId !== association.chatId) {
                    log(`[${instanceId}] Real-time LID resolution for label association: ${originalLid} -> ${resolvedChatId}`)
                    association.chatId = resolvedChatId

                    // Mark as having LID mapping for tracking
                    ;(association as any).lidMapping = {
                        resolved: true,
                        resolvedAt: new Date(),
                        originalLid: originalLid
                    }

                    // Update metrics
                    if (enableMetrics) {
                        updateLidResolutionMetrics('realtime-label-associations', 'resolved')
                    }
                }
            }

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
            
            const result = await withConnection(async () =>
                collections.labelAssociations.replaceOne(
                    filter,
                    {
                        ...association,
                        instanceId,
                        updatedAt: new Date()
                    },
                    { upsert: true }
                )
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
                    const conflictingJobs = existingJobs.filter((job: Job<LabelAssociationJob>) => {
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
                    for (const conflictingJob of conflictingJobs as Job<LabelAssociationJob>[]) {
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
            
            const result = await withConnection(async () =>
                collections.labelAssociations.deleteOne(filter)
            )
            
            if (result.deletedCount === 0) {
                logWarn(`[Direct] Warning: No label association found to delete - chatId: ${association.chatId}, labelId: ${association.labelId}, type: ${association.type}`)
            } else {
                log(`[Direct] ✅ Label association deleted - type: ${association.type}`)
            }
        },
        // bind method continues with event handling...
        bind(ev: BaileysEventEmitter): void {
            // Check if already bound to prevent duplicate bindings
            if (isBound) {
                log(`[${instanceId}] store.bind() already called, skipping duplicate binding`)
                return
            }
            isBound = true
            
            // Store the event emitter reference for cleanup
            currentEventEmitter = ev
            
            log(`[${instanceId}] store.bind() called - setting up event listeners with selective storage`)
            
            // Connection update
            const connectionUpdateHandler = async (update: any) => {
                if (enableMetrics) updateEventMetrics('connection.update', 'received')
                if (await shouldStoreEvent('connection.update', update)) {
                    await storeImpl.updateState(update)
                    if (enableMetrics) updateEventMetrics('connection.update', 'stored')
                    if (hooks.afterStore) await hooks.afterStore('connection.update', update)
                }
            }
            eventHandlers.set('connection.update', connectionUpdateHandler)
            ev.on('connection.update', connectionUpdateHandler)

            // Messages upsert - CRITICAL for storing messages including polls
            const messagesUpsertHandler = async ({ messages }: any) => {
                if (enableMetrics) updateEventMetrics('messages.upsert', 'received')
                
                for (const msg of messages) {
                    let jid = msg.key.remoteJid
                    if (!jid) continue
                    
                    // Skip protocol messages that shouldn't be stored as regular messages
                    if (msg.message?.protocolMessage) {
                        const protoType = msg.message.protocolMessage.type
                        
                        // Handle REVOKE messages - update the revoked message instead of storing the revoke message
                        if (protoType === proto.Message.ProtocolMessage.Type.REVOKE && msg.message.protocolMessage.key) {
                            const revokedKey = msg.message.protocolMessage.key
                            const outerChatJid = msg.key.remoteJid || jid
                            
                            // Always prefer the outer chat JID; protocolMessage.key.remoteJid can be unreliable
                            let targetJid = outerChatJid
                            if (lidHandler && targetJid) {
                                try {
                                    const normalized = await (lidHandler as any).normalizeJid(targetJid)
                                    if (normalized) targetJid = normalized
                                } catch {}
                            }
                            
                            log(`🔄 [REVOKE] Processing revoke message for ${revokedKey.id} in chat ${targetJid}`)
                            
                            try {
                                // Update the revoked message to mark it as deleted/revoked
                                const baseSet: any = {
                                    'message.protocolMessage': msg.message?.protocolMessage,
                                    revoked: true,
                                    revokedAt: new Date(),
                                    revokedBy: msg.key.fromMe ? 'me' : msg.key.participant || msg.key.remoteJid,
                                    messageStubType: 1 // REVOKE
                                }
                                
                                let updateResult = await withConnection(async () =>
                                    collections.messages.updateOne(
                                        { instanceId, jid: targetJid, 'key.id': revokedKey.id },
                                        { $set: baseSet }
                                    )
                                )
                                
                                // Fallback 1: try with outer (non-normalized) JID if different
                                if (updateResult.matchedCount === 0 && targetJid !== outerChatJid) {
                                    updateResult = await withConnection(async () =>
                                        collections.messages.updateOne(
                                            { instanceId, jid: outerChatJid, 'key.id': revokedKey.id },
                                            { $set: baseSet }
                                        )
                                    )
                                }
                                
                                // Fallback 2: match by id only within the instance (jid may have changed due to normalization/history)
                                if (updateResult.matchedCount === 0) {
                                    updateResult = await withConnection(async () =>
                                        collections.messages.updateOne(
                                            { instanceId, 'key.id': revokedKey.id },
                                            { $set: baseSet }
                                        )
                                    )
                                }
                                
                                if (updateResult.matchedCount > 0) {
                                    log(`✅ [REVOKE] Successfully marked message ${revokedKey.id} as revoked`)
                                } else {
                                    log(`⚠️ [REVOKE] Message ${revokedKey.id} not found to revoke`)
                                }
                            } catch (error) {
                                logError(`[REVOKE] Failed to process revoke for message ${revokedKey.id}:`, error)
                            }
                            
                            // Skip storing the REVOKE message itself
                            continue
                        }
                        
                        // Skip other protocol message types that shouldn't be stored
                        const skipTypes = [
                            proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION,
                            proto.Message.ProtocolMessage.Type.APP_STATE_SYNC_KEY_SHARE,
                            proto.Message.ProtocolMessage.Type.INITIAL_SECURITY_NOTIFICATION_SETTING_SYNC,
                            proto.Message.ProtocolMessage.Type.APP_STATE_SYNC_KEY_REQUEST
                        ]
                        
                        if (protoType && skipTypes.includes(protoType)) {
                            log(`⏭️ [Protocol] Skipping protocol message of type ${protoType} for instance ${instanceId}`)
                            continue
                        }
                    }
                    
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
                                    await lidHandler.storeLidMapping(remoteJid, senderPn, msg.pushName || msg.verifiedBizName)
                                    
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
                                            // Outgoing message: do not persist pushName (it's our own)
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
                                // Pattern X: FromMe=false, senderLid present and remoteJid already a phone number
                                // Proactively store mapping with pushName
                                else if (!isFromMe && senderLid && lidHandler.isLidFormat(senderLid) && remoteJid && !lidHandler.isLidFormat(remoteJid)) {
                                    log(`[LID] Pattern X: FromMe=false, senderLid with phone remoteJid`)
                                    await lidHandler.storeLidMapping(senderLid, remoteJid, msg.pushName || msg.verifiedBizName)
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
                                
                                const quotedMsg = await resolveQuotedMessage(msg, jid, collections, instanceId, log, withConnection)
                                if (quotedMsg && quotedMsg.message) {
                                    // Update the message with the resolved quoted content
                                    msg.message.extendedTextMessage.contextInfo.quotedMessage = quotedMsg.message
                                    log(`✅ [Event Handler] Updated message with resolved quoted content for ${msg.key?.id}`)
                                }
                            }
                            
                            // Decrypt poll vote before storing if present
                            if (msg.message?.pollUpdateMessage) {
                                const pollVoteDecrypted = await decryptPollVote(msg, collections, instanceId, meId, log, withConnection)
                                if (pollVoteDecrypted) {
                                    // Add decrypted poll vote data to the message
                                    (msg as any).pollVoteDecrypted = pollVoteDecrypted
                                    log(`✅ [Event Handler] Decrypted poll vote for ${msg.key?.id}`)
                                }
                            }
                            
                            // Before storing, persist pushName to contacts.notify (no override),
                            // only for incoming messages and user JIDs
                            try {
                                const pushName = (msg as any)?.pushName
                                const targetJid = jid
                                if (pushName && !msg.key.fromMe && targetJid && targetJid.endsWith('@s.whatsapp.net')) {
                                    // Route through CONTACTS queue to serialize writes
                                    if (queues.has(QueueType.CONTACTS)) {
                                        const queue = queues.get(QueueType.CONTACTS)!
                                        await queue.add(
                                            'update',
                                            {
                                                type: 'update',
                                                contact: { id: targetJid, notify: pushName },
                                                instanceId,
                                                timestamp: Date.now()
                                            },
                                            { ...defaultJobOptions, priority: 3 }
                                        )
                                    } else {
                                        // Fallback to direct guarded update
                                        const filter: any = {
                                            instanceId,
                                            id: targetJid,
                                            $or: [
                                                { notify: { $exists: false } },
                                                { notify: { $in: [null, ''] } }
                                            ]
                                        }
                                        await withConnection(async () => {
                                            try {
                                                await collections.contacts.updateOne(
                                                    filter,
                                                    {
                                                        $set: { notify: pushName, updatedAt: new Date() },
                                                        $setOnInsert: { instanceId, id: targetJid }
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
                                        })
                                    }
                                }
                            } catch (err) {
                                logWarn(`⚠️ [Contacts] Failed to persist pushName for ${jid}: ${String(err)}`)
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
                                    const existing = await withConnection(async () =>
                                        collections.messages.findOne({
                                            instanceId: validatedInstanceId,
                                            mediaHash: hash,
                                            mediaUrl: { $exists: true }
                                        })
                                    ) as any
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
                                
                                // Prefer background queue when available
                                const mediaInfo = extractMediaInfo(msg)
                                if (mediaInfo && sharedQueueManager && useSharedQueues) {
                                    try {
                                        const queueAttempts = config.media?.maxRetries ? Math.max(config.media.maxRetries, 3) : 5
                                        const backoffDelay = config.media?.retryDelay ?? 1000
                                        await sharedQueueManager.addJob(
                                            JobType.MEDIA_DOWNLOAD,
                                            { message: msg, mediaInfo, jid },
                                            validatedInstanceId,
                                            5,
                                            {
                                                attempts: queueAttempts,
                                                backoff: { type: 'exponential', delay: backoffDelay }
                                            }
                                        )
                                        log(`📥 Media download queued for message ${msg.key?.id}`)
                                        // Do not also attempt inline; queue will update DB when done
                                    } catch (e) {
                                        logError(`❌ Failed to queue media download, falling back inline:`, e)
                                    }
                                }

                                // Fallback inline behavior
                                let mediaResult
                                if (isOfficialAPI) {
                                    config.logger?.info({ 
                                        messageId: msg.key?.id,
                                        jid
                                    }, '📥 Attempting Official API media download (inline)')
                                    mediaResult = await downloadOfficialAPIMedia(msg, instanceId, config.media, config.logger, checkExistingMedia)
                                } else {
                                    mediaResult = await downloadMedia(msg, instanceId, config.media, config.logger, checkExistingMedia)
                                }

                                if (mediaResult.success && mediaResult.localPath) {
                                    await withConnection(async () =>
                                        collections.messages.updateOne(
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
                                    )
                                    if (mediaResult.reused) {
                                        log(`♻️  Media reused for message ${msg.key.id}: ${mediaResult.localPath}`)
                                    } else {
                                        log(`✅ Media downloaded inline for message ${msg.key.id}: ${mediaResult.localPath}`)
                                    }
                                } else if (!mediaResult.success && mediaResult.error) {
                                    log(`⚠️ Media download failed inline for message ${msg.key.id}: ${mediaResult.error}`)
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
            }
            eventHandlers.set('messages.upsert', messagesUpsertHandler)
            ev.on('messages.upsert', messagesUpsertHandler)

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
                    
                    // Deduplicate identical updates for the same message within a short window
                    try {
                        const updateSignature = JSON.stringify(update.update?.message?.editedMessage || update.update || {})
                        const dedupKey = `mu_${update.key.id}_${hashForLogging(updateSignature)}`
                        if (processedEditCache.get(dedupKey)) {
                            continue
                        }
                        processedEditCache.set(dedupKey, true, 5)
                    } catch {}

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
                                    if (shouldLogOnce(`mu_det_${update.key.id}`, 5)) {
                                        log(`🔄 [messages.update] Detected MESSAGE_EDIT for ${update.key.id}`)
                                    }
                                    if (shouldLogOnce(`mu_ts_${update.key.id}`, 5)) {
                                        log(`⏰ [messages.update] Original timestamp: ${originalTimestamp}, New timestamp in update: ${update.update?.messageTimestamp}`)
                                    }
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
                                    if (shouldLogOnce(`mu_pres_${update.key.id}`, 5)) {
                                        log(`✅ [messages.update] Preserved original messageTimestamp: ${originalTimestamp}`)
                                    }
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
                                if (shouldLogOnce(`mu_done_${update.key.id}`, 2)) {
                                    log(`✅ Updated message ${update.key.id} preserving quoted message structure${isMessageEdit ? ' and original timestamp' : ''}`)
                                }
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
                    const fromMe = item.keys[0]?.fromMe
                    if (await shouldStoreEvent('messages.delete', item)) {
                        try {
                            // Retain original messages but mark them as revoked/deleted instead of removing
                            const updateFields: any = {
                                revoked: true,
                                revokedAt: new Date(),
                                updatedAt: new Date()
                            }
                            if (typeof fromMe === 'boolean') {
                                updateFields.revokedBy = fromMe ? 'me' : 'remote'
                            }
                            await withConnection(async () =>
                                collections.messages.updateMany(
                                    {
                                        instanceId,
                                        jid,
                                        'key.id': { $in: ids }
                                    },
                                    { $set: updateFields }
                                )
                            )
                            if (shouldLogOnce(`md_mark_${hashForLogging(jid)}_${hashForLogging(ids.join(','))}`, 5)) {
                                log(`✅ Marked ${ids.length} message(s) as revoked in ${jid}`)
                            }
                            if (enableMetrics) updateEventMetrics('messages.delete', 'stored')
                            if (hooks.afterStore) await hooks.afterStore('messages.delete', item)
                        } catch (error) {
                            logError(`Failed to mark messages revoked for ${jid}:`, error)
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
                            // Retain messages but mark them as deleted for the chat
                            await withConnection(async () =>
                                collections.messages.updateMany(
                                    { instanceId, jid },
                                    { $set: { deleted: true, deletedAt: new Date(), updatedAt: new Date() } }
                                )
                            )
                            if (shouldLogOnce(`md_mark_all_${hashForLogging(jid)}`, 10)) {
                                log(`✅ Marked all messages as deleted in ${jid}`)
                            }
                            if (enableMetrics) updateEventMetrics('messages.delete', 'stored')
                            if (hooks.afterStore) await hooks.afterStore('messages.delete', item)
                        } catch (error) {
                            logError(`Failed to mark all messages deleted for ${item.jid}:`, error)
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
            // Messaging history sync with debouncing
            ev.on('messaging-history.set', async ({ chats: newChats, contacts: newContacts, messages: newMessages, isLatest }) => {
                if (enableMetrics) updateEventMetrics('messaging-history.set', 'received')
                
                const historyData = { chats: newChats, contacts: newContacts, messages: newMessages, isLatest }
                if (await shouldStoreEvent('messaging-history.set', historyData)) {
                    try {
                        // Add to pending data
                        pendingHistoryData.push(historyData)
                        
                        // Clear existing timer if any
                        if (historyDebounceTimer) {
                            clearTimeout(historyDebounceTimer)
                        }
                        
                        // Set new timer to process accumulated data (respect config)
                        const debounceDelay = config.debounceHistoryEvents === false ? 0 : (config.debounceDelay || 500)
                        historyDebounceTimer = setTimeout(async () => {
                            historyDebounceTimer = null
                            
                            try {
                                if (pendingHistoryData.length === 0) return
                                
                                const dataToProcess = [...pendingHistoryData]
                                pendingHistoryData.length = 0 // Clear array
                                
                                log(`[${instanceId}] Processing ${dataToProcess.length} accumulated history events`)
                                
                                // Check if any event has isLatest=true
                                const hasLatest = dataToProcess.some(data => data.isLatest)
                                
                                // Clear all data if any event has isLatest=true and clearAllOnHistorySync is enabled
                                if (hasLatest && config.clearAllOnHistorySync) {
                                log(`[${instanceId}] Clearing data before syncing latest history (isLatest=true, clearAllOnHistorySync=true) while preserving contacts`)
                                await storeImpl.clearAll({ preserve: ['contacts'] })
                                }
                                
                                // Merge all history data
                                const mergedChats = new Map<string, Chat>()
                                const mergedContacts = new Map<string, Contact>()
                                const mergedMessages = new Map<string, proto.IWebMessageInfo[]>()
                                
                                for (const data of dataToProcess) {
                                    // Merge chats
                                    if (data.chats?.length) {
                                        for (const chat of data.chats) {
                                            mergedChats.set(chat.id, chat)
                                        }
                                    }
                                    
                                    // Merge contacts
                                    if (data.contacts?.length) {
                                        for (const contact of data.contacts) {
                                            mergedContacts.set(contact.id, contact)
                                        }
                                    }
                                    
                                    // Merge messages by chat
                                    if (data.messages?.length) {
                                        for (const msg of data.messages) {
                                            const chatId = msg.key.remoteJid!
                                            if (!mergedMessages.has(chatId)) {
                                                mergedMessages.set(chatId, [])
                                            }
                                            mergedMessages.get(chatId)!.push(msg)
                                        }
                                    }
                                }
                                
                                // Process the merged data
                                const promises: Promise<void>[] = []
                                
                                const allChats = Array.from(mergedChats.values())
                                const allContacts = Array.from(mergedContacts.values())
                                const allMessages = Array.from(mergedMessages.values()).flat()
                                
                                if (allChats.length) {
                                    promises.push(storeImpl.upsertChats(...allChats))
                                }
                                
                                if (allContacts.length) {
                                    promises.push(storeImpl.upsertContacts(allContacts))
                                }
                                
                                if (allMessages.length) {
                                    // Group messages by chat for batch processing
                                    const messagesByChat = new Map<string, proto.IWebMessageInfo[]>()
                                    for (const msg of allMessages) {
                                        const chatId = msg.key.remoteJid!
                                        if (!messagesByChat.has(chatId)) {
                                            messagesByChat.set(chatId, [])
                                        }
                                        messagesByChat.get(chatId)!.push(msg)
                                    }
                                    
                                    // Process messages for each chat
                                    for (const [chatId, messages] of messagesByChat) {
                                        for (const msg of messages) {
                                            promises.push(storeImpl.upsertMessage(chatId, msg, true))
                                        }
                                    }
                                }
                                
                                await Promise.all(promises)
                                log(`[${instanceId}] Successfully processed ${dataToProcess.length} accumulated history events`)

                                // Proactively resolve LID jids in historical messages (only once per instance)
                                if (hasLatest && lidHandler && !historyLidResolutionDone && (finalLidConfig.proactiveHistoryResolution === true)) {
                                    try {
                                        await storeImpl.performProactiveLidResolutionForHistory()
                                        historyLidResolutionDone = true
                                        log(`[${instanceId}] Completed proactive LID resolution for historical messages`)
                                    } catch (error) {
                                        logError(`[${instanceId}] Error during proactive LID resolution:`, error)
                                    }
                                }
                            } catch (error) {
                                logError(`[${instanceId}] Error processing accumulated history:`, error)
                            }
                        }, debounceDelay)
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
                            const deleteResult = await withConnection(async () =>
                                collections.labelAssociations.deleteMany({
                                    instanceId,
                                    labelId: label.id
                                })
                            )
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

        // Method to update socket reference after store creation
        setSock(socket: any): void {
            sock = socket
            log(`[${instanceId}] Socket reference updated in store`)
            
            // SAFETY: Auto-rebind if we have an event emitter and were previously bound
            if (socket?.ev && isBound) {
                log(`[${instanceId}] Auto-rebinding to new socket's event emitter for safety`)
                storeImpl.rebind(socket.ev)
            }
            
            // Re-initialize profile picture retrieval if enabled
            if (profilePictureConfig?.enabled && socket) {
                log(`[${instanceId}] Re-initializing profile picture retrieval with new socket`)
                // Profile picture functionality will use the updated socket
            }

            // Pass reuploadRequest into media config if available for more reliable downloads
            try {
                // Best-effort detection of Baileys reupload function
                const reupload = (socket as any)?.waUploadToServer || (socket as any)?.reuploadRequest || (socket as any)?.reuploadMedia
                if (reupload && config.media) {
                    (config.media as any).reuploadRequest = reupload
                    log(`[${instanceId}] Enabled reuploadRequest for media downloads`)
                }
            } catch {}

        },

        // Health check method for store and database connection
        async isHealthy(): Promise<boolean> {
            try {
                // Check MongoDB connection state
                if (mongoConnectionState !== MongoConnectionState.CONNECTED) {
                    log(`[${instanceId}] Health check failed: MongoDB not connected`)
                    return false
                }
                
                // Ping the database to ensure it's responsive
                await db.admin().ping()
                
                // Check Redis connection if Bull is initialized
                if (bullInitialized && redisConnection) {
                    await redisConnection.ping()
                }
                
                log(`[${instanceId}] Health check passed`)
                return true
            } catch (error) {
                logError(`[${instanceId}] Health check failed:`, error)
                return false
            }
        },

        // Unbind method to properly remove all event listeners
        unbind(): void {
            if (!isBound || !currentEventEmitter) {
                log(`[${instanceId}] unbind() called but not currently bound`)
                return
            }

            log(`[${instanceId}] Unbinding event listeners from current emitter`)
            
            // Clear history debounce timer if active
            if (historyDebounceTimer) {
                clearTimeout(historyDebounceTimer)
                historyDebounceTimer = null
                pendingHistoryData.length = 0 // Clear pending data
                log(`[${instanceId}] Cleared pending history sync data`)
            }

            // Remove all registered event listeners
            for (const [eventName, handler] of eventHandlers.entries()) {
                try {
                    (currentEventEmitter as any).off(eventName, handler)
                    log(`[${instanceId}] Removed listener for: ${eventName}`)
                } catch (error) {
                    logWarn(`[${instanceId}] Failed to remove listener for ${eventName}:`, error)
                }
            }

            // Clear the handlers registry
            eventHandlers.clear()
            
            // Reset state
            currentEventEmitter = null
            isBound = false

            log(`[${instanceId}] Successfully unbound all event listeners`)
        },

        // Safe rebind method - unbinds existing listeners before binding new ones
        rebind(ev: BaileysEventEmitter): void {
            // First unbind from previous emitter if bound
            if (isBound && currentEventEmitter) {
                storeImpl.unbind()
            }
            
            // Now bind to new emitter
            storeImpl.bind(ev)
        },

        // Connection recovery method
        async reconnect(): Promise<void> {
            // If already reconnecting, wait for it to complete
            if (mongoConnectionState === MongoConnectionState.CONNECTING || 
                mongoConnectionState === MongoConnectionState.RECONNECTING) {
                log(`[${instanceId}] Already reconnecting, waiting for completion...`)
                return new Promise((resolve, reject) => {
                    const timeout = setTimeout(() => {
                        connectionStateEmitter.off('connected', onConnected)
                        connectionStateEmitter.off('failed', onFailed)
                        reject(new Error('Reconnection timeout'))
                    }, 30000)
                    
                    const onConnected = () => {
                        clearTimeout(timeout)
                        resolve()
                    }
                    
                    const onFailed = (error: Error) => {
                        clearTimeout(timeout)
                        reject(error)
                    }
                    
                    connectionStateEmitter.once('connected', onConnected)
                    connectionStateEmitter.once('failed', onFailed)
                })
            }
            
            // Force reconnection
            log(`[${instanceId}] Initiating reconnection...`)
            await ensureConnection()
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
                
                messages = await withConnection(async () =>
                    collections.messages
                        .find(query)
                        .sort({ messageTimestamp: -1 })
                        .limit(count)
                        .toArray()
                        .then(msgs => msgs.map(({ _id, instanceId: _instanceId, jid: _jid, updatedAt: _updatedAt, ...msg }) => convertBinaryToBuffer(msg)))
                )
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

        async clearAll(options?: { preserve?: Array<'chats' | 'contacts' | 'messages' | 'presences'> }): Promise<void> {
            // Check if clearAll is already in progress
            if (clearAllInProgress) {
                log(`[${instanceId}] clearAll already in progress, skipping duplicate call`)
                return
            }
            
            // Check if store is closing
            if (isClosing) {
                log(`[${instanceId}] Store is closing, skipping clearAll`)
                return
            }
            
            clearAllInProgress = true
            const startTime = Date.now()
            
            try {
                log(`[CLEAR_ALL_START] Instance: ${instanceId}, Pending ops: ${pendingOperations.size}`)
                
                // Cancel any pending profile picture fetching
                if (profilePictureFetchHandle) {
                    clearImmediate(profilePictureFetchHandle)
                    profilePictureFetchHandle = null
                    log(`[${instanceId}] Cancelled profile picture background fetch`)
                }
                
                // Wait for pending operations with timeout (if configured)
                if (pendingOperations.size > 0 && config.waitForPendingOps !== false) {
                    log(`[${instanceId}] Waiting for ${pendingOperations.size} pending operations...`)
                    const maxWait = config.maxPendingOpsWait || config.clearAllTimeout || 10000
                    await Promise.race([
                        Promise.all(Array.from(pendingOperations)),
                        new Promise(resolve => setTimeout(resolve, maxWait))
                    ])
                    pendingOperations.clear()
                }
                
                // Pause all queues if using Bull
                if (bullInitialized) {
                    log(`[${instanceId}] Pausing queues before clearAll...`)
                    for (const [queueType, queue] of queues) {
                        try {
                            await queue.pause()
                            // Drain any pending jobs
                            await queue.drain()
                            log(`[${instanceId}] Paused and drained queue: ${queueType}`)
                        } catch (error) {
                            logWarn(`[${instanceId}] Failed to pause queue ${queueType}:`, error)
                        }
                    }
                }
                
                // Pause SharedQueueManager processing for this instance
                if (sharedQueueManager && useSharedQueues) {
                    log(`[${instanceId}] Pausing SharedQueueManager processing...`)
                    try {
                        // Temporarily unregister processors to stop processing
                        sharedQueueManager.unregisterInstanceProcessors(validatedInstanceId)
                        log(`[${instanceId}] SharedQueueManager processors paused`)
                    } catch (error) {
                        logWarn(`[${instanceId}] Failed to pause SharedQueueManager:`, error)
                    }
                }
                
                // Clear cache entries
                const keys = binaryConversionCache.keys()
                keys.forEach(key => {
                    if (key.startsWith(`msg_${instanceId}_`)) {
                        binaryConversionCache.del(key)
                    }
                })
                
                // Perform the actual deletion
                const preserveSet = new Set(options?.preserve || [])

                const runDeletes = async (session?: ClientSession) => {
                    const deletions: Array<Promise<any>> = []

                    log(`[${instanceId}] Deleting collections${preserveSet.size ? ` (preserving: ${Array.from(preserveSet).join(', ')})` : ''}...`)
                    if (!preserveSet.has('chats')) {
                        deletions.push(collections.chats.deleteMany({ instanceId: validatedInstanceId }, { session }))
                    }
                    if (!preserveSet.has('contacts')) {
                        deletions.push(collections.contacts.deleteMany({ instanceId: validatedInstanceId }, { session }))
                    }
                    if (!preserveSet.has('messages')) {
                        deletions.push(collections.messages.deleteMany({ instanceId: validatedInstanceId }, { session }))
                    }
                    if (!preserveSet.has('presences')) {
                        deletions.push(collections.presences.deleteMany({ instanceId: validatedInstanceId }, { session }))
                    }

                    if (deletions.length === 0) {
                        log(`[${instanceId}] No collections selected for deletion during clearAll`)
                        return
                    }

                    await Promise.all(deletions)
                }

                await withConnection(async () => {
                    await runDeletes()
                })

                const duration = Date.now() - startTime
                log(`[CLEAR_ALL_END] Instance: ${instanceId}, Duration: ${duration}ms`)
                
            } catch (error) {
                logError(`[CLEAR_ALL_ERROR] Instance: ${instanceId}, Error:`, error)
                throw error
            } finally {
                // Resume queues
                if (bullInitialized) {
                    for (const [queueType, queue] of queues) {
                        try {
                            await queue.resume()
                            log(`[${instanceId}] Resumed queue: ${queueType}`)
                        } catch (error) {
                            logWarn(`[${instanceId}] Failed to resume queue ${queueType}:`, error)
                        }
                    }
                }
                
                // Re-register SharedQueueManager processors
                if (sharedQueueManager && useSharedQueues) {
                    log(`[${instanceId}] Re-registering SharedQueueManager processors...`)
                    try {
                        await registerSharedQueueProcessors()
                        log(`[${instanceId}] SharedQueueManager processors re-registered`)
                    } catch (error) {
                        logWarn(`[${instanceId}] Failed to re-register SharedQueueManager processors:`, error)
                    }
                }
                
                clearAllInProgress = false
            }
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
            return withConnection(async () => {
                let totalCreated = 0
                let totalFailed = 0
                const allDetails: string[] = []
                
                // Define index definitions (same as in createIndexes)
                const indexDefinitions: Record<string, IndexSpec[]> = {
                    chats: [
                        { name: 'chats_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } },
                        { name: 'chats_ttl', spec: { updatedAt: 1 }, options: { expireAfterSeconds: getTTLForCollection('chats') * 24 * 60 * 60 } }
                    ],
                    contacts: [
                        { name: 'contacts_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } },
                        { name: 'contacts_lid_lookup', spec: { instanceId: 1, lid: 1 }, options: { unique: true, partialFilterExpression: { lid: { $type: 'string' } } } },
                        // NEW: Composite index for efficient $in queries with projection fields
                        { name: 'contacts_batch_lookup', spec: { instanceId: 1, id: 1, name: 1, profilePic: 1, profilePicUpdatedAt: 1, notify: 1, lid: 1 }, options: {} },
                        // NEW: Covering index for lid->id lookups to avoid document fetch
                        { name: 'contacts_lid_id_cover', spec: { instanceId: 1, lid: 1, id: 1 }, options: { partialFilterExpression: { lid: { $type: 'string' } } } }
                    ],
                    messages: [
                        { name: 'messages_primary', spec: { instanceId: 1, jid: 1, 'key.id': 1 }, options: { unique: true } },
                        { name: 'messages_jid_timestamp', spec: { instanceId: 1, jid: 1, messageTimestamp: -1 }, options: {} },
                        { name: 'messages_ttl', spec: { updatedAt: 1 }, options: { expireAfterSeconds: getTTLForCollection('messages') * 24 * 60 * 60 } },
                        // Ensure compound index exists for manual per-instance cleanup
                        { name: 'messages_instance_updatedAt', spec: { instanceId: 1, updatedAt: 1 }, options: {} },
                        { name: 'messages_media_dedup', spec: { instanceId: 1, mediaHash: 1 }, options: { sparse: true } },
                        { name: 'messages_remote_fallback', spec: { instanceId: 1, 'key.remoteJid': 1, 'key.id': 1 }, options: {} },
                        { name: 'messages_keyid_direct', spec: { instanceId: 1, 'key.id': 1 }, options: {} },
                        // Supports reverse lookup for LID discovery when only senderLid is present on incoming messages
                        { name: 'messages_senderLid_lookup', spec: { instanceId: 1, 'key.fromMe': 1, 'key.senderLid': 1 }, options: {} },
                        // Index for LID resolution tracking
                        { name: 'messages_lid_resolution', spec: { instanceId: 1, 'lidMapping.resolved': 1 }, options: { sparse: true } },
                        // Index for media deduplication by fileHash
                        { name: 'messages_media_fileHash', spec: { 'mediaInfo.fileHash': 1 }, options: { sparse: true } }
                    ],
                    groupMetadata: [
                        { name: 'groupMetadata_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } }
                    ],
                    state: [
                        { name: 'state_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } },
                        { name: 'state_ttl', spec: { updatedAt: 1 }, options: { expireAfterSeconds: getTTLForCollection('state') * 24 * 60 * 60 } }
                    ],
                    presences: [
                        { name: 'presences_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } },
                        { name: 'presences_ttl', spec: { updatedAt: 1 }, options: { expireAfterSeconds: getTTLForCollection('presences') * 24 * 60 * 60 } }
                    ],
                    labels: [
                        { name: 'labels_primary', spec: { instanceId: 1, id: 1 }, options: { unique: true } }
                    ],
                    labelAssociations: [
                        { name: 'labelAssociations_primary', spec: { instanceId: 1, chatId: 1, messageId: 1, labelId: 1 }, options: { unique: true } },
                        { name: 'labelAssociations_chatId_labelId', spec: { instanceId: 1, chatId: 1, labelId: 1 }, options: {} },
                        { name: 'labelAssociations_messageId_labelId', spec: { instanceId: 1, messageId: 1, labelId: 1 }, options: {} },
                        // Index for LID resolution tracking
                        { name: 'labelAssociations_lid_resolution', spec: { instanceId: 1, 'lidMapping.resolved': 1 }, options: { sparse: true } }
                    ],
                    // Include LidHandler's collection in smart index management
                    lidMappings: [
                        { name: 'lidMappings_primary', spec: { instanceId: 1, lid: 1 }, options: { unique: true } },
                        { name: 'lidMappings_phone_lookup', spec: { instanceId: 1, phoneNumber: 1 }, options: {} }
                    ]
                }
                
                // Process each collection
                for (const [collectionName, requiredIndexes] of Object.entries(indexDefinitions)) {
                    try {
                        const collection = collections[collectionName as keyof typeof collections]
                        
                        // Apply timeout to indexes
                        const timedIndexes = requiredIndexes.map(addIndexTimeout)
                        
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
            const collectionNames = ['chats', 'contacts', 'messages', 'groupMetadata', 'state', 'presences', 'labels', 'labelAssociations']
            const indexStatus = []
            
            for (const collName of collectionNames) {
                try {
                    const collection = collections[collName as keyof MongoCollections]
                    const indexes = await withConnection(async () => 
                        collection.listIndexes().toArray()
                    )
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
                // Fetch the message from database (fallback to id-only match if needed)
                let message = await withConnection(async () =>
                    collections.messages.findOne({
                        instanceId,
                        jid,
                        'key.id': messageId
                    })
                )
                if (!message) {
                    message = await withConnection(async () =>
                        collections.messages.findOne({
                            instanceId,
                            'key.id': messageId
                        })
                    )
                }
                
                if (!message) {
                    return { success: false, error: 'Message not found' }
                }
                
                // Check if media already downloaded
                if ((message as any).mediaUrl) {
                    return { success: true, localPath: (message as any).mediaUrl }
                }
                
                // Function to check for existing media by hash
                const checkExistingMedia = async (hash: string): Promise<string | null> => {
                    const existing = await withConnection(async () =>
                        collections.messages.findOne({
                            instanceId: validatedInstanceId,
                            mediaHash: hash,
                            mediaUrl: { $exists: true }
                        })
                    ) as any
                    return existing?.mediaUrl || null
                }

                // Determine download strategy
                const isOfficialAPI = (message as any).official_api === true
                let mediaResult
                if (isOfficialAPI) {
                    mediaResult = await downloadOfficialAPIMedia(
                        message as proto.IWebMessageInfo,
                        instanceId,
                        config.media,
                        config.logger,
                        checkExistingMedia,
                        { attempt: 1 }
                    )
                } else {
                    mediaResult = await downloadMedia(
                        message as proto.IWebMessageInfo,
                        instanceId,
                        config.media,
                        config.logger,
                        checkExistingMedia
                    )
                }
                
                if (mediaResult.success && mediaResult.localPath) {
                    // Update message with media URL
                    const filter = (message as any).jid
                        ? { instanceId, jid: (message as any).jid, 'key.id': messageId }
                        : jid
                            ? { instanceId, jid, 'key.id': messageId }
                            : { instanceId, 'key.id': messageId }
                    const mediaUpdate: Record<string, any> = {
                        mediaUrl: mediaResult.localPath,
                        mediaDownloadedAt: new Date()
                    }
                    if (mediaResult.mediaType) mediaUpdate.mediaType = mediaResult.mediaType
                    if (mediaResult.fileName) mediaUpdate.mediaFileName = mediaResult.fileName
                    if (typeof mediaResult.fileSize === 'number') mediaUpdate.mediaFileSize = mediaResult.fileSize
                    if (mediaResult.mediaHash) mediaUpdate.mediaHash = mediaResult.mediaHash
                    if (typeof mediaResult.reused !== 'undefined') mediaUpdate.mediaReused = mediaResult.reused
                    
                    await withConnection(async () =>
                        collections.messages.updateOne(
                            filter,
                            { 
                                $set: mediaUpdate 
                            }
                        )
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
        
        async getConnectionMetrics(): Promise<any> {
            if (isUsingSharedConnection && connectionManager) {
                // Get metrics from connection manager
                return connectionManager.getMetrics()
            } else if (client) {
                // Return basic metrics for dedicated connection
                return {
                    type: 'dedicated',
                    instanceId: validatedInstanceId,
                    connected: true, // If client exists, it's connected
                    connectionConfig: {
                        maxPoolSize: dedicatedMaxPoolSize,
                        minPoolSize: dedicatedMinPoolSize,
                        maxIdleTimeMS: 30000
                    }
                }
            } else {
                return {
                    type: 'none',
                    error: 'No active connection'
                }
            }
        },
        
        async cleanup(deleteData: boolean = false): Promise<void> {
            log(`[${instanceId}] Starting cleanup - deleteData: ${deleteData}`)
            
            try {
                if (deleteData) {
                    log(`[${instanceId}] Deleting all data for instance with prefix: ${collectionPrefix}`)
                    
                    // Define all collections with the correct prefix
                    const collections = [
                        `${collectionPrefix}chats`,
                        `${collectionPrefix}contacts`,
                        `${collectionPrefix}messages`,
                        `${collectionPrefix}groupMetadata`,
                        `${collectionPrefix}state`,
                        `${collectionPrefix}presences`,
                        `${collectionPrefix}labels`,
                        `${collectionPrefix}labelAssociations`,
                        `${collectionPrefix}lidMappings`
                    ]
                    
                    let totalDeleted = 0
                    
                    for (const collName of collections) {
                        try {
                            const result = await withConnection(async () =>
                                db.collection(collName).deleteMany({ instanceId: validatedInstanceId })
                            )
                            
                            if (result.deletedCount > 0) {
                                log(`[${instanceId}] Deleted ${result.deletedCount} documents from ${collName}`)
                                totalDeleted += result.deletedCount
                            }
                        } catch (error) {
                            logError(`[${instanceId}] Error deleting data from ${collName}:`, error)
                        }
                    }
                    
                    log(`[${instanceId}] Cleanup completed: ${totalDeleted} total documents deleted`)
                    
                    // Clear any cached data
                    const cacheKeys = binaryConversionCache.keys()
                    cacheKeys.forEach(key => {
                        if (key.startsWith(`msg_${validatedInstanceId}_`)) {
                            binaryConversionCache.del(key)
                        }
                    })
                }
                
                // Always close connections after data cleanup
                await storeImpl.close()
                
            } catch (error) {
                logError(`[${instanceId}] Cleanup failed:`, error)
                throw error
            }
        },
        
        async close(): Promise<void> {
            // Set closing flag to stop background operations
            isClosing = true
            
            // Unbind all event listeners to prevent memory leaks
            if (isBound && currentEventEmitter) {
                log(`[${instanceId}] Unbinding all event listeners during close`)
                storeImpl.unbind()
            }
            
            // Clear profile picture fetch handle if it exists
            if (profilePictureFetchHandle) {
                clearImmediate(profilePictureFetchHandle)
                profilePictureFetchHandle = null
            }
            
            // Clear stale operation cleanup timer
            if (staleOperationCleanupTimer) {
                clearInterval(staleOperationCleanupTimer)
                staleOperationCleanupTimer = null
                log(`[${instanceId}] Stopped stale operation cleanup timer`)
            }
            
            // Unregister instance processors from SharedQueueManager to prevent memory leaks
            if (sharedQueueManager && useSharedQueues) {
                log(`🗑️ Unregistering processors for instance ${instanceId} from SharedQueueManager`)
                sharedQueueManager.unregisterInstanceProcessors(validatedInstanceId)
                try {
                    await sharedQueueManager.releaseInstanceOwnership(validatedInstanceId)
                    log(`[${instanceId}] Released shared queue ownership`)
                } catch (error) {
                    logWarn(`[${instanceId}] Failed to release shared queue ownership:`, error)
                }
            }
            
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
            if (bullInitialized && !useSharedQueues) {
                log(`🛑 Closing Bull queues for instance ${instanceId}...`)
                try {
                    // Close all workers and remove event listeners
                    for (const worker of workers.values()) {
                        // Remove event listeners if they exist
                        if ((worker as any).__eventHandlers) {
                            const handlers = (worker as any).__eventHandlers
                            worker.off('completed', handlers.completed)
                            worker.off('failed', handlers.failed)
                            worker.off('stalled', handlers.stalled)
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
                    if (lidRedisClient && lidRedisClient !== redisConnection) {
                        try { lidRedisClient.disconnect() } catch {}
                    }
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
            
            // Handle connection cleanup based on connection type
            if (isUsingSharedConnection && connectionManager) {
                // Unregister from connection manager
                await connectionManager.unregisterInstance(validatedInstanceId)
                connectionManager = null
                currentSharedPoolId = null
            } else if (client && !isUsingSharedConnection) {
                // Close dedicated connection
                await client.close()
                currentSharedPoolId = null
            }
            
            // Reset connection state
            mongoConnectionState = MongoConnectionState.DISCONNECTED
            reconnectAttempts = 0
            
            // Clean up EventEmitter to prevent memory leaks
            connectionStateEmitter.removeAllListeners()
            
            // Reset binding state
            isBound = false
        },

        /**
         * Proactively resolve LID jids in historical messages to phone numbers
         * This runs once per instance during initial history sync
         */
        async performProactiveLidResolutionForHistory(): Promise<void> {
            if (!lidHandler) return

            log(`[${instanceId}] Starting proactive LID resolution for historical messages`)

            // Get all LID mappings from contacts collection
            const lidMappings = await withConnection(async () =>
                collections.contacts.find(
                    {
                        instanceId: validatedInstanceId,
                        lid: { $exists: true, $ne: '' }
                    },
                    { projection: { lid: 1, id: 1 } }
                ).toArray()
            )

            if (lidMappings.length === 0) {
                log(`[${instanceId}] No LID mappings found, skipping proactive resolution`)
                return
            }

            log(`[${instanceId}] Found ${lidMappings.length} LID mappings for proactive resolution`)

            // Process mappings in batches to avoid overwhelming the database
            const batchSize = 50
            let totalMessagesUpdated = 0

            for (let i = 0; i < lidMappings.length; i += batchSize) {
                const batch = lidMappings.slice(i, i + batchSize)
                const batchPromises = batch.map(async (mapping: any) => {
                    const lid = mapping.lid
                    const phoneNumber = mapping.id

                    if (!lidHandler!.isLidFormat(lid) || lidHandler!.isLidFormat(phoneNumber)) {
                        return 0 // Skip invalid mappings
                    }

                    try {
                        // Update messages where jid equals the LID
                        const updateResult = await withConnection(async () =>
                            collections.messages.updateMany(
                                {
                                    instanceId: validatedInstanceId,
                                    jid: lid
                                },
                                {
                                    $set: {
                                        jid: phoneNumber,
                                        'key.remoteJid': phoneNumber,
                                        'lidMapping.resolved': true,
                                        'lidMapping.resolvedAt': new Date(),
                                        'lidMapping.originalLid': lid,
                                        updatedAt: new Date()
                                    }
                                }
                            )
                        )

                        if (updateResult.modifiedCount > 0) {
                            log(`[${instanceId}] Updated ${updateResult.modifiedCount} historical messages: ${lid} -> ${phoneNumber}`)
                        }

                        return updateResult.modifiedCount
                    } catch (error) {
                        logError(`[${instanceId}] Error updating messages for LID ${lid}:`, error)
                        return 0
                    }
                })

                const batchResults = await Promise.all(batchPromises)
                totalMessagesUpdated += batchResults.reduce((sum, count) => sum + count, 0)

                // Small delay between batches to prevent overwhelming
                if (i + batchSize < lidMappings.length) {
                    await new Promise(resolve => setTimeout(resolve, 100))
                }
            }

            // Perform proactive LID resolution for label associations after messages are done
            let totalLabelAssociationsUpdated = 0
            if (finalLidConfig.proactiveHistoryResolution === true) {
                log(`[${instanceId}] Starting proactive LID resolution for label associations`)
                totalLabelAssociationsUpdated = await performProactiveLidResolutionForLabelAssociations(lidMappings as Array<{ lid: string | undefined; id: string }>)
            } else {
                log(`[${instanceId}] Skipping proactive LID resolution for label associations (disabled)`)
            }

            log(`[${instanceId}] Proactive LID resolution completed: ${totalMessagesUpdated} historical messages and ${totalLabelAssociationsUpdated} label associations updated`)
        }
    }

    return storeImpl
}
