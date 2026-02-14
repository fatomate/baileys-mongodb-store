type Comparable<T, K> = { key: (t: T) => K }
import type { Logger } from 'pino'
import type { proto, WASocket } from 'baileys'
import type { BaileysEventEmitter, Chat, ConnectionState, Contact, GroupMetadata, PresenceData, WAMessageCursor } from 'baileys'
import type { Label } from 'baileys/lib/Types/Label.js'
import type { LabelAssociation } from 'baileys/lib/Types/LabelAssociation.js'
import type { RedisOptions } from 'ioredis'
import type { AuthConfig } from './utils/auth.js'
import type { MemoryConfig } from './utils/memory.js'
import type { TTLConfig } from './utils/ttl.js'
import type { MediaConfig } from './utils/media.js'
import type { LidHandlerConfig } from './utils/lidHandler.js'
import type { ConnectionConfig, ConnectionManagerConfig } from './types/connection.js'

/**
 * Event types that can be stored in MongoDB
 */
export type StorableEventType = 
    | 'connection.update'
    | 'messaging-history.set'
    | 'contacts.upsert'
    | 'contacts.update'
    | 'chats.upsert'
    | 'chats.update'
    | 'chats.delete'
    | 'labels.edit'
    | 'labels.association'
    | 'presence.update'
    | 'messages.upsert'
    | 'messages.update'
    | 'messages.delete'
    | 'groups.update'
    | 'groups.upsert'
    | 'group-participants.update'
    | 'message-receipt.update'
    | 'messages.reaction'
    | 'lid-mapping.update'  // Baileys v7+ LID mapping update event

/**
 * Configuration for individual event storage
 */
export interface EventStorageConfig {
    /**
     * Whether to store this event type (default: true)
     */
    enabled?: boolean
    
    /**
     * TTL (Time To Live) in days for this event type
     * If not specified, uses the global ttlDays or defaults
     */
    ttlDays?: number
    
    /**
     * Whether to use batch processing for this event type
     * Applicable for messages and label associations
     */
    useBatch?: boolean
    
    /**
     * Custom filter function to determine if an event should be stored
     * Return true to store, false to skip
     */
    filter?: (data: any) => boolean
    
    /**
     * Transform function to modify data before storing
     */
    transform?: (data: any) => any
}

/**
 * Configuration for event storage with per-event settings
 */
export interface EventsConfig {
    /**
     * Configuration for each event type
     * If an event is not specified here, it uses default settings
     */
    [key: string]: EventStorageConfig | undefined
    
    // Predefined event configurations
    'connection.update'?: EventStorageConfig
    'messaging-history.set'?: EventStorageConfig
    'contacts.upsert'?: EventStorageConfig
    'contacts.update'?: EventStorageConfig
    'chats.upsert'?: EventStorageConfig
    'chats.update'?: EventStorageConfig
    'chats.delete'?: EventStorageConfig
    'labels.edit'?: EventStorageConfig
    'labels.association'?: EventStorageConfig
    'presence.update'?: EventStorageConfig
    'messages.upsert'?: EventStorageConfig
    'messages.update'?: EventStorageConfig
    'messages.delete'?: EventStorageConfig
    'groups.update'?: EventStorageConfig
    'groups.upsert'?: EventStorageConfig
    'group-participants.update'?: EventStorageConfig
    'message-receipt.update'?: EventStorageConfig
    'messages.reaction'?: EventStorageConfig
    'lid-mapping.update'?: EventStorageConfig  // Baileys v7+ LID mapping update event
}

/**
 * Collection-specific TTL configuration
 */
export interface CollectionTTLConfig {
    chats?: number
    contacts?: number
    messages?: number
    groupMetadata?: number
    state?: number
    presences?: number
    labels?: number
    labelAssociations?: number
}

export interface RedisConfig {
    /**
     * Redis connection options for Bull queue
     * Can be a connection string or RedisOptions object
     */
    connection: string | RedisOptions | any
    
    /**
     * Optional queue name prefix (default: 'baileys')
     */
    queuePrefix?: string
    
    /**
     * Enable Bull queue for label associations (default: true if Redis config provided)
     */
    enableLabelQueue?: boolean
    
    /**
     * Enable Bull queue for messages (default: false)
     */
    enableMessageQueue?: boolean
    
    /**
     * Max jobs to process concurrently (default: 50)
     */
    concurrency?: number
    
    /**
     * Remove completed jobs after this many seconds (default: 3600)
     */
    removeOnComplete?: number
    
    /**
     * Remove failed jobs after this many seconds (default: 86400)
     */
    removeOnFail?: number
    
    /**
     * Use shared queues across all instances
     * Default: true (recommended for multi-instance setups)
     */
    useSharedQueues?: boolean
    
    /**
     * Concurrency settings for shared queues
     * Only applies when useSharedQueues is true
     */
    queueConcurrency?: {
        highPriority?: number
        dataSync?: number
        media?: number
        lowPriority?: number
    }
}

/**
 * Configuration for profile picture auto-retrieval
 */
export interface ProfilePictureConfig {
    /**
     * Enable automatic profile picture retrieval
     * Default: false
     */
    enabled?: boolean
    
    /**
     * Days before refreshing profile pictures
     * Default: 7 days
     */
    refreshIntervalDays?: number
    
    /**
     * Delay between profile picture requests in milliseconds
     * Default: 500ms
     */
    requestDelay?: number
    
    /**
     * Maximum concurrent profile picture fetches
     * Default: 5
     */
    maxConcurrent?: number
    
    /**
     * Number of retry attempts for failed fetches
     * Default: 3
     */
    retryAttempts?: number
    
    /**
     * Whether to log privacy errors (when user has restricted profile picture)
     * Default: false
     */
    logPrivacyErrors?: boolean
}

export interface EnhancedMongoDBStoreConfig {
    /**
     * MongoDB connection URI
     */
    uri: string
    
    /**
     * Database name
     */
    database: string
    
    /**
     * Instance ID to support multiple WhatsApp instances
     */
    instanceId: string
    
    /**
     * Global TTL (Time To Live) in days for automatic data expiration
     * Can be overridden per collection or per event
     * Default: 30 days
     */
    ttlDays?: number
    
    /**
     * Per-collection TTL configuration
     * Overrides global ttlDays for specific collections
     */
    collectionTTL?: CollectionTTLConfig
    
    /**
     * Event storage configuration
     * Defines which events to store and their individual settings
     */
    events?: EventsConfig
    
    /**
     * Whether to store all events by default
     * If false, only events explicitly enabled in 'events' config will be stored
     * Default: true
     */
    storeAllByDefault?: boolean
    
    /**
     * Optional logger instance
     */
    logger?: Logger
    
    /**
     * Optional chat key comparator
     */
    chatKey?: Comparable<Chat, string>
    
    /**
     * Optional label association key comparator
     */
    labelAssociationKey?: Comparable<LabelAssociation, string>
    
    /**
     * Collection name prefix (default: 'baileys_')
     */
    collectionPrefix?: string
    
    /**
     * Optional Redis configuration for Bull queue
     * If provided, will use Bull for robust queue handling
     */
    redis?: RedisConfig
    
    /**
     * Log level for debugging purposes
     * - 'none': No logging (default)
     * - 'error': Only error messages
     * - 'warn': Warning and error messages
     * - 'all': All messages including info/debug
     * Default: 'none'
     */
    logLevel?: 'none' | 'error' | 'warn' | 'all'
    
    /**
     * Optional authentication configuration
     * If provided, will enable authentication and access control
     */
    auth?: AuthConfig
    
    /**
     * Optional memory management configuration
     * Controls batch processing memory limits and backpressure
     */
    memory?: MemoryConfig
    
    /**
     * Optional TTL monitoring configuration
     * Enables verification and monitoring of TTL indexes
     */
    ttlMonitoring?: Omit<TTLConfig, 'days'>
    
    /**
     * Bot's WhatsApp JID for poll vote decryption
     * Required for decrypting poll votes where the bot is the poll creator
     */
    meId?: string
    
    /**
     * Enable performance monitoring
     * Tracks metrics for each event type
     */
    enableMetrics?: boolean
    
    /**
     * Custom hooks for pre/post processing
     */
    hooks?: {
        /**
         * Called before storing any event
         * Return false to skip storing
         */
        beforeStore?: (eventType: string, data: any) => boolean | Promise<boolean>
        
        /**
         * Called after successfully storing an event
         */
        afterStore?: (eventType: string, data: any) => void | Promise<void>
        
        /**
         * Called when an error occurs during storage
         */
        onError?: (eventType: string, error: Error, data: any) => void
        
        /**
         * Called when a label operation (add/remove) is tracked
         * Useful for triggering automation workflows
         */
        onLabelOperation?: (instanceId: string, operation: 'add' | 'remove', chatId: string, labelId: string) => void | Promise<void>
    }
    
    /**
     * Media download configuration
     * Enables automatic download and storage of media files
     */
    media?: MediaConfig
    
    /**
     * Optional LID (LinkedIn ID) handler configuration
     * Enables automatic @lid to phone number mapping
     */
    lidHandler?: LidHandlerConfig
    
    /**
     * Optional connection configuration for tiered pooling
     * Controls how connections are shared and managed
     */
    connectionConfig?: ConnectionConfig

    /**
     * Optional shared ConnectionManager configuration
     * Allows tailoring pool sizes and global connection limits
     */
    connectionManager?: ConnectionManagerConfig
    
    /**
     * Whether to use shared connections via ConnectionManager
     * Default: true (recommended for multiple instances)
     */
    useSharedConnections?: boolean
    
    /**
     * WhatsApp socket instance for profile picture retrieval
     * Required if profilePictureConfig is enabled
     */
    sock?: WASocket
    
    /**
     * Profile picture auto-retrieval configuration
     * Enables automatic download of contact profile pictures
     */
    profilePictureConfig?: ProfilePictureConfig
    
    /**
     * Whether to clear all data when receiving isLatest=true in messaging-history.set
     * Default: false (for backward compatibility)
     * Set to true if you want to clear all existing data when receiving the latest history
     */
    clearAllOnHistorySync?: boolean
    
    /**
     * Timeout for clearAll operation (in milliseconds)
     * Default: 10000 (10 seconds)
     * Maximum time to wait for pending operations before proceeding with clearAll
     */
    clearAllTimeout?: number
    
    /**
     * Whether to debounce messaging-history.set events
     * Default: true
     * Helps prevent crashes from rapid successive events
     */
    debounceHistoryEvents?: boolean
    
    /**
     * Delay in milliseconds for debouncing history events
     * Default: 500
     * Time to wait for additional events before processing
     */
    debounceDelay?: number
    
    /**
     * Whether to wait for pending operations before clearAll
     * Default: true
     * Ensures all operations complete before clearing data
     */
    waitForPendingOps?: boolean
    
    /**
     * Maximum time to wait for pending operations (in milliseconds)
     * Default: 10000 (10 seconds)
     * Timeout for waiting on pending operations
     */
    maxPendingOpsWait?: number
    
    /**
     * Interval for cleaning up stale pending operations (in milliseconds)
     * Default: 120000 (2 minutes)
     * How often to check for and clean up stale operations
     */
    staleOperationCleanupInterval?: number
    
    /**
     * Threshold for considering an operation stale (in milliseconds)
     * Default: 300000 (5 minutes)
     * Operations older than this are considered stale
     */
    staleOperationThreshold?: number
    
    /**
     * Index management configuration for smart index creation
     * Controls how indexes are created and managed
     */
    indexManagement?: {
        /**
         * Skip index creation for collections that already exist
         * When true, only creates indexes for new collections or missing indexes
         * Default: true (recommended for performance)
         */
        skipExistingCollectionIndexes?: boolean
        
        /**
         * Force recreation of all indexes, even if they exist
         * Overrides skipExistingCollectionIndexes when true
         * Default: false
         */
        forceRecreateIndexes?: boolean
        
        /**
         * Enable detailed logging for index creation process
         * Default: true
         */
        enableIndexHealthLogging?: boolean
        
        /**
         * Timeout for index creation operations (in milliseconds)
         * Default: 30000 (30 seconds)
         */
        indexCreationTimeout?: number
    }
    
    /**
     * LID handling configuration for Baileys v7+
     * Controls how LID-to-phone number mappings are managed
     */
    lidConfig?: {
        /**
         * Enable LID handling
         * Default: true
         */
        enabled?: boolean
        /**
         * Delay between LID lookup requests in milliseconds
         * Default: 500
         */
        requestDelay?: number
        /**
         * Number of retry attempts for failed lookups
         * Default: 3
         */
        retryAttempts?: number
        /**
         * Maximum concurrent LID lookups
         * Default: 10
         */
        maxConcurrentLookups?: number
        /**
         * TTL in seconds for negative cache entries (lookups with no result)
         * Default: 300
         */
        negativeCacheTTL?: number
        /**
         * Enable LID lookups (set to false to only use cached mappings)
         * Default: true
         */
        lookupsEnabled?: boolean
        /**
         * Prefer reverse lookup from messages before querying contacts
         * Default: true
         */
        preferReverseLookupFirst?: boolean
        /**
         * Maximum time in ms for contacts collection queries
         * Default: 500
         */
        contactsQueryMaxTimeMS?: number
        /**
         * Enable dynamic backoff for negative cache TTL
         * Default: true
         */
        dynamicNegativeBackoff?: boolean
        /**
         * Minimum negative cache TTL in seconds (when backoff enabled)
         * Default: 300
         */
        minNegativeCacheTTL?: number
        /**
         * Maximum negative cache TTL in seconds (when backoff enabled)
         * Default: 3600
         */
        maxNegativeCacheTTL?: number
        /**
         * Enable proactive LID resolution for historical messages
         * Default: false
         */
        proactiveHistoryResolution?: boolean
        /**
         * Sync MongoDB mappings to Baileys v7 native store on startup
         * Default: false
         */
        syncToNativeStoreOnInit?: boolean
        /**
         * Listen for lid-mapping.update events from Baileys v7
         * Default: true
         */
        handleLidMappingEvents?: boolean
    }
}

/**
 * Event metrics for monitoring
 */
export interface EventMetrics {
    eventType: string
    totalReceived: number
    totalStored: number
    totalSkipped: number
    totalErrors: number
    lastProcessedAt?: Date
    averageProcessingTime?: number
}

export interface LidResolutionMetrics {
    operationType: string
    totalResolved: number
    totalErrors: number
    lastProcessedAt?: Date
}

export interface EnhancedMongoDBStore {
    /**
     * Instance ID for this store
     */
    instanceId: string
    
    /**
     * Get event configuration for a specific event type
     */
    getEventConfig(eventType: string): EventStorageConfig
    
    /**
     * Update event configuration at runtime
     */
    updateEventConfig(eventType: string, config: EventStorageConfig): void
    
    /**
     * Get metrics for all or specific event types
     */
    getEventMetrics(eventType?: string): EventMetrics | EventMetrics[]
    
    /**
     * Reset metrics for specific or all event types
     */
    resetEventMetrics(eventType?: string): void

    /**
     * Get LID resolution metrics for specific operation type or all operation types
     */
    getLidResolutionMetrics(operationType?: string): LidResolutionMetrics | LidResolutionMetrics[]

    /**
     * Reset LID resolution metrics for specific operation type or all operation types
     */
    resetLidResolutionMetrics(operationType?: string): void
    
    // All existing MongoDBStore methods...
    getChats(): Promise<Chat[]>
    getChat(jid: string): Promise<Chat | null>
    upsertChats(...chats: Chat[]): Promise<void>
    updateChat(jid: string, update: Partial<Chat>): Promise<boolean>
    deleteChats(jids: string[]): Promise<void>
    getContacts(): Promise<{ [id: string]: Contact }>
    getContact(jid: string): Promise<Contact | null>
    upsertContacts(contacts: Contact[]): Promise<void>
    getMessages(jid: string): Promise<proto.IWebMessageInfo[]>
    getMessage(jid: string, id: string): Promise<proto.IWebMessageInfo | null>
    upsertMessage(jid: string, message: proto.IWebMessageInfo, useBatch?: boolean): Promise<void>
    updateMessage(jid: string, id: string, update: Partial<proto.IWebMessageInfo>): Promise<boolean>
    deleteMessages(jid: string, ids?: string[]): Promise<void>
    getGroupMetadata(jid: string): Promise<GroupMetadata | null>
    getAllGroupMetadata(): Promise<GroupMetadata[]>
    upsertGroupMetadata(jid: string, metadata: GroupMetadata): Promise<void>
    getState(): Promise<ConnectionState>
    updateState(update: Partial<ConnectionState>): Promise<void>
    getPresences(): Promise<{ [id: string]: { [participant: string]: PresenceData } }>
    updatePresence(id: string, presences: { [participant: string]: PresenceData }): Promise<void>
    getLabels(): Promise<{ [id: string]: Label }>
    upsertLabel(id: string, label: Label): Promise<void>
    deleteLabel(id: string): Promise<void>
    getLabelAssociations(): Promise<LabelAssociation[]>
    getChatLabels(chatId: string): Promise<LabelAssociation[]>
    getMessageLabels(messageId: string): Promise<string[]>
    upsertLabelAssociation(association: LabelAssociation): Promise<void>
    deleteLabelAssociation(association: LabelAssociation): Promise<void>
    bind(ev: BaileysEventEmitter): void
    unbind(): void
    setSock(socket: any): void
    isHealthy(): Promise<boolean>
    rebind(ev: BaileysEventEmitter): void
    reconnect(): Promise<void>
    loadMessages(jid: string, count: number, cursor: WAMessageCursor): Promise<proto.IWebMessageInfo[]>
    loadMessage(jid: string, id: string): Promise<proto.IWebMessageInfo | undefined>
    mostRecentMessage(jid: string): Promise<proto.IWebMessageInfo | undefined>
    clearAll(options?: { preserve?: Array<'chats' | 'contacts' | 'messages' | 'presences'> }): Promise<void>
    getPerformanceStats(): {
        messagesProcessed: number
        labelsProcessed: number
        batchesProcessed: number
        errors: number
        lastResetTime: Date
        uptime: number
        labelStats?: {
            totalReceived: number
            totalProcessed: number
            currentQueueSize: number
            isProcessing: boolean
        }
        eventMetrics?: { [key: string]: EventMetrics }
    }
    flushLabelAssociations(): Promise<void>
    resetPerformanceStats(): void
    recreateIndexes(): Promise<{ created: number; failed: number; details: string[] }>
    getIndexStatus(): Promise<{ collection: string; indexes: any[] }[]>
    getTTLStatus(): Promise<{
        enabled: boolean
        ttlDays?: number
        collectionTTL?: CollectionTTLConfig
        summary?: {
            totalCollections: number
            collectionsWithTTL: number
            collectionsWithExpiredDocs: number
            totalExpiredDocuments: number
        }
        details?: any[]
        metrics?: any
        error?: string
        message?: string
    }>
    
    /**
     * Clean up old media files for this instance
     * @param daysToKeep Number of days to keep media files (default: 30)
     */
    cleanupOldMedia(daysToKeep?: number): Promise<{ deleted: number; errors: number }>
    
    /**
     * Get connection metrics for this instance
     */
    getConnectionMetrics(): Promise<any>
    
    /**
     * Get media statistics for this instance
     */
    getMediaStats(): Promise<{
        totalFiles: number
        totalSize: number
        byType: Record<string, { count: number; size: number }>
    }>
    
    /**
     * Download media for a specific message
     * @param jid Chat JID
     * @param messageId Message ID
     */
    downloadMessageMedia(jid: string, messageId: string): Promise<{
        success: boolean
        localPath?: string
        error?: string
    }>
    
    /**
     * Clean up MongoDB data and optionally close connections for this instance
     * @param deleteData Whether to delete all data for the instance (default: false)
     */
    cleanup(deleteData?: boolean): Promise<void>

    /**
     * Proactively resolve LID jids in historical messages to phone numbers
     * This runs once per instance during initial history sync
     */
    performProactiveLidResolutionForHistory(): Promise<void>

    close(): Promise<void>
}
