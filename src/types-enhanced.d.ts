import type { Comparable } from '@adiwajshing/keyed-db/lib/Types'
import type { Logger } from 'pino'
import type { proto } from 'baileys'
import type { BaileysEventEmitter, Chat, ConnectionState, Contact, GroupMetadata, PresenceData, WAMessageCursor } from 'baileys'
import type { Label } from 'baileys/lib/Types/Label'
import type { LabelAssociation } from 'baileys/lib/Types/LabelAssociation'
import type { RedisOptions } from 'ioredis'
import type { AuthConfig } from './utils/auth'
import type { MemoryConfig } from './utils/memory'
import type { TTLConfig } from './utils/ttl'

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
    connection: string | RedisOptions
    
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
    loadMessages(jid: string, count: number, cursor: WAMessageCursor): Promise<proto.IWebMessageInfo[]>
    loadMessage(jid: string, id: string): Promise<proto.IWebMessageInfo | undefined>
    mostRecentMessage(jid: string): Promise<proto.IWebMessageInfo | undefined>
    clearAll(): Promise<void>
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
    close(): Promise<void>
}