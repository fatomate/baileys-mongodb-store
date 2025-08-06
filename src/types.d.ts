import type { Comparable } from '@adiwajshing/keyed-db/lib/Types'
import type { Logger } from 'pino'
import type { proto } from 'baileys'
import type { BaileysEventEmitter, Chat, ConnectionState, Contact, GroupMetadata, PresenceData, WAMessageCursor } from 'baileys'
import type { Label } from 'baileys/lib/Types/Label'
import type { LabelAssociation } from 'baileys/lib/Types/LabelAssociation'
import type { RedisOptions } from 'ioredis'

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

export interface MongoDBStoreConfig {
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
     * TTL (Time To Live) in days for automatic data expiration
     * Default: 30 days
     */
    ttlDays?: number
    
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
}

export interface MongoDBStore {
    /**
     * Instance ID for this store
     */
    instanceId: string
    
    /**
     * Get all chats
     */
    getChats(): Promise<Chat[]>
    
    /**
     * Get a specific chat
     */
    getChat(jid: string): Promise<Chat | null>
    
    /**
     * Upsert chats
     */
    upsertChats(...chats: Chat[]): Promise<void>
    
    /**
     * Update a chat
     */
    updateChat(jid: string, update: Partial<Chat>): Promise<boolean>
    
    /**
     * Delete chats
     */
    deleteChats(jids: string[]): Promise<void>
    
    /**
     * Get all contacts
     */
    getContacts(): Promise<{ [id: string]: Contact }>
    
    /**
     * Get a specific contact
     */
    getContact(jid: string): Promise<Contact | null>
    
    /**
     * Upsert contacts
     */
    upsertContacts(contacts: Contact[]): Promise<void>
    
    /**
     * Get messages for a chat
     */
    getMessages(jid: string): Promise<proto.IWebMessageInfo[]>
    
    /**
     * Get a specific message
     */
    getMessage(jid: string, id: string): Promise<proto.IWebMessageInfo | null>
    
    /**
     * Upsert a message
     * @param useBatch - Whether to use batch processing (for bulk operations)
     */
    upsertMessage(jid: string, message: proto.IWebMessageInfo, useBatch?: boolean): Promise<void>
    
    /**
     * Update a message
     */
    updateMessage(jid: string, id: string, update: Partial<proto.IWebMessageInfo>): Promise<boolean>
    
    /**
     * Delete messages
     */
    deleteMessages(jid: string, ids?: string[]): Promise<void>
    
    /**
     * Get group metadata
     */
    getGroupMetadata(jid: string): Promise<GroupMetadata | null>
    
    /**
     * Upsert group metadata
     */
    upsertGroupMetadata(jid: string, metadata: GroupMetadata): Promise<void>
    
    /**
     * Get connection state
     */
    getState(): Promise<ConnectionState>
    
    /**
     * Update connection state
     */
    updateState(update: Partial<ConnectionState>): Promise<void>
    
    /**
     * Get presences
     */
    getPresences(): Promise<{ [id: string]: { [participant: string]: PresenceData } }>
    
    /**
     * Update presence
     */
    updatePresence(id: string, presences: { [participant: string]: PresenceData }): Promise<void>
    
    /**
     * Get all labels
     */
    getLabels(): Promise<{ [id: string]: Label }>
    
    /**
     * Upsert a label
     */
    upsertLabel(id: string, label: Label): Promise<void>
    
    /**
     * Delete a label
     */
    deleteLabel(id: string): Promise<void>
    
    /**
     * Get all label associations
     */
    getLabelAssociations(): Promise<LabelAssociation[]>
    
    /**
     * Get label associations for a chat
     */
    getChatLabels(chatId: string): Promise<LabelAssociation[]>
    
    /**
     * Get label associations for a message
     */
    getMessageLabels(messageId: string): Promise<string[]>
    
    /**
     * Upsert label association
     */
    upsertLabelAssociation(association: LabelAssociation): Promise<void>
    
    /**
     * Delete label association
     */
    deleteLabelAssociation(association: LabelAssociation): Promise<void>
    
    /**
     * Bind to Baileys event emitter
     */
    bind(ev: BaileysEventEmitter): void
    
    /**
     * Load messages from the store
     */
    loadMessages(jid: string, count: number, cursor: WAMessageCursor): Promise<proto.IWebMessageInfo[]>
    
    /**
     * Load a specific message
     */
    loadMessage(jid: string, id: string): Promise<proto.IWebMessageInfo | undefined>
    
    /**
     * Get the most recent message in a chat
     */
    mostRecentMessage(jid: string): Promise<proto.IWebMessageInfo | undefined>
    
    /**
     * Clear all data for this instance
     */
    clearAll(): Promise<void>
    
    /**
     * Get performance statistics
     */
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
    }
    
    /**
     * Force flush all pending label associations
     */
    flushLabelAssociations(): Promise<void>
    
    /**
     * Reset performance statistics
     */
    resetPerformanceStats(): void
    
    /**
     * Recreate all indexes (useful if initial creation failed)
     */
    recreateIndexes(): Promise<{ created: number; failed: number; details: string[] }>
    
    /**
     * Get current index status for all collections
     */
    getIndexStatus(): Promise<{ collection: string; indexes: any[] }[]>
    
    /**
     * Close the MongoDB connection
     */
    close(): Promise<void>
}