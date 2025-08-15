import { Collection, Db } from 'mongodb'
import NodeCache from 'node-cache'
import { proto } from 'baileys'

export interface LidMapping {
    instanceId: string
    lid: string
    phoneNumber: string
    firstSeen: Date
    lastSeen: Date
    updatedAt: Date
}

export interface LidHandlerConfig {
    cacheTTL?: number // Cache TTL in seconds (default: 3600)
    enableCache?: boolean // Enable caching (default: true)
}

export class LidHandler {
    private lidMappingsCollection: Collection<LidMapping> | null = null
    private cache: NodeCache
    private config: Required<LidHandlerConfig>
    private instanceId: string

    constructor(instanceId: string, config?: LidHandlerConfig) {
        this.instanceId = instanceId
        this.config = {
            cacheTTL: config?.cacheTTL ?? 3600,
            enableCache: config?.enableCache ?? true
        }
        
        // Initialize cache with configured TTL
        this.cache = new NodeCache({ 
            stdTTL: this.config.cacheTTL,
            checkperiod: Math.floor(this.config.cacheTTL / 10),
            useClones: false
        })
    }

    /**
     * Initialize the handler with MongoDB connection
     */
    async initialize(db: Db, collectionPrefix = ''): Promise<void> {
        const collectionName = collectionPrefix ? `${collectionPrefix}_lidMappings` : 'lidMappings'
        this.lidMappingsCollection = db.collection<LidMapping>(collectionName)
        
        // Create indexes for efficient lookups
        await this.createIndexes()
    }

    /**
     * Create MongoDB indexes for efficient lookups
     */
    private async createIndexes(): Promise<void> {
        if (!this.lidMappingsCollection) return
        
        try {
            await Promise.all([
                // Compound index for instance + lid lookup
                this.lidMappingsCollection.createIndex(
                    { instanceId: 1, lid: 1 },
                    { unique: true }
                ),
                // Compound index for instance + phone number lookup
                this.lidMappingsCollection.createIndex(
                    { instanceId: 1, phoneNumber: 1 }
                ),
                // Index for TTL (auto-delete old mappings after 90 days)
                this.lidMappingsCollection.createIndex(
                    { lastSeen: 1 },
                    { expireAfterSeconds: 90 * 24 * 60 * 60 }
                )
            ])
        } catch (error) {
            console.error('Failed to create LID mapping indexes:', error)
        }
    }

    /**
     * Check if a JID is in @lid format
     */
    isLidFormat(jid: string | undefined | null): boolean {
        if (!jid) return false
        return jid.includes('@lid')
    }

    /**
     * Extract LID and phone number from a message
     */
    extractLidInfo(message: proto.IWebMessageInfo): {
        lid?: string
        phoneNumber?: string
    } {
        const result: { lid?: string; phoneNumber?: string } = {}
        
        // Check remoteJid for @lid
        if (this.isLidFormat(message.key?.remoteJid)) {
            result.lid = message.key!.remoteJid!
        }
        
        // Check senderLid (some messages have this)
        if (this.isLidFormat((message.key as any)?.senderLid)) {
            result.lid = (message.key as any).senderLid
        }
        
        // Extract phone number from senderPn
        if ((message.key as any)?.senderPn && !this.isLidFormat((message.key as any).senderPn)) {
            result.phoneNumber = (message.key as any).senderPn
        }
        
        // If remoteJid is not @lid, it might be the phone number
        if (message.key?.remoteJid && !this.isLidFormat(message.key.remoteJid)) {
            result.phoneNumber = message.key.remoteJid
        }
        
        return result
    }

    /**
     * Store or update a LID to phone number mapping
     */
    async storeLidMapping(lid: string, phoneNumber: string): Promise<void> {
        if (!this.lidMappingsCollection) {
            console.warn('LidHandler not initialized with database')
            return
        }
        
        // Validate inputs
        if (!lid || !phoneNumber) return
        if (!this.isLidFormat(lid)) return
        
        const now = new Date()
        
        try {
            // Upsert the mapping
            await this.lidMappingsCollection.updateOne(
                { 
                    instanceId: this.instanceId, 
                    lid 
                },
                {
                    $set: {
                        phoneNumber,
                        lastSeen: now,
                        updatedAt: now
                    },
                    $setOnInsert: {
                        instanceId: this.instanceId,
                        lid,
                        firstSeen: now
                    }
                },
                { upsert: true }
            )
            
            // Update cache if enabled
            if (this.config.enableCache) {
                // Cache both directions
                this.cache.set(`lid:${this.instanceId}:${lid}`, phoneNumber)
                this.cache.set(`phone:${this.instanceId}:${phoneNumber}`, lid)
            }
            
            console.log(`[LidHandler] Stored mapping: ${lid} -> ${phoneNumber}`)
        } catch (error) {
            console.error('[LidHandler] Failed to store LID mapping:', error)
        }
    }

    /**
     * Get phone number from @lid
     */
    async getPhoneNumberFromLid(lid: string): Promise<string | null> {
        if (!this.isLidFormat(lid)) return lid
        
        // Check cache first
        if (this.config.enableCache) {
            const cached = this.cache.get<string>(`lid:${this.instanceId}:${lid}`)
            if (cached) return cached
        }
        
        // Query database
        if (!this.lidMappingsCollection) {
            console.warn('LidHandler not initialized with database')
            return null
        }
        
        try {
            const mapping = await this.lidMappingsCollection.findOne({
                instanceId: this.instanceId,
                lid
            })
            
            if (mapping) {
                // Update cache
                if (this.config.enableCache) {
                    this.cache.set(`lid:${this.instanceId}:${lid}`, mapping.phoneNumber)
                }
                
                // Update lastSeen
                await this.lidMappingsCollection.updateOne(
                    { _id: mapping._id },
                    { $set: { lastSeen: new Date() } }
                )
                
                return mapping.phoneNumber
            }
        } catch (error) {
            console.error('[LidHandler] Failed to get phone number from LID:', error)
        }
        
        return null
    }

    /**
     * Get @lid from phone number (reverse lookup)
     */
    async getLidFromPhoneNumber(phoneNumber: string): Promise<string | null> {
        if (this.isLidFormat(phoneNumber)) return phoneNumber
        
        // Check cache first
        if (this.config.enableCache) {
            const cached = this.cache.get<string>(`phone:${this.instanceId}:${phoneNumber}`)
            if (cached) return cached
        }
        
        // Query database
        if (!this.lidMappingsCollection) {
            console.warn('LidHandler not initialized with database')
            return null
        }
        
        try {
            const mapping = await this.lidMappingsCollection.findOne({
                instanceId: this.instanceId,
                phoneNumber
            })
            
            if (mapping) {
                // Update cache
                if (this.config.enableCache) {
                    this.cache.set(`phone:${this.instanceId}:${phoneNumber}`, mapping.lid)
                }
                
                return mapping.lid
            }
        } catch (error) {
            console.error('[LidHandler] Failed to get LID from phone number:', error)
        }
        
        return null
    }

    /**
     * Normalize a JID from @lid to phone number if possible
     * Returns the original JID if no mapping is found
     */
    async normalizeJid(jid: string | undefined | null): Promise<string | null> {
        if (!jid) return null
        
        if (this.isLidFormat(jid)) {
            const phoneNumber = await this.getPhoneNumberFromLid(jid)
            return phoneNumber || jid
        }
        
        return jid
    }

    /**
     * Process a message and extract/store LID mappings
     * Returns the normalized JID (phone number) if available
     */
    async processMessage(message: proto.IWebMessageInfo): Promise<{
        normalizedJid: string
        lidInfo: {
            lid?: string
            phoneNumber?: string
            mappingStored?: boolean
        }
    }> {
        const lidInfo = this.extractLidInfo(message)
        let normalizedJid = message.key?.remoteJid || ''
        let mappingStored = false
        
        // If we have both LID and phone number, store the mapping
        if (lidInfo.lid && lidInfo.phoneNumber) {
            await this.storeLidMapping(lidInfo.lid, lidInfo.phoneNumber)
            mappingStored = true
            normalizedJid = lidInfo.phoneNumber
        } 
        // If we only have LID, try to get phone number from database
        else if (lidInfo.lid) {
            const phoneNumber = await this.getPhoneNumberFromLid(lidInfo.lid)
            if (phoneNumber) {
                normalizedJid = phoneNumber
            }
        }
        
        return {
            normalizedJid,
            lidInfo: {
                ...lidInfo,
                mappingStored
            }
        }
    }

    /**
     * Clear cache for this instance
     */
    clearCache(): void {
        if (this.config.enableCache) {
            const keys = this.cache.keys()
            keys.forEach(key => {
                if (key.includes(`:${this.instanceId}:`)) {
                    this.cache.del(key)
                }
            })
        }
    }

    /**
     * Get all mappings for this instance (for debugging/export)
     */
    async getAllMappings(): Promise<LidMapping[]> {
        if (!this.lidMappingsCollection) {
            console.warn('LidHandler not initialized with database')
            return []
        }
        
        try {
            return await this.lidMappingsCollection
                .find({ instanceId: this.instanceId })
                .toArray()
        } catch (error) {
            console.error('[LidHandler] Failed to get all mappings:', error)
            return []
        }
    }

    /**
     * Delete old mappings that haven't been seen in specified days
     */
    async cleanupOldMappings(daysOld: number = 90): Promise<number> {
        if (!this.lidMappingsCollection) {
            console.warn('LidHandler not initialized with database')
            return 0
        }
        
        const cutoffDate = new Date()
        cutoffDate.setDate(cutoffDate.getDate() - daysOld)
        
        try {
            const result = await this.lidMappingsCollection.deleteMany({
                instanceId: this.instanceId,
                lastSeen: { $lt: cutoffDate }
            })
            
            console.log(`[LidHandler] Cleaned up ${result.deletedCount} old mappings`)
            return result.deletedCount
        } catch (error) {
            console.error('[LidHandler] Failed to cleanup old mappings:', error)
            return 0
        }
    }
}