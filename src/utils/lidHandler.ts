import { Collection, Db } from 'mongodb'
import NodeCache from 'node-cache'
import { proto } from 'baileys'
import { 
    normalizeJidForStorage,
    isLidFormat as isLidFormatUtil,
    isPhoneNumberFormat,
    areJidsEquivalent,
    extractLidPhonePair
} from './jidUtils'

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
    private messagesCollection: Collection<any> | null = null
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
        const collectionName = collectionPrefix ? `${collectionPrefix}lidMappings` : 'lidMappings'
        this.lidMappingsCollection = db.collection<LidMapping>(collectionName)
        
        // Also get reference to messages collection for reverse lookups
        const messagesCollectionName = collectionPrefix ? `${collectionPrefix}messages` : 'messages'
        this.messagesCollection = db.collection(messagesCollectionName)
        
        
        // Create indexes for efficient lookups
        await this.createIndexes()
    }

    /**
     * Create MongoDB indexes for efficient lookups
     */
    private async createIndexes(): Promise<void> {
        const promises: Promise<any>[] = []
        
        // Create indexes for lidMappings collection
        if (this.lidMappingsCollection) {
            // Drop existing TTL index if it exists (migration from older versions)
            promises.push(
                this.lidMappingsCollection.dropIndex('lastSeen_1').catch(() => {
                    // Index might not exist, ignore error
                })
            )
            
            promises.push(
                // Compound index for instance + lid lookup
                this.lidMappingsCollection.createIndex(
                    { instanceId: 1, lid: 1 },
                    { unique: true }
                ),
                // Compound index for instance + phone number lookup
                this.lidMappingsCollection.createIndex(
                    { instanceId: 1, phoneNumber: 1 }
                )
                // TTL index removed - lid mappings will persist until explicitly deleted
            )
        }
        
        // Create indexes for messages collection (for reverse lookup)
        if (this.messagesCollection) {
            promises.push(
                // Compound index for reverse lookup: find messages by senderLid
                this.messagesCollection.createIndex(
                    { instanceId: 1, 'key.fromMe': 1, 'key.senderLid': 1 },
                    { background: true }
                )
            )
        }
        
        try {
            if (promises.length > 0) {
                await Promise.all(promises)
                console.log('[LidHandler] Indexes created successfully')
            }
        } catch (error) {
            console.error('Failed to create LID handler indexes:', error)
        }
    }

    /**
     * Check if a JID is in @lid format (handles :XX suffixes)
     */
    isLidFormat(jid: string | undefined | null): jid is string {
        return isLidFormatUtil(jid)
    }

    /**
     * Extract LID and phone number from a message
     */
    extractLidInfo(message: proto.IWebMessageInfo): {
        lid?: string
        phoneNumber?: string
        needsReverseLookup?: boolean
        debug?: string[]
    } {
        const result: { lid?: string; phoneNumber?: string; needsReverseLookup?: boolean; debug?: string[] } = {}
        const debug: string[] = []
        
        // Check if this is a sent message (fromMe: true) with LID remoteJid
        if (message.key?.fromMe && this.isLidFormat(message.key?.remoteJid)) {
            result.lid = message.key.remoteJid!
            result.needsReverseLookup = true
            debug.push(`Found LID in remoteJid (fromMe=true, needs reverse lookup): ${result.lid}`)
            
            // For sent messages, we won't find phone number in the message itself
            // We need to do a reverse lookup
            if (process.env.NODE_ENV !== 'production' || debug.length > 0) {
                result.debug = debug
            }
            return result
        }
        
        // Check remoteJid for @lid
        if (this.isLidFormat(message.key?.remoteJid)) {
            result.lid = message.key!.remoteJid!
            debug.push(`Found LID in remoteJid: ${result.lid}`)
        }
        
        // Check senderLid (some messages have this)
        if (this.isLidFormat((message.key as any)?.senderLid)) {
            result.lid = (message.key as any).senderLid
            debug.push(`Found LID in senderLid: ${result.lid}`)
        }
        
        // Extract phone number from senderPn
        // Special handling: For fromMe messages with LID remoteJid, senderPn might incorrectly be a LID too
        // In this case, we should ignore senderPn and rely on reverse lookup
        if ((message.key as any)?.senderPn) {
            const senderPn = (message.key as any).senderPn
            if (!this.isLidFormat(senderPn)) {
                result.phoneNumber = senderPn
                debug.push(`Found phone in senderPn: ${result.phoneNumber}`)
            } else if (message.key?.fromMe && this.isLidFormat(message.key?.remoteJid)) {
                // For fromMe messages to LID, if senderPn is also LID, ignore it
                // This is likely a WhatsApp bug where both fields are set to the recipient's LID
                debug.push(`Ignoring LID senderPn in fromMe message: ${senderPn}`)
            } else {
                debug.push(`senderPn is LID format: ${senderPn}`)
            }
        }
        
        // If remoteJid is not @lid, it might be the phone number
        if (message.key?.remoteJid && !this.isLidFormat(message.key.remoteJid)) {
            result.phoneNumber = message.key.remoteJid
            debug.push(`Found phone in remoteJid: ${result.phoneNumber}`)
        }
        
        // Check other potential fields for phone numbers
        const potentialPhoneFields = [
            (message.key as any)?.participant,
            (message as any)?.participant,
            (message as any)?.senderKeyDistributionMessage?.groupId
        ]
        
        for (const field of potentialPhoneFields) {
            if (field && !this.isLidFormat(field) && field.includes('@s.whatsapp.net')) {
                if (!result.phoneNumber) {
                    result.phoneNumber = field
                    debug.push(`Found phone in alternative field: ${field}`)
                }
            }
        }
        
        if (process.env.NODE_ENV !== 'production' || debug.length > 0) {
            result.debug = debug
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
        
        // Normalize JIDs for storage
        const normalizedLid = normalizeJidForStorage(lid)
        const normalizedPhone = normalizeJidForStorage(phoneNumber)
        
        // Validate inputs
        if (!normalizedLid || !normalizedPhone) return
        if (!this.isLidFormat(normalizedLid)) return
        if (!isPhoneNumberFormat(normalizedPhone)) return
        
        // Prevent storing LID->LID mappings (this is invalid)
        if (normalizedLid === normalizedPhone) {
            console.warn(`[LidHandler] Attempted to store invalid LID->LID mapping: ${normalizedLid} -> ${normalizedPhone}`)
            return
        }
        
        // Additional check: both shouldn't be LID format
        if (this.isLidFormat(normalizedPhone)) {
            console.warn(`[LidHandler] Attempted to store LID as phone number: ${normalizedLid} -> ${normalizedPhone}`)
            return
        }
        
        const now = new Date()
        
        try {
            // Upsert the mapping
            await this.lidMappingsCollection.updateOne(
                { 
                    instanceId: this.instanceId, 
                    lid: normalizedLid 
                },
                {
                    $set: {
                        phoneNumber: normalizedPhone,
                        lastSeen: now,
                        updatedAt: now
                    },
                    $setOnInsert: {
                        instanceId: this.instanceId,
                        lid: normalizedLid,
                        firstSeen: now
                    }
                },
                { upsert: true }
            )
            
            // Update cache if enabled (use normalized JIDs for cache keys)
            if (this.config.enableCache) {
                // Cache both directions
                this.cache.set(`lid:${this.instanceId}:${normalizedLid}`, normalizedPhone)
                this.cache.set(`phone:${this.instanceId}:${normalizedPhone}`, normalizedLid)
            }
            
            console.log(`[LidHandler] Stored mapping: ${normalizedLid} -> ${normalizedPhone}`)
        } catch (error) {
            console.error('[LidHandler] Failed to store LID mapping:', error)
        }
    }

    /**
     * Get phone number from @lid
     */
    async getPhoneNumberFromLid(lid: string): Promise<string | null> {
        const normalizedLid = normalizeJidForStorage(lid)
        if (!this.isLidFormat(normalizedLid)) return lid
        
        // Check cache first (use normalized JID for cache key)
        if (this.config.enableCache) {
            const cached = this.cache.get<string>(`lid:${this.instanceId}:${normalizedLid}`)
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
                lid: normalizedLid
            })
            
            if (mapping) {
                // Update cache
                if (this.config.enableCache) {
                    this.cache.set(`lid:${this.instanceId}:${normalizedLid}`, mapping.phoneNumber)
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
        const normalizedPhone = normalizeJidForStorage(phoneNumber)
        if (this.isLidFormat(normalizedPhone)) return normalizedPhone
        
        // Check cache first
        if (this.config.enableCache) {
            const cached = this.cache.get<string>(`phone:${this.instanceId}:${normalizedPhone}`)
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
                phoneNumber: normalizedPhone
            })
            
            if (mapping) {
                // Update cache
                if (this.config.enableCache) {
                    this.cache.set(`phone:${this.instanceId}:${normalizedPhone}`, mapping.lid)
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
     * Store a discovered LID-phone mapping from JID mismatch
     * This is called when we find a message with mismatched JIDs that form a LID-phone pair
     */
    async storeDiscoveredMapping(jid1: string, jid2: string): Promise<boolean> {
        console.log(`[LidHandler] storeDiscoveredMapping called with: jid1="${jid1}", jid2="${jid2}"`)
        
        const pair = extractLidPhonePair(jid1, jid2)
        if (!pair) {
            console.log(`[LidHandler] Failed to extract LID-phone pair from: "${jid1}" and "${jid2}"`)
            
            // Additional debugging to understand why it failed
            if (process.env.DEBUG_LID === 'true') {
                console.log(`[LidHandler] Debug info:`)
                console.log(`  - jid1 isLid: ${this.isLidFormat(jid1)}, isPhone: ${isPhoneNumberFormat(jid1)}`)
                console.log(`  - jid2 isLid: ${this.isLidFormat(jid2)}, isPhone: ${isPhoneNumberFormat(jid2)}`)
                console.log(`  - areJidsEquivalent: ${areJidsEquivalent(jid1, jid2)}`)
            }
            return false
        }
        
        console.log(`[LidHandler] Successfully extracted mapping: ${pair.lid} <-> ${pair.phoneNumber}`)
        await this.storeLidMapping(pair.lid, pair.phoneNumber)
        return true
    }

    /**
     * Discover phone number from sent messages (fromMe: true) with LID remoteJid
     * Searches for received messages where senderLid matches the LID
     */
    async discoverPhoneFromSentMessage(lid: string): Promise<string | null> {
        if (!this.messagesCollection) {
            console.warn('[LidHandler] Messages collection not available for reverse lookup')
            return null
        }
        
        const normalizedLid = normalizeJidForStorage(lid)
        console.log(`[LidHandler] Searching for phone number from sent message with LID: ${normalizedLid}`)
        
        try {
            // Search for messages where:
            // 1. fromMe is false (received messages)
            // 2. senderLid matches our LID
            const receivedMessage = await this.messagesCollection.findOne({
                instanceId: this.instanceId,
                'key.fromMe': false,
                $or: [
                    { 'key.senderLid': normalizedLid },
                    { 'key.senderLid': lid } // Try original format too
                ]
            })
            
            if (receivedMessage) {
                // Extract phone number from senderPn or remoteJid
                let phoneNumber: string | null = null
                
                // First try senderPn
                if (receivedMessage.key?.senderPn && !this.isLidFormat(receivedMessage.key.senderPn)) {
                    phoneNumber = receivedMessage.key.senderPn
                    console.log(`[LidHandler] Found phone number in senderPn: ${phoneNumber}`)
                }
                // Then try remoteJid if it's not a LID
                else if (receivedMessage.key?.remoteJid && !this.isLidFormat(receivedMessage.key.remoteJid)) {
                    phoneNumber = receivedMessage.key.remoteJid
                    console.log(`[LidHandler] Found phone number in remoteJid: ${phoneNumber}`)
                }
                // Also check participant field
                else if (receivedMessage.key?.participant && !this.isLidFormat(receivedMessage.key.participant)) {
                    phoneNumber = receivedMessage.key.participant
                    console.log(`[LidHandler] Found phone number in participant: ${phoneNumber}`)
                }
                
                if (phoneNumber && isPhoneNumberFormat(phoneNumber)) {
                    // Store the discovered mapping
                    console.log(`[LidHandler] Discovered phone number ${phoneNumber} for LID ${normalizedLid} via reverse lookup`)
                    await this.storeLidMapping(normalizedLid, phoneNumber)
                    return phoneNumber
                }
            } else {
                console.log(`[LidHandler] No received messages found with senderLid: ${normalizedLid}`)
            }
        } catch (error) {
            console.error('[LidHandler] Error during reverse lookup:', error)
        }
        
        return null
    }

    /**
     * Check if two JIDs are equivalent (same JID with different formats)
     */
    areJidsEquivalent(jid1: string, jid2: string): boolean {
        return areJidsEquivalent(jid1, jid2)
    }

    /**
     * Reverse lookup phone number from messages for a given LID
     * Used when fromMe=true messages have LID in remoteJid
     */
    async reversePhoneLookupFromMessages(lid: string): Promise<string | null> {
        if (!this.messagesCollection) {
            console.warn('[LidHandler] Messages collection not available for reverse lookup')
            return null
        }
        
        const normalizedLid = normalizeJidForStorage(lid)
        console.log(`[LidHandler] Attempting reverse lookup for LID: ${normalizedLid}`)
        
        try {
            // Look for messages where this LID appears with a phone number
            const message = await this.messagesCollection.findOne({
                instanceId: this.instanceId,
                $or: [
                    // Case 1: LID in senderLid with phone in senderPn
                    { 
                        'key.senderLid': normalizedLid,
                        'key.senderPn': { $exists: true, $not: { $regex: '@lid$' } }
                    },
                    // Case 2: LID in remoteJid with phone in senderPn (fromMe=false)
                    {
                        'key.remoteJid': normalizedLid,
                        'key.fromMe': false,
                        'key.senderPn': { $exists: true, $not: { $regex: '@lid$' } }
                    }
                ]
            })
            
            if (message?.key?.senderPn && !this.isLidFormat(message.key.senderPn)) {
                console.log(`[LidHandler] Reverse lookup found: ${normalizedLid} -> ${message.key.senderPn}`)
                return message.key.senderPn
            }
        } catch (error) {
            console.error('[LidHandler] Error during reverse lookup:', error)
        }
        
        return null
    }

    /**
     * Update existing messages that have a LID to use the phone number
     */
    async updateExistingMessages(lid: string, phoneNumber: string): Promise<void> {
        if (!this.messagesCollection) {
            console.warn('[LidHandler] Messages collection not available for updates')
            return
        }
        
        try {
            // Update messages where remoteJid is the LID
            const result = await this.messagesCollection.updateMany(
                {
                    instanceId: this.instanceId,
                    'key.remoteJid': lid
                },
                {
                    $set: {
                        'key.remoteJid': phoneNumber,
                        jid: phoneNumber,
                        'lidMapping.resolved': true,
                        'lidMapping.resolvedAt': new Date()
                    }
                }
            )
            
            if (result.modifiedCount > 0) {
                console.log(`[LidHandler] Updated ${result.modifiedCount} messages from LID ${lid} to ${phoneNumber}`)
            }
        } catch (error) {
            console.error('[LidHandler] Error updating existing messages:', error)
        }
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
            needsReverseLookup?: boolean
            debug?: string[]
        }
    }> {
        const lidInfo = this.extractLidInfo(message)
        let normalizedJid = message.key?.remoteJid || ''
        let mappingStored = false
        
        console.log(`[LidHandler] Processing message ${message.key?.id}: ${JSON.stringify(lidInfo)}`)
        
        // Handle fromMe messages with LID that need reverse lookup
        if (lidInfo.needsReverseLookup && lidInfo.lid) {
            console.log(`[LidHandler] Performing reverse lookup for sent message with LID: ${lidInfo.lid}`)
            const discoveredPhone = await this.discoverPhoneFromSentMessage(lidInfo.lid)
            
            if (discoveredPhone) {
                console.log(`[LidHandler] Reverse lookup successful: ${lidInfo.lid} -> ${discoveredPhone}`)
                lidInfo.phoneNumber = discoveredPhone
                normalizedJid = discoveredPhone
                mappingStored = true // Mapping was stored during discovery
            } else {
                // Try existing mapping as fallback
                const existingPhone = await this.getPhoneNumberFromLid(lidInfo.lid)
                if (existingPhone) {
                    console.log(`[LidHandler] Using existing mapping: ${lidInfo.lid} -> ${existingPhone}`)
                    normalizedJid = existingPhone
                    lidInfo.phoneNumber = existingPhone
                } else {
                    console.log(`[LidHandler] No phone number found for sent message LID: ${lidInfo.lid}`)
                    // IMPORTANT: Keep the LID for now, but mark it for future update
                    // The phone number might become available later through getMessage discovery
                    normalizedJid = lidInfo.lid
                    console.log(`[LidHandler] Using LID as temporary JID, will update when phone number is discovered`)
                }
            }
        }
        // If we have both LID and phone number, store the mapping
        else if (lidInfo.lid && lidInfo.phoneNumber) {
            // Additional validation: Don't store if both are the same or both are LIDs
            if (lidInfo.lid === lidInfo.phoneNumber) {
                console.warn(`[LidHandler] Skipping invalid mapping where LID equals phone number: ${lidInfo.lid}`)
            } else if (this.isLidFormat(lidInfo.phoneNumber)) {
                console.warn(`[LidHandler] Skipping invalid mapping where phone number is also a LID: ${lidInfo.lid} -> ${lidInfo.phoneNumber}`)
            } else {
                console.log(`[LidHandler] Storing mapping: ${lidInfo.lid} -> ${lidInfo.phoneNumber}`)
                await this.storeLidMapping(lidInfo.lid, lidInfo.phoneNumber)
                mappingStored = true
                normalizedJid = lidInfo.phoneNumber
            }
        } 
        // If we only have LID, try to get phone number from database
        else if (lidInfo.lid) {
            console.log(`[LidHandler] Looking up existing mapping for LID: ${lidInfo.lid}`)
            const phoneNumber = await this.getPhoneNumberFromLid(lidInfo.lid)
            if (phoneNumber) {
                console.log(`[LidHandler] Found existing mapping: ${lidInfo.lid} -> ${phoneNumber}`)
                normalizedJid = phoneNumber
                lidInfo.phoneNumber = phoneNumber
            } else {
                console.log(`[LidHandler] No existing mapping found for LID: ${lidInfo.lid}`)
                // Keep the LID for now, might be resolved later
                normalizedJid = lidInfo.lid
            }
        } else {
            console.log(`[LidHandler] No LID found in message, using original JID: ${normalizedJid}`)
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