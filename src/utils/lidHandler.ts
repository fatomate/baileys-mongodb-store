import { Collection, Db } from 'mongodb'
import Redis from 'ioredis'
import NodeCache from 'node-cache'
import { proto } from 'baileys'
import { 
    normalizeJidForStorage,
    isLidFormat as isLidFormatUtil,
    isPhoneNumberFormat,
    areJidsEquivalent,
    extractLidPhonePair
} from './jidUtils'
import { retryWithBackoff } from './connectionRetry'

export interface LidMapping {
    instanceId: string
    lid: string
    phoneNumber: string
    firstSeen: Date
    updatedAt: Date
}

export interface LidHandlerConfig {
    cacheTTL?: number // Cache TTL in seconds (default: 3600)
    enableCache?: boolean // Enable caching (default: true)
    skipIndexCreation?: boolean // Skip creating indexes here (default: true; smart manager handles it)
    ensureConnection?: () => Promise<void> // Optional callback to ensure connection before operations
    /**
     * When false, DB lookups into contacts are disabled; only legacy lidMappings and
     * reverse lookups from messages will be attempted. Default: true
     */
    lookupsEnabled?: boolean
    /**
     * When true, a successful legacy read from lidMappings triggers a best-effort
     * auto-fill into contacts to migrate the mapping forward (no pushName).
     * Default: false
     */
    autoFillFromLegacy?: boolean
    /**
     * Maximum number of concurrent normalization/lookup operations that may hit MongoDB.
     * Default: 10 (disable limiting with 0 or negative numbers).
     */
    maxConcurrentLookups?: number
    /**
     * TTL (in seconds) for negative cache entries (misses). Default derived from cacheTTL.
     */
    negativeCacheTTL?: number
    /** Optional Redis client for persistent caching */
    redisClient?: Redis
    /** Cache TTL in seconds for positive mappings in Redis (default: 3 days) */
    cacheTTLSeconds?: number
    /**
     * Prefer attempting a quick reverse lookup in messages before querying contacts.
     * Default: true
     */
    preferReverseLookupFirst?: boolean
    /**
     * Maximum time (ms) to spend on a single DB lookup; query is aborted after this.
     * Default: 500ms
     */
    contactsQueryMaxTimeMS?: number
    /**
     * When true, increase negative cache TTL exponentially per-lid on repeated misses.
     * Default: true
     */
    dynamicNegativeBackoff?: boolean
    /**
     * Lower and upper bounds for negative cache TTL (when backoff enabled).
     * Defaults: min 300s (5m), max 3600s (1h)
     */
    minNegativeCacheTTL?: number
    maxNegativeCacheTTL?: number
}

export class LidHandler {
    private static readonly NEGATIVE_CACHE_SENTINEL = '__MISS__'
    private lidMappingsCollection: Collection<LidMapping> | null = null // legacy read-only fallback
    private contactsCollection: Collection<any> | null = null
    private messagesCollection: Collection<any> | null = null
    private db: Db | null = null
    private cache: NodeCache
    private ensureConnectionCb?: () => Promise<void>
    private config: { cacheTTL: number; enableCache: boolean; skipIndexCreation: boolean; autoFillFromLegacy: boolean; lookupsEnabled: boolean; preferReverseLookupFirst: boolean; contactsQueryMaxTimeMS: number; dynamicNegativeBackoff: boolean; minNegativeCacheTTL: number; maxNegativeCacheTTL: number; cacheTTLSeconds: number }
    private instanceId: string
    private isInitialized: boolean = false
    private pendingLidLookups: Map<string, Promise<string | null>> = new Map()
    private pendingPhoneLookups: Map<string, Promise<string | null>> = new Map()
    private negativeCacheTTL: number
    private maxConcurrentLookups: number
    private activeLookups = 0
    private lookupQueue: Array<() => void> = []
    private missBackoffCounts: Map<string, number> = new Map()
    private redis: Redis | null = null

    constructor(instanceId: string, config?: LidHandlerConfig) {
        this.instanceId = instanceId
        this.config = {
            cacheTTL: config?.cacheTTL ?? 3600,
            enableCache: config?.enableCache ?? true,
            skipIndexCreation: config?.skipIndexCreation ?? true,
            autoFillFromLegacy: config?.autoFillFromLegacy ?? true,
            lookupsEnabled: config?.lookupsEnabled ?? true,
            preferReverseLookupFirst: config?.preferReverseLookupFirst ?? true,
            contactsQueryMaxTimeMS: config?.contactsQueryMaxTimeMS ?? 500,
            dynamicNegativeBackoff: config?.dynamicNegativeBackoff ?? true,
            minNegativeCacheTTL: config?.minNegativeCacheTTL ?? 300,
            maxNegativeCacheTTL: config?.maxNegativeCacheTTL ?? 3600,
            cacheTTLSeconds: config?.cacheTTLSeconds ?? (3 * 24 * 60 * 60)
        }
        this.ensureConnectionCb = config?.ensureConnection
        this.redis = config?.redisClient ?? null

        const derivedNegativeTTL = Math.max(60, Math.min(300, Math.floor((this.config.cacheTTL || 600) / 6)))
        this.negativeCacheTTL = config?.negativeCacheTTL ?? derivedNegativeTTL
        if (!Number.isFinite(this.negativeCacheTTL) || this.negativeCacheTTL <= 0) {
            this.negativeCacheTTL = 300
        }
        this.maxConcurrentLookups = config?.maxConcurrentLookups ?? 10

        // Initialize cache with configured TTL
        this.cache = new NodeCache({ 
            stdTTL: this.config.cacheTTL,
            checkperiod: Math.floor(this.config.cacheTTL / 10),
            useClones: false
        })
    }

    private getRedisKey(type: 'lid' | 'phone', value: string): string {
        return `${type}:${this.instanceId}:${value}`
    }

    private async cacheGet(key: string): Promise<string | typeof LidHandler.NEGATIVE_CACHE_SENTINEL | undefined> {
        if (this.redis) {
            try {
                const val = await this.redis.get(key)
                if (val === null) return undefined
                return val
            } catch (err) {
                console.debug('[LidHandler] Redis get failed:', (err as any)?.message)
            }
        }
        return this.cache.get<string | typeof LidHandler.NEGATIVE_CACHE_SENTINEL>(key)
    }

    private async cacheSet(key: string, value: string | typeof LidHandler.NEGATIVE_CACHE_SENTINEL, ttlSeconds?: number): Promise<void> {
        if (this.redis) {
            try {
                if (ttlSeconds && Number.isFinite(ttlSeconds) && ttlSeconds > 0) {
                    await this.redis.set(key, value as string, 'EX', Math.floor(ttlSeconds))
                } else {
                    await this.redis.set(key, value as string)
                }
                return
            } catch (err) {
                console.debug('[LidHandler] Redis set failed:', (err as any)?.message)
            }
        }
        const ttl = ttlSeconds && ttlSeconds > 0 ? ttlSeconds : this.config.cacheTTL
        this.cache.set(key, value as any, ttl)
    }

    private async cacheDel(keys: string[]): Promise<void> {
        if (this.redis) {
            try {
                if (keys.length > 0) {
                    await this.redis.del(...keys)
                }
            } catch (err) {
                console.debug('[LidHandler] Redis del failed:', (err as any)?.message)
            }
        } else {
            keys.forEach(k => this.cache.del(k))
        }
    }

    /**
     * Initialize the handler with MongoDB connection
     */
    async initialize(db: Db, collectionPrefix = ''): Promise<void> {
        // Legacy: keep lidMappings reference for temporary read fallback only
        const legacyCollectionName = collectionPrefix ? `${collectionPrefix}lidMappings` : 'lidMappings'
        this.lidMappingsCollection = db.collection<LidMapping>(legacyCollectionName)
        
        // Primary: contacts collection for centralized mapping
        const contactsCollectionName = collectionPrefix ? `${collectionPrefix}contacts` : 'contacts'
        this.contactsCollection = db.collection(contactsCollectionName)
        
        // Also get reference to messages collection for reverse lookups
        const messagesCollectionName = collectionPrefix ? `${collectionPrefix}messages` : 'messages'
        this.messagesCollection = db.collection(messagesCollectionName)
        this.db = db
        this.isInitialized = true
        
        // Create indexes for efficient lookups with retry logic (optional)
        if (!this.config.skipIndexCreation) {
            await this.createIndexes()
        } else {
            console.log('[LidHandler] Skipping index creation (managed by smart index manager)')
        }
    }

    /**
     * Check if the handler is properly initialized and connected
     */
    private isConnected(): boolean {
        // Treat handler as connected if initialized and we have a DB/collection reference.
        // Actual connection issues will be caught during operations and handled there.
        return this.isInitialized && !!this.db && !!this.contactsCollection
    }

    /**
     * Execute a database operation with connection check
     */
    private async withConnectionCheck<T>(
        operation: () => Promise<T>,
        fallback: T,
        operationName: string
    ): Promise<T> {
        // Best-effort: even if our connectivity heuristic says "not connected",
        // attempt the operation and fall back only on real connection errors.
        
        try {
            // Let the caller ensure/repair connection (shared ConnectionManager) if provided
            if (this.ensureConnectionCb) {
                try {
                    await this.ensureConnectionCb()
                } catch (e) {
                    // If ensureConnection fails, still attempt the operation; it'll fall back on error
                    console.debug(`[LidHandler] ${operationName}: ensureConnection failed, attempting operation anyway`)
                }
            }
            if (!this.isConnected()) {
                console.debug(`[LidHandler] ${operationName}: connectivity uncertain, attempting operation`)
            }
            return await operation()
        } catch (error: any) {
            // Check if this is a connection error
            if (error.name === 'MongoNotConnectedError' ||
                error.name === 'MongoPoolClosedError' ||
                error.message?.includes('Client must be connected') ||
                error.message?.includes('server is closed') ||
                error.message?.includes('Topology is closed')) {
                console.warn(`[LidHandler] ${operationName}: Connection lost during operation, returning fallback`)
                this.isInitialized = false // Mark as not initialized so caller can re-init after reconnection
                return fallback
            }
            
            // For other errors, log and re-throw
            console.error(`[LidHandler] ${operationName} failed:`, error)
            throw error
        }
    }

    private async runWithLimiter<T>(operation: () => Promise<T>): Promise<T> {
        if (this.maxConcurrentLookups <= 0) {
            return operation()
        }

        if (this.activeLookups >= this.maxConcurrentLookups) {
            await new Promise<void>((resolve) => this.lookupQueue.push(resolve))
        }

        this.activeLookups++
        try {
            return await operation()
        } finally {
            this.activeLookups--
            const next = this.lookupQueue.shift()
            if (next) {
                try {
                    next()
                } catch (_err) {
                    // no-op; releasing the queue should not throw
                }
            }
        }
    }

    /**
     * Create MongoDB indexes for efficient lookups with retry logic
     */
    private async createIndexes(): Promise<void> {
        const createIndexWithRetry = async (collection: Collection<any>, indexSpec: any, options?: any) => {
            const result = await retryWithBackoff(
                () => collection.createIndex(indexSpec, options),
                {
                    maxAttempts: 5,
                    initialDelay: 200,
                    maxDelay: 10000,
                    factor: 2,
                    jitter: true
                },
                (attempt, error, delay) => {
                    console.log(`[LID Handler] Retry attempt ${attempt} for index creation after error: ${error.message}. Waiting ${delay}ms...`)
                }
            )
            
            if (!result.success) {
                console.error(`[LID Handler] Failed to create index after ${result.attempts} attempts:`, result.error)
                // Don't throw - indexes are optimization, not critical for operation
            }
            
            return result
        }
        
        const promises: Promise<any>[] = []
        
        // Create indexes for contacts collection (centralized mapping)
        if (this.contactsCollection) {
            // Unique lid per instance across contacts where lid exists
            promises.push(
                createIndexWithRetry(
                    this.contactsCollection,
                    { instanceId: 1, lid: 1 },
                    { unique: true, background: true, partialFilterExpression: { lid: { $type: 'string' } } }
                )
            )
        }
        
        // Create indexes for messages collection (for reverse lookup)
        if (this.messagesCollection) {
            promises.push(
                // Compound index for reverse lookup: find messages by senderLid
                createIndexWithRetry(
                    this.messagesCollection,
                    { instanceId: 1, 'key.fromMe': 1, 'key.senderLid': 1 },
                    { background: true }
                )
            )
        }
        
        // Wait for all index operations to complete
        try {
            if (promises.length > 0) {
                await Promise.all(promises)
                console.log('[LidHandler] All indexes created successfully')
            }
        } catch (error) {
            console.error('[LidHandler] Some indexes failed to create, but continuing:', error)
            // Don't throw - indexes are optimization, not critical
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
     * Supports both new format (remoteJidAlt + addressingMode) and legacy format (senderLid + senderPn)
     */
    extractLidInfo(message: proto.IWebMessageInfo): {
        lid?: string
        phoneNumber?: string
        needsReverseLookup?: boolean
        addressingMode?: string
        debug?: string[]
    } {
        const result: { lid?: string; phoneNumber?: string; needsReverseLookup?: boolean; addressingMode?: string; debug?: string[] } = {}
        const debug: string[] = []
        
        // Extract new format fields (remoteJidAlt + addressingMode)
        const addressingMode = (message.key as any)?.addressingMode
        const remoteJidAlt = (message.key as any)?.remoteJidAlt
        
        // NEW FORMAT: Handle remoteJidAlt + addressingMode
        if (addressingMode && remoteJidAlt) {
            result.addressingMode = addressingMode
            debug.push(`New format detected: addressingMode=${addressingMode}`)
            
            if (addressingMode === 'pn') {
                // Incoming message: remoteJid is phone number, remoteJidAlt is LID
                if (this.isLidFormat(remoteJidAlt)) {
                    result.lid = remoteJidAlt
                    debug.push(`Found LID in remoteJidAlt (addressingMode=pn): ${result.lid}`)
                }
                if (message.key?.remoteJid && !this.isLidFormat(message.key.remoteJid)) {
                    result.phoneNumber = message.key.remoteJid
                    debug.push(`Found phone in remoteJid (addressingMode=pn): ${result.phoneNumber}`)
                }
            } else if (addressingMode === 'lid') {
                // Outgoing message: remoteJid is LID, remoteJidAlt is phone number
                if (message.key?.remoteJid && this.isLidFormat(message.key.remoteJid)) {
                    result.lid = message.key.remoteJid
                    debug.push(`Found LID in remoteJid (addressingMode=lid): ${result.lid}`)
                }
                if (!this.isLidFormat(remoteJidAlt)) {
                    result.phoneNumber = remoteJidAlt
                    debug.push(`Found phone in remoteJidAlt (addressingMode=lid): ${result.phoneNumber}`)
                }
                // For outgoing messages with LID remoteJid but phone in remoteJidAlt, no reverse lookup needed
                if (result.lid && result.phoneNumber) {
                    result.needsReverseLookup = false
                    debug.push(`Both LID and phone found via new format, no reverse lookup needed`)
                }
            }
            
            // If we got both LID and phone from new format, return early
            if (result.lid && result.phoneNumber) {
                if (process.env.NODE_ENV !== 'production' || debug.length > 0) {
                    result.debug = debug
                }
                return result
            }
        }
        
        // LEGACY FORMAT: Check if this is a sent message (fromMe: true) with LID remoteJid
        if (message.key?.fromMe && this.isLidFormat(message.key?.remoteJid)) {
            result.lid = message.key.remoteJid!
            // Only need reverse lookup if we don't already have phone from new format
            if (!result.phoneNumber) {
                result.needsReverseLookup = true
                debug.push(`Found LID in remoteJid (fromMe=true, needs reverse lookup): ${result.lid}`)
            }
            
            // For sent messages, we won't find phone number in the message itself
            // We need to do a reverse lookup (unless new format provided it)
            if (result.needsReverseLookup) {
                if (process.env.NODE_ENV !== 'production' || debug.length > 0) {
                    result.debug = debug
                }
                return result
            }
        }
        
        // LEGACY: Check remoteJid for @lid
        if (!result.lid && this.isLidFormat(message.key?.remoteJid)) {
            result.lid = message.key!.remoteJid!
            debug.push(`Found LID in remoteJid: ${result.lid}`)
        }
        
        // LEGACY: Check senderLid (some messages have this)
        if (!result.lid && this.isLidFormat((message.key as any)?.senderLid)) {
            result.lid = (message.key as any).senderLid
            debug.push(`Found LID in senderLid: ${result.lid}`)
        }
        
        // LEGACY: Extract phone number from senderPn
        // Special handling: For fromMe messages with LID remoteJid, senderPn might incorrectly be a LID too
        // In this case, we should ignore senderPn and rely on reverse lookup
        if (!result.phoneNumber && (message.key as any)?.senderPn) {
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
        
        // LEGACY: If remoteJid is not @lid, it might be the phone number
        if (!result.phoneNumber && message.key?.remoteJid && !this.isLidFormat(message.key.remoteJid)) {
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
    async storeLidMapping(lid: string, phoneNumber: string, pushName?: string): Promise<void> {
        if (!this.isConnected()) {
            console.debug('[LidHandler] Cannot store LID mapping - database not connected')
            // Still update cache if enabled
            if (this.config.enableCache) {
                const normalizedLid = normalizeJidForStorage(lid)
                const normalizedPhone = normalizeJidForStorage(phoneNumber)
                if (normalizedLid && normalizedPhone && this.isLidFormat(normalizedLid) && isPhoneNumberFormat(normalizedPhone)) {
                    this.cache.set(`lid:${this.instanceId}:${normalizedLid}`, normalizedPhone)
                    this.cache.set(`phone:${this.instanceId}:${normalizedPhone}`, normalizedLid)
                }
            }
            return
        }
        
        if (!this.contactsCollection) {
            console.warn('LidHandler not initialized with contacts collection')
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
        const cleanedPushName = (typeof pushName === 'string' ? pushName.trim() : '') || undefined
        
        // Decide whether to update fields to avoid unnecessary writes
        let shouldSetPushName = false
        let shouldSetLid = true // default true; will turn false if existing doc has same lid
        try {
            const existing = await this.contactsCollection.findOne(
                { instanceId: this.instanceId, id: normalizedPhone },
                { projection: { pushName: 1, lid: 1 } }
            )
            if (existing) {
                if (cleanedPushName && existing.pushName !== cleanedPushName) {
                    shouldSetPushName = true
                }
                // Only set lid if changed
                if (existing.lid === normalizedLid) {
                    shouldSetLid = false
                }
            } else {
                // New document: set phone and pushName (if provided)
                shouldSetLid = true
                shouldSetPushName = !!cleanedPushName
            }
        } catch (_err) {
            // If read fails, proceed with setting both (safe; idempotent if equal)
            shouldSetLid = true
            shouldSetPushName = !!cleanedPushName
        }
        
        try {
            // Upsert the mapping into contacts (by phone JID as contact id)
            try {
                await this.contactsCollection.updateOne(
                    { 
                        instanceId: this.instanceId, 
                        id: normalizedPhone 
                    },
                    {
                        $set: {
                            ...(shouldSetLid ? { lid: normalizedLid } : {}),
                            updatedAt: now,
                            ...(shouldSetPushName ? { pushName: cleanedPushName, pushNameUpdatedAt: now } : {})
                        },
                        $setOnInsert: {
                            instanceId: this.instanceId,
                            id: normalizedPhone,
                            lidFirstSeen: now
                        }
                    },
                    { upsert: true }
                )
            } catch (e: any) {
                if (e?.code === 11000) {
                    // Retry without upsert; don't override existing pushName if set
                    await this.contactsCollection.updateOne(
                        { 
                            instanceId: this.instanceId, 
                            id: normalizedPhone,
                            ...(shouldSetPushName ? { $or: [ { pushName: { $exists: false } }, { pushName: { $in: [null, ''] } } ] } : {})
                        } as any,
                        {
                            $set: {
                                ...(shouldSetLid ? { lid: normalizedLid } : {}),
                                updatedAt: now,
                                ...(shouldSetPushName ? { pushName: cleanedPushName, pushNameUpdatedAt: now } : {})
                            }
                        },
                        { upsert: false }
                    )
                } else {
                    throw e
                }
            }

            // Update cache if enabled (use normalized JIDs for cache keys)
            if (this.config.enableCache) {
                // Cache both directions
                this.cache.set(`lid:${this.instanceId}:${normalizedLid}`, normalizedPhone)
                this.cache.set(`phone:${this.instanceId}:${normalizedPhone}`, normalizedLid)
            }

            if (this.lidMappingsCollection) {
                try {
                    const result = await this.lidMappingsCollection.updateOne(
                        { instanceId: this.instanceId, lid: normalizedLid },
                        {
                            $setOnInsert: {
                                instanceId: this.instanceId,
                                lid: normalizedLid,
                                phoneNumber: normalizedPhone,
                                firstSeen: now,
                                updatedAt: now
                            }
                        },
                        { upsert: true }
                    )

                    if (result.matchedCount > 0 && result.upsertedCount === 0 && process.env.DEBUG_LID === 'true') {
                        const existing = await this.lidMappingsCollection.findOne(
                            { instanceId: this.instanceId, lid: normalizedLid },
                            { projection: { phoneNumber: 1 } }
                        )
                        if (existing && existing.phoneNumber !== normalizedPhone) {
                            console.debug(
                                `[LidHandler] Existing mapping for ${normalizedLid} uses ${existing.phoneNumber}; ` +
                                `ignoring new phone ${normalizedPhone}`
                            )
                        }
                    }
                } catch (mappingError) {
                    console.debug('[LidHandler] Failed to upsert lidMappings entry:', (mappingError as any)?.message)
                }
            }
            
            console.log(`[LidHandler] Stored mapping in contacts: ${normalizedLid} -> ${normalizedPhone}`)
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

        const cacheKey = this.getRedisKey('lid', normalizedLid)
        if (this.config.enableCache) {
            const cached = await this.cacheGet(cacheKey)
            if (cached !== undefined) {
                return cached === LidHandler.NEGATIVE_CACHE_SENTINEL ? null : cached
            }
        }

        if (this.pendingLidLookups.has(normalizedLid)) {
            return this.pendingLidLookups.get(normalizedLid)!
        }

        const lookupPromise = this.runWithLimiter(async () => {
            const result = await this.withConnectionCheck(
                async () => {
                    if (!this.contactsCollection && !this.lidMappingsCollection) {
                        return null
                    }

                    let phoneNumber: string | null = null
                    let foundInContacts = false

                    // 0) Quick reverse lookup from messages first (optional, fast path)
                    if (this.config.preferReverseLookupFirst) {
                        try {
                            const reverse = await this.reversePhoneLookupFromMessages(normalizedLid)
                            if (reverse) {
                                phoneNumber = normalizeJidForStorage(reverse)
                            }
                        } catch (_err) {
                            // Ignore errors during reverse lookup fallback
                        }
                    }

                    if (this.lidMappingsCollection) {
                        try {
                            const mapping = await this.lidMappingsCollection.findOne(
                                { instanceId: this.instanceId, lid: normalizedLid },
                                { projection: { phoneNumber: 1 } }
                            )
                            if (mapping?.phoneNumber) {
                                phoneNumber = normalizeJidForStorage(mapping.phoneNumber)
                                if (phoneNumber && this.config.autoFillFromLegacy) {
                                    try {
                                        await this.storeLidMapping(normalizedLid, phoneNumber)
                                    } catch (fillError) {
                                        console.debug('[LidHandler] Auto-fill from legacy mapping failed:', (fillError as any)?.message)
                                    }
                                }
                            }
                        } catch (err) {
                            console.debug('[LidHandler] Failed to read lidMappings entry:', (err as any)?.message)
                        }
                    }

                    if (!phoneNumber && this.contactsCollection && this.config.lookupsEnabled) {
                        const query = { instanceId: this.instanceId, lid: normalizedLid }
                        try {
                            const contact = await this.contactsCollection.findOne(
                                query,
                                {
                                    projection: { id: 1 },
                                    maxTimeMS: this.config.contactsQueryMaxTimeMS
                                } as any
                            )
                            if (contact?.id) {
                                phoneNumber = normalizeJidForStorage(contact.id)
                                foundInContacts = true
                            }
                        } catch (err: any) {
                            // On timeout or transient errors, skip and fall back to null
                            if (err?.code === 50 || /exceeded time limit|operation exceeded time limit/i.test(err?.message || '')) {
                                console.debug('[LidHandler] Contacts lookup timed out, skipping')
                            } else {
                                console.debug('[LidHandler] Contacts lookup failed:', err?.message)
                            }
                        }
                    }

                    if (phoneNumber && this.lidMappingsCollection && foundInContacts) {
                        const now = new Date()
                        try {
                            const result = await this.lidMappingsCollection.updateOne(
                                { instanceId: this.instanceId, lid: normalizedLid },
                                {
                                    $setOnInsert: {
                                        instanceId: this.instanceId,
                                        lid: normalizedLid,
                                        phoneNumber,
                                        firstSeen: now,
                                        updatedAt: now
                                    }
                                },
                                { upsert: true }
                            )

                            if (result.matchedCount > 0 && result.upsertedCount === 0 && process.env.DEBUG_LID === 'true') {
                                const existing = await this.lidMappingsCollection.findOne(
                                    { instanceId: this.instanceId, lid: normalizedLid },
                                    { projection: { phoneNumber: 1 } }
                                )
                                if (existing && existing.phoneNumber !== phoneNumber) {
                                    console.debug(
                                        `[LidHandler] Skipping lidMappings update for ${normalizedLid}; ` +
                                        `existing phone ${existing.phoneNumber}, candidate ${phoneNumber}`
                                    )
                                }
                            }
                        } catch (err) {
                            console.debug('[LidHandler] Failed to backfill lidMappings from contacts:', (err as any)?.message)
                        }
                    }

                    return phoneNumber
                },
                null,
                'getPhoneNumberFromLid'
            )

            return result
        })

        this.pendingLidLookups.set(normalizedLid, lookupPromise)

        try {
            const result = await lookupPromise
            if (this.config.enableCache) {
                if (result) {
                    await this.cacheSet(cacheKey, result, this.config.cacheTTLSeconds)
                    await this.cacheSet(this.getRedisKey('phone', result), normalizedLid, this.config.cacheTTLSeconds)
                    // reset backoff on success
                    this.missBackoffCounts.delete(normalizedLid)
                } else {
                    // Compute dynamic backoff TTL for this lid
                    let ttl = this.negativeCacheTTL
                    if (this.config.dynamicNegativeBackoff) {
                        const misses = (this.missBackoffCounts.get(normalizedLid) || 0) + 1
                        this.missBackoffCounts.set(normalizedLid, misses)
                        const base = Math.max(this.config.minNegativeCacheTTL, this.negativeCacheTTL)
                        const candidate = base * Math.pow(2, Math.max(0, misses - 1))
                        ttl = Math.min(this.config.maxNegativeCacheTTL, candidate)
                    }
                    await this.cacheSet(cacheKey, LidHandler.NEGATIVE_CACHE_SENTINEL, ttl)
                }
            }
            return result
        } finally {
            this.pendingLidLookups.delete(normalizedLid)
        }
    }

    /**
     * Get @lid from phone number (reverse lookup)
     */
    async getLidFromPhoneNumber(phoneNumber: string): Promise<string | null> {
        const normalizedPhone = normalizeJidForStorage(phoneNumber)
        if (this.isLidFormat(normalizedPhone)) return normalizedPhone
        
        const cacheKey = this.getRedisKey('phone', normalizedPhone)
        if (this.config.enableCache) {
            const cached = await this.cacheGet(cacheKey)
            if (cached !== undefined) {
                return cached === LidHandler.NEGATIVE_CACHE_SENTINEL ? null : cached
            }
        }

        if (this.pendingPhoneLookups.has(normalizedPhone)) {
            return this.pendingPhoneLookups.get(normalizedPhone)!
        }

        const lookupPromise = this.runWithLimiter(async () => {
            const result = await this.withConnectionCheck(
                async () => {
                    if (!this.contactsCollection && !this.lidMappingsCollection) {
                        return null
                    }

                    let lid: string | null = null
                    let foundInContacts = false

                    if (this.lidMappingsCollection) {
                        try {
                            const mapping = await this.lidMappingsCollection.findOne(
                                { instanceId: this.instanceId, phoneNumber: normalizedPhone },
                                { projection: { lid: 1 } }
                            )
                            if (mapping?.lid) {
                                lid = normalizeJidForStorage(mapping.lid)
                                if (lid && this.config.autoFillFromLegacy) {
                                    try {
                                        await this.storeLidMapping(lid, normalizedPhone)
                                    } catch (fillError) {
                                        console.debug('[LidHandler] Auto-fill from legacy mapping (reverse) failed:', (fillError as any)?.message)
                                    }
                                }
                            }
                        } catch (err) {
                            console.debug('[LidHandler] Failed to read lidMappings by phone:', (err as any)?.message)
                        }
                    }

                    if (!lid && this.contactsCollection) {
                        try {
                            const contact = await this.contactsCollection.findOne(
                                { instanceId: this.instanceId, id: normalizedPhone },
                                { projection: { lid: 1 }, maxTimeMS: this.config.contactsQueryMaxTimeMS } as any
                            )
                            if (contact?.lid && this.isLidFormat(contact.lid)) {
                                lid = normalizeJidForStorage(contact.lid)
                                foundInContacts = true
                            }
                        } catch (err) {
                            console.debug('[LidHandler] Failed to read contacts by phone:', (err as any)?.message)
                        }
                    }

                    if (lid && this.lidMappingsCollection && foundInContacts) {
                        const now = new Date()
                        try {
                            const result = await this.lidMappingsCollection.updateOne(
                                { instanceId: this.instanceId, lid },
                                {
                                    $setOnInsert: {
                                        instanceId: this.instanceId,
                                        lid,
                                        phoneNumber: normalizedPhone,
                                        firstSeen: now,
                                        updatedAt: now
                                    }
                                },
                                { upsert: true }
                            )

                            if (result.matchedCount > 0 && result.upsertedCount === 0 && process.env.DEBUG_LID === 'true') {
                                const existing = await this.lidMappingsCollection.findOne(
                                    { instanceId: this.instanceId, lid },
                                    { projection: { phoneNumber: 1 } }
                                )
                                if (existing && existing.phoneNumber !== normalizedPhone) {
                                    console.debug(
                                        `[LidHandler] Skipping lidMappings update for ${lid}; ` +
                                        `existing phone ${existing.phoneNumber}, candidate ${normalizedPhone}`
                                    )
                                }
                            }
                        } catch (err) {
                            console.debug('[LidHandler] Failed to backfill lidMappings from contacts (reverse):', (err as any)?.message)
                        }
                    }

                    return lid
                },
                null,
                'getLidFromPhoneNumber'
            )

            return result
        })

        this.pendingPhoneLookups.set(normalizedPhone, lookupPromise)

        try {
            const result = await lookupPromise
            if (this.config.enableCache) {
                if (result) {
                    await this.cacheSet(cacheKey, result, this.config.cacheTTLSeconds)
                    await this.cacheSet(this.getRedisKey('lid', result), normalizedPhone, this.config.cacheTTLSeconds)
                } else {
                    await this.cacheSet(cacheKey, LidHandler.NEGATIVE_CACHE_SENTINEL, this.negativeCacheTTL)
                }
            }
            return result
        } finally {
            this.pendingPhoneLookups.delete(normalizedPhone)
        }
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
     * Searches for received messages where senderLid or remoteJidAlt matches the LID
     * Supports both legacy (senderLid/senderPn) and new (remoteJidAlt/addressingMode) formats
     */
    async discoverPhoneFromSentMessage(lid: string): Promise<string | null> {
        const normalizedLid = normalizeJidForStorage(lid)
        console.log(`[LidHandler] Searching for phone number from sent message with LID: ${normalizedLid}`)
        
        return await this.withConnectionCheck(
            async () => {
                if (!this.messagesCollection) {
                    return null
                }
                
                // Search for messages where:
                // 1. fromMe is false (received messages)
                // 2. senderLid matches our LID (legacy format)
                // OR remoteJidAlt matches our LID with addressingMode='pn' (new format)
                const receivedMessage = await this.messagesCollection.findOne({
                    instanceId: this.instanceId,
                    'key.fromMe': false,
                    $or: [
                        // Legacy format: senderLid
                        { 'key.senderLid': normalizedLid },
                        { 'key.senderLid': lid },
                        // New format: remoteJidAlt is LID when addressingMode='pn' (incoming)
                        { 'key.remoteJidAlt': normalizedLid, 'key.addressingMode': 'pn' },
                        { 'key.remoteJidAlt': lid, 'key.addressingMode': 'pn' }
                    ]
                }, { maxTimeMS: this.config.contactsQueryMaxTimeMS } as any)
                
                if (receivedMessage) {
                    // Extract phone number from various fields
                    let phoneNumber: string | null = null
                    
                    // NEW FORMAT: Check remoteJid when addressingMode='pn' (phone is in remoteJid)
                    const addressingMode = (receivedMessage.key as any)?.addressingMode
                    if (addressingMode === 'pn' && receivedMessage.key?.remoteJid && !this.isLidFormat(receivedMessage.key.remoteJid)) {
                        phoneNumber = receivedMessage.key.remoteJid
                        console.log(`[LidHandler] Found phone number in remoteJid (addressingMode=pn): ${phoneNumber}`)
                    }
                    // LEGACY: First try senderPn
                    else if ((receivedMessage.key as any)?.senderPn && !this.isLidFormat((receivedMessage.key as any).senderPn)) {
                        phoneNumber = (receivedMessage.key as any).senderPn
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
                    console.log(`[LidHandler] No received messages found with senderLid/remoteJidAlt: ${normalizedLid}`)
                }
                
                return null
            },
            null,
            'discoverPhoneFromSentMessage'
        )
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
     * Supports both legacy (senderLid/senderPn) and new (remoteJidAlt/addressingMode) formats
     */
    async reversePhoneLookupFromMessages(lid: string): Promise<string | null> {
        const normalizedLid = normalizeJidForStorage(lid)
        console.log(`[LidHandler] Attempting reverse lookup for LID: ${normalizedLid}`)
        
        return await this.withConnectionCheck(
            async () => {
                if (!this.messagesCollection) {
                    return null
                }
                
                // Look for messages where this LID appears with a phone number
                // Add projection to only fetch needed fields for performance
                const message = await this.messagesCollection.findOne({
                    instanceId: this.instanceId,
                    $or: [
                        // LEGACY Case 1: LID in senderLid with phone in senderPn
                        { 
                            'key.senderLid': normalizedLid,
                            'key.senderPn': { 
                                $exists: true, 
                                $nin: [null, '']
                            }
                        },
                        // LEGACY Case 2: LID in remoteJid with phone in senderPn (fromMe=false)
                        {
                            'key.remoteJid': normalizedLid,
                            'key.fromMe': false,
                            'key.senderPn': { 
                                $exists: true,
                                $nin: [null, '']
                            }
                        },
                        // NEW FORMAT Case 3: LID in remoteJidAlt with addressingMode='pn' (phone in remoteJid)
                        {
                            'key.remoteJidAlt': normalizedLid,
                            'key.addressingMode': 'pn'
                        },
                        // NEW FORMAT Case 4: LID in remoteJid with addressingMode='lid' (phone in remoteJidAlt)
                        {
                            'key.remoteJid': normalizedLid,
                            'key.addressingMode': 'lid',
                            'key.remoteJidAlt': { 
                                $exists: true,
                                $nin: [null, '']
                            }
                        }
                    ]
                }, {
                    projection: { 
                        'key.senderPn': 1, 
                        'key.remoteJid': 1,
                        'key.remoteJidAlt': 1,
                        'key.addressingMode': 1
                    },
                    maxTimeMS: this.config.contactsQueryMaxTimeMS
                } as any)
                
                if (message?.key) {
                    const addressingMode = (message.key as any)?.addressingMode
                    const remoteJidAlt = (message.key as any)?.remoteJidAlt
                    
                    // NEW FORMAT: Check based on addressingMode
                    if (addressingMode === 'pn' && message.key.remoteJid && !this.isLidFormat(message.key.remoteJid)) {
                        console.log(`[LidHandler] Reverse lookup found (new format, addressingMode=pn): ${normalizedLid} -> ${message.key.remoteJid}`)
                        return message.key.remoteJid
                    }
                    if (addressingMode === 'lid' && remoteJidAlt && !this.isLidFormat(remoteJidAlt)) {
                        console.log(`[LidHandler] Reverse lookup found (new format, addressingMode=lid): ${normalizedLid} -> ${remoteJidAlt}`)
                        return remoteJidAlt
                    }
                    
                    // LEGACY: Check senderPn
                    const senderPn = (message.key as any)?.senderPn
                    if (senderPn && !this.isLidFormat(senderPn)) {
                        console.log(`[LidHandler] Reverse lookup found (legacy): ${normalizedLid} -> ${senderPn}`)
                        return senderPn
                    }
                }
                
                return null
            },
            null,
            'reversePhoneLookupFromMessages'
        )
    }

    /**
     * Update existing messages that have a LID to use the phone number
     */
    async updateExistingMessages(lid: string, phoneNumber: string): Promise<void> {
        await this.withConnectionCheck(
            async () => {
                if (!this.messagesCollection) {
                    return
                }
                
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
            },
            undefined,
            'updateExistingMessages'
        )
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
            addressingMode?: string
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
                // Outgoing message: do not persist pushName (it's our own display)
                await this.storeLidMapping(lidInfo.lid, discoveredPhone)
                mappingStored = true
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
                const fromMe = !!message.key?.fromMe
                const pushName = !fromMe ? ((message as any)?.pushName || (message as any)?.verifiedBizName) : undefined
                await this.storeLidMapping(lidInfo.lid, lidInfo.phoneNumber, pushName)
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
        if (!this.config.enableCache) return
        if (this.redis) {
            const patterns = [
                `lid:${this.instanceId}:*`,
                `phone:${this.instanceId}:*`
            ]
            const run = async () => {
                for (const pattern of patterns) {
                    let cursor = '0'
                    do {
                        try {
                            const res = await this.redis!.scan(cursor, 'MATCH', pattern, 'COUNT', 100)
                            cursor = res[0]
                            const keys = res[1]
                            if (keys && keys.length) {
                                await this.cacheDel(keys)
                            }
                        } catch (err) {
                            console.debug('[LidHandler] Redis scan failed:', (err as any)?.message)
                            break
                        }
                    } while (cursor !== '0')
                }
            }
            run().catch(() => {})
        } else {
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
    async getAllMappings(): Promise<Array<LidMapping & { pushName?: string; pushNameUpdatedAt?: Date }>> {
        return await this.withConnectionCheck(
            async () => {
                if (!this.contactsCollection) {
                    return []
                }
                const docs = await this.contactsCollection
                    .find(
                        { instanceId: this.instanceId, lid: { $exists: true, $nin: [null, ''] } },
                        { projection: { id: 1, lid: 1, lidFirstSeen: 1, updatedAt: 1, pushName: 1, pushNameUpdatedAt: 1 } }
                    )
                    .toArray()
                return docs.map((d: any) => ({
                    instanceId: this.instanceId,
                    lid: d.lid,
                    phoneNumber: d.id,
                    firstSeen: d.lidFirstSeen || d.updatedAt || new Date(),
                    updatedAt: d.updatedAt || new Date(),
                    pushName: d.pushName,
                    pushNameUpdatedAt: d.pushNameUpdatedAt
                }))
            },
            [],
            'getAllMappings'
        )
    }

    /**
     * Delete old mappings that haven't been seen in specified days
     */
    async cleanupOldMappings(_daysOld: number = 90): Promise<number> {
        // No-op: contacts do not use lidLastSeen/lidMappingUpdatedAt anymore; avoid unintended cleanup
        return 0
    }
}
