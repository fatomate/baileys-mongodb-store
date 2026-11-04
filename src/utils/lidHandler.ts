import { Collection, Db, ObjectId } from 'mongodb'
import Redis from 'ioredis'
import NodeCache from 'node-cache'
import { proto } from 'baileys'
import { 
    normalizeJidForStorage,
    isLidFormat as isLidFormatUtil,
    isPhoneNumberFormat,
    areJidsEquivalent,
    extractLidPhonePair,
    canonicalPhoneJid
} from './jidUtils'
import { retryWithBackoff } from './connectionRetry'
import { errorKind, reportBurst } from './burstLog'
import type { BaileysLIDMappingStore, BaileysLIDMapping } from './lidCompatibility'

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
     * WAB-978: runs each mapping write with the store's connection recovery (reconnect, retry,
     * operation counting). The operation reads the collection handles on each attempt, so a
     * retry after a reconnect uses the new client.
     */
    runOperation?: <T>(operation: () => Promise<T>) => Promise<T>
    /** WAB-978: store-level limit for concurrent lid-mapping.update persistence. Default: 16 */
    mappingUpdateConcurrency?: number
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

/**
 * WAB-1275: Redis-side fence for the lookup cache. Every handler of an instance shares it.
 * For cache key K: `{K}:gen` counts ownership writes and `{K}:busy` marks a write in progress.
 * The hash tag keeps the three keys in one cluster slot. A lookup caches its result only when no
 * write is in progress and no write started since the lookup began.
 */
export const LID_CACHE_SCRIPTS = Object.freeze({
    // KEYS: key, gen, busy. ARGV: gen TTL s, busy TTL ms
    beginWrite: `redis.call('DEL', KEYS[1])
redis.call('INCR', KEYS[2]) redis.call('EXPIRE', KEYS[2], ARGV[1])
redis.call('INCR', KEYS[3]) redis.call('PEXPIRE', KEYS[3], ARGV[2])
return 1`,
    // KEYS: key, gen, busy. ARGV: gen TTL s
    endWrite: `redis.call('DEL', KEYS[1])
redis.call('INCR', KEYS[2]) redis.call('EXPIRE', KEYS[2], ARGV[1])
if redis.call('DECR', KEYS[3]) <= 0 then redis.call('DEL', KEYS[3]) end
return 1`,
    // KEYS: key, gen, busy. ARGV: generation seen when the lookup began ('' = none), value, TTL s
    publish: `if redis.call('EXISTS', KEYS[3]) == 1 then return 0 end
if (redis.call('GET', KEYS[2]) or '') ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
return 1`,
})

type LookupFence = { stale: boolean; redisGeneration: string | null }

export class LidHandler {
    private static readonly NEGATIVE_CACHE_SENTINEL = '__MISS__'
    // A write in progress blocks lookup caching for at most this long if its end call fails
    private static readonly FENCE_BUSY_TTL_MS = 5 * 60 * 1000
    private static readonly FENCE_GENERATION_TTL_SECONDS = 60 * 60
    // WAB-1275: counts ordinary pairs that disagree with the current owner (no IDs are logged)
    private static readonly KEPT_LABEL = '[LidHandler] LID kept by existing owner'
    private static readonly PHONE_JID_PATTERN = /(@s\.whatsapp\.net|@c\.us)$/
    // WAB-1275: one ownership write per LID at a time in this process
    private lidLocks: Map<string, Promise<void>> = new Map()
    // WAB-1275: in-flight lookups per cache key; a write marks them stale so they do not cache old reads
    private lookupFences: Map<string, Set<LookupFence>> = new Map()
    // WAB-1275: cache keys with an ownership write in progress; a lookup that starts then never caches
    private keysBeingWritten: Map<string, number> = new Map()
    private lidMappingsCollection: Collection<LidMapping> | null = null // legacy read-only fallback
    private contactsCollection: Collection<any> | null = null
    private messagesCollection: Collection<any> | null = null
    private db: Db | null = null
    private cache: NodeCache
    private ensureConnectionCb?: () => Promise<void>
    private runOperation: <T>(operation: () => Promise<T>) => Promise<T>
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
    
    // Baileys v7 native LID mapping store reference
    private baileysLidMappingStore: BaileysLIDMappingStore | null = null

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
        this.runOperation = config?.runOperation ?? (<T>(operation: () => Promise<T>) => operation())
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

    private fenceKeys(key: string): [string, string, string] {
        return [key, `{${key}}:gen`, `{${key}}:busy`]
    }

    // WAB-1275: a write marks its keys busy and empties them, in Redis and in the local fallback.
    // `begun` records each key whose Redis begin succeeded; only those are ended later, so a failed
    // begin never removes another writer's busy marker. A Redis error throws, so the caller stops
    // before it changes MongoDB.
    private async beginCacheWrite(keys: string[], begun: string[]): Promise<void> {
        this.startKeyWrites(keys)
        keys.forEach(k => this.cache.del(k))
        if (!this.redis) return
        for (const key of keys) {
            await this.redis.eval(LID_CACHE_SCRIPTS.beginWrite, 3, ...this.fenceKeys(key),
                LidHandler.FENCE_GENERATION_TTL_SECONDS, LidHandler.FENCE_BUSY_TTL_MS)
            begun.push(key)
        }
    }

    // The write ends: empty the keys again and advance the generation, so no lookup that overlapped
    // the write can cache what it read. A Redis error here only delays caching until busy expires.
    private async endCacheWrite(keys: string[], begun: string[]): Promise<void> {
        keys.forEach(k => this.cache.del(k))
        if (this.redis) {
            for (const key of begun) {
                await this.redis.eval(LID_CACHE_SCRIPTS.endWrite, 3, ...this.fenceKeys(key), LidHandler.FENCE_GENERATION_TTL_SECONDS)
                    .catch((error: unknown) => console.debug('[LidHandler] Cache fence end failed:', errorKind(error)))
            }
        }
        this.endKeyWrites(keys)
    }

    private beginFence(key: string): LookupFence {
        const fence: LookupFence = { stale: this.keysBeingWritten.has(key), redisGeneration: null }
        let fences = this.lookupFences.get(key)
        if (!fences) {
            fences = new Set()
            this.lookupFences.set(key, fences)
        }
        fences.add(fence)
        return fence
    }

    // Read the Redis write generation before the lookup reads MongoDB. Without it the lookup does not
    // publish to Redis (only to the fenced local fallback).
    private async readFenceGeneration(key: string, fence: LookupFence): Promise<void> {
        if (!this.redis || !this.config.enableCache) return
        try {
            fence.redisGeneration = (await this.redis.get(this.fenceKeys(key)[1])) ?? ''
        } catch (err) {
            console.debug('[LidHandler] Redis get failed:', (err as any)?.message)
        }
    }

    // Cache a lookup result only if no ownership write overlapped the lookup, in this handler or in
    // any handler that shares Redis. With Redis configured, a lookup never writes the local fallback:
    // the local fence cannot see writes made by another handler.
    private async publishLookup(key: string, value: string, ttlSeconds: number, fence: LookupFence): Promise<void> {
        if (fence.stale) return
        if (!this.redis) {
            this.cache.set(key, value, ttlSeconds > 0 ? ttlSeconds : this.config.cacheTTL)
            return
        }
        if (fence.redisGeneration === null) return
        try {
            await this.redis.eval(LID_CACHE_SCRIPTS.publish, 3, ...this.fenceKeys(key),
                fence.redisGeneration, value, Math.max(1, Math.floor(ttlSeconds)))
        } catch (err) {
            console.debug('[LidHandler] Redis set failed:', (err as any)?.message)
        }
    }

    private endFence(key: string, fence: LookupFence): void {
        const fences = this.lookupFences.get(key)
        if (!fences) return
        fences.delete(fence)
        if (fences.size === 0) this.lookupFences.delete(key)
    }

    private invalidateFences(keys: string[]): void {
        for (const key of keys) this.lookupFences.get(key)?.forEach(fence => { fence.stale = true })
    }

    private startKeyWrites(keys: string[]): void {
        for (const key of keys) this.keysBeingWritten.set(key, (this.keysBeingWritten.get(key) ?? 0) + 1)
        this.invalidateFences(keys)
    }

    private endKeyWrites(keys: string[]): void {
        this.invalidateFences(keys)
        for (const key of keys) {
            const count = (this.keysBeingWritten.get(key) ?? 1) - 1
            if (count > 0) this.keysBeingWritten.set(key, count)
            else this.keysBeingWritten.delete(key)
        }
    }

    private async withLidLock<T>(lid: string, operation: () => Promise<T>): Promise<T> {
        const previous = this.lidLocks.get(lid) ?? Promise.resolve()
        let release!: () => void
        const done = new Promise<void>(resolve => { release = resolve })
        const current = previous.then(() => done)
        this.lidLocks.set(lid, current)
        await previous
        try {
            return await operation()
        } finally {
            release()
            if (this.lidLocks.get(lid) === current) this.lidLocks.delete(lid)
        }
    }

    private asPhoneJid(jid: string | null | undefined): string | null {
        const normalized = jid ? normalizeJidForStorage(jid) : null
        return normalized && isPhoneNumberFormat(normalized) ? normalized : null
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
                // WAB-978: keep isInitialized. Clearing it made storeLidMapping skip writes silently
                // until the next re-init; the store's reconnect refreshes the handles instead.
                console.warn(`[LidHandler] ${operationName}: Connection lost during operation, returning fallback`)
                return fallback
            }
            
            // For other errors, log and re-throw
            console.error(`[LidHandler] ${operationName} failed:`, ((error as any)?.codeName || (error as any)?.name || 'Error'))
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
            console.error('[LidHandler] Some indexes failed to create, but continuing:', ((error as any)?.codeName || (error as any)?.name || 'Error'))
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
     * Store a LID to phone number mapping under the WAB-1275 ownership rule.
     *
     * The owner of a LID is the phone contact that holds it in contacts and the valid
     * (phone-valued) lidMappings row. A self-referencing @lid doc (id equal to the LID) is never
     * an owner and always yields. An ordinary pair writes only when it agrees with every owner.
     * Only an authoritative pair (Baileys lid-mapping.update) moves a LID from another owner.
     *
     * Order: invalidate caches, write the mapping (the cutover), release the holder, assign the
     * contact, publish the caches. A failed step stops the later steps; there is no duplicate-key
     * retry loop. Redis never disagrees with MongoDB: a Redis failure before the MongoDB writes leaves
     * MongoDB unchanged. The write empties the affected keys and never publishes; the next lookup
     * caches the new pair through the Redis-side fence (LID_CACHE_SCRIPTS).
     * An authoritative mapping write is the cutover and is kept if a later step fails.
     */
    async storeLidMapping(lid: string, phoneNumber: string, pushName?: string, options: { authoritative?: boolean } = {}): Promise<void> {
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
            console.warn(`[LidHandler] Attempted to store invalid LID->LID mapping: [redacted] -> [redacted]`)
            return
        }
        
        // Additional check: both shouldn't be LID format
        if (this.isLidFormat(normalizedPhone)) {
            console.warn(`[LidHandler] Attempted to store LID as phone number: [redacted] -> [redacted]`)
            return
        }

        const cleanedPushName = (typeof pushName === 'string' ? pushName.trim() : '') || undefined
        await this.withLidLock(normalizedLid, () =>
            this.writeLidOwnership(normalizedLid, normalizedPhone, cleanedPushName, options.authoritative === true)
        )
    }

    private async writeLidOwnership(lid: string, phone: string, pushName: string | undefined, authoritative: boolean): Promise<void> {
        const instanceId = this.instanceId
        // WAB-978: read the handles inside each attempt; a reconnect re-initializes them
        const contacts = () => this.contactsCollection!
        const mappings = () => this.lidMappingsCollection
        const now = new Date()
        const touchedKeys = [this.getRedisKey('lid', lid), this.getRedisKey('phone', phone)]
        let insertedMappingId: ObjectId | null = null
        let invalidated = false
        const begunKeys: string[] = []

        try {
            const [holder, target, mapping] = await Promise.all([
                this.runOperation(() => contacts().findOne({ instanceId, lid }, { projection: { _id: 1, id: 1 } })),
                this.runOperation(() => contacts().findOne({ instanceId, id: phone }, { projection: { _id: 1, pushName: 1, lid: 1 } })),
                mappings() ? this.runOperation(() => mappings()!.findOne({ instanceId, lid }, { projection: { _id: 1, phoneNumber: 1 } })) : Promise.resolve(null),
            ])
            const holderId: string | null = holder ? (holder.id ?? null) : null
            const holderIsSelfRef = !!holder && typeof holderId === 'string' && normalizeJidForStorage(holderId) === lid
            // Every holder except the target and a verified self-ref is an owner, whatever its id
            const holderIsOwner = !!holder && holderId !== phone && !holderIsSelfRef
            const contactOwner = holderIsOwner && typeof holderId === 'string' && isPhoneNumberFormat(holderId) ? holderId : null
            const mappedPhone = this.asPhoneJid(mapping?.phoneNumber)
            const mappingOwner = mappedPhone && mappedPhone !== phone ? mappedPhone : null

            if (!authoritative && (holderIsOwner || mappingOwner)) {
                reportBurst(LidHandler.KEPT_LABEL, holderIsOwner ? 'contact' : 'mapping')
                return
            }

            const setPushName = !!pushName && target?.pushName !== pushName
            const contactDone = target?.lid === lid && !setPushName
            const mappingDone = mappedPhone === phone
            if (contactDone && mappingDone) return

            for (const displaced of [contactOwner, mappingOwner]) {
                if (displaced && isPhoneNumberFormat(displaced)) touchedKeys.push(this.getRedisKey('phone', normalizeJidForStorage(displaced)))
            }
            invalidated = true
            if (this.config.enableCache) await this.beginCacheWrite(touchedKeys, begunKeys)
            else this.startKeyWrites(touchedKeys)

            // The mapping write is the cutover: lookups read lidMappings first. It also runs when the
            // mapping already agreed but contact work remains, because another writer can move the
            // mapping after our reads (ordinary: the conditional upsert stops; authoritative: last wins).
            if (mappings() && (!mappingDone || !contactDone)) {
                const filter = authoritative
                    ? { instanceId, lid }
                    : { instanceId, lid, $or: [{ phoneNumber: phone }, { phoneNumber: { $not: LidHandler.PHONE_JID_PATTERN } }] }
                try {
                    const result = await this.runOperation(() => mappings()!.updateOne(
                        filter,
                        { $set: { phoneNumber: phone, updatedAt: now }, $setOnInsert: { instanceId, lid, firstSeen: now } },
                        { upsert: true }
                    ))
                    insertedMappingId = result.upsertedId ?? null
                } catch (e: any) {
                    // Another writer stored a different phone for this LID after our checks
                    if (!authoritative && e?.code === 11000) {
                        reportBurst(LidHandler.KEPT_LABEL, 'mapping')
                        return
                    }
                    throw e
                }
            }

            const set = { lid, updatedAt: now, ...(setPushName ? { pushName, pushNameUpdatedAt: now } : {}) }
            // The pushName applies only to this call's own phone; a later writer's phone gets the LID only
            const assign = async (target: string = phone): Promise<void> => {
                const targetSet = target === phone ? set : { lid, updatedAt: now }
                try {
                    await this.runOperation(() => contacts().updateOne(
                        { instanceId, id: target },
                        { $set: targetSet, $setOnInsert: { instanceId, id: target, lidFirstSeen: now } },
                        { upsert: true }
                    ))
                } catch (e: any) {
                    // A concurrent insert of the same contact id: update it once without upsert.
                    // A conflict on the LID key is never retried here.
                    const keyPattern = e?.keyPattern
                    if (e?.code !== 11000 || !keyPattern || !('id' in keyPattern) || 'lid' in keyPattern) throw e
                    await this.runOperation(() => contacts().updateOne({ instanceId, id: target }, { $set: targetSet }))
                }
            }

            if (authoritative) {
                if (mappings()) await this.reconcileAuthoritativeContact(lid, phone, now, assign)
                else if (!contactDone) await assign()
            } else {
                if (holder && holderId !== phone) {
                    const released = await this.runOperation(() => contacts().updateOne(
                        { _id: holder._id, instanceId, lid, id: holderId },
                        { $unset: { lid: 1 }, $set: { updatedAt: now } }
                    ))
                    // The holder changed after our reads (for example a re-key): stop, never retry
                    if (released.matchedCount !== 1) throw Object.assign(new Error('LID holder changed'), { name: 'HolderChanged' })
                }
                if (!contactDone) await assign()
            }

            // No cache publication here: the next lookup caches the new pair through the fence
            console.log(`[LidHandler] Stored mapping in contacts: [redacted] -> [redacted]`)
        } catch (error) {
            const insertedId = insertedMappingId
            // An authoritative mapping write is the cutover and stays. An ordinary write removes the row
            // it inserted, so lidMappings does not disagree with contacts.
            if (!authoritative && insertedId !== null && mappings()) {
                await this.runOperation(() => mappings()!.deleteOne({ _id: insertedId, instanceId, lid, phoneNumber: phone }))
                    .catch((cleanupError) => console.debug('[LidHandler] Failed to remove the new lidMappings entry:', errorKind(cleanupError)))
            }
            // WAB-978: one summary line per burst instead of one line per write
            reportBurst('[LidHandler] Failed to store LID mapping', errorKind(error))
        } finally {
            // A lookup that ran during this write must not cache what it read
            if (invalidated) {
                if (this.config.enableCache) await this.endCacheWrite(touchedKeys, begunKeys)
                else this.endKeyWrites(touchedKeys)
            }
        }
    }

    /**
     * WAB-1275: after an authoritative mapping write, make contacts agree with the mapping. Another
     * process can run its own authoritative write for the same LID at the same time, so every attempt
     * converges on the phone that the mapping names NOW (ours or a later writer's): release the current
     * holder (compare-and-set), assign that phone, then re-read the mapping. It returns only when the
     * mapping still names the holder after its last contact write, so the writer of the last contact
     * change always verifies it. At most five attempts, then it throws (the next event repairs).
     */
    private async reconcileAuthoritativeContact(lid: string, phone: string, now: Date, assign: (target: string) => Promise<void>): Promise<void> {
        const instanceId = this.instanceId
        const contacts = () => this.contactsCollection!
        const mappings = () => this.lidMappingsCollection!
        const mappedPhone = async () => this.asPhoneJid((await this.runOperation(() =>
            mappings().findOne({ instanceId, lid }, { projection: { phoneNumber: 1 } })))?.phoneNumber)
        for (let attempt = 1; attempt <= 5; attempt++) {
            const target = await mappedPhone()
            if (!target) return // no valid mapping: nothing to converge on
            if (target !== phone) reportBurst(LidHandler.KEPT_LABEL, 'superseded')
            const holder = await this.runOperation(() => contacts().findOne({ instanceId, lid }, { projection: { _id: 1, id: 1 } }))
            if (holder?.id !== target) {
                if (holder) {
                    const released = await this.runOperation(() => contacts().updateOne(
                        { _id: holder._id, instanceId, lid, id: holder.id ?? null },
                        { $unset: { lid: 1 }, $set: { updatedAt: now } }
                    ))
                    if (released.matchedCount !== 1) continue
                }
                try {
                    await assign(target)
                } catch (e: any) {
                    if (e?.code !== 11000) throw e
                    continue
                }
            }
            // Verify after the contact step: a writer that changed the mapping meanwhile is converged next
            if (await mappedPhone() === target) return
        }
        throw Object.assign(new Error('LID ownership race not settled'), { name: 'OwnershipRace' })
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
            if (cached === LidHandler.NEGATIVE_CACHE_SENTINEL) return null
            // WAB-1248: older releases cached a LID as the phone; treat that as a miss
            if (cached !== undefined && isPhoneNumberFormat(cached)) return cached
        }

        if (this.pendingLidLookups.has(normalizedLid)) {
            return this.pendingLidLookups.get(normalizedLid)!
        }

        // WAB-1248: only a phone JID is a valid answer (self-referencing @lid contacts and lid->lid rows exist)
        const asPhone = (jid: string | null | undefined): string | null => {
            const normalized = jid ? normalizeJidForStorage(jid) : null
            return normalized && isPhoneNumberFormat(normalized) ? normalized : null
        }

        // WAB-1275: an ownership write during this lookup marks the fence; then the result is not cached
        const fence = this.beginFence(cacheKey)
        const lookupPromise = this.runWithLimiter(async () => {
            await this.readFenceGeneration(cacheKey, fence)
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
                            phoneNumber = asPhone(reverse)
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
                            const mappedPhone = asPhone(mapping?.phoneNumber)
                            if (mappedPhone) {
                                phoneNumber = mappedPhone
                                if (this.config.autoFillFromLegacy) {
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
                            const contactPhone = asPhone(contact?.id)
                            if (contactPhone) {
                                phoneNumber = contactPhone
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
            // WAB-1275: cache only this lookup's own key; the reverse key is written by its own lookup
            if (this.config.enableCache && !fence.stale) {
                if (result) {
                    await this.publishLookup(cacheKey, result, this.config.cacheTTLSeconds, fence)
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
                    await this.publishLookup(cacheKey, LidHandler.NEGATIVE_CACHE_SENTINEL, ttl, fence)
                }
            }
            return result
        } finally {
            this.endFence(cacheKey, fence)
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

        // WAB-1275: see getPhoneNumberFromLid
        const fence = this.beginFence(cacheKey)
        const lookupPromise = this.runWithLimiter(async () => {
            await this.readFenceGeneration(cacheKey, fence)
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
            if (this.config.enableCache && !fence.stale) {
                if (result) {
                    await this.publishLookup(cacheKey, result, this.config.cacheTTLSeconds, fence)
                } else {
                    await this.publishLookup(cacheKey, LidHandler.NEGATIVE_CACHE_SENTINEL, this.negativeCacheTTL, fence)
                }
            }
            return result
        } finally {
            this.endFence(cacheKey, fence)
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
        console.log(`[LidHandler] storeDiscoveredMapping called with: jid1="[redacted]", jid2="[redacted]"`)
        
        const pair = extractLidPhonePair(jid1, jid2)
        if (!pair) {
            console.log(`[LidHandler] Failed to extract LID-phone pair from: "[redacted]" and "[redacted]"`)
            
            // Additional debugging to understand why it failed
            if (process.env.DEBUG_LID === 'true') {
                console.log(`[LidHandler] Debug info:`)
                console.log(`  - jid1 isLid: ${this.isLidFormat(jid1)}, isPhone: ${isPhoneNumberFormat(jid1)}`)
                console.log(`  - jid2 isLid: ${this.isLidFormat(jid2)}, isPhone: ${isPhoneNumberFormat(jid2)}`)
                console.log(`  - areJidsEquivalent: ${areJidsEquivalent(jid1, jid2)}`)
            }
            return false
        }
        
        console.log(`[LidHandler] Successfully extracted mapping: [redacted] <-> [redacted]`)
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
        console.log(`[LidHandler] Searching for phone number from sent message with LID: [redacted]`)
        
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
                        console.log(`[LidHandler] Found phone number in remoteJid (addressingMode=pn): [redacted]`)
                    }
                    // LEGACY: First try senderPn
                    else if ((receivedMessage.key as any)?.senderPn && !this.isLidFormat((receivedMessage.key as any).senderPn)) {
                        phoneNumber = (receivedMessage.key as any).senderPn
                        console.log(`[LidHandler] Found phone number in senderPn: [redacted]`)
                    }
                    // Then try remoteJid if it's not a LID
                    else if (receivedMessage.key?.remoteJid && !this.isLidFormat(receivedMessage.key.remoteJid)) {
                        phoneNumber = receivedMessage.key.remoteJid
                        console.log(`[LidHandler] Found phone number in remoteJid: [redacted]`)
                    }
                    // Also check participant field
                    else if (receivedMessage.key?.participant && !this.isLidFormat(receivedMessage.key.participant)) {
                        phoneNumber = receivedMessage.key.participant
                        console.log(`[LidHandler] Found phone number in participant: [redacted]`)
                    }
                    
                    if (phoneNumber && isPhoneNumberFormat(phoneNumber)) {
                        // Store the discovered mapping
                        console.log(`[LidHandler] Discovered phone number [redacted] for LID [redacted] via reverse lookup`)
                        await this.storeLidMapping(normalizedLid, phoneNumber)
                        return phoneNumber
                    }
                } else {
                    console.log(`[LidHandler] No received messages found with senderLid/remoteJidAlt: [redacted]`)
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
        console.log(`[LidHandler] Attempting reverse lookup for LID: [redacted]`)
        
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
                        console.log(`[LidHandler] Reverse lookup found (new format, addressingMode=pn): [redacted] -> [redacted]`)
                        return message.key.remoteJid
                    }
                    if (addressingMode === 'lid' && remoteJidAlt && !this.isLidFormat(remoteJidAlt)) {
                        console.log(`[LidHandler] Reverse lookup found (new format, addressingMode=lid): [redacted] -> [redacted]`)
                        return remoteJidAlt
                    }
                    
                    // LEGACY: Check senderPn
                    const senderPn = (message.key as any)?.senderPn
                    if (senderPn && !this.isLidFormat(senderPn)) {
                        console.log(`[LidHandler] Reverse lookup found (legacy): [redacted] -> [redacted]`)
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
     * WAB-859: move older messages stored under a LID to the phone jid, bounded and best-effort.
     *
     * - Selects only rows whose `jid` and `key.remoteJid` are both the LID.
     * - Each write repeats that identity and the observed `key.remoteJidAlt`, so a row that another
     *   writer changed after the scan is left alone (counted as skipped).
     * - A row whose `remoteJidAlt` names a different phone, or whose phone twin exists (E11000), is
     *   skipped. Other errors propagate to the caller, which saved the new message first.
     * - One deadline covers connection preparation, the scan and every write. When it expires, no
     *   further write starts.
     * - `remaining` counts scanned rows that were not processed; `complete` is false when more rows
     *   may exist. Leftover rows stay for the WAB-259 repair CLI.
     */
    async updateExistingMessages(
        lid: string,
        phoneNumber: string,
        budget: { maxRows?: number; maxMs?: number } = {}
    ): Promise<{ updated: number; skipped: number; remaining: number; complete: boolean }> {
        const counts = { updated: 0, skipped: 0, remaining: 0, complete: true }
        const phone = canonicalPhoneJid(phoneNumber)
        const normalizedLid = normalizeJidForStorage(lid)
        if (!phone || !this.isLidFormat(normalizedLid)) return counts
        const maxRows = budget.maxRows ?? 200
        const deadline = Date.now() + (budget.maxMs ?? 2000)
        const left = () => deadline - Date.now()
        let expired = false

        const CONNECTION_LOST = Symbol('connectionLost')
        const work = this.withConnectionCheck<void | typeof CONNECTION_LOST>(
            async () => {
                if (!this.messagesCollection) return
                if (expired || left() <= 0) {
                    counts.complete = false
                    return
                }
                const rows = await this.messagesCollection
                    .find(
                        { instanceId: this.instanceId, jid: normalizedLid, 'key.remoteJid': normalizedLid },
                        { projection: { _id: 1, 'key.remoteJidAlt': 1 }, limit: maxRows + 1, maxTimeMS: Math.max(1, left()) }
                    )
                    .toArray()
                if (rows.length > maxRows) {
                    counts.complete = false
                    rows.length = maxRows
                }

                for (let i = 0; i < rows.length; i++) {
                    if (expired || left() <= 0) {
                        counts.complete = false
                        counts.remaining += rows.length - i
                        break
                    }
                    const row = rows[i]
                    const alt = (row as any).key?.remoteJidAlt
                    if (alt !== undefined && alt !== null && alt !== normalizedLid && canonicalPhoneJid(alt) !== phone) {
                        counts.skipped++
                        continue
                    }
                    try {
                        const result = await this.messagesCollection.updateOne(
                            {
                                _id: row._id,
                                instanceId: this.instanceId,
                                jid: normalizedLid,
                                'key.remoteJid': normalizedLid,
                                'key.remoteJidAlt': alt === undefined ? { $exists: false } : alt
                            },
                            {
                                $set: {
                                    jid: phone,
                                    'key.remoteJid': phone,
                                    'key.remoteJidAlt': normalizedLid,
                                    'key.addressingMode': 'pn',
                                    'lidMapping.resolved': true,
                                    'lidMapping.resolvedAt': new Date(),
                                    updatedAt: new Date()
                                }
                            },
                            { maxTimeMS: Math.max(1, left()) }
                        )
                        if (result.modifiedCount === 1) counts.updated++
                        else counts.skipped++
                    } catch (error: any) {
                        if (error?.code !== 11000) throw error
                        counts.skipped++
                    }
                }
            },
            CONNECTION_LOST,
            'updateExistingMessages'
        )

        // The deadline also bounds connection preparation and a stalled driver call. After it
        // expires the loop above starts no further write; an in-flight guarded write may still land.
        let timer: NodeJS.Timeout | undefined
        const timeout = new Promise<'timeout'>((resolve) => {
            timer = setTimeout(() => resolve('timeout'), Math.max(0, left()))
            timer.unref?.()
        })
        try {
            const outcome = await Promise.race([work, timeout])
            if (outcome === 'timeout') {
                expired = true
                counts.complete = false
                work.catch(() => {})
            } else if (outcome === CONNECTION_LOST) {
                counts.complete = false
            }
        } finally {
            if (timer) clearTimeout(timer)
        }

        if (counts.updated || counts.skipped || !counts.complete) {
            console.log(`[LidHandler] Re-keyed ${counts.updated} older messages; skipped ${counts.skipped}; remaining ${counts.remaining}; complete ${counts.complete}`)
        }
        return counts
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
        
        console.log(`[LidHandler] Processing message ${message.key?.id}: [redacted]`)
        
        // Handle fromMe messages with LID that need reverse lookup
        if (lidInfo.needsReverseLookup && lidInfo.lid) {
            console.log(`[LidHandler] Performing reverse lookup for sent message with LID: [redacted]`)
            const discoveredPhone = await this.discoverPhoneFromSentMessage(lidInfo.lid)
            
            if (discoveredPhone) {
                console.log(`[LidHandler] Reverse lookup successful: [redacted] -> [redacted]`)
                lidInfo.phoneNumber = discoveredPhone
                normalizedJid = discoveredPhone
                // Outgoing message: do not persist pushName (it's our own display)
                await this.storeLidMapping(lidInfo.lid, discoveredPhone)
                mappingStored = true
            } else {
                // Try existing mapping as fallback
                const existingPhone = await this.getPhoneNumberFromLid(lidInfo.lid)
                if (existingPhone) {
                    console.log(`[LidHandler] Using existing mapping: [redacted] -> [redacted]`)
                    normalizedJid = existingPhone
                    lidInfo.phoneNumber = existingPhone
                } else {
                    console.log(`[LidHandler] No phone number found for sent message LID: [redacted]`)
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
                console.warn(`[LidHandler] Skipping invalid mapping where LID equals phone number: [redacted]`)
            } else if (this.isLidFormat(lidInfo.phoneNumber)) {
                console.warn(`[LidHandler] Skipping invalid mapping where phone number is also a LID: [redacted] -> [redacted]`)
            } else {
                console.log(`[LidHandler] Storing mapping: [redacted] -> [redacted]`)
                const fromMe = !!message.key?.fromMe
                const pushName = !fromMe ? ((message as any)?.pushName || (message as any)?.verifiedBizName) : undefined
                await this.storeLidMapping(lidInfo.lid, lidInfo.phoneNumber, pushName)
                mappingStored = true
                normalizedJid = lidInfo.phoneNumber
            }
        } 
        // If we only have LID, try to get phone number from database
        else if (lidInfo.lid) {
            console.log(`[LidHandler] Looking up existing mapping for LID: [redacted]`)
            const phoneNumber = await this.getPhoneNumberFromLid(lidInfo.lid)
            if (phoneNumber) {
                console.log(`[LidHandler] Found existing mapping: [redacted] -> [redacted]`)
                normalizedJid = phoneNumber
                lidInfo.phoneNumber = phoneNumber
            } else {
                console.log(`[LidHandler] No existing mapping found for LID: [redacted]`)
                // Keep the LID for now, might be resolved later
                normalizedJid = lidInfo.lid
            }
        } else {
            console.log(`[LidHandler] No LID found in message, using original JID: [redacted]`)
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

    // ==========================================
    // Baileys v7 Native LID Mapping Integration
    // ==========================================

    /**
     * Set the Baileys v7 native LID mapping store reference
     * This enables direct integration with Baileys' internal LID mapping
     * 
     * @param store - Baileys v7 LIDMappingStore (sock.signalRepository.lidMapping)
     */
    setBaileysLidMappingStore(store: BaileysLIDMappingStore | null): void {
        this.baileysLidMappingStore = store
        if (store) {
            console.log('[LidHandler] Baileys v7 native LID mapping store connected')
        }
    }

    /**
     * Get the Baileys v7 native LID mapping store reference
     */
    getBaileysLidMappingStore(): BaileysLIDMappingStore | null {
        return this.baileysLidMappingStore
    }

    /**
     * Check if Baileys v7 native store is available
     */
    hasNativeStore(): boolean {
        return this.baileysLidMappingStore !== null
    }

    /**
     * Sync a mapping from Baileys v7 native store to MongoDB
     * Returns the LID if found, null otherwise
     * 
     * @param pn - Phone number JID (@s.whatsapp.net)
     */
    async syncFromBaileysStore(pn: string): Promise<string | null> {
        if (!this.baileysLidMappingStore) {
            return null
        }

        try {
            const lid = await this.baileysLidMappingStore.getLIDForPN(pn)
            if (lid) {
                // Store in MongoDB for persistence
                await this.storeLidMapping(lid, pn)
                console.log(`[LidHandler] Synced from Baileys store: [redacted] -> [redacted]`)
                return lid
            }
        } catch (err) {
            console.debug('[LidHandler] syncFromBaileysStore failed:', (err as Error).message)
        }

        return null
    }

    /**
     * Batch sync multiple PNs from Baileys v7 native store to MongoDB
     * 
     * @param pns - Array of phone number JIDs (@s.whatsapp.net)
     * @returns Map of PN -> LID for successfully synced mappings
     */
    async batchSyncFromBaileysStore(pns: string[]): Promise<Map<string, string>> {
        const result = new Map<string, string>()
        
        if (!this.baileysLidMappingStore || pns.length === 0) {
            return result
        }

        try {
            const mappings = await this.baileysLidMappingStore.getLIDsForPNs(pns)
            if (mappings) {
                for (const mapping of mappings) {
                    const { pn, lid } = mapping
                    result.set(pn, lid)
                    
                    // Store in MongoDB for persistence
                    try {
                        await this.storeLidMapping(lid, pn)
                    } catch (err) {
                        console.debug('[LidHandler] Failed to persist mapping:', (err as Error).message)
                    }
                }
                
                if (result.size > 0) {
                    console.log(`[LidHandler] Batch synced ${result.size} mappings from Baileys store`)
                }
            }
        } catch (err) {
            console.debug('[LidHandler] batchSyncFromBaileysStore failed:', (err as Error).message)
        }

        return result
    }

    /**
     * Sync a mapping to Baileys v7 native store from MongoDB
     * Useful for restoring mappings after reconnection
     * 
     * @param lid - LID JID (@lid)
     * @param phoneNumber - Phone number JID (@s.whatsapp.net)
     */
    async syncToBaileysStore(lid: string, phoneNumber: string): Promise<boolean> {
        if (!this.baileysLidMappingStore) {
            return false
        }

        try {
            // Use storeLIDPNMappings for batch efficiency (single item)
            if (this.baileysLidMappingStore.storeLIDPNMappings) {
                await this.baileysLidMappingStore.storeLIDPNMappings([{ lid, pn: phoneNumber }])
                console.log(`[LidHandler] Synced to Baileys store: [redacted] -> [redacted]`)
                return true
            }
        } catch (err) {
            console.debug('[LidHandler] syncToBaileysStore failed:', (err as Error).message)
        }

        return false
    }

    /**
     * Sync all MongoDB mappings to Baileys v7 native store
     * Useful for restoring all mappings on reconnection
     * 
     * @returns Number of mappings successfully synced
     */
    async syncAllToBaileysStore(): Promise<{ synced: number; failed: number }> {
        let synced = 0
        let failed = 0
        
        if (!this.baileysLidMappingStore?.storeLIDPNMappings) {
            return { synced, failed }
        }

        try {
            const allMappings = await this.getAllMappings()
            if (allMappings.length === 0) {
                return { synced, failed }
            }

            // Convert to Baileys format and batch sync
            const baileysFormat: BaileysLIDMapping[] = allMappings.map(m => ({
                lid: m.lid,
                pn: m.phoneNumber
            }))

            try {
                await this.baileysLidMappingStore.storeLIDPNMappings(baileysFormat)
                synced = allMappings.length
                console.log(`[LidHandler] Synced ${synced} mappings to Baileys store`)
            } catch (err) {
                console.error('[LidHandler] Batch sync to Baileys store failed:', (err as Error).message)
                failed = allMappings.length
            }
        } catch (err) {
            console.error('[LidHandler] Failed to get mappings for sync:', (err as Error).message)
        }

        return { synced, failed }
    }

    /**
     * Handle lid-mapping.update event from Baileys v7
     * Stores the mapping in MongoDB
     * 
     * @param data - Event data containing lid and pn
     */
    async handleLidMappingUpdate(data: { lid: string; pn: string }): Promise<void> {
        const { lid, pn } = data
        
        if (!lid || !pn) {
            console.debug('[LidHandler] Invalid lid-mapping.update data:', data)
            return
        }

        console.log(`[LidHandler] Received lid-mapping.update: [redacted] -> [redacted]`)
        
        // WAB-1275: the only authoritative source; it may move the LID from another owner
        await this.storeLidMapping(lid, pn, undefined, { authoritative: true })
    }

    /**
     * Get LID for a phone number, trying Baileys v7 native store first
     * Falls back to MongoDB if native store doesn't have the mapping
     * 
     * @param pn - Phone number JID (@s.whatsapp.net)
     */
    async getLidWithNativeFallback(pn: string): Promise<string | null> {
        // Try native store first if available
        if (this.baileysLidMappingStore) {
            try {
                const lid = await this.baileysLidMappingStore.getLIDForPN(pn)
                if (lid) {
                    // Also persist to MongoDB
                    try {
                        await this.storeLidMapping(lid, pn)
                    } catch {
                        // Ignore persistence errors
                    }
                    return lid
                }
            } catch (err) {
                console.debug('[LidHandler] Native getLIDForPN failed:', (err as Error).message)
            }
        }

        // Fall back to MongoDB
        return this.getLidFromPhoneNumber(pn)
    }

    /**
     * Get phone number for a LID, trying Baileys v7 native store first
     * Falls back to MongoDB if native store doesn't have the mapping
     * 
     * @param lid - LID JID (@lid)
     */
    async getPhoneWithNativeFallback(lid: string): Promise<string | null> {
        // Try native store first if available
        if (this.baileysLidMappingStore) {
            try {
                const pn = await this.baileysLidMappingStore.getPNForLID(lid)
                if (pn) {
                    // Also persist to MongoDB
                    try {
                        await this.storeLidMapping(lid, pn)
                    } catch {
                        // Ignore persistence errors
                    }
                    return pn
                }
            } catch (err) {
                console.debug('[LidHandler] Native getPNForLID failed:', (err as Error).message)
            }
        }

        // Fall back to MongoDB
        return this.getPhoneNumberFromLid(lid)
    }
}
