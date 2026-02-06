/**
 * TTL (Time To Live) monitoring and verification utilities
 */

import { Db } from 'mongodb'

export interface TTLConfig {
    /**
     * TTL in days
     */
    days: number
    
    /**
     * Enable TTL monitoring
     */
    enableMonitoring?: boolean
    
    /**
     * Check interval in minutes
     */
    checkIntervalMinutes?: number
    
    /**
     * Alert threshold - alert if data older than TTL + threshold days exists
     */
    alertThresholdDays?: number

    /**
     * Optional collection name prefix used by the store (e.g., 'wabot_')
     * When provided, the monitor will look up collections using this prefix
     * while reporting friendly (unprefixed) names in warnings/metrics.
     */
    collectionPrefix?: string

    /**
     * List of logical collection names to check (without prefix).
     * If omitted, falls back to the legacy full set of collections.
     * Stores should pass only TTL-managed collections to reduce noise.
     */
    collectionsToCheck?: string[]

    /**
     * When true, only emit critical alerts (e.g., expired documents or very old data),
     * and suppress non-critical warnings such as missing TTL index messages.
     */
    onlyCriticalAlerts?: boolean

    /**
     * Enable sorted lookup for the oldest document in each collection to compute age.
     * This performs an indexed sorted read; keep disabled by default to minimize scans.
     */
    enableOldestDocumentLookup?: boolean
}

export interface TTLIndexInfo {
    collection: string
    field: string
    ttlSeconds: number
    exists: boolean
    isValid: boolean
}

export interface TTLVerificationResult {
    collection: string
    totalDocuments: number
    expiredDocuments: number
    oldestDocument?: {
        id: string
        age: number
        updatedAt: Date
    }
    ttlIndexExists: boolean
    ttlWorking: boolean
}

export interface TTLMetrics {
    lastCheck: Date
    collectionsChecked: number
    totalExpiredDocuments: number
    failedCollections: string[]
    warnings: string[]
}

/**
 * TTL Monitor for verifying and monitoring TTL indexes
 */
export class TTLMonitor {
    private db: Db
    private config: {
        days: number
        enableMonitoring: boolean
        checkIntervalMinutes: number
        alertThresholdDays: number
        collectionPrefix?: string
        collectionsToCheck?: string[]
        onlyCriticalAlerts?: boolean
        enableOldestDocumentLookup?: boolean
    }
    private metrics: TTLMetrics = {
        lastCheck: new Date(),
        collectionsChecked: 0,
        totalExpiredDocuments: 0,
        failedCollections: [],
        warnings: []
    }
    private checkInterval: NodeJS.Timeout | null = null
    
    constructor(db: Db, config: TTLConfig) {
        this.db = db
        this.config = {
            days: config.days,
            enableMonitoring: config.enableMonitoring ?? true,
            checkIntervalMinutes: config.checkIntervalMinutes ?? 60,
            alertThresholdDays: config.alertThresholdDays ?? 1,
            collectionPrefix: config.collectionPrefix,
            collectionsToCheck: config.collectionsToCheck,
            onlyCriticalAlerts: config.onlyCriticalAlerts,
            enableOldestDocumentLookup: config.enableOldestDocumentLookup ?? false
        }
    }
    
    /**
     * Start monitoring TTL indexes
     */
    startMonitoring(onAlert?: (message: string) => void): void {
        if (!this.config.enableMonitoring) {
            return
        }
        
        // Initial check
        this.performTTLCheck(onAlert)
        
        // Schedule periodic checks
        this.checkInterval = setInterval(() => {
            this.performTTLCheck(onAlert)
        }, this.config.checkIntervalMinutes * 60 * 1000)
    }
    
    /**
     * Stop monitoring
     */
    stopMonitoring(): void {
        if (this.checkInterval) {
            clearInterval(this.checkInterval)
            this.checkInterval = null
        }
    }
    
    /**
     * Verify TTL index exists and is configured correctly
     */
    async verifyTTLIndex(collectionName: string, fieldName: string = 'updatedAt'): Promise<TTLIndexInfo> {
        try {
            const collection = this.db.collection(collectionName)
            const indexes = await collection.indexes()
            
            const ttlIndex = indexes.find(index => 
                index.key[fieldName] === 1 && 
                index.expireAfterSeconds !== undefined
            )
            
            if (!ttlIndex) {
                return {
                    collection: collectionName,
                    field: fieldName,
                    ttlSeconds: 0,
                    exists: false,
                    isValid: false
                }
            }
            
            const expectedTTL = this.config.days * 24 * 60 * 60
            const actualTTL = ttlIndex.expireAfterSeconds || 0
            const tolerance = 60 // 1 minute tolerance
            
            return {
                collection: collectionName,
                field: fieldName,
                ttlSeconds: actualTTL,
                exists: true,
                isValid: Math.abs(actualTTL - expectedTTL) <= tolerance
            }
        } catch (error) {
            return {
                collection: collectionName,
                field: fieldName,
                ttlSeconds: 0,
                exists: false,
                isValid: false
            }
        }
    }
    
    /**
     * Create or update TTL index
     */
    async ensureTTLIndex(
        collectionName: string, 
        fieldName: string = 'updatedAt',
        ttlDays?: number
    ): Promise<void> {
        const collection = this.db.collection(collectionName)
        const ttlSeconds = (ttlDays || this.config.days) * 24 * 60 * 60
        
        // Drop existing TTL index if any
        const indexes = await collection.indexes()
        const existingTTL = indexes.find(index => 
            index.key[fieldName] === 1 && 
            index.expireAfterSeconds !== undefined
        )
        
        if (existingTTL) {
            await collection.dropIndex(existingTTL.name!)
        }
        
        // Create new TTL index
        await collection.createIndex(
            { [fieldName]: 1 },
            { expireAfterSeconds: ttlSeconds }
        )
    }
    
    /**
     * Check for expired documents in a collection
     */
    async checkExpiredDocuments(
        collectionName: string,
        fieldName: string = 'updatedAt'
    ): Promise<TTLVerificationResult> {
        try {
            // Support prefixed collection names when checking
            const dbCollectionName = `${this.config.collectionPrefix ?? ''}${collectionName}`
            const collection = this.db.collection(dbCollectionName)
            const ttlInfo = await this.verifyTTLIndex(dbCollectionName, fieldName)
            
            // Calculate expiry date
            const expiryDate = new Date()
            expiryDate.setDate(expiryDate.getDate() - this.config.days)
            
            // Count total documents using metadata to avoid collection scan
            const totalDocuments = await collection.estimatedDocumentCount()
            
            // Count expired documents; hint TTL field index when present
            const expiredDocuments = await collection.countDocuments(
                { [fieldName]: { $lt: expiryDate } },
                { hint: { [fieldName]: 1 } as any }
            )
            
            // Optionally find oldest document (guarded)
            let oldestDoc: any = null
            if (this.config.enableOldestDocumentLookup) {
                const cursor = collection.find({}, { projection: { [fieldName]: 1 } }).sort({ [fieldName]: 1 }).limit(1)
                try {
                    // Best-effort hint
                    (cursor as any).hint({ [fieldName]: 1 })
                } catch (_e) {
                    // ignore hint errors
                }
                oldestDoc = await cursor.next()
            }
            
            let oldestDocument
            if (oldestDoc && oldestDoc[fieldName]) {
                const age = Math.floor((Date.now() - oldestDoc[fieldName].getTime()) / (1000 * 60 * 60 * 24))
                oldestDocument = {
                    id: oldestDoc._id.toString(),
                    age,
                    updatedAt: oldestDoc[fieldName]
                }
            }
            
            return {
                // Report friendly (unprefixed) name in results
                collection: collectionName,
                totalDocuments,
                expiredDocuments,
                oldestDocument,
                ttlIndexExists: ttlInfo.exists,
                ttlWorking: ttlInfo.exists && expiredDocuments === 0
            }
        } catch (error) {
            return {
                collection: collectionName,
                totalDocuments: 0,
                expiredDocuments: 0,
                ttlIndexExists: false,
                ttlWorking: false
            }
        }
    }
    
    /**
     * Perform TTL check on all collections
     */
    private async performTTLCheck(onAlert?: (message: string) => void): Promise<void> {
        const collections = this.config.collectionsToCheck ?? [
            // Legacy default: full set (may include non-TTL collections)
            'chats', 'contacts', 'messages', 'groupMetadata',
            'state', 'presences', 'labels', 'labelAssociations'
        ]
        
        this.metrics = {
            lastCheck: new Date(),
            collectionsChecked: 0,
            totalExpiredDocuments: 0,
            failedCollections: [],
            warnings: []
        }
        
        for (const collectionName of collections) {
            try {
                const result = await this.checkExpiredDocuments(collectionName)
                this.metrics.collectionsChecked++
                
                if (!result.ttlIndexExists) {
                    this.metrics.warnings.push(`TTL index missing for ${collectionName}`)
                    if (onAlert && !this.config.onlyCriticalAlerts) {
                        onAlert(`Warning: TTL index missing for collection ${collectionName}`)
                    }
                }
                
                if (result.expiredDocuments > 0) {
                    this.metrics.totalExpiredDocuments += result.expiredDocuments
                    this.metrics.warnings.push(
                        `${result.expiredDocuments} expired documents in ${collectionName}`
                    )
                    
                    if (onAlert) {
                        onAlert(
                            `Alert: ${result.expiredDocuments} documents older than ${this.config.days} days ` +
                            `found in ${collectionName}. TTL may not be working properly.`
                        )
                    }
                }
                
                if (this.config.enableOldestDocumentLookup && result.oldestDocument && 
                    result.oldestDocument.age > this.config.days + this.config.alertThresholdDays) {
                    this.metrics.warnings.push(
                        `Very old document (${result.oldestDocument.age} days) in ${collectionName}`
                    )
                    
                    if (onAlert) {
                        onAlert(
                            `Critical: Document ${result.oldestDocument.id} in ${collectionName} ` +
                            `is ${result.oldestDocument.age} days old (expected max: ${this.config.days} days)`
                        )
                    }
                }
            } catch (error) {
                this.metrics.failedCollections.push(collectionName)
            }
        }
    }
    
    /**
     * Get current metrics
     */
    getMetrics(): TTLMetrics {
        return { ...this.metrics }
    }
    
    /**
     * Manually cleanup expired documents
     */
    async cleanupExpiredDocuments(
        collectionName: string,
        fieldName: string = 'updatedAt',
        dryRun: boolean = true
    ): Promise<{ deleted: number; dryRun: boolean }> {
        const collection = this.db.collection(collectionName)
        const expiryDate = new Date()
        expiryDate.setDate(expiryDate.getDate() - this.config.days)
        
        if (dryRun) {
            const count = await collection.countDocuments({
                [fieldName]: { $lt: expiryDate }
            })
            return { deleted: count, dryRun: true }
        }
        
        const result = await collection.deleteMany({
            [fieldName]: { $lt: expiryDate }
        })
        
        return { deleted: result.deletedCount, dryRun: false }
    }
    
    /**
     * Get TTL status report
     */
    async getTTLStatusReport(): Promise<{
        summary: {
            totalCollections: number
            collectionsWithTTL: number
            collectionsWithExpiredDocs: number
            totalExpiredDocuments: number
        }
        details: TTLVerificationResult[]
    }> {
        const collections = this.config.collectionsToCheck ?? [
            'chats', 'contacts', 'messages', 'groupMetadata',
            'state', 'presences', 'labels', 'labelAssociations'
        ]
        
        const details: TTLVerificationResult[] = []
        let collectionsWithTTL = 0
        let collectionsWithExpiredDocs = 0
        let totalExpiredDocuments = 0
        
        for (const collection of collections) {
            const result = await this.checkExpiredDocuments(collection)
            details.push(result)
            
            if (result.ttlIndexExists) collectionsWithTTL++
            if (result.expiredDocuments > 0) {
                collectionsWithExpiredDocs++
                totalExpiredDocuments += result.expiredDocuments
            }
        }
        
        return {
            summary: {
                totalCollections: collections.length,
                collectionsWithTTL,
                collectionsWithExpiredDocs,
                totalExpiredDocuments
            },
            details
        }
    }
}

/**
 * Create TTL cleanup job
 */
export function createTTLCleanupJob(
    db: Db,
    config: TTLConfig,
    logger?: (message: string) => void
): () => void {
    const monitor = new TTLMonitor(db, config)
    
    const cleanup = async () => {
        const report = await monitor.getTTLStatusReport()
        
        if (logger) {
            logger(`TTL Status Report:`)
            logger(`- Collections with TTL: ${report.summary.collectionsWithTTL}/${report.summary.totalCollections}`)
            logger(`- Expired documents: ${report.summary.totalExpiredDocuments}`)
            
            if (report.summary.collectionsWithExpiredDocs > 0) {
                logger(`Collections with expired documents:`)
                report.details
                    .filter(d => d.expiredDocuments > 0)
                    .forEach(d => {
                        logger(`  - ${d.collection}: ${d.expiredDocuments} expired docs`)
                    })
            }
        }
    }
    
    // Run cleanup immediately
    cleanup()
    
    // Schedule periodic cleanup
    const interval = setInterval(cleanup, config.checkIntervalMinutes! * 60 * 1000)
    
    // Return cleanup function
    return () => {
        clearInterval(interval)
        monitor.stopMonitoring()
    }
}
