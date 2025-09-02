import { Db, Collection } from 'mongodb'

interface IndexLock {
    instanceId: string
    collectionName: string
    lockId: string
    acquiredAt: Date
    expiresAt: Date
}

/**
 * Distributed lock manager for index operations
 * Prevents concurrent index modifications across multiple instances
 */
export class IndexLockManager {
    private locksCollection: Collection<IndexLock>
    private instanceId: string
    private lockTTL: number // in milliseconds
    
    constructor(db: Db, instanceId: string, collectionPrefix: string = 'baileys_', lockTTL: number = 30000) {
        this.instanceId = instanceId
        this.lockTTL = lockTTL
        this.locksCollection = db.collection<IndexLock>(`${collectionPrefix}index_locks`)
        
        // Create TTL index for automatic lock cleanup
        this.initializeLockCollection()
    }
    
    private async initializeLockCollection(): Promise<void> {
        try {
            // Create TTL index to auto-expire stale locks
            await this.locksCollection.createIndex(
                { expiresAt: 1 },
                { expireAfterSeconds: 0 } // Expire at the exact time specified in expiresAt
            )
            
            // Create unique index for lock identity
            await this.locksCollection.createIndex(
                { collectionName: 1 },
                { unique: true }
            )
        } catch (error: any) {
            // Ignore if indexes already exist
            if (error.code !== 11000 && error.code !== 85 && error.code !== 86) {
                console.warn('Failed to create index lock indexes:', error.message)
            }
        }
    }
    
    /**
     * Acquire a lock for index operations on a specific collection
     * @param collectionName The collection to lock
     * @param maxWaitTime Maximum time to wait for lock acquisition (ms)
     * @returns true if lock acquired, false otherwise
     */
    async acquireLock(collectionName: string, maxWaitTime: number = 10000): Promise<boolean> {
        const lockId = `${this.instanceId}_${Date.now()}_${Math.random()}`
        const startTime = Date.now()
        
        while (Date.now() - startTime < maxWaitTime) {
            try {
                const now = new Date()
                const expiresAt = new Date(now.getTime() + this.lockTTL)
                
                // Try to insert lock document
                await this.locksCollection.insertOne({
                    instanceId: this.instanceId,
                    collectionName,
                    lockId,
                    acquiredAt: now,
                    expiresAt
                })
                
                console.log(`🔒 Acquired index lock for ${collectionName} by instance ${this.instanceId}`)
                return true
            } catch (error: any) {
                // Lock exists, check if it's expired
                if (error.code === 11000) { // Duplicate key error
                    // Try to clean up expired lock
                    const deleted = await this.locksCollection.deleteOne({
                        collectionName,
                        expiresAt: { $lt: new Date() }
                    })
                    
                    if (deleted.deletedCount > 0) {
                        console.log(`🧹 Cleaned up expired lock for ${collectionName}`)
                        continue // Try again immediately
                    }
                    
                    // Lock is held by another instance, wait
                    const waitTime = Math.min(500 + Math.random() * 500, maxWaitTime - (Date.now() - startTime))
                    if (waitTime > 0) {
                        console.log(`⏳ Waiting ${Math.round(waitTime)}ms for lock on ${collectionName}`)
                        await new Promise(resolve => setTimeout(resolve, waitTime))
                    }
                } else {
                    console.error(`Failed to acquire lock for ${collectionName}:`, error)
                    return false
                }
            }
        }
        
        console.log(`⏱️ Timeout waiting for lock on ${collectionName}`)
        return false
    }
    
    /**
     * Release a lock for index operations on a specific collection
     * @param collectionName The collection to unlock
     */
    async releaseLock(collectionName: string): Promise<void> {
        try {
            const result = await this.locksCollection.deleteOne({
                collectionName,
                instanceId: this.instanceId
            })
            
            if (result.deletedCount > 0) {
                console.log(`🔓 Released index lock for ${collectionName} by instance ${this.instanceId}`)
            }
        } catch (error) {
            console.error(`Failed to release lock for ${collectionName}:`, error)
        }
    }
    
    /**
     * Execute an operation with distributed locking
     * @param collectionName The collection to lock
     * @param operation The operation to execute
     * @param maxWaitTime Maximum time to wait for lock acquisition (ms)
     */
    async withLock<T>(
        collectionName: string,
        operation: () => Promise<T>,
        maxWaitTime: number = 10000
    ): Promise<T> {
        const lockAcquired = await this.acquireLock(collectionName, maxWaitTime)
        
        if (!lockAcquired) {
            throw new Error(`Failed to acquire lock for ${collectionName} after ${maxWaitTime}ms`)
        }
        
        try {
            return await operation()
        } finally {
            await this.releaseLock(collectionName)
        }
    }
    
    /**
     * Clean up all expired locks
     */
    async cleanupExpiredLocks(): Promise<number> {
        try {
            const result = await this.locksCollection.deleteMany({
                expiresAt: { $lt: new Date() }
            })
            
            if (result.deletedCount > 0) {
                console.log(`🧹 Cleaned up ${result.deletedCount} expired index locks`)
            }
            
            return result.deletedCount
        } catch (error) {
            console.error('Failed to cleanup expired locks:', error)
            return 0
        }
    }
}