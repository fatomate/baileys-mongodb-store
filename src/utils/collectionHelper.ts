import { Collection, Db } from 'mongodb'

export interface IndexSpec {
    name: string
    spec: any
    options?: any
}

export interface IndexCheckResult {
    createAll: boolean
    missingIndexes: IndexSpec[]
    existingCount: number
    requiredCount: number
    collectionExists: boolean
}

// Cache for existing collections to avoid repeated database calls
let existingCollectionsCache: { collections: string[], timestamp: number } | null = null
const CACHE_DURATION = 30000 // 30 seconds

/**
 * Check if a specific collection exists in the database
 * @param db MongoDB database instance
 * @param collectionName Name of the collection to check
 * @returns Promise resolving to true if collection exists, false otherwise
 */
export async function checkCollectionExists(db: Db, collectionName: string): Promise<boolean> {
    try {
        const existingCollections = await getExistingCollections(db)
        return existingCollections.includes(collectionName)
    } catch (error) {
        console.warn(`⚠️ Failed to check collection existence for ${collectionName}, assuming it exists:`, error)
        // On error, assume collection exists to fall back to safe behavior
        return true
    }
}

/**
 * Get list of existing collections with caching for performance
 * @param db MongoDB database instance
 * @returns Promise resolving to array of collection names
 */
export async function getExistingCollections(db: Db): Promise<string[]> {
    const now = Date.now()
    
    // Return cached result if still valid
    if (existingCollectionsCache && (now - existingCollectionsCache.timestamp) < CACHE_DURATION) {
        return existingCollectionsCache.collections
    }
    
    try {
        const collections = await db.listCollections().toArray()
        const collectionNames = collections.map(col => col.name)
        
        // Update cache
        existingCollectionsCache = {
            collections: collectionNames,
            timestamp: now
        }
        
        return collectionNames
    } catch (error) {
        console.error(`❌ Failed to list collections:`, error)
        // Clear cache on error
        existingCollectionsCache = null
        throw error
    }
}

/**
 * Compare existing indexes with required indexes to find missing ones
 * @param existingIndexes Array of existing index objects from MongoDB
 * @param requiredIndexes Array of required index specifications
 * @returns Array of missing index specifications
 */
export function findMissingIndexes(existingIndexes: any[], requiredIndexes: IndexSpec[]): IndexSpec[] {
    const missingIndexes: IndexSpec[] = []
    
    for (const required of requiredIndexes) {
        // Check if an index with the same key pattern exists
        const exists = existingIndexes.some(existing => {
            // Skip the default _id index
            if (existing.name === '_id_') return false
            
            // Compare key patterns (the 'key' field in existing indexes matches 'spec' in required)
            return JSON.stringify(existing.key) === JSON.stringify(required.spec)
        })
        
        if (!exists) {
            missingIndexes.push(required)
        }
    }
    
    return missingIndexes
}

/**
 * Validate if existing index options match required options
 * Used for checking if index recreation is needed due to option changes
 * @param existingIndex Existing index object from MongoDB
 * @param requiredIndex Required index specification
 * @returns True if options match, false if recreation needed
 */
export function validateIndexOptions(existingIndex: any, requiredIndex: IndexSpec): boolean {
    // Compare critical options that would require index recreation
    const criticalOptions = ['unique', 'expireAfterSeconds', 'sparse', 'partialFilterExpression']
    
    for (const option of criticalOptions) {
        const existingValue = existingIndex[option]
        const requiredValue = requiredIndex.options?.[option]
        
        // Handle undefined/null comparisons
        if (existingValue !== requiredValue) {
            // Special handling for expireAfterSeconds (TTL) - allow some tolerance
            if (option === 'expireAfterSeconds' && typeof existingValue === 'number' && typeof requiredValue === 'number') {
                const tolerance = 60 // Allow 1 minute difference
                if (Math.abs(existingValue - requiredValue) <= tolerance) {
                    continue
                }
            }
            return false
        }
    }
    
    return true
}

/**
 * Enhanced missing index detection that also checks for option mismatches
 * @param existingIndexes Array of existing index objects from MongoDB
 * @param requiredIndexes Array of required index specifications
 * @returns Object containing missing indexes and indexes needing recreation
 */
export function findMissingIndexesEnhanced(existingIndexes: any[], requiredIndexes: IndexSpec[]): {
    missing: IndexSpec[],
    needRecreation: IndexSpec[]
} {
    const missing: IndexSpec[] = []
    const needRecreation: IndexSpec[] = []
    
    for (const required of requiredIndexes) {
        // Find existing index with same key pattern
        const existingIndex = existingIndexes.find(existing => {
            if (existing.name === '_id_') return false
            return JSON.stringify(existing.key) === JSON.stringify(required.spec)
        })
        
        if (!existingIndex) {
            missing.push(required)
        } else if (!validateIndexOptions(existingIndex, required)) {
            needRecreation.push(required)
        }
    }
    
    return { missing, needRecreation }
}

/**
 * Main function to determine what indexes need creation for a collection
 * @param collection MongoDB collection instance
 * @param requiredIndexes Array of required index specifications
 * @returns Promise resolving to IndexCheckResult with creation strategy
 */
export async function shouldCreateIndexes(collection: Collection<any>, requiredIndexes: IndexSpec[]): Promise<IndexCheckResult> {
    try {
        const db = (collection as any).db as Db
        const collectionName = collection.collectionName
        
        // Check if collection exists
        const collectionExists = await checkCollectionExists(db, collectionName)
        
        if (!collectionExists) {
            // New collection - create all indexes
            return {
                createAll: true,
                missingIndexes: requiredIndexes,
                existingCount: 0,
                requiredCount: requiredIndexes.length,
                collectionExists: false
            }
        }
        
        // Collection exists - check which indexes are missing
        const existingIndexes = await collection.indexes()
        const { missing, needRecreation } = findMissingIndexesEnhanced(existingIndexes, requiredIndexes)
        
        // Combine missing and recreation-needed indexes
        const allMissingIndexes = [...missing, ...needRecreation]
        
        return {
            createAll: false,
            missingIndexes: allMissingIndexes,
            existingCount: existingIndexes.length - 1, // Subtract 1 for _id index
            requiredCount: requiredIndexes.length,
            collectionExists: true
        }
    } catch (error) {
        console.warn(`⚠️ Failed to analyze indexes for collection ${collection.collectionName}, falling back to create all:`, error)
        
        // On error, fall back to creating all indexes (safe behavior)
        return {
            createAll: true,
            missingIndexes: requiredIndexes,
            existingCount: 0,
            requiredCount: requiredIndexes.length,
            collectionExists: false
        }
    }
}

/**
 * Clear the collection cache (useful for testing or manual cache invalidation)
 */
export function clearCollectionCache(): void {
    existingCollectionsCache = null
}

/**
 * Get cache status for debugging
 */
export function getCacheStatus(): { cached: boolean, age?: number, collections?: string[] } {
    if (!existingCollectionsCache) {
        return { cached: false }
    }
    
    return {
        cached: true,
        age: Date.now() - existingCollectionsCache.timestamp,
        collections: existingCollectionsCache.collections
    }
}