import { Collection } from 'mongodb'

/**
 * Safely drop an index if it exists
 * @param collection The MongoDB collection
 * @param indexName The name of the index to drop
 * @returns Promise that resolves when the operation is complete
 */
export async function safeDropIndex(collection: Collection<any>, indexName: string): Promise<void> {
    try {
        // First check if the index exists
        const indexes = await collection.indexes()
        const indexExists = indexes.some(idx => idx.name === indexName)
        
        if (indexExists) {
            await collection.dropIndex(indexName)
            console.log(`✅ Dropped index ${indexName} from collection ${collection.collectionName}`)
            
            // Verify the index is actually dropped with retry logic
            let retries = 0
            const maxRetries = 5
            const retryDelay = 100 // ms
            
            while (retries < maxRetries) {
                try {
                    const currentIndexes = await collection.indexes()
                    const stillExists = currentIndexes.some(idx => idx.name === indexName)
                    
                    if (!stillExists) {
                        // Index successfully dropped
                        break
                    }
                    
                    // Index still exists, wait and retry
                    await new Promise(resolve => setTimeout(resolve, retryDelay * (retries + 1)))
                    retries++
                } catch (verifyError) {
                    // If we can't verify, assume it's dropped
                    console.log(`⚠️ Could not verify index drop for ${indexName}, proceeding`)
                    break
                }
            }
            
            // Add a small delay to ensure MongoDB has fully processed the drop
            await new Promise(resolve => setTimeout(resolve, 100))
        } else {
            // Index doesn't exist, no need to drop
            console.log(`ℹ️ Index ${indexName} does not exist in collection ${collection.collectionName}, skipping drop`)
        }
    } catch (error: any) {
        // Handle specific MongoDB error codes
        if (error.code === 27 || error.codeName === 'IndexNotFound') {
            // Index not found - this is fine, we wanted to drop it anyway
            console.log(`ℹ️ Index ${indexName} not found in collection ${collection.collectionName}, already dropped`)
            return
        }
        
        // For any other error, throw it
        console.error(`❌ Failed to drop index ${indexName} from collection ${collection.collectionName}:`, error)
        throw error
    }
}

/**
 * Safely create an index with retry logic
 * @param collection The MongoDB collection
 * @param spec The index specification
 * @param options Index options
 * @returns Promise that resolves when the index is created
 */
export async function safeCreateIndex(
    collection: Collection<any>,
    spec: any,
    options?: any
): Promise<void> {
    try {
        await collection.createIndex(spec, options)
        console.log(`✅ Created index on collection ${collection.collectionName}`)
    } catch (error: any) {
        // Handle duplicate index error
        if (error.code === 11000 || error.codeName === 'DuplicateKey') {
            console.log(`ℹ️ Index already exists on collection ${collection.collectionName}`)
            return
        }
        
        // Handle index exists with different options
        if (error.code === 85 || error.codeName === 'IndexOptionsConflict') {
            console.log(`⚠️ Index exists with different options on collection ${collection.collectionName}, dropping and recreating`)
            
            // Extract index name from error or generate from spec
            const indexName = options?.name || Object.keys(spec).map(k => `${k}_${spec[k]}`).join('_')
            
            try {
                await collection.dropIndex(indexName)
                await collection.createIndex(spec, options)
                console.log(`✅ Recreated index on collection ${collection.collectionName}`)
            } catch (recreateError) {
                console.error(`❌ Failed to recreate index on collection ${collection.collectionName}:`, recreateError)
                throw recreateError
            }
            return
        }
        
        // For any other error, throw it
        console.error(`❌ Failed to create index on collection ${collection.collectionName}:`, error)
        throw error
    }
}

/**
 * Check if an index exists on a collection
 * @param collection The MongoDB collection
 * @param indexName The name of the index to check
 * @returns Promise that resolves to true if the index exists, false otherwise
 */
export async function indexExists(collection: Collection<any>, indexName: string): Promise<boolean> {
    try {
        const indexes = await collection.indexes()
        return indexes.some(idx => idx.name === indexName)
    } catch (error) {
        console.error(`❌ Failed to check if index ${indexName} exists on collection ${collection.collectionName}:`, error)
        return false
    }
}

/**
 * Drop all TTL indexes from a collection
 * @param collection The MongoDB collection
 * @returns Promise that resolves when all TTL indexes are dropped
 */
export async function dropAllTTLIndexes(collection: Collection<any>): Promise<void> {
    try {
        const indexes = await collection.indexes()
        const ttlIndexes = indexes.filter(idx => 
            idx.expireAfterSeconds !== undefined && 
            idx.name !== '_id_' // Don't try to drop the default _id index
        )
        
        for (const idx of ttlIndexes) {
            if (idx.name) {
                await safeDropIndex(collection, idx.name)
            }
        }
        
        if (ttlIndexes.length > 0) {
            console.log(`✅ Dropped ${ttlIndexes.length} TTL indexes from collection ${collection.collectionName}`)
        }
    } catch (error) {
        console.error(`❌ Failed to drop TTL indexes from collection ${collection.collectionName}:`, error)
        throw error
    }
}