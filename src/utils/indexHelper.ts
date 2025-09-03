import { Collection } from 'mongodb'

/**
 * Find an index by its key pattern
 * @param collection The MongoDB collection
 * @param keyPattern The key pattern to match
 * @returns The name of the matching index, or null if not found
 */
async function findIndexByKeyPattern(collection: Collection<any>, keyPattern: any): Promise<string | null> {
    try {
        const indexes = await collection.indexes()
        for (const idx of indexes) {
            // Skip the default _id index
            if (idx.name === '_id_') continue
            
            // Compare key patterns
            if (JSON.stringify(idx.key) === JSON.stringify(keyPattern)) {
                return idx.name || null
            }
        }
    } catch (error) {
        console.error(`❌ Failed to find index by key pattern on collection ${collection.collectionName}:`, error)
    }
    return null
}

/**
 * Safely drop an index if it exists
 * @param collection The MongoDB collection
 * @param indexName The name of the index to drop
 * @returns Promise that resolves when the operation is complete
 */
export async function safeDropIndex(collection: Collection<any>, indexName: string): Promise<void> {
    const maxRetries = 3
    let retryCount = 0
    
    while (retryCount <= maxRetries) {
        try {
            // First check if the index exists
            const indexes = await collection.indexes()
            const indexExists = indexes.some(idx => idx.name === indexName)
            
            if (indexExists) {
                await collection.dropIndex(indexName)
                console.log(`✅ Dropped index ${indexName} from collection ${collection.collectionName}`)
                
                // Verify the index is actually dropped with retry logic
                let verifyRetries = 0
                const maxVerifyRetries = 5
                const retryDelay = 100 // ms
                
                while (verifyRetries < maxVerifyRetries) {
                    try {
                        const currentIndexes = await collection.indexes()
                        const stillExists = currentIndexes.some(idx => idx.name === indexName)
                        
                        if (!stillExists) {
                            // Index successfully dropped
                            break
                        }
                        
                        // Index still exists, wait and retry
                        await new Promise(resolve => setTimeout(resolve, retryDelay * (verifyRetries + 1)))
                        verifyRetries++
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
            return // Success
        } catch (error: any) {
            // Handle specific MongoDB error codes
            if (error.code === 27 || error.codeName === 'IndexNotFound') {
                // Index not found - this is fine, we wanted to drop it anyway
                console.log(`ℹ️ Index ${indexName} not found in collection ${collection.collectionName}, already dropped`)
                return
            }
            
            // Handle IndexBuildAborted - concurrent operations conflict
            if (error.code === 276 || error.codeName === 'IndexBuildAborted') {
                retryCount++
                if (retryCount <= maxRetries) {
                    // Exponential backoff: 1s, 2s, 4s
                    const backoffMs = Math.pow(2, retryCount - 1) * 1000
                    console.log(`⚠️ Index operation aborted when dropping ${indexName} from collection ${collection.collectionName}, retrying in ${backoffMs}ms (attempt ${retryCount}/${maxRetries})`)
                    await new Promise(resolve => setTimeout(resolve, backoffMs))
                    continue // Retry the operation
                } else {
                    console.error(`❌ Failed to drop index ${indexName} after ${maxRetries} retries from collection ${collection.collectionName}:`, error)
                    throw error
                }
            }
            
            // For any other error, throw it
            console.error(`❌ Failed to drop index ${indexName} from collection ${collection.collectionName}:`, error)
            throw error
        }
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
    const maxRetries = 3
    let retryCount = 0
    
    while (retryCount <= maxRetries) {
        try {
            await collection.createIndex(spec, options)
            console.log(`✅ Created index on collection ${collection.collectionName}`)
            return
        } catch (error: any) {
            // Handle duplicate key error - this indicates data integrity issues, not "index exists"
            if (error.code === 11000 || error.codeName === 'DuplicateKey') {
                console.error(`❌ Duplicate data prevents creating unique index on collection ${collection.collectionName}: ${error.message}`)
                throw error
            }
            
            // Handle index already exists error
            if (error.code === 68 || error.codeName === 'IndexAlreadyExists') {
                console.log(`ℹ️ Index already exists on collection ${collection.collectionName}`)
                return
            }
            
            // Handle cannot create index error
            if (error.code === 67 || error.codeName === 'CannotCreateIndex') {
                console.log(`⚠️ Cannot create index on collection ${collection.collectionName}: ${error.message}`)
                // This is usually a constraint violation - don't retry
                throw error
            }
            
            // Handle index exists with different options
            if (error.code === 85 || error.codeName === 'IndexOptionsConflict') {
                console.log(`⚠️ Index exists with different options on collection ${collection.collectionName}, finding and recreating`)
                
                try {
                    // First, try to find the actual conflicting index by key pattern
                    const existingIndexName = await findIndexByKeyPattern(collection, spec)
                    
                    if (existingIndexName) {
                        console.log(`📋 Found conflicting index: ${existingIndexName} on collection ${collection.collectionName}`)
                        await safeDropIndex(collection, existingIndexName)
                    } else {
                        // Fallback: If we can't find by pattern, try the provided name
                        // This handles cases where indexes might be in an inconsistent state
                        const indexName = options?.name || Object.keys(spec).map(k => `${k}_${spec[k]}`).join('_')
                        console.log(`⚠️ Could not find index by pattern, attempting to drop by name: ${indexName}`)
                        
                        // Use safeDropIndex which handles IndexNotFound gracefully
                        await safeDropIndex(collection, indexName)
                    }
                    
                    // Wait a bit for MongoDB to fully process the drop
                    await new Promise(resolve => setTimeout(resolve, 500))
                    
                    // Now create the new index with the desired options
                    await collection.createIndex(spec, options)
                    console.log(`✅ Successfully recreated index on collection ${collection.collectionName}`)
                    return
                } catch (recreateError: any) {
                    // If recreation fails due to IndexBuildAborted, retry
                    if (recreateError.code === 276 || recreateError.codeName === 'IndexBuildAborted') {
                        console.log(`⚠️ Index build aborted during recreation, will retry...`)
                        // Continue to retry logic below to trigger retry
                    } else if (recreateError.code === 27 || recreateError.codeName === 'IndexNotFound') {
                        // If index not found during drop, that's fine - just create it
                        console.log(`ℹ️ Index to drop not found, proceeding with creation on collection ${collection.collectionName}`)
                        try {
                            await collection.createIndex(spec, options)
                            console.log(`✅ Created index on collection ${collection.collectionName}`)
                            return
                        } catch (createError: any) {
                            console.error(`❌ Failed to create index after drop attempt on collection ${collection.collectionName}:`, createError)
                            throw createError
                        }
                    } else {
                        console.error(`❌ Failed to recreate index on collection ${collection.collectionName}:`, recreateError)
                        throw recreateError
                    }
                }
            }
            
            // Handle IndexKeySpecsConflict - index with same name but different spec
            if (error.code === 86 || error.codeName === 'IndexKeySpecsConflict') {
                console.log(`⚠️ Index conflict detected on collection ${collection.collectionName}, attempting to resolve...`)
                
                // Extract index name from error or generate from spec
                const indexName = options?.name || Object.keys(spec).map(k => `${k}_${spec[k]}`).join('_')
                
                try {
                    // First try to drop the conflicting index
                    await safeDropIndex(collection, indexName)
                    
                    // Wait a bit for MongoDB to process the drop
                    await new Promise(resolve => setTimeout(resolve, 500))
                    
                    // Retry creating the index
                    await collection.createIndex(spec, options)
                    console.log(`✅ Resolved index conflict and created index on collection ${collection.collectionName}`)
                    return
                } catch (resolveError: any) {
                    if (resolveError.code === 276 || resolveError.codeName === 'IndexBuildAborted') {
                        console.log(`⚠️ Index build aborted during conflict resolution, will retry...`)
                        // Continue to retry logic below
                    } else {
                        console.error(`❌ Failed to resolve index conflict on collection ${collection.collectionName}:`, resolveError)
                        throw resolveError
                    }
                }
            }
            
            // Handle write conflict error
            if (error.code === 112 || error.codeName === 'WriteConflict') {
                retryCount++
                if (retryCount <= maxRetries) {
                    // Short delay with jitter for write conflicts
                    const jitter = Math.random() * 200 // 0-200ms jitter
                    const backoffMs = 100 + jitter
                    console.log(`⚠️ Write conflict on collection ${collection.collectionName}, retrying in ${Math.round(backoffMs)}ms (attempt ${retryCount}/${maxRetries})`)
                    await new Promise(resolve => setTimeout(resolve, backoffMs))
                    continue // Retry the operation
                }
            }
            
            // Handle not primary/master error
            if (error.code === 13436 || error.codeName === 'NotMaster' || error.codeName === 'NotPrimaryOrSecondary') {
                retryCount++
                if (retryCount <= maxRetries) {
                    // Longer delay for primary election
                    const backoffMs = 1000 * retryCount
                    console.log(`⚠️ Not primary error on collection ${collection.collectionName}, retrying in ${backoffMs}ms (attempt ${retryCount}/${maxRetries})`)
                    await new Promise(resolve => setTimeout(resolve, backoffMs))
                    continue // Retry the operation
                }
            }
            
            // Handle IndexBuildAborted - concurrent operations conflict
            if (error.code === 276 || error.codeName === 'IndexBuildAborted') {
                retryCount++
                if (retryCount <= maxRetries) {
                    // Exponential backoff with jitter: 1s, 2s, 4s + random jitter
                    const jitter = Math.random() * 500 // 0-500ms jitter
                    const backoffMs = Math.pow(2, retryCount - 1) * 1000 + jitter
                    console.log(`⚠️ Index build aborted on collection ${collection.collectionName}, retrying in ${Math.round(backoffMs)}ms (attempt ${retryCount}/${maxRetries})`)
                    await new Promise(resolve => setTimeout(resolve, backoffMs))
                    continue // Retry the operation
                } else {
                    console.error(`❌ Failed to create index after ${maxRetries} retries on collection ${collection.collectionName}:`, error)
                    throw error
                }
            }
            
            // For any other error, throw it
            console.error(`❌ Failed to create index on collection ${collection.collectionName}:`, error)
            throw error
        }
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

/**
 * Batch create multiple indexes with proper error handling and logging
 * @param collection The MongoDB collection
 * @param indexSpecs Array of index specifications
 * @returns Promise resolving to batch creation results
 */
export async function batchCreateIndexes(collection: Collection<any>, indexSpecs: Array<{ name: string, spec: any, options?: any }>): Promise<{
    successful: number,
    failed: number,
    details: string[]
}> {
    const results = { successful: 0, failed: 0, details: [] as string[] }
    
    if (indexSpecs.length === 0) {
        results.details.push(`No indexes to create for collection ${collection.collectionName}`)
        return results
    }
    
    console.log(`📋 Creating ${indexSpecs.length} indexes for collection ${collection.collectionName}`)
    
    for (const indexSpec of indexSpecs) {
        try {
            await safeCreateIndex(collection, indexSpec.spec, indexSpec.options)
            results.successful++
            results.details.push(`✅ Created index: ${indexSpec.name}`)
        } catch (error) {
            results.failed++
            const errorMsg = `❌ Failed to create index ${indexSpec.name}: ${error instanceof Error ? error.message : String(error)}`
            results.details.push(errorMsg)
            console.error(errorMsg)
        }
    }
    
    const summary = `Batch index creation completed for ${collection.collectionName}: ${results.successful} successful, ${results.failed} failed`
    results.details.push(summary)
    console.log(summary)
    
    return results
}

/**
 * Batch drop multiple indexes with proper error handling
 * @param collection The MongoDB collection
 * @param indexNames Array of index names to drop
 * @returns Promise resolving to batch drop results
 */
export async function batchDropIndexes(collection: Collection<any>, indexNames: string[]): Promise<{
    successful: number,
    failed: number,
    details: string[]
}> {
    const results = { successful: 0, failed: 0, details: [] as string[] }
    
    if (indexNames.length === 0) {
        results.details.push(`No indexes to drop for collection ${collection.collectionName}`)
        return results
    }
    
    console.log(`🗑️ Dropping ${indexNames.length} indexes from collection ${collection.collectionName}`)
    
    for (const indexName of indexNames) {
        try {
            await safeDropIndex(collection, indexName)
            results.successful++
            results.details.push(`✅ Dropped index: ${indexName}`)
        } catch (error) {
            results.failed++
            const errorMsg = `❌ Failed to drop index ${indexName}: ${error instanceof Error ? error.message : String(error)}`
            results.details.push(errorMsg)
            console.error(errorMsg)
        }
    }
    
    const summary = `Batch index drop completed for ${collection.collectionName}: ${results.successful} successful, ${results.failed} failed`
    results.details.push(summary)
    console.log(summary)
    
    return results
}

/**
 * Recreate indexes by dropping and creating them
 * Useful for index option changes or corruption recovery
 * @param collection The MongoDB collection
 * @param indexSpecs Array of index specifications to recreate
 * @returns Promise resolving to recreation results
 */
export async function recreateIndexes(collection: Collection<any>, indexSpecs: Array<{ name: string, spec: any, options?: any }>): Promise<{
    successful: number,
    failed: number,
    details: string[]
}> {
    const results = { successful: 0, failed: 0, details: [] as string[] }
    
    if (indexSpecs.length === 0) {
        results.details.push(`No indexes to recreate for collection ${collection.collectionName}`)
        return results
    }
    
    console.log(`🔄 Recreating ${indexSpecs.length} indexes for collection ${collection.collectionName}`)
    
    // First, try to drop existing indexes
    const indexNames = []
    for (const indexSpec of indexSpecs) {
        try {
            // Find existing index by key pattern
            const existingIndexName = await findIndexByKeyPattern(collection, indexSpec.spec)
            if (existingIndexName) {
                indexNames.push(existingIndexName)
            }
        } catch (error) {
            results.details.push(`⚠️ Could not find existing index for ${indexSpec.name}, will create new`)
        }
    }
    
    // Drop existing indexes if found
    if (indexNames.length > 0) {
        const dropResults = await batchDropIndexes(collection, indexNames)
        results.details.push(...dropResults.details)
    }
    
    // Create all indexes
    const createResults = await batchCreateIndexes(collection, indexSpecs)
    results.successful = createResults.successful
    results.failed = createResults.failed
    results.details.push(...createResults.details)
    
    return results
}

/**
 * Validate index health by checking if all expected indexes exist with correct options
 * @param collection The MongoDB collection
 * @param expectedIndexes Array of expected index specifications
 * @returns Promise resolving to health check results
 */
export async function validateIndexHealth(collection: Collection<any>, expectedIndexes: Array<{ name: string, spec: any, options?: any }>): Promise<{
    healthy: boolean,
    missing: string[],
    optionMismatches: string[],
    unexpected: string[],
    details: string[]
}> {
    const results = {
        healthy: true,
        missing: [] as string[],
        optionMismatches: [] as string[],
        unexpected: [] as string[],
        details: [] as string[]
    }
    
    try {
        const existingIndexes = await collection.indexes()
        const existingIndexMap = new Map()
        
        // Build map of existing indexes (excluding _id)
        existingIndexes.forEach(idx => {
            if (idx.name !== '_id_') {
                existingIndexMap.set(JSON.stringify(idx.key), idx)
            }
        })
        
        // Check for missing indexes and option mismatches
        for (const expected of expectedIndexes) {
            const keyString = JSON.stringify(expected.spec)
            const existing = existingIndexMap.get(keyString)
            
            if (!existing) {
                results.missing.push(expected.name)
                results.healthy = false
                results.details.push(`❌ Missing index: ${expected.name}`)
            } else {
                // Check critical options
                const criticalOptions = ['unique', 'expireAfterSeconds', 'sparse']
                for (const option of criticalOptions) {
                    const expectedValue = expected.options?.[option]
                    const existingValue = existing[option]
                    
                    if (expectedValue !== existingValue) {
                        // Special handling for TTL tolerance
                        if (option === 'expireAfterSeconds' && typeof expectedValue === 'number' && typeof existingValue === 'number') {
                            if (Math.abs(expectedValue - existingValue) <= 60) { // 1 minute tolerance
                                continue
                            }
                        }
                        
                        results.optionMismatches.push(`${expected.name} (${option}: expected ${expectedValue}, got ${existingValue})`)
                        results.healthy = false
                        results.details.push(`⚠️ Option mismatch in ${expected.name}: ${option}`)
                    }
                }
            }
        }
        
        // Check for unexpected indexes (informational only)
        const expectedKeyStrings = new Set(expectedIndexes.map(idx => JSON.stringify(idx.spec)))
        for (const [keyString, existing] of existingIndexMap.entries()) {
            if (!expectedKeyStrings.has(keyString)) {
                results.unexpected.push(existing.name || 'unnamed')
                results.details.push(`ℹ️ Unexpected index found: ${existing.name}`)
            }
        }
        
        if (results.healthy) {
            results.details.push(`✅ All ${expectedIndexes.length} indexes are healthy for collection ${collection.collectionName}`)
        } else {
            results.details.push(`❌ Index health issues found for collection ${collection.collectionName}`)
        }
        
    } catch (error) {
        results.healthy = false
        results.details.push(`❌ Failed to validate index health: ${error instanceof Error ? error.message : String(error)}`)
    }
    
    return results
}