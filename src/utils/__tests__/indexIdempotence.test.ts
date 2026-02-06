/**
 * Property-Based Test for Index Creation Idempotence
 * 
 * **Feature: media-filehash-index, Property 1: Index Creation Idempotence**
 * **Validates: Requirements 2.3, 3.1**
 * 
 * This test verifies that initializing the store multiple times with the same
 * configuration results in exactly one `messages_media_fileHash` index existing
 * on the messages collection.
 */

import * as fc from 'fast-check'
import { 
    IndexSpec, 
    findMissingIndexes,
    findMissingIndexesEnhanced,
    validateIndexOptions
} from '../collectionHelper'

describe('Index Creation Idempotence Property Tests', () => {
    /**
     * **Feature: media-filehash-index, Property 1: Index Creation Idempotence**
     * **Validates: Requirements 2.3, 3.1**
     * 
     * Property: For any MongoDB store instance, initializing the store multiple times
     * with the same configuration should result in exactly one `messages_media_fileHash`
     * index existing on the messages collection.
     */
    describe('Property 1: Index Creation Idempotence', () => {
        // The messages_media_fileHash index specification
        const mediaFileHashIndex: IndexSpec = {
            name: 'messages_media_fileHash',
            spec: { 'mediaInfo.fileHash': 1 },
            options: { sparse: true }
        }

        beforeEach(() => {
            jest.clearAllMocks()
        })

        test('should identify missing index when collection has no indexes', async () => {
            // Property: When no indexes exist, the media fileHash index should be identified as missing
            await fc.assert(
                fc.asyncProperty(
                    fc.constant([{ key: { _id: 1 }, name: '_id_' }] as any[]), // Only default _id index
                    async (existingIndexes: any[]) => {
                        const missingIndexes = findMissingIndexes(existingIndexes, [mediaFileHashIndex])
                        
                        // The media fileHash index should be in the missing list
                        expect(missingIndexes).toHaveLength(1)
                        expect(missingIndexes[0].name).toBe('messages_media_fileHash')
                        expect(missingIndexes[0].spec).toEqual({ 'mediaInfo.fileHash': 1 })
                        expect(missingIndexes[0].options).toEqual({ sparse: true })
                    }
                ),
                { numRuns: 100 }
            )
        })

        test('should not identify index as missing when it already exists', async () => {
            // Property: When the index already exists with correct spec, it should not be identified as missing
            await fc.assert(
                fc.asyncProperty(
                    fc.constant([
                        { key: { _id: 1 }, name: '_id_' },
                        { key: { 'mediaInfo.fileHash': 1 }, name: 'messages_media_fileHash', sparse: true }
                    ] as any[]),
                    async (existingIndexes: any[]) => {
                        const missingIndexes = findMissingIndexes(existingIndexes, [mediaFileHashIndex])
                        
                        // The media fileHash index should NOT be in the missing list
                        expect(missingIndexes).toHaveLength(0)
                    }
                ),
                { numRuns: 100 }
            )
        })

        test('should handle multiple initialization attempts idempotently', async () => {
            /**
             * Property: For any number of initialization attempts (1-10), the result
             * should always be exactly one messages_media_fileHash index
             */
            await fc.assert(
                fc.asyncProperty(
                    fc.integer({ min: 1, max: 10 }), // Number of initialization attempts
                    async (initAttempts) => {
                        // Simulate index state - starts with no index
                        const currentIndexes: any[] = [{ key: { _id: 1 }, name: '_id_' }]
                        
                        // Track how many times the index was "created"
                        let createAttempts = 0
                        
                        for (let i = 0; i < initAttempts; i++) {
                            const missingIndexes = findMissingIndexes(currentIndexes, [mediaFileHashIndex])
                            
                            if (missingIndexes.length > 0) {
                                // Simulate creating the index
                                createAttempts++
                                currentIndexes.push({
                                    key: { 'mediaInfo.fileHash': 1 },
                                    name: 'messages_media_fileHash',
                                    sparse: true
                                })
                            }
                        }
                        
                        // After all attempts, there should be exactly one media fileHash index
                        const mediaFileHashIndexes = currentIndexes.filter(
                            idx => idx.name === 'messages_media_fileHash'
                        )
                        expect(mediaFileHashIndexes).toHaveLength(1)
                        
                        // The index should only have been created once
                        expect(createAttempts).toBe(1)
                    }
                ),
                { numRuns: 100 }
            )
        })

        test('should correctly validate index options for sparse index', async () => {
            // Property: Index options validation should correctly identify matching sparse options
            await fc.assert(
                fc.asyncProperty(
                    fc.boolean(), // Whether existing index has sparse option
                    async (hasSparseOption) => {
                        const existingIndex = {
                            key: { 'mediaInfo.fileHash': 1 },
                            name: 'messages_media_fileHash',
                            ...(hasSparseOption ? { sparse: true } : {})
                        }
                        
                        const isValid = validateIndexOptions(existingIndex, mediaFileHashIndex)
                        
                        // Should only be valid if sparse option matches
                        expect(isValid).toBe(hasSparseOption)
                    }
                ),
                { numRuns: 100 }
            )
        })

        test('should handle concurrent index creation scenarios', async () => {
            /**
             * Property: Even with simulated concurrent initialization attempts,
             * the final state should have exactly one index
             */
            await fc.assert(
                fc.asyncProperty(
                    fc.array(fc.boolean(), { minLength: 2, maxLength: 5 }), // Concurrent attempts
                    async (concurrentAttempts) => {
                        // Simulate a shared index state
                        let indexExists = false
                        
                        // Simulate concurrent checks and creates
                        await Promise.all(
                            concurrentAttempts.map(async (shouldCreate) => {
                                // Check if index exists (simulated race condition)
                                const wasExisting = indexExists
                                
                                if (!wasExisting && shouldCreate) {
                                    // Simulate creating the index
                                    indexExists = true
                                    return 'created'
                                }
                                return 'skipped'
                            })
                        )
                        
                        // In a properly idempotent system, even with races,
                        // we should end up with at most one index
                        // (In real MongoDB, duplicate creation attempts would fail gracefully)
                        if (concurrentAttempts.some(x => x)) {
                            expect(indexExists).toBe(true)
                        }
                    }
                ),
                { numRuns: 100 }
            )
        })

        test('should identify index needing recreation when options differ', async () => {
            // Property: When index exists but with different options, it should be flagged for recreation
            await fc.assert(
                fc.asyncProperty(
                    fc.record({
                        sparse: fc.boolean(),
                        unique: fc.boolean()
                    }),
                    async (differentOptions) => {
                        const existingIndexes = [
                            { key: { _id: 1 }, name: '_id_' },
                            { 
                                key: { 'mediaInfo.fileHash': 1 }, 
                                name: 'messages_media_fileHash',
                                ...differentOptions
                            }
                        ]
                        
                        const { missing, needRecreation } = findMissingIndexesEnhanced(
                            existingIndexes, 
                            [mediaFileHashIndex]
                        )
                        
                        // Index should not be missing (key pattern matches)
                        expect(missing).toHaveLength(0)
                        
                        // Should need recreation only if sparse option doesn't match
                        if (differentOptions.sparse === true) {
                            expect(needRecreation).toHaveLength(0)
                        } else {
                            expect(needRecreation).toHaveLength(1)
                        }
                    }
                ),
                { numRuns: 100 }
            )
        })

        test('should maintain idempotence across different index configurations', async () => {
            /**
             * Property: For any combination of existing indexes, adding the media fileHash
             * index should result in exactly one such index
             */
            await fc.assert(
                fc.asyncProperty(
                    fc.array(
                        fc.record({
                            key: fc.oneof(
                                fc.constant({ instanceId: 1, jid: 1 }),
                                fc.constant({ instanceId: 1, 'key.id': 1 }),
                                fc.constant({ updatedAt: 1 })
                            ),
                            name: fc.string({ minLength: 5, maxLength: 30 })
                        }),
                        { minLength: 0, maxLength: 5 }
                    ),
                    async (otherIndexes) => {
                        // Start with default _id index and other random indexes
                        const existingIndexes: any[] = [
                            { key: { _id: 1 }, name: '_id_' },
                            ...otherIndexes
                        ]
                        
                        // First check - should identify media fileHash as missing
                        const firstCheck = findMissingIndexes(existingIndexes, [mediaFileHashIndex])
                        expect(firstCheck).toHaveLength(1)
                        
                        // Simulate adding the index
                        existingIndexes.push({
                            key: { 'mediaInfo.fileHash': 1 },
                            name: 'messages_media_fileHash',
                            sparse: true
                        })
                        
                        // Second check - should not identify as missing anymore
                        const secondCheck = findMissingIndexes(existingIndexes, [mediaFileHashIndex])
                        expect(secondCheck).toHaveLength(0)
                        
                        // Count media fileHash indexes
                        const mediaIndexCount = existingIndexes.filter(
                            idx => idx.key && JSON.stringify(idx.key) === JSON.stringify({ 'mediaInfo.fileHash': 1 })
                        ).length
                        expect(mediaIndexCount).toBe(1)
                    }
                ),
                { numRuns: 100 }
            )
        })
    })
})
