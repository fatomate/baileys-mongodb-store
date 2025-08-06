/**
 * Test file for Redis/Bull integration
 * Run this to verify the hybrid queue implementation works correctly
 */

const { makeMongoDBStore } = require('./dist')

async function testRedisIntegration() {
    console.log('🧪 Testing Baileys MongoDB Store with Redis/Bull Integration\n')
    
    // Test 1: Store with Redis/Bull enabled
    console.log('📝 Test 1: Creating store WITH Redis configuration...')
    let store1
    try {
        store1 = await makeMongoDBStore({
            uri: 'mongodb://localhost:27017',
            database: 'test_whatsapp_store',
            instanceId: 'test_redis_001',
            ttlDays: 1,
            redis: {
                connection: 'redis://localhost:6379',
                queuePrefix: 'test_baileys',
                enableLabelQueue: true,
                enableMessageQueue: false,
                concurrency: 10,
                removeOnComplete: 60,
                removeOnFail: 300
            }
        })
        
        const stats1 = store1.getPerformanceStats()
        console.log('✅ Store created successfully')
        console.log('   Bull initialized:', stats1.bullStats?.initialized)
        console.log('   Redis connected:', stats1.bullStats?.redisConnected)
        console.log('   Label queue:', stats1.bullStats?.labelQueue)
        
        // Test label association with Bull
        console.log('\n📝 Testing label association with Bull queue...')
        await store1.upsertLabel('test_label_1', {
            id: 'test_label_1',
            name: 'Test Label 1',
            predefinedId: '0',
            color: 1,
            deleted: false
        })
        
        await store1.upsertLabelAssociation({
            chatId: 'test_chat_1',
            labelId: 'test_label_1'
        })
        
        // Wait a bit for processing
        await new Promise(resolve => setTimeout(resolve, 1000))
        
        const stats2 = store1.getPerformanceStats()
        console.log('✅ Label association queued')
        console.log('   Labels received:', stats2.labelStats.totalReceived)
        console.log('   Labels processed:', stats2.labelStats.totalProcessed)
        
    } catch (error) {
        console.error('❌ Test 1 failed:', error.message)
    } finally {
        if (store1) await store1.close()
    }
    
    console.log('\n' + '='.repeat(60) + '\n')
    
    // Test 2: Store without Redis (fallback to in-memory)
    console.log('📝 Test 2: Creating store WITHOUT Redis configuration...')
    let store2
    try {
        store2 = await makeMongoDBStore({
            uri: 'mongodb://localhost:27017',
            database: 'test_whatsapp_store',
            instanceId: 'test_memory_001',
            ttlDays: 1
            // No redis config - should use in-memory processing
        })
        
        const stats3 = store2.getPerformanceStats()
        console.log('✅ Store created successfully')
        console.log('   Bull initialized:', stats3.bullStats?.initialized)
        console.log('   Fallback reason:', stats3.bullStats?.reason)
        
        // Test label association with in-memory
        console.log('\n📝 Testing label association with in-memory queue...')
        await store2.upsertLabel('test_label_2', {
            id: 'test_label_2',
            name: 'Test Label 2',
            predefinedId: '1',
            color: 2,
            deleted: false
        })
        
        await store2.upsertLabelAssociation({
            chatId: 'test_chat_2',
            labelId: 'test_label_2'
        })
        
        // Flush to ensure processing
        await store2.flushLabelAssociations()
        
        const stats4 = store2.getPerformanceStats()
        console.log('✅ Label association processed')
        console.log('   Labels received:', stats4.labelStats.totalReceived)
        console.log('   Labels processed:', stats4.labelStats.totalProcessed)
        
    } catch (error) {
        console.error('❌ Test 2 failed:', error.message)
    } finally {
        if (store2) await store2.close()
    }
    
    console.log('\n' + '='.repeat(60) + '\n')
    
    // Test 3: Fallback behavior with invalid Redis
    console.log('📝 Test 3: Testing fallback with INVALID Redis configuration...')
    let store3
    try {
        store3 = await makeMongoDBStore({
            uri: 'mongodb://localhost:27017',
            database: 'test_whatsapp_store',
            instanceId: 'test_fallback_001',
            ttlDays: 1,
            redis: {
                connection: 'redis://invalid-host:6379',
                enableLabelQueue: true
            }
        })
        
        const stats5 = store3.getPerformanceStats()
        console.log('✅ Store created with fallback to in-memory')
        console.log('   Bull initialized:', stats5.bullStats?.initialized)
        console.log('   Fallback reason:', stats5.bullStats?.reason || 'initialization failed')
        
        // Should still work with in-memory processing
        await store3.upsertLabelAssociation({
            chatId: 'test_chat_3',
            labelId: 'test_label_3'
        })
        
        await store3.flushLabelAssociations()
        
        const stats6 = store3.getPerformanceStats()
        console.log('✅ Fallback processing working')
        console.log('   Labels received:', stats6.labelStats.totalReceived)
        
    } catch (error) {
        console.error('❌ Test 3 failed:', error.message)
    } finally {
        if (store3) await store3.close()
    }
    
    console.log('\n' + '='.repeat(60) + '\n')
    console.log('🎉 All tests completed!')
}

// Performance test
async function performanceTest() {
    console.log('\n📊 Performance Test: Bulk Label Associations\n')
    
    const store = await makeMongoDBStore({
        uri: 'mongodb://localhost:27017',
        database: 'test_whatsapp_store',
        instanceId: 'test_perf_001',
        ttlDays: 1,
        redis: {
            connection: 'redis://localhost:6379',
            queuePrefix: 'test_perf',
            enableLabelQueue: true,
            concurrency: 100
        }
    })
    
    const stats = store.getPerformanceStats()
    const usingBull = stats.bullStats?.initialized
    
    console.log(`Using ${usingBull ? 'Bull Queue' : 'In-Memory'} processing`)
    console.log('Inserting 1000 label associations...')
    
    const startTime = Date.now()
    const promises = []
    
    for (let i = 0; i < 1000; i++) {
        promises.push(
            store.upsertLabelAssociation({
                chatId: `chat_${i % 100}`,
                labelId: `label_${i % 10}`,
                messageId: `msg_${i}`
            })
        )
    }
    
    await Promise.all(promises)
    console.log('All associations queued')
    
    // Wait for processing
    if (!usingBull) {
        await store.flushLabelAssociations()
    } else {
        // For Bull, wait a bit for processing
        await new Promise(resolve => setTimeout(resolve, 5000))
    }
    
    const endTime = Date.now()
    const finalStats = store.getPerformanceStats()
    
    console.log('\n📈 Results:')
    console.log(`   Time taken: ${(endTime - startTime) / 1000}s`)
    console.log(`   Labels received: ${finalStats.labelStats.totalReceived}`)
    console.log(`   Labels processed: ${finalStats.labelStats.totalProcessed}`)
    console.log(`   Errors: ${finalStats.errors}`)
    console.log(`   Processing rate: ${Math.round(1000 / ((endTime - startTime) / 1000))} labels/second`)
    
    await store.close()
}

// Run tests
if (require.main === module) {
    testRedisIntegration()
        .then(() => performanceTest())
        .catch(console.error)
        .finally(() => process.exit(0))
}

module.exports = { testRedisIntegration, performanceTest }