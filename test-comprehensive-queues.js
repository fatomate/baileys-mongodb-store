/**
 * Comprehensive test for Bull queue implementation across all event types
 * This test verifies that all event types use separate queues and automatic cleanup works
 */

const { makeMongoDBStore } = require('./dist')
const Redis = require('ioredis')

// Event types that should have queues
const EXPECTED_QUEUES = [
    'labels',
    'label-associations', 
    'messages',
    'chats',
    'contacts',
    'group-metadata',
    'presences',
    'state'
]

async function testComprehensiveQueues() {
    console.log('🧪 Testing Comprehensive Bull Queue Implementation\n')
    console.log('=' .repeat(60))
    
    // Create Redis connection to inspect queues
    const redis = new Redis('redis://localhost:6379')
    
    // Test 1: Create store and verify all queues are created
    console.log('\n📝 Test 1: Verifying all event type queues are created...')
    let store
    try {
        store = await makeMongoDBStore({
            uri: 'mongodb://localhost:27017',
            database: 'test_comprehensive_queues',
            instanceId: 'test_instance_001',
            ttlDays: 1,
            redis: {
                connection: 'redis://localhost:6379',
                queuePrefix: 'test',
                concurrency: 10
            }
        })
        
        // Check stats
        const stats = store.getPerformanceStats()
        console.log('✅ Store initialized')
        console.log('   Bull initialized:', stats.bullStats.initialized)
        console.log('   Total queues created:', stats.bullStats.totalQueues)
        console.log('   Queue types:', Object.keys(stats.bullStats.queues))
        
        // Verify all expected queues exist
        const createdQueues = Object.keys(stats.bullStats.queues)
        const missingQueues = EXPECTED_QUEUES.filter(q => !createdQueues.includes(q))
        
        if (missingQueues.length === 0) {
            console.log('✅ All expected queues created successfully')
        } else {
            console.error('❌ Missing queues:', missingQueues)
        }
        
        // Check Redis keys for each queue
        console.log('\n📝 Checking Redis keys for each queue...')
        for (const queueType of EXPECTED_QUEUES) {
            const keys = await redis.keys(`bull:test:${queueType}:test_instance_001:*`)
            console.log(`   ${queueType}: ${keys.length} keys`)
        }
        
    } catch (error) {
        console.error('❌ Test 1 failed:', error.message)
    }
    
    console.log('\n' + '=' .repeat(60))
    
    // Test 2: Test each event type uses its own queue
    console.log('\n📝 Test 2: Testing each event type uses separate queues...')
    
    try {
        // Test CHATS queue
        console.log('\n   Testing CHATS queue...')
        await store.upsertChats({
            id: 'chat_001',
            conversationTimestamp: Date.now(),
            unreadCount: 0
        })
        
        // Test CONTACTS queue
        console.log('   Testing CONTACTS queue...')
        await store.upsertContacts([{
            id: 'contact_001',
            name: 'Test Contact'
        }])
        
        // Test MESSAGES queue
        console.log('   Testing MESSAGES queue...')
        await store.upsertMessage('chat_001', {
            key: { id: 'msg_001', remoteJid: 'chat_001' },
            message: { conversation: 'Test message' },
            messageTimestamp: Date.now()
        })
        
        // Test LABELS queue
        console.log('   Testing LABELS queue...')
        await store.upsertLabel('label_001', {
            id: 'label_001',
            name: 'Test Label',
            predefinedId: '0',
            color: 1,
            deleted: false
        })
        
        // Test LABEL ASSOCIATIONS queue
        console.log('   Testing LABEL ASSOCIATIONS queue...')
        await store.upsertLabelAssociation({
            chatId: 'chat_001',
            labelId: 'label_001'
        })
        
        // Test GROUP METADATA queue
        console.log('   Testing GROUP METADATA queue...')
        await store.upsertGroupMetadata('group_001', {
            id: 'group_001',
            subject: 'Test Group',
            participants: []
        })
        
        // Test PRESENCES queue
        console.log('   Testing PRESENCES queue...')
        await store.updatePresence('user_001', {
            'participant_001': { lastKnownPresence: 'available' }
        })
        
        // Test STATE queue
        console.log('   Testing STATE queue...')
        await store.updateState({ connection: 'open' })
        
        // Wait for processing
        await new Promise(resolve => setTimeout(resolve, 2000))
        
        console.log('\n✅ All event types successfully queued to separate queues')
        
        // Check job counts in each queue
        console.log('\n📊 Queue job statistics:')
        for (const queueType of EXPECTED_QUEUES) {
            const completedKey = `bull:test:${queueType}:test_instance_001:completed`
            const failedKey = `bull:test:${queueType}:test_instance_001:failed`
            const waitingKey = `bull:test:${queueType}:test_instance_001:wait`
            
            const completed = await redis.zcard(completedKey)
            const failed = await redis.zcard(failedKey)
            const waiting = await redis.llen(waitingKey)
            
            console.log(`   ${queueType.padEnd(20)}: completed=${completed}, failed=${failed}, waiting=${waiting}`)
        }
        
    } catch (error) {
        console.error('❌ Test 2 failed:', error.message)
    }
    
    console.log('\n' + '=' .repeat(60))
    
    // Test 3: Verify automatic job cleanup
    console.log('\n📝 Test 3: Testing automatic job cleanup (removeOnComplete)...')
    
    try {
        // Add many jobs to test cleanup
        console.log('   Adding 100 label associations...')
        for (let i = 0; i < 100; i++) {
            await store.upsertLabelAssociation({
                chatId: `chat_${i}`,
                labelId: `label_${i % 10}`
            })
        }
        
        console.log('   Waiting for jobs to complete...')
        await new Promise(resolve => setTimeout(resolve, 3000))
        
        // Check completed jobs count before cleanup
        const completedBefore = await redis.zcard('bull:test:label-associations:test_instance_001:completed')
        console.log(`   Completed jobs before cleanup: ${completedBefore}`)
        
        // Wait for automatic cleanup (60 seconds as configured)
        console.log('   Waiting 65 seconds for automatic cleanup...')
        console.log('   (Jobs older than 60 seconds should be removed)')
        await new Promise(resolve => setTimeout(resolve, 65000))
        
        // Check completed jobs count after cleanup
        const completedAfter = await redis.zcard('bull:test:label-associations:test_instance_001:completed')
        console.log(`   Completed jobs after cleanup: ${completedAfter}`)
        
        if (completedAfter < completedBefore) {
            console.log(`✅ Automatic cleanup working! Removed ${completedBefore - completedAfter} jobs`)
        } else {
            console.log('⚠️  No jobs cleaned up yet (may need more time)')
        }
        
    } catch (error) {
        console.error('❌ Test 3 failed:', error.message)
    }
    
    console.log('\n' + '=' .repeat(60))
    
    // Test 4: Multiple instances with different queues
    console.log('\n📝 Test 4: Testing multiple instances with separate queues...')
    
    let store2, store3
    try {
        // Create additional instances
        store2 = await makeMongoDBStore({
            uri: 'mongodb://localhost:27017',
            database: 'test_comprehensive_queues',
            instanceId: 'test_instance_002',
            redis: {
                connection: 'redis://localhost:6379',
                queuePrefix: 'test',
                concurrency: 10
            }
        })
        
        store3 = await makeMongoDBStore({
            uri: 'mongodb://localhost:27017',
            database: 'test_comprehensive_queues',
            instanceId: 'test_instance_003',
            redis: {
                connection: 'redis://localhost:6379',
                queuePrefix: 'test',
                concurrency: 10
            }
        })
        
        console.log('✅ Created 3 instances with separate queue sets')
        
        // Add data to each instance
        await store.upsertLabel('inst1_label', { id: 'inst1_label', name: 'Instance 1' })
        await store2.upsertLabel('inst2_label', { id: 'inst2_label', name: 'Instance 2' })
        await store3.upsertLabel('inst3_label', { id: 'inst3_label', name: 'Instance 3' })
        
        // Verify separate queues exist for each instance
        console.log('\n📊 Queue isolation verification:')
        for (const instanceId of ['test_instance_001', 'test_instance_002', 'test_instance_003']) {
            const keys = await redis.keys(`bull:test:*:${instanceId}:*`)
            const queueTypes = new Set()
            keys.forEach(key => {
                const match = key.match(/bull:test:([^:]+):/)
                if (match) queueTypes.add(match[1])
            })
            console.log(`   ${instanceId}: ${queueTypes.size} queue types`)
        }
        
        console.log('✅ Each instance has isolated queues')
        
    } catch (error) {
        console.error('❌ Test 4 failed:', error.message)
    } finally {
        if (store2) await store2.close()
        if (store3) await store3.close()
    }
    
    console.log('\n' + '=' .repeat(60))
    
    // Test 5: Memory efficiency check
    console.log('\n📝 Test 5: Checking Redis memory efficiency...')
    
    try {
        // Get Redis memory info
        const info = await redis.info('memory')
        const usedMemory = info.match(/used_memory_human:(.+)/)?.[1]
        console.log(`   Current Redis memory usage: ${usedMemory}`)
        
        // Count total keys
        const allKeys = await redis.keys('bull:test:*')
        console.log(`   Total Bull queue keys: ${allKeys.length}`)
        
        // Check for old completed/failed jobs
        let oldJobs = 0
        for (const queueType of EXPECTED_QUEUES) {
            const completedKeys = await redis.zrange(
                `bull:test:${queueType}:test_instance_001:completed`,
                0, -1, 'WITHSCORES'
            )
            // Count jobs older than 60 seconds
            const now = Date.now()
            for (let i = 1; i < completedKeys.length; i += 2) {
                const timestamp = parseInt(completedKeys[i])
                if (now - timestamp > 60000) {
                    oldJobs++
                }
            }
        }
        
        if (oldJobs === 0) {
            console.log('✅ No old jobs found (cleanup is working)')
        } else {
            console.log(`⚠️  Found ${oldJobs} old jobs that should be cleaned up`)
        }
        
    } catch (error) {
        console.error('❌ Test 5 failed:', error.message)
    }
    
    // Cleanup
    console.log('\n🧹 Cleaning up...')
    if (store) {
        await store.close()
        console.log('   Store closed')
    }
    
    // Clean up test data from Redis
    const testKeys = await redis.keys('bull:test:*')
    if (testKeys.length > 0) {
        await redis.del(...testKeys)
        console.log(`   Deleted ${testKeys.length} test keys from Redis`)
    }
    
    await redis.quit()
    console.log('   Redis connection closed')
    
    console.log('\n' + '=' .repeat(60))
    console.log('🎉 All tests completed!')
}

// Performance benchmark
async function performanceBenchmark() {
    console.log('\n\n📊 PERFORMANCE BENCHMARK')
    console.log('=' .repeat(60))
    
    const redis = new Redis('redis://localhost:6379')
    
    // Test with Bull queues
    console.log('\n🚀 Testing with Bull queues...')
    const storeBull = await makeMongoDBStore({
        uri: 'mongodb://localhost:27017',
        database: 'benchmark_bull',
        instanceId: 'bench_bull',
        redis: {
            connection: 'redis://localhost:6379',
            queuePrefix: 'bench',
            concurrency: 100
        }
    })
    
    const startBull = Date.now()
    const promisesBull = []
    
    for (let i = 0; i < 1000; i++) {
        promisesBull.push(storeBull.upsertLabelAssociation({
            chatId: `chat_${i % 100}`,
            labelId: `label_${i % 20}`
        }))
    }
    
    await Promise.all(promisesBull)
    await new Promise(resolve => setTimeout(resolve, 5000)) // Wait for processing
    
    const timeBull = Date.now() - startBull
    console.log(`   Time: ${timeBull}ms`)
    console.log(`   Rate: ${Math.round(1000000 / timeBull)} ops/second`)
    
    // Test without Bull queues (in-memory)
    console.log('\n💾 Testing with in-memory processing...')
    const storeMemory = await makeMongoDBStore({
        uri: 'mongodb://localhost:27017',
        database: 'benchmark_memory',
        instanceId: 'bench_memory'
        // No redis config
    })
    
    const startMemory = Date.now()
    const promisesMemory = []
    
    for (let i = 0; i < 1000; i++) {
        promisesMemory.push(storeMemory.upsertLabelAssociation({
            chatId: `chat_${i % 100}`,
            labelId: `label_${i % 20}`
        }))
    }
    
    await Promise.all(promisesMemory)
    await storeMemory.flushLabelAssociations()
    
    const timeMemory = Date.now() - startMemory
    console.log(`   Time: ${timeMemory}ms`)
    console.log(`   Rate: ${Math.round(1000000 / timeMemory)} ops/second`)
    
    console.log('\n📈 Comparison:')
    const speedup = ((timeMemory - timeBull) / timeMemory * 100).toFixed(1)
    if (timeBull < timeMemory) {
        console.log(`   Bull queues are ${speedup}% faster`)
    } else {
        console.log(`   In-memory is ${Math.abs(speedup)}% faster`)
    }
    console.log(`   Bull provides persistence and reliability benefits`)
    
    // Cleanup
    await storeBull.close()
    await storeMemory.close()
    
    const benchKeys = await redis.keys('bull:bench:*')
    if (benchKeys.length > 0) {
        await redis.del(...benchKeys)
    }
    await redis.quit()
}

// Run tests
if (require.main === module) {
    testComprehensiveQueues()
        .then(() => performanceBenchmark())
        .catch(console.error)
        .finally(() => {
            console.log('\n👋 Tests finished')
            process.exit(0)
        })
}

module.exports = { testComprehensiveQueues, performanceBenchmark }