const { makeMongoDBStore } = require('../dist')
const { MongoClient } = require('mongodb')

async function testLabelAssociations() {
    console.log('Starting label association test...')
    
    // Create store
    const store = await makeMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: 'baileys_test',
        instanceId: 'test_instance_' + Date.now(),
        
        // Enable all logs to see label operations
        logLevel: 'all',
        
        // Enable Redis/Bull for testing - label associations use concurrency: 1
        redis: process.env.REDIS_URL ? {
            connection: process.env.REDIS_URL,
            queuePrefix: 'label-test',
            concurrency: 50 // All queues except label associations
        } : undefined
    })
    
    const stats = store.getPerformanceStats()
    console.log('Queue system:', stats.bullStats?.initialized ? 'Redis/Bull' : 'In-Memory')
    if (stats.bullStats?.initialized) {
        console.log('Label association processing: Sequential (concurrency: 1)')
    }
    
    // Simulate 109 label associations being added rapidly
    const totalAssociations = 109
    const promises = []
    
    console.log(`\nAdding ${totalAssociations} label associations...`)
    
    for (let i = 1; i <= totalAssociations; i++) {
        const association = {
            chatId: `chat_${Math.floor(i / 10)}@s.whatsapp.net`,
            labelId: `label_${i % 10}`,
            messageId: i % 3 === 0 ? `msg_${i}` : undefined,
            type: 'ChatLabelAssociation'
        }
        
        // Add association without waiting
        const promise = store.upsertLabelAssociation(association)
            .then(() => console.log(`✓ Association ${i} queued`))
            .catch(err => console.error(`✗ Association ${i} failed:`, err))
        
        promises.push(promise)
        
        // Add small random delay to simulate real-world timing
        if (i % 10 === 0) {
            await new Promise(resolve => setTimeout(resolve, Math.random() * 10))
        }
    }
    
    console.log('\nWaiting for all associations to be queued...')
    await Promise.all(promises)
    
    // Get initial stats
    let stats = store.getPerformanceStats()
    console.log('\n=== Initial Stats ===')
    console.log(`Total Received: ${stats.labelStats.totalReceived}`)
    console.log(`Total Processed: ${stats.labelStats.totalProcessed}`)
    console.log(`Queue Size: ${stats.labelStats.currentQueueSize}`)
    console.log(`Is Processing: ${stats.labelStats.isProcessing}`)
    
    // Force flush to ensure all are processed
    console.log('\nForcing flush of pending associations...')
    await store.flushLabelAssociations()
    
    // Get final stats
    stats = store.getPerformanceStats()
    console.log('\n=== Final Stats ===')
    console.log(`Total Received: ${stats.labelStats.totalReceived}`)
    console.log(`Total Processed: ${stats.labelStats.totalProcessed}`)
    console.log(`Queue Size: ${stats.labelStats.currentQueueSize}`)
    console.log(`Is Processing: ${stats.labelStats.isProcessing}`)
    
    // Verify all were processed
    const allAssociations = await store.getLabelAssociations()
    console.log(`\n=== Database Verification ===`)
    console.log(`Associations in database: ${allAssociations.length}`)
    
    if (stats.labelStats.totalReceived === stats.labelStats.totalProcessed && 
        stats.labelStats.totalProcessed === totalAssociations) {
        console.log('\n✅ SUCCESS: All label associations were processed correctly!')
    } else {
        console.log('\n❌ FAILURE: Some label associations were lost!')
        console.log(`Expected: ${totalAssociations}`)
        console.log(`Received: ${stats.labelStats.totalReceived}`)
        console.log(`Processed: ${stats.labelStats.totalProcessed}`)
        console.log(`In Database: ${allAssociations.length}`)
    }
    
    // Cleanup
    await store.clearAll()
    await store.close()
    
    console.log('\nTest completed.')
}

// Run the test
testLabelAssociations().catch(console.error)