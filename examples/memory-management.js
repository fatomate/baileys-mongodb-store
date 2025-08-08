const { makeMongoDBStore } = require('../src')
const pino = require('pino')

const logger = pino({ level: 'info' })

/**
 * Example: Memory Management and Batch Processing
 * 
 * This example demonstrates:
 * - Memory-aware batch processing
 * - Backpressure control
 * - Performance monitoring
 * - Batch size optimization
 */

async function createMemoryOptimizedStore() {
    const store = await makeMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: 'whatsapp_memory_test',
        instanceId: 'memory-test-instance',
        ttlDays: 7, // Shorter TTL for testing
        
        // Memory management configuration
        memory: {
            // Set a lower threshold for testing (256MB)
            maxMemoryMB: 256,
            
            // Smaller batch size for testing
            maxBatchSize: 500,
            
            // Shorter time window for faster processing
            batchTimeWindowMs: 50,
            
            // Enable monitoring
            enableMonitoring: true
        },
        
        logger: logger.child({ module: 'memory-test' }),
        logLevel: 'all' // Show all logs for demo
    })
    
    return store
}

async function demonstrateMemoryManagement() {
    const store = await createMemoryOptimizedStore()
    
    console.log('🧠 Memory Management Demo Started\n')
    
    // Monitor memory usage
    const memoryMonitor = setInterval(() => {
        const stats = store.getPerformanceStats()
        if (stats.memoryStats) {
            const pressure = stats.memoryStats.memoryPressure
            const heapUsed = stats.memoryStats.heapUsedMB
            
            console.log(`Memory: ${heapUsed}MB used | Pressure: ${(pressure * 100).toFixed(1)}%`)
            
            // Visual pressure indicator
            const bars = Math.floor(pressure * 20)
            const indicator = '█'.repeat(bars) + '░'.repeat(20 - bars)
            console.log(`[${indicator}] ${pressure > 0.8 ? '⚠️ HIGH' : pressure > 0.6 ? '⚡ MEDIUM' : '✅ LOW'}`)
            console.log('---')
        }
    }, 2000) // Check every 2 seconds
    
    // Simulate batch processing with different loads
    console.log('📊 Starting batch processing simulation...\n')
    
    // Test 1: Normal load
    console.log('Test 1: Normal load (100 items)')
    await simulateBatchOperation(store, 100)
    await delay(3000)
    
    // Test 2: High load
    console.log('\nTest 2: High load (1000 items)')
    await simulateBatchOperation(store, 1000)
    await delay(3000)
    
    // Test 3: Burst load
    console.log('\nTest 3: Burst load (5000 items)')
    await simulateBatchOperation(store, 5000)
    await delay(3000)
    
    // Test 4: Sustained load
    console.log('\nTest 4: Sustained load (100 items every second for 10 seconds)')
    for (let i = 0; i < 10; i++) {
        await simulateBatchOperation(store, 100)
        await delay(1000)
    }
    
    // Final statistics
    clearInterval(memoryMonitor)
    console.log('\n📈 Final Performance Report:')
    const finalStats = store.getPerformanceStats()
    
    console.log('Messages processed:', finalStats.messagesProcessed)
    console.log('Labels processed:', finalStats.labelsProcessed)
    console.log('Batches processed:', finalStats.batchesProcessed)
    console.log('Errors:', finalStats.errors)
    
    if (finalStats.labelStats) {
        console.log('\nLabel Processing Stats:')
        console.log('Total received:', finalStats.labelStats.totalReceived)
        console.log('Total processed:', finalStats.labelStats.totalProcessed)
        console.log('Current queue size:', finalStats.labelStats.currentQueueSize)
    }
    
    if (finalStats.memoryStats?.batchMetrics) {
        const metrics = finalStats.memoryStats.batchMetrics
        console.log('\nBatch Processing Metrics:')
        console.log('Average batch size:', metrics.avgBatchSize)
        console.log('Max batch size:', metrics.maxBatchSize)
        console.log('Memory peak:', metrics.memoryPeakMB + 'MB')
    }
    
    // Cleanup
    await store.close()
    console.log('\n✅ Demo completed!')
}

// Simulate batch operations
async function simulateBatchOperation(store, itemCount) {
    const startTime = Date.now()
    const promises = []
    
    // Create test label associations
    for (let i = 0; i < itemCount; i++) {
        const association = {
            chatId: `test-chat-${i % 10}@s.whatsapp.net`,
            labelId: `label-${i % 5}`,
            messageId: Math.random() > 0.5 ? `msg-${i}` : undefined
        }
        
        promises.push(store.upsertLabelAssociation(association))
        
        // Add some variety with messages
        if (i % 10 === 0) {
            promises.push(store.upsertMessage(
                `test-chat-${i % 10}@s.whatsapp.net`,
                {
                    key: {
                        id: `msg-${Date.now()}-${i}`,
                        remoteJid: `test-chat-${i % 10}@s.whatsapp.net`,
                        fromMe: false
                    },
                    message: {
                        conversation: `Test message ${i}`
                    },
                    messageTimestamp: Math.floor(Date.now() / 1000)
                }
            ))
        }
    }
    
    // Wait for all operations
    await Promise.all(promises)
    
    const duration = Date.now() - startTime
    console.log(`Processed ${itemCount} items in ${duration}ms (${Math.round(itemCount / (duration / 1000))} items/sec)`)
}

// Delay helper
function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
}

// Advanced example: Custom backpressure handling
async function advancedBackpressureExample() {
    console.log('\n🔧 Advanced Backpressure Example\n')
    
    const store = await makeMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: 'whatsapp_backpressure',
        instanceId: 'backpressure-instance',
        
        memory: {
            maxMemoryMB: 128, // Very low threshold for testing
            maxBatchSize: 100,
            batchTimeWindowMs: 25,
            enableMonitoring: true
        },
        
        logLevel: 'all'
    })
    
    // Monitor backpressure events
    let pauseCount = 0
    let resumeCount = 0
    
    // Simulate continuous high load
    console.log('Generating continuous high load...')
    let shouldContinue = true
    let totalProcessed = 0
    
    const loadGenerator = async () => {
        while (shouldContinue) {
            const stats = store.getPerformanceStats()
            const pressure = stats.memoryStats?.memoryPressure || 0
            
            // Adjust load based on pressure
            const batchSize = pressure > 0.8 ? 10 : pressure > 0.6 ? 50 : 100
            
            if (pressure > 0.8) {
                pauseCount++
                console.log(`⏸️  Pausing - High pressure detected (${(pressure * 100).toFixed(1)}%)`)
                await delay(500) // Back off
            } else if (pressure < 0.6 && pauseCount > resumeCount) {
                resumeCount++
                console.log(`▶️  Resuming - Pressure reduced (${(pressure * 100).toFixed(1)}%)`)
            }
            
            // Process batch
            await simulateBatchOperation(store, batchSize)
            totalProcessed += batchSize
            
            // Small delay between batches
            await delay(10)
        }
    }
    
    // Run for 30 seconds
    loadGenerator()
    await delay(30000)
    shouldContinue = false
    
    // Final report
    console.log('\n📊 Backpressure Test Results:')
    console.log('Total items processed:', totalProcessed)
    console.log('Pause events:', pauseCount)
    console.log('Resume events:', resumeCount)
    console.log('Average throughput:', Math.round(totalProcessed / 30), 'items/sec')
    
    await store.close()
}

// Run the examples
if (require.main === module) {
    console.log('Choose an example:')
    console.log('1. Basic memory management demo')
    console.log('2. Advanced backpressure example')
    
    const example = process.argv[2] || '1'
    
    if (example === '2') {
        advancedBackpressureExample().catch(console.error)
    } else {
        demonstrateMemoryManagement().catch(console.error)
    }
}

module.exports = {
    createMemoryOptimizedStore,
    simulateBatchOperation
}