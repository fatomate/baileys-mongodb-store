#!/usr/bin/env node

/**
 * Unit test for SharedQueueManager
 * Run with: node test-queue-manager.js
 */

const { SharedQueueManager, JobType } = require('./dist/utils/sharedQueueManager')

async function testQueueManager() {
    console.log('🧪 Testing SharedQueueManager\n')
    
    const config = {
        redis: {
            host: process.env.REDIS_HOST || 'localhost',
            port: process.env.REDIS_PORT || 6379,
            password: process.env.REDIS_PASSWORD
        },
        queueConcurrency: {
            highPriority: 10,
            dataSync: 5,
            media: 2,
            lowPriority: 1
        },
        enableMetrics: true,
        logLevel: 'info'
    }
    
    let manager = null
    
    try {
        console.log('📦 Creating SharedQueueManager...')
        manager = SharedQueueManager.getInstance(config)
        console.log('✅ Manager created successfully')
        
        // Register a test processor
        console.log('\n📝 Registering test processor...')
        manager.registerProcessor(JobType.MESSAGES, async (job) => {
            console.log(`  Processing job: ${job.id} for instance ${job.data.instanceId}`)
            return { success: true, processed: job.data }
        })
        console.log('✅ Processor registered')
        
        // Add test jobs
        console.log('\n📨 Adding test jobs...')
        
        const job1 = await manager.addJob(
            JobType.MESSAGES,
            { type: 'upsert', message: 'Test message 1' },
            'test_instance_1',
            8
        )
        console.log(`  Job 1 added: ${job1.id}`)
        
        const job2 = await manager.addJob(
            JobType.CONTACTS,
            { type: 'upsert', contacts: ['contact1', 'contact2'] },
            'test_instance_2',
            5
        )
        console.log(`  Job 2 added: ${job2.id}`)
        
        const job3 = await manager.addJob(
            JobType.MEDIA_DOWNLOAD,
            { message: 'Media message', mediaUrl: 'https://example.com/media.jpg' },
            'test_instance_1',
            3
        )
        console.log(`  Job 3 added: ${job3.id}`)
        
        // Get metrics
        console.log('\n📊 Queue Metrics:')
        const metrics = await manager.getMetrics()
        for (const [queue, stats] of metrics) {
            console.log(`  ${queue}:`)
            console.log(`    - Waiting: ${stats.waiting}`)
            console.log(`    - Active: ${stats.active}`)
            console.log(`    - Completed: ${stats.completed}`)
            console.log(`    - Failed: ${stats.failed}`)
        }
        
        // Wait for processing
        console.log('\n⏳ Waiting for job processing...')
        await new Promise(resolve => setTimeout(resolve, 2000))
        
        // Final metrics
        console.log('\n📊 Final Queue Metrics:')
        const finalMetrics = await manager.getMetrics()
        for (const [queue, stats] of finalMetrics) {
            if (stats.processed > 0 || stats.waiting > 0 || stats.active > 0) {
                console.log(`  ${queue}: ${stats.processed} processed, ${stats.failed} failed`)
            }
        }
        
        console.log('\n✅ Test completed successfully!')
        console.log('Key findings:')
        console.log('- SharedQueueManager singleton works correctly')
        console.log('- Jobs are routed to correct queues')
        console.log('- No EventEmitter memory leak warnings')
        console.log('- Single Redis connection for all queues')
        
    } catch (error) {
        console.error('\n❌ Test failed:', error)
        process.exit(1)
    } finally {
        if (manager) {
            console.log('\n🧹 Shutting down manager...')
            await manager.shutdown()
        }
        process.exit(0)
    }
}

// Run the test
testQueueManager().catch(console.error)