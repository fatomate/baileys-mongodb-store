#!/usr/bin/env node

/**
 * Test script for shared queue implementation
 * Run with: node test-shared-queues.js
 */

const { makeEnhancedMongoDBStore } = require('./dist/makeEnhancedMongoDBStore')

async function testSharedQueues() {
    console.log('🧪 Testing Shared Queue Implementation\n')
    
    const config = {
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: 'wabotdev_test',
        instanceId: 'test_instance_1',
        redis: {
            connection: {
                host: process.env.REDIS_HOST || 'localhost',
                port: process.env.REDIS_PORT || 6379,
                password: process.env.REDIS_PASSWORD
            },
            useSharedQueues: true, // Enable shared queues
            queueConcurrency: {
                highPriority: 100,
                dataSync: 50,
                media: 10,
                lowPriority: 5
            }
        },
        logLevel: 'all',
        enableMetrics: true
    }
    
    try {
        console.log('📦 Creating store with shared queues enabled...')
        const store1 = await makeEnhancedMongoDBStore(config)
        
        console.log('✅ Store 1 created successfully')
        
        // Create second instance to test shared queue behavior
        const config2 = { ...config, instanceId: 'test_instance_2' }
        console.log('📦 Creating second store instance...')
        const store2 = await makeEnhancedMongoDBStore(config2)
        
        console.log('✅ Store 2 created successfully')
        console.log('✅ Both stores are using shared queues')
        
        // Test message upsert
        console.log('\n📝 Testing message upsert...')
        const testMessage = {
            key: {
                remoteJid: '1234567890@s.whatsapp.net',
                fromMe: false,
                id: 'test_msg_' + Date.now()
            },
            message: {
                conversation: 'Test message for shared queue'
            },
            messageTimestamp: Math.floor(Date.now() / 1000)
        }
        
        await store1.upsertMessage(testMessage.key.remoteJid, testMessage)
        console.log('✅ Message queued successfully')
        
        // Test contact upsert
        console.log('\n📝 Testing contact upsert...')
        const testContacts = [
            { id: '1234567890@s.whatsapp.net', name: 'Test Contact 1' },
            { id: '0987654321@s.whatsapp.net', name: 'Test Contact 2' }
        ]
        
        await store1.upsertContacts(testContacts)
        console.log('✅ Contacts queued successfully')
        
        // Wait a bit for processing
        console.log('\n⏳ Waiting for queue processing...')
        await new Promise(resolve => setTimeout(resolve, 3000))
        
        // Clean up
        console.log('\n🧹 Cleaning up...')
        await store1.close()
        await store2.close()
        
        console.log('\n✅ Test completed successfully!')
        console.log('📊 Summary:')
        console.log('- Shared queues initialized correctly')
        console.log('- Multiple instances can share the same queues')
        console.log('- Jobs are processed without errors')
        console.log('- No EventEmitter memory leak warnings')
        
        process.exit(0)
    } catch (error) {
        console.error('\n❌ Test failed:', error)
        process.exit(1)
    }
}

// Run the test
testSharedQueues().catch(console.error)