/**
 * Example: Using Redis/Bull Queue Integration with Baileys MongoDB Store
 * 
 * This example demonstrates how to configure the MongoDB store with Redis/Bull
 * for robust queue handling of label associations and messages.
 */

const { makeMongoDBStore } = require('../dist')
const makeWASocket = require('baileys').default
const { useMultiFileAuthState } = require('baileys')

async function connectWithRedisQueue() {
    // Configure MongoDB Store with Redis/Bull integration
    const storeConfig = {
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_store',
        instanceId: 'instance_001',
        ttlDays: 30,
        
        // Set to 'warn' to see warnings about queue operations
        logLevel: 'warn',
        
        // Redis/Bull configuration
        redis: {
            // Option 1: Simple connection string
            connection: 'redis://localhost:6379',
            // Option 2: Detailed connection options
            // connection: {
            //     host: 'localhost',
            //     port: 6379,
            //     password: 'your-redis-password',
            //     db: 0,
            //     tls: {} // for TLS connections
            // },
            
            // Queue configuration
            queuePrefix: 'baileys',         // Prefix for queue names
            enableLabelQueue: true,         // Use Bull for label associations (default: true)
            enableMessageQueue: false,      // Use Bull for messages (default: false)
            concurrency: 50,                // Max concurrent jobs
            removeOnComplete: 3600,         // Remove completed jobs after 1 hour
            removeOnFail: 86400,           // Remove failed jobs after 24 hours
        }
    }
    
    // Create the store with Redis/Bull integration
    const store = await makeMongoDBStore(storeConfig)
    
    // Setup WhatsApp socket
    const { state, saveCreds } = await useMultiFileAuthState('./auth_info')
    
    const sock = makeWASocket({
        auth: state,
        printQRInTerminal: true
    })
    
    // Bind store to socket events
    store.bind(sock.ev)
    
    // Save credentials when updated
    sock.ev.on('creds.update', saveCreds)
    
    // Monitor connection status
    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect } = update
        
        if (connection === 'close') {
            const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== 401
            console.log('Connection closed, reconnecting:', shouldReconnect)
            
            if (shouldReconnect) {
                connectWithRedisQueue()
            }
        } else if (connection === 'open') {
            console.log('Connected to WhatsApp')
            
            // Check store performance stats
            const stats = store.getPerformanceStats()
            console.log('Store Statistics:', {
                messages: stats.messagesProcessed,
                labels: stats.labelsProcessed,
                batches: stats.batchesProcessed,
                errors: stats.errors,
                uptime: `${Math.floor(stats.uptime / 1000)}s`,
                bullStatus: stats.bullStats,
                labelQueue: stats.labelStats
            })
        }
    })
    
    // Handle label events with Bull queue
    sock.ev.on('labels.association', async ({ type, association }) => {
        console.log(`Label ${type}:`, association)
        // The store will automatically use Bull queue if configured
        // or fall back to in-memory processing if Redis is unavailable
    })
    
    // Monitor Bull queue health
    setInterval(async () => {
        const stats = store.getPerformanceStats()
        if (stats.bullStats?.initialized) {
            console.log('Bull Queue Status:', {
                labelQueue: stats.bullStats.labelQueue,
                messageQueue: stats.bullStats.messageQueue,
                redisConnected: stats.bullStats.redisConnected,
                labelsInQueue: stats.labelStats.currentQueueSize,
                labelsProcessed: stats.labelStats.totalProcessed,
                labelsReceived: stats.labelStats.totalReceived
            })
        }
    }, 30000) // Check every 30 seconds
    
    // Graceful shutdown
    process.on('SIGINT', async () => {
        console.log('Shutting down gracefully...')
        
        // Flush any pending label associations
        await store.flushLabelAssociations()
        
        // Close connections
        await store.close()
        sock.end()
        
        process.exit(0)
    })
    
    return { sock, store }
}

// Example: Testing with multiple instances using same Redis
async function multiInstanceExample() {
    const instances = []
    
    // Create multiple instances sharing the same Redis
    for (let i = 1; i <= 3; i++) {
        const store = await makeMongoDBStore({
            uri: 'mongodb://localhost:27017',
            database: 'whatsapp_store',
            instanceId: `instance_${i.toString().padStart(3, '0')}`,
            redis: {
                connection: 'redis://localhost:6379',
                queuePrefix: 'baileys',
                enableLabelQueue: true,
                concurrency: 20 // Lower concurrency per instance
            }
        })
        
        instances.push(store)
        console.log(`Instance ${i} initialized with Bull queue`)
    }
    
    // Each instance has its own queue but shares Redis infrastructure
    // This allows for distributed processing and better resource utilization
    
    // Clean up
    setTimeout(async () => {
        for (const store of instances) {
            await store.close()
        }
    }, 60000)
}

// Example: Fallback behavior when Redis is unavailable
async function fallbackExample() {
    const store = await makeMongoDBStore({
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_store',
        instanceId: 'fallback_test',
        redis: {
            // Invalid Redis connection to demonstrate fallback
            connection: 'redis://invalid-host:6379',
            enableLabelQueue: true
        }
    })
    
    // The store will automatically fall back to in-memory processing
    // Check the stats to confirm
    const stats = store.getPerformanceStats()
    console.log('Fallback mode active:', !stats.bullStats.initialized)
    console.log('Fallback reason:', stats.bullStats.reason)
    
    // The store still works normally, just without Bull queue benefits
    await store.upsertLabel('label_1', {
        id: 'label_1',
        name: 'Important',
        predefinedId: '0',
        color: 1,
        deleted: false
    })
    
    await store.close()
}

// Run the example
if (require.main === module) {
    connectWithRedisQueue().catch(console.error)
    
    // Uncomment to test other examples:
    // multiInstanceExample().catch(console.error)
    // fallbackExample().catch(console.error)
}

module.exports = { connectWithRedisQueue }