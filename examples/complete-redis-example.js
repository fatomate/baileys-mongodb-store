/**
 * Complete example of using Baileys MongoDB Store with Redis/Bull Queue Integration
 * 
 * This example shows the recommended configuration and best practices for production use.
 */

const { makeMongoDBStore } = require('../dist')
const makeWASocket = require('baileys').default
const { useMultiFileAuthState, DisconnectReason } = require('baileys')
const { Boom } = require('@hapi/boom')
const Redis = require('ioredis')

// Configuration
const CONFIG = {
    mongodb: {
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: process.env.MONGODB_DB || 'whatsapp_store'
    },
    redis: {
        connection: process.env.REDIS_URL || 'redis://localhost:6379',
        // For production with authentication:
        // connection: {
        //     host: 'redis.example.com',
        //     port: 6379,
        //     password: 'your-redis-password',
        //     db: 0,
        //     retryStrategy: (times) => Math.min(times * 50, 2000)
        // }
    },
    instanceId: process.env.INSTANCE_ID || 'production_001',
    authFolder: './auth_info'
}

// Main connection function
async function connectToWhatsApp() {
    console.log('🚀 Starting WhatsApp connection with Redis/Bull queues...\n')
    
    // Create MongoDB store with Redis/Bull integration
    const store = await makeMongoDBStore({
        uri: CONFIG.mongodb.uri,
        database: CONFIG.mongodb.database,
        instanceId: CONFIG.instanceId,
        ttlDays: 30, // Keep data for 30 days
        
        // Redis/Bull configuration for production
        redis: {
            connection: CONFIG.redis.connection,
            queuePrefix: 'wa-prod',    // Use a meaningful prefix
            concurrency: 50,           // Process 50 jobs concurrently (except labels)
            // Note: Label associations automatically use concurrency: 1
        }
    })
    
    // Verify Bull queue initialization
    const stats = store.getPerformanceStats()
    if (stats.bullStats.initialized) {
        console.log('✅ Bull queues initialized successfully')
        console.log(`   Total queues: ${stats.bullStats.totalQueues}`)
        console.log(`   Queue types: ${Object.keys(stats.bullStats.queues).join(', ')}`)
        console.log(`   Redis connected: ${stats.bullStats.redisConnected}`)
    } else {
        console.warn('⚠️ Bull queues not initialized, using in-memory fallback')
        console.warn(`   Reason: ${stats.bullStats.reason}`)
    }
    
    // Setup WhatsApp authentication
    const { state, saveCreds } = await useMultiFileAuthState(CONFIG.authFolder)
    
    // Create WhatsApp socket
    const sock = makeWASocket({
        auth: state,
        printQRInTerminal: true,
        // Additional recommended options for production
        syncFullHistory: false,
        getMessage: async (key) => {
            // Retrieve message from store for retries
            const msg = await store.getMessage(key.remoteJid!, key.id!)
            return msg?.message || undefined
        }
    })
    
    // IMPORTANT: Bind store to socket events
    store.bind(sock.ev)
    
    // Save credentials when updated
    sock.ev.on('creds.update', saveCreds)
    
    // Connection update handler
    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update
        
        if (qr) {
            console.log('📱 Scan QR code to connect')
        }
        
        if (connection === 'close') {
            const shouldReconnect = (lastDisconnect?.error as Boom)?.output?.statusCode !== DisconnectReason.loggedOut
            console.log('Connection closed due to', lastDisconnect?.error, ', reconnecting:', shouldReconnect)
            
            if (shouldReconnect) {
                // Reconnect with exponential backoff
                const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000)
                reconnectAttempts++
                console.log(`Reconnecting in ${delay / 1000} seconds...`)
                setTimeout(() => connectToWhatsApp(), delay)
            } else {
                // Logged out - clean up
                await store.close()
                process.exit(0)
            }
        } else if (connection === 'open') {
            console.log('✅ Connected to WhatsApp')
            reconnectAttempts = 0
            
            // Log initial statistics
            logStatistics(store)
        }
    })
    
    // Monitor specific events to see Bull queues in action
    sock.ev.on('chats.upsert', (chats) => {
        console.log(`📝 Received ${chats.length} chat(s) - queued to Bull`)
    })
    
    sock.ev.on('messages.upsert', ({ messages }) => {
        console.log(`💬 Received ${messages.length} message(s) - queued to Bull`)
        
        // Example: Process received messages
        for (const msg of messages) {
            if (!msg.key.fromMe && msg.message?.conversation) {
                console.log(`   From ${msg.key.remoteJid}: ${msg.message.conversation}`)
            }
        }
    })
    
    sock.ev.on('labels.association', ({ type, association }) => {
        // Label associations use concurrency: 1 to maintain order
        console.log(`🏷️ Label ${type}: ${association.labelId} -> ${association.chatId}`)
    })
    
    // Setup monitoring and health checks
    setupMonitoring(store, sock)
    
    // Graceful shutdown handler
    setupGracefulShutdown(store, sock)
    
    return { sock, store }
}

// Monitoring function
function setupMonitoring(store, sock) {
    // Performance monitoring every 30 seconds
    setInterval(() => {
        const stats = store.getPerformanceStats()
        
        // Only log if there's activity
        if (stats.messagesProcessed > 0 || stats.labelsProcessed > 0) {
            console.log('\n📊 Performance Statistics:')
            console.log(`   Messages processed: ${stats.messagesProcessed}`)
            console.log(`   Labels processed: ${stats.labelsProcessed}`)
            console.log(`   Batches processed: ${stats.batchesProcessed}`)
            console.log(`   Errors: ${stats.errors}`)
            console.log(`   Uptime: ${Math.floor(stats.uptime / 1000 / 60)} minutes`)
            
            if (stats.bullStats.initialized) {
                console.log('   Bull Queue Status:')
                console.log(`     Redis connected: ${stats.bullStats.redisConnected}`)
                console.log(`     Active queues: ${stats.bullStats.totalQueues}`)
            }
        }
    }, 30000)
    
    // Health check for queue system
    setInterval(async () => {
        const stats = store.getPerformanceStats()
        
        // Alert if Redis connection lost
        if (stats.bullStats.initialized && !stats.bullStats.redisConnected) {
            console.error('⚠️ ALERT: Redis connection lost! Falling back to in-memory processing')
            // Here you could send an alert to your monitoring system
        }
        
        // Alert if error rate is high
        if (stats.errors > 0 && stats.messagesProcessed > 0) {
            const errorRate = (stats.errors / stats.messagesProcessed) * 100
            if (errorRate > 1) {
                console.warn(`⚠️ High error rate detected: ${errorRate.toFixed(2)}%`)
            }
        }
    }, 60000) // Check every minute
}

// Graceful shutdown
function setupGracefulShutdown(store, sock) {
    const shutdown = async (signal) => {
        console.log(`\n📛 ${signal} received, shutting down gracefully...`)
        
        try {
            // Stop accepting new connections
            sock.ws.close()
            
            // Give time for pending operations
            console.log('⏳ Waiting for pending operations...')
            await new Promise(resolve => setTimeout(resolve, 2000))
            
            // Flush any pending label associations
            console.log('💾 Flushing pending data...')
            await store.flushLabelAssociations()
            
            // Get final statistics
            const finalStats = store.getPerformanceStats()
            console.log('\n📊 Final Statistics:')
            console.log(`   Total messages processed: ${finalStats.messagesProcessed}`)
            console.log(`   Total labels processed: ${finalStats.labelsProcessed}`)
            console.log(`   Total errors: ${finalStats.errors}`)
            
            // Close store connection (this also closes Bull queues)
            console.log('🔌 Closing connections...')
            await store.close()
            
            console.log('✅ Shutdown complete')
            process.exit(0)
        } catch (error) {
            console.error('❌ Error during shutdown:', error)
            process.exit(1)
        }
    }
    
    // Register shutdown handlers
    process.on('SIGTERM', () => shutdown('SIGTERM'))
    process.on('SIGINT', () => shutdown('SIGINT'))
    
    // Handle uncaught errors
    process.on('uncaughtException', (error) => {
        console.error('❌ Uncaught Exception:', error)
        shutdown('UNCAUGHT_EXCEPTION')
    })
    
    process.on('unhandledRejection', (reason, promise) => {
        console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason)
        // Don't exit on unhandled rejection, just log it
    })
}

// Statistics logging
function logStatistics(store) {
    const stats = store.getPerformanceStats()
    console.log('\n📈 Current Store Statistics:')
    console.log(`   Instance ID: ${store.instanceId}`)
    console.log(`   Bull Queues: ${stats.bullStats.initialized ? 'Active' : 'Inactive'}`)
    if (stats.bullStats.initialized) {
        console.log(`   Queue Types: ${Object.keys(stats.bullStats.queues).join(', ')}`)
        console.log(`   Redis Status: ${stats.bullStats.redisConnected ? 'Connected' : 'Disconnected'}`)
    }
}

// Helper to test Redis connection
async function testRedisConnection() {
    try {
        const redis = new Redis(CONFIG.redis.connection)
        await redis.ping()
        console.log('✅ Redis connection test successful')
        
        // Check if Bull queues exist
        const keys = await redis.keys('bull:wa-prod:*')
        if (keys.length > 0) {
            console.log(`   Found ${keys.length} existing Bull queue keys`)
        }
        
        await redis.quit()
        return true
    } catch (error) {
        console.error('❌ Redis connection test failed:', error.message)
        return false
    }
}

// Reconnection counter
let reconnectAttempts = 0

// Main execution
async function main() {
    console.log('=====================================')
    console.log('WhatsApp Bot with Redis/Bull Queues')
    console.log('=====================================\n')
    
    // Test Redis connection first
    console.log('🔍 Testing Redis connection...')
    const redisOk = await testRedisConnection()
    
    if (!redisOk) {
        console.warn('\n⚠️ Redis not available - will use in-memory fallback')
        console.warn('   For production, ensure Redis is running and accessible\n')
    }
    
    // Start WhatsApp connection
    try {
        const { sock, store } = await connectToWhatsApp()
        
        // Example: Send a message after connection
        sock.ev.on('connection.update', async (update) => {
            if (update.connection === 'open') {
                // Example: Send a startup message to yourself
                // const myNumber = sock.user?.id.split(':')[0] + '@s.whatsapp.net'
                // await sock.sendMessage(myNumber, { text: '🤖 Bot started with Redis/Bull queues!' })
            }
        })
        
    } catch (error) {
        console.error('❌ Failed to start:', error)
        process.exit(1)
    }
}

// Run the bot
if (require.main === module) {
    main().catch(console.error)
}

module.exports = { connectToWhatsApp, testRedisConnection }