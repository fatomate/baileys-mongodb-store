/**
 * Complete example showing MongoDB store usage with Baileys
 * Now includes Redis/Bull queue integration for production reliability
 */

const { default: makeWASocket, DisconnectReason, useMultiFileAuthState } = require('@whiskeysockets/baileys')
const { Boom } = require('@hapi/boom')
const { makeMongoDBStore, cleanupMongoDBStore } = require('../dist')
const pino = require('pino')

const logger = pino({ level: 'info' })

async function connectToWhatsApp() {
    // MongoDB store configuration with Redis/Bull queues
    const store = await makeMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: process.env.MONGODB_DB || 'whatsapp_bot',
        instanceId: process.env.INSTANCE_ID || 'main_instance',
        ttlDays: parseInt(process.env.TTL_DAYS) || 30,
        logger: logger.child({ module: 'mongodb-store' }),
        
        // Set log level for debugging
        // LOG_LEVEL=all for all logs, warn for warnings only, error for errors only
        logLevel: process.env.LOG_LEVEL || 'none',
        
        // Redis/Bull configuration for production reliability
        redis: process.env.REDIS_URL ? {
            connection: process.env.REDIS_URL,
            queuePrefix: 'wa-bot',
            concurrency: parseInt(process.env.QUEUE_CONCURRENCY) || 50
        } : undefined
    })

    // Check if Bull queues are active
    const stats = store.getPerformanceStats()
    if (stats.bullStats?.initialized) {
        logger.info('🚀 Redis/Bull queues initialized successfully', {
            totalQueues: stats.bullStats.totalQueues,
            redisConnected: stats.bullStats.redisConnected,
            queues: Object.keys(stats.bullStats.queues)
        })
    } else {
        logger.info('💾 Using in-memory processing', {
            reason: stats.bullStats?.reason || 'Redis not configured'
        })
    }

    logger.info('MongoDB store initialized')

    // Auth state (similar to useRedisAuthState)
    const { state, saveCreds } = await useMultiFileAuthState('./auth_info_baileys')

    // Create WhatsApp socket
    const sock = makeWASocket({
        auth: state,
        printQRInTerminal: true,
        logger,
        // Use MongoDB store for message retrieval
        getMessage: async (key) => {
            const msg = await store.loadMessage(key.remoteJid, key.id)
            return msg?.message || null
        }
    })

    // Bind store to socket events
    store.bind(sock.ev)

    // Connection handler
    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update

        if (qr) {
            console.log('QR Code received, scan with WhatsApp')
        }

        if (connection === 'close') {
            const shouldReconnect = (lastDisconnect?.error)?.output?.statusCode !== DisconnectReason.loggedOut
            logger.info(`Connection closed due to ${lastDisconnect?.error}, reconnecting: ${shouldReconnect}`)

            if (shouldReconnect) {
                connectToWhatsApp()
            } else {
                // Graceful shutdown on logout
                await store.close()
            }
        } else if (connection === 'open') {
            logger.info('WhatsApp connection opened successfully')

            // Fetch and store all group metadata on connection
            logger.info('Fetching all group metadata...')
            const groups = await sock.groupFetchAllParticipating()
            logger.info(`Found ${Object.keys(groups).length} groups`)
            
            // IMPORTANT: Groups are automatically saved via store.bind(sock.ev)
            // The store listens for groups.upsert events that are triggered
            // when groupFetchAllParticipating completes
            
            // Optional: Manually save if you need immediate persistence
            // or if you're not using store.bind()
            // for (const [groupId, metadata] of Object.entries(groups)) {
            //     await store.upsertGroupMetadata(groupId, metadata)
            // }

            // Example: Access store data
            const chats = await store.getChats()
            const contacts = await store.getContacts()
            logger.info(`Loaded ${chats.length} chats from MongoDB`)
            logger.info(`Loaded ${Object.keys(contacts).length} contacts`)
            logger.info(`Loaded ${Object.keys(groups).length} groups`)
            
            // Log queue statistics
            const currentStats = store.getPerformanceStats()
            logger.info('Store statistics:', {
                messagesProcessed: currentStats.messagesProcessed,
                labelsProcessed: currentStats.labelsProcessed,
                errors: currentStats.errors,
                uptime: Math.floor(currentStats.uptime / 1000) + 's'
            })
        }
    })

    // Save credentials
    sock.ev.on('creds.update', saveCreds)

    // Message handler
    sock.ev.on('messages.upsert', async ({ messages, type }) => {
        logger.info(`Received ${messages.length} new messages (type: ${type})`)

        for (const msg of messages) {
            // Skip own messages
            if (msg.key.fromMe) continue

            const messageContent = msg.message?.conversation || 
                                 msg.message?.extendedTextMessage?.text || ''

            if (!messageContent) continue

            logger.info(`Message from ${msg.key.remoteJid}: ${messageContent}`)

            // Example commands
            if (messageContent.toLowerCase() === '!ping') {
                await sock.sendMessage(msg.key.remoteJid, { 
                    text: 'Pong! 🏓' 
                })
            }

            if (messageContent.toLowerCase() === '!stats') {
                const chats = await store.getChats()
                const contacts = await store.getContacts()
                const storeStats = store.getPerformanceStats()
                
                const statsText = `📊 Bot Statistics:\n\n` +
                    `• Active Chats: ${chats.length}\n` +
                    `• Saved Contacts: ${Object.keys(contacts).length}\n` +
                    `• Instance ID: ${store.instanceId}\n` +
                    `• Messages Processed: ${storeStats.messagesProcessed}\n` +
                    `• Labels Processed: ${storeStats.labelsProcessed}\n` +
                    `• Queue System: ${storeStats.bullStats?.initialized ? 'Redis/Bull ⚡' : 'In-Memory 💾'}\n` +
                    `• Errors: ${storeStats.errors}\n` +
                    `• Uptime: ${Math.floor(storeStats.uptime / 1000 / 60)}m`
                
                await sock.sendMessage(msg.key.remoteJid, { text: statsText })
            }

            // Show group info if in a group
            if (messageContent.toLowerCase() === '!groupinfo' && msg.key.remoteJid.endsWith('@g.us')) {
                const groupId = msg.key.remoteJid
                const metadata = await store.getGroupMetadata(groupId)
                
                if (metadata) {
                    const groupInfo = `👥 Group Information:\n\n` +
                        `• Name: ${metadata.subject}\n` +
                        `• ID: ${groupId}\n` +
                        `• Owner: ${metadata.owner || 'Unknown'}\n` +
                        `• Participants: ${metadata.participants.length}\n` +
                        `• Admins: ${metadata.participants.filter(p => p.isAdmin).length}\n` +
                        `• Created: ${metadata.creation ? new Date(metadata.creation * 1000).toLocaleString() : 'Unknown'}\n` +
                        `• Description: ${metadata.desc || 'No description'}`
                    
                    await sock.sendMessage(msg.key.remoteJid, { text: groupInfo })
                } else {
                    await sock.sendMessage(msg.key.remoteJid, { text: '❌ Group metadata not found' })
                }
            }

            if (messageContent.toLowerCase() === '!queues') {
                const storeStats = store.getPerformanceStats()
                
                if (storeStats.bullStats?.initialized) {
                    const queueText = `🔄 Queue Status:\n\n` +
                        `• Redis Connected: ${storeStats.bullStats.redisConnected ? '✅' : '❌'}\n` +
                        `• Total Queues: ${storeStats.bullStats.totalQueues}\n` +
                        `• Queue Types: ${Object.keys(storeStats.bullStats.queues).join(', ')}\n` +
                        `• Label Associations: ${storeStats.labelStats.totalProcessed} processed\n` +
                        `• Current Queue Size: ${storeStats.labelStats.currentQueueSize}`
                    
                    await sock.sendMessage(msg.key.remoteJid, { text: queueText })
                } else {
                    await sock.sendMessage(msg.key.remoteJid, { 
                        text: `💾 In-Memory Mode\n\nReason: ${storeStats.bullStats?.reason || 'Redis not configured'}` 
                    })
                }
            }

            if (messageContent.toLowerCase() === '!help') {
                await sock.sendMessage(msg.key.remoteJid, { 
                    text: `🤖 Available Commands:\n\n` +
                          `!ping - Check if bot is online\n` +
                          `!stats - View bot statistics\n` +
                          `!queues - View queue status\n` +
                          `!help - Show this help message`
                })
            }
        }
    })

    // Group update handler
    sock.ev.on('groups.update', async (updates) => {
        for (const update of updates) {
            const metadata = await store.getGroupMetadata(update.id)
            if (metadata) {
                logger.info(`Group ${metadata.subject} updated`)
            }
        }
    })

    // Chat update handler
    sock.ev.on('chats.update', async (updates) => {
        logger.info(`${updates.length} chats updated`)
    })

    // Labels update handler (shows Bull queue in action)
    sock.ev.on('labels.association', ({ type, association }) => {
        logger.info(`Label ${type}: ${association.labelId} -> ${association.chatId}`, {
            queuedToBull: stats.bullStats?.initialized || false
        })
    })

    // Set up performance monitoring
    setupPerformanceMonitoring(store, logger)

    return { sock, store }
}

// Performance monitoring function
function setupPerformanceMonitoring(store, logger) {
    // Log performance stats every 5 minutes
    setInterval(() => {
        const stats = store.getPerformanceStats()
        
        if (stats.messagesProcessed > 0 || stats.labelsProcessed > 0) {
            logger.info('📈 Performance Report', {
                messages: stats.messagesProcessed,
                labels: stats.labelsProcessed,
                batches: stats.batchesProcessed,
                errors: stats.errors,
                uptime: Math.floor(stats.uptime / 1000 / 60) + 'm',
                bullActive: stats.bullStats?.initialized || false,
                redisConnected: stats.bullStats?.redisConnected
            })
        }
    }, 5 * 60 * 1000) // Every 5 minutes

    // Health check every minute
    setInterval(() => {
        const stats = store.getPerformanceStats()
        
        // Alert if Redis disconnected
        if (stats.bullStats?.initialized && !stats.bullStats.redisConnected) {
            logger.warn('🚨 Redis connection lost - running in fallback mode')
        }
        
        // Alert if error rate is high
        if (stats.errors > 0 && stats.messagesProcessed > 0) {
            const errorRate = (stats.errors / stats.messagesProcessed) * 100
            if (errorRate > 1) {
                logger.warn(`⚠️ High error rate: ${errorRate.toFixed(2)}%`)
            }
        }
    }, 60 * 1000) // Every minute
}

// Main function
async function main() {
    try {
        const { sock, store } = await connectToWhatsApp()

        // Graceful shutdown handler
        process.on('SIGINT', async () => {
            logger.info('Shutting down gracefully...')
            
            try {
                // Flush any pending operations
                await store.flushLabelAssociations()
                
                // Close socket
                sock.end()
                
                // Close store (includes Bull queues)
                await store.close()
                
                logger.info('Shutdown complete')
                process.exit(0)
            } catch (error) {
                logger.error('Error during shutdown:', error)
                process.exit(1)
            }
        })

        // Example: Cleanup specific instance on demand
        process.on('SIGUSR1', async () => {
            logger.info('Cleaning up instance data...')
            await cleanupMongoDBStore(store.instanceId, true)
            logger.info('Instance data cleaned up')
        })

        // Error handler
        process.on('unhandledRejection', (err) => {
            logger.error('Unhandled rejection:', err)
            // Don't exit immediately, let health checks handle it
        })

        process.on('uncaughtException', (err) => {
            logger.error('Uncaught exception:', err)
            process.exit(1)
        })

    } catch (error) {
        logger.error('Failed to start:', error)
        process.exit(1)
    }
}

// Environment variable guide
if (require.main === module) {
    console.log('🚀 Starting WhatsApp Bot with MongoDB Store + Redis/Bull Queues\n')
    
    console.log('📋 Environment Variables:')
    console.log('  MONGODB_URI     - MongoDB connection string')
    console.log('  MONGODB_DB      - Database name')
    console.log('  REDIS_URL       - Redis connection string (optional)')
    console.log('  INSTANCE_ID     - Unique instance identifier')
    console.log('  TTL_DAYS        - Data retention period')
    console.log('  QUEUE_CONCURRENCY - Queue processing concurrency\n')
    
    if (!process.env.REDIS_URL) {
        console.log('ℹ️  Redis not configured - will use in-memory processing')
        console.log('   For production, set REDIS_URL for better reliability\n')
    }
    
    // Run the bot
    main()
}

module.exports = { connectToWhatsApp, setupPerformanceMonitoring }