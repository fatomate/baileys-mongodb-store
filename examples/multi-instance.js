const { default: makeWASocket, DisconnectReason, useMultiFileAuthState } = require('@whiskeysockets/baileys')
const { Boom } = require('@hapi/boom')
const { makeMongoDBStore, cleanupMongoDBStore } = require('../dist')
const pino = require('pino')

const logger = pino({ level: 'info' })

// WhatsApp instance configuration
const instances = [
    {
        id: 'support_bot',
        name: 'Customer Support',
        authFolder: './auth_support',
        ttlDays: 60 // Keep support chats for 60 days
    },
    {
        id: 'sales_bot',
        name: 'Sales Team',
        authFolder: './auth_sales',
        ttlDays: 90 // Keep sales chats for 90 days
    },
    {
        id: 'notification_bot',
        name: 'Notifications',
        authFolder: './auth_notifications',
        ttlDays: 7 // Keep notification data for 7 days only
    }
]

async function createWhatsAppInstance(instance) {
    const instanceLogger = logger.child({ instance: instance.id })
    
    // Create MongoDB store for this instance with Redis/Bull queues
    const store = await makeMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: 'whatsapp_multi_instance',
        instanceId: instance.id,
        ttlDays: instance.ttlDays,
        logger: instanceLogger,
        
        // Redis/Bull configuration - each instance gets isolated queues
        redis: process.env.REDIS_URL ? {
            connection: process.env.REDIS_URL,
            queuePrefix: 'multi-wa', // Queues will be like 'multi-wa:messages:support_bot'
            concurrency: 25 // Lower concurrency per instance when running multiple
        } : undefined
    })
    
    // Log queue status for this instance
    const stats = store.getPerformanceStats()
    if (stats.bullStats?.initialized) {
        instanceLogger.info(`Bull queues active for ${instance.name}:`, {
            queues: stats.bullStats.totalQueues,
            redis: stats.bullStats.redisConnected,
            queueTypes: Object.keys(stats.bullStats.queues)
        })
    } else {
        instanceLogger.info(`${instance.name} - Using in-memory processing:`, {
            reason: stats.bullStats?.reason || 'Redis not configured'
        })
    }

    instanceLogger.info(`MongoDB store created for ${instance.name}`)

    // Multi-file auth state per instance
    const { state, saveCreds } = await useMultiFileAuthState(instance.authFolder)

    // Create socket for this instance
    const sock = makeWASocket({
        auth: state,
        printQRInTerminal: true,
        logger: instanceLogger,
        getMessage: async (key) => {
            const msg = await store.loadMessage(key.remoteJid, key.id)
            return msg?.message || null
        }
    })

    // Bind store to this instance's events
    store.bind(sock.ev)

    // Connection handler
    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update

        if (connection === 'close') {
            const shouldReconnect = (lastDisconnect?.error)?.output?.statusCode !== DisconnectReason.loggedOut
            instanceLogger.info(`${instance.name} connection closed, reconnecting: ${shouldReconnect}`)

            if (shouldReconnect) {
                // Recreate this instance
                setTimeout(() => createWhatsAppInstance(instance), 5000)
            }
        } else if (connection === 'open') {
            instanceLogger.info(`${instance.name} connected successfully`)
            
            // Log instance statistics
            const chats = await store.getChats()
            const contacts = await store.getContacts()
            const currentStats = store.getPerformanceStats()
            
            instanceLogger.info(`${instance.name} loaded:`, {
                chats: chats.length,
                contacts: Object.keys(contacts).length,
                bullActive: currentStats.bullStats?.initialized || false,
                queuesActive: currentStats.bullStats?.totalQueues || 0
            })
        }
    })

    // Save credentials
    sock.ev.on('creds.update', saveCreds)

    // Instance-specific message handler
    sock.ev.on('messages.upsert', async ({ messages, type }) => {
        instanceLogger.info(`${instance.name} received ${messages.length} messages (type: ${type})`)

        for (const msg of messages) {
            if (msg.key.fromMe) continue

            const messageContent = msg.message?.conversation || 
                                 msg.message?.extendedTextMessage?.text || ''

            if (!messageContent) continue

            // Instance-specific responses
            if (messageContent.toLowerCase() === '!info') {
                const stats = store.getPerformanceStats()
                const responseText = `🤖 ${instance.name}\n\n` +
                    `Instance ID: ${instance.id}\n` +
                    `Queue System: ${stats.bullStats?.initialized ? 'Redis/Bull ⚡' : 'In-Memory 💾'}\n` +
                    `Messages Processed: ${stats.messagesProcessed}\n` +
                    `Labels Processed: ${stats.labelsProcessed}\n` +
                    `Uptime: ${Math.floor(stats.uptime / 1000 / 60)}m`

                await sock.sendMessage(msg.key.remoteJid, { text: responseText })
            }

            // Instance-specific behavior
            if (instance.id === 'support_bot' && messageContent.toLowerCase().includes('help')) {
                await sock.sendMessage(msg.key.remoteJid, {
                    text: '🆘 Customer Support Bot\n\nHow can I help you today? Please describe your issue.'
                })
            }

            if (instance.id === 'sales_bot' && messageContent.toLowerCase().includes('price')) {
                await sock.sendMessage(msg.key.remoteJid, {
                    text: '💰 Sales Bot\n\nI can help you with pricing information! What product are you interested in?'
                })
            }

            if (instance.id === 'notification_bot') {
                // Notification bot only sends, doesn't respond to messages
                instanceLogger.info(`Notification received from ${msg.key.remoteJid}: ${messageContent}`)
            }
        }
    })

    return { instance, sock, store }
}

// Monitor all instances
function setupMultiInstanceMonitoring(instanceData) {
    setInterval(() => {
        logger.info('📊 Multi-Instance Status Report:')
        
        instanceData.forEach(({ instance, store }) => {
            const stats = store.getPerformanceStats()
            
            logger.info(`  ${instance.name} (${instance.id}):`, {
                messages: stats.messagesProcessed,
                labels: stats.labelsProcessed,
                errors: stats.errors,
                bullActive: stats.bullStats?.initialized || false,
                redisConnected: stats.bullStats?.redisConnected,
                uptime: Math.floor(stats.uptime / 1000 / 60) + 'm'
            })
        })
    }, 5 * 60 * 1000) // Every 5 minutes
}

// Main function to start all instances
async function startAllInstances() {
    console.log('🚀 Starting Multi-Instance WhatsApp Bots with Redis/Bull Queues\n')
    
    console.log('📋 Configuration:')
    console.log(`  MongoDB: ${process.env.MONGODB_URI || 'mongodb://localhost:27017'}`)
    console.log(`  Redis: ${process.env.REDIS_URL || 'Not configured (will use in-memory)'}`)
    console.log(`  Instances: ${instances.length}\n`)
    
    const instanceData = []

    // Create all instances
    for (const instance of instances) {
        try {
            logger.info(`Creating instance: ${instance.name}`)
            const data = await createWhatsAppInstance(instance)
            instanceData.push(data)
            
            // Small delay between instances
            await new Promise(resolve => setTimeout(resolve, 2000))
        } catch (error) {
            logger.error(`Failed to create instance ${instance.name}:`, error)
        }
    }

    logger.info(`Successfully created ${instanceData.length}/${instances.length} instances`)

    // Setup monitoring
    setupMultiInstanceMonitoring(instanceData)

    // Graceful shutdown
    process.on('SIGINT', async () => {
        logger.info('Shutting down all instances...')

        for (const { instance, sock, store } of instanceData) {
            try {
                logger.info(`Shutting down ${instance.name}...`)
                
                // Close socket
                sock.end()
                
                // Flush pending operations
                await store.flushLabelAssociations()
                
                // Close store (includes Bull queues)
                await store.close()
                
                logger.info(`${instance.name} shut down successfully`)
            } catch (error) {
                logger.error(`Error shutting down ${instance.name}:`, error)
            }
        }

        logger.info('All instances shut down')
        process.exit(0)
    })

    // Handle cleanup signal
    process.on('SIGUSR1', async () => {
        logger.info('Cleaning up all instance data...')
        
        for (const { instance } of instanceData) {
            await cleanupMongoDBStore(instance.id, true)
            logger.info(`Cleaned up data for ${instance.name}`)
        }
    })

    return instanceData
}

// Export for testing
module.exports = { 
    createWhatsAppInstance, 
    startAllInstances, 
    setupMultiInstanceMonitoring,
    instances 
}

// Run if called directly
if (require.main === module) {
    startAllInstances().catch(error => {
        logger.error('Failed to start instances:', error)
        process.exit(1)
    })
}