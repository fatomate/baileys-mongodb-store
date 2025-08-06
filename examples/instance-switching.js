/**
 * Example: Switching between WhatsApp instances with proper cleanup
 * Now includes Redis/Bull queue integration with proper queue isolation per instance
 */

const { default: makeWASocket, DisconnectReason, useMultiFileAuthState } = require('@whiskeysockets/baileys')
const { Boom } = require('@hapi/boom')
const { makeMongoDBStore, cleanupMongoDBStore } = require('../dist')
const pino = require('pino')

const logger = pino({ level: 'info' })

// Active instances map
const activeInstances = new Map()

/**
 * Create or switch to a WhatsApp instance
 * @param {string} instanceId - The instance ID to switch to
 * @param {boolean} deleteOldData - Whether to delete data from previous instance
 */
async function switchToInstance(instanceId, deleteOldData = false) {
    logger.info(`Switching to instance: ${instanceId}`)
    
    // Check if we have an active instance
    const currentInstance = Array.from(activeInstances.values())[0]
    
    if (currentInstance) {
        logger.info(`Cleaning up current instance: ${currentInstance.instanceId}`)
        
        // Close the socket
        if (currentInstance.sock) {
            currentInstance.sock.end()
        }
        
        // Close store (includes Bull queues)
        if (currentInstance.store) {
            await currentInstance.store.close()
        }
        
        // Cleanup MongoDB store
        await cleanupMongoDBStore(currentInstance.instanceId, deleteOldData)
        
        // Remove from active instances
        activeInstances.delete(currentInstance.instanceId)
        
        logger.info(`Cleaned up instance: ${currentInstance.instanceId}`)
    }
    
    // Create new instance
    logger.info(`Creating new instance: ${instanceId}`)
    
    // Create MongoDB store for new instance with Redis/Bull queues
    const store = await makeMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: process.env.MONGODB_DB || 'whatsapp_bot',
        instanceId: instanceId,
        ttlDays: 30,
        logger: logger.child({ module: 'mongodb-store', instance: instanceId }),
        
        // Redis/Bull configuration - each instance gets isolated queues
        redis: process.env.REDIS_URL ? {
            connection: process.env.REDIS_URL,
            queuePrefix: 'wa-instance', // Each instance will have queues like 'wa-instance:messages:instance_001'
            concurrency: parseInt(process.env.QUEUE_CONCURRENCY) || 50
        } : undefined
    })
    
    // Check Bull queue initialization
    const stats = store.getPerformanceStats()
    if (stats.bullStats?.initialized) {
        logger.info(`✅ Instance ${instanceId} - Bull queues initialized`, {
            totalQueues: stats.bullStats.totalQueues,
            redisConnected: stats.bullStats.redisConnected,
            queueTypes: Object.keys(stats.bullStats.queues)
        })
    } else {
        logger.info(`💾 Instance ${instanceId} - Using in-memory processing`, {
            reason: stats.bullStats?.reason || 'Redis not configured'
        })
    }
    
    // Auth state with instance-specific folder
    const { state, saveCreds } = await useMultiFileAuthState(`./auth_${instanceId}`)
    
    // Create socket
    const sock = makeWASocket({
        auth: state,
        printQRInTerminal: true,
        logger: logger.child({ instance: instanceId }),
        getMessage: async (key) => {
            const msg = await store.loadMessage(key.remoteJid, key.id)
            return msg?.message || null
        }
    })
    
    // Bind store to events
    store.bind(sock.ev)
    
    // Save instance reference
    const instance = { instanceId, sock, store }
    activeInstances.set(instanceId, instance)
    
    // Connection handler
    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update
        
        if (connection === 'close') {
            const shouldReconnect = (lastDisconnect?.error)?.output?.statusCode !== DisconnectReason.loggedOut
            
            if (shouldReconnect && activeInstances.has(instanceId)) {
                logger.info(`Reconnecting instance: ${instanceId}`)
                await switchToInstance(instanceId, false)
            }
        } else if (connection === 'open') {
            logger.info(`Instance ${instanceId} connected successfully`)
            
            // Load instance stats
            const chats = await store.getChats()
            const contacts = await store.getContacts()
            const currentStats = store.getPerformanceStats()
            
            logger.info(`Instance ${instanceId} statistics:`, {
                chats: chats.length,
                contacts: Object.keys(contacts).length,
                bullActive: currentStats.bullStats?.initialized || false,
                redisConnected: currentStats.bullStats?.redisConnected,
                queuesActive: currentStats.bullStats?.totalQueues || 0
            })
        }
    })
    
    // Save credentials
    sock.ev.on('creds.update', saveCreds)
    
    // Message handler
    sock.ev.on('messages.upsert', async ({ messages }) => {
        for (const msg of messages) {
            if (msg.key.fromMe) continue
            
            const text = msg.message?.conversation || msg.message?.extendedTextMessage?.text || ''
            
            // Switch instance command
            if (text.startsWith('!switch ')) {
                const newInstanceId = text.replace('!switch ', '').trim()
                
                if (newInstanceId && newInstanceId !== instanceId) {
                    await sock.sendMessage(msg.key.remoteJid, {
                        text: `🔄 Switching to instance: ${newInstanceId}...`
                    })
                    
                    // Switch to new instance
                    await switchToInstance(newInstanceId, false)
                }
            }
            
            // Clean instance command
            if (text.startsWith('!clean ')) {
                const targetInstanceId = text.replace('!clean ', '').trim()
                
                if (targetInstanceId) {
                    await sock.sendMessage(msg.key.remoteJid, {
                        text: `🧹 Cleaning data for instance: ${targetInstanceId}...`
                    })
                    
                    // Clean specific instance data
                    await cleanupMongoDBStore(targetInstanceId, true)
                    
                    await sock.sendMessage(msg.key.remoteJid, {
                        text: `✅ Cleaned data for instance: ${targetInstanceId}`
                    })
                }
            }
            
            // Show instances command
            if (text === '!instances') {
                const instances = Array.from(activeInstances.keys())
                const instanceInfo = []
                
                for (const id of instances) {
                    const inst = activeInstances.get(id)
                    const stats = inst?.store?.getPerformanceStats()
                    const status = id === instanceId ? '(current)' : ''
                    const queueStatus = stats?.bullStats?.initialized ? '⚡Bull' : '💾Memory'
                    instanceInfo.push(`• ${id} ${status} - ${queueStatus}`)
                }
                
                await sock.sendMessage(msg.key.remoteJid, {
                    text: `📱 Active Instances:\n\n${instanceInfo.join('\n')}\n\n💡 Commands:\n!switch <id> - Switch instance\n!clean <id> - Clean instance data\n!stats - View current stats`
                })
            }
            
            // Show current instance stats
            if (text === '!stats') {
                const currentStats = store.getPerformanceStats()
                const chats = await store.getChats()
                
                const statsText = `📊 Instance: ${instanceId}\n\n` +
                    `• Chats: ${chats.length}\n` +
                    `• Messages Processed: ${currentStats.messagesProcessed}\n` +
                    `• Labels Processed: ${currentStats.labelsProcessed}\n` +
                    `• Queue System: ${currentStats.bullStats?.initialized ? 'Redis/Bull ⚡' : 'In-Memory 💾'}\n` +
                    `• Redis Connected: ${currentStats.bullStats?.redisConnected ? '✅' : '❌'}\n` +
                    `• Active Queues: ${currentStats.bullStats?.totalQueues || 0}\n` +
                    `• Errors: ${currentStats.errors}\n` +
                    `• Uptime: ${Math.floor(currentStats.uptime / 1000 / 60)}m`
                
                await sock.sendMessage(msg.key.remoteJid, { text: statsText })
            }
        }
    })
    
    // Monitor queue health for this instance
    setupInstanceMonitoring(instance, logger)
    
    return instance
}

/**
 * Set up monitoring for a specific instance
 */
function setupInstanceMonitoring(instance, logger) {
    const { instanceId, store } = instance
    
    // Health check every 2 minutes for this instance
    const healthInterval = setInterval(() => {
        const stats = store.getPerformanceStats()
        
        // Alert if Redis disconnected for this instance
        if (stats.bullStats?.initialized && !stats.bullStats.redisConnected) {
            logger.warn(`🚨 Instance ${instanceId} - Redis connection lost, running in fallback mode`)
        }
        
        // Log activity if there is any
        if (stats.messagesProcessed > 0 || stats.labelsProcessed > 0) {
            logger.info(`📈 Instance ${instanceId} activity:`, {
                messages: stats.messagesProcessed,
                labels: stats.labelsProcessed,
                errors: stats.errors,
                bullActive: stats.bullStats?.initialized || false
            })
        }
    }, 2 * 60 * 1000) // Every 2 minutes
    
    // Store the interval so we can clear it later
    instance.healthInterval = healthInterval
}

/**
 * Cleanup all instances
 */
async function cleanupAllInstances(deleteData = false) {
    logger.info('Cleaning up all instances...')
    
    for (const [instanceId, instance] of activeInstances) {
        logger.info(`Cleaning up instance: ${instanceId}`)
        
        // Clear health monitoring
        if (instance.healthInterval) {
            clearInterval(instance.healthInterval)
        }
        
        // Close socket
        if (instance.sock) {
            instance.sock.end()
        }
        
        // Close store (includes Bull queues)
        if (instance.store) {
            try {
                await instance.store.flushLabelAssociations()
                await instance.store.close()
            } catch (error) {
                logger.error(`Error closing store for ${instanceId}:`, error)
            }
        }
        
        // Cleanup MongoDB
        await cleanupMongoDBStore(instanceId, deleteData)
    }
    
    activeInstances.clear()
    logger.info('All instances cleaned up')
}

/**
 * Get information about all instances
 */
async function getInstancesInfo() {
    const info = []
    
    for (const [instanceId, instance] of activeInstances) {
        const stats = instance.store?.getPerformanceStats()
        const chats = await instance.store?.getChats() || []
        
        info.push({
            instanceId,
            chats: chats.length,
            bullActive: stats?.bullStats?.initialized || false,
            redisConnected: stats?.bullStats?.redisConnected || false,
            queues: stats?.bullStats?.totalQueues || 0,
            messagesProcessed: stats?.messagesProcessed || 0,
            labelsProcessed: stats?.labelsProcessed || 0,
            errors: stats?.errors || 0,
            uptime: stats?.uptime || 0
        })
    }
    
    return info
}

// Main function
async function main() {
    console.log('🚀 Starting Multi-Instance WhatsApp Bot with Redis/Bull Queues\n')
    
    // Environment info
    console.log('📋 Configuration:')
    console.log(`  MongoDB: ${process.env.MONGODB_URI || 'mongodb://localhost:27017'}`)
    console.log(`  Redis: ${process.env.REDIS_URL || 'Not configured (will use in-memory)'}`)
    console.log(`  Default Instance: ${process.env.DEFAULT_INSTANCE || 'default'}\n`)
    
    // Start with default instance
    const defaultInstanceId = process.env.DEFAULT_INSTANCE || 'default'
    
    try {
        await switchToInstance(defaultInstanceId)
        
        // Graceful shutdown
        process.on('SIGINT', async () => {
            logger.info('Shutting down all instances...')
            await cleanupAllInstances(false) // Don't delete data on shutdown
            process.exit(0)
        })
        
        // Handle instance switch via signal
        process.on('SIGUSR2', async () => {
            // Example: Switch to a different instance
            const instances = ['instance_a', 'instance_b', 'instance_c']
            const currentIndex = instances.indexOf(Array.from(activeInstances.keys())[0])
            const nextIndex = (currentIndex + 1) % instances.length
            
            logger.info(`Signal received - switching to ${instances[nextIndex]}`)
            await switchToInstance(instances[nextIndex], false)
        })
        
        // Log instances status every 10 minutes
        setInterval(async () => {
            const info = await getInstancesInfo()
            if (info.length > 0) {
                logger.info('📊 Instances Status:', info)
            }
        }, 10 * 60 * 1000)
        
    } catch (error) {
        logger.error('Failed to start:', error)
        await cleanupAllInstances(false)
        process.exit(1)
    }
}

// Export functions for external use
module.exports = { 
    switchToInstance, 
    cleanupAllInstances, 
    getInstancesInfo,
    setupInstanceMonitoring
}

// Run the application
if (require.main === module) {
    main()
}