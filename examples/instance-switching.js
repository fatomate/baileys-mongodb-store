/**
 * Example: Switching between WhatsApp instances with proper cleanup
 * This demonstrates how to properly clean up and switch between instances
 */

const { default: makeWASocket, DisconnectReason, useMultiFileAuthState } = require('@whiskeysockets/baileys')
const { Boom } = require('@hapi/boom')
const { makeMongoDBStore, cleanupMongoDBStore } = require('@baileys/mongodb-store')
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
        
        // Cleanup MongoDB store
        await cleanupMongoDBStore(currentInstance.instanceId, deleteOldData)
        
        // Remove from active instances
        activeInstances.delete(currentInstance.instanceId)
        
        logger.info(`Cleaned up instance: ${currentInstance.instanceId}`)
    }
    
    // Create new instance
    logger.info(`Creating new instance: ${instanceId}`)
    
    // Create MongoDB store for new instance
    const store = await makeMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: process.env.MONGODB_DB || 'whatsapp_bot',
        instanceId: instanceId,
        ttlDays: 30,
        logger: logger.child({ module: 'mongodb-store', instance: instanceId })
    })
    
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
            logger.info(`Instance ${instanceId} - Chats: ${chats.length}, Contacts: ${Object.keys(contacts).length}`)
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
                        text: `Switching to instance: ${newInstanceId}...`
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
                        text: `Cleaning data for instance: ${targetInstanceId}...`
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
                await sock.sendMessage(msg.key.remoteJid, {
                    text: `📱 Active Instances:\n\n${instances.map(id => `• ${id} ${id === instanceId ? '(current)' : ''}`).join('\n')}`
                })
            }
        }
    })
    
    return instance
}

/**
 * Cleanup all instances
 */
async function cleanupAllInstances(deleteData = false) {
    logger.info('Cleaning up all instances...')
    
    for (const [instanceId, instance] of activeInstances) {
        logger.info(`Cleaning up instance: ${instanceId}`)
        
        // Close socket
        if (instance.sock) {
            instance.sock.end()
        }
        
        // Cleanup MongoDB
        await cleanupMongoDBStore(instanceId, deleteData)
    }
    
    activeInstances.clear()
    logger.info('All instances cleaned up')
}

// Main function
async function main() {
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
            
            await switchToInstance(instances[nextIndex], false)
        })
        
    } catch (error) {
        logger.error('Failed to start:', error)
        await cleanupAllInstances(false)
        process.exit(1)
    }
}

// Run the application
main()

module.exports = { switchToInstance, cleanupAllInstances }