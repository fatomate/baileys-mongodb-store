/**
 * Complete example showing MongoDB store usage with Baileys
 * Similar to @baileys/redis-auth-state pattern
 */

const { default: makeWASocket, DisconnectReason, useMultiFileAuthState } = require('@whiskeysockets/baileys')
const { Boom } = require('@hapi/boom')
const { makeMongoDBStore, cleanupMongoDBStore } = require('@baileys/mongodb-store')
const pino = require('pino')

const logger = pino({ level: 'info' })

async function connectToWhatsApp() {
    // MongoDB store configuration
    const store = await makeMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: process.env.MONGODB_DB || 'whatsapp_bot',
        instanceId: process.env.INSTANCE_ID || 'main_instance',
        ttlDays: parseInt(process.env.TTL_DAYS) || 30,
        logger: logger.child({ module: 'mongodb-store' })
    })

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
            }
        } else if (connection === 'open') {
            logger.info('WhatsApp connection opened successfully')

            // Example: Access store data
            const chats = await store.getChats()
            logger.info(`Loaded ${chats.length} chats from MongoDB`)
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
                
                await sock.sendMessage(msg.key.remoteJid, { 
                    text: `📊 Bot Statistics:\n\n` +
                          `• Active Chats: ${chats.length}\n` +
                          `• Saved Contacts: ${Object.keys(contacts).length}\n` +
                          `• Instance ID: ${store.instanceId}\n` +
                          `• Using MongoDB Store ✅`
                })
            }

            if (messageContent.toLowerCase() === '!help') {
                await sock.sendMessage(msg.key.remoteJid, { 
                    text: `🤖 Available Commands:\n\n` +
                          `!ping - Check if bot is online\n` +
                          `!stats - View bot statistics\n` +
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

    return { sock, store }
}

// Main function
async function main() {
    try {
        const { sock, store } = await connectToWhatsApp()

        // Graceful shutdown handler
        process.on('SIGINT', async () => {
            logger.info('Shutting down gracefully...')
            
            // Close socket
            sock.end()
            
            // Cleanup all MongoDB connections
            await cleanupMongoDBStore()
            
            logger.info('Shutdown complete')
            process.exit(0)
        })

        // Error handler
        process.on('unhandledRejection', (err) => {
            logger.error('Unhandled rejection:', err)
            process.exit(1)
        })

    } catch (error) {
        logger.error('Failed to start:', error)
        process.exit(1)
    }
}

// Run the bot
main()