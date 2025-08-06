const { default: makeWASocket, DisconnectReason, useMultiFileAuthState } = require('@whiskeysockets/baileys')
const { Boom } = require('@hapi/boom')
const { makeMongoDBStore, cleanupMongoDBStore } = require('../src')
const pino = require('pino')

const logger = pino({ level: 'info' })

async function connectToWhatsApp() {
    // Create MongoDB store
    // Note: The store automatically handles:
    // - Batch processing for labels and messages
    // - Connection pooling (100 connections max)
    // - Smart caching with auto-invalidation
    // - Queue management for concurrent operations
    const store = await makeMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: 'whatsapp_bot',
        instanceId: 'instance_001',
        ttlDays: 30,
        logger: logger.child({ module: 'mongodb-store' })
    })

    logger.info('MongoDB store created successfully')

    // Multi-file auth state
    const { state, saveCreds } = await useMultiFileAuthState('./auth_info_baileys')

    // Create socket with store configuration
    const suki = makeWASocket({
        auth: state,
        printQRInTerminal: true,
        logger,
        getMessage: async (key) => {
            // Retrieve message from MongoDB store
            const msg = await store.loadMessage(key.remoteJid!, key.id!)
            return msg?.message || undefined
        }
    })

    // Bind store to socket events
    store.bind(suki.ev)
    logger.info('Store bound to socket events')

    // Connection update handler
    suki.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update
        
        if (connection === 'close') {
            const shouldReconnect = (lastDisconnect?.error as Boom)?.output?.statusCode !== DisconnectReason.loggedOut
            logger.info('Connection closed due to', lastDisconnect?.error, ', reconnecting:', shouldReconnect)
            
            if (shouldReconnect) {
                connectToWhatsApp()
            } else {
                // Close MongoDB connection on logout
                await store.close()
            }
        } else if (connection === 'open') {
            logger.info('WhatsApp connection opened')
            
            // Example: Get all chats
            const chats = await store.getChats()
            logger.info(`Found ${chats.length} chats in store`)
            
            // Example: Get all contacts
            const contacts = await store.getContacts()
            logger.info(`Found ${Object.keys(contacts).length} contacts in store`)
        }
    })

    // Save credentials
    suki.ev.on('creds.update', saveCreds)

    // Message handler
    suki.ev.on('messages.upsert', async ({ messages, type }) => {
        logger.info(`Received ${messages.length} messages of type ${type}`)
        
        for (const msg of messages) {
            // Skip if message is from self
            if (msg.key.fromMe) continue
            
            const messageContent = msg.message?.conversation || 
                                 msg.message?.extendedTextMessage?.text || 
                                 'Non-text message'
            
            logger.info(`Message from ${msg.key.remoteJid}: ${messageContent}`)
            
            // Example: Auto-reply
            if (messageContent.toLowerCase().includes('hello')) {
                await suki.sendMessage(msg.key.remoteJid!, {
                    text: 'Hello! This is an automated response from MongoDB store example.'
                })
            }
            
            // Example: Get chat info
            const chat = await store.getChat(msg.key.remoteJid!)
            if (chat) {
                logger.info(`Chat info - Name: ${chat.name}, Unread: ${chat.unreadCount}`)
            }
            
            // Example: Get contact info
            const contact = await store.getContact(msg.key.participant || msg.key.remoteJid!)
            if (contact) {
                logger.info(`Contact info - Name: ${contact.name || contact.notify || 'Unknown'}`)
            }
        }
    })

    // Group update handler
    suki.ev.on('groups.update', async (updates) => {
        for (const update of updates) {
            logger.info(`Group ${update.id} updated:`, update)
            
            // Get updated group metadata from store
            const metadata = await store.getGroupMetadata(update.id!)
            if (metadata) {
                logger.info(`Group ${metadata.subject} has ${metadata.participants.length} participants`)
            }
        }
    })

    // Presence update handler
    suki.ev.on('presence.update', async ({ id, presences }) => {
        logger.info(`Presence update for ${id}:`, presences)
    })

    // Chat update handler
    suki.ev.on('chats.update', async (updates) => {
        for (const update of updates) {
            logger.info(`Chat ${update.id} updated:`, update)
        }
    })

    // Graceful shutdown
    process.on('SIGINT', async () => {
        logger.info('Shutting down...')
        await cleanupMongoDBStore()
        process.exit(0)
    })
}

// Start the bot
connectToWhatsApp().catch((err) => {
    logger.error('Failed to start bot:', err)
    process.exit(1)
})