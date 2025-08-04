const { default: makeWASocket, DisconnectReason, useMultiFileAuthState } = require('@whiskeysockets/baileys')
const { Boom } = require('@hapi/boom')
const { makeMongoDBStore, cleanupMongoDBStore } = require('../src')
const pino = require('pino')

const logger = pino({ level: 'info' })

// WhatsApp instance configuration
// @typedef {Object} WhatsAppInstance
// @property {string} id - Instance ID
// @property {string} name - Instance name
// @property {string} authFolder - Auth folder path
// @property {number} ttlDays - TTL in days

const instances: WhatsAppInstance[] = [
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

async function createWhatsAppInstance(instance: WhatsAppInstance) {
    const instanceLogger = logger.child({ instance: instance.id })
    
    // Create MongoDB store for this instance
    const store = await makeMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: 'whatsapp_multi_instance',
        instanceId: instance.id,
        ttlDays: instance.ttlDays,
        logger: instanceLogger
    })

    instanceLogger.info(`MongoDB store created for ${instance.name}`)

    // Multi-file auth state per instance
    const { state, saveCreds } = await useMultiFileAuthState(instance.authFolder)

    // Create socket
    const suki = makeWASocket({
        auth: state,
        printQRInTerminal: true,
        logger: instanceLogger,
        getMessage: async (key) => {
            const msg = await store.loadMessage(key.remoteJid!, key.id!)
            return msg?.message || undefined
        }
    })

    // Bind store to socket events
    store.bind(suki.ev)

    // Connection handler
    suki.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update
        
        if (qr) {
            instanceLogger.info(`QR Code for ${instance.name} - Scan with WhatsApp`)
        }
        
        if (connection === 'close') {
            const shouldReconnect = (lastDisconnect?.error as Boom)?.output?.statusCode !== DisconnectReason.loggedOut
            instanceLogger.info(`Connection closed for ${instance.name}`, lastDisconnect?.error)
            
            if (shouldReconnect) {
                // Reconnect with delay
                setTimeout(() => createWhatsAppInstance(instance), 5000)
            } else {
                await store.close()
            }
        } else if (connection === 'open') {
            instanceLogger.info(`${instance.name} connected successfully`)
            
            // Log instance statistics
            const chats = await store.getChats()
            const contacts = await store.getContacts()
            instanceLogger.info(`${instance.name} stats - Chats: ${chats.length}, Contacts: ${Object.keys(contacts).length}`)
        }
    })

    // Save credentials
    suki.ev.on('creds.update', saveCreds)

    // Instance-specific message handlers
    suki.ev.on('messages.upsert', async ({ messages, type }) => {
        for (const msg of messages) {
            if (msg.key.fromMe) continue
            
            const text = msg.message?.conversation || msg.message?.extendedTextMessage?.text || ''
            
            // Instance-specific logic
            switch (instance.id) {
                case 'support_bot':
                    await handleSupportMessage(suki, store, msg, text)
                    break
                case 'sales_bot':
                    await handleSalesMessage(suki, store, msg, text)
                    break
                case 'notification_bot':
                    await handleNotificationMessage(suki, store, msg, text)
                    break
            }
        }
    })

    return { socket: suki, store, instance }
}

// Support bot logic
async function handleSupportMessage(socket, store, msg, text) {
    const jid = msg.key.remoteJid!
    
    if (text.toLowerCase().includes('help')) {
        await socket.sendMessage(jid, {
            text: '🤝 Support Bot Here!\n\n' +
                  'How can I help you today?\n' +
                  '1. Technical Issues\n' +
                  '2. Account Problems\n' +
                  '3. General Questions\n\n' +
                  'Reply with the number of your choice.'
        })
    }
    
    // Store support ticket info
    const chat = await store.getChat(jid)
    if (chat) {
        await store.updateChat(jid, {
            ...chat,
            name: chat.name || 'Support Ticket'
        })
    }
}

// Sales bot logic
async function handleSalesMessage(socket, store, msg, text) {
    const jid = msg.key.remoteJid!
    
    if (text.toLowerCase().includes('price') || text.toLowerCase().includes('buy')) {
        await socket.sendMessage(jid, {
            text: '💰 Sales Bot Here!\n\n' +
                  'Our current offers:\n' +
                  '• Basic Plan: $9.99/month\n' +
                  '• Pro Plan: $19.99/month\n' +
                  '• Enterprise: Contact us\n\n' +
                  'Would you like more information?'
        })
    }
}

// Notification bot logic
async function handleNotificationMessage(socket, store, msg, text) {
    const jid = msg.key.remoteJid!
    
    // Notification bot typically doesn't respond to messages
    // It just sends notifications
    logger.info(`Notification bot received message from ${jid}: ${text}`)
}

// Start all instances
async function startAllInstances() {
    logger.info('Starting all WhatsApp instances...')
    
    const activeInstances = []
    
    for (const instance of instances) {
        try {
            const whatsappInstance = await createWhatsAppInstance(instance)
            activeInstances.push(whatsappInstance)
            logger.info(`Started instance: ${instance.name}`)
            
            // Add delay between instances to avoid rate limiting
            await new Promise(resolve => setTimeout(resolve, 3000))
        } catch (error) {
            logger.error(`Failed to start ${instance.name}:`, error)
        }
    }
    
    logger.info(`Successfully started ${activeInstances.length} instances`)
    
    // Graceful shutdown
    process.on('SIGINT', async () => {
        logger.info('Shutting down all instances...')
        
        // Cleanup all MongoDB connections
        await cleanupMongoDBStore()
        
        process.exit(0)
    })
}

// Example: Send notification from notification bot
async function sendBulkNotification(socket, store, message) {
    const chats = await store.getChats()
    const eligibleChats = chats.filter(chat => !chat.archived && chat.id.includes('@s.whatsapp.net'))
    
    logger.info(`Sending notification to ${eligibleChats.length} contacts`)
    
    for (const chat of eligibleChats) {
        try {
            await socket.sendMessage(chat.id, { text: message })
            // Add delay to avoid rate limiting
            await new Promise(resolve => setTimeout(resolve, 1000))
        } catch (error) {
            logger.error(`Failed to send notification to ${chat.id}:`, error)
        }
    }
}

// Start the multi-instance bot
startAllInstances().catch((err) => {
    logger.error('Failed to start instances:', err)
    process.exit(1)
})