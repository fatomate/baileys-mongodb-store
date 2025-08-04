const { default: makeWASocket, useMultiFileAuthState, makeInMemoryStore } = require('@whiskeysockets/baileys')
const { makeMongoDBStore, cleanupMongoDBStore } = require('../src')
const fs = require('fs/promises')
const pino = require('pino')

const logger = pino({ level: 'info' })

/**
 * Example: Migrate from in-memory store to MongoDB store
 */
async function migrateToMongoDB() {
    logger.info('Starting migration from in-memory store to MongoDB...')

    // Step 1: Load existing data from JSON file (if using file persistence)
    let existingData: any = null
    try {
        const jsonData = await fs.readFile('./baileys_store.json', 'utf-8')
        existingData = JSON.parse(jsonData)
        logger.info('Loaded existing data from baileys_store.json')
    } catch (error) {
        logger.warn('No existing store file found, starting fresh')
    }

    // Step 2: Create MongoDB store
    const mongoStore = await makeMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: 'whatsapp_bot',
        instanceId: 'migrated_instance',
        ttlDays: 30,
        logger: logger.child({ module: 'mongodb-store' })
    })

    // Step 3: Import existing data into MongoDB
    if (existingData) {
        logger.info('Importing data into MongoDB...')

        // Import chats
        if (existingData.chats?.all) {
            const chats = existingData.chats.all()
            await mongoStore.upsertChats(...chats)
            logger.info(`Imported ${chats.length} chats`)
        }

        // Import contacts
        if (existingData.contacts) {
            const contacts = Object.values(existingData.contacts)
            await mongoStore.upsertContacts(contacts as any[])
            logger.info(`Imported ${contacts.length} contacts`)
        }

        // Import messages
        if (existingData.messages) {
            for (const [jid, messageData] of Object.entries(existingData.messages)) {
                const messages = (messageData as any).array || []
                for (const msg of messages) {
                    await mongoStore.upsertMessage(jid, msg, 'append')
                }
                logger.info(`Imported ${messages.length} messages for ${jid}`)
            }
        }

        // Import labels
        if (existingData.labels) {
            const labels = Object.entries(existingData.labels)
            for (const [id, label] of labels) {
                await mongoStore.upsertLabel(id, label as any)
            }
            logger.info(`Imported ${labels.length} labels`)
        }

        // Import label associations
        if (existingData.labelAssociations?.all) {
            const associations = existingData.labelAssociations.all()
            for (const assoc of associations) {
                await mongoStore.upsertLabelAssociation(assoc)
            }
            logger.info(`Imported ${associations.length} label associations`)
        }

        logger.info('Data migration completed successfully!')
    }

    // Step 4: Create WhatsApp connection with MongoDB store
    const { state, saveCreds } = await useMultiFileAuthState('./auth_info_baileys')
    
    const suki = makeWASocket({
        auth: state,
        printQRInTerminal: true,
        logger,
        getMessage: async (key) => {
            const msg = await mongoStore.loadMessage(key.remoteJid!, key.id!)
            return msg?.message || undefined
        }
    })

    // Bind store to socket events
    mongoStore.bind(suki.ev)

    // Your existing event handlers work exactly the same
    suki.ev.on('creds.update', saveCreds)
    
    suki.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect } = update
        if (connection === 'open') {
            logger.info('Connected to WhatsApp with MongoDB store!')
        }
    })

    suki.ev.on('messages.upsert', async ({ messages }) => {
        for (const msg of messages) {
            logger.info(`New message stored in MongoDB: ${msg.key.id}`)
        }
    })

    return { socket: suki, store: mongoStore }
}

/**
 * Example: Run both stores in parallel for comparison
 */
async function compareStores() {
    logger.info('Running comparison between in-memory and MongoDB stores...')

    // Create both stores
    const memoryStore = makeInMemoryStore({ logger })
    const mongoStore = await makeMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: 'whatsapp_bot',
        instanceId: 'comparison_test',
        ttlDays: 30,
        logger
    })

    const { state, saveCreds } = await useMultiFileAuthState('./auth_info_baileys')
    
    const suki = makeWASocket({
        auth: state,
        printQRInTerminal: true,
        logger
    })

    // Bind both stores
    memoryStore.bind(suki.ev)
    mongoStore.bind(suki.ev)

    // Compare data after some events
    suki.ev.on('chats.upsert', async (chats) => {
        // Wait a bit for both stores to process
        await new Promise(resolve => setTimeout(resolve, 100))

        // Compare chat counts
        const memoryChats = memoryStore.chats.all()
        const mongoChats = await mongoStore.getChats()

        logger.info(`Memory store chats: ${memoryChats.length}`)
        logger.info(`MongoDB store chats: ${mongoChats.length}`)

        if (memoryChats.length !== mongoChats.length) {
            logger.warn('Chat count mismatch between stores!')
        }
    })

    suki.ev.on('messages.upsert', async ({ messages }) => {
        for (const msg of messages) {
            const jid = msg.key.remoteJid!
            
            // Wait a bit for both stores to process
            await new Promise(resolve => setTimeout(resolve, 100))

            // Compare message retrieval
            const memoryMsg = await memoryStore.loadMessage(jid, msg.key.id!)
            const mongoMsg = await mongoStore.loadMessage(jid, msg.key.id!)

            if (!memoryMsg || !mongoMsg) {
                logger.warn(`Message ${msg.key.id} missing in one of the stores`)
            } else {
                logger.info(`Message ${msg.key.id} successfully stored in both stores`)
            }
        }
    })

    return { socket: suki, memoryStore, mongoStore }
}

// Export functions for use in other files
module.exports = { migrateToMongoDB, compareStores }

// Run migration if this file is executed directly
if (require.main === module) {
    migrateToMongoDB()
        .then(() => logger.info('Migration completed'))
        .catch(err => {
            logger.error('Migration failed:', err)
            process.exit(1)
        })
}