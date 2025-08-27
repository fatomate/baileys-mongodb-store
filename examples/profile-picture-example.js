const makeWASocket = require('@whiskeysockets/baileys').default
const { useMultiFileAuthState } = require('@whiskeysockets/baileys')
const { makeEnhancedMongoDBStore } = require('../dist/makeEnhancedMongoDBStore')

async function connectWithProfilePictures() {
    // Initialize auth state
    const { state, saveCreds } = await useMultiFileAuthState('./auth_info')
    
    // Create WhatsApp socket
    const sock = makeWASocket({
        auth: state,
        printQRInTerminal: true
    })
    
    // Create enhanced MongoDB store with profile picture auto-retrieval
    const store = await makeEnhancedMongoDBStore({
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_bot',
        instanceId: 'profile_pic_demo',
        ttlDays: 30,
        
        // Pass the socket instance for profile picture retrieval
        sock: sock,
        
        // Configure profile picture auto-retrieval
        profilePictureConfig: {
            enabled: true,                    // Enable auto-retrieval
            refreshIntervalDays: 7,           // Refresh every 7 days
            requestDelay: 500,                // 500ms delay between requests
            maxConcurrent: 5,                 // Max 5 concurrent fetches
            retryAttempts: 3,                 // Retry 3 times on failure
            logPrivacyErrors: false           // Don't log privacy restrictions
        },
        
        // Redis configuration for queue processing
        redis: {
            connection: 'redis://localhost:6379',
            queuePrefix: 'profile_demo'
        },
        
        logLevel: 'all' // Enable detailed logging
    })
    
    // Bind store to socket events
    store.bind(sock.ev)
    
    // Save credentials when updated
    sock.ev.on('creds.update', saveCreds)
    
    // Handle connection updates
    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect } = update
        
        if (connection === 'close') {
            console.log('Connection closed')
        } else if (connection === 'open') {
            console.log('Connected to WhatsApp')
            console.log('Profile pictures will be automatically fetched for all contacts')
        }
    })
    
    // Handle contacts upsert - profile pictures will be automatically fetched
    sock.ev.on('contacts.upsert', async (contacts) => {
        console.log(`Received ${contacts.length} contacts`)
        console.log('Profile pictures will be fetched in the background...')
        
        // After a few seconds, check if profile pictures were fetched
        setTimeout(async () => {
            for (const contact of contacts.slice(0, 5)) { // Check first 5 contacts
                const savedContact = await store.getContact(contact.id)
                if (savedContact?.profilePic) {
                    console.log(`✅ Profile picture fetched for ${contact.notify || contact.id}: ${savedContact.profilePic}`)
                } else {
                    console.log(`⚠️ No profile picture available for ${contact.notify || contact.id} (may be privacy restricted)`)
                }
            }
        }, 10000) // Wait 10 seconds for processing
    })
    
    // Example: Manually fetch a contact's profile picture
    sock.ev.on('messages.upsert', async ({ messages }) => {
        const msg = messages[0]
        if (!msg.key.fromMe && msg.message?.conversation === '!profile') {
            const senderId = msg.key.remoteJid
            
            try {
                // This will trigger profile picture fetch if not already cached
                const contact = await store.getContact(senderId)
                
                if (contact?.profilePic) {
                    await sock.sendMessage(senderId, {
                        text: `Your profile picture URL: ${contact.profilePic}`
                    })
                } else {
                    await sock.sendMessage(senderId, {
                        text: 'Could not fetch your profile picture (privacy settings may be restricting access)'
                    })
                }
            } catch (error) {
                console.error('Error fetching profile:', error)
            }
        }
    })
    
    return sock
}

// Run the example
connectWithProfilePictures().catch(console.error)