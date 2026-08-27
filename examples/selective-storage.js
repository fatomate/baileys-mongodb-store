/**
 * Example: Selective Event Storage
 * Store only specific WhatsApp events to optimize storage usage
 */

const makeWASocket = require('@whiskeysockets/baileys').default
const { useMultiFileAuthState } = require('@whiskeysockets/baileys')
const { makeEnhancedMongoDBStore } = require('@fatomate/baileys-mongodb-store')

async function connectWithSelectiveStorage() {
    // Create store with selective event storage
    const store = await makeEnhancedMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: 'whatsapp_selective',
        instanceId: 'selective_bot',
        
        // Don't store all events by default
        storeAllByDefault: false,
        
        // Only store these specific events
        events: {
            // Essential connection tracking
            'connection.update': { 
                enabled: true, 
                ttlDays: 7 // Keep connection logs for a week
            },
            
            // Store messages with filtering
            'messages.upsert': { 
                enabled: true,
                ttlDays: 30,
                filter: (data) => {
                    const { messages, type } = data
                    
                    // Only store notify messages (not history)
                    if (type !== 'notify') return false
                    
                    // Filter out ephemeral messages
                    return messages.some(msg => !msg.message?.ephemeralMessage)
                },
                useBatch: true // Use batch processing for performance
            },
            
            // Store chat updates
            'chats.upsert': { 
                enabled: true,
                ttlDays: 90 // Keep chats for 3 months
            },
            
            // Store contact updates but filter broadcasts
            'contacts.upsert': {
                enabled: true,
                ttlDays: 365, // Keep contacts for a year
                filter: (contacts) => {
                    // Remove broadcast and status contacts
                    const filtered = contacts.filter(contact => 
                        !contact.id?.includes('@broadcast') &&
                        !contact.id?.includes('status@broadcast')
                    )
                    return filtered.length > 0
                }
            },
            
            // Store groups with longer retention
            'groups.upsert': {
                enabled: true,
                ttlDays: 180 // Keep groups for 6 months
            },
            
            // Skip these high-volume, low-value events
            'presence.update': { enabled: false },
            'message-receipt.update': { enabled: false },
            'messages.reaction': { enabled: false }
        },
        
        // Enable metrics to monitor what's being stored
        enableMetrics: true,
        
        // Add hooks for monitoring
        hooks: {
            beforeStore: async (eventType, data) => {
                console.log(`[STORE] Processing ${eventType} event`)
                return true
            },
            afterStore: async (eventType, data) => {
                // Log successful storage
                if (eventType === 'messages.upsert') {
                    console.log(`[STORE] Stored ${data.messages?.length || 0} messages`)
                }
            },
            onError: (eventType, error, data) => {
                console.error(`[STORE ERROR] Failed to store ${eventType}:`, error.message)
            }
        },
        
        logLevel: 'warn' // Only log warnings and errors
    })
    
    // Set up WhatsApp connection
    const { state, saveCreds } = await useMultiFileAuthState('./auth_selective')
    
    const sock = makeWASocket({
        auth: state,
        printQRInTerminal: true,
        getMessage: async (key) => {
            return await store.loadMessage(key.remoteJid!, key.id!)
        }
    })
    
    // Bind store to socket events
    store.bind(sock.ev)
    
    sock.ev.on('creds.update', saveCreds)
    
    // Monitor what's being stored
    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect } = update
        
        if (connection === 'open') {
            console.log('✅ Connected to WhatsApp')
            
            // Start monitoring metrics
            setInterval(() => {
                const stats = store.getPerformanceStats()
                const metrics = store.getEventMetrics()
                
                console.log('\n📊 Storage Metrics:')
                console.log('═══════════════════')
                
                if (Array.isArray(metrics)) {
                    metrics.forEach(metric => {
                        if (metric.totalReceived > 0) {
                            const storageRate = ((metric.totalStored / metric.totalReceived) * 100).toFixed(1)
                            console.log(`${metric.eventType}:`)
                            console.log(`  Received: ${metric.totalReceived}`)
                            console.log(`  Stored: ${metric.totalStored} (${storageRate}%)`)
                            console.log(`  Skipped: ${metric.totalSkipped}`)
                            if (metric.totalErrors > 0) {
                                console.log(`  Errors: ${metric.totalErrors}`)
                            }
                        }
                    })
                }
                
                console.log('\n📈 Performance:')
                console.log(`  Messages: ${stats.messagesProcessed}`)
                console.log(`  Batches: ${stats.batchesProcessed}`)
                console.log(`  Errors: ${stats.errors}`)
                console.log(`  Uptime: ${Math.floor(stats.uptime / 1000)}s`)
            }, 60000) // Every minute
        }
        
        if (connection === 'close') {
            const shouldReconnect = (lastDisconnect?.error?.output?.statusCode !== 401)
            console.log('Connection closed due to', lastDisconnect?.error, ', reconnecting', shouldReconnect)
            
            if (shouldReconnect) {
                setTimeout(() => connectWithSelectiveStorage(), 5000)
            } else {
                // Clean up before exit
                store.close()
            }
        }
    })
    
    // Example: Dynamically adjust storage based on time
    const adjustStorageByTime = () => {
        const hour = new Date().getHours()
        
        if (hour >= 22 || hour <= 6) {
            // Night time - minimal storage
            console.log('🌙 Night mode: Minimal storage')
            store.updateEventConfig('messages.upsert', {
                enabled: true,
                ttlDays: 1, // Only keep messages for 1 day
                filter: (data) => {
                    // Only store messages from saved contacts
                    return data.messages?.some(msg => 
                        msg.key?.fromMe || msg.isGroup
                    )
                }
            })
        } else {
            // Day time - normal storage
            console.log('☀️ Day mode: Normal storage')
            store.updateEventConfig('messages.upsert', {
                enabled: true,
                ttlDays: 30,
                filter: (data) => {
                    // Store all non-ephemeral messages
                    return data.messages?.some(msg => 
                        !msg.message?.ephemeralMessage
                    )
                }
            })
        }
    }
    
    // Adjust storage every hour
    setInterval(adjustStorageByTime, 60 * 60 * 1000)
    adjustStorageByTime() // Run immediately
    
    // Example: Monitor storage size
    sock.ev.on('messages.upsert', async ({ messages }) => {
        // Check how many messages are actually being stored
        const beforeCount = (await store.getMessages(messages[0].key.remoteJid!)).length
        
        // After storage (wait a bit for async processing)
        setTimeout(async () => {
            const afterCount = (await store.getMessages(messages[0].key.remoteJid!)).length
            if (afterCount > beforeCount) {
                console.log(`💾 Stored ${afterCount - beforeCount} new messages`)
            } else {
                console.log(`⏭️ Skipped storing messages (filtered out)`)
            }
        }, 1000)
    })
    
    return { sock, store }
}

// Run the example
connectWithSelectiveStorage().catch(console.error)