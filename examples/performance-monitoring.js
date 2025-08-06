const makeWASocket = require('@whiskeysockets/baileys').default
const { useMultiFileAuthState } = require('@whiskeysockets/baileys')
const { makeMongoDBStore } = require('../dist/index')

/**
 * Example: Performance Monitoring with High-Load Handling
 * 
 * This example demonstrates:
 * - How to monitor store performance
 * - Automatic batch processing for high-load scenarios
 * - Performance statistics tracking
 */

async function connectWithPerformanceMonitoring() {
    // Create MongoDB store with your configuration
    const store = await makeMongoDBStore({
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_bot',
        instanceId: 'performance_test',
        ttlDays: 30
    })

    // Set up performance monitoring
    const performanceLogger = setInterval(() => {
        const stats = store.getPerformanceStats()
        console.log('\n📊 Performance Stats:')
        console.log(`├─ Messages Processed: ${stats.messagesProcessed}`)
        console.log(`├─ Labels Processed: ${stats.labelsProcessed}`)
        console.log(`├─ Batches Processed: ${stats.batchesProcessed}`)
        console.log(`├─ Errors: ${stats.errors}`)
        console.log(`└─ Uptime: ${Math.floor(stats.uptime / 1000)}s`)
        
        // Calculate processing rate
        const uptimeSeconds = stats.uptime / 1000
        if (uptimeSeconds > 0) {
            const messagesPerSecond = (stats.messagesProcessed / uptimeSeconds).toFixed(2)
            const labelsPerSecond = (stats.labelsProcessed / uptimeSeconds).toFixed(2)
            console.log(`\n📈 Processing Rate:`)
            console.log(`├─ Messages: ${messagesPerSecond}/sec`)
            console.log(`└─ Labels: ${labelsPerSecond}/sec`)
        }
    }, 10000) // Log every 10 seconds

    const { state, saveCreds } = await useMultiFileAuthState('./auth_info_performance')
    
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
    
    // Simulate high-load label processing
    sock.ev.on('labels.association', async ({ type, association }) => {
        // Labels are automatically batched internally
        // No special handling needed - the store handles it!
        if (type === 'add') {
            await store.upsertLabelAssociation(association)
        }
    })

    // Handle bulk message history
    sock.ev.on('messaging-history.set', async ({ messages, isLatest }) => {
        console.log(`\n📥 Receiving ${messages?.length || 0} messages from history...`)
        const startTime = Date.now()
        
        // Messages are automatically processed in batches
        // The store handles chunking and delays internally
        
        // After processing, check how long it took
        setTimeout(() => {
            const endTime = Date.now()
            const duration = (endTime - startTime) / 1000
            console.log(`✅ Processed ${messages?.length || 0} messages in ${duration.toFixed(2)}s`)
        }, 1000)
    })

    // Monitor real-time messages
    sock.ev.on('messages.upsert', async ({ messages, type }) => {
        if (type === 'notify') {
            console.log(`\n💬 New message received`)
        }
    })

    // Performance test command
    sock.ev.on('messages.upsert', async ({ messages }) => {
        const msg = messages[0]
        if (!msg.message) return
        
        const text = msg.message.conversation || msg.message.extendedTextMessage?.text || ''
        
        // Command to check performance stats
        if (text === '!stats') {
            const stats = store.getPerformanceStats()
            const uptimeMinutes = Math.floor(stats.uptime / 60000)
            
            const replyText = `*📊 Store Performance Stats*\n\n` +
                `📨 Messages: ${stats.messagesProcessed}\n` +
                `🏷️ Labels: ${stats.labelsProcessed}\n` +
                `📦 Batches: ${stats.batchesProcessed}\n` +
                `❌ Errors: ${stats.errors}\n` +
                `⏱️ Uptime: ${uptimeMinutes} minutes\n\n` +
                `_Performance optimizations are working automatically!_`
            
            await sock.sendMessage(msg.key.remoteJid!, { text: replyText })
        }
        
        // Command to reset stats
        if (text === '!reset-stats') {
            store.resetPerformanceStats()
            await sock.sendMessage(msg.key.remoteJid!, { 
                text: '✅ Performance statistics have been reset!' 
            })
        }
        
        // Command to simulate high load
        if (text === '!test-load') {
            await sock.sendMessage(msg.key.remoteJid!, { 
                text: '🚀 Simulating high load...' 
            })
            
            // Simulate creating many labels
            console.log('\n🧪 Starting load test...')
            const startTime = Date.now()
            
            // Create 1000 label associations
            const promises = []
            for (let i = 0; i < 1000; i++) {
                promises.push(store.upsertLabelAssociation({
                    type: 'chat',
                    chatId: msg.key.remoteJid!,
                    labelId: `test-label-${i}`
                }))
            }
            
            await Promise.all(promises)
            
            const duration = (Date.now() - startTime) / 1000
            const finalText = `✅ Load test completed!\n\n` +
                `Created 1000 label associations in ${duration.toFixed(2)}s\n` +
                `Check performance with !stats`
            
            await sock.sendMessage(msg.key.remoteJid!, { text: finalText })
        }
    })

    // Graceful shutdown
    process.on('SIGINT', async () => {
        console.log('\n🛑 Shutting down...')
        clearInterval(performanceLogger)
        
        // Show final stats
        const stats = store.getPerformanceStats()
        console.log('\n📊 Final Performance Stats:')
        console.log(`Total Messages: ${stats.messagesProcessed}`)
        console.log(`Total Labels: ${stats.labelsProcessed}`)
        console.log(`Total Batches: ${stats.batchesProcessed}`)
        console.log(`Total Errors: ${stats.errors}`)
        
        await store.close()
        process.exit(0)
    })
}

// Usage instructions
console.log('🚀 Performance Monitoring Example')
console.log('================================')
console.log('This example demonstrates:')
console.log('- Real-time performance monitoring')
console.log('- Automatic batch processing')
console.log('- High-load handling')
console.log('\nCommands you can send via WhatsApp:')
console.log('!stats - Show current performance statistics')
console.log('!reset-stats - Reset performance counters')
console.log('!test-load - Simulate high load (1000 labels)')
console.log('\n')

connectWithPerformanceMonitoring().catch(console.error)