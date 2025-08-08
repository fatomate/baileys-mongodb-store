const { default: makeWASocket, useMultiFileAuthState } = require('@whiskeysockets/baileys')
const { makeMongoDBStore } = require('../src')
const { generateApiKey } = require('../src/utils/auth')
const pino = require('pino')

const logger = pino({ level: 'info' })

/**
 * Example: Secure MongoDB Store Configuration
 * 
 * This example demonstrates how to configure the MongoDB store with:
 * - API key authentication
 * - Instance isolation
 * - Memory management
 * - TTL monitoring
 */

async function createSecureStore() {
    // Generate API keys for different instances
    const apiKey1 = generateApiKey()
    const apiKey2 = generateApiKey()
    
    console.log('Generated API Keys:')
    console.log('Instance 1:', apiKey1)
    console.log('Instance 2:', apiKey2)
    console.log('Save these keys securely!')
    
    // Create secure store with all security features
    const store = await makeMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: 'whatsapp_secure',
        instanceId: 'production-instance-1',
        ttlDays: 30,
        
        // Security configuration
        auth: {
            // Enable API key authentication
            enableApiKey: true,
            
            // Secret key for signing tokens (use environment variable in production)
            secretKey: process.env.JWT_SECRET || 'your-secret-key-here',
            
            // Enable strict instance isolation
            strictIsolation: true,
            
            // Whitelist specific instances
            allowedInstances: ['production-instance-1', 'production-instance-2'],
            
            // Map API keys to instances
            apiKeys: new Map([
                [apiKey1, 'production-instance-1'],
                [apiKey2, 'production-instance-2']
            ])
        },
        
        // Memory management configuration
        memory: {
            // Maximum memory usage before applying backpressure (MB)
            maxMemoryMB: 512,
            
            // Maximum items per batch
            maxBatchSize: 1000,
            
            // Time window for batch accumulation (ms)
            batchTimeWindowMs: 100,
            
            // Enable memory monitoring
            enableMonitoring: true
        },
        
        // TTL monitoring configuration
        ttlMonitoring: {
            // Enable automatic TTL monitoring
            enableMonitoring: true,
            
            // Check interval in minutes
            checkIntervalMinutes: 60,
            
            // Alert if documents are older than TTL + threshold days
            alertThresholdDays: 1
        },
        
        logger: logger.child({ module: 'secure-store' }),
        logLevel: 'warn' // Only log warnings and errors
    })
    
    return { store, apiKey1 }
}

async function demonstrateSecurityFeatures() {
    const { store, apiKey1 } = await createSecureStore()
    
    logger.info('Secure store created successfully')
    
    // 1. Demonstrate performance monitoring with memory stats
    const stats = store.getPerformanceStats()
    console.log('\n📊 Performance Statistics:')
    console.log('Messages processed:', stats.messagesProcessed)
    console.log('Labels processed:', stats.labelsProcessed)
    
    if (stats.memoryStats) {
        console.log('\n💾 Memory Usage:')
        console.log(`Heap Used: ${stats.memoryStats.heapUsedMB} MB`)
        console.log(`Heap Total: ${stats.memoryStats.heapTotalMB} MB`)
        console.log(`RSS: ${stats.memoryStats.rssMB} MB`)
        console.log(`Memory Pressure: ${(stats.memoryStats.memoryPressure * 100).toFixed(1)}%`)
    }
    
    // 2. Check TTL status
    const ttlStatus = await store.getTTLStatus()
    console.log('\n⏰ TTL Monitoring Status:')
    console.log('Enabled:', ttlStatus.enabled)
    console.log('TTL Days:', ttlStatus.ttlDays)
    
    if (ttlStatus.summary) {
        console.log('Collections with TTL:', ttlStatus.summary.collectionsWithTTL)
        console.log('Total expired documents:', ttlStatus.summary.totalExpiredDocuments)
    }
    
    // 3. Demonstrate input validation (these would throw ValidationError)
    console.log('\n🔒 Input Validation Examples:')
    
    try {
        // Valid JID formats
        await store.getChat('1234567890@s.whatsapp.net') // Valid individual
        await store.getChat('120363024958650834@g.us') // Valid group
        console.log('✅ Valid JIDs accepted')
    } catch (error) {
        console.error('❌ Validation error:', error.message)
    }
    
    try {
        // Invalid JID format - would throw ValidationError
        await store.getChat('invalid-jid-format')
    } catch (error) {
        console.log('✅ Invalid JID rejected:', error.name)
    }
    
    // 4. Multi-file auth state
    const { state, saveCreds } = await useMultiFileAuthState('./auth_secure')
    
    // 5. Create WhatsApp connection
    const suki = makeWASocket({
        auth: state,
        printQRInTerminal: true,
        logger
    })
    
    // Bind store to events
    store.bind(suki.ev)
    
    // 6. Monitor memory during operation
    suki.ev.on('messages.upsert', async ({ messages }) => {
        // Check memory pressure
        const currentStats = store.getPerformanceStats()
        if (currentStats.memoryStats?.memoryPressure > 0.8) {
            logger.warn('High memory pressure detected:', currentStats.memoryStats.memoryPressure)
        }
        
        logger.info(`Processing ${messages.length} messages`)
    })
    
    // 7. Set up TTL monitoring alerts
    setInterval(async () => {
        const ttlCheck = await store.getTTLStatus()
        if (ttlCheck.summary?.totalExpiredDocuments > 0) {
            logger.warn(`⚠️ Found ${ttlCheck.summary.totalExpiredDocuments} expired documents!`)
            logger.warn('TTL cleanup may not be working properly')
        }
    }, 3600000) // Check every hour
    
    // Save credentials
    suki.ev.on('creds.update', saveCreds)
    
    // Connection handler
    suki.ev.on('connection.update', async (update) => {
        const { connection } = update
        if (connection === 'open') {
            logger.info('Connected to WhatsApp')
            
            // Log final security status
            console.log('\n🔐 Security Status:')
            console.log('✅ API Key Authentication: Enabled')
            console.log('✅ Instance Isolation: Strict')
            console.log('✅ Input Validation: Active')
            console.log('✅ Memory Monitoring: Active')
            console.log('✅ TTL Monitoring: Active')
            console.log(`✅ Instance ID: ${store.instanceId}`)
        }
    })
    
    // Graceful shutdown
    process.on('SIGINT', async () => {
        logger.info('Shutting down securely...')
        
        // Flush any pending operations
        await store.flushLabelAssociations()
        
        // Get final metrics
        const finalStats = store.getPerformanceStats()
        console.log('\n📊 Final Statistics:')
        console.log('Total messages processed:', finalStats.messagesProcessed)
        console.log('Total errors:', finalStats.errors)
        
        // Close store
        await store.close()
        process.exit(0)
    })
}

// Example usage with API key authentication from environment
async function connectWithApiKey() {
    const apiKey = process.env.BAILEYS_API_KEY
    
    if (!apiKey) {
        console.error('Please set BAILEYS_API_KEY environment variable')
        console.log('Generate one using: generateApiKey()')
        return
    }
    
    const store = await makeMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: 'whatsapp_api',
        instanceId: 'api-instance',
        auth: {
            enableApiKey: true,
            apiKeys: new Map([
                [apiKey, 'api-instance']
            ])
        }
    })
    
    console.log('Connected with API key authentication')
    return store
}

// Run the example
if (require.main === module) {
    demonstrateSecurityFeatures().catch((err) => {
        logger.error('Failed to start secure example:', err)
        process.exit(1)
    })
}

module.exports = {
    createSecureStore,
    connectWithApiKey
}