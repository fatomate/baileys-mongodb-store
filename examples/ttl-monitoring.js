const { makeMongoDBStore, makeEnhancedMongoDBStore } = require('../src')
const pino = require('pino')

const logger = pino({ level: 'info' })

/**
 * Example: TTL Monitoring and Data Retention
 * 
 * This example demonstrates:
 * - TTL index verification
 * - Expired document detection
 * - Custom TTL per collection (enhanced store)
 * - Manual cleanup options
 */

async function createTTLMonitoredStore() {
    const store = await makeMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: 'whatsapp_ttl_test',
        instanceId: 'ttl-test-instance',
        ttlDays: 7, // 7 days retention
        
        // TTL monitoring configuration
        ttlMonitoring: {
            // Enable automatic monitoring
            enableMonitoring: true,
            
            // Check every 5 minutes for demo (normally 60 minutes)
            checkIntervalMinutes: 5,
            
            // Alert if documents are 1 day past TTL
            alertThresholdDays: 1
        },
        
        logger: logger.child({ module: 'ttl-test' }),
        logLevel: 'all'
    })
    
    return store
}

async function createEnhancedTTLStore() {
    const store = await makeEnhancedMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: 'whatsapp_ttl_enhanced',
        instanceId: 'ttl-enhanced-instance',
        
        // Global TTL
        ttlDays: 30, // 30 days default
        
        // Custom TTL per collection
        collectionTTL: {
            messages: 7,      // Keep messages for 7 days
            chats: 30,        // Keep chats for 30 days
            contacts: 90,     // Keep contacts for 90 days
            presences: 1,     // Keep presence data for 1 day
            state: 365        // Keep connection state for 1 year
        },
        
        ttlMonitoring: {
            enableMonitoring: true,
            checkIntervalMinutes: 10,
            alertThresholdDays: 2
        },
        
        logger: logger.child({ module: 'ttl-enhanced' }),
        logLevel: 'warn'
    })
    
    return store
}

async function demonstrateTTLMonitoring() {
    const store = await createTTLMonitoredStore()
    
    console.log('⏰ TTL Monitoring Demo Started\n')
    
    // 1. Check initial TTL status
    console.log('📊 Initial TTL Status:')
    const initialStatus = await store.getTTLStatus()
    console.log('TTL enabled:', initialStatus.enabled)
    console.log('TTL days:', initialStatus.ttlDays)
    
    if (initialStatus.summary) {
        console.log('\nCollection Status:')
        console.log('Total collections:', initialStatus.summary.totalCollections)
        console.log('Collections with TTL:', initialStatus.summary.collectionsWithTTL)
        console.log('Collections with expired docs:', initialStatus.summary.collectionsWithExpiredDocs)
        console.log('Total expired documents:', initialStatus.summary.totalExpiredDocuments)
    }
    
    // 2. Check index status
    console.log('\n📑 Index Status:')
    const indexStatus = await store.getIndexStatus()
    
    for (const collection of indexStatus) {
        const ttlIndex = collection.indexes.find(idx => idx.expireAfterSeconds !== undefined)
        if (ttlIndex) {
            const days = ttlIndex.expireAfterSeconds / (24 * 60 * 60)
            console.log(`${collection.collection}: TTL = ${days} days`)
        } else {
            console.log(`${collection.collection}: No TTL index`)
        }
    }
    
    // 3. Insert test data with different ages
    console.log('\n📝 Inserting test data with various ages...')
    
    // Current message
    await store.upsertMessage('test@s.whatsapp.net', {
        key: { id: 'current-msg', remoteJid: 'test@s.whatsapp.net', fromMe: false },
        message: { conversation: 'Current message' },
        messageTimestamp: Math.floor(Date.now() / 1000)
    })
    
    // Old message (manually set old timestamp for testing)
    const oldMessage = {
        key: { id: 'old-msg', remoteJid: 'test@s.whatsapp.net', fromMe: false },
        message: { conversation: 'Old message' },
        messageTimestamp: Math.floor((Date.now() - 10 * 24 * 60 * 60 * 1000) / 1000) // 10 days old
    }
    
    // Note: In production, TTL is based on updatedAt field, not messageTimestamp
    console.log('Created test messages')
    
    // 4. Monitor TTL status over time
    console.log('\n🔍 Monitoring TTL status (check every minute)...')
    let checkCount = 0
    
    const ttlMonitor = setInterval(async () => {
        checkCount++
        console.log(`\n--- TTL Check #${checkCount} ---`)
        
        const status = await store.getTTLStatus()
        
        if (status.metrics) {
            console.log('Last check:', new Date(status.metrics.lastCheck).toLocaleTimeString())
            console.log('Collections checked:', status.metrics.collectionsChecked)
            console.log('Expired documents found:', status.metrics.totalExpiredDocuments)
            
            if (status.metrics.warnings.length > 0) {
                console.log('\n⚠️  Warnings:')
                status.metrics.warnings.forEach(warning => console.log(`  - ${warning}`))
            }
            
            if (status.metrics.failedCollections.length > 0) {
                console.log('\n❌ Failed collections:', status.metrics.failedCollections.join(', '))
            }
        }
        
        // Show detailed status for each collection
        if (status.details && checkCount === 3) {
            console.log('\n📊 Detailed Collection Status:')
            for (const detail of status.details) {
                if (detail.totalDocuments > 0) {
                    console.log(`\n${detail.collection}:`)
                    console.log(`  Total documents: ${detail.totalDocuments}`)
                    console.log(`  Expired documents: ${detail.expiredDocuments}`)
                    console.log(`  TTL index exists: ${detail.ttlIndexExists}`)
                    console.log(`  TTL working: ${detail.ttlWorking}`)
                    
                    if (detail.oldestDocument) {
                        console.log(`  Oldest document: ${detail.oldestDocument.age} days old`)
                    }
                }
            }
        }
        
        if (checkCount >= 5) {
            clearInterval(ttlMonitor)
            console.log('\n✅ TTL monitoring demo completed!')
            await store.close()
        }
    }, 60000) // Check every minute
    
    // For demo purposes, also do an immediate detailed check
    setTimeout(async () => {
        console.log('\n🔎 Immediate detailed TTL check:')
        const detailedStatus = await store.getTTLStatus()
        
        if (detailedStatus.details) {
            detailedStatus.details.forEach(detail => {
                if (detail.expiredDocuments > 0) {
                    console.log(`\n⚠️  ${detail.collection} has ${detail.expiredDocuments} expired documents!`)
                    console.log('TTL may not be working properly for this collection.')
                }
            })
        }
    }, 5000)
}

// Example: Enhanced store with custom TTL
async function demonstrateEnhancedTTL() {
    console.log('\n🚀 Enhanced TTL Configuration Demo\n')
    
    const store = await createEnhancedTTLStore()
    
    // Show configuration
    console.log('📋 Collection-specific TTL Configuration:')
    const status = await store.getTTLStatus()
    
    if (status.collectionTTL) {
        Object.entries(status.collectionTTL).forEach(([collection, days]) => {
            console.log(`  ${collection}: ${days} days`)
        })
    }
    
    // Test with different event types
    console.log('\n📝 Testing different event types...')
    
    // These will have different retention periods based on configuration
    await store.bind({
        on: (event, handler) => {
            // Simulate events
            if (event === 'messages.upsert') {
                handler({
                    messages: [{
                        key: { id: 'test-1', remoteJid: 'user@s.whatsapp.net', fromMe: false },
                        message: { conversation: 'Test message' },
                        messageTimestamp: Date.now() / 1000
                    }],
                    type: 'notify'
                })
            }
            
            if (event === 'presence.update') {
                handler({
                    id: 'user@s.whatsapp.net',
                    presences: { 'user@s.whatsapp.net': { lastKnownPresence: 'available' } }
                })
            }
        }
    })
    
    // Check event metrics
    const eventMetrics = store.getEventMetrics()
    console.log('\n📈 Event Processing Metrics:')
    eventMetrics.forEach(metric => {
        console.log(`${metric.eventType}:`)
        console.log(`  Received: ${metric.totalReceived}`)
        console.log(`  Stored: ${metric.totalStored}`)
    })
    
    await store.close()
    console.log('\n✅ Enhanced TTL demo completed!')
}

// Example: Manual cleanup
async function demonstrateManualCleanup() {
    console.log('\n🧹 Manual Cleanup Demo\n')
    
    const { TTLMonitor } = require('../src/utils/ttl')
    const { MongoClient } = require('mongodb')
    
    // Connect directly to MongoDB
    const client = new MongoClient(process.env.MONGODB_URI || 'mongodb://localhost:27017')
    await client.connect()
    const db = client.db('whatsapp_cleanup_test')
    
    // Create TTL monitor
    const monitor = new TTLMonitor(db, {
        days: 7,
        enableMonitoring: false // Manual mode
    })
    
    // Check for expired documents
    console.log('🔍 Checking for expired documents...')
    const collections = ['messages', 'chats', 'contacts']
    
    for (const collection of collections) {
        const result = await monitor.checkExpiredDocuments(`baileys_${collection}`)
        
        if (result.expiredDocuments > 0) {
            console.log(`\n${collection}: Found ${result.expiredDocuments} expired documents`)
            
            // Dry run first
            const dryRun = await monitor.cleanupExpiredDocuments(`baileys_${collection}`, 'updatedAt', true)
            console.log(`  Would delete: ${dryRun.deleted} documents (dry run)`)
            
            // Ask for confirmation (in real app)
            console.log('  To actually delete, run with dryRun=false')
            
            // Actual cleanup (commented out for safety)
            // const cleanup = await monitor.cleanupExpiredDocuments(`baileys_${collection}`, 'updatedAt', false)
            // console.log(`  Deleted: ${cleanup.deleted} documents`)
        }
    }
    
    await client.close()
    console.log('\n✅ Manual cleanup demo completed!')
}

// Run the examples
if (require.main === module) {
    console.log('Choose an example:')
    console.log('1. Basic TTL monitoring')
    console.log('2. Enhanced store with custom TTL')
    console.log('3. Manual cleanup demo')
    
    const example = process.argv[2] || '1'
    
    switch (example) {
        case '2':
            demonstrateEnhancedTTL().catch(console.error)
            break
        case '3':
            demonstrateManualCleanup().catch(console.error)
            break
        default:
            demonstrateTTLMonitoring().catch(console.error)
    }
}

module.exports = {
    createTTLMonitoredStore,
    createEnhancedTTLStore
}