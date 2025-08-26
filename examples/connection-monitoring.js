const { makeMongoDBStore, getConnectionMetrics, ConnectionManager } = require('../dist')
const pino = require('pino')

const logger = pino({ level: 'info' })

/**
 * Example demonstrating connection monitoring and management
 * for applications with many WhatsApp instances (1200+)
 */

// Simulated instance data
const generateInstances = (count) => {
    const instances = []
    for (let i = 1; i <= count; i++) {
        instances.push({
            id: `instance_${i.toString().padStart(4, '0')}`,
            name: `WhatsApp Bot ${i}`,
            activityLevel: i <= 50 ? 'hot' : i <= 250 ? 'warm' : 'cold'
        })
    }
    return instances
}

async function monitorConnections() {
    console.log('\n📊 Connection Metrics Dashboard')
    console.log('================================')
    
    const metrics = getConnectionMetrics()
    
    console.log('\n🔌 Connection Pools:')
    console.log(`  Total Connections: ${metrics.totalConnections}`)
    console.log(`  Total Pools: ${metrics.totalPools}`)
    console.log(`  Pool Distribution:`)
    console.log(`    - Hot: ${metrics.pools.hot} pools`)
    console.log(`    - Warm: ${metrics.pools.warm} pools`)
    console.log(`    - Cold: ${metrics.pools.cold} pools`)
    
    console.log('\n👥 Instance Distribution:')
    console.log(`  Total Instances: ${metrics.instances.total}`)
    console.log(`    - Hot: ${metrics.instances.hot} instances (high activity)`)
    console.log(`    - Warm: ${metrics.instances.warm} instances (moderate activity)`)
    console.log(`    - Cold: ${metrics.instances.cold} instances (low/idle)`)
    
    console.log('\n📈 Performance:')
    console.log(`  Utilization Rate: ${(metrics.utilizationRate * 100).toFixed(1)}%`)
    console.log(`  Avg Response Time: ${metrics.avgResponseTime.toFixed(2)}ms`)
    console.log(`  Connection Wait Time: ${metrics.connectionWaitTime.toFixed(2)}ms`)
    
    console.log('\n🔍 Pool Utilization Details:')
    Object.entries(metrics.poolUtilization).forEach(([poolId, pool]) => {
        console.log(`  ${pool.tier.toUpperCase()} Pool:`)
        console.log(`    - Instances: ${pool.instances}`)
        console.log(`    - Active Ops: ${pool.activeOperations}`)
        console.log(`    - Utilization: ${pool.utilizationPercent.toFixed(1)}%`)
    })
}

async function createInstanceWithMonitoring(instance) {
    try {
        // Create store with shared connections enabled (default)
        const store = await makeMongoDBStore({
            uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
            database: 'whatsapp_multi_instance',
            instanceId: instance.id,
            
            // Connection configuration (optional)
            connectionConfig: {
                // Initial tier hint based on expected activity
                tier: instance.activityLevel,
                // Enable automatic tier adjustment (default: true)
                autoAdjust: true,
                // Use shared connection pooling (default: 'shared')
                poolStrategy: 'shared'
            },
            
            // Enable shared connections (default: true)
            // Set to false only if you need dedicated connections
            useSharedConnections: true,
            
            ttlDays: 30,
            logLevel: 'none'
        })
        
        // Get connection metrics for this specific instance
        const instanceMetrics = store.getConnectionMetrics()
        
        logger.info(`Instance ${instance.id} connected:`, {
            connectionType: instanceMetrics.type,
            tier: instanceMetrics.tier,
            poolSize: instanceMetrics.poolSize
        })
        
        // Simulate some activity
        await simulateActivity(store, instance)
        
        return store
    } catch (error) {
        logger.error(`Failed to create instance ${instance.id}:`, error)
        throw error
    }
}

async function simulateActivity(store, instance) {
    // Simulate different activity levels
    const operationsPerMinute = 
        instance.activityLevel === 'hot' ? 200 :
        instance.activityLevel === 'warm' ? 50 : 5
    
    // Perform some operations
    for (let i = 0; i < Math.min(operationsPerMinute / 10, 10); i++) {
        await store.getChats()
        await new Promise(resolve => setTimeout(resolve, 100))
    }
}

async function demonstrateTierMigration() {
    console.log('\n🔄 Demonstrating Tier Migration')
    console.log('==================================')
    
    const manager = ConnectionManager.getInstance()
    const instanceId = 'demo_instance_001'
    
    // Create an instance starting as 'cold'
    const store = await makeMongoDBStore({
        uri: 'mongodb://localhost:27017',
        database: 'demo_db',
        instanceId,
        connectionConfig: {
            tier: 'cold'
        }
    })
    
    let metrics = manager.getInstanceMetrics(instanceId)
    console.log(`Initial tier: ${metrics?.tier}`)
    
    // Simulate increased activity
    console.log('\nSimulating increased activity...')
    for (let i = 0; i < 150; i++) {
        manager.recordActivity(instanceId, Math.random() * 100)
    }
    
    // Force tier evaluation (normally happens automatically)
    await manager.setInstanceTier(instanceId, 'hot')
    
    metrics = manager.getInstanceMetrics(instanceId)
    console.log(`New tier after activity: ${metrics?.tier}`)
    console.log(`Operations per minute: ${metrics?.operationsPerMinute}`)
    console.log(`Average response time: ${metrics?.avgResponseTime?.toFixed(2)}ms`)
    
    await store.close()
}

async function main() {
    console.log('🚀 Connection Monitoring Example for 1200+ Instances')
    console.log('=====================================================\n')
    
    // Configure connection manager for high instance count
    const manager = ConnectionManager.getInstance({
        maxTotalConnections: 500, // Limit total connections
        tierConfigurations: {
            hot: {
                maxPoolSize: 50,
                minPoolSize: 20,
                maxInstancesPerPool: 20,
                maxIdleTimeMS: 60000
            },
            warm: {
                maxPoolSize: 20,
                minPoolSize: 5,
                maxInstancesPerPool: 100,
                maxIdleTimeMS: 120000
            },
            cold: {
                maxPoolSize: 10,
                minPoolSize: 2,
                maxInstancesPerPool: 500,
                maxIdleTimeMS: 300000
            }
        },
        monitoringInterval: 60000, // Check every minute
        cleanupInterval: 300000, // Clean up every 5 minutes
        enableMetrics: true,
        logLevel: 'info'
    })
    
    // Simulate creating many instances
    const instances = generateInstances(50) // Simulate 50 instances (scale to 1200+ in production)
    const stores = []
    
    console.log(`Creating ${instances.length} instances...`)
    console.log(`  - Hot instances: ${instances.filter(i => i.activityLevel === 'hot').length}`)
    console.log(`  - Warm instances: ${instances.filter(i => i.activityLevel === 'warm').length}`)
    console.log(`  - Cold instances: ${instances.filter(i => i.activityLevel === 'cold').length}`)
    
    // Create instances in batches to avoid overwhelming the system
    const batchSize = 10
    for (let i = 0; i < instances.length; i += batchSize) {
        const batch = instances.slice(i, i + batchSize)
        const batchStores = await Promise.all(
            batch.map(instance => createInstanceWithMonitoring(instance))
        )
        stores.push(...batchStores)
        
        console.log(`  Created ${Math.min(i + batchSize, instances.length)}/${instances.length} instances`)
        
        // Show metrics every 20 instances
        if ((i + batchSize) % 20 === 0) {
            await monitorConnections()
        }
    }
    
    console.log(`\n✅ All ${instances.length} instances created successfully!`)
    
    // Final metrics
    await monitorConnections()
    
    // Demonstrate tier migration
    await demonstrateTierMigration()
    
    // Monitor for 2 minutes
    console.log('\n⏰ Monitoring connections for 2 minutes...')
    console.log('   (Instances will auto-adjust tiers based on activity)\n')
    
    const monitoringInterval = setInterval(async () => {
        await monitorConnections()
    }, 30000) // Every 30 seconds
    
    // Simulate varying activity levels
    const activityInterval = setInterval(async () => {
        // Random instances get activity bursts
        const randomStores = stores
            .sort(() => Math.random() - 0.5)
            .slice(0, 5)
        
        for (const store of randomStores) {
            await store.getChats()
        }
    }, 5000) // Every 5 seconds
    
    // Clean up after 2 minutes
    setTimeout(async () => {
        clearInterval(monitoringInterval)
        clearInterval(activityInterval)
        
        console.log('\n🛑 Shutting down all instances...')
        
        // Close all stores
        for (const store of stores) {
            await store.close()
        }
        
        // Shutdown connection manager
        await manager.shutdown()
        
        console.log('✅ All instances shut down successfully')
        
        // Final metrics
        console.log('\n📊 Final Statistics:')
        console.log(`  Total instances managed: ${instances.length}`)
        console.log(`  Maximum connections used: ${500}`)
        console.log(`  Connection reduction: ${((1 - (500 / (instances.length * 100))) * 100).toFixed(1)}%`)
        
        process.exit(0)
    }, 120000) // 2 minutes
}

// Export for testing
module.exports = {
    monitorConnections,
    createInstanceWithMonitoring,
    demonstrateTierMigration
}

// Run if called directly
if (require.main === module) {
    main().catch(error => {
        logger.error('Fatal error:', error)
        process.exit(1)
    })
}