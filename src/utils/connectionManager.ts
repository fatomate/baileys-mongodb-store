import { MongoClient, Db, MongoClientOptions } from 'mongodb'
import {
    ConnectionTier,
    TierConfiguration,
    InstanceMetrics,
    ConnectionPool,
    ConnectionConfig,
    ConnectionManagerConfig,
    ConnectionMetrics,
    PoolSelectionResult,
    InstanceRegistration,
    TierClassificationRules
} from '../types/connection'

/**
 * Singleton ConnectionManager for managing shared MongoDB connections
 * across multiple instances with automatic tiered pooling
 */
export class ConnectionManager {
    private static instance: ConnectionManager | null = null
    
    // Connection pools organized by tier
    private pools: Map<string, ConnectionPool> = new Map()
    private instanceMetrics: Map<string, InstanceMetrics> = new Map()
    private instancePools: Map<string, string> = new Map() // instanceId -> poolId
    
    // Configuration
    private config: Required<ConnectionManagerConfig>
    private tierConfigs: Record<ConnectionTier, TierConfiguration>
    private classificationRules: Required<TierClassificationRules>
    
    // Monitoring
    private monitoringTimer: NodeJS.Timeout | null = null
    private cleanupTimer: NodeJS.Timeout | null = null
    private metricsEnabled: boolean = true
    
    // Logging
    private logLevel: ConnectionManagerConfig['logLevel'] = 'none'

    private redactConnectionString(input: string): string {
        if (!input) {
            return input
        }

        try {
            if (/mongodb(\+srv)?:\/\//i.test(input)) {
                const sanitized = input.replace(/:\/\/([^:@]+):([^@]+)@/i, '://$1:***@')
                const url = new URL(sanitized)
                url.search = ''
                return url.toString()
            }
            return input
        } catch (error) {
            return input
        }
    }
    
    private constructor(config?: ConnectionManagerConfig) {
        // Set default configuration
        this.config = {
            maxTotalConnections: config?.maxTotalConnections ?? 500,
            monitoringInterval: config?.monitoringInterval ?? 60000, // 1 minute
            cleanupInterval: config?.cleanupInterval ?? 300000, // 5 minutes
            enableMetrics: config?.enableMetrics ?? true,
            logLevel: config?.logLevel ?? 'none',
            tierConfigurations: config?.tierConfigurations ?? {}
        }
        
        this.logLevel = this.config.logLevel
        this.metricsEnabled = this.config.enableMetrics
        
        // Set tier configurations with defaults
        this.tierConfigs = {
            hot: {
                maxPoolSize: config?.tierConfigurations?.hot?.maxPoolSize ?? 50,
                minPoolSize: config?.tierConfigurations?.hot?.minPoolSize ?? 20,
                maxInstancesPerPool: config?.tierConfigurations?.hot?.maxInstancesPerPool ?? 20,
                maxIdleTimeMS: config?.tierConfigurations?.hot?.maxIdleTimeMS ?? 60000
            },
            warm: {
                maxPoolSize: config?.tierConfigurations?.warm?.maxPoolSize ?? 20,
                minPoolSize: config?.tierConfigurations?.warm?.minPoolSize ?? 5,
                maxInstancesPerPool: config?.tierConfigurations?.warm?.maxInstancesPerPool ?? 100,
                maxIdleTimeMS: config?.tierConfigurations?.warm?.maxIdleTimeMS ?? 120000
            },
            cold: {
                maxPoolSize: config?.tierConfigurations?.cold?.maxPoolSize ?? 10,
                minPoolSize: config?.tierConfigurations?.cold?.minPoolSize ?? 2,
                maxInstancesPerPool: config?.tierConfigurations?.cold?.maxInstancesPerPool ?? 500,
                maxIdleTimeMS: config?.tierConfigurations?.cold?.maxIdleTimeMS ?? 300000
            }
        }
        
        // Set classification rules
        this.classificationRules = {
            hotThreshold: 100,
            warmThreshold: 10,
            idleMinutes: 30,
            evaluationWindow: 5
        }
        
        // Start monitoring
        this.startMonitoring()
        this.startCleanup()
    }
    
    /**
     * Get singleton instance of ConnectionManager
     */
    public static getInstance(config?: ConnectionManagerConfig): ConnectionManager {
        if (!ConnectionManager.instance) {
            ConnectionManager.instance = new ConnectionManager(config)
        }
        return ConnectionManager.instance
    }
    
    /**
     * Register an instance and get a connection
     */
    public async registerInstance(registration: InstanceRegistration): Promise<{ db: Db; client: MongoClient }> {
        const { instanceId, uri, database, config } = registration
        
        this.log('info', `Registering instance ${instanceId}`)
        
        // Initialize metrics for new instance
        if (!this.instanceMetrics.has(instanceId)) {
            const initialTier = config?.tier || 'warm'
            this.instanceMetrics.set(instanceId, {
                instanceId,
                tier: initialTier,
                operationsPerMinute: 0,
                lastActivityTime: new Date(),
                totalOperations: 0,
                avgResponseTime: 0,
                queueDepth: 0,
                tierChangedAt: new Date(),
                operationHistory: [0, 0, 0, 0, 0]
            })
        }
        
        // Update activity
        this.recordActivity(instanceId)
        
        // Get or create appropriate pool
        const poolSelection = await this.selectOrCreatePool(instanceId, uri, database, config)
        
        // Track instance-pool mapping
        this.instancePools.set(instanceId, poolSelection.pool.id)
        poolSelection.pool.instances.add(instanceId)

        this.log('info', `Instance ${instanceId} assigned to pool ${this.redactConnectionString(poolSelection.pool.id)} (${poolSelection.pool.tier} tier)`)
        
        return {
            db: poolSelection.pool.db,
            client: poolSelection.pool.client
        }
    }
    
    /**
     * Unregister an instance
     */
    public async unregisterInstance(instanceId: string): Promise<void> {
        this.log('info', `Unregistering instance ${instanceId}`)
        
        const poolId = this.instancePools.get(instanceId)
        if (poolId) {
            const pool = this.pools.get(poolId)
            if (pool) {
                pool.instances.delete(instanceId)
                
                // Close pool if no more instances
                if (pool.instances.size === 0) {
                    await this.closePool(poolId)
                }
            }
        }
        
        this.instancePools.delete(instanceId)
        this.instanceMetrics.delete(instanceId)
    }
    
    /**
     * Record activity for an instance
     */
    public recordActivity(instanceId: string, responseTime?: number): void {
        const metrics = this.instanceMetrics.get(instanceId)
        if (!metrics) return
        
        metrics.lastActivityTime = new Date()
        metrics.totalOperations++
        metrics.operationsPerMinute++
        
        if (responseTime !== undefined) {
            // Update average response time
            metrics.avgResponseTime = (metrics.avgResponseTime * (metrics.totalOperations - 1) + responseTime) / metrics.totalOperations
        }
    }
    
    /**
     * Update queue depth for an instance
     */
    public updateQueueDepth(instanceId: string, depth: number): void {
        const metrics = this.instanceMetrics.get(instanceId)
        if (metrics) {
            metrics.queueDepth = depth
        }
    }
    
    /**
     * Get connection metrics
     */
    public getMetrics(): ConnectionMetrics {
        const metrics: ConnectionMetrics = {
            totalConnections: 0,
            totalPools: this.pools.size,
            pools: { hot: 0, warm: 0, cold: 0 },
            instances: { total: 0, hot: 0, warm: 0, cold: 0 },
            utilizationRate: 0,
            avgResponseTime: 0,
            connectionWaitTime: 0,
            poolUtilization: {}
        }
        
        // Count pools and connections by tier
        for (const [poolId, pool] of this.pools) {
            metrics.pools[pool.tier]++
            metrics.totalConnections += this.tierConfigs[pool.tier].maxPoolSize
            
            metrics.poolUtilization[poolId] = {
                tier: pool.tier,
                instances: pool.instances.size,
                activeOperations: pool.activeOperations,
                utilizationPercent: (pool.instances.size / this.tierConfigs[pool.tier].maxInstancesPerPool) * 100
            }
        }
        
        // Count instances by tier
        let totalResponseTime = 0
        for (const instanceMetric of this.instanceMetrics.values()) {
            metrics.instances.total++
            metrics.instances[instanceMetric.tier]++
            totalResponseTime += instanceMetric.avgResponseTime
        }
        
        if (metrics.instances.total > 0) {
            metrics.avgResponseTime = totalResponseTime / metrics.instances.total
        }
        
        // Calculate utilization rate
        if (this.config.maxTotalConnections > 0) {
            metrics.utilizationRate = metrics.totalConnections / this.config.maxTotalConnections
        }
        
        return metrics
    }
    
    /**
     * Force tier change for an instance
     */
    public async setInstanceTier(instanceId: string, tier: ConnectionTier): Promise<void> {
        const metrics = this.instanceMetrics.get(instanceId)
        if (!metrics) return
        
        if (metrics.tier === tier) return
        
        this.log('info', `Manually changing instance ${instanceId} from ${metrics.tier} to ${tier}`)
        
        // Update tier
        const oldTier = metrics.tier
        metrics.tier = tier
        metrics.tierChangedAt = new Date()
        
        // Migrate to appropriate pool
        await this.migrateInstancePool(instanceId, oldTier, tier)
    }
    
    /**
     * Get instance metrics
     */
    public getInstanceMetrics(instanceId: string): InstanceMetrics | undefined {
        return this.instanceMetrics.get(instanceId)
    }
    
    /**
     * Shutdown connection manager
     */
    public async shutdown(): Promise<void> {
        this.log('info', 'Shutting down ConnectionManager')
        
        // Stop monitoring
        if (this.monitoringTimer) {
            clearInterval(this.monitoringTimer)
            this.monitoringTimer = null
        }
        
        if (this.cleanupTimer) {
            clearInterval(this.cleanupTimer)
            this.cleanupTimer = null
        }
        
        // Close all pools
        const closePromises: Promise<void>[] = []
        for (const poolId of this.pools.keys()) {
            closePromises.push(this.closePool(poolId))
        }
        
        await Promise.all(closePromises)
        
        // Clear all data
        this.pools.clear()
        this.instanceMetrics.clear()
        this.instancePools.clear()
        
        // Clear singleton instance
        ConnectionManager.instance = null
    }
    
    // Private methods
    
    private async selectOrCreatePool(
        instanceId: string,
        uri: string,
        database: string,
        config?: ConnectionConfig
    ): Promise<PoolSelectionResult> {
        const metrics = this.instanceMetrics.get(instanceId)!
        const tier = metrics.tier

        if (config?.poolStrategy === 'dedicated') {
            if (this.getTotalConnections() + this.tierConfigs[tier].minPoolSize > this.config.maxTotalConnections) {
                await this.closeIdlePools()

                if (this.getTotalConnections() + this.tierConfigs[tier].minPoolSize > this.config.maxTotalConnections) {
                    throw new Error(`Cannot allocate dedicated pool: would exceed maximum connections (${this.config.maxTotalConnections})`)
                }
            }
            const pool = await this.createPool(uri, database, tier)
            return { pool, isNew: true, reason: 'dedicated' }
        }

        // Look for existing pool with capacity
        for (const [, pool] of this.pools) {
            if (pool.uri === uri && 
                pool.database === database && 
                pool.tier === tier &&
                pool.instances.size < this.tierConfigs[tier].maxInstancesPerPool) {

                pool.lastUsedAt = new Date()
                return { pool, isNew: false, reason: 'existing' }
            }
        }
        
        // Check if we can create a new pool
        if (this.getTotalConnections() + this.tierConfigs[tier].minPoolSize > this.config.maxTotalConnections) {
            // Try to free up space by closing idle pools
            await this.closeIdlePools()
            
            // If still over limit, find the least loaded pool
            const fallbackPool = this.findLeastLoadedPool(uri, database)
            if (fallbackPool) {
                this.log('warn', `Connection limit reached, using fallback pool for ${instanceId}`)
                return { pool: fallbackPool, isNew: false, reason: 'existing' }
            }

            throw new Error(`Cannot create new pool: would exceed maximum connections (${this.config.maxTotalConnections})`)
        }
        
        // Create new pool
        const pool = await this.createPool(uri, database, tier)
        return { pool, isNew: true, reason: 'created' }
    }
    
    private async createPool(uri: string, database: string, tier: ConnectionTier): Promise<ConnectionPool> {
        const poolId = `${uri}:${database}:${tier}:${Date.now()}`
        const tierConfig = this.tierConfigs[tier]
        
        this.log('info', `Creating new ${tier} pool: ${this.redactConnectionString(poolId)}`)
        
        const clientOptions: MongoClientOptions = {
            maxPoolSize: tierConfig.maxPoolSize,
            minPoolSize: tierConfig.minPoolSize,
            maxIdleTimeMS: tierConfig.maxIdleTimeMS,
            writeConcern: { w: 1, j: false }
        }
        
        const client = new MongoClient(uri, clientOptions)
        await client.connect()
        
        const pool: ConnectionPool = {
            id: poolId,
            uri,
            database,
            tier,
            client,
            db: client.db(database),
            instances: new Set(),
            createdAt: new Date(),
            lastUsedAt: new Date(),
            activeOperations: 0,
            isClosing: false,
            acceptingOperations: true
        }
        
        this.pools.set(poolId, pool)
        return pool
    }
    
    private async closePool(poolId: string): Promise<void> {
        const pool = this.pools.get(poolId)
        if (!pool) return
        
        this.log('info', `Closing pool ${this.redactConnectionString(poolId)}`)
        
        // Mark pool as closing
        pool.isClosing = true
        
        // Stop accepting new operations
        pool.acceptingOperations = false
        
        // Wait for active operations to complete (with timeout)
        const waitForOperations = async () => {
            const maxWait = 5000 // 5 seconds
            const startTime = Date.now()
            
            while (pool.activeOperations > 0) {
                if (Date.now() - startTime > maxWait) {
                    this.log('warn', `Timeout waiting for operations to complete in pool ${poolId}, forcing close`)
                    break
                }
                await new Promise(resolve => setTimeout(resolve, 100))
            }
        }
        
        try {
            await waitForOperations()
            
            // Close the MongoDB client with force flag
            await pool.client.close(true)
            
            // Wait a bit for the close to complete
            await new Promise(resolve => setTimeout(resolve, 200))
            
        } catch (error) {
            this.log('error', `Error closing pool ${poolId}: ${error}`)
        } finally {
            // Always remove from pools map
            this.pools.delete(poolId)
            
            // Clean up instance associations
            for (const [instanceId, associatedPoolId] of this.instancePools) {
                if (associatedPoolId === poolId) {
                    this.instancePools.delete(instanceId)
                }
            }
        }
        
        this.log('info', `Pool ${poolId} closed successfully`)
    }
    
    private async migrateInstancePool(instanceId: string, fromTier: ConnectionTier, toTier: ConnectionTier): Promise<void> {
        const currentPoolId = this.instancePools.get(instanceId)
        if (!currentPoolId) return
        
        const currentPool = this.pools.get(currentPoolId)
        if (!currentPool) return
        
        // Get registration info from current pool
        const { uri, database } = currentPool
        
        // Find or create new pool
        const newPoolSelection = await this.selectOrCreatePool(instanceId, uri, database)
        
        // Remove from old pool
        currentPool.instances.delete(instanceId)
        
        // Add to new pool
        newPoolSelection.pool.instances.add(instanceId)
        this.instancePools.set(instanceId, newPoolSelection.pool.id)
        
        // Close old pool if empty
        if (currentPool.instances.size === 0) {
            await this.closePool(currentPoolId)
        }
        
        this.log('info', `Migrated instance ${instanceId} from ${fromTier} to ${toTier}`)
    }
    
    private classifyInstance(metrics: InstanceMetrics): ConnectionTier {
        const minutesSinceActivity = (Date.now() - metrics.lastActivityTime.getTime()) / 60000
        
        // Check if idle
        if (minutesSinceActivity > this.classificationRules.idleMinutes) {
            return 'cold'
        }
        
        // Calculate average operations per minute over evaluation window
        const avgOperations = metrics.operationHistory.reduce((a, b) => a + b, 0) / metrics.operationHistory.length
        
        // Classify based on activity
        if (avgOperations >= this.classificationRules.hotThreshold) {
            return 'hot'
        } else if (avgOperations >= this.classificationRules.warmThreshold) {
            return 'warm'
        } else {
            return 'cold'
        }
    }
    
    private async monitorInstances(): Promise<void> {
        // Update operation histories
        for (const metrics of this.instanceMetrics.values()) {
            // Shift history and add current minute
            metrics.operationHistory.shift()
            metrics.operationHistory.push(metrics.operationsPerMinute)
            metrics.operationsPerMinute = 0 // Reset counter
            
            // Classify and potentially migrate
            const newTier = this.classifyInstance(metrics)
            if (newTier !== metrics.tier) {
                const oldTier = metrics.tier
                metrics.tier = newTier
                metrics.tierChangedAt = new Date()
                
                this.log('info', `Auto-reclassifying instance ${metrics.instanceId} from ${oldTier} to ${newTier}`)
                
                // Migrate to appropriate pool
                await this.migrateInstancePool(metrics.instanceId, oldTier, newTier)
            }
        }
        
        // Log metrics if enabled
        if (this.metricsEnabled && this.logLevel === 'debug') {
            const metrics = this.getMetrics()
            this.log('debug', `Connection metrics: ${JSON.stringify(metrics)}`)
        }
    }
    
    private async closeIdlePools(): Promise<void> {
        const now = Date.now()
        const poolsToClose: string[] = []
        
        for (const [poolId, pool] of this.pools) {
            // Don't close pools with active instances
            if (pool.instances.size > 0) continue
            
            const idleTime = now - pool.lastUsedAt.getTime()
            const maxIdleTime = this.tierConfigs[pool.tier].maxIdleTimeMS
            
            if (idleTime > maxIdleTime) {
                poolsToClose.push(poolId)
            }
        }
        
        for (const poolId of poolsToClose) {
            await this.closePool(poolId)
        }
        
        if (poolsToClose.length > 0) {
            this.log('info', `Closed ${poolsToClose.length} idle pools`)
        }
    }
    
    private findLeastLoadedPool(uri: string, database: string): ConnectionPool | null {
        let leastLoadedPool: ConnectionPool | null = null
        let minLoad = Infinity
        
        for (const pool of this.pools.values()) {
            if (pool.uri !== uri || pool.database !== database) continue

            const load = pool.instances.size / this.tierConfigs[pool.tier].maxInstancesPerPool
            if (load >= 1) continue
            if (load < minLoad) {
                minLoad = load
                leastLoadedPool = pool
            }
        }
        
        return leastLoadedPool
    }
    
    private getTotalConnections(): number {
        let total = 0
        for (const pool of this.pools.values()) {
            total += this.tierConfigs[pool.tier].maxPoolSize
        }
        return total
    }
    
    private startMonitoring(): void {
        if (this.monitoringTimer) return
        
        this.monitoringTimer = setInterval(() => {
            this.monitorInstances().catch(error => {
                this.log('error', `Monitoring error: ${error}`)
            })
        }, this.config.monitoringInterval)
    }
    
    private startCleanup(): void {
        if (this.cleanupTimer) return
        
        this.cleanupTimer = setInterval(() => {
            this.closeIdlePools().catch(error => {
                this.log('error', `Cleanup error: ${error}`)
            })
        }, this.config.cleanupInterval)
    }
    
    private log(level: ConnectionManagerConfig['logLevel'], message: string): void {
        if (!level || this.logLevel === 'none') return
        
        const levels: readonly string[] = ['none', 'error', 'warn', 'info', 'debug']
        const currentLevelIndex = levels.indexOf(this.logLevel || 'none')
        const messageLevelIndex = levels.indexOf(level)
        
        if (messageLevelIndex <= currentLevelIndex) {
            const prefix = `[ConnectionManager] [${level.toUpperCase()}]`
            
            switch (level) {
                case 'error':
                    console.error(`${prefix} ${message}`)
                    break
                case 'warn':
                    console.warn(`${prefix} ${message}`)
                    break
                case 'info':
                case 'debug':
                    console.log(`${prefix} ${message}`)
                    break
            }
        }
    }
}

// Export singleton getter for convenience
export const getConnectionManager = (config?: ConnectionManagerConfig): ConnectionManager => {
    return ConnectionManager.getInstance(config)
}

// Export metrics getter for convenience
export const getConnectionMetrics = (): ConnectionMetrics => {
    return ConnectionManager.getInstance().getMetrics()
}
