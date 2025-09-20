import { MongoClient, Db } from 'mongodb'

export type ConnectionTier = 'hot' | 'warm' | 'cold'

export interface TierConfiguration {
    maxPoolSize: number
    minPoolSize: number
    maxInstancesPerPool: number
    maxIdleTimeMS: number
}

export interface InstanceMetrics {
    instanceId: string
    tier: ConnectionTier
    operationsPerMinute: number
    lastActivityTime: Date
    totalOperations: number
    avgResponseTime: number
    queueDepth: number
    tierChangedAt: Date
    operationHistory: number[] // Operations per minute for last 5 minutes
}

export interface ConnectionPool {
    id: string
    uri: string
    database: string
    tier: ConnectionTier
    client: MongoClient
    db: Db
    instances: Set<string>
    createdAt: Date
    lastUsedAt: Date
    activeOperations: number
    isClosing?: boolean
    acceptingOperations?: boolean
}

export interface PendingMigration {
    fromPoolId: string
    toPoolId: string
    createdAt: Date
}

export interface ConnectionConfig {
    tier?: ConnectionTier           // Initial tier hint (optional)
    autoAdjust?: boolean            // Enable automatic tier adjustment (default: true)
    poolStrategy?: 'shared' | 'dedicated'  // Pool strategy (default: 'shared')
    maxPoolSize?: number           // Override max pool size
    minPoolSize?: number           // Override min pool size
    customTierRules?: TierClassificationRules  // Custom classification rules
}

export interface TierClassificationRules {
    hotThreshold?: number      // Operations per minute to be classified as hot (default: 100)
    warmThreshold?: number     // Operations per minute to be classified as warm (default: 10)
    idleMinutes?: number       // Minutes of inactivity to be classified as cold (default: 30)
    evaluationWindow?: number  // Minutes to consider for classification (default: 5)
}

export interface ConnectionManagerConfig {
    maxTotalConnections?: number   // Global connection limit (default: 500)
    tierConfigurations?: {
        hot?: TierConfiguration
        warm?: TierConfiguration
        cold?: TierConfiguration
    }
    monitoringInterval?: number    // Milliseconds between monitoring cycles (default: 60000)
    cleanupInterval?: number       // Milliseconds between cleanup cycles (default: 300000)
    enableMetrics?: boolean        // Enable detailed metrics collection (default: true)
    logLevel?: 'none' | 'error' | 'warn' | 'info' | 'debug'  // Logging level
}

export interface ConnectionMetrics {
    totalConnections: number
    totalPools: number
    pools: {
        hot: number
        warm: number
        cold: number
    }
    instances: {
        total: number
        hot: number
        warm: number
        cold: number
    }
    utilizationRate: number
    avgResponseTime: number
    connectionWaitTime: number
    poolUtilization: {
        [poolId: string]: {
            tier: ConnectionTier
            instances: number
            activeOperations: number
            utilizationPercent: number
        }
    }
}

export interface PoolSelectionResult {
    pool: ConnectionPool
    isNew: boolean
    reason: 'existing' | 'created' | 'promoted' | 'demoted' | 'dedicated'
}

export interface InstanceRegistration {
    instanceId: string
    uri: string
    database: string
    config?: ConnectionConfig
}

export interface InstanceRegistrationResult {
    client: MongoClient
    db: Db
    poolId: string
}

export interface InstancePoolState {
    currentPoolId?: string
    pendingMigration?: PendingMigration
}
