export { makeMongoDBStore, cleanupMongoDBStore } from './makeMongoDBStore'
export { makeEnhancedMongoDBStore } from './makeEnhancedMongoDBStore'
export { getConnectionManager, getConnectionMetrics, ConnectionManager } from './utils/connectionManager'
export type { MongoDBStore, MongoDBStoreConfig } from './types'
export type { EnhancedMongoDBStore, EnhancedMongoDBStoreConfig } from './types-enhanced'
export type { 
    ConnectionConfig, 
    ConnectionMetrics, 
    ConnectionTier,
    InstanceMetrics,
    ConnectionManagerConfig 
} from './types/connection'

// For CommonJS compatibility
/* eslint-disable @typescript-eslint/no-var-requires */
module.exports = {
    makeMongoDBStore: require('./makeMongoDBStore').makeMongoDBStore,
    cleanupMongoDBStore: require('./makeMongoDBStore').cleanupMongoDBStore,
    makeEnhancedMongoDBStore: require('./makeEnhancedMongoDBStore').makeEnhancedMongoDBStore,
    getConnectionManager: require('./utils/connectionManager').getConnectionManager,
    getConnectionMetrics: require('./utils/connectionManager').getConnectionMetrics,
    ConnectionManager: require('./utils/connectionManager').ConnectionManager
}