export { makeMongoDBStore, cleanupMongoDBStore } from './makeMongoDBStore'
export { makeEnhancedMongoDBStore } from './makeEnhancedMongoDBStore'
export type { MongoDBStore, MongoDBStoreConfig } from './types'
export type { EnhancedMongoDBStore, EnhancedMongoDBStoreConfig } from './types-enhanced'

// For CommonJS compatibility
/* eslint-disable @typescript-eslint/no-var-requires */
module.exports = {
    makeMongoDBStore: require('./makeMongoDBStore').makeMongoDBStore,
    cleanupMongoDBStore: require('./makeMongoDBStore').cleanupMongoDBStore,
    makeEnhancedMongoDBStore: require('./makeEnhancedMongoDBStore').makeEnhancedMongoDBStore
}