export { makeMongoDBStore, cleanupMongoDBStore } from './makeMongoDBStore'
export type { MongoDBStore, MongoDBStoreConfig } from './types'

// For CommonJS compatibility
module.exports = {
    makeMongoDBStore: require('./makeMongoDBStore').makeMongoDBStore,
    cleanupMongoDBStore: require('./makeMongoDBStore').cleanupMongoDBStore
}