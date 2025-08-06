export { makeMongoDBStore, cleanupMongoDBStore } from './makeMongoDBStore'
export type { MongoDBStore, MongoDBStoreConfig } from './types'

// For CommonJS compatibility
/* eslint-disable @typescript-eslint/no-var-requires */
module.exports = {
    makeMongoDBStore: require('./makeMongoDBStore').makeMongoDBStore,
    cleanupMongoDBStore: require('./makeMongoDBStore').cleanupMongoDBStore
}