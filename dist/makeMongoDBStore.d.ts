import type { MongoDBStoreConfig, MongoDBStore } from './types';
export declare const makeMongoDBStore: (config: MongoDBStoreConfig) => Promise<MongoDBStore>;
export declare const cleanupMongoDBStore: (instanceId?: string, deleteData?: boolean) => Promise<void>;
//# sourceMappingURL=makeMongoDBStore.d.ts.map