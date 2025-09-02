Returning full permissions for 660BD6D64B7F7 (from cache)
User retention preference: 30 days for instance 660BD6D64B7F7
Cached retention preference: 30 days for instance 660BD6D64B7F7
Media download enabled for instance 660BD6D64B7F7
ℹ️ Index lastSeen_1 does not exist in collection wabot_lidMappings, skipping drop
[LidHandler] All indexes created successfully
ℹ️ Index updatedAt_1 does not exist in collection wabot_groupMetadata, skipping drop
ℹ️ Index updatedAt_1 does not exist in collection wabot_labels, skipping drop
ℹ️ Index updatedAt_1 does not exist in collection wabot_labelAssociations, skipping drop
✅ Created index on collection wabot_groupMetadata
✅ Created index on collection wabot_labels
ℹ️ Index instanceId_1_chatId_1_labelId_1 does not exist in collection wabot_labelAssociations, skipping drop
ℹ️ Index instanceId_1_type_1_chatId_1_labelId_1 does not exist in collection wabot_labelAssociations, skipping drop

❌ Failed to create index on collection wabot_labelAssociations: MongoServerError: An existing index has the same name as the requested index. When index names are not specified, they are auto generated and can cause conflicts. Please refer to our documentation. Requested index: { v: 2, unique: true, key: { instanceId: 1, type: 1, chatId: 1, labelId: 1 }, name: "instanceId_1_type_1_chatId_1_labelId_1", partialFilterExpression: { type: "label_jid" } }, existing index: { v: 2, unique: true, key: { instanceId: 1, type: 1, chatId: 1, labelId: 1 }, name: "instanceId_1_type_1_chatId_1_labelId_1" }
    at Connection.sendCommand (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cmap/connection.js:305:27)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async Connection.command (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cmap/connection.js:333:26)
    at async Server.command (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/sdam/server.js:171:29)
    at async CreateIndexesOperation.executeCommand (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/operations/command.js:76:16)
    at async CreateIndexesOperation.execute (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/operations/indexes.js:122:9)
    at async tryOperation (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/operations/execute_operation.js:207:20)
    at async executeOperation (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/operations/execute_operation.js:75:16)
    at async Collection.createIndex (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/collection.js:329:25)
    at async safeCreateIndex (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/indexHelper.js:49:9)
    at async /home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:1975:13
    at async /home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:619:24
    at async retryWithBackoff (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionRetry.js:43:28)
    at async withConnection (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:614:28)
    at async Promise.all (index 16)
    at async createIndexes (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:1978:9) {
  errorLabelSet: Set(0) {},
  errorResponse: {
    ok: 0,
    errmsg: 'An existing index has the same name as the requested index. When index names are not specified, they are auto generated and can cause conflicts. Please refer to our documentation. Requested index: { v: 2, unique: true, key: { instanceId: 1, type: 1, chatId: 1, labelId: 1 }, name: "instanceId_1_type_1_chatId_1_labelId_1", partialFilterExpression: { type: "label_jid" } }, existing index: { v: 2, unique: true, key: { instanceId: 1, type: 1, chatId: 1, labelId: 1 }, name: "instanceId_1_type_1_chatId_1_labelId_1" }',
    code: 86,
    codeName: 'IndexKeySpecsConflict',
    '$clusterTime': {
      clusterTime: new Timestamp({ t: 1756777372, i: 4 }),
      signature: [Object]
    },
    operationTime: new Timestamp({ t: 1756777226, i: 18 })
  },
  ok: 0,
  code: 86,
  codeName: 'IndexKeySpecsConflict',
  '$clusterTime': {
    clusterTime: new Timestamp({ t: 1756777372, i: 4 }),
    signature: {
      hash: Binary.createFromBase64('Kcocs14aGC2vbWqFacpko0jZe+Y=', 0),
      keyId: new Long('7542628940246941701')
    }
  },
  operationTime: new Timestamp({ t: 1756777226, i: 18 })
}
[withConnection] Operation failed after 1 attempts: MongoServerError: An existing index has the same name as the requested index. When index names are not specified, they are auto generated and can cause conflicts. Please refer to our documentation. Requested index: { v: 2, unique: true, key: { instanceId: 1, type: 1, chatId: 1, labelId: 1 }, name: "instanceId_1_type_1_chatId_1_labelId_1", partialFilterExpression: { type: "label_jid" } }, existing index: { v: 2, unique: true, key: { instanceId: 1, type: 1, chatId: 1, labelId: 1 }, name: "instanceId_1_type_1_chatId_1_labelId_1" }
    at Connection.sendCommand (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cmap/connection.js:305:27)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async Connection.command (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cmap/connection.js:333:26)
    at async Server.command (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/sdam/server.js:171:29)
    at async CreateIndexesOperation.executeCommand (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/operations/command.js:76:16)
    at async CreateIndexesOperation.execute (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/operations/indexes.js:122:9)
    at async tryOperation (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/operations/execute_operation.js:207:20)
    at async executeOperation (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/operations/execute_operation.js:75:16)
    at async Collection.createIndex (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/collection.js:329:25)
    at async safeCreateIndex (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/indexHelper.js:49:9)
    at async /home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:1975:13
    at async /home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:619:24
    at async retryWithBackoff (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionRetry.js:43:28)
    at async withConnection (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:614:28)
    at async Promise.all (index 16)
    at async createIndexes (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:1978:9) {
  errorLabelSet: Set(0) {},
  errorResponse: {
    ok: 0,
    errmsg: 'An existing index has the same name as the requested index. When index names are not specified, they are auto generated and can cause conflicts. Please refer to our documentation. Requested index: { v: 2, unique: true, key: { instanceId: 1, type: 1, chatId: 1, labelId: 1 }, name: "instanceId_1_type_1_chatId_1_labelId_1", partialFilterExpression: { type: "label_jid" } }, existing index: { v: 2, unique: true, key: { instanceId: 1, type: 1, chatId: 1, labelId: 1 }, name: "instanceId_1_type_1_chatId_1_labelId_1" }',
    code: 86,
    codeName: 'IndexKeySpecsConflict',
    '$clusterTime': {
      clusterTime: new Timestamp({ t: 1756777372, i: 4 }),
      signature: [Object]
    },
    operationTime: new Timestamp({ t: 1756777226, i: 18 })
  },
  ok: 0,
  code: 86,
  codeName: 'IndexKeySpecsConflict',
  '$clusterTime': {
    clusterTime: new Timestamp({ t: 1756777372, i: 4 }),
    signature: {
      hash: Binary.createFromBase64('Kcocs14aGC2vbWqFacpko0jZe+Y=', 0),
      keyId: new Long('7542628940246941701')
    }
  },
  operationTime: new Timestamp({ t: 1756777226, i: 18 })
}

Media download disabled for instance 689BEB98EFD94
ℹ️ Index lastSeen_1 does not exist in collection wabot_lidMappings, skipping drop
[LidHandler] All indexes created successfully
ℹ️ Index updatedAt_1 does not exist in collection wabot_groupMetadata, skipping drop
ℹ️ Index updatedAt_1 does not exist in collection wabot_labels, skipping drop
ℹ️ Index updatedAt_1 does not exist in collection wabot_labelAssociations, skipping drop
✅ Created index on collection wabot_groupMetadata
✅ Created index on collection wabot_labels
ℹ️ Index instanceId_1_chatId_1_labelId_1 does not exist in collection wabot_labelAssociations, skipping drop
ℹ️ Index instanceId_1_type_1_chatId_1_labelId_1 does not exist in collection wabot_labelAssociations, skipping drop
❌ Failed to create index on collection wabot_labelAssociations: MongoClientClosedError: Operation interrupted because client was closed
    at ConnectionPool.closeCheckedOutConnections (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cmap/connection_pool.js:260:26)
    at Server.closeCheckedOutConnections (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/sdam/server.js:109:26)
    at Topology.closeCheckedOutConnections (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/sdam/topology.js:221:27)
    at MongoClient._close (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/mongo_client.js:342:24)
    at MongoClient.close (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/mongo_client.js:325:35)
    at ConnectionManager.closePool (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionManager.js:261:31)
    at async ConnectionManager.migrateInstancePool (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionManager.js:290:13)
    at async ConnectionManager.monitorInstances (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionManager.js:321:17) {
  errorLabelSet: Set(0) {}
}
[withConnection] Operation failed after 1 attempts: MongoClientClosedError: Operation interrupted because client was closed
    at ConnectionPool.closeCheckedOutConnections (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cmap/connection_pool.js:260:26)
    at Server.closeCheckedOutConnections (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/sdam/server.js:109:26)
    at Topology.closeCheckedOutConnections (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/sdam/topology.js:221:27)
    at MongoClient._close (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/mongo_client.js:342:24)
    at MongoClient.close (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/mongo_client.js:325:35)
    at ConnectionManager.closePool (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionManager.js:261:31)
    at async ConnectionManager.migrateInstancePool (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionManager.js:290:13)
    at async ConnectionManager.monitorInstances (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionManager.js:321:17) {
  errorLabelSet: Set(0) {}
}
<< a few more similar lines of error > >
Cached permissions for instance 689BECE881234
permissions for module whatsapp_poll_template is defined for 689BECE881234
permissions for module whatsapp_live_chat is defined for 689BECE881234 (from cache)
Fetching account data from DB for instance_id: 689BECE881234
Account data cached for 689BECE881234 (token: 689BECE881234)
Returning full permissions for 689BECE881234 (from cache)
Live chat disabled, user retention: 7 days for instance 689BECE881234
Cached retention preference: 7 days for instance 689BECE881234
Media download disabled for instance 689BECE881234
[LID Handler] Retry attempt 1 for index creation after error: Client must be connected before running operations. Waiting 151.54036363336033ms...
<< a few more similar lines of error > >
❌ Failed to drop index lastSeen_1 from collection wabot_lidMappings: MongoNotConnectedError: Client must be connected before running operations
    at autoConnect (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/operations/execute_operation.js:95:19)
    at executeOperation (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/operations/execute_operation.js:38:40)
    at ListIndexesCursor._initialize (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/list_indexes_cursor.js:27:73)
    at ListIndexesCursor.cursorInit (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/abstract_cursor.js:632:38)
    at ListIndexesCursor.fetchBatch (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/abstract_cursor.js:666:24)
    at ListIndexesCursor.next (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/abstract_cursor.js:342:28)
    at [Symbol.asyncIterator] (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/abstract_cursor.js:250:45)
    at AsyncGenerator.next (<anonymous>)
    at ListIndexesCursor.toArray (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/abstract_cursor.js:422:26)
    at Collection.indexes (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/collection.js:485:57)
    at safeDropIndex (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/indexHelper.js:9:42)
    at LidHandler.createIndexes (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/lidHandler.js:51:59)
    at LidHandler.initialize (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/lidHandler.js:31:20)
    at initResult.maxAttempts (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:427:91)
    at retryWithBackoff (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionRetry.js:43:34)
    at makeEnhancedMongoDBStore (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:427:73) {
  errorLabelSet: Set(0) {}
}
[LidHandler] Some indexes failed to create, but continuing: MongoNotConnectedError: Client must be connected before running operations
    at autoConnect (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/operations/execute_operation.js:95:19)
    at executeOperation (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/operations/execute_operation.js:38:40)
    at ListIndexesCursor._initialize (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/list_indexes_cursor.js:27:73)
    at ListIndexesCursor.cursorInit (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/abstract_cursor.js:632:38)
    at ListIndexesCursor.fetchBatch (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/abstract_cursor.js:666:24)
    at ListIndexesCursor.next (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/abstract_cursor.js:342:28)
    at [Symbol.asyncIterator] (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/abstract_cursor.js:250:45)
    at AsyncGenerator.next (<anonymous>)
    at ListIndexesCursor.toArray (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/abstract_cursor.js:422:26)
    at Collection.indexes (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/collection.js:485:57)
    at safeDropIndex (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/indexHelper.js:9:42)
    at LidHandler.createIndexes (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/lidHandler.js:51:59)
    at LidHandler.initialize (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/lidHandler.js:31:20)
    at initResult.maxAttempts (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:427:91)
    at retryWithBackoff (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionRetry.js:43:34)
    at makeEnhancedMongoDBStore (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:427:73) {
  errorLabelSet: Set(0) {}
}
[LID Handler] Retry attempt 1 for index creation after error: Client must be connected before running operations. Waiting 127.08142284991814ms...
<< a few more similar lines of error > ></a>
❌ Failed to drop index lastSeen_1 from collection wabot_lidMappings: MongoNotConnectedError: Client must be connected before running operations
    at autoConnect (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/operations/execute_operation.js:95:19)
    at executeOperation (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/operations/execute_operation.js:38:40)
    at ListIndexesCursor._initialize (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/list_indexes_cursor.js:27:73)
    at ListIndexesCursor.cursorInit (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/abstract_cursor.js:632:38)
    at ListIndexesCursor.fetchBatch (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/abstract_cursor.js:666:24)
    at ListIndexesCursor.next (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/abstract_cursor.js:342:28)
    at [Symbol.asyncIterator] (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/abstract_cursor.js:250:45)
    at AsyncGenerator.next (<anonymous>)
    at ListIndexesCursor.toArray (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/cursor/abstract_cursor.js:422:26)
    at Collection.indexes (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/node_modules/mongodb/lib/collection.js:485:57)
    at safeDropIndex (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/indexHelper.js:9:42)
    at LidHandler.createIndexes (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/lidHandler.js:51:59)
    at LidHandler.initialize (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/lidHandler.js:31:20)
    at reinitResult.maxAttempts (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:505:109)
    at retryWithBackoff (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionRetry.js:43:34)
    at ensureConnection (/home/wabot/api-wabot-v3/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:505:91) {
  errorLabelSet: Set(0) {}
}
<< a few more similar lines of error > >
ℹ️ Index updatedAt_1 does not exist in collection wabot_groupMetadata, skipping drop
✅ Created index on collection wabot_groupMetadata
ℹ️ Index updatedAt_1 does not exist in collection wabot_labels, skipping drop
✅ Created index on collection wabot_labels
[2025-09-02T01:44:11.095Z] [PID:2744150] DB connection healthy
✅ Created index on collection wabot_labelAssociations
✅ Created index on collection wabot_labelAssociations
Store created for 670C4D8DDF1A7: Index TTL=30d, User retention=30d, Media=true
MongoDB store test: Found 0 existing groups for instance 670C4D8DDF1A7
new session created for instance id 670C4D8DDF1A7
Cached permissions for instance 689BE96D77EEF
permissions for module whatsapp_poll_template is defined for 689BE96D77EEF
permissions for module whatsapp_live_chat is defined for 689BE96D77EEF (from cache)
Fetching account data from DB for instance_id: 689BE96D77EEF
Account data cached for 689BE96D77EEF (token: 689BE96D77EEF)
Returning full permissions for 689BE96D77EEF (from cache)
Live chat disabled, user retention: 7 days for instance 689BE96D77EEF
Cached retention preference: 7 days for instance 689BE96D77EEF
Media download disabled for instance 689BE96D77EEF
ℹ️ Index lastSeen_1 does not exist in collection wabot_lidMappings, skipping drop
[LidHandler] All indexes created successfully
ℹ️ Index updatedAt_1 does not exist in collection wabot_groupMetadata, skipping drop
ℹ️ Index updatedAt_1 does not exist in collection wabot_labelAssociations, skipping drop
ℹ️ Index updatedAt_1 does not exist in collection wabot_labels, skipping drop
ℹ️ Index instanceId_1_chatId_1_labelId_1 does not exist in collection wabot_labelAssociations, skipping drop
✅ Created index on collection wabot_groupMetadata
✅ Created index on collection wabot_labels
✅ Dropped index instanceId_1_type_1_chatId_1_labelId_1 from collection wabot_labelAssociations
Checking for stale campaigns
Checking for stale campaigns...
No campaigns in the processing set.
Checking for stuck processing campaigns in database...
No stuck processing campaigns found in database.
Label Queues Health Check: {
  labelManager: { waiting: 0, active: 0, failed: 0 },
  automationLog: { waiting: 0, active: 0, failed: 0 }
}
✅ Created index on collection wabot_labelAssociations
✅ Created index on collection wabot_labelAssociations
Store created for 689BE96D77EEF: Index TTL=30d, User retention=7d, Media=false
MongoDB store test: Found 12 existing groups for instance 689BE96D77EEF
new session created for instance id 689BE96D77EEF
permissions for module whatsapp_poll_template is defined for 670F8D2E76653 (from cache)
permissions for module whatsapp_live_chat is defined for 670F8D2E76653 (from cache)
Fetching account data from DB for instance_id: 670F8D2E76653
Account data cached for 670F8D2E76653 (token: 670F8D2E76653)
Returning full permissions for 670F8D2E76653 (from cache)
Live chat disabled, user retention: 7 days for instance 670F8D2E76653
Cached retention preference: 7 days for instance 670F8D2E76653
Media download disabled for instance 670F8D2E76653
ℹ️ Index lastSeen_1 does not exist in collection wabot_lidMappings, skipping drop
[LidHandler] All indexes created successfully
ℹ️ Index updatedAt_1 does not exist in collection wabot_groupMetadata, skipping drop
ℹ️ Index updatedAt_1 does not exist in collection wabot_labels, skipping drop
ℹ️ Index updatedAt_1 does not exist in collection wabot_labelAssociations, skipping drop
✅ Created index on collection wabot_groupMetadata
✅ Created index on collection wabot_labels
ℹ️ Index instanceId_1_chatId_1_labelId_1 does not exist in collection wabot_labelAssociations, skipping drop
✅ Dropped index instanceId_1_type_1_chatId_1_labelId_1 from collection wabot_labelAssociations
[2025-09-02T01:46:11.095Z] [PID:2744150] DB connection healthy