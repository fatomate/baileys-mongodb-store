Returning full permissions for 6860DCA0E2819 (from cache)
User retention preference: 7 days for instance 6860DCA0E2819
Cached retention preference: 7 days for instance 6860DCA0E2819
Enabling poll template features for instance 6860DCA0E2819
Using shared connection for instance 6860DCA0E2819
[TTL Monitor] Warning: TTL index missing for collection chats
[TTL Monitor] Warning: TTL index missing for collection contacts
[TTL Monitor] Warning: TTL index missing for collection messages
[TTL Monitor] Warning: TTL index missing for collection groupMetadata
[TTL Monitor] Warning: TTL index missing for collection state
[TTL Monitor] Warning: TTL index missing for collection presences
[TTL Monitor] Warning: TTL index missing for collection labels
[TTL Monitor] Warning: TTL index missing for collection labelAssociations
ℹ️ Index lastSeen_1 does not exist in collection wabot_lidMappings, skipping drop
[LidHandler] All indexes created successfully
[LID Handler] Initialized for instance 6860DCA0E2819
🚀 Initializing shared queue manager for instance 6860DCA0E2819...
✅ Registered all shared queue processors for instance 6860DCA0E2819
✅ Shared queue manager initialized successfully for instance 6860DCA0E2819
🔧 Starting sequential index creation for instance 6860DCA0E2819...
IMPORTANT! Eviction policy is allkeys-lru. It should be "noeviction"
IMPORTANT! Eviction policy is allkeys-lru. It should be "noeviction"
IMPORTANT! Eviction policy is allkeys-lru. It should be "noeviction"
IMPORTANT! Eviction policy is allkeys-lru. It should be "noeviction"
IMPORTANT! Eviction policy is allkeys-lru. It should be "noeviction"
IMPORTANT! Eviction policy is allkeys-lru. It should be "noeviction"
IMPORTANT! Eviction policy is allkeys-lru. It should be "noeviction"
IMPORTANT! Eviction policy is allkeys-lru. It should be "noeviction"
IMPORTANT! Eviction policy is allkeys-lru. It should be "noeviction"
IMPORTANT! Eviction policy is allkeys-lru. It should be "noeviction"
IMPORTANT! Eviction policy is allkeys-lru. It should be "noeviction"
IMPORTANT! Eviction policy is allkeys-lru. It should be "noeviction"
IMPORTANT! Eviction policy is allkeys-lru. It should be "noeviction"
IMPORTANT! Eviction policy is allkeys-lru. It should be "noeviction"
IMPORTANT! Eviction policy is allkeys-lru. It should be "noeviction"
IMPORTANT! Eviction policy is allkeys-lru. It should be "noeviction"
🔒 Acquired index lock for wabot_chats by instance 6860DCA0E2819
✅ Created index on collection wabot_chats
✅ Created index on collection wabot_chats
🔓 Released index lock for wabot_chats by instance 6860DCA0E2819
🔒 Acquired index lock for wabot_contacts by instance 6860DCA0E2819
✅ Created index on collection wabot_contacts
✅ Created index on collection wabot_contacts
🔓 Released index lock for wabot_contacts by instance 6860DCA0E2819
🔒 Acquired index lock for wabot_messages by instance 6860DCA0E2819
✅ Created index on collection wabot_messages
✅ Created index on collection wabot_messages
✅ Created index on collection wabot_messages
✅ Created index on collection wabot_messages
✅ Created index on collection wabot_messages
✅ Created index on collection wabot_messages
🔓 Released index lock for wabot_messages by instance 6860DCA0E2819
🔒 Acquired index lock for wabot_groupMetadata by instance 6860DCA0E2819
ℹ️ Index updatedAt_1 does not exist in collection wabot_groupMetadata, skipping drop
✅ Created index on collection wabot_groupMetadata
🔓 Released index lock for wabot_groupMetadata by instance 6860DCA0E2819
🔒 Acquired index lock for wabot_state by instance 6860DCA0E2819
✅ Created index on collection wabot_state
✅ Created index on collection wabot_state
🔓 Released index lock for wabot_state by instance 6860DCA0E2819
🔒 Acquired index lock for wabot_presences by instance 6860DCA0E2819
✅ Created index on collection wabot_presences
✅ Created index on collection wabot_presences
🔓 Released index lock for wabot_presences by instance 6860DCA0E2819
🔒 Acquired index lock for wabot_labels by instance 6860DCA0E2819
ℹ️ Index updatedAt_1 does not exist in collection wabot_labels, skipping drop
✅ Created index on collection wabot_labels
🔓 Released index lock for wabot_labels by instance 6860DCA0E2819
🔒 Acquired index lock for wabot_labelAssociations by instance 6860DCA0E2819
ℹ️ Index updatedAt_1 does not exist in collection wabot_labelAssociations, skipping drop
ℹ️ Index instanceId_1_chatId_1_labelId_1 does not exist in collection wabot_labelAssociations, skipping drop
✅ Dropped index instanceId_1_type_1_chatId_1_labelId_1 from collection wabot_labelAssociations
✅ Created index on collection wabot_labelAssociations
⚠️ Index exists with different options on collection wabot_labelAssociations, dropping and recreating
❌ Failed to recreate index on collection wabot_labelAssociations: MongoServerError: index not found with name [label_message_unique_partial]
    at Connection.sendCommand (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/cmap/connection.js:305:27)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async Connection.command (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/cmap/connection.js:333:26)
    at async Server.command (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/sdam/server.js:171:29)
    at async DropIndexOperation.executeCommand (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/command.js:76:16)
    at async DropIndexOperation.execute (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/indexes.js:141:16)
    at async tryOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:207:20)
    at async executeOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:75:16)
    at async Collection.dropIndex (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/collection.js:373:16)
    at async safeCreateIndex (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/indexHelper.js:91:21)
    at async /home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:2001:13
    at async /home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:621:24
    at async retryWithBackoff (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionRetry.js:45:28)
    at async withConnection (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:616:28)
    at async IndexLockManager.withLock (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/indexLock.js:84:20)
    at async createIndexes (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:1992:9) {
  errorLabelSet: Set(0) {},
  errorResponse: {
    ok: 0,
    errmsg: 'index not found with name [label_message_unique_partial]',
    code: 27,
    codeName: 'IndexNotFound',
    '$clusterTime': {
      clusterTime: new Timestamp({ t: 1756782530, i: 1 }),
      signature: [Object]
    },
    operationTime: new Timestamp({ t: 1756782530, i: 1 })
  },
  ok: 0,
  code: 27,
  codeName: 'IndexNotFound',
  '$clusterTime': {
    clusterTime: new Timestamp({ t: 1756782530, i: 1 }),
    signature: {
      hash: Binary.createFromBase64('vKQoGytQxUAWonHluYOJNjouCSw=', 0),
      keyId: new Long('7486214321475682307')
    }
  },
  operationTime: new Timestamp({ t: 1756782530, i: 1 })
}
[withConnection] Operation failed after 1 attempts: MongoServerError: index not found with name [label_message_unique_partial]
    at Connection.sendCommand (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/cmap/connection.js:305:27)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async Connection.command (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/cmap/connection.js:333:26)
    at async Server.command (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/sdam/server.js:171:29)
    at async DropIndexOperation.executeCommand (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/command.js:76:16)
    at async DropIndexOperation.execute (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/indexes.js:141:16)
    at async tryOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:207:20)
    at async executeOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:75:16)
    at async Collection.dropIndex (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/collection.js:373:16)
    at async safeCreateIndex (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/indexHelper.js:91:21)
    at async /home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:2001:13
    at async /home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:621:24
    at async retryWithBackoff (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionRetry.js:45:28)
    at async withConnection (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:616:28)
    at async IndexLockManager.withLock (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/indexLock.js:84:20)
    at async createIndexes (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:1992:9) {
  errorLabelSet: Set(0) {},
  errorResponse: {
    ok: 0,
    errmsg: 'index not found with name [label_message_unique_partial]',
    code: 27,
    codeName: 'IndexNotFound',
    '$clusterTime': {
      clusterTime: new Timestamp({ t: 1756782530, i: 1 }),
      signature: [Object]
    },
    operationTime: new Timestamp({ t: 1756782530, i: 1 })
  },
  ok: 0,
  code: 27,
  codeName: 'IndexNotFound',
  '$clusterTime': {
    clusterTime: new Timestamp({ t: 1756782530, i: 1 }),
    signature: {
      hash: Binary.createFromBase64('vKQoGytQxUAWonHluYOJNjouCSw=', 0),
      keyId: new Long('7486214321475682307')
    }
  },
  operationTime: new Timestamp({ t: 1756782530, i: 1 })
}
[withConnection] Operation failed: MongoServerError: index not found with name [label_message_unique_partial]
    at Connection.sendCommand (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/cmap/connection.js:305:27)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async Connection.command (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/cmap/connection.js:333:26)
    at async Server.command (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/sdam/server.js:171:29)
    at async DropIndexOperation.executeCommand (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/command.js:76:16)
    at async DropIndexOperation.execute (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/indexes.js:141:16)
    at async tryOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:207:20)
    at async executeOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:75:16)
    at async Collection.dropIndex (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/collection.js:373:16)
    at async safeCreateIndex (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/indexHelper.js:91:21)
    at async /home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:2001:13
    at async /home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:621:24
    at async retryWithBackoff (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionRetry.js:45:28)
    at async withConnection (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:616:28)
    at async IndexLockManager.withLock (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/indexLock.js:84:20)
    at async createIndexes (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:1992:9) {
  errorLabelSet: Set(0) {},
  errorResponse: {
    ok: 0,
    errmsg: 'index not found with name [label_message_unique_partial]',
    code: 27,
    codeName: 'IndexNotFound',
    '$clusterTime': {
      clusterTime: new Timestamp({ t: 1756782530, i: 1 }),
      signature: [Object]
    },
    operationTime: new Timestamp({ t: 1756782530, i: 1 })
  },
  ok: 0,
  code: 27,
  codeName: 'IndexNotFound',
  '$clusterTime': {
    clusterTime: new Timestamp({ t: 1756782530, i: 1 }),
    signature: {
      hash: Binary.createFromBase64('vKQoGytQxUAWonHluYOJNjouCSw=', 0),
      keyId: new Long('7486214321475682307')
    }
  },
  operationTime: new Timestamp({ t: 1756782530, i: 1 })
}
🔓 Released index lock for wabot_labelAssociations by instance 6860DCA0E2819
Failed to create MongoDB store for 6860DCA0E2819: MongoServerError: index not found with name [label_message_unique_partial]
    at Connection.sendCommand (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/cmap/connection.js:305:27)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async Connection.command (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/cmap/connection.js:333:26)
    at async Server.command (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/sdam/server.js:171:29)
    at async DropIndexOperation.executeCommand (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/command.js:76:16)
    at async DropIndexOperation.execute (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/indexes.js:141:16)
    at async tryOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:207:20)
    at async executeOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:75:16)
    at async Collection.dropIndex (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/collection.js:373:16)
    at async safeCreateIndex (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/indexHelper.js:91:21)
    at async /home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:2001:13
    at async /home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:621:24
    at async retryWithBackoff (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionRetry.js:45:28)
    at async withConnection (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:616:28)
    at async IndexLockManager.withLock (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/indexLock.js:84:20)
    at async createIndexes (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:1992:9) {
  errorLabelSet: Set(0) {},
  errorResponse: {
    ok: 0,
    errmsg: 'index not found with name [label_message_unique_partial]',
    code: 27,
    codeName: 'IndexNotFound',
    '$clusterTime': {
      clusterTime: new Timestamp({ t: 1756782530, i: 1 }),
      signature: [Object]
    },
    operationTime: new Timestamp({ t: 1756782530, i: 1 })
  },
  ok: 0,
  code: 27,
  codeName: 'IndexNotFound',
  '$clusterTime': {
    clusterTime: new Timestamp({ t: 1756782530, i: 1 }),
    signature: {
      hash: Binary.createFromBase64('vKQoGytQxUAWonHluYOJNjouCSw=', 0),
      keyId: new Long('7486214321475682307')
    }
  },
  operationTime: new Timestamp({ t: 1756782530, i: 1 })
}
store[instance_id] could not be created for instance  6860DCA0E2819
sessions[instance_id] could not be created for instance  6860DCA0E2819