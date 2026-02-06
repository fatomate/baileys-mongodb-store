final storeConfig for instance  6860DCA0E2819  is  {
  "uri": "mongodb+srv://firdaus:5NxqHPj4CHG3Y12F@wabot-dev-free.jsrperj.mongodb.net/?retryWrites=true&w=majority&appName=wabot-dev-free",
  "database": "wabotdev",
  "instanceId": "6860DCA0E2819",
  "ttlDays": 30,
  "logLevel": "all",
  "collectionPrefix": "wabot_",
  "redis": {
    "connection": "redis://:qlqWhZLGyyhWBNFC8WzwKw8e@10.0.0.1:7279",
    "queuePrefix": "wabotdev_store",
    "useSharedQueues": true,
    "queueConcurrency": {
      "highPriority": 400,
      "dataSync": 200,
      "media": 30,
      "lowPriority": 10
    }
  },
  "useSharedConnections": true,
  "connectionConfig": {
    "maxPoolSize": 100,
    "minPoolSize": 10
  },
  "enableMetrics": true,
  "storeAllByDefault": false,
  "events": {
    "groups.update": {
      "enabled": true
    },
    "groups.upsert": {
      "enabled": true
    },
    "groups.create": {
      "enabled": true
    },
    "messages.upsert": {
      "enabled": true
    },
    "messages.update": {
      "enabled": false
    },
    "chats.upsert": {
      "enabled": false
    },
    "chats.update": {
      "enabled": false
    },
    "contacts.upsert": {
      "enabled": true
    },
    "contacts.update": {
      "enabled": false
    },
    "connection.update": {
      "enabled": false
    },
    "presence.update": {
      "enabled": false
    },
    "labels.edit": {
      "enabled": true
    },
    "labels.association": {
      "enabled": true
    },
    "messaging-history.set": {
      "enabled": true
    }
  },
  "memory": {
    "maxHeapUsedPercent": 80,
    "checkIntervalMs": 30000
  },
  "media": {
    "enabled": false
  },
  "profilePictureConfig": {
    "enabled": false
  },
  "ttlMonitoring": {
    "intervalMs": 3600000,
    "alertThresholdPercent": 10
  },
  "clearAllOnHistorySync": true,
  "lidHandler": {
    "cacheTTL": 3600,
    "enableCache": true
  }
}
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
ℹ️ Index instanceId_1_type_1_chatId_1_labelId_1 does not exist in collection wabot_labelAssociations, skipping drop
✅ Created index on collection wabot_labelAssociations
✅ Created index on collection wabot_labelAssociations
🔓 Released index lock for wabot_labelAssociations by instance 6860DCA0E2819
✅ All indexes created successfully with custom TTL settings for instance 6860DCA0E2819
[TTL Monitor] Verifying TTL indexes...
[TTL Monitor] All TTL indexes verified successfully
✅ Verified labelAssociations partial unique indexes are present
Store created for 6860DCA0E2819: Index TTL=30d, User retention=7d, Media=false
✅ SharedQueueManager is active and handling queues
[6860DCA0E2819] store.bind() called - setting up event listeners with selective storage
[6860DCA0E2819] Socket reference updated in store
[6860DCA0E2819] Auto-rebinding to new socket's event emitter for safety
[6860DCA0E2819] Unbinding event listeners from current emitter
[6860DCA0E2819] Removed listener for: connection.update
[6860DCA0E2819] Removed listener for: messages.upsert
[6860DCA0E2819] Successfully unbound all event listeners
[6860DCA0E2819] store.bind() called - setting up event listeners with selective storage

// reconnected existing instance...

final storeConfig for instance  68B56FF52B090  is  {
  "uri": "mongodb+srv://firdaus:5NxqHPj4CHG3Y12F@wabot-dev-free.jsrperj.mongodb.net/?retryWrites=true&w=majority&appName=wabot-dev-free",
  "database": "wabotdev",
  "instanceId": "68B56FF52B090",
  "ttlDays": 30,
  "logLevel": "all",
  "collectionPrefix": "wabot_",
  "redis": {
    "connection": "redis://:qlqWhZLGyyhWBNFC8WzwKw8e@10.0.0.1:7279",
    "queuePrefix": "wabotdev_store",
    "useSharedQueues": true,
    "queueConcurrency": {
      "highPriority": 400,
      "dataSync": 200,
      "media": 30,
      "lowPriority": 10
    }
  },
  "useSharedConnections": true,
  "connectionConfig": {
    "maxPoolSize": 100,
    "minPoolSize": 10
  },
  "enableMetrics": true,
  "storeAllByDefault": false,
  "events": {
    "groups.update": {
      "enabled": true
    },
    "groups.upsert": {
      "enabled": true
    },
    "groups.create": {
      "enabled": true
    },
    "messages.upsert": {
      "enabled": true
    },
    "messages.update": {
      "enabled": false
    },
    "chats.upsert": {
      "enabled": false
    },
    "chats.update": {
      "enabled": false
    },
    "contacts.upsert": {
      "enabled": true
    },
    "contacts.update": {
      "enabled": false
    },
    "connection.update": {
      "enabled": false
    },
    "presence.update": {
      "enabled": false
    },
    "labels.edit": {
      "enabled": true
    },
    "labels.association": {
      "enabled": true
    },
    "messaging-history.set": {
      "enabled": true
    }
  },
  "memory": {
    "maxHeapUsedPercent": 80,
    "checkIntervalMs": 30000
  },
  "media": {
    "enabled": false
  },
  "profilePictureConfig": {
    "enabled": false
  },
  "ttlMonitoring": {
    "intervalMs": 3600000,
    "alertThresholdPercent": 10
  },
  "clearAllOnHistorySync": true,
  "lidHandler": {
    "cacheTTL": 3600,
    "enableCache": true
  }
}
Using shared connection for instance 68B56FF52B090
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
[LID Handler] Initialized for instance 68B56FF52B090
🚀 Initializing shared queue manager for instance 68B56FF52B090...
✅ Registered all shared queue processors for instance 68B56FF52B090
✅ Shared queue manager initialized successfully for instance 68B56FF52B090
🔧 Starting sequential index creation for instance 68B56FF52B090...
🔒 Acquired index lock for wabot_chats by instance 68B56FF52B090
✅ Created index on collection wabot_chats
✅ Created index on collection wabot_chats
🔓 Released index lock for wabot_chats by instance 68B56FF52B090
🔒 Acquired index lock for wabot_contacts by instance 68B56FF52B090
✅ Created index on collection wabot_contacts
✅ Created index on collection wabot_contacts
🔓 Released index lock for wabot_contacts by instance 68B56FF52B090
🔒 Acquired index lock for wabot_messages by instance 68B56FF52B090
✅ Created index on collection wabot_messages
✅ Created index on collection wabot_messages
✅ Created index on collection wabot_messages
✅ Created index on collection wabot_messages
✅ Created index on collection wabot_messages
✅ Created index on collection wabot_messages
🔓 Released index lock for wabot_messages by instance 68B56FF52B090
🔒 Acquired index lock for wabot_groupMetadata by instance 68B56FF52B090
ℹ️ Index updatedAt_1 does not exist in collection wabot_groupMetadata, skipping drop
✅ Created index on collection wabot_groupMetadata
🔓 Released index lock for wabot_groupMetadata by instance 68B56FF52B090
🔒 Acquired index lock for wabot_state by instance 68B56FF52B090
✅ Created index on collection wabot_state
✅ Created index on collection wabot_state
🔓 Released index lock for wabot_state by instance 68B56FF52B090
🔒 Acquired index lock for wabot_presences by instance 68B56FF52B090
✅ Created index on collection wabot_presences
✅ Created index on collection wabot_presences
🔓 Released index lock for wabot_presences by instance 68B56FF52B090
🔒 Acquired index lock for wabot_labels by instance 68B56FF52B090
ℹ️ Index updatedAt_1 does not exist in collection wabot_labels, skipping drop
✅ Created index on collection wabot_labels
🔓 Released index lock for wabot_labels by instance 68B56FF52B090
🔒 Acquired index lock for wabot_labelAssociations by instance 68B56FF52B090
ℹ️ Index updatedAt_1 does not exist in collection wabot_labelAssociations, skipping drop
ℹ️ Index instanceId_1_chatId_1_labelId_1 does not exist in collection wabot_labelAssociations, skipping drop
ℹ️ Index instanceId_1_type_1_chatId_1_labelId_1 does not exist in collection wabot_labelAssociations, skipping drop
✅ Created index on collection wabot_labelAssociations
✅ Created index on collection wabot_labelAssociations
🔓 Released index lock for wabot_labelAssociations by instance 68B56FF52B090
✅ All indexes created successfully with custom TTL settings for instance 68B56FF52B090
[TTL Monitor] Verifying TTL indexes...
[TTL Monitor] All TTL indexes verified successfully
✅ Verified labelAssociations partial unique indexes are present
Store created for 68B56FF52B090: Index TTL=30d, User retention=30d, Media=false
✅ SharedQueueManager is active and handling queues
[68B56FF52B090] store.bind() called - setting up event listeners with selective storage
[68B56FF52B090] Socket reference updated in store
[68B56FF52B090] Auto-rebinding to new socket's event emitter for safety
[68B56FF52B090] Unbinding event listeners from current emitter
[68B56FF52B090] Removed listener for: connection.update
[68B56FF52B090] Removed listener for: messages.upsert
[68B56FF52B090] Successfully unbound all event listeners
[68B56FF52B090] store.bind() called - setting up event listeners with selective storage
Store bound to socket events for instance 68B56FF52B090 (initial binding)
sessions[instance_id] could not be created for instance  68B56FF52B090
new session created for instance id 68B56FF52B090
[2025-09-03T14:10:08.909Z] [PID:1284742] DB connection healthy
Login Successful for instance 68B56FF52B090 recreating socket
Recreating socket for 68B56FF52B090 (store persists)
[68B56FF52B090] Socket reference updated in store
[68B56FF52B090] Auto-rebinding to new socket's event emitter for safety
[68B56FF52B090] Unbinding event listeners from current emitter
[68B56FF52B090] Removed listener for: connection.update
[68B56FF52B090] Removed listener for: messages.upsert
[68B56FF52B090] Successfully unbound all event listeners
[68B56FF52B090] store.bind() called - setting up event listeners with selective storage
[68B56FF52B090] Unbinding event listeners from current emitter
[68B56FF52B090] Removed listener for: connection.update
[68B56FF52B090] Removed listener for: messages.upsert
[68B56FF52B090] Successfully unbound all event listeners
[68B56FF52B090] store.bind() called - setting up event listeners with selective storage