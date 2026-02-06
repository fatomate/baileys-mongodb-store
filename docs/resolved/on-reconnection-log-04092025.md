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
ℹ️ Index lastSeen_1 does not exist in collection wabot_lidMappings, skipping drop
[LidHandler] All indexes created successfully
[LID Handler] Initialized for instance 68B56FF52B090
🚀 Initializing shared queue manager for instance 68B56FF52B090...
✅ Registered all shared queue processors for instance 68B56FF52B090
✅ Shared queue manager initialized successfully for instance 68B56FF52B090
[TTL Monitor] Warning: TTL index missing for collection chats
ℹ️ Index lastSeen_1 does not exist in collection wabot_lidMappings, skipping drop
[TTL Monitor] Warning: TTL index missing for collection contacts
[LidHandler] All indexes created successfully
Reconnected to MongoDB (shared) for instance 68B56FF52B090
ℹ️ Index updatedAt_1 does not exist in collection wabot_labelAssociations, skipping drop
ℹ️ Index updatedAt_1 does not exist in collection wabot_groupMetadata, skipping drop
ℹ️ Index updatedAt_1 does not exist in collection wabot_labels, skipping drop
🔄 Migration completed: Removed obsolete TTL indexes
🔧 Smart index management for enhanced instance 68B56FF52B090...
   Settings: skipExisting=true, forceRecreate=false
✅ Collection chats: All 2 indexes exist, skipping creation
✅ Collection contacts: All 2 indexes exist, skipping creation
[TTL Monitor] Warning: TTL index missing for collection messages
✅ Collection messages: All 6 indexes exist, skipping creation
✅ Collection groupMetadata: All 1 indexes exist, skipping creation
✅ Collection state: All 2 indexes exist, skipping creation
[TTL Monitor] Warning: TTL index missing for collection groupMetadata
✅ Collection presences: All 2 indexes exist, skipping creation
✅ Collection labels: All 1 indexes exist, skipping creation
🔨 Collection labelAssociations: Creating missing 1 indexes
📋 Creating 1 indexes for collection wabot_labelAssociations
[TTL Monitor] Warning: TTL index missing for collection state
✅ Created index on collection wabot_labelAssociations
Batch index creation completed for wabot_labelAssociations: 1 successful, 0 failed
✅ Smart enhanced index management completed for instance 68B56FF52B090:
   📊 Total indexes: 17 required
   🔨 Created: 1
   ⏭️  Skipped (existing): 16
   📈 Efficiency: 94% reduction in index operations
📋 Enhanced index creation details:
   labelAssociations: 1 created
🧹 Cleared collection cache after index creation
[TTL Monitor] No new TTL indexes to verify
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
[TTL Monitor] Warning: TTL index missing for collection presences
[TTL Monitor] Warning: TTL index missing for collection labels
[TTL Monitor] Warning: TTL index missing for collection labelAssociations
[2025-09-04T01:01:12.214Z] Memory (app): RSS=238.45 MB, Heap=122.35 MB/157.91 MB
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
Socket recreated and store rebound for 68B56FF52B090
Re-attached event handler for 68B56FF52B090
Event handlers re-attached for 68B56FF52B090 (1 handlers)