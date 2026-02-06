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
[LidHandler] Skipping index creation (managed by smart index manager)
[LID Handler] Initialized for instance 68B56FF52B090
🚀 Initializing shared queue manager for instance 68B56FF52B090...
✅ Registered all shared queue processors for instance 68B56FF52B090
✅ Shared queue manager initialized successfully for instance 68B56FF52B090
[LidHandler] Skipping index creation (managed by smart index manager)
Reconnected to MongoDB (shared) for instance 68B56FF52B090
🔄 Migration completed: Removed obsolete TTL indexes
🔧 Smart index management for enhanced instance 68B56FF52B090...
   Settings: skipExisting=true, forceRecreate=false
✅ Collection chats: All 2 indexes exist, skipping creation
✅ Collection contacts: All 2 indexes exist, skipping creation
✅ Collection messages: All 7 indexes exist, skipping creation
✅ Collection groupMetadata: All 1 indexes exist, skipping creation
✅ Collection state: All 2 indexes exist, skipping creation
✅ Collection presences: All 2 indexes exist, skipping creation
✅ Collection labels: All 1 indexes exist, skipping creation
🔨 Collection labelAssociations: Creating missing 1 indexes
📋 Creating 1 indexes for collection wabot_labelAssociations
✅ Created index on collection wabot_labelAssociations
Batch index creation completed for wabot_labelAssociations: 1 successful, 0 failed
❌ Failed to process indexes for collection lidMappings: TypeError: Cannot read properties of undefined (reading 'collectionName')
    at shouldCreateIndexes (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/collectionHelper.js:126:80)
    at createIndexes (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:1968:90)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async makeEnhancedMongoDBStore (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:2081:5)
    at async Object.makeWASocket (/home/wabotdev/api-wabot-dev/public_html/waziper/waziper.js:1307:30)
    at async Object.session (/home/wabotdev/api-wabot-dev/public_html/waziper/waziper.js:3452:31)
    at async Object.instance (/home/wabotdev/api-wabot-dev/public_html/waziper/waziper.js:3579:29)
    at async Queue.<anonymous> (/home/wabotdev/api-wabot-dev/public_html/waziper/extend.js:3163:15)
Failed to create MongoDB store for 68B56FF52B090: TypeError: Cannot read properties of undefined (reading 'collectionName')
    at shouldCreateIndexes (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/collectionHelper.js:126:80)
    at createIndexes (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:1968:90)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async makeEnhancedMongoDBStore (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:2081:5)
    at async Object.makeWASocket (/home/wabotdev/api-wabot-dev/public_html/waziper/waziper.js:1307:30)
    at async Object.session (/home/wabotdev/api-wabot-dev/public_html/waziper/waziper.js:3452:31)
    at async Object.instance (/home/wabotdev/api-wabot-dev/public_html/waziper/waziper.js:3579:29)
    at async Queue.<anonymous> (/home/wabotdev/api-wabot-dev/public_html/waziper/extend.js:3163:15)