Initializing MongoDB store for instance 6860DCA0E2819
Cached permissions for instance 6860DCA0E2819
permissions for module whatsapp_poll_template is defined for 6860DCA0E2819
permissions for module whatsapp_crm is defined for 6860DCA0E2819 (from cache)
Permission check for instance  6860DCA0E2819  hasPollTemplate:  true  hasMessageHistory:  true
Returning full permissions for 6860DCA0E2819 (from cache)
User retention preference: 7 days for instance 6860DCA0E2819
Cached retention preference: 7 days for instance 6860DCA0E2819
Enabling poll template features for instance 6860DCA0E2819
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
[LidHandler] All indexes created successfully
[LID Handler] Initialized for instance 6860DCA0E2819
🚀 Initializing shared queue manager for instance 6860DCA0E2819...
✅ Registered all shared queue processors for instance 6860DCA0E2819
✅ Shared queue manager initialized successfully for instance 6860DCA0E2819
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
[LidHandler] All indexes created successfully
Reconnected to MongoDB (shared) for instance 6860DCA0E2819
🔄 Migration completed: Removed obsolete TTL indexes
🔧 Smart index management for enhanced instance 6860DCA0E2819...
   Settings: skipExisting=true, forceRecreate=false
✅ Collection chats: All 2 indexes exist, skipping creation
✅ Collection contacts: All 2 indexes exist, skipping creation
✅ Collection messages: All 6 indexes exist, skipping creation
✅ Collection groupMetadata: All 1 indexes exist, skipping creation
✅ Collection state: All 2 indexes exist, skipping creation
✅ Collection presences: All 2 indexes exist, skipping creation
✅ Collection labels: All 1 indexes exist, skipping creation
🔨 Collection labelAssociations: Creating missing 1 indexes
📋 Creating 1 indexes for collection wabot_labelAssociations
✅ Created index on collection wabot_labelAssociations
Batch index creation completed for wabot_labelAssociations: 1 successful, 0 failed
✅ Smart enhanced index management completed for instance 6860DCA0E2819:
   📊 Total indexes: 17 required
   🔨 Created: 1
   ⏭️  Skipped (existing): 16
   📈 Efficiency: 94% reduction in index operations
📋 Enhanced index creation details:
   labelAssociations: 1 created
🧹 Cleared collection cache after index creation
[TTL Monitor] No new TTL indexes to verify
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
Store bound to socket events for instance 6860DCA0E2819 (initial binding)
sessions[instance_id] could not be created for instance  6860DCA0E2819
new session created for instance id 6860DCA0E2819
Current version: 2.3000.1026692228
Initializing MongoDB store for instance 68B56FF52B090
Cached permissions for instance 68B56FF52B090
permissions for module whatsapp_poll_template is defined for 68B56FF52B090
permissions for module whatsapp_crm is defined for 68B56FF52B090 (from cache)
Permission check for instance  68B56FF52B090  hasPollTemplate:  true  hasMessageHistory:  true
Returning full permissions for 68B56FF52B090 (from cache)
User retention preference: 30 days for instance 68B56FF52B090
Cached retention preference: 30 days for instance 68B56FF52B090
Enabling poll template features for instance 68B56FF52B090
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
[LidHandler] All indexes created successfully
[LID Handler] Initialized for instance 68B56FF52B090
🚀 Initializing shared queue manager for instance 68B56FF52B090...
✅ Registered all shared queue processors for instance 68B56FF52B090
✅ Shared queue manager initialized successfully for instance 68B56FF52B090
[LidHandler] All indexes created successfully
Reconnected to MongoDB (shared) for instance 68B56FF52B090
🔄 Migration completed: Removed obsolete TTL indexes
🔧 Smart index management for enhanced instance 68B56FF52B090...
   Settings: skipExisting=true, forceRecreate=false
✅ Collection chats: All 2 indexes exist, skipping creation
✅ Collection contacts: All 2 indexes exist, skipping creation
✅ Collection messages: All 6 indexes exist, skipping creation
✅ Collection groupMetadata: All 1 indexes exist, skipping creation
✅ Collection state: All 2 indexes exist, skipping creation
✅ Collection presences: All 2 indexes exist, skipping creation
✅ Collection labels: All 1 indexes exist, skipping creation
🔨 Collection labelAssociations: Creating missing 1 indexes
📋 Creating 1 indexes for collection wabot_labelAssociations
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
connection opened for instance 6860DCA0E2819
connection opened for instance 68B56FF52B090
messages.upsert received for instance 68B56FF52B090 with message: {
  "messages": [
    {
      "key": {
        "remoteJid": "60173577321@s.whatsapp.net",
        "fromMe": true,
        "id": "3A196C06197674EEB1DB"
      },
      "messageTimestamp": 1756975853,
      "pushName": "Fames Automate",
      "broadcast": false,
      "status": 2,
      "message": {
        "conversation": "botcheck"
      },
      "verifiedBizName": "Fames Automate"
    }
  ],
  "type": "notify"
}
subscriber not found. getting subscriber avatar for instance 68B56FF52B090 with official_api false
Would save message to MongoDB: 3A196C06197674EEB1DB
Emitted new_message_1 for message: 3A196C06197674EEB1DB (type: text, media: pending)
{"level":50,"time":"2025-09-04T08:50:54.521Z","pid":1627938,"hostname":"wabotv3-sql","key":{"remoteJid":"60196794989@s.whatsapp.net","fromMe":false,"id":"3A196C06197674EEB1DB","senderLid":"4088943141087@lid"},"err":{"type":"SessionError","message":"No session record","stack":"SessionError: No session record\n    at 60196794989.0 [as awaitable] (/home/wabotdev/api-wabot-dev/public_html/node_modules/libsignal/src/session_cipher.js:169:23)\n    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)\n    at async _asyncQueueExecutor (/home/wabotdev/api-wabot-dev/public_html/node_modules/libsignal/src/queue_job.js:20:29)","name":"SessionError"},"messageType":"msg","sender":"60196794989@s.whatsapp.net","author":"60196794989@s.whatsapp.net","isSessionRecordError":true,"msg":"failed to decrypt message"}
messages.upsert received for instance 6860DCA0E2819 with message: {
  "messages": [
    {
      "key": {
        "remoteJid": "60196794989@s.whatsapp.net",
        "fromMe": false,
        "id": "3A196C06197674EEB1DB",
        "senderLid": "4088943141087@lid"
      },
      "messageTimestamp": 1756975853,
      "pushName": "Fames Automate",
      "broadcast": false,
      "messageStubType": 2,
      "messageStubParameters": [
        "No session record"
      ],
      "verifiedBizName": "Fames Automate"
    }
  ],
  "type": "notify"
}
[LID] Pattern 3: FromMe=false, only senderLid (waiting for phone discovery)
[LidHandler] getPhoneNumberFromLid: Database not connected, returning fallback
Chatbot is enabled for instance 6860DCA0E2819
Autoresponder is enabled for instance 6860DCA0E2819
Would save message to MongoDB: 3A196C06197674EEB1DB
Emitted new_message_1 for message: 3A196C06197674EEB1DB (type: text, media: pending)
Contacts update received for instance id 6860DCA0E2819
Contacts Updated: [
  {
    "id": "60196794989@s.whatsapp.net",
    "notify": "Fames Automate",
    "verifiedName": "Fames Automate"
  }
] for instance id 6860DCA0E2819
Subscriber cache cleared: 60196794989@s.whatsapp.net 6860DCA0E2819
Subscriber cache cleared: 60196794989@s.whatsapp.net 6860DCA0E2819
{"level":50,"time":"2025-09-04T08:51:00.888Z","pid":1627938,"hostname":"wabotv3-sql","key":{"remoteJid":"60196794989@s.whatsapp.net","fromMe":false,"id":"3A196C06197674EEB1DB","senderLid":"4088943141087@lid"},"err":{"type":"SessionError","message":"No session record","stack":"SessionError: No session record\n    at 60196794989.0 [as awaitable] (/home/wabotdev/api-wabot-dev/public_html/node_modules/libsignal/src/session_cipher.js:169:23)\n    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)\n    at async _asyncQueueExecutor (/home/wabotdev/api-wabot-dev/public_html/node_modules/libsignal/src/queue_job.js:20:29)","name":"SessionError"},"messageType":"msg","sender":"60196794989@s.whatsapp.net","author":"60196794989@s.whatsapp.net","isSessionRecordError":true,"msg":"failed to decrypt message"}
messages.upsert received for instance 6860DCA0E2819 with message: {
  "messages": [
    {
      "key": {
        "remoteJid": "60196794989@s.whatsapp.net",
        "fromMe": false,
        "id": "3A196C06197674EEB1DB",
        "senderLid": "4088943141087@lid"
      },
      "messageTimestamp": 1756975860,
      "pushName": "Fames Automate",
      "broadcast": false,
      "messageStubType": 2,
      "messageStubParameters": [
        "No session record"
      ]
    }
  ],
  "type": "notify"
}
[LID] Pattern 3: FromMe=false, only senderLid (waiting for phone discovery)
[LidHandler] getPhoneNumberFromLid: Database not connected, returning fallback
messages.upsert received for instance 6860DCA0E2819 with message: {
  "messages": [
    {
      "key": {
        "remoteJid": "60173577321@s.whatsapp.net",
        "fromMe": true,
        "id": "A58CF0DA15458D5CAB638D6E7F57944A"
      },
      "messageTimestamp": 1756975860,
      "broadcast": false,
      "status": 2,
      "message": {
        "protocolMessage": {
          "type": "PEER_DATA_OPERATION_REQUEST_RESPONSE_MESSAGE",
          "peerDataOperationRequestResponseMessage": {
            "peerDataOperationRequestType": "PLACEHOLDER_MESSAGE_RESEND",
            "stanzaId": "3EB04C7DBC9898DEE9C0F9",
            "peerDataOperationResult": [
              {
                "mediaUploadResult": "SUCCESS",
                "placeholderMessageResendResponse": {
                  "webMessageInfoBytes": "CjQKGjYwMTk2Nzk0OTg5QHMud2hhdHNhcHAubmV0EAAaFDNBMTk2QzA2MTk3Njc0RUVCMURCEgoKCGJvdGNoZWNrGO2l5cUGigMg2JXLFY25MszNfMpYBAaNZCVmLfm5tv1FRf07S+okV6zyAxYKFAEKZK2bxox3g2vDxpVDSkCh6hXYiAQA"
                }
              }
            ]
          }
        }
      }
    }
  ],
  "type": "notify"
}
Skipping duplicate message: 3A196C06197674EEB1DB:stub
Contacts update received for instance id 6860DCA0E2819
Contacts Updated: [
  {
    "id": "60196794989@s.whatsapp.net",
    "notify": "Fames Automate"
  }
] for instance id 6860DCA0E2819
Skipping protocolMessage for instance 6860DCA0E2819: 17
messages.upsert received for instance 6860DCA0E2819 with message: {
  "messages": [
    {
      "key": {
        "remoteJid": "60196794989@s.whatsapp.net",
        "fromMe": false,
        "id": "3A196C06197674EEB1DB"
      },
      "message": {
        "conversation": "botcheck"
      },
      "messageTimestamp": "1756975853",
      "messageSecret": "2JXLFY25MszNfMpYBAaNZCVmLfm5tv1FRf07S+okV6w=",
      "reportingTokenInfo": {
        "reportingTag": "AQpkrZvGjHeDa8PGlUNKQKHqFdg="
      },
      "isMentionedInStatus": false
    }
  ],
  "type": "notify",
  "requestId": "3EB04C7DBC9898DEE9C0F9"
}
Skipping duplicate message: 3A196C06197674EEB1DB:stub
messages.upsert received for instance 68B56FF52B090 with message: {
  "messages": [
    {
      "key": {
        "remoteJid": "60173577321@s.whatsapp.net",
        "fromMe": false,
        "id": "3EB046A3AAF286EC5A8110",
        "senderLid": "80758756622573@lid"
      },
      "messageTimestamp": 1756975863,
      "pushName": "Wabot Demo",
      "broadcast": false,
      "message": {
        "extendedTextMessage": {
          "text": "Hai cik! 😊\n\nSaya Fara, agen khidmat pelanggan untuk Wabot dari Team Fames.\n\nAda yang boleh saya bantu hari ini?"
        }
      },
      "verifiedBizName": "Wabot Demo"
    }
  ],
  "type": "notify"
}
[LID] Pattern 3: FromMe=false, only senderLid (waiting for phone discovery)
[LidHandler] getPhoneNumberFromLid: Database not connected, returning fallback
Would save message to MongoDB: 3EB046A3AAF286EC5A8110
Emitted new_message_1 for message: 3EB046A3AAF286EC5A8110 (type: text, media: pending)
Contacts update received for instance id 68B56FF52B090
Contacts Updated: [
  {
    "id": "60173577321@s.whatsapp.net",
    "notify": "Wabot Demo",
    "verifiedName": "Wabot Demo"
  }
] for instance id 68B56FF52B090
messages.upsert received for instance 6860DCA0E2819 with message: {
  "messages": [
    {
      "key": {
        "remoteJid": "60196794989@s.whatsapp.net",
        "fromMe": true,
        "id": "3EB046A3AAF286EC5A8110"
      },
      "message": {
        "extendedTextMessage": {
          "text": "Hai cik! 😊\n\nSaya Fara, agen khidmat pelanggan untuk Wabot dari Team Fames.\n\nAda yang boleh saya bantu hari ini?"
        }
      },
      "messageTimestamp": "1756975862",
      "status": "PENDING"
    }
  ],
  "type": "append"
}
Would save message to MongoDB: 3EB046A3AAF286EC5A8110
messages.upsert received for instance 6860DCA0E2819 with message: {
  "messages": [
    {
      "key": {
        "remoteJid": "60196794989@s.whatsapp.net",
        "fromMe": false,
        "id": "3A196C06197674EEB1DB",
        "senderLid": "4088943141087@lid"
      },
      "messageTimestamp": 1756975866,
      "pushName": "Fames Automate",
      "broadcast": false,
      "message": {
        "conversation": "botcheck",
        "messageContextInfo": {
          "deviceListMetadata": {
            "senderKeyHash": "EONhJnY9BPOdJQ==",
            "senderTimestamp": "1756947672",
            "recipientKeyHash": "57YfN6uMoUfUbQ==",
            "recipientTimestamp": "1756795237"
          },
          "deviceListMetadataVersion": 2,
          "messageSecret": "2JXLFY25MszNfMpYBAaNZCVmLfm5tv1FRf07S+okV6w="
        }
      }
    }
  ],
  "type": "notify"
}
[LID] Pattern 3: FromMe=false, only senderLid (waiting for phone discovery)
[LidHandler] getPhoneNumberFromLid: Database not connected, returning fallback
messages.upsert received for instance 6860DCA0E2819 with message: {
  "messages": [
    {
      "key": {
        "remoteJid": "60173577321@s.whatsapp.net",
        "fromMe": true,
        "id": "A570EC37ACD5201171C1901C26EFD953"
      },
      "messageTimestamp": 1756975866,
      "broadcast": false,
      "status": 2,
      "message": {
        "protocolMessage": {
          "type": "PEER_DATA_OPERATION_REQUEST_RESPONSE_MESSAGE",
          "peerDataOperationRequestResponseMessage": {
            "peerDataOperationRequestType": "PLACEHOLDER_MESSAGE_RESEND",
            "stanzaId": "3EB04734C58DB24FCBC03E",
            "peerDataOperationResult": [
              {
                "mediaUploadResult": "SUCCESS",
                "placeholderMessageResendResponse": {
                  "webMessageInfoBytes": "CjQKGjYwMTk2Nzk0OTg5QHMud2hhdHNhcHAubmV0EAAaFDNBMTk2QzA2MTk3Njc0RUVCMURCEgoKCGJvdGNoZWNrGO2l5cUGigMg2JXLFY25MszNfMpYBAaNZCVmLfm5tv1FRf07S+okV6zyAxYKFAEKZK2bxox3g2vDxpVDSkCh6hXYiAQA"
                }
              }
            ]
          }
        }
      }
    }
  ],
  "type": "notify"
}
Skipping protocolMessage for instance 6860DCA0E2819: 17
chatMessage is botcheck
Would save message to MongoDB: 3A196C06197674EEB1DB
Emitted new_message_1 for message: 3A196C06197674EEB1DB (type: text, media: pending)
Contacts update received for instance id 6860DCA0E2819
Contacts Updated: [
  {
    "id": "60196794989@s.whatsapp.net",
    "notify": "Fames Automate"
  }
] for instance id 6860DCA0E2819
Sending text message to 60196794989@s.whatsapp.net from instance id 6860DCA0E2819
chatbot connection is active for instance 6860DCA0E2819
Updated ai_credit_count for team 1 to 396
next_update for team 1 is greater than currentTime, skipping update
messages.upsert received for instance 68B56FF52B090 with message: {
  "messages": [
    {
      "key": {
        "remoteJid": "60173577321@s.whatsapp.net",
        "fromMe": false,
        "id": "3EB025CD971870828595DB",
        "senderLid": "80758756622573@lid"
      },
      "messageTimestamp": 1756975867,
      "pushName": "Wabot Demo",
      "broadcast": false,
      "message": {
        "extendedTextMessage": {
          "text": "_chatbot connection is active_"
        }
      },
      "verifiedBizName": "Wabot Demo"
    }
  ],
  "type": "notify"
}
[LID] Pattern 3: FromMe=false, only senderLid (waiting for phone discovery)
[LidHandler] getPhoneNumberFromLid: Database not connected, returning fallback
Chatbot is disabled for instance 68B56FF52B090
Autoresponder is disabled for instance 68B56FF52B090
handleResponse will not be called for 60173577321@s.whatsapp.net because chatbot or autoresponder are disabled for instance 68B56FF52B090
Would save message to MongoDB: 3EB025CD971870828595DB
Emitted new_message_1 for message: 3EB025CD971870828595DB (type: text, media: pending)
Contacts update received for instance id 68B56FF52B090
Contacts Updated: [
  {
    "id": "60173577321@s.whatsapp.net",
    "notify": "Wabot Demo",
    "verifiedName": "Wabot Demo"
  }
] for instance id 68B56FF52B090
messages.upsert received for instance 6860DCA0E2819 with message: {
  "messages": [
    {
      "key": {
        "remoteJid": "60196794989@s.whatsapp.net",
        "fromMe": true,
        "id": "3EB025CD971870828595DB"
      },
      "message": {
        "extendedTextMessage": {
          "text": "_chatbot connection is active_"
        }
      },
      "messageTimestamp": "1756975866",
      "status": "PENDING"
    },
    {
      "key": {
        "remoteJid": "60196794989@s.whatsapp.net",
        "fromMe": false,
        "id": "3A196C06197674EEB1DB"
      },
      "message": {
        "conversation": "botcheck"
      },
      "messageTimestamp": "1756975853",
      "messageSecret": "2JXLFY25MszNfMpYBAaNZCVmLfm5tv1FRf07S+okV6w=",
      "reportingTokenInfo": {
        "reportingTag": "AQpkrZvGjHeDa8PGlUNKQKHqFdg="
      },
      "isMentionedInStatus": false
    }
  ],
  "type": "append"
}
Would save message to MongoDB: 3EB025CD971870828595DB
Emitted new_message_1 for message: 3EB025CD971870828595DB (type: text, media: pending)