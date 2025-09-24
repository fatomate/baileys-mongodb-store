// My Application's code

handleMessagesUpsert: async function(instance_id, messages, isTestSimulation = false) {
    const WA = sessions[instance_id];
    if (!WA) {
      console.error(`No active session for instance ${instance_id}`);
      return false;
    }

    // Log if test simulation
    if (isTestSimulation) {
      console.log("=== TEST SIMULATION MODE - Processing messages ===");
    }

    // Get user phone
    let userPhone = WA.user ? Common.get_phone(WA.user.id) : null;
    
    // Trigger webhook
    const messagesUpsertEvent = { event: "messages.upsert", data: messages };
    await this.webhook(instance_id, messagesUpsertEvent);
    
    // Check for excluded numbers
    const excluded_wabot_numbers = ['60196195886', '60127953307', '60125153307'];
    const bypassedMessage = messages.find(message => 
      excluded_wabot_numbers.some(excludedNumber => message.key.remoteJid.includes(excludedNumber))
    );
    
    if (bypassedMessage && !isTestSimulation) {
      console.log("Bypassed message from excluded number:", bypassedMessage.key.remoteJid);
      return;
    }
    
    // Process each message
    for (const message of messages) {
      try {
        // Skip protocolMessage types (REVOKE, E2E_DEVICE_CHANGED, E2E_IDENTITY_CHANGED, etc.)
        if (message.message?.protocolMessage) {
          console.log(`Skipping protocolMessage for instance ${instance_id}:`, message.message.protocolMessage.type || 'unknown');
          continue;
        }
        
        // Handle @lid format - normalize the remoteJid if needed
        const originalRemoteJid = message.key.remoteJid;
        const normalizedRemoteJid = await this.normalizeChatId(instance_id, originalRemoteJid, message);
        
        // If we got a phone number from @lid, update the message for downstream processing
        if (originalRemoteJid !== normalizedRemoteJid && this.isLidFormat(originalRemoteJid)) {
          console.log(`Normalized @lid ${originalRemoteJid} to ${normalizedRemoteJid}`);
          // Store original for reference
          message.key.originalRemoteJid = originalRemoteJid;
          // Update remoteJid to use the phone number
          message.key.remoteJid = normalizedRemoteJid;
          
          // Trigger migration if we just discovered the phone number
          if (message?.key?.senderPn) {
            await this.migrateFromLidToPhone(instance_id, originalRemoteJid, normalizedRemoteJid);
          }
        }
        
        // Skip duplicate check for test simulations
        if (!isTestSimulation) {
          // Create unique identifier combining message ID and stub status
          const uniqueId = `${message.key.id}:${message.messageStubParameters ? 'stub' : 'message'}`;
          const messageKey = `message_ids:${instance_id}`;
          const messageExists = await redisClient.hget(messageKey, uniqueId);
          
          if (messageExists) {
            console.log("Skipping duplicate message:", uniqueId);
            continue;
          }
          
          // Store unique identifier in Redis
          await redisClient.hset(messageKey, uniqueId, '1');
          await redisClient.expire(messageKey, 24 * 60 * 60); // 24 hours
        }
        
        // Process the message
        await this.processMessage(message, instance_id, userPhone);
        
        // Save to MongoDB if not a group message and not test simulation
        const remoteJid = message.key.remoteJid;
        const isGroupMessage = remoteJid.includes("@g.us");
        
        if (!isGroupMessage && !isTestSimulation) {
          try {
            const sessionData = await Common.db_get('sp_whatsapp_sessions', [
              { instance_id: instance_id },
              { app: appVersion }
            ]);
            
            const teamId = sessionData ? sessionData.team_id : null;
            if (teamId) {
              // Handle message saving to MongoDB
              console.log(`Would save message to MongoDB: ${message.key.id}`);

              // Determine message type based on message content
              let messageType = 'text';
              if (message.message?.audioMessage) {
                messageType = 'audio';
              } else if (message.message?.imageMessage) {
                messageType = 'image';
              } else if (message.message?.videoMessage) {
                messageType = 'video';
              } else if (message.message?.documentMessage) {
                messageType = 'document';
              } else if (message.message?.stickerMessage) {
                messageType = 'sticker';
              } else if (message.message?.locationMessage) {
                messageType = 'location';
              } else if (message.message?.contactMessage) {
                messageType = 'contact';
              }
              
              // Extract text content from different message types
              let messageText = '';
              if (message.message?.conversation) {
                messageText = message.message.conversation;
              } else if (message.message?.extendedTextMessage?.text) {
                messageText = message.message.extendedTextMessage.text;
              } else if (message.message?.imageMessage?.caption) {
                messageText = message.message.imageMessage.caption;
              } else if (message.message?.videoMessage?.caption) {
                messageText = message.message.videoMessage.caption;
              } else if (message.message?.documentMessage?.caption) {
                messageText = message.message.documentMessage.caption;
              }
              
              // Check if media is available in the store for media messages
              let mediaUrl = null;
              if (messageType !== 'text' && store[instance_id]) {
                try {
                  // Optional: wait a short time to allow store to queue/download media
                  await new Promise(r => setTimeout(r, 1500));
                  const mediaResult = await store[instance_id].downloadMessageMedia(remoteJid, message.key.id);
                  if (mediaResult.success && mediaResult.localPath) {
                    mediaUrl = mediaResult.localPath;
                    console.log(`Media found in store for message ${message.key.id}: ${mediaUrl}`);
                  } else {
                    console.log(`Media not yet available for message ${message.key.id}, background queue will handle it`);
                    // Prefer fewer retries; store will emit/update when ready if your app subscribes
                    const checkMediaAvailability = async (attempts = 0, maxAttempts = 3) => {
                      if (attempts >= maxAttempts) {
                        console.log(`Max retry attempts reached for message ${message.key.id}`);
                        return;
                      }
                      const delay = Math.min(3000 * Math.pow(2, attempts), 30000);
                      setTimeout(async () => {
                        try {
                          const retryResult = await store[instance_id].downloadMessageMedia(remoteJid, message.key.id);
                          if (retryResult.success && retryResult.localPath) {
                            const updateData = {
                              instance_id: instance_id,
                              message: {
                                message_id: message.key.id,
                                chat_id: remoteJid,
                                media_url: retryResult.localPath,
                                type: messageType
                              }
                            };
                            WAZIPER.io.emit(`message_update_${teamId}`, updateData);
                            console.log(`Emitted message_update_${teamId} with media URL for message: ${message.key.id} (attempt ${attempts + 1})`);
                          } else {
                            console.log(`Media still not ready for message ${message.key.id}, retrying... (attempt ${attempts + 1}/${maxAttempts})`);
                            checkMediaAvailability(attempts + 1, maxAttempts);
                          }
                        } catch (err) {
                          console.error(`Error checking media availability for message ${message.key.id} (attempt ${attempts + 1}):`, err);
                          checkMediaAvailability(attempts + 1, maxAttempts);
                        }
                      }, delay);
                    };
                    checkMediaAvailability();
                  }
                } catch (err) {
                  console.error(`Error getting media from store for message ${message.key.id}:`, err);
                }
              }
              
              // Emit socket event to whatsapp_live_chat module
              const socketData = {
                instance_id: instance_id,
                message: {
                  message_id: message.key.id,
                  chat_id: remoteJid,
                  text: messageText,
                  from_me: message.key.fromMe || false,
                  timestamp: message.messageTimestamp * 1000,
                  type: messageType,
                  push_name: message.pushName || '',
                  media_url: mediaUrl, // Include media URL if available
                  // raw_message: message
                }
              };
              
              // Emit to whatsapp_live_chat module
              WAZIPER.io.emit(`new_message_${teamId}`, socketData);
              console.log(`Emitted new_message_${teamId} for message: ${message.key.id} (type: ${messageType}, media: ${mediaUrl ? 'yes' : 'pending'})`);
            }
          } catch (mongoError) {
            console.error('Error saving message to MongoDB:', mongoError);
          }
        }
      } catch (error) {
        console.error(`Error processing message ${message.key?.id}:`, error);
      }
    }
    
    return true;
  },

  Update store configuration: {
    "uri": "mongodb+srv://firdaus:5NxqHPj4CHG3Y12F@wabot-dev-free.jsrperj.mongodb.net/?retryWrites=true&w=1&readPreference=secondaryPreferred&serverSelectionTimeoutMS=15000&connectTimeoutMS=10000&socketTimeoutMS=45000&appName=wabot-dev-free",
    "database": "wabotdev",
    "instanceId": "6719BC2A93532",
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
        "enabled": true
      },
      "chats.upsert": {
        "enabled": true
      },
      "chats.update": {
        "enabled": true
      },
      "contacts.upsert": {
        "enabled": true
      },
      "contacts.update": {
        "enabled": true
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
      "enabled": true,
      "baseDir": "/home/wabotdev/api-wabot-dev/public_html/media",
      "maxSizeInMB": 1,
      "allowedTypes": [
        "image",
        "video",
        "audio",
        "document"
      ],
      "skipGroupMessages": true,
      "maxRetries": 3,
      "retryDelay": 2000,
      "downloadTimeout": 60000
    },
    "profilePictureConfig": {
      "enabled": true,
      "refreshIntervalDays": 7,
      "requestDelay": 500,
      "maxConcurrent": 1,
      "retryAttempts": 3,
      "logPrivacyErrors": false
    },
    "ttlMonitoring": {
      "intervalMs": 3600000,
      "alertThresholdPercent": 10
    },
    "clearAllOnHistorySync": false,
    "lidHandler": {
      "cacheTTL": 3600,
      "enableCache": true
    }
  }