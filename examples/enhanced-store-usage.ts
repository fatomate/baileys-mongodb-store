/**
 * Example usage of the Enhanced MongoDB Store with selective event storage and custom TTL
 */

import { makeEnhancedMongoDBStore, EnhancedMongoDBStoreConfig } from '@baileys/mongodb-store'
import makeWASocket, { DisconnectReason, useMultiFileAuthState } from 'baileys'

async function main() {
    // Example 1: Basic configuration with selective event storage
    const basicConfig: EnhancedMongoDBStoreConfig = {
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_store',
        instanceId: 'instance_001',
        
        // Global TTL of 30 days
        ttlDays: 30,
        
        // Only store specific events
        storeAllByDefault: false,
        events: {
            'messages.upsert': { enabled: true },
            'messages.update': { enabled: true },
            'chats.upsert': { enabled: true },
            'contacts.upsert': { enabled: true },
            'connection.update': { enabled: true }
        }
    }

    // Example 2: Advanced configuration with per-event TTL and filtering
    const advancedConfig: EnhancedMongoDBStoreConfig = {
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_store',
        instanceId: 'instance_002',
        
        // Global TTL
        ttlDays: 30,
        
        // Different TTL for different collections
        collectionTTL: {
            messages: 7,        // Keep messages for 7 days
            chats: 90,         // Keep chats for 90 days
            contacts: 365,     // Keep contacts for 1 year
            presences: 1,      // Keep presence for 1 day
            state: 30,         // Keep state for 30 days
            groupMetadata: 180 // Keep group metadata for 6 months
        },
        
        // Store all events by default but with custom configurations
        storeAllByDefault: true,
        
        events: {
            // Messages with filtering - only store important messages
            'messages.upsert': {
                enabled: true,
                ttlDays: 7, // Override collection TTL
                filter: (data) => {
                    // Only store messages that are not ephemeral
                    const messages = data.messages || []
                    return messages.some((msg: any) => 
                        !msg.message?.ephemeralMessage && 
                        !msg.key?.fromMe
                    )
                },
                transform: (data) => {
                    // Remove unnecessary fields to save space
                    const transformed = { ...data }
                    if (transformed.messages) {
                        transformed.messages = transformed.messages.map((msg: any) => {
                            const { mediaKey, ...rest } = msg
                            return rest
                        })
                    }
                    return transformed
                },
                useBatch: true // Use batch processing for better performance
            },
            
            // Don't store presence updates
            'presence.update': {
                enabled: false
            },
            
            // Store group updates with longer TTL
            'groups.update': {
                enabled: true,
                ttlDays: 180 // 6 months for group updates
            },
            
            // Store contact updates but filter out certain contacts
            'contacts.upsert': {
                enabled: true,
                filter: (contacts) => {
                    // Filter out broadcast lists and status updates
                    return contacts.filter((contact: any) => 
                        !contact.id?.includes('@broadcast') &&
                        !contact.id?.includes('status@broadcast')
                    ).length > 0
                }
            },
            
            // Store chat updates with custom TTL
            'chats.upsert': {
                enabled: true,
                ttlDays: 90
            },
            
            // Store labels with permanent storage (no TTL)
            'labels.edit': {
                enabled: true,
                ttlDays: 0 // 0 means no expiration
            },
            
            // Store message receipts for analytics
            'message-receipt.update': {
                enabled: true,
                ttlDays: 3 // Keep receipts for 3 days
            }
        },
        
        // Enable metrics to track event processing
        enableMetrics: true,
        
        // Add hooks for monitoring
        hooks: {
            beforeStore: async (eventType, data) => {
                console.log(`Processing ${eventType} event`)
                
                // Add custom validation or processing
                if (eventType === 'messages.upsert') {
                    // Check message size, content, etc.
                    return true // Return false to skip storing
                }
                
                return true
            },
            
            afterStore: async (eventType, data) => {
                console.log(`Successfully stored ${eventType} event`)
                
                // Send to analytics, webhook, etc.
                if (eventType === 'messages.upsert') {
                    // Track message statistics
                }
            },
            
            onError: (eventType, error, data) => {
                console.error(`Error storing ${eventType}:`, error)
                // Send to error tracking service
            }
        },
        
        // Redis configuration for queue processing (optional)
        redis: {
            connection: 'redis://localhost:6379',
            queuePrefix: 'whatsapp_queue',
            enableLabelQueue: true,
            enableMessageQueue: true,
            concurrency: 100
        },
        
        logLevel: 'warn' // Only log warnings and errors
    }

    // Example 3: Minimal storage configuration (only essential data)
    const minimalConfig: EnhancedMongoDBStoreConfig = {
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_minimal',
        instanceId: 'instance_003',
        
        // Short TTL for all data
        ttlDays: 3,
        
        // Only store absolutely necessary events
        storeAllByDefault: false,
        events: {
            'connection.update': { enabled: true, ttlDays: 1 },
            'messages.upsert': { 
                enabled: true, 
                ttlDays: 3,
                // Only store text messages
                filter: (data) => {
                    return data.messages?.some((msg: any) => 
                        msg.message?.conversation || 
                        msg.message?.extendedTextMessage
                    )
                }
            },
            'chats.upsert': { enabled: true, ttlDays: 7 }
        }
    }

    // Initialize the store with chosen configuration
    const store = await makeEnhancedMongoDBStore(advancedConfig)
    
    // Use with Baileys
    const { state, saveCreds } = await useMultiFileAuthState('./auth_info')
    
    const sock = makeWASocket({
        auth: state,
        printQRInTerminal: true
    })
    
    // Bind store to socket events
    store.bind(sock.ev)
    
    // Runtime configuration updates
    setTimeout(() => {
        // Disable storing presence updates after 5 minutes
        store.updateEventConfig('presence.update', { enabled: false })
        
        // Change message TTL after 10 minutes
        store.updateEventConfig('messages.upsert', { ttlDays: 14 })
        
        // Add custom filter for chats
        store.updateEventConfig('chats.update', {
            filter: (updates) => {
                // Only store updates for non-muted chats
                return updates.some((update: any) => !update.mute)
            }
        })
    }, 5 * 60 * 1000)
    
    // Monitor metrics
    setInterval(() => {
        const metrics = store.getEventMetrics()
        console.log('Event Metrics:', metrics)
        
        const stats = store.getPerformanceStats()
        console.log('Performance Stats:', stats)
    }, 60 * 1000) // Every minute
    
    // Handle connection updates
    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update
        
        if (connection === 'close') {
            const shouldReconnect = (lastDisconnect?.error as any)?.output?.statusCode !== DisconnectReason.loggedOut
            
            if (!shouldReconnect) {
                // Clean up before exit
                await store.close()
            }
        }
    })
}

// Example 4: Dynamic event configuration based on conditions
async function dynamicConfiguration() {
    const config: EnhancedMongoDBStoreConfig = {
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_dynamic',
        instanceId: 'instance_004',
        ttlDays: 30,
        storeAllByDefault: true,
        enableMetrics: true
    }
    
    const store = await makeEnhancedMongoDBStore(config)
    
    // Adjust storage based on time of day
    const adjustStorageByTime = () => {
        const hour = new Date().getHours()
        
        if (hour >= 9 && hour <= 17) {
            // Business hours - store everything
            store.updateEventConfig('messages.upsert', { 
                enabled: true, 
                ttlDays: 30 
            })
            store.updateEventConfig('presence.update', { 
                enabled: true 
            })
        } else {
            // After hours - minimal storage
            store.updateEventConfig('messages.upsert', { 
                enabled: true, 
                ttlDays: 7,
                filter: (data) => {
                    // Only store messages from important contacts
                    return data.messages?.some((msg: any) => 
                        msg.key?.participant?.includes('important')
                    )
                }
            })
            store.updateEventConfig('presence.update', { 
                enabled: false 
            })
        }
    }
    
    // Run every hour
    setInterval(adjustStorageByTime, 60 * 60 * 1000)
    adjustStorageByTime() // Run immediately
    
    return store
}

// Example 5: Storage with data retention policies
async function dataRetentionPolicies() {
    const config: EnhancedMongoDBStoreConfig = {
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_retention',
        instanceId: 'instance_005',
        
        // Base TTL
        ttlDays: 90,
        
        // Compliance-based retention
        collectionTTL: {
            messages: 30,      // GDPR compliance - 30 days
            contacts: 365,     // Keep contacts longer
            chats: 90,         // Standard retention
            groupMetadata: 180,// 6 months for groups
            presences: 1,      // Minimal presence storage
            labels: 0,         // Permanent labels
            labelAssociations: 0 // Permanent associations
        },
        
        events: {
            // Personal data handling
            'messages.upsert': {
                enabled: true,
                ttlDays: 30,
                transform: (data) => {
                    // Anonymize or redact sensitive information
                    const transformed = { ...data }
                    if (transformed.messages) {
                        transformed.messages = transformed.messages.map((msg: any) => {
                            // Remove phone numbers from message content
                            if (msg.message?.conversation) {
                                msg.message.conversation = msg.message.conversation
                                    .replace(/\d{10,}/g, '[REDACTED]')
                            }
                            return msg
                        })
                    }
                    return transformed
                }
            },
            
            // Compliance logging
            'chats.delete': {
                enabled: true,
                ttlDays: 365, // Keep deletion logs for audit
                transform: (data) => ({
                    ...data,
                    deletedAt: new Date(),
                    deletedBy: 'system'
                })
            }
        },
        
        hooks: {
            afterStore: async (eventType, data) => {
                // Log for compliance audit
                if (eventType === 'messages.delete' || eventType === 'chats.delete') {
                    console.log(`Data deletion logged for compliance: ${eventType}`)
                }
            }
        }
    }
    
    return await makeEnhancedMongoDBStore(config)
}

// Run examples
main().catch(console.error)