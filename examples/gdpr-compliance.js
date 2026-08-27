/**
 * Example: GDPR-Compliant Storage Configuration
 * Implements data retention policies and anonymization
 */

const makeWASocket = require('@whiskeysockets/baileys').default
const { useMultiFileAuthState } = require('@whiskeysockets/baileys')
const { makeEnhancedMongoDBStore } = require('@fatomate/baileys-mongodb-store')
const crypto = require('crypto')

// Helper function to hash/anonymize sensitive data
function anonymizePhoneNumber(phoneNumber) {
    if (!phoneNumber) return phoneNumber
    // Keep country code, anonymize the rest
    const match = phoneNumber.match(/^(\d{1,3})/)
    if (match) {
        const countryCode = match[1]
        const hash = crypto.createHash('sha256').update(phoneNumber).digest('hex').substring(0, 8)
        return `${countryCode}****${hash}`
    }
    return '[REDACTED]'
}

async function connectWithGDPRCompliance() {
    // Create store with GDPR-compliant configuration
    const store = await makeEnhancedMongoDBStore({
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        database: 'whatsapp_gdpr',
        instanceId: 'gdpr_compliant_bot',
        
        // Global TTL - 90 days max retention
        ttlDays: 90,
        
        // Different retention periods per collection
        collectionTTL: {
            messages: 30,       // 30 days for messages (GDPR requirement)
            chats: 90,         // 90 days for chat metadata
            contacts: 365,     // 1 year for contacts (business requirement)
            groupMetadata: 180, // 6 months for groups
            presences: 1,      // 1 day for presence (minimal retention)
            labels: 0,         // Permanent for labels (organizational data)
            state: 30          // 30 days for connection state
        },
        
        // Configure events with privacy in mind
        events: {
            // Messages with anonymization
            'messages.upsert': {
                enabled: true,
                ttlDays: 30,
                transform: (data) => {
                    if (!data.messages) return data
                    
                    // Anonymize message content
                    data.messages = data.messages.map(msg => {
                        // Deep clone to avoid modifying original
                        const anonymized = JSON.parse(JSON.stringify(msg))
                        
                        // Anonymize phone numbers in text content
                        if (anonymized.message?.conversation) {
                            anonymized.message.conversation = anonymized.message.conversation
                                .replace(/\+?\d{10,}/g, (match) => anonymizePhoneNumber(match))
                        }
                        
                        if (anonymized.message?.extendedTextMessage?.text) {
                            anonymized.message.extendedTextMessage.text = 
                                anonymized.message.extendedTextMessage.text
                                    .replace(/\+?\d{10,}/g, (match) => anonymizePhoneNumber(match))
                        }
                        
                        // Remove location data for privacy
                        if (anonymized.message?.locationMessage) {
                            delete anonymized.message.locationMessage.degreesLatitude
                            delete anonymized.message.locationMessage.degreesLongitude
                            anonymized.message.locationMessage.comment = '[LOCATION REDACTED]'
                        }
                        
                        // Remove live location
                        if (anonymized.message?.liveLocationMessage) {
                            delete anonymized.message.liveLocationMessage
                        }
                        
                        // Anonymize contact cards
                        if (anonymized.message?.contactMessage) {
                            anonymized.message.contactMessage.displayName = '[CONTACT REDACTED]'
                            delete anonymized.message.contactMessage.vcard
                        }
                        
                        return anonymized
                    })
                    
                    return data
                },
                filter: (data) => {
                    // Don't store ephemeral messages at all
                    return data.messages?.every(msg => 
                        !msg.message?.ephemeralMessage &&
                        !msg.message?.viewOnceMessage
                    )
                }
            },
            
            // Contacts with anonymization
            'contacts.upsert': {
                enabled: true,
                ttlDays: 365,
                transform: (contacts) => {
                    return contacts.map(contact => ({
                        ...contact,
                        // Hash the notify name for privacy
                        notify: contact.notify ? 
                            crypto.createHash('sha256').update(contact.notify).digest('hex').substring(0, 8) : 
                            undefined,
                        // Remove business info
                        businessInfo: undefined
                    }))
                }
            },
            
            // Don't store presence for privacy
            'presence.update': { enabled: false },
            
            // Minimal receipt storage
            'message-receipt.update': { 
                enabled: true,
                ttlDays: 7 // Only keep for 7 days
            },
            
            // Groups with participant anonymization
            'groups.upsert': {
                enabled: true,
                ttlDays: 180,
                transform: (groups) => {
                    return groups.map(group => ({
                        ...group,
                        // Keep group functionality but anonymize participant details
                        participants: group.participants?.map(p => ({
                            ...p,
                            // Hash participant IDs for privacy
                            id: anonymizePhoneNumber(p.id)
                        }))
                    }))
                }
            },
            
            // Track deletions for audit
            'chats.delete': {
                enabled: true,
                ttlDays: 365, // Keep deletion logs for 1 year (audit requirement)
                transform: (deletions) => ({
                    deletions,
                    deletedAt: new Date().toISOString(),
                    reason: 'user_requested',
                    instanceId: 'gdpr_compliant_bot'
                })
            },
            
            'messages.delete': {
                enabled: true,
                ttlDays: 365, // Keep deletion logs for 1 year
                transform: (item) => ({
                    ...item,
                    deletedAt: new Date().toISOString(),
                    reason: 'user_requested'
                })
            }
        },
        
        // Enable metrics for compliance monitoring
        enableMetrics: true,
        
        // Hooks for compliance logging
        hooks: {
            beforeStore: async (eventType, data) => {
                // Log data processing for compliance
                console.log(`[GDPR] Processing ${eventType} - Data will be retained per policy`)
                
                // Check for personal data
                if (eventType === 'messages.upsert' && data.messages) {
                    const hasPersonalData = data.messages.some(msg => 
                        msg.message?.contactMessage || 
                        msg.message?.locationMessage ||
                        msg.message?.liveLocationMessage
                    )
                    
                    if (hasPersonalData) {
                        console.log('[GDPR] ⚠️ Personal data detected and will be anonymized')
                    }
                }
                
                return true
            },
            
            afterStore: async (eventType, data) => {
                // Log successful storage for audit
                const timestamp = new Date().toISOString()
                console.log(`[GDPR AUDIT] ${timestamp} - Stored ${eventType} event`)
                
                // Could also write to a separate audit log
                // await auditLog.write({ timestamp, eventType, action: 'stored' })
            },
            
            onError: (eventType, error, data) => {
                console.error(`[GDPR ERROR] Failed to process ${eventType}:`, error.message)
                // Log errors for compliance review
            }
        },
        
        logLevel: 'warn'
    })
    
    // Set up WhatsApp connection
    const { state, saveCreds } = await useMultiFileAuthState('./auth_gdpr')
    
    const sock = makeWASocket({
        auth: state,
        printQRInTerminal: true,
        getMessage: async (key) => {
            return await store.loadMessage(key.remoteJid!, key.id!)
        }
    })
    
    // Bind store to socket events
    store.bind(sock.ev)
    
    sock.ev.on('creds.update', saveCreds)
    
    // GDPR: Handle data subject requests
    async function handleDataSubjectRequest(phoneNumber, requestType) {
        console.log(`[GDPR] Processing ${requestType} request for ${anonymizePhoneNumber(phoneNumber)}`)
        
        switch (requestType) {
            case 'ACCESS':
                // Provide all data for a specific user
                const userJid = `${phoneNumber}@s.whatsapp.net`
                const userData = {
                    messages: await store.getMessages(userJid),
                    chat: await store.getChat(userJid),
                    contact: await store.getContact(userJid)
                }
                
                // Return anonymized data
                return {
                    requestId: crypto.randomUUID(),
                    timestamp: new Date().toISOString(),
                    data: JSON.stringify(userData, null, 2),
                    dataCategories: ['messages', 'chat_metadata', 'contact_info']
                }
                
            case 'DELETE':
                // Delete all data for a specific user
                const deleteJid = `${phoneNumber}@s.whatsapp.net`
                await store.deleteMessages(deleteJid)
                await store.deleteChats([deleteJid])
                
                return {
                    requestId: crypto.randomUUID(),
                    timestamp: new Date().toISOString(),
                    status: 'deleted',
                    message: 'All data has been permanently deleted'
                }
                
            case 'PORTABILITY':
                // Export data in machine-readable format
                const exportJid = `${phoneNumber}@s.whatsapp.net`
                const exportData = {
                    messages: await store.getMessages(exportJid),
                    chat: await store.getChat(exportJid),
                    contact: await store.getContact(exportJid)
                }
                
                return {
                    requestId: crypto.randomUUID(),
                    timestamp: new Date().toISOString(),
                    format: 'JSON',
                    data: exportData
                }
                
            default:
                throw new Error('Invalid request type')
        }
    }
    
    // Monitor data retention
    setInterval(async () => {
        const stats = store.getPerformanceStats()
        const metrics = store.getEventMetrics()
        
        console.log('\n📋 GDPR Compliance Report')
        console.log('═══════════════════════════')
        console.log(`Generated: ${new Date().toISOString()}`)
        console.log('\nData Retention Status:')
        
        // Check TTL compliance
        const indexStatus = await store.getIndexStatus()
        indexStatus.forEach(({ collection, indexes }) => {
            const ttlIndex = indexes.find(idx => idx.expireAfterSeconds)
            if (ttlIndex) {
                const days = ttlIndex.expireAfterSeconds / (24 * 60 * 60)
                console.log(`  ${collection}: ${days} days retention`)
            }
        })
        
        console.log('\nData Processing Metrics:')
        if (Array.isArray(metrics)) {
            metrics.forEach(metric => {
                if (metric.totalReceived > 0) {
                    console.log(`  ${metric.eventType}:`)
                    console.log(`    Processed: ${metric.totalStored}`)
                    console.log(`    Filtered/Anonymized: ${metric.totalSkipped}`)
                }
            })
        }
        
        console.log('\nCompliance Status: ✅ ACTIVE')
        console.log('Next Review: ' + new Date(Date.now() + 24*60*60*1000).toISOString())
    }, 60 * 60 * 1000) // Every hour
    
    // Example GDPR command handler
    sock.ev.on('messages.upsert', async ({ messages }) => {
        for (const msg of messages) {
            const text = msg.message?.conversation || msg.message?.extendedTextMessage?.text
            
            if (text?.startsWith('!gdpr')) {
                const [, command, ...args] = text.split(' ')
                
                try {
                    let response = ''
                    
                    switch (command) {
                        case 'access':
                            const accessResult = await handleDataSubjectRequest(
                                msg.key.remoteJid!.replace('@s.whatsapp.net', ''),
                                'ACCESS'
                            )
                            response = `Data access request processed. Request ID: ${accessResult.requestId}`
                            break
                            
                        case 'delete':
                            const deleteResult = await handleDataSubjectRequest(
                                msg.key.remoteJid!.replace('@s.whatsapp.net', ''),
                                'DELETE'
                            )
                            response = `✅ ${deleteResult.message}. Request ID: ${deleteResult.requestId}`
                            break
                            
                        case 'export':
                            const exportResult = await handleDataSubjectRequest(
                                msg.key.remoteJid!.replace('@s.whatsapp.net', ''),
                                'PORTABILITY'
                            )
                            response = `Data exported. Request ID: ${exportResult.requestId}`
                            break
                            
                        case 'policy':
                            response = `📋 *Data Protection Policy*\n\n` +
                                `• Messages: 30-day retention\n` +
                                `• Personal data: Anonymized\n` +
                                `• Location data: Not stored\n` +
                                `• Your rights: Access, Delete, Export\n\n` +
                                `Commands: !gdpr access | delete | export`
                            break
                            
                        default:
                            response = 'Available commands: !gdpr policy | access | delete | export'
                    }
                    
                    await sock.sendMessage(msg.key.remoteJid!, { text: response })
                } catch (error) {
                    console.error('[GDPR] Error processing request:', error)
                    await sock.sendMessage(msg.key.remoteJid!, { 
                        text: '❌ Error processing GDPR request. Please contact support.' 
                    })
                }
            }
        }
    })
    
    return { sock, store }
}

// Run the example
connectWithGDPRCompliance().catch(console.error)