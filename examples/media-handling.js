const { makeEnhancedMongoDBStore } = require('@fatomate/baileys-mongodb-store')
const makeWASocket = require('baileys').default
const { useMultiFileAuthState } = require('baileys')
const path = require('path')

/**
 * Example: Media Download and Management
 * 
 * This example demonstrates how to:
 * 1. Configure automatic media download
 * 2. Manage media storage
 * 3. Clean up old media files
 * 4. Access downloaded media URLs
 */

async function connectWithMediaHandling() {
    // Configure the enhanced store with media support
    const store = await makeEnhancedMongoDBStore({
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_media_example',
        instanceId: 'media_demo',
        
        // Media configuration
        media: {
            enabled: true,
            baseDir: '/var/whatsapp/media', // Base directory for media storage
            maxSizeInMB: 50,                // Max file size 50MB
            allowedTypes: ['image', 'video', 'audio', 'document'], // Skip stickers
            skipGroupMessages: false,        // Download group media too
            maxRetries: 3,
            retryDelay: 2000,
            downloadTimeout: 60000,          // Base timeout
            timeoutPerMB: 3000,              // Size-aware timeout multiplier (ms per MB)
            iosHeicSupport: true,            // Enable HEIC/HEIF support (default true)
            enableMediaDebug: false,         // Enable verbose media logs
            officialAPI: {                   // Optional Official API tuning
                maxUrlCacheMs: 60000,
                step1TimeoutMs: 20000,
                step2TimeoutMs: 120000
            }
        },
        
        // Optional: Set different TTL for messages with media
        collectionTTL: {
            messages: 60  // Keep messages for 60 days
        },
        
        // Enable metrics to track media downloads
        enableMetrics: true
    })
    
    // Setup authentication
    const { state, saveCreds } = await useMultiFileAuthState('auth_info_baileys')
    
    // Create socket
    const sock = makeWASocket({
        auth: state,
        printQRInTerminal: true
    })
    
    // Bind the store to socket events
    store.bind(sock.ev)
    
    // Handle connection updates
    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update
        
        if (connection === 'close') {
            const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== 401
            console.log('Connection closed, reconnecting:', shouldReconnect)
            
            if (shouldReconnect) {
                connectWithMediaHandling()
            }
        } else if (connection === 'open') {
            console.log('✅ Connected to WhatsApp')
            
            // Show media statistics after connection
            await showMediaStats(store)
        }
    })
    
    // Save credentials
    sock.ev.on('creds.update', saveCreds)
    
    // Monitor media downloads
    sock.ev.on('messages.upsert', async ({ messages }) => {
        for (const msg of messages) {
            if (msg.message?.imageMessage || 
                msg.message?.videoMessage || 
                msg.message?.audioMessage || 
                msg.message?.documentMessage) {
                
                console.log(`📥 Media message received from ${msg.key.remoteJid}`)
                
                // Media is automatically downloaded by the store
                // Wait a bit for download to complete, then check the URL
                setTimeout(async () => {
                    const result = await store.downloadMessageMedia(
                        msg.key.remoteJid,
                        msg.key.id
                    )
                    
                    if (result.success) {
                        console.log(`✅ Media saved to: ${result.localPath}`)
                    } else {
                        console.log(`❌ Media download failed: ${result.error}`)
                    }
                }, 5000)
            }
        }
    })
    
    // Setup periodic cleanup (every 24 hours)
    setInterval(async () => {
        console.log('🧹 Running media cleanup...')
        const result = await store.cleanupOldMedia(30) // Keep media for 30 days
        console.log(`Cleanup complete: ${result.deleted} files deleted, ${result.errors} errors`)
    }, 24 * 60 * 60 * 1000)
    
    return { sock, store }
}

/**
 * Display media statistics
 */
async function showMediaStats(store) {
    const stats = await store.getMediaStats()
    
    console.log('\n📊 Media Storage Statistics:')
    console.log(`Total files: ${stats.totalFiles}`)
    console.log(`Total size: ${formatBytes(stats.totalSize)}`)
    
    if (stats.byType) {
        console.log('\nBy type:')
        for (const [type, data] of Object.entries(stats.byType)) {
            if (data.count > 0) {
                console.log(`  ${type}: ${data.count} files (${formatBytes(data.size)})`)
            }
        }
    }
}

/**
 * Format bytes to human readable format
 */
function formatBytes(bytes) {
    if (bytes === 0) return '0 Bytes'
    
    const k = 1024
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

/**
 * Example: Download specific message media
 */
async function downloadSpecificMedia(store, jid, messageId) {
    console.log(`\n📥 Downloading media for message ${messageId}...`)
    
    const result = await store.downloadMessageMedia(jid, messageId)
    
    if (result.success) {
        console.log(`✅ Media downloaded to: ${result.localPath}`)
        
        // You can now serve this file through your API
        // Example: app.get('/media/:path', serveMediaFile)
        return result.localPath
    } else {
        console.log(`❌ Download failed: ${result.error}`)
        return null
    }
}

/**
 * Example: Serve media files through Express API
 */
function setupMediaAPI(app, store, mediaBaseDir) {
    const express = require('express')
    const fs = require('fs')
    
    // Endpoint to get media by message ID
    app.get('/api/media/:instanceId/:jid/:messageId', async (req, res) => {
        const { instanceId, jid, messageId } = req.params
        
        // Download if not already downloaded
        const result = await store.downloadMessageMedia(jid, messageId)
        
        if (!result.success) {
            return res.status(404).json({ error: result.error })
        }
        
        // Serve the file
        const filePath = path.join(mediaBaseDir, result.localPath)
        
        if (!fs.existsSync(filePath)) {
            return res.status(404).json({ error: 'File not found' })
        }
        
        res.sendFile(filePath)
    })
    
    // Endpoint to get media statistics
    app.get('/api/media/stats/:instanceId', async (req, res) => {
        const stats = await store.getMediaStats()
        res.json(stats)
    })
    
    // Endpoint to cleanup old media
    app.post('/api/media/cleanup/:instanceId', async (req, res) => {
        const daysToKeep = req.body.daysToKeep || 30
        const result = await store.cleanupOldMedia(daysToKeep)
        res.json(result)
    })
}

/**
 * Example: Media management with filtering
 */
async function setupSelectiveMediaDownload() {
    const store = await makeEnhancedMongoDBStore({
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_selective_media',
        instanceId: 'selective_demo',
        
        media: {
            enabled: true,
            baseDir: '/var/whatsapp/media',
            
            // Only download small images and documents
            maxSizeInMB: 10,
            allowedTypes: ['image', 'document'],
            
            // Skip group media to save space
            skipGroupMessages: true,
            
            maxRetries: 2,
            downloadTimeout: 30000
        },
        
        // Use hooks to add custom logic
        hooks: {
            afterStore: async (eventType, data) => {
                if (eventType === 'messages.upsert') {
                    // Custom logic after message stored
                    console.log('Message stored, checking for media...')
                }
            }
        }
    })
    
    return store
}

// Run the example
if (require.main === module) {
    connectWithMediaHandling().catch(console.error)
}

module.exports = {
    connectWithMediaHandling,
    showMediaStats,
    downloadSpecificMedia,
    setupMediaAPI,
    setupSelectiveMediaDownload
}