/**
 * Example: Proper GroupMetadata Handling with MongoDB Store
 * 
 * This example demonstrates how to properly capture and store group metadata
 * using the @baileys/mongodb-store package.
 */

const makeWASocket = require('baileys').default
const { useMultiFileAuthState } = require('baileys')
const { makeMongoDBStore } = require('@baileys/mongodb-store')

async function connectToWhatsApp() {
    // MongoDB store configuration
    const store = await makeMongoDBStore({
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_db',
        instanceId: 'my-instance-001',
        ttlDays: 30,
        logLevel: 'all' // Set to 'all' to see group metadata logs
    })

    // Create WhatsApp socket with store
    const { state, saveCreds } = await useMultiFileAuthState('auth_info')
    
    const sock = makeWASocket({
        auth: state,
        printQRInTerminal: true
    })

    // IMPORTANT: Bind the store to socket events
    // This enables automatic group metadata capture
    store.bind(sock.ev)

    // Handle connection updates
    sock.ev.on('connection.update', async (update) => {
        const { connection } = update
        
        if (connection === 'open') {
            console.log('✅ Connected to WhatsApp')
            
            // Fetch all group metadata on connection open
            console.log('📋 Fetching all group metadata...')
            const groups = await sock.groupFetchAllParticipating()
            
            // Store each group's metadata
            for (const [groupId, metadata] of Object.entries(groups)) {
                // The store.bind() will automatically handle this through events,
                // but you can also manually save if needed:
                await store.upsertGroupMetadata(groupId, metadata)
                console.log(`✅ Saved group: ${metadata.subject} (${groupId})`)
            }
            
            console.log(`📊 Total groups saved: ${Object.keys(groups).length}`)
        }
    })

    // Monitor group events (these are automatically handled by store.bind())
    
    // New groups or group info updates
    sock.ev.on('groups.upsert', async (groups) => {
        console.log('🆕 New/Updated groups:', groups.length)
        // Already handled by store.bind(), but you can add custom logic here
        for (const group of groups) {
            console.log(`  - ${group.subject} (${group.id})`)
        }
    })

    // Group metadata updates (name, description, etc.)
    sock.ev.on('groups.update', async (updates) => {
        console.log('📝 Group updates:', updates.length)
        // Already handled by store.bind(), but you can add custom logic here
        for (const update of updates) {
            console.log(`  - Group ${update.id} updated`)
        }
    })

    // Participant updates (add, remove, promote, demote)
    sock.ev.on('group-participants.update', async ({ id, participants, action }) => {
        console.log(`👥 Group ${id} participants ${action}:`, participants)
        // Already handled by store.bind(), but you can add custom logic here
    })

    // Save credentials
    sock.ev.on('creds.update', saveCreds)

    return { sock, store }
}

// Example: Querying stored group metadata
async function queryGroupMetadata(store) {
    console.log('\n📖 Querying stored group metadata...')
    
    // Get specific group metadata
    const groupId = '120363xxxxxx@g.us' // Replace with actual group ID
    const metadata = await store.getGroupMetadata(groupId)
    
    if (metadata) {
        console.log(`Group: ${metadata.subject}`)
        console.log(`Participants: ${metadata.participants.length}`)
        console.log(`Created: ${new Date(metadata.creation * 1000).toLocaleString()}`)
        console.log(`Owner: ${metadata.owner}`)
    }
}

// Example: Manual group metadata operations
async function manualGroupOperations(sock, store) {
    // Manually fetch and store a specific group's metadata
    const groupId = '120363xxxxxx@g.us' // Replace with actual group ID
    
    try {
        const metadata = await sock.groupMetadata(groupId)
        await store.upsertGroupMetadata(groupId, metadata)
        console.log(`✅ Manually saved group: ${metadata.subject}`)
    } catch (error) {
        console.error('Error fetching group metadata:', error)
    }
}

// Main execution
async function main() {
    try {
        const { sock, store } = await connectToWhatsApp()
        
        // Wait for connection to establish
        await new Promise(resolve => setTimeout(resolve, 5000))
        
        // Query stored metadata
        await queryGroupMetadata(store)
        
        // Example of manual operations (optional)
        // await manualGroupOperations(sock, store)
        
    } catch (error) {
        console.error('Error:', error)
    }
}

// Run the example
main()

/**
 * IMPORTANT NOTES:
 * 
 * 1. The store.bind(sock.ev) call is CRUCIAL - it automatically captures:
 *    - groups.upsert events (new groups or full metadata)
 *    - groups.update events (group info changes)
 *    - group-participants.update events (member changes)
 * 
 * 2. Group metadata is automatically stored when:
 *    - You join a new group
 *    - Group information changes (name, description, etc.)
 *    - Participants are added/removed/promoted/demoted
 * 
 * 3. Initial sync:
 *    - Call sock.groupFetchAllParticipating() after connection opens
 *    - This fetches ALL groups you're part of
 * 
 * 4. The MongoDB collections created:
 *    - baileys_groupMetadata: Stores all group metadata
 *    - Indexed by instanceId and group id for fast queries
 * 
 * 5. TTL (Time To Live):
 *    - Group metadata expires after ttlDays (default: 30)
 *    - Adjust based on your needs
 */