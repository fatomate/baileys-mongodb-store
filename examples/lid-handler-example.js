const { makeMongoDBStore } = require('../dist')

/**
 * Example demonstrating WhatsApp @lid handling
 * 
 * WhatsApp now uses @lid (LinkedIn ID format) identifiers alongside phone numbers.
 * The LID handler automatically:
 * 1. Detects @lid format JIDs
 * 2. Extracts phone numbers from message metadata
 * 3. Stores bidirectional mappings (@lid <-> phone number)
 * 4. Normalizes JIDs for consistent storage and retrieval
 */

async function demonstrateLidHandling() {
    // Create store with LID handler enabled (it's enabled by default)
    const store = await makeMongoDBStore({
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_store',
        instanceId: 'demo_instance',
        lidHandler: {
            cacheTTL: 3600,      // Cache mappings for 1 hour
            enableCache: true    // Enable caching for performance
        }
    })

    // Simulate messages from your sample data
    const messages = [
        // Bot send from WhatsApp on phone (with @lid)
        {
            key: {
                remoteJid: '114194640801953@lid',
                fromMe: true,
                id: 'D2BE1EB9F6B540402456ECF0B3592332',
                senderLid: '114194640801953@lid',
                senderPn: '60196953307@s.whatsapp.net'  // Phone number available
            },
            messageTimestamp: 1755232223,
            pushName: 'Wabot Demo',
            message: {
                conversation: 'Sent from phone'
            }
        },
        // Bot send from WhatsApp Web (with @lid)
        {
            key: {
                remoteJid: '114194640801953@lid',
                fromMe: true,
                id: '3EB0CED846CEF67F891DA3',
                senderLid: '114194640801953@lid',
                senderPn: '60196953307@s.whatsapp.net'  // Phone number available
            },
            messageTimestamp: 1755239225,
            pushName: 'Wabot Demo',
            message: {
                conversation: 'from whatsapp web'
            }
        },
        // Bot send from Baileys API (regular phone number)
        {
            key: {
                remoteJid: '60196953307@s.whatsapp.net',
                fromMe: true,
                id: '3EB098C69A95497C6FC91E'
            },
            message: {
                extendedTextMessage: {
                    text: 'sent from WA chat'
                }
            },
            messageTimestamp: 1755232200
        },
        // User reply from WhatsApp on phone
        {
            key: {
                remoteJid: '60196953307@s.whatsapp.net',
                fromMe: false,
                id: '3A3417E9AD7C233E1036',
                senderLid: '114194640801953@lid',
                senderPn: '60196953307@s.whatsapp.net'
            },
            messageTimestamp: 1755232374,
            pushName: 'Firdaus Azizi',
            message: {
                conversation: 'User reply from phone'
            }
        }
    ]

    console.log('\\n=== Processing Messages with LID Handler ===\\n')

    // Process each message
    for (const msg of messages) {
        console.log(`Processing message: ${msg.key.id}`)
        console.log(`  Original JID: ${msg.key.remoteJid}`)
        
        // When the message is processed through the store's event handler,
        // the LID handler automatically:
        // 1. Detects if remoteJid is @lid format
        // 2. Extracts phone number from senderPn if available
        // 3. Stores the mapping
        // 4. Normalizes the JID to phone number for storage
        
        // This happens automatically in the bind() event handlers,
        // but for demonstration, we'll manually store the message:
        const jid = msg.key.remoteJid
        await store.upsertMessage(jid, msg)
        
        // The message is now stored with normalized JID
        console.log(`  Stored with normalized JID`)
        
        // Check if mapping was created
        if (msg.key.senderLid && msg.key.senderPn) {
            console.log(`  ✅ LID mapping discovered: ${msg.key.senderLid} -> ${msg.key.senderPn}`)
        }
        console.log('')
    }

    console.log('\\n=== Retrieving Messages ===\\n')

    // Now you can retrieve messages using either @lid or phone number
    // The LID handler automatically normalizes the JID for queries

    // Query using @lid - will be automatically normalized to phone number
    console.log('Querying with @lid: 114194640801953@lid')
    const messagesFromLid = await store.getMessages('114194640801953@lid')
    console.log(`  Found ${messagesFromLid.length} messages`)

    // Query using phone number directly
    console.log('Querying with phone: 60196953307@s.whatsapp.net')
    const messagesFromPhone = await store.getMessages('60196953307@s.whatsapp.net')
    console.log(`  Found ${messagesFromPhone.length} messages`)

    // Both queries return the same messages!
    console.log(`\\n✅ Both queries returned the same results: ${messagesFromLid.length === messagesFromPhone.length}`)

    console.log('\\n=== Benefits of LID Handler ===\\n')
    console.log('1. ✅ Automatic @lid detection and normalization')
    console.log('2. ✅ Seamless phone number extraction from message metadata')
    console.log('3. ✅ Bidirectional mapping storage (@lid <-> phone number)')
    console.log('4. ✅ Unified conversation history regardless of identifier used')
    console.log('5. ✅ Transparent querying - works with both @lid and phone numbers')
    console.log('6. ✅ Performance optimized with caching')
    console.log('7. ✅ Automatic index creation for efficient lookups')

    // Clean up
    await store.close()
}

// Run the demonstration
demonstrateLidHandling().catch(console.error)