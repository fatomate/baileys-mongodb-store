const { makeMongoDBStore } = require('../dist')

/**
 * Debug script to check if LID handler is working
 */
async function debugLidHandler() {
    console.log('🔍 Debugging LID Handler Integration\n')
    
    try {
        // Create store with explicit LID handler config and logging
        const store = await makeMongoDBStore({
            uri: 'mongodb://localhost:27017', // Update with your MongoDB URI
            database: 'your_database_name',   // Update with your database name
            instanceId: '6860DCA0E2819',      // Your instance ID
            logLevel: 'all',                  // Enable all logging
            lidHandler: {
                cacheTTL: 3600,
                enableCache: true
            }
        })
        
        console.log('✅ Store created successfully')
        console.log('✅ LID handler should be initialized (check console for "[LID Handler] Initialized" message)')
        
        // Test the LID handler directly
        console.log('\n🧪 Testing LID handler directly...')
        
        // Create a test message that should trigger LID mapping
        const testMessage = {
            key: {
                remoteJid: "114194640801953@lid",
                fromMe: true,
                id: "TEST_" + Date.now(),
                senderLid: "114194640801953@lid",
                senderPn: "60196953307@s.whatsapp.net" // Proper phone format
            },
            messageTimestamp: Date.now(),
            message: { conversation: "Test LID mapping" }
        }
        
        console.log('📤 Upserting test message with proper senderPn...')
        await store.upsertMessage(testMessage.key.remoteJid, testMessage)
        console.log('✅ Message upserted')
        
        // Test retrieval with both LID and phone number
        console.log('\n🔍 Testing message retrieval...')
        
        const messagesFromLid = await store.getMessages('114194640801953@lid')
        const messagesFromPhone = await store.getMessages('60196953307@s.whatsapp.net')
        
        console.log(`📱 Messages from LID: ${messagesFromLid.length}`)
        console.log(`📞 Messages from phone: ${messagesFromPhone.length}`)
        
        if (messagesFromLid.length === messagesFromPhone.length && messagesFromLid.length > 0) {
            console.log('✅ LID handler is working! Both queries return same results')
        } else {
            console.log('❌ LID handler may not be working properly')
        }
        
        // Check for lidMapping field in messages
        const testMsg = messagesFromLid.find(m => m.key.id.startsWith('TEST_'))
        if (testMsg && testMsg.lidMapping) {
            console.log('✅ Found lidMapping in message:', testMsg.lidMapping)
        } else {
            console.log('❌ No lidMapping found in test message')
        }
        
        await store.close()
        
    } catch (error) {
        console.error('❌ Error:', error.message)
        
        if (error.message.includes('ECONNREFUSED')) {
            console.log('\n💡 MongoDB connection refused. Make sure MongoDB is running.')
        } else if (error.message.includes('Authentication failed')) {
            console.log('\n💡 MongoDB authentication failed. Check your credentials.')
        } else if (error.message.includes('database')) {
            console.log('\n💡 Database access issue. Check database name and permissions.')
        }
    }
}

// Enhanced error handling
debugLidHandler().catch(error => {
    console.error('❌ Debug script failed:', error)
    console.log('\n🔧 Troubleshooting steps:')
    console.log('1. Update MongoDB URI in this script')
    console.log('2. Update database name in this script') 
    console.log('3. Ensure MongoDB is running')
    console.log('4. Check if LID handler is enabled in your store configuration')
    console.log('5. Verify your store is using the latest version with LID handler')
})