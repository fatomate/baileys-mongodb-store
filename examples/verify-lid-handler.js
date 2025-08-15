const { MongoClient } = require('mongodb')
const { LidHandler } = require('../dist/utils/lidHandler')

/**
 * Script to verify LID handler functionality and debug issues
 */
async function verifyLidHandler() {
    // Connect to your MongoDB
    const client = new MongoClient('mongodb://localhost:27017') // Update with your connection string
    await client.connect()
    const db = client.db('your_database_name') // Update with your database name
    
    console.log('🔍 LID Handler Verification\n')
    
    // Initialize LID handler
    const lidHandler = new LidHandler('6860DCA0E2819') // Your instance ID
    await lidHandler.initialize(db, 'baileys_') // Your collection prefix
    
    // Test message from your data
    const testMessage = {
        key: {
            remoteJid: "114194640801953@lid",
            fromMe: true,
            id: "D393966474A87BE7525712ABE08E9EAE",
            senderLid: "114194640801953@lid",
            senderPn: "114194640801953@lid" // This is the problem - it's @lid instead of phone number
        },
        messageTimestamp: 1755245509,
        pushName: "Wabot Demo",
        message: {
            conversation: "Checking lid log"
        }
    }
    
    console.log('1. Testing isLidFormat detection:')
    console.log(`   remoteJid "${testMessage.key.remoteJid}" is LID: ${lidHandler.isLidFormat(testMessage.key.remoteJid)}`)
    console.log(`   senderPn "${testMessage.key.senderPn}" is LID: ${lidHandler.isLidFormat(testMessage.key.senderPn)}`)
    
    console.log('\n2. Testing extractLidInfo:')
    const lidInfo = lidHandler.extractLidInfo(testMessage)
    console.log(`   Extracted LID: ${lidInfo.lid}`)
    console.log(`   Extracted phone: ${lidInfo.phoneNumber}`)
    console.log(`   ❌ No phone number extracted because senderPn is also @lid format`)
    
    console.log('\n3. Testing processMessage:')
    const result = await lidHandler.processMessage(testMessage)
    console.log(`   Normalized JID: ${result.normalizedJid}`)
    console.log(`   Mapping stored: ${result.lidInfo.mappingStored}`)
    console.log(`   ❌ No mapping stored because no phone number was found`)
    
    console.log('\n4. Creating a proper test with phone number:')
    const properMessage = {
        key: {
            remoteJid: "114194640801953@lid",
            fromMe: true,
            id: "TEST_MESSAGE_ID",
            senderLid: "114194640801953@lid",
            senderPn: "60196953307@s.whatsapp.net" // Proper phone number format
        },
        messageTimestamp: Date.now(),
        message: { conversation: "Test with proper phone number" }
    }
    
    const properResult = await lidHandler.processMessage(properMessage)
    console.log(`   ✅ Normalized JID: ${properResult.normalizedJid}`)
    console.log(`   ✅ Mapping stored: ${properResult.lidInfo.mappingStored}`)
    
    console.log('\n5. Checking stored mappings:')
    const allMappings = await lidHandler.getAllMappings()
    console.log(`   Total mappings stored: ${allMappings.length}`)
    allMappings.forEach(mapping => {
        console.log(`   📱 ${mapping.lid} → ${mapping.phoneNumber}`)
    })
    
    console.log('\n6. Testing normalization:')
    const normalized1 = await lidHandler.normalizeJid('114194640801953@lid')
    const normalized2 = await lidHandler.normalizeJid('60196953307@s.whatsapp.net')
    console.log(`   LID → normalized: ${normalized1}`)
    console.log(`   Phone → normalized: ${normalized2}`)
    
    console.log('\n7. Checking your actual database:')
    const messagesCollection = db.collection('baileys_messages') // Update collection name if different
    const lidMappingsCollection = db.collection('baileys_lidMappings') // Update collection name if different
    
    const messageCount = await messagesCollection.countDocuments({ instanceId: '6860DCA0E2819' })
    const mappingCount = await lidMappingsCollection.countDocuments({ instanceId: '6860DCA0E2819' })
    
    console.log(`   Messages in DB: ${messageCount}`)
    console.log(`   LID mappings in DB: ${mappingCount}`)
    
    // Show some example messages
    const sampleMessages = await messagesCollection.find({ instanceId: '6860DCA0E2819' }).limit(3).toArray()
    console.log('\n   Sample messages:')
    sampleMessages.forEach(msg => {
        console.log(`     JID: ${msg.jid}, remoteJid: ${msg.key?.remoteJid}, senderPn: ${msg.key?.senderPn}`)
    })
    
    console.log('\n📋 ANALYSIS:')
    console.log('❌ Issue found: Your senderPn field contains @lid format instead of phone number')
    console.log('💡 Solution: The senderPn should contain the actual phone number like "60196953307@s.whatsapp.net"')
    console.log('🔧 This indicates that WhatsApp is not providing the phone number in the senderPn field')
    console.log('   for your specific setup or account type.')
    
    console.log('\n🛠️  RECOMMENDATIONS:')
    console.log('1. Check if other message fields contain phone numbers')
    console.log('2. Consider manually creating mappings for known LID→phone pairs')
    console.log('3. Update the LID handler to handle your specific message format')
    
    await client.close()
}

// Add error handling
verifyLidHandler().catch(error => {
    console.error('❌ Verification failed:', error)
    process.exit(1)
})