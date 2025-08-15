const { makeEnhancedMongoDBStore } = require('./dist/makeEnhancedMongoDBStore');

// Sample test messages from the examples
const testMessages = [
    // FromMe: true - Bot sending to user (LID in remoteJid)
    {
        key: {
            remoteJid: "114194640801953@lid",
            fromMe: true,
            id: "3EB092EE68C74464939B25",
            senderLid: "114194640801953@lid",
            senderPn: "114194640801953@lid"
        },
        messageTimestamp: 1755260069,
        pushName: "Wabot Demo",
        broadcast: false,
        status: 2,
        message: {
            extendedTextMessage: {
                text: "hola back",
                contextInfo: {
                    conversionSource: "unknown",
                    ephemeralSettingTimestamp: "1755249841",
                    disappearingMode: {
                        initiator: "CHANGED_IN_CHAT",
                        trigger: "CHAT_SETTING",
                        initiatedByMe: true
                    }
                },
                inviteLinkGroupTypeV2: "DEFAULT"
            }
        },
        verifiedBizName: "Wabot Demo"
    },
    // FromMe: false - User sending to bot (LID in remoteJid, phone in senderPn)
    {
        key: {
            remoteJid: "114194640801953@lid",
            fromMe: false,
            id: "3EB06E7C17F9DB9E2C71B6",
            senderLid: "114194640801953@lid",
            senderPn: "60196953307@s.whatsapp.net"
        },
        messageTimestamp: 1755264320,
        pushName: "Firdaus Azizi",
        broadcast: false,
        message: {
            extendedTextMessage: {
                text: "test 2",
                contextInfo: {
                    conversionSource: "unknown",
                    ephemeralSettingTimestamp: "1755251004",
                    disappearingMode: {
                        initiator: "CHANGED_IN_CHAT",
                        trigger: "CHAT_SETTING",
                        initiatedByMe: false
                    }
                },
                inviteLinkGroupTypeV2: "DEFAULT"
            }
        }
    }
];

async function testLidNormalization() {
    console.log('Testing LID normalization...\n');
    
    // Mock store configuration
    const mockConfig = {
        uri: 'mongodb://localhost:27017',
        database: 'test-baileys',
        instanceId: 'TEST123',
        logLevel: 'all',
        lidHandler: {
            enableCache: true,
            cacheTTL: 3600
        }
    };
    
    console.log('Test Configuration:');
    console.log('- Instance ID:', mockConfig.instanceId);
    console.log('- LID Handler enabled:', !!mockConfig.lidHandler);
    console.log('\n--- Testing Message Processing ---\n');
    
    for (const msg of testMessages) {
        console.log(`Message ${msg.key.id}:`);
        console.log(`- FromMe: ${msg.key.fromMe}`);
        console.log(`- Original remoteJid: ${msg.key.remoteJid}`);
        console.log(`- SenderLid: ${msg.key.senderLid}`);
        console.log(`- SenderPn: ${msg.key.senderPn}`);
        
        // Expected behavior after normalization:
        const expectedJid = msg.key.fromMe ? 
            '60196953307@s.whatsapp.net' : // Bot sending to user - should normalize to user's phone
            '60196953307@s.whatsapp.net';   // User sending to bot - should normalize to user's phone
            
        console.log(`- Expected normalized JID: ${expectedJid}`);
        console.log('');
    }
    
    console.log('\n--- Expected MongoDB Document Structure ---\n');
    console.log('After processing, documents should have:');
    console.log('1. key.remoteJid: "60196953307@s.whatsapp.net" (normalized phone number)');
    console.log('2. jid: "60196953307@s.whatsapp.net" (normalized phone number)');
    console.log('3. lidMapping: { lid: "114194640801953@lid", phoneNumber: "60196953307@s.whatsapp.net", originalJid: "114194640801953@lid" }');
    
    console.log('\n✅ Test script completed. The actual normalization happens when messages are processed through the store.');
    console.log('Run your bot and check MongoDB to verify that remoteJid is updated correctly.');
}

testLidNormalization().catch(console.error);