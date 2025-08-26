const { proto } = require('baileys')

// Test data for simulating REVOKE messages
const mockRevokeMessage = {
    key: {
        remoteJid: "60196953307@s.whatsapp.net",
        fromMe: true,
        id: "3EB05A3103789DFE8E35B3" // This is the REVOKE message's own ID
    },
    message: {
        protocolMessage: {
            key: {
                remoteJid: "60196953307@s.whatsapp.net",
                fromMe: true,
                id: "3EB0D8FCABF3ED7424BF84" // This is the ID of the message being revoked
            },
            type: proto.Message.ProtocolMessage.Type.REVOKE
        }
    },
    messageTimestamp: "1756163674",
    status: "PENDING"
}

const mockNormalMessage = {
    key: {
        remoteJid: "601164458400@s.whatsapp.net",
        fromMe: false,
        id: "835434525730317A0A42529B2C86BF01"
    },
    message: {
        conversation: "Hello, this is a test message"
    },
    messageTimestamp: "1756163670",
    status: "PENDING"
}

// Protocol messages that should be skipped
const mockHistorySyncMessage = {
    key: {
        remoteJid: "status@broadcast",
        fromMe: true,
        id: "HISTORY_SYNC_001"
    },
    message: {
        protocolMessage: {
            type: proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION,
            historySyncNotification: {
                fileSha256: "QIPOuK+XlI1e5vEov0QURS2wfvIFPE2HoFs7qMmgips=",
                fileLength: "308",
                mediaKey: "FuheG4LXxDY7wAdSU7JIwGg2q66R67Z222dDl+8oHzY="
            }
        }
    },
    messageTimestamp: "1756163674"
}

// Export test messages
module.exports = {
    mockRevokeMessage,
    mockNormalMessage,
    mockHistorySyncMessage,
    
    // Helper function to log test results
    logTestResult: (testName, passed, details = '') => {
        const symbol = passed ? '✅' : '❌'
        console.log(`${symbol} ${testName}${details ? ': ' + details : ''}`)
    },
    
    // Test scenarios
    testScenarios: [
        {
            name: 'REVOKE message handling',
            description: 'Should update the revoked message status instead of storing REVOKE message',
            message: mockRevokeMessage,
            expectedBehavior: 'Update target message as revoked, skip storing REVOKE message'
        },
        {
            name: 'Normal message handling',
            description: 'Should store normal messages without issues',
            message: mockNormalMessage,
            expectedBehavior: 'Store message normally'
        },
        {
            name: 'History sync protocol message',
            description: 'Should skip history sync protocol messages',
            message: mockHistorySyncMessage,
            expectedBehavior: 'Skip storing this protocol message'
        }
    ]
}