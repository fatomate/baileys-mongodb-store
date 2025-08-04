const { makeMongoDBStore } = require('./dist/index.js');

// Test configuration
const config = {
    uri: 'mongodb://localhost:27017',
    database: 'baileys_test',
    instanceId: 'test_instance_1',
    ttlDays: 7
};

async function testMongoDBStore() {
    console.log('Testing Baileys MongoDB Store...\n');
    
    try {
        // Create store instance
        console.log('1. Creating MongoDB store instance...');
        const store = await makeMongoDBStore(config);
        console.log('✓ Store created successfully');
        
        // Test state management
        console.log('\n2. Testing state management...');
        await store.updateState({ connection: 'open' });
        const state = await store.getState();
        console.log('✓ State:', state);
        
        // Test chat operations
        console.log('\n3. Testing chat operations...');
        const testChat = {
            id: 'test@example.com',
            conversationTimestamp: Date.now() / 1000,
            unreadCount: 0
        };
        await store.upsertChats(testChat);
        const chats = await store.getChats();
        console.log('✓ Chats:', chats.length);
        
        // Test contact operations
        console.log('\n4. Testing contact operations...');
        const testContact = {
            id: 'test@example.com',
            name: 'Test Contact'
        };
        await store.upsertContacts([testContact]);
        const contacts = await store.getContacts();
        console.log('✓ Contacts:', Object.keys(contacts).length);
        
        // Test message operations
        console.log('\n5. Testing message operations...');
        const testMessage = {
            key: {
                remoteJid: 'test@example.com',
                id: 'test_message_1',
                fromMe: true
            },
            message: {
                conversation: 'Test message'
            },
            messageTimestamp: Date.now() / 1000
        };
        await store.upsertMessage('test@example.com', testMessage);
        const messages = await store.getMessages('test@example.com');
        console.log('✓ Messages:', messages.length);
        
        // Test label operations
        console.log('\n6. Testing label operations...');
        const testLabel = {
            id: 'label_1',
            name: 'Test Label',
            color: 5,
            deleted: false
        };
        await store.upsertLabel('label_1', testLabel);
        const labels = await store.getLabels();
        console.log('✓ Labels:', Object.keys(labels).length);
        
        // Clean up
        console.log('\n7. Cleaning up test data...');
        await store.clearAll();
        console.log('✓ Test data cleared');
        
        // Close connection
        console.log('\n8. Closing connection...');
        await store.close();
        console.log('✓ Connection closed');
        
        console.log('\n✅ All tests passed successfully!');
        
    } catch (error) {
        console.error('\n❌ Test failed:', error);
        process.exit(1);
    }
}

// Run tests
testMongoDBStore();