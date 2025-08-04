const { makeMongoDBStore, cleanupMongoDBStore } = require('../src')
const { MongoClient } = require('mongodb')
const pino = require('pino')

const logger = pino({ level: 'debug' })

/**
 * Test MongoDB store functionality without WhatsApp connection
 */
async function testMongoDBStore() {
    logger.info('Testing MongoDB store functionality...')

    // Create store
    const store = await makeMongoDBStore({
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_test',
        instanceId: 'test_instance',
        ttlDays: 1, // 1 day for testing
        logger
    })

    try {
        // Test 1: Chat operations
        logger.info('Testing chat operations...')
        
        await store.upsertChats(
            {
                id: '1234567890@s.whatsapp.net',
                conversationTimestamp: Date.now() / 1000,
                unreadCount: 5,
                name: 'Test User 1',
                archived: false,
                pinned: 1
            },
            {
                id: '0987654321@s.whatsapp.net',
                conversationTimestamp: Date.now() / 1000,
                unreadCount: 0,
                name: 'Test User 2',
                archived: true,
                pinned: 0
            }
        )

        const chats = await store.getChats()
        logger.info(`Retrieved ${chats.length} chats`)

        const chat = await store.getChat('1234567890@s.whatsapp.net')
        logger.info('Retrieved single chat:', chat)

        await store.updateChat('1234567890@s.whatsapp.net', {
            unreadCount: 0,
            archived: false
        })
        logger.info('Updated chat successfully')

        // Test 2: Contact operations
        logger.info('Testing contact operations...')
        
        await store.upsertContacts([
            {
                id: '1234567890@s.whatsapp.net',
                name: 'John Doe',
                notify: 'Johnny',
                imgUrl: 'https://example.com/avatar.jpg'
            }
        ])

        const contacts = await store.getContacts()
        logger.info(`Retrieved ${Object.keys(contacts).length} contacts`)

        // Test 3: Message operations
        logger.info('Testing message operations...')
        
        const testMessage = {
            key: {
                remoteJid: '1234567890@s.whatsapp.net',
                fromMe: false,
                id: 'MSG001',
                participant: undefined
            },
            messageTimestamp: Math.floor(Date.now() / 1000),
            message: {
                conversation: 'Hello, this is a test message!'
            }
        }

        await store.upsertMessage('1234567890@s.whatsapp.net', testMessage as any, 'append')
        
        const messages = await store.getMessages('1234567890@s.whatsapp.net')
        logger.info(`Retrieved ${messages.length} messages`)

        const singleMessage = await store.getMessage('1234567890@s.whatsapp.net', 'MSG001')
        logger.info('Retrieved single message:', singleMessage)

        // Test 4: Group metadata operations
        logger.info('Testing group metadata operations...')
        
        await store.upsertGroupMetadata('123456789@g.us', {
            id: '123456789@g.us',
            subject: 'Test Group',
            subjectOwner: '1234567890@s.whatsapp.net',
            subjectTime: Date.now() / 1000,
            creation: Date.now() / 1000,
            owner: '1234567890@s.whatsapp.net',
            desc: 'This is a test group',
            descId: 'DESC001',
            participants: [
                { id: '1234567890@s.whatsapp.net', isAdmin: true, isSuperAdmin: true },
                { id: '0987654321@s.whatsapp.net', isAdmin: false, isSuperAdmin: false }
            ],
            ephemeralDuration: 0
        })

        const groupMetadata = await store.getGroupMetadata('123456789@g.us')
        logger.info('Retrieved group metadata:', groupMetadata)

        // Test 5: Label operations
        logger.info('Testing label operations...')
        
        await store.upsertLabel('label1', {
            id: 'label1',
            name: 'Important',
            color: 1,
            deleted: false,
            predefinedId: '1'
        })

        const labels = await store.getLabels()
        logger.info(`Retrieved ${Object.keys(labels).length} labels`)

        // Test label associations
        await store.upsertLabelAssociation({
            type: 1, // Chat type
            chatId: '1234567890@s.whatsapp.net',
            labelId: 'label1'
        })

        const chatLabels = await store.getChatLabels('1234567890@s.whatsapp.net')
        logger.info(`Chat has ${chatLabels.length} labels`)

        // Test 6: State operations
        logger.info('Testing state operations...')
        
        await store.updateState({
            connection: 'open',
            lastDisconnect: undefined,
            isNewLogin: false
        })

        const state = await store.getState()
        logger.info('Current state:', state)

        // Test 7: Presence operations
        logger.info('Testing presence operations...')
        
        await store.updatePresence('1234567890@s.whatsapp.net', {
            'default': {
                lastKnownPresence: 'available',
                lastSeen: Date.now() / 1000
            }
        })

        const presences = await store.getPresences()
        logger.info('Presences:', presences)

        // Test 8: Load messages with cursor
        logger.info('Testing message pagination...')
        
        // Add more messages for pagination test
        for (let i = 0; i < 10; i++) {
            await store.upsertMessage('1234567890@s.whatsapp.net', {
                key: {
                    remoteJid: '1234567890@s.whatsapp.net',
                    fromMe: false,
                    id: `MSG00${i + 2}`,
                    participant: undefined
                },
                messageTimestamp: Math.floor(Date.now() / 1000) - i * 60, // 1 minute apart
                message: {
                    conversation: `Test message ${i + 2}`
                }
            } as any, 'append')
        }

        const paginatedMessages = await store.loadMessages(
            '1234567890@s.whatsapp.net',
            5,
            { before: { id: 'MSG005', remoteJid: '1234567890@s.whatsapp.net' } }
        )
        logger.info(`Loaded ${paginatedMessages.length} messages with pagination`)

        // Test 9: Most recent message
        const recentMessage = await store.mostRecentMessage('1234567890@s.whatsapp.net')
        logger.info('Most recent message:', recentMessage)

        // Test 10: Delete operations
        logger.info('Testing delete operations...')
        
        await store.deleteMessages('1234567890@s.whatsapp.net', ['MSG001', 'MSG002'])
        const remainingMessages = await store.getMessages('1234567890@s.whatsapp.net')
        logger.info(`${remainingMessages.length} messages remaining after deletion`)

        await store.deleteChats(['0987654321@s.whatsapp.net'])
        const remainingChats = await store.getChats()
        logger.info(`${remainingChats.length} chats remaining after deletion`)

        await store.deleteLabel('label1')
        const remainingLabels = await store.getLabels()
        logger.info(`${Object.keys(remainingLabels).length} labels remaining after deletion`)

        logger.info('All tests completed successfully!')

        // Test TTL by checking indexes
        logger.info('Checking TTL indexes...')
        const client = new MongoClient('mongodb://localhost:27017')
        await client.connect()
        const db = client.db('whatsapp_test')
        
        const collections = [
            'baileys_chats',
            'baileys_contacts',
            'baileys_messages',
            'baileys_groupMetadata',
            'baileys_labels',
            'baileys_labelAssociations'
        ]

        for (const collName of collections) {
            const indexes = await db.collection(collName).indexes()
            const ttlIndex = indexes.find(idx => idx.expireAfterSeconds !== undefined)
            if (ttlIndex) {
                logger.info(`${collName} has TTL index: ${ttlIndex.expireAfterSeconds} seconds`)
            }
        }

        await client.close()

    } catch (error) {
        logger.error('Test failed:', error)
        throw error
    } finally {
        // Clean up
        logger.info('Cleaning up test data...')
        await store.clearAll()
        await store.close()
        logger.info('Test completed and cleaned up')
    }
}

// Run tests
if (require.main === module) {
    testMongoDBStore()
        .then(() => {
            logger.info('All tests passed!')
            process.exit(0)
        })
        .catch((error) => {
            logger.error('Tests failed:', error)
            process.exit(1)
        })
}