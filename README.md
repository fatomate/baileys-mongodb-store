# Baileys MongoDB Store

A MongoDB store implementation for [Baileys](https://github.com/WhiskeySockets/Baileys) WhatsApp Web API with multi-instance support and automatic TTL (Time To Live) for data expiration.

## Features

- ✨ Full compatibility with Baileys store interface
- 🚀 Multi-instance support - Run multiple WhatsApp accounts with isolated data
- ⏰ Automatic TTL - Data expires after configurable days
- 📦 MongoDB indexes for optimal performance
- 🔄 Real-time event binding support
- 💾 Persistent storage across restarts
- 🎯 TypeScript support with full type definitions

## Installation

Add to your `package.json`:

```json
"dependencies": {
    "@baileys/mongodb-store": "github:fatomate/baileys-mongodb-store",
    "mongodb": "^6.3.0"
}
```

Then install:

```bash
npm install
# or
yarn install
```

## Quick Start

```javascript
const makeWASocket = require('@whiskeysockets/baileys').default
const { useMultiFileAuthState } = require('@whiskeysockets/baileys')
const { makeMongoDBStore, cleanupMongoDBStore } = require('@baileys/mongodb-store')

async function connectToWhatsApp() {
    // Create MongoDB store with your configuration
    const store = await makeMongoDBStore({
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_bot',
        instanceId: 'instance_001', // Unique ID for each WhatsApp instance
        ttlDays: 30 // Data expires after 30 days
    })

    const { state, saveCreds } = await useMultiFileAuthState('./auth_info_baileys')
    
    const suki = makeWASocket({
        auth: state,
        printQRInTerminal: true,
        getMessage: async (key) => {
            return await store.loadMessage(key.remoteJid!, key.id!)
        }
    })

    // Bind store to socket events
    store.bind(suki.ev)

    suki.ev.on('creds.update', saveCreds)
    
    suki.ev.on('messages.upsert', async ({ messages }) => {
        console.log('got messages', messages)
        
        // Messages are automatically saved to MongoDB
        // You can retrieve them later:
        const savedMsg = await store.loadMessage(messages[0].key.remoteJid!, messages[0].key.id!)
        console.log('Retrieved message:', savedMsg)
    })

    // Access store data
    const chats = await store.getChats()
    console.log('All chats:', chats)
}

connectToWhatsApp()
```

## Configuration Options

```typescript
interface MongoDBStoreConfig {
    // MongoDB connection URI
    uri: string
    
    // Database name
    database: string
    
    // Unique instance ID for multi-instance support
    instanceId: string
    
    // TTL in days for automatic data expiration (default: 30)
    ttlDays?: number
    
    // Optional logger instance
    logger?: Logger
    
    // Collection name prefix (default: 'baileys_')
    collectionPrefix?: string
}
```

## Multi-Instance Example

Run multiple WhatsApp accounts simultaneously with isolated data:

```javascript
const { makeMongoDBStore, cleanupMongoDBStore } = require('@baileys/mongodb-store')

// Instance 1 - Customer Support
const supportStore = await makeMongoDBStore({
    uri: 'mongodb://localhost:27017',
    database: 'whatsapp_bot',
    instanceId: 'support_account',
    ttlDays: 60 // Keep support conversations for 60 days
})

// Instance 2 - Sales Team
const salesStore = await makeMongoDBStore({
    uri: 'mongodb://localhost:27017',
    database: 'whatsapp_bot',
    instanceId: 'sales_account',
    ttlDays: 90 // Keep sales conversations for 90 days
})

// Each instance has completely isolated data
```

## Store Methods

### Chat Management

```typescript
// Get all chats
const chats = await store.getChats()

// Get specific chat
const chat = await store.getChat('123456789@s.whatsapp.net')

// Update chat
await store.updateChat('123456789@s.whatsapp.net', {
    unreadCount: 0,
    archived: false
})
```

### Message Management

```typescript
// Get all messages in a chat
const messages = await store.getMessages('123456789@s.whatsapp.net')

// Get specific message
const message = await store.getMessage('123456789@s.whatsapp.net', 'message-id')

// Load messages with pagination
const messages = await store.loadMessages(
    '123456789@s.whatsapp.net',
    50, // count
    { before: { id: 'cursor-message-id', remoteJid: '123456789@s.whatsapp.net' } }
)

// Get most recent message
const latestMsg = await store.mostRecentMessage('123456789@s.whatsapp.net')
```

### Contact Management

```typescript
// Get all contacts
const contacts = await store.getContacts()

// Get specific contact
const contact = await store.getContact('123456789@s.whatsapp.net')

// Upsert contacts
await store.upsertContacts([
    { id: '123456789@s.whatsapp.net', name: 'John Doe' }
])
```

### Group Management

```typescript
// Get group metadata
const groupInfo = await store.getGroupMetadata('123456789@g.us')

// Update group metadata
await store.upsertGroupMetadata('123456789@g.us', {
    id: '123456789@g.us',
    subject: 'Family Group',
    participants: [...]
})
```

### Label Management

```typescript
// Get all labels
const labels = await store.getLabels()

// Get chat labels
const chatLabels = await store.getChatLabels('123456789@s.whatsapp.net')

// Get message labels
const messageLabels = await store.getMessageLabels('message-id')

// Add label to chat
await store.upsertLabelAssociation({
    type: 'chat',
    chatId: '123456789@s.whatsapp.net',
    labelId: 'label-1'
})
```

## MongoDB Collections

The store creates the following collections with appropriate indexes:

- `baileys_chats` - Chat information
- `baileys_contacts` - Contact details
- `baileys_messages` - Message history
- `baileys_groupMetadata` - Group information
- `baileys_state` - Connection state
- `baileys_presences` - User presence data
- `baileys_labels` - Label definitions
- `baileys_labelAssociations` - Label-chat/message associations

All collections include:
- `instanceId` field for multi-instance isolation
- `updatedAt` field with TTL index for automatic expiration
- Optimized indexes for query performance

## TTL (Time To Live) Feature

Data automatically expires after the configured number of days:

```typescript
const store = await makeMongoDBStore({
    uri: 'mongodb://localhost:27017',
    database: 'whatsapp_bot',
    instanceId: 'my_instance',
    ttlDays: 7 // All data expires after 7 days
})
```

This helps:
- Prevent database growth
- Comply with data retention policies
- Reduce storage costs
- Maintain performance

## Error Handling & Cleanup

```javascript
const { makeMongoDBStore, cleanupMongoDBStore } = require('@baileys/mongodb-store')

try {
    const store = await makeMongoDBStore({
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_bot',
        instanceId: 'my_instance'
    })
} catch (error) {
    console.error('Failed to connect to MongoDB:', error)
}

// Graceful shutdown - close all connections
process.on('SIGINT', async () => {
    console.log('Shutting down...')
    await cleanupMongoDBStore() // Closes all connections
    process.exit(0)
})

// Or cleanup a specific instance
await cleanupMongoDBStore('my_instance') // Just close connection
await cleanupMongoDBStore('my_instance', true) // Delete all data and close
```

### Cleanup Function Options

The `cleanupMongoDBStore` function provides flexible cleanup options:

```javascript
// Option 1: Close all connections (no data deletion)
await cleanupMongoDBStore()

// Option 2: Close connection for specific instance (no data deletion)
await cleanupMongoDBStore('instance_001')

// Option 3: Delete all data for instance AND close connection
await cleanupMongoDBStore('instance_001', true)

// Example: Clean up before switching instances
async function switchInstance(oldInstanceId, newInstanceId) {
    // Clean up old instance data
    await cleanupMongoDBStore(oldInstanceId, true)
    
    // Create new store for new instance
    const newStore = await makeMongoDBStore({
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_bot',
        instanceId: newInstanceId,
        ttlDays: 30
    })
    
    return newStore
}
```

## Performance Tips

1. **Indexes**: The store automatically creates optimal indexes on first run
2. **Connection Pooling**: MongoDB driver handles connection pooling automatically
3. **Batch Operations**: The store uses bulk operations where possible
4. **TTL**: Configure appropriate TTL to prevent unlimited data growth

## Migration from In-Memory Store

Migrating from the default in-memory store is straightforward:

```typescript
// Before (in-memory store)
import { makeInMemoryStore } from '@whiskeysockets/baileys'
const store = makeInMemoryStore({})

// After (MongoDB store)
const { makeMongoDBStore } = require('@baileys/mongodb-store')
const store = await makeMongoDBStore({
    uri: 'mongodb://localhost:27017',
    database: 'whatsapp_bot',
    instanceId: 'my_instance'
})

// The rest of your code remains the same!
```

## Requirements

- Node.js >= 14
- MongoDB >= 4.4
- Baileys (peer dependency)

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and feature requests, please create an issue on GitHub.