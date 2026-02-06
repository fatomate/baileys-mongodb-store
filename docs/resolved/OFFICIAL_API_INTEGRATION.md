# Official WhatsApp Business API Integration Guide

## Overview
This guide explains how to integrate the Official WhatsApp Business API with your Baileys MongoDB Store to have unified message storage for both WhatsApp Web (Baileys) and Official API messages.

## Features
- ✅ Unified message storage in `baileys_messages` collection
- ✅ Automatic media download and deduplication
- ✅ Support for all message types (text, media, location, interactive, etc.)
- ✅ Status updates (sent, delivered, read, failed)
- ✅ Seamless format conversion between Official API and Baileys

## Installation

1. **Build the package:**
```bash
npm run build
```

2. **Import the integration modules in your application:**
```javascript
// In your main waziper.js or application file
const officialApi = require('./officialApiIntegration');
```

## Integration Steps

### Step 1: Initialize the Official Store

When an instance connects with Official API credentials:

```javascript
const { makeEnhancedMongoDBStore } = require('@baileys/mongodb-store');
const officialApi = require('./officialApiIntegration');

// Create the enhanced MongoDB store
const store = await makeEnhancedMongoDBStore({
    uri: 'mongodb://localhost:27017',
    database: 'whatsapp_db',
    instanceId: instance_id,
    // ... other config
});

// Initialize Official API store if account uses Official API
if (accountData.login_type === 1) {
    officialApi.initializeOfficialStore(store, instance_id, accountData);
}
```

### Step 2: Process Incoming Webhooks

Add webhook processing to your webhook handler:

```javascript
// In your webhook endpoint handler
app.post('/webhook/:instance_id', async (req, res) => {
    const instance_id = req.params.instance_id;
    const webhookData = req.body;
    
    // Check if this is an Official API webhook
    if (webhookData.entry) {
        // Format webhook data
        const formattedData = {
            messaging_product: 'whatsapp',
            metadata: webhookData.entry[0]?.changes[0]?.value?.metadata || {},
            messages: webhookData.entry[0]?.changes[0]?.value?.messages || [],
            statuses: webhookData.entry[0]?.changes[0]?.value?.statuses || [],
            contacts: webhookData.entry[0]?.changes[0]?.value?.contacts || []
        };
        
        // Process and store in MongoDB
        await officialApi.handleOfficialWebhook(WAZIPER, instance_id, formattedData);
    }
    
    res.sendStatus(200);
});
```

### Step 3: Save Outgoing Messages

After sending a message via Official API:

```javascript
// In your send message function
async function sendOfficialMessage(instance_id, recipientPhone, messagePayload) {
    const accountData = await getAccountData(instance_id);
    const { access_token, phone_number_id } = JSON.parse(accountData.tmp);
    
    // Send via Official API
    const response = await axios.post(
        `https://graph.facebook.com/v23.0/${phone_number_id}/messages`,
        messagePayload,
        {
            headers: {
                'Authorization': `Bearer ${access_token}`,
                'Content-Type': 'application/json'
            }
        }
    );
    
    // Save to MongoDB
    await officialApi.handleOfficialOutgoing(
        WAZIPER,
        instance_id,
        recipientPhone,
        messagePayload,
        response.data
    );
    
    return response.data;
}
```

### Step 4: Media Management

The integration automatically handles media:

```javascript
// Get media URL for a message
const mediaUrl = await officialApi.getOfficialMediaUrl(
    instance_id,
    messageId,
    store,
    accountData
);

// Clean up old media (schedule daily)
const cron = require('node-cron');
cron.schedule('0 3 * * *', async () => {
    await officialApi.cleanupOfficialMedia(
        instance_id,
        30, // Keep for 30 days
        store,
        accountData
    );
});
```

### Step 5: Cleanup on Disconnect

When an instance disconnects:

```javascript
// In your disconnect handler
async function handleDisconnect(instance_id) {
    // Clear cached store
    officialApi.clearOfficialStore(instance_id);
    
    // Optionally delete all media
    if (shouldDeleteMedia) {
        await officialApi.cleanupOfficialMedia(
            instance_id,
            0, // Delete all
            store,
            accountData
        );
    }
}
```

## Message Format Examples

### Incoming Webhook Message
```javascript
{
    "messaging_product": "whatsapp",
    "metadata": {
        "display_phone_number": "15550555555",
        "phone_number_id": "123456789"
    },
    "messages": [{
        "from": "1234567890",
        "id": "wamid.xxx",
        "timestamp": "1234567890",
        "type": "text",
        "text": {
            "body": "Hello World"
        }
    }]
}
```

### Stored in MongoDB (Baileys Format)
```javascript
{
    "instanceId": "your_instance_id",
    "jid": "1234567890@s.whatsapp.net",
    "key": {
        "remoteJid": "1234567890@s.whatsapp.net",
        "fromMe": false,
        "id": "wamid.xxx"
    },
    "messageTimestamp": 1234567890,
    "message": {
        "conversation": "Hello World"
    },
    "mediaUrl": "media/instance_id/official/file.jpg", // If media
    "mediaHash": "sha256_hash", // For deduplication
    "updatedAt": "2024-01-01T00:00:00.000Z"
}
```

## Supported Message Types

| Official API Type | Baileys Type | Notes |
|------------------|--------------|-------|
| text | conversation | Plain text messages |
| image | imageMessage | With caption support |
| video | videoMessage | With caption support |
| audio | audioMessage | Voice notes supported |
| document | documentMessage | All file types |
| sticker | stickerMessage | Animated stickers supported |
| location | locationMessage | With name/address |
| reaction | reactionMessage | Emoji reactions |
| interactive | extendedTextMessage | Stored with metadata |
| button | extendedTextMessage | Stored with payload |

## Media Deduplication

The system automatically detects duplicate media using SHA256 hashes:
- If a media file with the same hash already exists, it reuses the existing file
- This significantly reduces storage space for frequently shared media
- The `mediaReused` field indicates if a file was reused

## Error Handling

All functions include error handling and logging:

```javascript
try {
    await officialApi.processOfficialWebhook(instance_id, webhookData, store, accountData);
} catch (error) {
    console.error('Failed to process webhook:', error);
    // Handle error appropriately
}
```

## Configuration

The Official API configuration is extracted from your account data:

```javascript
{
    "phoneNumberId": "from account.tmp.phone_number_id",
    "accessToken": "from account.tmp.access_token",
    "apiVersion": "v23.0", // or v19.0 for wabot_pro
    "provider": "meta" // or "wabot_pro"
}
```

## Testing

To test the integration:

1. Send a test message via Official API
2. Check MongoDB for the stored message
3. Verify media download (if applicable)
4. Check status updates are being processed

## Troubleshooting

### Messages not being stored
- Verify the instance has `login_type: 1` in the database
- Check that the enhanced store is properly initialized
- Ensure webhook data is correctly formatted

### Media not downloading
- Check media directory permissions
- Verify access token has media download permissions
- Check available disk space

### Status updates not working
- Ensure webhook endpoint receives status events
- Verify message IDs match between sent and status updates

## Performance Considerations

- Media downloads are asynchronous and won't block message processing
- Deduplication check uses indexed `mediaHash` field for fast lookups
- Cleanup tasks should be scheduled during low-traffic periods
- Consider implementing rate limiting for webhook processing

## Security

- Always validate webhook signatures
- Store access tokens securely
- Implement proper error handling to avoid exposing sensitive data
- Use HTTPS for all API communications
- Regularly rotate access tokens