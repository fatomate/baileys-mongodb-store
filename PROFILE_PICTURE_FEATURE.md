# Profile Picture Auto-Retrieval Feature

## Overview

The enhanced MongoDB store now supports automatic profile picture retrieval for WhatsApp contacts. This feature automatically fetches and stores profile picture URLs when contacts are added or updated, similar to the existing media download and poll decryption features.

## Features

- **Automatic Retrieval**: Profile pictures are automatically fetched when contacts are added
- **Smart Refresh**: Stale profile pictures (older than 7 days by default) are automatically refreshed
- **Privacy Handling**: Gracefully handles privacy restrictions without errors
- **Queue Processing**: Uses Bull queue for reliable background processing
- **Rate Limiting**: Configurable delays between requests to avoid rate limiting
- **Retry Logic**: Automatic retries with exponential backoff for network failures

## Configuration

```typescript
import { makeEnhancedMongoDBStore } from 'baileys-mongodb-store'
import makeWASocket from '@whiskeysockets/baileys'

// Create socket
const sock = makeWASocket({
    auth: state,
    printQRInTerminal: true
})

// Create store with profile picture configuration
const store = await makeEnhancedMongoDBStore({
    uri: 'mongodb://localhost:27017',
    database: 'whatsapp_bot',
    instanceId: 'my_instance',
    
    // Pass the socket instance (required for profile picture retrieval)
    sock: sock,
    
    // Configure profile picture auto-retrieval
    profilePictureConfig: {
        enabled: true,                    // Enable auto-retrieval
        refreshIntervalDays: 7,           // Refresh every 7 days
        requestDelay: 500,                // 500ms delay between requests
        maxConcurrent: 5,                 // Max 5 concurrent fetches
        retryAttempts: 3,                 // Retry 3 times on failure
        logPrivacyErrors: false           // Don't log privacy restrictions
    },
    
    // Redis required for queue processing
    redis: {
        connection: 'redis://localhost:6379'
    }
})

// Bind store to socket events
store.bind(sock.ev)
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | false | Enable/disable profile picture auto-retrieval |
| `refreshIntervalDays` | number | 7 | Days before refreshing existing profile pictures |
| `requestDelay` | number | 500 | Milliseconds to wait between profile picture requests |
| `maxConcurrent` | number | 5 | Maximum concurrent profile picture fetches |
| `retryAttempts` | number | 3 | Number of retry attempts for failed fetches |
| `logPrivacyErrors` | boolean | false | Whether to log privacy restriction errors |

## How It Works

### 1. Contact Processing
When contacts are upserted (added or updated), the system checks each contact:
- If no `profilePic` field exists → queues fetch
- If `profilePic` exists but `updatedAt` is older than `refreshIntervalDays` → queues refresh
- If `profilePic` exists but no `profilePicUpdatedAt` timestamp → queues refresh

### 2. Queue Processing
Profile picture fetches are processed asynchronously through Bull queues:
- Jobs are queued with random delays (0-1 second) to spread out requests
- Failed requests are retried with exponential backoff
- Privacy errors are handled silently (no retries)

### 3. Data Storage
Successfully fetched profile pictures are stored in the contact document:
```javascript
{
  "_id": "...",
  "id": "60196794989@s.whatsapp.net",
  "notify": "Contact Name",
  "profilePic": "https://pps.whatsapp.net/v/t61.24694-24/...",
  "profilePicUpdatedAt": ISODate("2025-08-27T10:00:00.000Z"),
  "updatedAt": ISODate("2025-08-27T10:00:00.000Z")
}
```

## Error Handling

### Privacy Restrictions
When a user has restricted their profile picture visibility:
- The error is caught and handled silently
- No retries are attempted
- Optionally logged if `logPrivacyErrors: true`

### Network Errors
Network-related errors trigger automatic retries:
- Exponential backoff between retries
- Configurable maximum retry attempts
- Failed jobs after max retries are logged

## Performance Considerations

### Rate Limiting
- Default 500ms delay between requests
- Random initial delays (0-1s) to spread load
- Configurable concurrency limits

### Database Impact
- Single database query per contact to check existing data
- Single update operation per successful fetch
- Indexes automatically created for optimal performance

## Requirements

- **Redis**: Required for Bull queue processing
- **MongoDB**: For storing contact data and profile pictures
- **WhatsApp Socket**: Must pass `sock` instance to the store configuration

## Example Usage

```javascript
// Basic usage - profile pictures are fetched automatically
sock.ev.on('contacts.upsert', async (contacts) => {
    console.log(`Processing ${contacts.length} contacts`)
    // Profile pictures will be fetched in the background
})

// Check if profile picture was fetched
setTimeout(async () => {
    const contact = await store.getContact('1234567890@s.whatsapp.net')
    if (contact?.profilePic) {
        console.log('Profile picture:', contact.profilePic)
    }
}, 5000)

// Manual profile picture check
const contact = await store.getContact('1234567890@s.whatsapp.net')
if (!contact?.profilePic || isStale(contact.profilePicUpdatedAt)) {
    // Will be automatically queued for refresh on next contact update
}
```

## Monitoring

Enable logging to monitor profile picture operations:

```javascript
const store = await makeEnhancedMongoDBStore({
    // ... other config
    logLevel: 'all',  // Enable detailed logging
    profilePictureConfig: {
        enabled: true,
        logPrivacyErrors: true  // Log privacy restrictions
    }
})
```

Log messages:
- `📸 [Profile Picture Queue] Fetching profile picture for {contactId}`
- `✅ [Profile Picture Queue] Updated profile picture for {contactId}`
- `⚠️ [Profile Picture Queue] No profile picture available for {contactId}`
- `🔒 [Profile Picture Queue] Privacy restricted for {contactId}`

## Limitations

1. **WhatsApp Privacy Settings**: Users can restrict profile picture visibility
2. **Rate Limiting**: WhatsApp may rate limit profile picture requests
3. **Socket Required**: Must provide WhatsApp socket instance to store
4. **Redis Required**: Feature requires Redis for queue processing

## Migration

Existing installations can enable this feature by:

1. Ensure Redis is available and configured
2. Update store configuration with `sock` and `profilePictureConfig`
3. Profile pictures will be fetched for new contacts automatically
4. Existing contacts will get profile pictures on next update

## Best Practices

1. **Use reasonable delays**: Keep `requestDelay` at 500ms or higher
2. **Limit concurrency**: Keep `maxConcurrent` at 5-10 for stability
3. **Monitor logs**: Enable logging during initial setup
4. **Handle privacy**: Accept that some profile pictures won't be available
5. **Regular refreshes**: 7-day refresh interval is recommended