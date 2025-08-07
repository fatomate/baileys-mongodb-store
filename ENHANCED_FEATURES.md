# Enhanced MongoDB Store Features

## Overview

The enhanced MongoDB store provides flexible control over which WhatsApp events are stored and how long they are retained. This allows for optimized storage usage, compliance with data retention policies, and improved performance.

## Key Features

### 1. Selective Event Storage

Control which Baileys events are stored in MongoDB:

```typescript
const config: EnhancedMongoDBStoreConfig = {
    // ... other config
    storeAllByDefault: false, // Only store explicitly enabled events
    events: {
        'messages.upsert': { enabled: true },
        'chats.upsert': { enabled: true },
        'presence.update': { enabled: false } // Skip presence updates
    }
}
```

### 2. Per-Event TTL Configuration

Set different retention periods for different event types:

```typescript
events: {
    'messages.upsert': { 
        enabled: true, 
        ttlDays: 7  // Keep messages for 7 days
    },
    'contacts.upsert': { 
        enabled: true, 
        ttlDays: 365 // Keep contacts for 1 year
    },
    'labels.edit': { 
        enabled: true, 
        ttlDays: 0   // Keep labels forever (0 = no expiration)
    }
}
```

### 3. Per-Collection TTL

Configure TTL at the collection level:

```typescript
collectionTTL: {
    messages: 7,        // 7 days for messages
    chats: 90,         // 90 days for chats
    contacts: 365,     // 1 year for contacts
    presences: 1,      // 1 day for presence data
    groupMetadata: 180 // 6 months for group metadata
}
```

### 4. Event Filtering

Filter events before storage using custom logic:

```typescript
events: {
    'messages.upsert': {
        enabled: true,
        filter: (data) => {
            // Only store non-ephemeral messages
            return data.messages?.some(msg => 
                !msg.message?.ephemeralMessage
            )
        }
    },
    'contacts.upsert': {
        enabled: true,
        filter: (contacts) => {
            // Skip broadcast lists
            return contacts.filter(c => 
                !c.id?.includes('@broadcast')
            ).length > 0
        }
    }
}
```

### 5. Data Transformation

Transform data before storing:

```typescript
events: {
    'messages.upsert': {
        enabled: true,
        transform: (data) => {
            // Remove sensitive fields
            const transformed = { ...data }
            if (transformed.messages) {
                transformed.messages = transformed.messages.map(msg => {
                    const { mediaKey, ...rest } = msg
                    return rest
                })
            }
            return transformed
        }
    }
}
```

### 6. Hooks System

Add pre and post-processing hooks:

```typescript
hooks: {
    beforeStore: async (eventType, data) => {
        console.log(`Processing ${eventType}`)
        // Return false to skip storing
        return true
    },
    
    afterStore: async (eventType, data) => {
        // Send to analytics, webhooks, etc.
        console.log(`Stored ${eventType}`)
    },
    
    onError: (eventType, error, data) => {
        console.error(`Error in ${eventType}:`, error)
    }
}
```

### 7. Runtime Configuration

Update event configuration at runtime:

```typescript
// Disable an event type
store.updateEventConfig('presence.update', { enabled: false })

// Change TTL
store.updateEventConfig('messages.upsert', { ttlDays: 14 })

// Add a filter
store.updateEventConfig('chats.update', {
    filter: (updates) => updates.some(u => !u.mute)
})
```

### 8. Event Metrics

Track event processing statistics:

```typescript
// Enable metrics
const config = {
    // ... other config
    enableMetrics: true
}

// Get metrics
const metrics = store.getEventMetrics('messages.upsert')
// Returns: { 
//   eventType, totalReceived, totalStored, 
//   totalSkipped, totalErrors, lastProcessedAt 
// }
```

## Supported Event Types

All Baileys events are supported:

- `connection.update`
- `messaging-history.set`
- `contacts.upsert`
- `contacts.update`
- `chats.upsert`
- `chats.update`
- `chats.delete`
- `labels.edit`
- `labels.association`
- `presence.update`
- `messages.upsert`
- `messages.update`
- `messages.delete`
- `groups.update`
- `groups.upsert`
- `group-participants.update`
- `message-receipt.update`
- `messages.reaction`

## Use Cases

### 1. Minimal Storage (Cost Optimization)

Store only essential data with short retention:

```typescript
{
    storeAllByDefault: false,
    ttlDays: 3,
    events: {
        'connection.update': { enabled: true, ttlDays: 1 },
        'messages.upsert': { 
            enabled: true, 
            ttlDays: 3,
            filter: (data) => {
                // Only text messages
                return data.messages?.some(msg => 
                    msg.message?.conversation
                )
            }
        }
    }
}
```

### 2. Compliance-Focused (GDPR)

Implement data retention policies:

```typescript
{
    collectionTTL: {
        messages: 30,      // 30-day retention
        contacts: 365,     // 1-year retention
        presences: 1       // 24-hour retention
    },
    events: {
        'messages.upsert': {
            enabled: true,
            ttlDays: 30,
            transform: (data) => {
                // Anonymize sensitive data
                return anonymizeData(data)
            }
        }
    }
}
```

### 3. Analytics-Optimized

Store events for analysis with appropriate retention:

```typescript
{
    enableMetrics: true,
    events: {
        'messages.upsert': { enabled: true, ttlDays: 90 },
        'message-receipt.update': { enabled: true, ttlDays: 30 },
        'groups.update': { enabled: true, ttlDays: 180 },
        'presence.update': { enabled: false } // Skip high-volume, low-value data
    }
}
```

### 4. Development/Testing

Full storage with short retention:

```typescript
{
    storeAllByDefault: true,
    ttlDays: 1,
    logLevel: 'all',
    enableMetrics: true
}
```

## Migration from Standard Store

To migrate from the standard `makeMongoDBStore` to `makeEnhancedMongoDBStore`:

1. Update imports:
```typescript
// Before
import { makeMongoDBStore } from 'baileys-mongodb-store'

// After
import { makeEnhancedMongoDBStore } from 'baileys-mongodb-store'
```

2. Update configuration:
```typescript
// Before
const config = {
    uri: 'mongodb://localhost:27017',
    database: 'whatsapp',
    instanceId: 'instance_001',
    ttlDays: 30
}

// After - backward compatible
const config = {
    uri: 'mongodb://localhost:27017',
    database: 'whatsapp',
    instanceId: 'instance_001',
    ttlDays: 30,
    storeAllByDefault: true // Maintains same behavior
}
```

3. Add event-specific configurations as needed:
```typescript
const config = {
    // ... existing config
    events: {
        'presence.update': { enabled: false },
        'messages.upsert': { ttlDays: 7 }
    }
}
```

## Performance Considerations

1. **Filtering**: Apply filters to reduce storage volume
2. **Transformation**: Remove unnecessary fields to save space
3. **TTL**: Use appropriate TTL values to automatically clean old data
4. **Batch Processing**: Enable for high-volume events like messages
5. **Selective Storage**: Disable unnecessary event types

## Best Practices

1. **Start Conservative**: Begin with minimal storage and add events as needed
2. **Monitor Metrics**: Use metrics to understand storage patterns
3. **Test Filters**: Ensure filters don't accidentally exclude important data
4. **Document Configuration**: Keep configuration documented for team reference
5. **Regular Review**: Periodically review and adjust TTL values
6. **Use Hooks Wisely**: Avoid heavy processing in hooks to maintain performance