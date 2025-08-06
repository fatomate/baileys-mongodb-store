# Redis/Bull Queue Integration for Baileys MongoDB Store

## Overview

This implementation provides a hybrid approach combining Redis/Bull queue for robust event processing with fallback to in-memory processing when Redis is unavailable. This ensures both high reliability when infrastructure is available and graceful degradation when it's not.

## Features

### 🚀 Key Improvements

1. **Persistent Queue Storage**: Events are stored in Redis, preventing loss during crashes
2. **Distributed Processing**: Multiple instances can share the workload
3. **Automatic Retries**: Failed jobs retry with exponential backoff
4. **Dead Letter Queue**: Failed jobs are tracked for debugging
5. **Graceful Fallback**: Automatically falls back to in-memory processing if Redis unavailable
6. **Performance Monitoring**: Built-in statistics for queue health monitoring
7. **Memory Protection**: Prevents memory leaks from event accumulation

## Configuration

### Basic Setup

```javascript
const store = await makeMongoDBStore({
    uri: 'mongodb://localhost:27017',
    database: 'whatsapp_store',
    instanceId: 'instance_001',
    
    // Add Redis configuration
    redis: {
        connection: 'redis://localhost:6379',
        enableLabelQueue: true,
        enableMessageQueue: false,
        concurrency: 50
    }
})
```

### Advanced Configuration

```javascript
const store = await makeMongoDBStore({
    uri: 'mongodb://localhost:27017',
    database: 'whatsapp_store',
    instanceId: 'instance_001',
    
    redis: {
        // Connection options
        connection: {
            host: 'redis.example.com',
            port: 6379,
            password: 'your-password',
            db: 0,
            tls: {}, // For TLS connections
            retryStrategy: (times) => Math.min(times * 50, 2000)
        },
        
        // Queue configuration
        queuePrefix: 'baileys',           // Prefix for queue names
        enableLabelQueue: true,           // Use Bull for labels (default: true)
        enableMessageQueue: false,        // Use Bull for messages (default: false)
        concurrency: 50,                  // Max concurrent jobs (default: 50)
        removeOnComplete: 3600,           // Remove completed jobs after 1 hour
        removeOnFail: 86400              // Remove failed jobs after 24 hours
    }
})
```

## How It Works

### Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────┐
│   Baileys   │────▶│ MongoDB Store│────▶│  Redis   │
│   Events    │     │   (Router)   │     │  Queue   │
└─────────────┘     └──────────────┘     └──────────┘
                            │                   │
                            ▼                   ▼
                    ┌──────────────┐     ┌──────────┐
                    │  In-Memory   │     │  Bull    │
                    │   Fallback   │     │  Worker  │
                    └──────────────┘     └──────────┘
                            │                   │
                            └───────┬───────────┘
                                    ▼
                            ┌──────────────┐
                            │   MongoDB    │
                            │  Collections │
                            └──────────────┘
```

### Event Flow

1. **Event Received**: Baileys emits an event (e.g., label association)
2. **Queue Decision**: 
   - If Redis/Bull configured and connected → Queue to Bull
   - If Redis unavailable or not configured → Use in-memory batch
3. **Processing**:
   - **Bull**: Worker processes jobs with retries and error handling
   - **In-Memory**: Batch processor handles after timeout or batch full
4. **Storage**: Data persisted to MongoDB

### Fallback Behavior

The system automatically falls back to in-memory processing when:
- Redis connection fails during initialization
- Redis becomes unavailable during runtime
- No Redis configuration provided

```javascript
// Check current mode
const stats = store.getPerformanceStats()
console.log('Using Bull:', stats.bullStats.initialized)
console.log('Redis connected:', stats.bullStats.redisConnected)
```

## Benefits Over Pure In-Memory

| Feature | In-Memory Only | With Redis/Bull |
|---------|---------------|-----------------|
| Event Persistence | ❌ Lost on crash | ✅ Persisted in Redis |
| Retry Logic | ❌ Manual | ✅ Automatic with backoff |
| Distributed Processing | ❌ Single instance | ✅ Multiple workers |
| Memory Management | ⚠️ Can grow unbounded | ✅ Controlled by Redis |
| Monitoring | ⚠️ Basic stats | ✅ Full queue metrics |
| Error Recovery | ❌ Events lost | ✅ Dead letter queue |
| Scalability | ⚠️ Limited | ✅ Highly scalable |

## Performance Considerations

### Recommended Settings

**For High Volume (>1000 events/minute):**
```javascript
redis: {
    connection: 'redis://localhost:6379',
    enableLabelQueue: true,
    enableMessageQueue: true,  // Also enable for messages
    concurrency: 100,           // Higher concurrency
    removeOnComplete: 300,      // Clean up faster
}
```

**For Low Volume (<100 events/minute):**
```javascript
redis: {
    connection: 'redis://localhost:6379',
    enableLabelQueue: true,
    enableMessageQueue: false,  // Keep messages in-memory
    concurrency: 20,            // Lower concurrency
    removeOnComplete: 3600,     // Keep longer for debugging
}
```

### Memory Usage

- **With Redis**: Memory usage is controlled and predictable
- **Without Redis**: Memory grows with event volume, cleared on batch processing

### Latency

- **With Redis**: Slight increase (~5-10ms) but guaranteed delivery
- **Without Redis**: Lower latency but risk of event loss

## Monitoring

### Real-time Statistics

```javascript
const stats = store.getPerformanceStats()

console.log({
    // Processing metrics
    messagesProcessed: stats.messagesProcessed,
    labelsProcessed: stats.labelsProcessed,
    errors: stats.errors,
    
    // Queue status
    bullInitialized: stats.bullStats.initialized,
    redisConnected: stats.bullStats.redisConnected,
    
    // Label queue details
    labelsReceived: stats.labelStats.totalReceived,
    labelsProcessed: stats.labelStats.totalProcessed,
    labelsQueued: stats.labelStats.currentQueueSize
})
```

### Health Checks

```javascript
// Monitor queue health
setInterval(async () => {
    const stats = store.getPerformanceStats()
    
    if (stats.bullStats?.initialized) {
        // Check for processing delays
        const pending = stats.labelStats.totalReceived - stats.labelStats.totalProcessed
        if (pending > 1000) {
            console.warn('Queue backlog detected:', pending)
        }
        
        // Check error rate
        const errorRate = stats.errors / stats.labelsProcessed
        if (errorRate > 0.01) {
            console.warn('High error rate:', errorRate)
        }
    }
}, 30000)
```

## Migration Guide

### From Pure In-Memory Store

1. **Install Dependencies**:
```bash
npm install bullmq ioredis
```

2. **Update Configuration**:
```javascript
// Before
const store = await makeMongoDBStore({
    uri: 'mongodb://localhost:27017',
    database: 'whatsapp_store',
    instanceId: 'instance_001'
})

// After
const store = await makeMongoDBStore({
    uri: 'mongodb://localhost:27017',
    database: 'whatsapp_store',
    instanceId: 'instance_001',
    redis: {
        connection: 'redis://localhost:6379',
        enableLabelQueue: true
    }
})
```

3. **No Code Changes Required**: The store API remains the same

### Testing Migration

Use the provided test file to verify the integration:

```bash
node test-redis-integration.js
```

## Troubleshooting

### Redis Connection Issues

```javascript
// The store will log and fall back to in-memory
// ❌ Failed to initialize Bull queues for instance instance_001: [Error]
// ⚠️ Falling back to in-memory queue processing
```

### Queue Cleanup

```javascript
// Force flush all pending items
await store.flushLabelAssociations()

// Graceful shutdown
await store.close()
```

### Debug Mode

Check queue status in Redis:
```bash
redis-cli
> KEYS baileys:*
> LLEN bull:baileys:labels:instance_001:wait
```

## Best Practices

1. **Always Configure Graceful Shutdown**:
```javascript
process.on('SIGINT', async () => {
    await store.flushLabelAssociations()
    await store.close()
    process.exit(0)
})
```

2. **Monitor Queue Health**: Set up alerts for queue backlogs
3. **Use Appropriate Concurrency**: Match to your MongoDB capacity
4. **Regular Cleanup**: Configure appropriate TTLs for completed jobs
5. **Test Fallback**: Ensure your app works without Redis

## FAQ

**Q: What happens if Redis goes down during operation?**
A: The store automatically falls back to in-memory processing. Events may be delayed but won't be lost if MongoDB is available.

**Q: Can multiple instances share the same Redis?**
A: Yes! Each instance gets its own queue namespace. They can share Redis infrastructure.

**Q: How much Redis memory is needed?**
A: Approximately 1KB per queued event. 1GB can handle ~1 million pending events.

**Q: Is this backward compatible?**
A: Yes, 100%. If you don't configure Redis, it works exactly as before.

## Support

For issues or questions about the Redis/Bull integration, please open an issue on the repository.