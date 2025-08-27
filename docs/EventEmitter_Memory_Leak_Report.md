# EventEmitter Memory Leak Bug Report - MongoDB Store Bull Queue Implementation

## Executive Summary
A memory leak warning is triggered when initializing multiple WhatsApp instances using the `@baileys/mongodb-store` package. The issue stems from Bull queues adding excessive event listeners to shared Redis connections, exceeding Node.js's default MaxListeners limit of 10.

## Issue Details

### Error Message
```
(node:3627757) MaxListenersExceededWarning: Possible EventEmitter memory leak detected. 
11 connected listeners added to [EventEmitter]. MaxListeners is 10. 
11 failed listeners added to [EventEmitter].
```

### Environment
- **Node.js Version**: Not specified (using PM2)
- **Package**: `@baileys/mongodb-store`
- **Related Package**: `baileys` (WhatsApp Web API)
- **Queue System**: Bull (Redis-based queue)
- **Database**: MongoDB
- **Cache**: Redis (ioredis)

## Root Cause Analysis

### 1. Queue Creation Pattern
Each WhatsApp instance initialization creates 9 separate Bull queues:
- `messages` queue (concurrency: 5)
- `chats` queue (concurrency: 5)
- `contacts` queue (concurrency: 5)
- `group-metadata` queue (concurrency: 5)
- `presences` queue (concurrency: 5)
- `state` queue (concurrency: 5)
- `labels` queue (concurrency: 5)
- `label-associations` queue (concurrency: 1)
- `profile-pictures` queue (concurrency: 1)

### 2. Event Listener Accumulation
Each Bull queue instance adds at least 2 event listeners to the Redis client:
- `connected` event listener
- `failed` event listener

With 9 queues × 2 listeners = 18 listeners per instance minimum.

### 3. Shared Redis Connection
All queues appear to share the same Redis connection/EventEmitter, as evidenced by:
- The warning appears after creating just 1-2 instances
- The warning specifically mentions 11 listeners (exceeding the limit of 10)
- Multiple instances compound the problem

### 4. No Cleanup Mechanism
When stores are recreated or instances are reinitialized:
- Old Bull queue listeners remain attached
- No evidence of proper queue termination/cleanup
- Listeners accumulate over time

## Evidence from Debug Logs

### Instance 1 (66639568C68AF)
```
Line 75: 🐂 Initializing Bull queues for instance 66639568C68AF...
Lines 77-86: [9 queues created]
Line 89-91: MaxListenersExceededWarning appears immediately after
```

### Instance 2 (6860DCA0E2819)
```
Line 133: 🐂 Initializing Bull queues for instance 6860DCA0E2819...
Lines 135-144: [9 queues created]
Lines 148-149: MaxListenersExceededWarning appears again
```

### Instance 3 (672874B2A7484)
```
Line 199: 🐂 Initializing Bull queues for instance 672874B2A7484...
Lines 201-210: [9 queues created]
Lines 213-214: MaxListenersExceededWarning appears again
```

## Impact

### Performance Implications
1. **Memory Usage**: Each listener holds references preventing garbage collection
2. **Event Processing**: All listeners execute on Redis events, causing unnecessary overhead
3. **Debugging Difficulty**: Warnings clutter logs and may mask other issues

### Scalability Issues
- Problem worsens with more instances
- Each instance adds ~18 listeners minimum
- System becomes unstable with many concurrent instances

## Reproduction Steps

1. Initialize multiple WhatsApp instances using `makeEnhancedMongoDBStore`
2. Each instance creates a store with the following config:
```javascript
{
  uri: 'mongodb://...',
  database: 'wabotdev',
  instanceId: 'instance_id',
  redis: {
    connection: redisConfig,
    queuePrefix: 'wabotdev_store',
    concurrency: 5
  }
}
```
3. After 1-2 instances, MaxListenersExceededWarning appears
4. Warning repeats for each new instance

## Proposed Solutions

### Solution 1: Increase MaxListeners Limit
Configure Redis client with higher limit before passing to store:
```javascript
const redisConnection = new Redis(config);
redisConnection.setMaxListeners(100); // or 0 for unlimited
```

### Solution 2: Queue Connection Pooling
Implement connection pooling for Bull queues:
```javascript
class QueueConnectionPool {
  constructor(redisConfig, maxListeners = 100) {
    this.connection = new Redis(redisConfig);
    this.connection.setMaxListeners(maxListeners);
  }
  
  getConnection() {
    return this.connection.duplicate();
  }
}
```

### Solution 3: Proper Cleanup Implementation
Add cleanup method to properly terminate queues:
```javascript
async cleanupQueues() {
  for (const queue of this.queues) {
    await queue.close();
    queue.removeAllListeners();
  }
}
```

### Solution 4: Singleton Redis Connection
Use a single Redis connection for all queues per instance:
```javascript
// Instead of each queue creating its own connection
const sharedRedis = new Redis(config);
sharedRedis.setMaxListeners(0); // Unlimited

// Pass shared connection to all queues
const messageQueue = new Queue('messages', { connection: sharedRedis });
const chatsQueue = new Queue('chats', { connection: sharedRedis });
// etc...
```

## Recommendations

### Immediate Fix (Workaround)
Add to store initialization:
```javascript
// In makeEnhancedMongoDBStore
if (redis.connection && redis.connection.setMaxListeners) {
  redis.connection.setMaxListeners(Math.max(50, instanceCount * 20));
}
```

### Long-term Fix
1. **Refactor Queue Architecture**: Consider using a single queue with different job types instead of 9 separate queues
2. **Implement Proper Lifecycle Management**: Ensure all queues are properly closed and listeners removed on cleanup
3. **Add Connection Pooling**: Implement proper connection pooling for Redis clients
4. **Add Debug Logging**: Log queue creation/destruction and listener counts for monitoring

## Additional Notes

- The issue is not related to WhatsApp socket reconnections as initially suspected
- The debug logs show `WA.ev` reports "No event emitter found", suggesting the Baileys library might use a different event system
- The warning appears consistently after Bull queue initialization, confirming the root cause
- Redis eviction policy warning (`allkeys-lru`) suggests suboptimal Redis configuration for Bull queues

## Debug Log Reference

Full debug log available at: `/home/wabotdev/api-wabot-dev/public_html/waziper/reference/eventemitters-log-issue.txt`

Key indicators in log:
- Warning appears at lines: 89-91, 148-149, 213-214
- Pattern: Warning always follows "Bull queues initialized successfully" message
- No WhatsApp socket reconnection issues detected (all show "Call #1")

## References
- [Node.js EventEmitter Documentation](https://nodejs.org/api/events.html#emittersetmaxlistenersn)
- [Bull Queue Documentation](https://github.com/OptimalBits/bull)
- [Redis Connection Pooling Best Practices](https://redis.io/docs/manual/clients/#connection-pooling)
- [@baileys/mongodb-store Repository](https://github.com/baileys/mongodb-store)

---

**Report Generated**: 2024-08-27  
**Severity**: Medium (Performance impact, not a breaking issue)  
**Priority**: High (Affects all production instances)  
**Discovered By**: Debug analysis of WhatsApp multi-instance initialization