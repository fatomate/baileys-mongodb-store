# MongoDB Store (@baileys/mongodb-store) Issues Report

## ✅ UPDATE: All Issues Fixed in v2.9.1

**Status**: All critical issues have been resolved in version 2.9.1 (Released: 2025-08-27)

### Fixed Issues Summary:
- ✅ EventEmitter memory leak - Fixed with proper listener limits and cleanup
- ✅ Duplicate binding protection - Added isBound flag
- ✅ Bull queue worker event handlers - Verified and working correctly
- ✅ Profile picture socket initialization - Added setSock() method
- ✅ Redis connection listener limits - Set to reasonable limit (30)
- ✅ Connection state cleanup - Enhanced close() method
- ✅ New API methods - Added isHealthy(), rebind(), and reconnect()

---

## Original Issues (Now Fixed)

### 1. ✅ FIXED: EventEmitter Memory Leak - connectionStateEmitter
**Location**: `makeEnhancedMongoDBStore.js` line 349, 471-472
**Issue**: Multiple event listeners accumulate on `connectionStateEmitter` during reconnection attempts
```javascript
// Line 349
const connectionStateEmitter = new events_1.EventEmitter();

// Lines 471-472 (inside ensureConnection())
connectionStateEmitter.once('connected', onConnected);
connectionStateEmitter.once('failed', onFailed);
```
**Problem**: 
- Each reconnection attempt adds new listeners
- No cleanup of previous listeners if connection times out differently than expected
- Default EventEmitter limit is 10, causing warnings at 11+ listeners

**Required Fix**:
```javascript
// Add before line 471
connectionStateEmitter.removeAllListeners('connected');
connectionStateEmitter.removeAllListeners('failed');

// Or set higher limit at line 349
connectionStateEmitter.setMaxListeners(50);
```

### 2. ✅ FIXED: Missing Duplicate Binding Protection in store.bind()
**Location**: `makeEnhancedMongoDBStore.js` line 2861
**Issue**: The `bind()` method has NO protection against multiple bindings
```javascript
bind(ev) {
    log(`[${instanceId}] store.bind() called - setting up event listeners with selective storage`);
    // Immediately starts adding 19+ event listeners without checking
    ev.on('connection.update', async (update) => {
```
**Problem**: Multiple calls to `bind()` create duplicate event handlers

**Required Fix**:
```javascript
// Add binding state tracking
let isBound = false;
bind(ev) {
    if (isBound) {
        log(`[${instanceId}] store.bind() already called, skipping duplicate binding`);
        return;
    }
    isBound = true;
    log(`[${instanceId}] store.bind() called - setting up event listeners with selective storage`);
    // ... rest of bind implementation
}

// Or in close() method, add:
isBound = false;
```

### 3. ✅ FIXED: Bull Queue Worker Event Handler Syntax Error
**Location**: `makeEnhancedMongoDBStore.js` lines 1066-1072
**Issue**: Malformed event handler attachment syntax
```javascript
// Current broken code:
worker.on('completed', completedHandler);
worker.on('failed', failedHandler);
worker.on('stalled', stalledHandler)(worker).__eventHandlers = {
    completed: completedHandler,
    failed: failedHandler,
    stalled: stalledHandler
};
```
**Problem**: The syntax `stalledHandler)(worker).__eventHandlers` is invalid JavaScript

**Required Fix**:
```javascript
worker.on('completed', completedHandler);
worker.on('failed', failedHandler);
worker.on('stalled', stalledHandler);

// Store handlers for cleanup
worker.__eventHandlers = {
    completed: completedHandler,
    failed: failedHandler,
    stalled: stalledHandler
};
```

### 4. ✅ FIXED: Redis Connection Listener Limit
**Location**: `makeEnhancedMongoDBStore.js` line 1016
**Issue**: Setting unlimited listeners on Redis connection
```javascript
redisConnection.setMaxListeners(0);
```
**Problem**: This allows unlimited listeners, masking potential memory leaks

**Recommended Fix**:
```javascript
redisConnection.setMaxListeners(30); // Set reasonable limit
```

### 5. ✅ FIXED: Missing Socket Reference Update Method
**Location**: Store creation requires `sock` parameter at initialization
**Issue**: The store requires the socket for profile picture retrieval, but socket isn't available until after store creation

**Required Enhancement**:
```javascript
// Add method to update socket after store creation
updateSocket(newSocket) {
    this.sock = newSocket;
    log(`[${instanceId}] Socket reference updated in store`);
}

// Or make sock optional in config and add setSock method
setSock(socket) {
    if (!this.sock) {
        this.sock = socket;
        // Re-initialize components that need socket
        if (this.profilePictureConfig?.enabled) {
            this.initializeProfilePictureRetrieval();
        }
    }
}
```

### 6. ✅ FIXED: Connection State Not Properly Reset on Close
**Location**: `makeEnhancedMongoDBStore.js` line 3897
**Issue**: `mongoConnectionState` not fully reset, `connectionStateEmitter` listeners not cleaned
```javascript
mongoConnectionState = MongoConnectionState.DISCONNECTED;
reconnectAttempts = 0;
// Missing: connectionStateEmitter cleanup
```

**Required Fix**:
```javascript
async close() {
    // ... existing cleanup code ...
    
    // Add EventEmitter cleanup
    connectionStateEmitter.removeAllListeners();
    
    mongoConnectionState = MongoConnectionState.DISCONNECTED;
    reconnectAttempts = 0;
}
```

## Recommended Store API Enhancements

### 1. Add Store Health Check Method
```javascript
async isHealthy() {
    try {
        if (mongoConnectionState !== MongoConnectionState.CONNECTED) {
            return false;
        }
        await db.admin().ping();
        return true;
    } catch (error) {
        return false;
    }
}
```

### 2. Add Safe Rebind Method
```javascript
rebind(ev) {
    // Unbind existing listeners first
    if (this.boundEventEmitter) {
        this.unbindAll();
    }
    this.bind(ev);
}

unbindAll() {
    if (this.boundEventEmitter) {
        // Remove all listeners added by this store
        this.eventHandlers.forEach((handler, event) => {
            this.boundEventEmitter.off(event, handler);
        });
        this.eventHandlers.clear();
        this.boundEventEmitter = null;
    }
}
```

### 3. Add Connection Recovery Method
```javascript
async reconnect() {
    if (mongoConnectionState === MongoConnectionState.CONNECTING || 
        mongoConnectionState === MongoConnectionState.RECONNECTING) {
        // Already reconnecting
        return new Promise((resolve, reject) => {
            connectionStateEmitter.once('connected', resolve);
            connectionStateEmitter.once('failed', reject);
        });
    }
    
    await this.ensureConnection();
}
```

## Summary for Repository Developer

### Priority 1 (Critical - Causing Production Issues):
1. Fix EventEmitter memory leak in `connectionStateEmitter`
2. Add duplicate binding protection to `store.bind()`
3. Fix Bull queue worker event handler syntax error

### Priority 2 (Important - Architectural Issues):
1. Add method to update socket reference after store creation
2. Properly cleanup connectionStateEmitter on store close
3. Add store health check method

### Priority 3 (Nice to Have - Improvements):
1. Add safe rebind method
2. Add connection recovery method
3. Set reasonable Redis connection listener limits

## Temporary Workarounds (Until Fixes Are Released)

### For EventEmitter Memory Leak:
Increase Node.js max listeners globally in your application:
```javascript
require('events').EventEmitter.defaultMaxListeners = 50;
```

### For Duplicate Binding:
Track binding state in your application:
```javascript
if (!store[instance_id]._isBound) {
    store[instance_id].bind(WA.ev);
    store[instance_id]._isBound = true;
}
```

### For Socket Initialization Order:
Create store with null socket, then update:
```javascript
// Create store without socket
storeConfig.sock = null;
store[instance_id] = await makeEnhancedMongoDBStore(storeConfig);

// Create socket
const WA = makeWASocket(socketConfig);

// Use the new setSock method (available in v2.9.1)
store[instance_id].setSock(WA);
```

---

## ✅ Fixed in Version 2.9.1

### Implemented Solutions

#### 1. EventEmitter Memory Leak Prevention
- Set max listeners to 50 on connectionStateEmitter
- Clean up existing listeners before adding new ones
- Full cleanup in close() method

#### 2. Duplicate Binding Protection
- Added internal `isBound` flag tracking
- bind() method safely ignores duplicate calls
- Flag resets on close()

#### 3. Socket Management Enhancement
- New `setSock()` method allows updating socket after store creation
- Socket is now optional in initial configuration
- Profile picture functionality works with late socket binding

#### 4. Connection Management Improvements
- Redis connection listeners limited to 30 (instead of unlimited)
- Enhanced close() method with full cleanup
- Connection state properly reset

### New API Methods (v2.9.1)

#### setSock(socket)
Update socket reference after store creation:
```javascript
const store = await makeEnhancedMongoDBStore(config);
const sock = makeWASocket(socketConfig);
store.setSock(sock);
```

#### isHealthy()
Check store and database connection health:
```javascript
const healthy = await store.isHealthy();
console.log('Store health:', healthy);
```

#### rebind(ev)
Safe rebinding that prevents duplicate listeners:
```javascript
store.rebind(sock.ev); // Automatically handles unbinding
```

#### reconnect()
Force reconnection to MongoDB:
```javascript
await store.reconnect();
```

### Migration Guide to v2.9.1

1. **Update package version**:
   ```bash
   npm install @baileys/mongodb-store@2.9.1
   ```

2. **Use new socket initialization pattern**:
   ```javascript
   // Create store without socket
   const storeConfig = {
       uri: 'mongodb://localhost:27017',
       database: 'whatsapp',
       // sock is optional now
   };
   
   const store = await makeEnhancedMongoDBStore(storeConfig);
   
   // Create socket
   const sock = makeWASocket({...});
   
   // Update store with socket
   store.setSock(sock);
   
   // Bind events (duplicate calls are safe now)
   store.bind(sock.ev);
   ```

3. **Health monitoring**:
   ```javascript
   // Periodically check store health
   setInterval(async () => {
       const healthy = await store.isHealthy();
       if (!healthy) {
           await store.reconnect();
       }
   }, 60000);
   ```

### Changelog v2.9.1
- Fixed EventEmitter memory leak in connectionStateEmitter
- Added duplicate binding protection
- Implemented setSock() method for flexible socket management  
- Set reasonable Redis connection listener limits
- Enhanced close() method with complete cleanup
- Added isHealthy(), rebind(), and reconnect() methods
- Improved profile picture handling with late socket binding
- Full backward compatibility maintained