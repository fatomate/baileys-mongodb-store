# Label Association Processing Fix

## Problem
Label associations were being lost during batch processing - only 76 out of 109 labels from Redis queue were being captured in MongoDB.

## Root Cause
Race condition in batch processing where:
1. Items were added to `labelAssociationBatch.items` array
2. Timer was scheduled for batch processing
3. If batch reached BATCH_SIZE, immediate processing was triggered
4. Timer could still trigger duplicate processing
5. Items could be lost between addition and processing

## Solution Implemented

### 1. Promise-Based Queue System
- Each `upsertLabelAssociation` now returns a Promise that resolves when processed
- Promises are tracked alongside items in the batch
- Ensures every item gets proper resolution/rejection

### 2. Atomic Batch Operations
```typescript
// Before: Items could be lost between checks
const itemsToProcess = [...labelAssociationBatch.items]
labelAssociationBatch.items = []

// After: Atomic extraction
const itemsToProcess = labelAssociationBatch.items.splice(0)
const pendingPromises = labelAssociationBatch.pendingPromises?.splice(0, itemsToProcess.length) || []
```

### 3. Enhanced Tracking
Added tracking for:
- `totalReceived`: Total items received
- `totalProcessed`: Total items successfully processed
- `currentQueueSize`: Items currently in queue
- `isProcessing`: Processing status

### 4. Forced Flush Mechanism
New `flushLabelAssociations()` method that:
- Cancels pending timers
- Waits for current processing to complete
- Processes all remaining items
- Ensures no items are left behind

### 5. Automatic Backlog Detection
```typescript
if (stats.labelStats.currentQueueSize > BATCH_SIZE * 2) {
    console.log('[Label Event] Queue backlog detected, forcing flush')
    store.flushLabelAssociations()
}
```

### 6. Detailed Logging
Added comprehensive logging to track:
- When items are added to queue
- Queue size at each operation
- Processing status
- Total received vs processed

## Key Changes

### makeMongoDBStore.ts
- Enhanced `BatchAccumulator` interface with tracking fields
- Modified `processBatchedLabelAssociations()` for atomic operations
- Updated `scheduleLabelBatch()` to prevent duplicate scheduling
- Converted `upsertLabelAssociation()` to return Promise
- Added `flushLabelAssociations()` method
- Enhanced `getPerformanceStats()` with label statistics
- Added periodic flush checks in event handler

### types.d.ts
- Added `flushLabelAssociations()` method signature
- Enhanced `getPerformanceStats()` return type with labelStats

## Testing
Created `test-label-associations.js` to verify:
- All 109 associations are processed correctly
- No items are lost during rapid insertion
- Flush mechanism works properly
- Statistics accurately track progress

## Result
The fix ensures that all label associations are captured correctly with no data loss, even under high concurrent load.