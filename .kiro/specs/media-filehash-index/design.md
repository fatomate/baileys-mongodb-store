# Design Document: Media FileHash Index

## Overview

This feature adds a new MongoDB index on `mediaInfo.fileHash` for the messages collection to optimize media deduplication queries. The implementation follows the existing index management patterns established in the codebase, using the `IndexSpec` interface and smart index creation utilities.

The index addresses a performance issue identified by MongoDB Atlas where media deduplication queries scan 1.6M documents per query, running 1473 times/hour with 2253ms average execution time.

## Architecture

The implementation integrates with the existing index management system:

```
┌─────────────────────────────────────────────────────────────┐
│                    Store Initialization                      │
├─────────────────────────────────────────────────────────────┤
│  1. Define indexDefinitions (includes new fileHash index)   │
│  2. Call shouldCreateIndexes() for each collection          │
│  3. Call batchCreateIndexes() for missing indexes           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Index Helper Utilities                     │
├─────────────────────────────────────────────────────────────┤
│  - shouldCreateIndexes(): Check existing vs required        │
│  - batchCreateIndexes(): Create missing indexes safely      │
│  - safeCreateIndex(): Handle errors and retries             │
└─────────────────────────────────────────────────────────────┘
```

## Components and Interfaces

### Index Definition

The new index will be added to the `indexDefinitions` object in both store files:

```typescript
// In messages array within indexDefinitions
{ 
    name: 'messages_media_fileHash', 
    spec: { 'mediaInfo.fileHash': 1 }, 
    options: { sparse: true } 
}
```

### Affected Files

1. **src/makeMongoDBStore.ts** - Add index to `indexDefinitions.messages` array
2. **src/makeEnhancedMongoDBStore.ts** - Add index to `indexDefinitions.messages` array

### Existing Utilities Used

- `IndexSpec` interface from `src/utils/collectionHelper.ts`
- `shouldCreateIndexes()` from `src/utils/collectionHelper.ts`
- `batchCreateIndexes()` from `src/utils/indexHelper.ts`
- `safeCreateIndex()` from `src/utils/indexHelper.ts`

## Data Models

### Index Specification

```typescript
interface IndexSpec {
    name: string      // 'messages_media_fileHash'
    spec: any         // { 'mediaInfo.fileHash': 1 }
    options?: any     // { sparse: true }
}
```

### Message Document Structure (relevant fields)

```typescript
interface MessageDocument {
    instanceId: string
    jid: string
    key: {
        id: string
        remoteJid?: string
        fromMe?: boolean
        // ...
    }
    mediaInfo?: {
        fileHash?: string  // The field being indexed
        // ...
    }
    // ...
}
```

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Index Creation Idempotence

*For any* MongoDB store instance, initializing the store multiple times with the same configuration should result in exactly one `messages_media_fileHash` index existing on the messages collection.

**Validates: Requirements 2.3, 3.1**

## Error Handling

### Index Creation Failures

The existing `safeCreateIndex()` function handles various error scenarios:

1. **Index Already Exists (code 68)**: Logs info message and continues
2. **Index Options Conflict (code 85)**: Attempts to drop and recreate
3. **Index Key Specs Conflict (code 86)**: Attempts to resolve conflict
4. **Write Conflict (code 112)**: Retries with backoff
5. **Index Build Aborted (code 276)**: Retries with exponential backoff

All errors are logged but do not crash the store initialization.

### Sparse Index Behavior

The sparse index option ensures:
- Documents without `mediaInfo.fileHash` field are not included in the index
- Index size remains proportional to documents with media
- Queries on documents without the field will not use this index (expected behavior)

## Testing Strategy

### Property-Based Testing

The implementation will use **Jest** as the testing framework with property-based testing for the idempotence property.

**Property-based test requirements:**
- Each property-based test MUST run a minimum of 100 iterations
- Each test MUST be tagged with: `**Feature: media-filehash-index, Property {number}: {property_text}**`
- Tests should verify index existence and configuration after store operations

### Unit Tests

Unit tests will cover:
1. Index definition structure validation
2. Index exists after store initialization (integration test with MongoDB)
3. Index has correct sparse option configured

### Test Approach

Since this feature primarily involves configuration changes to existing index management code, testing focuses on:
1. Verifying the index definition is correctly structured
2. Integration testing that the index is created on store initialization
3. Verifying idempotent behavior when store initializes multiple times

**Note:** The existing `safeCreateIndex()` and `batchCreateIndexes()` functions are already tested. This feature adds a new index definition that uses those existing, tested utilities.
