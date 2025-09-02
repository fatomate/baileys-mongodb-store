## DROPINDEX bug — detailed resolution report

### Scope
- Files reviewed: `src/makeEnhancedMongoDBStore.ts`, `src/utils/indexHelper.ts`, `src/utils/connectionRetry.ts`, `src/utils/indexLock.ts`
- Log source: `docs/dropindex-bug.md` (focus around “IndexKeySpecsConflict”, “MongoClientClosedError”, and repeated “Created index” lines)

---

### What the log shows
- Index conflict when creating new partial unique index on `labelAssociations`:
  - **IndexKeySpecsConflict (code 86)**: an index with the same auto-generated name exists without the partial filter.
- Intermittent connection errors during index creation:
  - **MongoClientClosedError: Operation interrupted because client was closed**
  - **MongoNotConnectedError** while creating/dropping indexes (e.g., in LID handler).
- Duplicate “✅ Created index on collection wabot_labelAssociations” lines, suggesting parallel, cross-instance creation attempts.

---

### What the current code already fixes

- **Index conflicts (code 85/86) are handled and auto-resolved**
  - On conflict, the code drops the conflicting index then recreates it (including when options/partial filter differ).

```149:166:src/utils/indexHelper.ts
// Handle IndexKeySpecsConflict - index with same name but different spec
if (error.code === 86 || error.codeName === 'IndexKeySpecsConflict') {
    console.log(`⚠️ Index conflict detected on collection ${collection.collectionName}, attempting to resolve...`)
    const indexName = options?.name || Object.keys(spec).map(k => `${k}_${spec[k]}`).join('_')
    
    try {
        // First try to drop the conflicting index
        await safeDropIndex(collection, indexName)
        
        // Wait a bit for MongoDB to process the drop
        await new Promise(resolve => setTimeout(resolve, 500))
        
        // Retry creating the index
        await collection.createIndex(spec, options)
        console.log(`✅ Resolved index conflict and created index on collection ${collection.collectionName}`)
        return
    } catch (resolveError: any) {
        if (resolveError.code === 276 || resolveError.codeName === 'IndexBuildAborted') {
            console.log(`⚠️ Index build aborted during conflict resolution, will retry...`)
            // Continue to retry logic below
        } else {
            console.error(`❌ Failed to resolve index conflict on collection ${collection.collectionName}:`, resolveError)
            throw resolveError
        }
    }
}
```

- **Drop operations are robust and verify completion** (including `IndexBuildAborted` 276 retries and post-drop verification).

```24:41:src/utils/indexHelper.ts
while (verifyRetries < maxVerifyRetries) {
    try {
        const currentIndexes = await collection.indexes()
        const stillExists = currentIndexes.some(idx => idx.name === indexName)
        
        if (!stillExists) {
            // Index successfully dropped
            break
        }
        
        // Index still exists, wait and retry
        await new Promise(resolve => setTimeout(resolve, retryDelay * (verifyRetries + 1)))
        verifyRetries++
    } catch (verifyError) {
        // If we can't verify, assume it's dropped
        console.log(`⚠️ Could not verify index drop for ${indexName}, proceeding`)
        break
    }
}
```

- **Correct target index schema for `labelAssociations`**: two partial unique indexes split by `type` to avoid cross-type collisions.

```2999:3011:src/makeEnhancedMongoDBStore.ts
await safeCreateIndex(
    collections.labelAssociations,
    { instanceId: 1, type: 1, chatId: 1, labelId: 1 },
    { unique: true, partialFilterExpression: { type: 'label_jid' } }
)
await safeCreateIndex(
    collections.labelAssociations,
    { instanceId: 1, type: 1, chatId: 1, labelId: 1, messageId: 1 },
    { unique: true, partialFilterExpression: { type: 'label_message' } }
)
```

- **`MongoNotConnectedError` is retried** by the shared retry helper used via `withConnection`.

```24:50:src/utils/connectionRetry.ts
const retryableErrors = [
    'Client must be connected',
    'Topology is closed',
    'Connection pool closed',
    'ECONNREFUSED',
    'ETIMEDOUT',
    'ENETUNREACH',
    'MongoNetworkError',
    'MongoNotConnectedError',
    'MongoExpiredSessionError',
    'Cannot use a session that has ended',
    'session has ended',
    'connection timed out',
    'socket hang up'
]
...
return retryableErrors.some(msg => errorMessage.includes(msg)) ||
       error.code === 'ECONNREFUSED' ||
       error.code === 'ETIMEDOUT' ||
       error.code === 'ENETUNREACH' ||
       error.name === 'MongoNetworkError' ||
       error.name === 'MongoNotConnectedError' ||
       error.name === 'MongoExpiredSessionError'
```

- **Distributed locking already used for some collections** to prevent cross-instance races:
  - `chats`, `contacts`, `messages` use `IndexLockManager.withLock(...)`.

```2911:2918:src/makeEnhancedMongoDBStore.ts
await indexLockManager.withLock(
    `${collectionPrefix}chats`,
    async () => withConnection(async () => {
        await safeCreateIndex(collections.chats, { instanceId: 1, id: 1 }, { unique: true })
        await safeCreateIndex(collections.chats, { updatedAt: 1 }, { expireAfterSeconds: chatsTTL })
    }),
    15000 // 15 second timeout for lock acquisition
)
```

---

### Gaps that still show up in the log and how to close them

- Gap 1: MongoClientClosedError (client closed mid-operation)
  - Log: “MongoClientClosedError: Operation interrupted because client was closed”
  - Current `shouldRetry()` does not include `MongoClientClosedError`, so index operations may fail instead of retrying during pool migrations or managed client closes.
  - Fix: include both the error name and a string match (`'client was closed'`) in retry conditions.

- Gap 2: Cross-instance races on `labelAssociations`
  - You already lock `chats`, `contacts`, `messages`, but the `labelAssociations` index creation is not under `withLock`. That can cause duplicate concurrent `createIndex()` attempts (seen as repeated “✅ Created index …” lines).
  - Fix: wrap `labelAssociations` index work in `IndexLockManager.withLock(...)` (like others).

- Optional hardening:
  - Also lock `groupMetadata` and `labels` index creation to eliminate any residual index creation races there (lower risk but consistent).

---

### Recommended edits (ready to apply)

- Add retry support for MongoClientClosedError

```diff
// src/utils/connectionRetry.ts

-        const retryableErrors = [
+        const retryableErrors = [
             'Client must be connected',
             'Topology is closed',
             'Connection pool closed',
+            'client was closed', // covers “Operation interrupted because client was closed”
             'ECONNREFUSED',
             'ETIMEDOUT',
             'ENETUNREACH',
             'MongoNetworkError',
             'MongoNotConnectedError',
             'MongoExpiredSessionError',
             'Cannot use a session that has ended',
             'session has ended',
             'connection timed out',
             'socket hang up'
         ]
         
         return retryableErrors.some(msg => errorMessage.includes(msg)) ||
                error.code === 'ECONNREFUSED' ||
                error.code === 'ETIMEDOUT' ||
                error.code === 'ENETUNREACH' ||
                error.name === 'MongoNetworkError' ||
                error.name === 'MongoNotConnectedError' ||
-               error.name === 'MongoExpiredSessionError'
+               error.name === 'MongoExpiredSessionError' ||
+               error.name === 'MongoClientClosedError'
```

- Wrap `labelAssociations` index ops with a distributed lock

```ts
// src/makeEnhancedMongoDBStore.ts (inside createIndexes)

await indexLockManager.withLock(
    `${collectionPrefix}labelAssociations`,
    async () => withConnection(async () => {
        // Drop legacy indexes first
        await safeDropIndex(collections.labelAssociations, 'updatedAt_1')
        await safeDropIndex(collections.labelAssociations, 'instanceId_1_chatId_1_labelId_1')
        await safeDropIndex(collections.labelAssociations, 'instanceId_1_type_1_chatId_1_labelId_1')

        // Create partial unique index for chat-level labels (label_jid)
        await safeCreateIndex(
            collections.labelAssociations,
            { instanceId: 1, type: 1, chatId: 1, labelId: 1 },
            { unique: true, partialFilterExpression: { type: 'label_jid' } }
        )

        // Create partial unique index for message-level labels (label_message)
        await safeCreateIndex(
            collections.labelAssociations,
            { instanceId: 1, type: 1, chatId: 1, labelId: 1, messageId: 1 },
            { unique: true, partialFilterExpression: { type: 'label_message' } }
        )
    }),
    15000
)
```

- Optional: do the same lock-wrapping for `groupMetadata` and `labels` for consistency.

---

### Mapping log issues → status

- IndexKeySpecsConflict (code 86) when adding partial index:
  - Status: **Handled** (drop conflicting index by name, recreate with partial filter; 85 also handled).
- MongoNotConnectedError during drop/create:
  - Status: **Handled** (already retried by `withConnection` + `connectionRetry`).
- MongoClientClosedError: Operation interrupted because client was closed:
  - Status: **Pending** → add retry support (edit above).
- Duplicate “Created index …” on `labelAssociations`:
  - Status: **Partially mitigated** by conflict handling; **Recommend** locking to avoid parallel creation and log noise.

---

### Verification checklist

- Before changes:
  - Reproduce by starting multiple instances simultaneously; check for:
    - Code 86 on `labelAssociations` index creation.
    - “Operation interrupted because client was closed.”
    - Duplicate “Created index …” lines.

- After changes:
  - Confirm no failures on `labelAssociations` index:
    - No unhandled 86; no aborted `createIndex` when client closes (retries kick in).
    - Single “Created index …” per index after adding lock.
  - Run `store.getIndexStatus()` and verify:
    - `labelAssociations`: both partial unique indexes exist; legacy ones removed.
    - TTL indexes exist only where expected (no TTL on labels/labelAssociations/groupMetadata).

---

### Risks/Notes
- Locking adds small startup latency if many instances try to build indexes at once; safer overall.
- If your environment frequently migrates/rotates Mongo clients, the retry on `MongoClientClosedError` is crucial to keep index creation robust.

---

### Conclusion
- The current code already fixes the core index conflict seen in the log and safely drops/rebuilds indexes.
- Add `MongoClientClosedError` to retry logic and lock `labelAssociations` index creation to fully eliminate the remaining failures and duplicate logs observed in `dropindex-bug.md`.