# Enhanced LID Handling, Index Management, and Connection Reliability — Implementation Report

Date: 2025-09-04

## Overview

This report documents fixes and enhancements implemented to resolve issues around LID handling, index management, and connection reliability observed in `docs/replies-from-lid-decrypt-issue.md`, especially the recurring log line:

- `3A196C06197674EEB1DB`
- `[LID] Pattern 3: FromMe=false, only senderLid (waiting for phone discovery)`
- `[LidHandler] getPhoneNumberFromLid: Database not connected, returning fallback`

The updates deliver:

- Smart index management coverage for LID data and fast `senderLid` lookups
- Robust LidHandler connectivity checks integrated with the store’s connection lifecycle
- Proactive, event-driven LID→phone mapping with pushName capture for incoming messages only

## Key Fixes

**Smart Index Management**
- Added `lidMappings` to the centralized smart index manager so creation/health is tracked:
  - `lidMappings_primary`: `{ instanceId: 1, lid: 1 }` (unique)
  - `lidMappings_phone_lookup`: `{ instanceId: 1, phoneNumber: 1 }`
  - File: `src/makeEnhancedMongoDBStore.ts:2849`
- Added `messages_senderLid_lookup` index to speed reverse lookups when only `senderLid` is present:
  - `{ instanceId: 1, 'key.fromMe': 1, 'key.senderLid': 1 }`
  - File: `src/makeEnhancedMongoDBStore.ts:2826`

**LidHandler Connection Reliability**
- Reworked connectivity logic to avoid false negatives caused by probing `MongoClient` internals (driver v6):
  - Track `Db` reference; treat as connected if initialized and collections exist
  - Attempt operations; fallback only on real connection errors (e.g., `MongoNotConnectedError`)
  - On connection error, mark as uninitialized; the store’s reconnection path already re-inits LidHandler
  - File: `src/utils/lidHandler.ts:72–110`

**Shared Connection Lifecycle**
- LidHandler now accepts an `ensureConnection` callback and is wired to the store’s connection/retry logic (ConnectionManager), ensuring consistent reconnection behavior:
  - New config: `skipIndexCreation` (default: true), `ensureConnection?: () => Promise<void>`
  - Injection point: `src/makeEnhancedMongoDBStore.ts:684–705`

**Proactive LID→Phone Mapping (Event-driven)**
- Hooked into `messages.upsert` to store mappings in real time, eliminating manual backfill dependency:
  - Pattern 1 (incoming): `remoteJid` is LID, `senderPn` is phone → map LID→phone, update existing messages
    - Saves `pushName` for incoming messages
    - File: `src/makeEnhancedMongoDBStore.ts:4706`
  - Pattern 2 (outgoing): `remoteJid` is LID → reverse-lookup phone; if found, store mapping (no `pushName`)
    - File: `src/makeEnhancedMongoDBStore.ts:4730`
  - Pattern X (incoming): `senderLid` present, `remoteJid` is phone → store mapping with `pushName`
    - File: `src/makeEnhancedMongoDBStore.ts:4748`
- LidHandler’s `processMessage` also respects the incoming-only pushName rule:
  - Reverse-lookup path (fromMe=true): save mapping without `pushName`
  - Generic “have LID and phone” path: save `pushName` only if `fromMe=false`
  - File: `src/utils/lidHandler.ts:706,735`

**Mapping Schema Enhancement**
- `LidMapping` now includes optional `pushName` and `pushNameUpdatedAt` fields; both set only for incoming messages:
  - File: `src/utils/lidHandler.ts:14`
  - Upsert logic enriches `$set` and `$setOnInsert` accordingly
  - File: `src/utils/lidHandler.ts:304–339`

## Why These Changes Fix the Issues

- The recurring “Database not connected” was due to a fragile topology probe. By attempting the operation and falling back only on real errors, valid lookups now proceed without noise.
- Adding `lidMappings` to the smart index manager and the `senderLid` index ensures fast, predictable lookups and removes ad-hoc indexing.
- Proactive mapping upon message receipt addresses LID resolution early, reducing reliance on reverse lookups and improving normalization coverage.
- Capturing `pushName` only for incoming messages prevents storing our own display name (outgoing), aligning with expected semantics.

## Configuration Defaults and Behavior

- LidHandler default config:
  - `skipIndexCreation: true` (smart manager handles indexes)
  - `enableCache: true`, `cacheTTL: 3600`
  - `ensureConnection`: provided by the store; pre-checks connection before operations

## Files Changed (Highlights)

- `src/makeEnhancedMongoDBStore.ts`
  - Added index definitions and proactive mapping logic
  - Injected `ensureConnection` and default `skipIndexCreation` to LidHandler

- `src/utils/lidHandler.ts`
  - Connectivity logic modernized
  - `storeLidMapping` extended to accept `pushName`; schema enriched
  - PushName persisted only for incoming messages

- `src/utils/backfillLidMappings.ts` (utility only; not auto-run)
  - A safe, indexed backfill helper left for optional future use

## Validation Notes

- ESLint run and fixes applied as needed (see lint section below)
- Indices: smart manager logs confirm creation/skips per collection, including `lidMappings` and `messages_senderLid_lookup`
- Logs: noisy “Database not connected” fallback suppressed; real connection errors still handled

## Operational Guidance

- No manual backfill required. Mapping is maintained proactively on `messages.upsert`.
- Redis eviction warnings observed in logs (`allkeys-lru`) are environment-side; recommended to switch to `noeviction`.
- “SessionError: No session record” is outside this module (libsignal/WA session bootstrap) and unaffected by these changes.

## Next Steps (Optional)

- Add a guarded, one-time background backfill at startup, configurable via store options, if historical data normalization is needed.
- Consider a small metric/export endpoint to inspect mapping counts and recent mapping rates per instance.

