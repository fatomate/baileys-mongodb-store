# Unifying LID Mapping into `contacts` — Implementation Report

Date: 2025-09-04
Branch: `feat/unify-lid-into-contacts`

## Summary

This change centralizes LID → phone number mapping into the `contacts` collection, disables TTL for contacts, preserves user-saved `notify`, and introduces an optional auto-fill from legacy `lidMappings` when legacy reads occur. No new data is written to `lidMappings`.

## Key Changes

- Contacts TTL disabled
  - Removed contacts from TTL monitoring and stopped creating a TTL index for contacts.
  - Files:
    - `src/makeEnhancedMongoDBStore.ts:669` (TTL monitor excludes contacts)
    - `src/makeEnhancedMongoDBStore.ts:2863` (no contacts TTL index)
    - `src/makeEnhancedMongoDBStore.ts:5866` (no contacts TTL index in recreate)
    - `src/makeMongoDBStore.ts:383` (TTL monitor excludes contacts)
    - `src/makeMongoDBStore.ts:1546`, `:3279` (no contacts TTL index)

- New contacts index for LID lookups
  - `{ instanceId: 1, lid: 1 }` with `unique: true` and `partialFilterExpression` for docs with a `lid` field.
  - Files:
    - `src/makeEnhancedMongoDBStore.ts:2864`, `:5867`
    - `src/makeMongoDBStore.ts:1546`, `:3279`

- LID mapping now stored on `contacts`
  - `LidHandler.storeLidMapping(lid, phone, pushName?)` upserts by `contacts.id = phone` and sets:
    - `lid`, `lidFirstSeen`, `lidLastSeen`, `lidMappingUpdatedAt`, `updatedAt`
    - For incoming messages only, `pushName` + `pushNameUpdatedAt`
  - Legacy reads from `lidMappings` are supported (read-only).
  - Files:
    - `src/utils/lidHandler.ts:62–92`, `:296–406`, `:407–466`, `:467–521`, `:820–847`, `:848–865`

- Optional auto-fill from legacy (ON by default)
  - New config `autoFillFromLegacy?: boolean` on `LidHandlerConfig` (default: true).
  - When enabled and a legacy read returns a mapping, it auto-fills contacts using `storeLidMapping` (no pushName), then returns the value.
  - Files:
    - `src/utils/lidHandler.ts:18–27`, `:100–109`, `:439–447`, `:497–505`

- Preserve user-saved `notify`; keep both `name` and `pushName`
  - Reworked contacts upserts/updates to use `$set`/`$setOnInsert` and only set `notify` on insert (never override).
  - `name` (from contacts events) and `pushName` (from messages) both retained for source identification.
  - Files:
    - Enhanced store instance-processor: `src/makeEnhancedMongoDBStore.ts:1167–1210`
    - Enhanced store shared queue: `src/makeEnhancedMongoDBStore.ts:2068–2208`
    - Enhanced direct upsert path: `src/makeEnhancedMongoDBStore.ts:3325–3388`
    - Standard store queues: `src/makeMongoDBStore.ts:820–860`

## Behavior Notes

- Contacts persist without TTL. LID-related changes update `updatedAt` to keep contact documents fresh.
- `pushName` is only set for incoming messages; outgoing messages do not set it.
- Lookups prefer `contacts` and fall back to `lidMappings` (read-only). With `autoFillFromLegacy` enabled, a legacy read triggers best-effort forward migration.
- No new writes to `lidMappings`.

## Configuration

- `LidHandlerConfig.autoFillFromLegacy?: boolean` (default: false)
  - Enable to auto-fill contacts upon a successful legacy read.
  - Passed when constructing `LidHandler` in your store setup.

## Validation

- Lint: ran `npm run lint` — warnings only, no errors.
- Build: ran `npm run build` — success.

## Upgrade Guidance

1. Deploy this branch.
2. Ensure indexes are (re)created via the store’s index management (`recreateIndexes()` if desired).
3. Optionally enable `autoFillFromLegacy` to opportunistically migrate entries as they’re read.
4. Keep `lidMappings` around until you’re confident in the new path, then consider removal later.

## Future Considerations

- Add a one-time offline migration if you prefer immediate consolidation rather than on-demand auto-fill.
- Metrics endpoint for counts of contacts with `lid` and legacy fallback hits.
