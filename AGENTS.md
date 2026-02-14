# @baileys/mongodb-store

MongoDB store implementation for WhiskeySockets Baileys WhatsApp Web API with multi-instance support, TTL, and LID mapping.

## Purpose

Provides persistent MongoDB-backed storage for Baileys WhatsApp sessions, including messages, chats, contacts, groups, labels, presence data, and media. Supports both a standard store (`makeMongoDBStore`) and an enhanced store (`makeEnhancedMongoDBStore`) with Redis/BullMQ queues, media handling, shared queue management, and advanced connection pooling.

## Module System

This package is **pure ESM** (`"type": "module"` in package.json), matching Baileys v7 which is also pure ESM. All imports use `.js` extensions as required by the ESM spec. Requires Node.js >= 20.0.0.

## Key Files

| File | Description |
|------|-------------|
| `package.json` | Package config: `@baileys/mongodb-store` v3.0.0, pure ESM, dependencies (mongodb, ioredis, bullmq, node-cache, p-queue, axios) |
| `tsconfig.json` | TypeScript config: ES2020 target, ESNext module, bundler resolution, strict mode, ESM output to `dist/` |
| `jest.config.cjs` | Jest config: ts-jest preset with CJS transform for tests, moduleNameMapper strips .js extensions |
| `.eslintrc.json` | ESLint config for TypeScript |

## Subdirectories

| Directory | Description |
|-----------|-------------|
| `src/` | Source code: store factories, types, utilities |
| `docs/` | Bug reports, resolved issues, sample application code |
| `examples/` | Usage examples (JS/TS) for various features |
| `dist/` | Compiled output (do not edit) |

## Architecture

- **Two store variants**: `makeMongoDBStore` (standard with PQueue) and `makeEnhancedMongoDBStore` (with BullMQ, media, shared queues)
- **Connection pooling**: Tiered (hot/warm/cold) via `ConnectionManager` singleton
- **LID handling**: Maps WhatsApp LID identifiers to phone JIDs for Baileys v7+ compatibility
- **Security**: Input validation, JID sanitization, instance isolation via `InstanceAccessContext`
- **Memory management**: `MemoryMonitor`, `BackpressureController` for production stability
- **TTL**: Automatic document expiration via MongoDB TTL indexes

## AI Instructions

- Run `npm run build` (tsc) to verify TypeScript compilation -- outputs ESM to `dist/`
- Run `npm test` to execute Jest test suite (uses `mongodb-memory-server`)
- The two main store files are very large (3700+ and 7700+ lines); prefer targeted edits
- `baileys` is a devDependency/peerDependency -- do not add it to runtime dependencies
- All MongoDB operations use instance-scoped collection prefixes for multi-tenancy
- Never expose JIDs, instance IDs, or auth credentials in logs; use `hashForLogging()`
- All relative imports MUST include `.js` extensions (ESM requirement)
- Baileys deep imports (e.g. `baileys/lib/Types/Label.js`) must also use `.js` extensions

## Dependencies

### Runtime
- `mongodb` ^6.3.0 -- database driver
- `ioredis` ^5.7.0 -- Redis client for BullMQ
- `bullmq` ^5.58.4 -- job queue for label associations and messages
- `node-cache` ^5.1.2 -- in-memory caching
- `p-queue` ^8.1.0 -- promise-based concurrency control
- `axios` ^1.7.0 -- HTTP client for media downloads

### Peer
- `baileys` >=7.0.0-rc.9 -- WhatsApp Web API (pure ESM)
- `pino` -- logger

### Dev
- `baileys` ^7.0.0-rc.9 -- WhatsApp Web API (types only at runtime)
- `typescript`, `jest`, `ts-jest`, `mongodb-memory-server`
