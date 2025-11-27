# Requirements Document

## Introduction

This feature adds a new MongoDB index on `mediaInfo.fileHash` for the messages collection to optimize media deduplication queries. MongoDB Atlas has identified this as a high-impact optimization opportunity, with the query running 1473 times/hour at 2253ms average execution time, scanning 1.6M documents per query. The new index is expected to reduce disk reads by up to 1.8 GB per hour.

## Glossary

- **MongoDB Store**: The data persistence layer that stores WhatsApp messages, contacts, chats, and other data in MongoDB collections
- **Index**: A MongoDB data structure that improves query performance by allowing efficient lookups without scanning entire collections
- **mediaInfo.fileHash**: A field stored on message documents containing a hash of the media file content, used for deduplication
- **Media Deduplication**: The process of checking if a media file already exists before downloading, to avoid storing duplicate files
- **TTL (Time-To-Live)**: Automatic document expiration mechanism in MongoDB
- **Sparse Index**: An index that only includes documents containing the indexed field, reducing index size

## Requirements

### Requirement 1

**User Story:** As a system operator, I want media deduplication queries to use an index, so that query performance improves and database load decreases.

#### Acceptance Criteria

1. WHEN the MongoDB store initializes THEN the System SHALL create an index on `mediaInfo.fileHash` for the messages collection
2. WHEN a media deduplication query executes using `mediaInfo.fileHash` THEN the System SHALL use the new index instead of performing a collection scan
3. WHEN the index is created THEN the System SHALL configure it as a sparse index to exclude documents without the `mediaInfo.fileHash` field

### Requirement 2

**User Story:** As a developer, I want the new index to follow existing index management patterns, so that the codebase remains consistent and maintainable.

#### Acceptance Criteria

1. WHEN the index is defined THEN the System SHALL use the existing `IndexSpec` interface format with name, spec, and options properties
2. WHEN the index is created THEN the System SHALL use the existing `batchCreateIndexes` utility function
3. WHEN the store uses smart index management THEN the System SHALL skip index creation if the index already exists
4. WHEN the index is defined THEN the System SHALL include it in both `makeMongoDBStore.ts` and `makeEnhancedMongoDBStore.ts` files

### Requirement 3

**User Story:** As a system operator, I want the index creation to be safe and non-disruptive, so that existing deployments can upgrade without issues.

#### Acceptance Criteria

1. WHEN the store initializes with an existing messages collection THEN the System SHALL only create the new index if it does not already exist
2. WHEN index creation fails THEN the System SHALL log the error and continue store initialization without crashing
3. WHEN the `forceRecreateIndexes` option is enabled THEN the System SHALL recreate the index along with other indexes
