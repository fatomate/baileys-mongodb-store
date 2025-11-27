# Implementation Plan

- [x] 1. Add mediaInfo.fileHash index to makeMongoDBStore.ts
  - [x] 1.1 Add index definition to messages array in indexDefinitions
    - Add `{ name: 'messages_media_fileHash', spec: { 'mediaInfo.fileHash': 1 }, options: { sparse: true } }` to the messages index array
    - Place after existing message indexes for consistency
    - _Requirements: 1.1, 1.3, 2.1_

- [x] 2. Add mediaInfo.fileHash index to makeEnhancedMongoDBStore.ts
  - [x] 2.1 Add index definition to messages array in indexDefinitions
    - Add `{ name: 'messages_media_fileHash', spec: { 'mediaInfo.fileHash': 1 }, options: { sparse: true } }` to the messages index array
    - Place after existing message indexes for consistency
    - _Requirements: 1.1, 1.3, 2.1, 2.4_

- [x] 3. Checkpoint - Verify implementation
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Write property test for index creation idempotence
  - [x] 4.1 Write property test for idempotent index creation
    - **Property 1: Index Creation Idempotence**
    - **Validates: Requirements 2.3, 3.1**
    - Test that initializing store multiple times results in exactly one index
    - Use Jest as the testing framework

- [-] 5. Create git branch for the changes
  - [-] 5.1 Create new git branch named `feature/media-filehash-index`
    - Branch from current HEAD
    - Stage and commit all changes with descriptive message
    - _Requirements: All_

- [ ] 6. Final Checkpoint - Verify all changes
  - Ensure all tests pass, ask the user if questions arise.
