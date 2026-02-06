import { MongoClient } from 'mongodb'
import { backfillLidMappings } from '../src/utils/backfillLidMappings'

async function main() {
  const uri = process.env.MONGO_URI || ''
  const database = process.env.MONGO_DB || ''
  const instanceId = process.env.INSTANCE_ID || ''
  const collectionPrefix = process.env.COLLECTION_PREFIX || 'wabot_'
  const sinceDays = parseInt(process.env.SINCE_DAYS || '7', 10)
  const limit = process.env.LIMIT ? parseInt(process.env.LIMIT, 10) : undefined
  const batchSize = process.env.BATCH_SIZE ? parseInt(process.env.BATCH_SIZE, 10) : undefined
  const dryRun = (process.env.DRY_RUN || 'false').toLowerCase() === 'true'

  if (!uri || !database || !instanceId) {
    console.error('Missing required env: MONGO_URI, MONGO_DB, INSTANCE_ID')
    process.exit(1)
  }

  const client = new MongoClient(uri)
  await client.connect()
  const db = client.db(database)

  console.log('[BackfillLID] Starting backfill...', { instanceId, sinceDays, limit, batchSize, dryRun })
  const res = await backfillLidMappings(db, {
    instanceId,
    collectionPrefix,
    sinceDays,
    limit,
    batchSize,
    dryRun,
  })
  console.log('[BackfillLID] Completed:', res)

  await client.close()
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})

