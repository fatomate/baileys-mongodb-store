import { Db } from 'mongodb'
import { isLidFormat } from './jidUtils.js'

export interface BackfillOptions {
  instanceId: string
  collectionPrefix?: string
  sinceDays?: number // default 7
  limit?: number // max number of messages to scan
  batchSize?: number // default 500
  dryRun?: boolean // default false
  logEvery?: number // log progress every N upserts, default 1000
}

export interface BackfillResult {
  scanned: number
  considered: number
  upsertsAttempted: number
  upsertsSucceeded: number
  upsertsFailed: number
  distinctPairs: number
}

/**
 * Scans recent messages to backfill LID -> phone number mappings into lidMappings.
 * Uses indexed filters and bulk upserts with moderate batch sizes to limit resource usage.
 */
export async function backfillLidMappings(db: Db, options: BackfillOptions): Promise<BackfillResult> {
  const {
    instanceId,
    collectionPrefix = '',
    sinceDays = 7,
    limit,
    batchSize = 500,
    dryRun = false,
    logEvery = 1000,
  } = options

  const messagesCol = db.collection(`${collectionPrefix}messages`)
  const lidCol = db.collection(`${collectionPrefix}lidMappings`)

  // Filter: received messages with senderLid present
  const cutoff = new Date(Date.now() - sinceDays * 24 * 60 * 60 * 1000)
  const filter: any = {
    instanceId,
    'key.fromMe': false,
    'key.senderLid': { $exists: true, $nin: [null, ''] },
    updatedAt: { $gte: cutoff },
  }

  const projection = {
    'key.senderLid': 1,
    'key.senderPn': 1,
    'key.remoteJid': 1,
    updatedAt: 1,
  }

  // Use a cursor with a reasonable batch size; let MongoDB stream results
  const cursor = messagesCol.find(filter, { projection }).batchSize(Math.min(batchSize, 1000))

  const result: BackfillResult = {
    scanned: 0,
    considered: 0,
    upsertsAttempted: 0,
    upsertsSucceeded: 0,
    upsertsFailed: 0,
    distinctPairs: 0,
  }

  // Deduplicate within this run: one phone per LID (last one wins)
  const mappingMap = new Map<string, string>()
  const ops: any[] = []
  const now = new Date()

  while (await cursor.hasNext()) {
    const doc = await cursor.next()
    result.scanned++

    // Stop if limit reached
    if (limit && result.scanned > limit) break

    const senderLid: string | undefined = doc?.key?.senderLid
    if (!senderLid || !isLidFormat(senderLid)) continue

    // Derive phone number: prefer senderPn when not LID; else remoteJid when not LID
    const senderPn: string | undefined = doc?.key?.senderPn
    const remoteJid: string | undefined = doc?.key?.remoteJid

    let phone: string | undefined
    if (senderPn && !isLidFormat(senderPn)) phone = senderPn
    else if (remoteJid && !isLidFormat(remoteJid)) phone = remoteJid

    if (!phone) continue

    result.considered++
    // Record mapping
    mappingMap.set(senderLid, phone)
  }

  result.distinctPairs = mappingMap.size

  for (const [lid, phoneNumber] of mappingMap.entries()) {
    result.upsertsAttempted++
    if (dryRun) continue

    const update = {
      updateOne: {
        filter: { instanceId, lid },
        update: {
          $setOnInsert: {
            instanceId,
            lid,
            phoneNumber,
            firstSeen: now,
            updatedAt: now,
          },
        },
        upsert: true,
      },
    }
    ops.push(update)

    if (ops.length >= batchSize) {
      try {
        const res = await lidCol.bulkWrite(ops, { ordered: false })
        result.upsertsSucceeded += res.upsertedCount
      } catch (e) {
        result.upsertsFailed += ops.length
      }
      ops.length = 0
      if (result.upsertsAttempted % (logEvery || 1000) === 0) {
        console.log(`[BackfillLID] Progress: attempted=${result.upsertsAttempted}, success=${result.upsertsSucceeded}`)
      }
    }
  }

  if (!dryRun && ops.length > 0) {
    try {
      const res = await lidCol.bulkWrite(ops, { ordered: false })
      result.upsertsSucceeded += res.upsertedCount
    } catch (e) {
      result.upsertsFailed += ops.length
    }
  }

  return result
}
