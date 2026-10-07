// WAB-654: wabot_messages.messageTimestamp is always a BSON Int32 count of Unix seconds.
// Baileys hands the store uint64 timestamps as Long objects; a JSON round trip (the store's
// deep clone, BullMQ job payloads) turns them into strings or {low, high, unsigned}.
// MongoDB orders strings after numbers and range queries only match one BSON type,
// so every write normalizes here.
import { BSON, Long } from 'mongodb'
import type { Collection, Document, Filter, UpdateResult } from 'mongodb'

const MS_THRESHOLD = 9999999999n
const INT32_MAX = 2147483647n
// ponytail: Int32 seconds run out on 2038-01-19; widen to Int64 (and the migration's target type) before then.
const DECIMAL = /^\d{1,19}(\.\d+)?$/

const isInt32 = (value: unknown): value is number =>
    typeof value === 'number' && Number.isInteger(value) && value >= -(2 ** 31) && value <= 2 ** 31 - 1

const bsonType = (value: any): string | undefined =>
    value && typeof value === 'object' && typeof value._bsontype === 'string' ? value._bsontype : undefined

// Only a working Int32 wrapper is unwrapped; a look-alike with a broken valueOf is left as-is
// and then fails the integer check.
const unwrapInt32 = (value: any): unknown => {
    if (bsonType(value) !== 'Int32') return value
    try {
        return value.valueOf()
    } catch {
        return undefined
    }
}

const fromInteger = (value: bigint): number | undefined => {
    const seconds = value > MS_THRESHOLD ? value / 1000n : value
    return seconds >= 1n && seconds <= INT32_MAX ? Number(seconds) : undefined
}

// Integer part of an unsigned decimal string, parsed exactly. Exponent notation is rejected.
const fromDecimalString = (value: string): number | undefined => {
    const trimmed = value.trim()
    return DECIMAL.test(trimmed) ? fromInteger(BigInt(trimmed.split('.')[0])) : undefined
}

const fromNumber = (value: number): number | undefined => {
    if (!Number.isFinite(value)) return undefined
    const whole = Math.trunc(value)
    return Number.isSafeInteger(whole) ? fromInteger(BigInt(whole)) : undefined
}

// A Long, or the {low, high, unsigned} object a Long becomes after JSON.
const fromLongParts = (value: Record<string, unknown>): number | undefined => {
    if (Object.keys(value).some((key) => key !== 'low' && key !== 'high' && key !== 'unsigned')) return undefined
    const low = unwrapInt32(value.low)
    const high = unwrapInt32(value.high)
    if (!isInt32(low) || !isInt32(high)) return undefined
    if (value.unsigned !== undefined && typeof value.unsigned !== 'boolean') return undefined
    return fromInteger(BigInt(Long.fromBits(low, high, value.unsigned === true).toString()))
}

/**
 * Convert any stored or incoming messageTimestamp shape to Int32 Unix seconds.
 * Returns undefined when no trustworthy value exists; it never returns 0, NaN or a fraction.
 */
export const normalizeMessageTimestamp = (value: unknown): number | undefined => {
    if (typeof value === 'number') return fromNumber(value)
    if (typeof value === 'string') return fromDecimalString(value)
    if (typeof value === 'bigint') return fromInteger(value)
    if (!value || typeof value !== 'object' || Array.isArray(value) || value instanceof Date) return undefined

    const tag = bsonType(value)
    if (tag !== undefined) {
        // Real BSON wrappers convert through their own methods. JSON junk that only carries a
        // _bsontype tag can throw there, so any failure means "no trustworthy value".
        try {
            switch (tag) {
                case 'Long':
                    return fromInteger(BigInt((value as Long).toString()))
                case 'Int32':
                case 'Double':
                    return fromNumber((value as any).valueOf())
                case 'Decimal128':
                    return fromDecimalString(String(value))
                default:
                    return undefined
            }
        } catch {
            return undefined
        }
    }

    const record = value as Record<string, unknown>
    const keys = Object.keys(record)
    if (keys.length === 1 && keys[0] === '$numberLong') {
        return typeof record.$numberLong === 'string' ? fromDecimalString(record.$numberLong) : undefined
    }
    return 'low' in record && 'high' in record ? fromLongParts(record) : undefined
}

export type InvalidTimestampHandler = (context: string, raw: unknown) => void

const nowSeconds = () => Math.floor(Date.now() / 1000)

// The raw value is kept for investigation only when BSON can store it; a look-alike wrapper
// would make the whole write fail, so it is recorded as unserializable instead.
const storableRaw = (raw: unknown): unknown => {
    try {
        BSON.calculateObjectSize({ v: raw })
        return raw
    } catch {
        return { unserializable: true }
    }
}

const hasTimestamp = (doc: Document) => doc.messageTimestamp !== undefined && doc.messageTimestamp !== null

// Whole documents always leave with an Int32 timestamp. An unconvertible value becomes
// the receipt time, and the raw value is kept so it can be investigated.
const normalizeDocument = (doc: Document, onInvalid?: InvalidTimestampHandler): Document => {
    if (!doc || typeof doc !== 'object') return doc
    const normalized = normalizeMessageTimestamp(doc.messageTimestamp)
    if (normalized !== undefined) return { ...doc, messageTimestamp: normalized }
    if (!hasTimestamp(doc)) return { ...doc, messageTimestamp: nowSeconds() }
    onInvalid?.('document', doc.messageTimestamp)
    return { ...doc, messageTimestamp: nowSeconds(), messageTimestampInvalid: storableRaw(doc.messageTimestamp) }
}

// Partial updates only touch the timestamp when they set one. An unconvertible value is
// dropped so the stored timestamp survives. Pipelines are passed through unchanged.
const normalizeUpdate = (update: any, onInvalid?: InvalidTimestampHandler): any => {
    const set = update && !Array.isArray(update) ? update.$set : undefined
    if (!set || typeof set !== 'object' || !Object.prototype.hasOwnProperty.call(set, 'messageTimestamp')) return update
    const normalized = normalizeMessageTimestamp(set.messageTimestamp)
    if (normalized !== undefined) return { ...update, $set: { ...set, messageTimestamp: normalized } }
    const { messageTimestamp: raw, ...rest } = set
    onInvalid?.('update', raw)
    return { ...update, $set: rest }
}

const normalizeBulkOp = (op: any, onInvalid?: InvalidTimestampHandler): any => {
    if (op?.insertOne) return { insertOne: { ...op.insertOne, document: normalizeDocument(op.insertOne.document, onInvalid) } }
    if (op?.replaceOne) return { replaceOne: { ...op.replaceOne, replacement: normalizeDocument(op.replaceOne.replacement, onInvalid) } }
    if (op?.updateOne) return { updateOne: { ...op.updateOne, update: normalizeUpdate(op.updateOne.update, onInvalid) } }
    if (op?.updateMany) return { updateMany: { ...op.updateMany, update: normalizeUpdate(op.updateMany.update, onInvalid) } }
    return op
}

/**
 * Wrap the messages collection so every write stores an Int32 seconds messageTimestamp.
 * Covers insertOne, insertMany, replaceOne, updateOne/updateMany ($set) and bulkWrite.
 * Not covered: update pipelines, $setOnInsert and findOneAndUpdate/findOneAndReplace;
 * the store does not use them on messages. Every other member passes through unchanged.
 */
export const wrapMessageWrites = <T extends Document>(
    collection: Collection<T>,
    onInvalid?: InvalidTimestampHandler
): Collection<T> => {
    const target = collection as any
    const writes: Record<string, (...args: any[]) => any> = {
        insertOne: (doc, options) => target.insertOne(normalizeDocument(doc, onInvalid), options),
        insertMany: (docs, options) => target.insertMany(Array.from(docs, (doc: Document) => normalizeDocument(doc, onInvalid)), options),
        replaceOne: (filter, replacement, options) => target.replaceOne(filter, normalizeDocument(replacement, onInvalid), options),
        updateOne: (filter, update, options) => target.updateOne(filter, normalizeUpdate(update, onInvalid), options),
        updateMany: (filter, update, options) => target.updateMany(filter, normalizeUpdate(update, onInvalid), options),
        bulkWrite: (ops, options) => target.bulkWrite(Array.from(ops, (op: any) => normalizeBulkOp(op, onInvalid)), options),
    }
    return new Proxy(target, {
        get(obj, prop) {
            if (typeof prop === 'string' && Object.prototype.hasOwnProperty.call(writes, prop)) return writes[prop]
            const value = Reflect.get(obj, prop)
            return typeof value === 'function' ? value.bind(obj) : value
        },
    }) as Collection<T>
}

const TS_TYPE = { $type: '$messageTimestamp' }

/**
 * WAB-859: apply a message update without moving an existing valid send time.
 * Baileys receipts (rc13+) and merged whole-message patches carry a later time; a non-edit
 * update therefore never changes a stored time that normalizes to a valid value. If the stored
 * time is missing or invalid, the incoming valid time fills it with a compare-and-set on the
 * exact stored value and BSON type, so a time another writer stored first is kept.
 * Edits pass straight through: their callers already resolved the preserved time.
 */
export const writeMessageUpdate = async (
    collection: Collection<any>,
    filter: Filter<any>,
    set: Document,
    options: { isEdit?: boolean } = {}
): Promise<UpdateResult> => {
    if (options.isEdit || !Object.prototype.hasOwnProperty.call(set, 'messageTimestamp')) {
        return collection.updateOne(filter, { $set: set })
    }
    const { messageTimestamp: incoming, ...rest } = set
    const result = await collection.updateOne(filter, { $set: rest })
    if (normalizeMessageTimestamp(incoming) === undefined) return result

    const [stored] = await collection.aggregate(
        [{ $match: filter }, { $limit: 1 }, { $project: { _id: 1, t: TS_TYPE, v: '$messageTimestamp' } }],
        { promoteValues: false }
    ).toArray()
    if (!stored || normalizeMessageTimestamp(stored.v) !== undefined) return result

    const unchanged = stored.t === 'missing'
        ? { messageTimestamp: { $exists: false } }
        : { $expr: { $and: [{ $eq: [TS_TYPE, stored.t] }, { $eq: ['$messageTimestamp', { $literal: stored.v }] }] } }
    await collection.updateOne({ _id: stored._id, ...unchanged }, { $set: { messageTimestamp: incoming } })
    return result
}
