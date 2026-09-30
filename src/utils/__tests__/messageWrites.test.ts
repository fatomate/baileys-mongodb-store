import { Collection, Long } from 'mongodb'
import { wrapMessageWrites } from '../messageTimestamp'
import { Harness, INSTANCE, JID, S, messages, nowSeconds, startMongo, storedTimestamp } from '../../__tests__/helpers/storeHarness'

// WAB-654: every write form the store uses on wabot_messages stores an Int32 seconds timestamp.
// The wrapper is exercised against a real mongod; assertions read back the stored BSON type.
jest.setTimeout(30000)

let h: Harness
let wrapped: Collection

const doc = (id: string, messageTimestamp?: unknown) => ({
    instanceId: INSTANCE,
    jid: JID,
    key: { id, remoteJid: JID, fromMe: false },
    message: { conversation: id },
    ...(messageTimestamp === undefined ? {} : { messageTimestamp }),
})
const filter = (id: string) => ({ instanceId: INSTANCE, jid: JID, 'key.id': id })

beforeAll(async () => {
    h = await startMongo()
})
afterAll(async () => {
    await h.stop()
})
beforeEach(async () => {
    await messages(h).deleteMany({})
    wrapped = wrapMessageWrites(messages(h))
})

describe('wrapMessageWrites: inserts and replacements', () => {
    it('insertOne stores a string timestamp as Int32 seconds', async () => {
        await wrapped.insertOne(doc('i1', '1759100000'))
        expect(await storedTimestamp(h, 'i1')).toMatchObject({ type: 'int', value: S })
    })

    it('insertMany normalizes every document', async () => {
        await wrapped.insertMany([doc('m1', { low: S, high: 0, unsigned: true }), doc('m2', Long.fromNumber(S + 1))])
        expect(await storedTimestamp(h, 'm1')).toMatchObject({ type: 'int', value: S })
        expect(await storedTimestamp(h, 'm2')).toMatchObject({ type: 'int', value: S + 1 })
    })

    it('replaceOne upsert converts milliseconds to seconds', async () => {
        await wrapped.replaceOne(filter('r1'), doc('r1', 1759100000999), { upsert: true })
        expect(await storedTimestamp(h, 'r1')).toMatchObject({ type: 'int', value: S })
    })

    it('an invalid timestamp is replaced by receipt time and the raw value is kept in messageTimestampInvalid', async () => {
        const before = nowSeconds()
        await wrapped.replaceOne(filter('bad1'), doc('bad1', 'junk'), { upsert: true })
        const after = nowSeconds()
        const stored = await storedTimestamp(h, 'bad1')
        expect(stored?.type).toBe('int')
        expect(stored?.value as number).toBeGreaterThanOrEqual(before)
        expect(stored?.value as number).toBeLessThanOrEqual(after)
        expect(stored?.doc.messageTimestampInvalid).toBe('junk')
    })

    it('a fake BSON-tagged timestamp still stores the message with receipt time and a safe marker', async () => {
        const before = nowSeconds()
        await wrapped.insertOne(doc('fake1', { _bsontype: 'Long' }))
        await wrapped.insertOne(doc('fake2', { low: { _bsontype: 'Int32', valueOf: null }, high: 0 }))
        for (const id of ['fake1', 'fake2']) {
            const stored = await storedTimestamp(h, id)
            expect(stored?.type).toBe('int')
            expect(stored?.value as number).toBeGreaterThanOrEqual(before)
            expect(stored?.doc.messageTimestampInvalid).toEqual({ unserializable: true })
        }
    })

    it('a document without a timestamp gets receipt time and no invalid marker', async () => {
        const before = nowSeconds()
        await wrapped.insertOne(doc('none1'))
        const stored = await storedTimestamp(h, 'none1')
        expect(stored?.type).toBe('int')
        expect(stored?.value as number).toBeGreaterThanOrEqual(before)
        expect(stored?.doc).not.toHaveProperty('messageTimestampInvalid')
    })

    it('does not mutate the caller document', async () => {
        const input = doc('mut1', '1759100000')
        await wrapped.insertOne(input)
        expect(input.messageTimestamp).toBe('1759100000')
    })
})

describe('wrapMessageWrites: $set updates', () => {
    beforeEach(async () => {
        await messages(h).insertOne(doc('u1', S))
    })

    it('updateOne $set with a Long stores Int32 seconds', async () => {
        await wrapped.updateOne(filter('u1'), { $set: { messageTimestamp: Long.fromNumber(S + 5) } })
        expect(await storedTimestamp(h, 'u1')).toMatchObject({ type: 'int', value: S + 5 })
    })

    it('updateMany $set normalizes the timestamp', async () => {
        await wrapped.updateMany({ instanceId: INSTANCE }, { $set: { messageTimestamp: '1759100009' } })
        expect(await storedTimestamp(h, 'u1')).toMatchObject({ type: 'int', value: S + 9 })
    })

    it('an invalid $set timestamp is dropped and the stored value is kept, other fields still apply', async () => {
        await wrapped.updateOne(filter('u1'), { $set: { messageTimestamp: 'junk', status: 3 } })
        const stored = await storedTimestamp(h, 'u1')
        expect(stored).toMatchObject({ type: 'int', value: S })
        expect(stored?.doc.status).toBe(3)
    })

    it('an update without a timestamp leaves the stored timestamp alone', async () => {
        await wrapped.updateOne(filter('u1'), { $set: { status: 4 } })
        expect(await storedTimestamp(h, 'u1')).toMatchObject({ type: 'int', value: S })
    })
})

describe('wrapMessageWrites: bulkWrite', () => {
    it('normalizes insertOne, replaceOne and updateOne $set operations', async () => {
        await messages(h).insertOne(doc('b3', S))
        await wrapped.bulkWrite([
            { insertOne: { document: doc('b1', '1759100001') } },
            { replaceOne: { filter: filter('b2'), replacement: doc('b2', { low: S + 2, high: 0, unsigned: true }), upsert: true } },
            { updateOne: { filter: filter('b3'), update: { $set: { messageTimestamp: 1759100003000 } } } },
        ], { ordered: true })
        expect(await storedTimestamp(h, 'b1')).toMatchObject({ type: 'int', value: S + 1 })
        expect(await storedTimestamp(h, 'b2')).toMatchObject({ type: 'int', value: S + 2 })
        expect(await storedTimestamp(h, 'b3')).toMatchObject({ type: 'int', value: S + 3 })
    })
})

describe('wrapMessageWrites: everything else passes through', () => {
    it('keeps reads, metadata and detached method calls working', async () => {
        await messages(h).insertOne(doc('p1', '1759100000'))
        const { find } = wrapped
        expect(wrapped.collectionName).toBe(messages(h).collectionName)
        expect(await find.call(wrapped, { 'key.id': 'p1' }).toArray()).toHaveLength(1)
        expect(await wrapped.countDocuments({ 'key.id': 'p1' })).toBe(1)
        expect(await wrapped.aggregate([{ $match: { 'key.id': 'p1' } }, { $count: 'n' }]).toArray()).toEqual([{ n: 1 }])
        expect((await wrapped.indexes()).map((i) => i.name)).toContain('_id_')
        // Reads return stored data unchanged; normalization applies to writes only.
        expect((await wrapped.findOne({ 'key.id': 'p1' }))?.messageTimestamp).toBe('1759100000')
    })

    it('propagates driver errors', async () => {
        await wrapped.insertOne({ _id: 'dup' as any, ...doc('e1', S) })
        await expect(wrapped.insertOne({ _id: 'dup' as any, ...doc('e2', S) })).rejects.toMatchObject({ code: 11000 })
    })
})
