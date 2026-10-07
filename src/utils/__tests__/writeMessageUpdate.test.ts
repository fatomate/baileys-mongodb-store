import { Collection, Long } from 'mongodb'
import { wrapMessageWrites, writeMessageUpdate } from '../messageTimestamp'
import { Harness, INSTANCE, JID, S, messages, startMongo, storedTimestamp } from '../../__tests__/helpers/storeHarness'

// WAB-859: the shared write used by updateMessage and both queue update processors.
// A non-edit update never moves an existing valid time; a missing or invalid time is filled
// only if it is still missing or invalid at write time. Real mongod.
jest.setTimeout(30000)

let h: Harness
let wrapped: Collection
const filter = (id: string) => ({ instanceId: INSTANCE, jid: JID, 'key.id': id })
const seed = (id: string, messageTimestamp?: unknown) =>
    messages(h).insertOne({ instanceId: INSTANCE, jid: JID, key: { id, remoteJid: JID, fromMe: false }, ...(messageTimestamp === undefined ? {} : { messageTimestamp }) })

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

describe('writeMessageUpdate', () => {
    it('keeps an existing valid time and applies the other fields', async () => {
        await seed('w1', S)
        await writeMessageUpdate(wrapped, filter('w1'), { status: 4, messageTimestamp: S + 3600 })
        const stored = await storedTimestamp(h, 'w1')
        expect(stored).toMatchObject({ type: 'int', value: S })
        expect(stored?.doc.status).toBe(4)
    })

    it('keeps a stored Long time that is valid', async () => {
        await seed('w2', Long.fromNumber(S))
        await writeMessageUpdate(wrapped, filter('w2'), { messageTimestamp: S + 9 })
        expect(await storedTimestamp(h, 'w2')).toMatchObject({ type: 'long', value: S })
    })

    it.each([
        ['missing', undefined],
        ['null', null],
        ['a junk string', 'junk'],
        ['an object that cannot convert', { foo: 1 }],
    ])('fills a %s stored time with the normalized incoming time', async (_label, stored) => {
        await seed('w3', stored)
        await writeMessageUpdate(wrapped, filter('w3'), { messageTimestamp: '1759100005' })
        expect(await storedTimestamp(h, 'w3')).toMatchObject({ type: 'int', value: S + 5 })
    })

    it('does not fill when another writer stores a time between the read and the write', async () => {
        await seed('w4')
        const real = wrapped.aggregate.bind(wrapped)
        const racing = new Proxy(wrapped, {
            get(target, prop) {
                if (prop !== 'aggregate') return Reflect.get(target, prop)
                return (...args: any[]) => {
                    const cursor = real(...(args as [any, any]))
                    const toArray = cursor.toArray.bind(cursor)
                    cursor.toArray = async () => {
                        const rows = await toArray()
                        await messages(h).updateOne(filter('w4'), { $set: { messageTimestamp: S } })
                        return rows
                    }
                    return cursor
                }
            },
        }) as Collection
        await writeMessageUpdate(racing, filter('w4'), { messageTimestamp: S + 50 })
        expect(await storedTimestamp(h, 'w4')).toMatchObject({ type: 'int', value: S })
    })

    it('passes an edit straight through', async () => {
        await seed('w5', S)
        await writeMessageUpdate(wrapped, filter('w5'), { messageTimestamp: S - 7 }, { isEdit: true })
        expect(await storedTimestamp(h, 'w5')).toMatchObject({ type: 'int', value: S - 7 })
    })

    it('leaves the time alone when the update has none', async () => {
        await seed('w6', S)
        await writeMessageUpdate(wrapped, filter('w6'), { status: 3 })
        expect(await storedTimestamp(h, 'w6')).toMatchObject({ type: 'int', value: S })
    })
})
