import { Collection } from 'mongodb'
import { LidHandler } from '../lidHandler'
import { Harness, DB, PREFIX, INSTANCE, messages, startMongo } from '../../__tests__/helpers/storeHarness'

// WAB-859 round 1 (R1-1, R1-2, R1-8): the bounded live re-key against a real mongod.
jest.setTimeout(30000)

const LID = '123456789012345@lid'
const PHONE = '60123456789@s.whatsapp.net'
const OTHER = '60199999999@s.whatsapp.net'

let h: Harness
let handler: LidHandler

const row = (id: string, extra: Record<string, unknown> = {}, key: Record<string, unknown> = {}) => ({
    instanceId: INSTANCE,
    jid: LID,
    key: { id, remoteJid: LID, fromMe: false, ...key },
    messageTimestamp: 1759100000,
    ...extra,
})
const byId = (id: string) => messages(h).findOne({ 'key.id': id })
// Replaces the handler's messages collection with a proxy that runs `before` ahead of each updateOne.
const interceptUpdate = (before: (args: any[]) => Promise<void>) => {
    const real = (handler as any).messagesCollection as Collection
    ;(handler as any).messagesCollection = new Proxy(real, {
        get(target, prop) {
            if (prop === 'updateOne') return async (...args: any[]) => {
                await before(args)
                return (target.updateOne as any)(...args)
            }
            const value = Reflect.get(target, prop)
            return typeof value === 'function' ? value.bind(target) : value
        },
    })
}

beforeAll(async () => {
    h = await startMongo()
})
afterAll(async () => {
    await h.stop()
})
beforeEach(async () => {
    await messages(h).deleteMany({})
    handler = new LidHandler(INSTANCE, { enableCache: false, skipIndexCreation: true })
    await handler.initialize(h.raw.db(DB), PREFIX)
})
afterEach(() => {
    const cache = (handler as any).cache
    cache.close()
})

describe('updateExistingMessages', () => {
    it('moves a LID row to the phone and keeps the LID as remoteJidAlt', async () => {
        await messages(h).insertOne(row('a1'))
        const result = await handler.updateExistingMessages(LID, PHONE)
        expect(result).toEqual({ updated: 1, skipped: 0, remaining: 0, complete: true })
        expect(await byId('a1')).toMatchObject({ jid: PHONE, key: { remoteJid: PHONE, remoteJidAlt: LID, addressingMode: 'pn' } })
    })

    it('does not select a row whose jid already names another phone', async () => {
        await messages(h).insertOne(row('c1', { jid: OTHER }))
        const result = await handler.updateExistingMessages(LID, PHONE)
        expect(result).toMatchObject({ updated: 0 })
        expect(await byId('c1')).toMatchObject({ jid: OTHER, key: { remoteJid: LID } })
    })

    it('skips a row whose remoteJidAlt names another phone', async () => {
        await messages(h).insertOne(row('c2', {}, { remoteJidAlt: OTHER }))
        const result = await handler.updateExistingMessages(LID, PHONE)
        expect(result).toMatchObject({ updated: 0, skipped: 1 })
        expect(await byId('c2')).toMatchObject({ jid: LID, key: { remoteJidAlt: OTHER } })
    })

    it('leaves a row whose remoteJidAlt another writer changes after the scan', async () => {
        await messages(h).insertOne(row('r1'))
        interceptUpdate(async () => {
            await messages(h).updateOne({ 'key.id': 'r1' }, { $set: { 'key.remoteJidAlt': OTHER } })
        })
        const result = await handler.updateExistingMessages(LID, PHONE)
        expect(result).toMatchObject({ updated: 0, skipped: 1 })
        expect(await byId('r1')).toMatchObject({ jid: LID, key: { remoteJid: LID, remoteJidAlt: OTHER } })
    })

    it('leaves a row whose jid another writer changes after the scan', async () => {
        await messages(h).insertOne(row('r2'))
        interceptUpdate(async () => {
            await messages(h).updateOne({ 'key.id': 'r2' }, { $set: { jid: OTHER } })
        })
        const result = await handler.updateExistingMessages(LID, PHONE)
        expect(result).toMatchObject({ updated: 0, skipped: 1 })
        expect(await byId('r2')).toMatchObject({ jid: OTHER })
    })

    it('skips a row whose phone twin another writer creates during the call', async () => {
        await messages(h).createIndex({ instanceId: 1, jid: 1, 'key.id': 1 }, { unique: true })
        await messages(h).insertOne(row('t1'))
        interceptUpdate(async () => {
            await messages(h).insertOne({ instanceId: INSTANCE, jid: PHONE, key: { id: 't1', remoteJid: PHONE } })
        })
        const result = await handler.updateExistingMessages(LID, PHONE)
        expect(result).toMatchObject({ updated: 0, skipped: 1 })
        expect(await byId('t1')).toBeTruthy()
        expect(await messages(h).countDocuments({ 'key.id': 't1' })).toBe(2)
        await messages(h).dropIndexes()
    })

    it('starts no write after the deadline and returns within it when a write stalls', async () => {
        await messages(h).insertMany([row('d1'), row('d2'), row('d3')])
        let calls = 0
        interceptUpdate(async () => {
            calls++
            await new Promise((resolve) => setTimeout(resolve, 150))
        })
        const started = Date.now()
        const result = await handler.updateExistingMessages(LID, PHONE, { maxMs: 50 })
        const elapsed = Date.now() - started
        expect(elapsed).toBeLessThan(120)
        expect(result.complete).toBe(false)
        await new Promise((resolve) => setTimeout(resolve, 400))
        expect(calls).toBe(1)
        expect(await messages(h).countDocuments({ jid: LID })).toBe(2)
    })

    it('bounds a stalled connection check by the same deadline', async () => {
        await messages(h).insertOne(row('e1'))
        ;(handler as any).ensureConnectionCb = () => new Promise((resolve) => setTimeout(resolve, 300))
        const started = Date.now()
        const result = await handler.updateExistingMessages(LID, PHONE, { maxMs: 50 })
        expect(Date.now() - started).toBeLessThan(150)
        expect(result).toMatchObject({ updated: 0, complete: false })
        await new Promise((resolve) => setTimeout(resolve, 400))
        expect(await byId('e1')).toMatchObject({ jid: LID })
    })

    it('reports an incomplete pass when the connection is lost', async () => {
        await messages(h).insertOne(row('n1'))
        const real = (handler as any).messagesCollection as Collection
        ;(handler as any).messagesCollection = new Proxy(real, {
            get(target, prop) {
                if (prop === 'find') return () => ({
                    toArray: async () => {
                        throw Object.assign(new Error('Client must be connected before running operations'), { name: 'MongoNotConnectedError' })
                    },
                })
                const value = Reflect.get(target, prop)
                return typeof value === 'function' ? value.bind(target) : value
            },
        })
        const result = await handler.updateExistingMessages(LID, PHONE)
        expect(result).toMatchObject({ updated: 0, complete: false })
    })

    it('reports an incomplete pass when more rows than the row budget exist', async () => {
        await messages(h).insertMany([row('b1'), row('b2'), row('b3')])
        const result = await handler.updateExistingMessages(LID, PHONE, { maxRows: 2 })
        expect(result).toEqual({ updated: 2, skipped: 0, remaining: 0, complete: false })
        const next = await handler.updateExistingMessages(LID, PHONE, { maxRows: 2 })
        expect(next).toEqual({ updated: 1, skipped: 0, remaining: 0, complete: true })
    })

    it('reports scanned rows left unprocessed when the time budget runs out', async () => {
        await messages(h).insertMany([row('m1'), row('m2'), row('m3')])
        interceptUpdate(async () => {
            await new Promise((resolve) => setTimeout(resolve, 40))
        })
        const result = await handler.updateExistingMessages(LID, PHONE, { maxMs: 60 })
        expect(result.complete).toBe(false)
        expect(result.updated + result.remaining).toBeGreaterThanOrEqual(1)
    })
})
