// WAB-978: a closed store, a duplicate store or a closed pool must not lose LID mappings
// or break other stores that share the pool. Real mongod; shared ConnectionManager pools.
/* eslint-disable @typescript-eslint/no-var-requires */
jest.mock('baileys', () => require('./helpers/baileysDouble').baileysMock)
jest.mock('baileys/lib/Types/LabelAssociation', () => ({ LabelAssociationType: { Chat: 'label_jid', Message: 'label_message' } }), { virtual: true })

import { EventEmitter } from 'events'
import { Collection, MongoClient } from 'mongodb'
import { burstTotal } from '../utils/burstLog'
import { DB, Harness, PREFIX, startMongo } from './helpers/storeHarness'
import { makeEnhancedMongoDBStore } from '../makeEnhancedMongoDBStore'
import { ConnectionManager } from '../utils/connectionManager'
import { LidHandler } from '../utils/lidHandler'

jest.setTimeout(60000)

let h: Harness
let seq = 0
const errors: string[] = []
const warnings: string[] = []
const onWarning = (w: Error) => warnings.push(`${w.name}: ${w.message}`)

const lid = (i: number) => `${100000000000000 + i}@lid`
const pn = (i: number) => `60${100000000 + i}@s.whatsapp.net`
const newInstance = () => `wab978-${++seq}-${Date.now()}`

const cfg = (instanceId: string, extra: Record<string, unknown> = {}) => ({
    uri: h.uri,
    database: DB,
    instanceId,
    collectionPrefix: PREFIX,
    useSharedConnections: true,
    debounceHistoryEvents: false,
    logLevel: 'error' as const,
    lidHandler: { enableCache: false },
    events: { 'lid-mapping.update': { enabled: true } },
    ...extra,
})

const storedLids = (instanceId: string) =>
    h.raw.db(DB).collection(`${PREFIX}contacts`).countDocuments({ instanceId, lid: { $type: 'string' } })

const waitForCount = async (instanceId: string, n: number, timeoutMs = 20000) => {
    const deadline = Date.now() + timeoutMs
    let last = -1
    while (Date.now() < deadline) {
        last = await storedLids(instanceId)
        if (last >= n) return last
        await new Promise((r) => setTimeout(r, 50))
    }
    throw new Error(`expected ${n} stored mappings for ${instanceId}, found ${last}`)
}

const emitBurst = (ev: EventEmitter, n: number, offset = 0) => {
    for (let i = 0; i < n; i++) ev.emit('lid-mapping.update', { lid: lid(offset + i), pn: pn(offset + i) })
}

const lidWriteFailures = () => errors.filter((e) => /LID mapping|lid-mapping\.update/.test(e))
const listenerTotal = (ev: EventEmitter) => ev.eventNames().reduce((n, name) => n + ev.listenerCount(name), 0)
const deferred = () => {
    let resolve!: () => void
    const promise = new Promise<void>((r) => { resolve = r })
    return { promise, resolve }
}

const stores: any[] = []
const make = async (instanceId: string, extra?: Record<string, unknown>) => {
    const s: any = await makeEnhancedMongoDBStore(cfg(instanceId, extra) as any)
    stores.push(s)
    return s
}

beforeAll(async () => {
    h = await startMongo()
    process.on('warning', onWarning)
})
afterAll(async () => {
    process.off('warning', onWarning)
    await ConnectionManager.getInstance().shutdown()
    await h.stop()
})
beforeEach(async () => {
    errors.length = 0
    warnings.length = 0
    jest.spyOn(console, 'error').mockImplementation((...a: any[]) => {
        errors.push(a.map((x) => (x instanceof Error ? `${x.name}: ${x.message}` : String(x))).join(' '))
    })
    jest.spyOn(console, 'warn').mockImplementation(() => {})
    jest.spyOn(console, 'log').mockImplementation(() => {})
    jest.spyOn(console, 'debug').mockImplementation(() => {})
    await ConnectionManager.getInstance().shutdown()
})
afterEach(async () => {
    while (stores.length) {
        const s = stores.pop()
        try { s.unbind() } catch { /* already unbound */ }
        await s.close().catch(() => {})
    }
    jest.restoreAllMocks()
})

describe('WAB-978 store lifecycle', () => {
    test('R0: close() removes every listener the store added to its emitter', async () => {
        const store = await make(newInstance())
        const ev = new EventEmitter()
        store.bind(ev)
        expect(listenerTotal(ev)).toBeGreaterThan(2)

        await store.close()

        expect(ev.eventNames().filter((name) => ev.listenerCount(name) > 0)).toEqual([])
    })

    test('R1: an operation on a closed store rejects and does not break another store on the shared pool', async () => {
        const a = await make(newInstance())
        const bId = newInstance()
        const b = await make(bId)
        const evB = new EventEmitter()
        b.bind(evB)

        await a.close()
        await expect(a.getChats()).rejects.toMatchObject({ name: 'StoreClosedError' })

        emitBurst(evB, 200)
        await waitForCount(bId, 200)
        expect(lidWriteFailures()).toEqual([])
    })

    test('R0+R1: events on the former emitter of a closed store do not break another tenant', async () => {
        const instanceId = newInstance()
        const otherId = newInstance()
        const other = await make(otherId)
        const evOther = new EventEmitter()
        other.bind(evOther)
        const ev = new EventEmitter()
        const oldStore = await make(instanceId)
        oldStore.bind(ev)
        await oldStore.close()
        const newStore = await make(instanceId)
        newStore.bind(ev)

        emitBurst(ev, 200)
        await waitForCount(instanceId, 200)
        emitBurst(evOther, 200, 10000)
        await waitForCount(otherId, 200)
        expect(lidWriteFailures()).toEqual([])
    })

    test('R2: closing a duplicate store for the same instance keeps the live store writing', async () => {
        const instanceId = newInstance()
        const live = await make(instanceId)
        const duplicate = await make(instanceId)
        const ev = new EventEmitter()
        live.bind(ev)

        emitBurst(ev, 500)
        await duplicate.close()

        await waitForCount(instanceId, 500)
        expect(lidWriteFailures()).toEqual([])
    })

    test.each([
        ['the manager closes the pool', async (cm: ConnectionManager, poolId: string) => { await (cm as any).closePool(poolId) }],
        ['the pool client closes', async (cm: ConnectionManager, poolId: string) => { await (cm as any).pools.get(poolId).client.close() }],
    ])('R3/R4/R5: when %s during a burst, every mapping persists after one reconnect', async (_label, closeIt) => {
        const instanceId = newInstance()
        const store = await make(instanceId)
        const ev = new EventEmitter()
        store.bind(ev)
        const cm = ConnectionManager.getInstance()
        const poolId = cm.getInstancePoolState(instanceId).currentPoolId!
        expect(poolId).toBeDefined()
        const register = jest.spyOn(cm, 'registerInstance')

        emitBurst(ev, 500)
        await closeIt(cm, poolId)

        await waitForCount(instanceId, 500)
        const registrations = register.mock.calls.filter(([r]) => r.instanceId === instanceId).length
        expect(registrations).toBe(1)
        expect(warnings.filter((w) => w.includes('MaxListenersExceededWarning'))).toEqual([])
        expect(errors.filter((e) => e.includes('Failed to store LID mapping'))).toEqual([])
    })

    test('close during a shared reconnect: the store ends closed, releases its lease, and the pool stays usable', async () => {
        const instanceId = newInstance()
        const otherId = newInstance()
        const store = await make(instanceId)
        const other = await make(otherId)
        const evOther = new EventEmitter()
        other.bind(evOther)
        const cm = ConnectionManager.getInstance()
        const original = cm.registerInstance.bind(cm)
        const gate = deferred()
        const entered = deferred()
        jest.spyOn(cm, 'registerInstance').mockImplementation(async (r) => {
            if (r.instanceId === instanceId) {
                entered.resolve()
                await gate.promise
            }
            return original(r)
        })

        const pending = store.reconnect().catch((e: Error) => e)
        await entered.promise
        const closing = store.close()
        gate.resolve()
        await closing
        await pending

        expect(cm.getInstancePoolState(instanceId).currentPoolId).toBeUndefined()
        await expect(store.getChats()).rejects.toMatchObject({ name: 'StoreClosedError' })
        emitBurst(evOther, 100)
        await waitForCount(otherId, 100)
    })

    test('close during a dedicated reconnect: the late client is closed', async () => {
        const instanceId = newInstance()
        const store = await make(instanceId, { useSharedConnections: false })
        const clients: MongoClient[] = []
        const originalConnect = MongoClient.prototype.connect
        const gate = deferred()
        const entered = deferred()
        jest.spyOn(MongoClient.prototype, 'connect').mockImplementation(async function (this: MongoClient) {
            clients.push(this)
            entered.resolve()
            await gate.promise
            return originalConnect.call(this)
        })

        const pending = store.reconnect().catch((e: Error) => e)
        await entered.promise
        const closing = store.close()
        gate.resolve()
        await closing
        await pending

        expect(clients.length).toBeGreaterThan(0)
        for (const c of clients) expect((c as any).topology).toBeFalsy()
        await expect(store.getChats()).rejects.toMatchObject({ name: 'StoreClosedError' })
    })

    test('C1: lid-mapping.update persistence runs at most 16 at a time and stores every event', async () => {
        let active = 0
        let peak = 0
        const original = LidHandler.prototype.handleLidMappingUpdate
        jest.spyOn(LidHandler.prototype, 'handleLidMappingUpdate').mockImplementation(async function (this: LidHandler, data) {
            active++
            peak = Math.max(peak, active)
            try { return await original.call(this, data) } finally { active-- }
        })
        const instanceId = newInstance()
        const store = await make(instanceId)
        const ev = new EventEmitter()
        store.bind(ev)

        emitBurst(ev, 500)

        await waitForCount(instanceId, 500)
        expect(peak).toBeGreaterThan(1)
        expect(peak).toBeLessThanOrEqual(16)
    })

    test('C1: close() drains accepted mappings, ignores later events, and leaves other owners usable', async () => {
        const handled: string[] = []
        const original = LidHandler.prototype.handleLidMappingUpdate
        jest.spyOn(LidHandler.prototype, 'handleLidMappingUpdate').mockImplementation(async function (this: LidHandler, data) {
            handled.push(data.lid)
            return original.call(this, data)
        })
        const instanceId = newInstance()
        const otherId = newInstance()
        const store = await make(instanceId)
        const other = await make(otherId)
        const ev = new EventEmitter()
        const evOther = new EventEmitter()
        store.bind(ev)
        other.bind(evOther)

        emitBurst(ev, 300)
        const closing = store.close()
        emitBurst(ev, 50, 5000)
        await closing

        expect(await storedLids(instanceId)).toBe(300)
        expect(handled.filter((l) => l >= lid(5000))).toEqual([])
        emitBurst(evOther, 100, 20000)
        await waitForCount(otherId, 100)
    })

    test('close() uses one 15 s deadline for the drain and a blocked reconnect together', async () => {
        const instanceId = newInstance()
        const store = await make(instanceId)
        const ev = new EventEmitter()
        store.bind(ev)
        const cm = ConnectionManager.getInstance()
        const original = cm.registerInstance.bind(cm)
        jest.spyOn(cm, 'registerInstance').mockImplementation(async (r) => {
            if (r.instanceId === instanceId) await new Promise(() => {}) // Mongo never answers
            return original(r)
        })
        store.reconnect().catch(() => {})
        emitBurst(ev, 5)

        const started = Date.now()
        await store.close()
        const elapsed = Date.now() - started

        expect(elapsed).toBeGreaterThanOrEqual(14000)
        expect(elapsed).toBeLessThan(20000)
        expect(errors.some((e) => e.includes('close() stopped waiting after 15s'))).toBe(true)
    })

    test('close() keeps the 15 s deadline when a counted database operation never settles', async () => {
        const instanceId = newInstance()
        const store = await make(instanceId)
        const ev = new EventEmitter()
        store.bind(ev)
        // The pool counts this operation as active, and Mongo never answers it
        jest.spyOn(Collection.prototype, 'updateOne').mockImplementation(() => new Promise(() => {}) as any)
        emitBurst(ev, 1)
        await new Promise((r) => setTimeout(r, 300))

        const started = Date.now()
        await store.close()
        const elapsed = Date.now() - started

        expect(elapsed).toBeGreaterThanOrEqual(14000)
        expect(elapsed).toBeLessThan(20000)
        expect(ConnectionManager.getInstance().getInstancePoolState(instanceId).currentPoolId).toBeUndefined()
    })

    test('a burst of failing mapping events is counted under one log label only', async () => {
        const label = '[LidHandler] Failed to store LID mapping'
        const instanceId = newInstance()
        const store = await make(instanceId)
        const ev = new EventEmitter()
        store.bind(ev)
        const before = burstTotal(label)
        const withConnectionLabel = `[withConnection] [${instanceId}] Operation failed after retries`
        const eventLabel = `[${instanceId}] Failed to process lid-mapping.update`
        jest.spyOn(Collection.prototype, 'updateOne').mockRejectedValue(
            Object.assign(new Error('write refused'), { name: 'MongoServerError', code: 8000 })
        )

        emitBurst(ev, 50)
        await new Promise((r) => setTimeout(r, 1500))

        // Each event fails twice: the LidHandler write and the contact update
        expect(burstTotal(label) - before).toBe(100)
        expect(burstTotal(withConnectionLabel)).toBe(0)
        expect(burstTotal(eventLabel)).toBe(0)
    })
})
