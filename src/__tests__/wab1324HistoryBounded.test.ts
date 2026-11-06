// WAB-1324 acceptance: a history sync must not start one upsert per message at once.
// The handler ran Promise.all over every message of the merged batch, so a 150k-message
// history put 150k deep clones and MongoDB operations in flight and the engine ran out of heap.
// Real mongod; the public upsertMessage is wrapped only to count calls in flight.
/* eslint-disable @typescript-eslint/no-var-requires */
jest.mock('baileys', () => require('./helpers/baileysDouble').baileysMock)
jest.mock('baileys/lib/Types/LabelAssociation', () => ({ LabelAssociationType: { Chat: 'label_jid', Message: 'label_message' } }), { virtual: true })

import { EventEmitter } from 'events'
import { Harness, JID, S, baseStoreConfig, messages, startMongo } from './helpers/storeHarness'
import { makeEnhancedMongoDBStore } from '../makeEnhancedMongoDBStore'

jest.setTimeout(60000)

const DEFAULT_LIMIT = 10

let h: Harness

const historyMessage = (id: string) => ({
    key: { id, remoteJid: JID, fromMe: false },
    message: { conversation: id },
    messageTimestamp: S - 1000,
})

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

/** Counts the store's upsertMessage calls in flight. `gate(id)` may hold a call before it writes. */
const countInFlight = (store: any, gate: (id: string) => Promise<void> = async () => {}) => {
    const original = store.upsertMessage
    const counter = { now: 0, max: 0 }
    jest.spyOn(store, 'upsertMessage').mockImplementation(async (...args: any[]) => {
        counter.now++
        counter.max = Math.max(counter.max, counter.now)
        try {
            await gate(args[1]?.key?.id)
            return await original(...args)
        } finally {
            counter.now--
        }
    })
    return counter
}

const waitForCount = async (instanceId: string, expected: number, timeoutMs = 30000) => {
    const deadline = Date.now() + timeoutMs
    for (;;) {
        const n = await messages(h).countDocuments({ instanceId })
        if (n >= expected) return n
        if (Date.now() > deadline) throw new Error(`stored ${n} of ${expected} history messages within ${timeoutMs} ms`)
        await sleep(50)
    }
}

beforeAll(async () => {
    h = await startMongo()
})
afterAll(async () => {
    await h.stop()
})
afterEach(() => {
    jest.restoreAllMocks()
})

it(`stores every message of a 1,000-message history with at most ${DEFAULT_LIMIT} upserts in flight`, async () => {
    const instanceId = 'inst-wab1324-a'
    const store: any = await makeEnhancedMongoDBStore({ ...baseStoreConfig(h.uri), instanceId } as any)
    const ev = new EventEmitter()
    try {
        const counter = countInFlight(store)
        store.bind(ev)
        const batch = Array.from({ length: 1000 }, (_, i) => historyMessage(`a${i}`))
        ev.emit('messaging-history.set', { chats: [], contacts: [], messages: batch, isLatest: false })

        expect(await waitForCount(instanceId, 1000)).toBe(1000)
        expect(counter.max).toBeGreaterThan(0)
        expect(counter.max).toBeLessThanOrEqual(DEFAULT_LIMIT)
    } finally {
        store.unbind()
        await store.close()
    }
})

it('clearAll drops queued history writes, so cleared history does not come back', async () => {
    const instanceId = 'inst-wab1324-c'
    const store: any = await makeEnhancedMongoDBStore({ ...baseStoreConfig(h.uri), instanceId, historyWriteConcurrency: 5 } as any)
    const ev = new EventEmitter()
    let open!: () => void
    const gate = new Promise<void>((resolve) => { open = resolve })
    try {
        countInFlight(store, () => gate)
        store.bind(ev)
        ev.emit('messaging-history.set', {
            chats: [], contacts: [], isLatest: false,
            messages: Array.from({ length: 50 }, (_, i) => historyMessage(`old${i}`)),
        })
        await sleep(200) // 5 writes wait at the gate, 45 wait in the queue
        const cleared = store.clearAll({ preserve: ['contacts'] })
        await sleep(100)
        open()
        await cleared
        await sleep(500)
        expect(await messages(h).countDocuments({ instanceId })).toBe(0)
    } finally {
        open?.()
        store.unbind()
        await store.close()
    }
})

it('keeps one limit per store across two history batches that overlap', async () => {
    const instanceId = 'inst-wab1324-b'
    const store: any = await makeEnhancedMongoDBStore({ ...baseStoreConfig(h.uri), instanceId, historyWriteConcurrency: 5 } as any)
    const ev = new EventEmitter()
    let open!: () => void
    const firstBatchGate = new Promise<void>((resolve) => { open = resolve })
    try {
        // The first batch's writes wait at the gate, so that batch is still running when the second starts.
        const counter = countInFlight(store, (id) => (id.startsWith('first') ? firstBatchGate : Promise.resolve()))
        store.bind(ev)
        ev.emit('messaging-history.set', {
            chats: [], contacts: [], isLatest: false,
            messages: Array.from({ length: 20 }, (_, i) => historyMessage(`first${i}`)),
        })
        await sleep(200)
        ev.emit('messaging-history.set', {
            chats: [], contacts: [], isLatest: false,
            messages: Array.from({ length: 20 }, (_, i) => historyMessage(`second${i}`)),
        })
        await sleep(200)
        expect(counter.max).toBeLessThanOrEqual(5)

        open()
        expect(await waitForCount(instanceId, 40)).toBe(40)
        expect(counter.max).toBeGreaterThan(0)
        expect(counter.max).toBeLessThanOrEqual(5)
    } finally {
        open?.()
        store.unbind()
        await store.close()
    }
})
