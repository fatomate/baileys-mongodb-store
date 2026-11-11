// WAB-1324 acceptance: a history sync must not start one upsert per message at once.
// The handler ran Promise.all over every message of the merged batch, so a 150k-message
// history put 150k deep clones and MongoDB operations in flight and the engine ran out of heap.
// Real mongod; the public upsertMessage is wrapped only to count calls in flight.
/* eslint-disable @typescript-eslint/no-require-imports */
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

const deferred = () => {
    let resolve!: () => void
    const promise = new Promise<void>((r) => { resolve = r })
    return { promise, resolve }
}

// Review round 1 (R1-1): a clear that keeps the messages must keep the history still queued.
it('clearAll that preserves messages keeps queued history writes', async () => {
    const instanceId = 'inst-wab1324-d'
    const store: any = await makeEnhancedMongoDBStore({ ...baseStoreConfig(h.uri), instanceId, historyWriteConcurrency: 1 } as any)
    const ev = new EventEmitter()
    const started = deferred()
    const gate = deferred()
    try {
        countInFlight(store, async () => { started.resolve(); await gate.promise })
        store.bind(ev)
        ev.emit('messaging-history.set', {
            chats: [], contacts: [], isLatest: false,
            messages: Array.from({ length: 5 }, (_, i) => historyMessage(`keep${i}`)),
        })
        await started.promise
        const cleared = store.clearAll({ preserve: ['messages', 'chats', 'contacts', 'presences'] })
        gate.resolve()
        await cleared
        expect(await waitForCount(instanceId, 5, 5000)).toBe(5)
    } finally {
        gate.resolve()
        store.unbind()
        await store.close()
    }
})

// Review round 1 (R1-2): an isLatest batch that calls clearAll during another clear must wait for that clear,
// not write inside its delete window and lose its messages.
it('an isLatest history batch during another clearAll keeps its messages', async () => {
    const instanceId = 'inst-wab1324-e'
    const store: any = await makeEnhancedMongoDBStore({ ...baseStoreConfig(h.uri), instanceId, historyWriteConcurrency: 1, clearAllOnHistorySync: true } as any)
    const ev = new EventEmitter()
    const oldStarted = deferred()
    const gate = deferred()
    const clearInLatest = deferred()
    const originalClear = store.clearAll
    let clearCalls = 0
    jest.spyOn(store, 'clearAll').mockImplementation((...args: any[]) => {
        clearCalls++
        const result = originalClear(...args)
        if (clearCalls === 2) clearInLatest.resolve()
        return result
    })
    try {
        countInFlight(store, async (id) => {
            if (id.startsWith('old')) { oldStarted.resolve(); await gate.promise }
        })
        store.bind(ev)
        ev.emit('messaging-history.set', { chats: [], contacts: [], isLatest: false, messages: [historyMessage('old1'), historyMessage('old2')] })
        await oldStarted.promise
        const cleared = store.clearAll({ preserve: ['contacts'] })
        ev.emit('messaging-history.set', { chats: [], contacts: [], isLatest: true, messages: [historyMessage('latest1')] })
        await clearInLatest.promise
        await sleep(20)
        gate.resolve()
        await cleared
        await sleep(200)
        expect(await messages(h).countDocuments({ instanceId, 'key.id': 'latest1' })).toBe(1)
    } finally {
        gate.resolve()
        store.unbind()
        await store.close()
    }
})

// PR review (Codex P2): close() must not leave the queued history to start after it returns. Accepted history that
// is not written is lost at close, as before the limiter (owner decision: no drain).
it('close cancels queued history writes', async () => {
    const instanceId = 'inst-wab1324-f'
    const store: any = await makeEnhancedMongoDBStore({ ...baseStoreConfig(h.uri), instanceId, historyWriteConcurrency: 1 } as any)
    const ev = new EventEmitter()
    const started = deferred()
    const gate = deferred()
    let calls = 0
    try {
        countInFlight(store, async () => { calls++; started.resolve(); await gate.promise })
        store.bind(ev)
        ev.emit('messaging-history.set', {
            chats: [], contacts: [], isLatest: false,
            messages: Array.from({ length: 20 }, (_, i) => historyMessage(`closing${i}`)),
        })
        await started.promise
        const closed = store.close()
        await sleep(50)
        gate.resolve()
        await closed
        await sleep(300)
        expect(calls).toBe(1)
    } finally {
        gate.resolve()
        await store.close()
    }
})

const within = async <T>(promise: Promise<T>, ms = 1500): Promise<T> => {
    let timer: NodeJS.Timeout | undefined
    try {
        return await Promise.race([promise, new Promise<T>((_, reject) => { timer = setTimeout(() => reject(new Error('test deadline')), ms) })])
    } finally {
        clearTimeout(timer)
    }
}

// Review round 3 (R3-1): close() must also stop history that was still being admitted or waiting for a clear.
it('close does not drain the queue and leaves no unhandled rejection', async () => {
    const instanceId = 'inst-wab1324-g'
    const store: any = await makeEnhancedMongoDBStore({ ...baseStoreConfig(h.uri), instanceId, historyWriteConcurrency: 1 } as any)
    const ev = new EventEmitter()
    const started = deferred()
    const gate = deferred()
    const unhandled: unknown[] = []
    const onUnhandled = (error: unknown) => { unhandled.push(error) }
    process.on('unhandledRejection', onUnhandled)
    let calls = 0
    try {
        countInFlight(store, async () => { calls++; started.resolve(); await gate.promise })
        store.bind(ev)
        ev.emit('messaging-history.set', {
            chats: [], contacts: [], isLatest: false,
            messages: Array.from({ length: 20 }, (_, i) => historyMessage(`queued${i}`)),
        })
        await within(started.promise)
        await within(store.close()) // the gate still holds the running write: close must not wait for the queue
        expect(calls).toBe(1)
        gate.resolve()
        await sleep(100)
        expect(calls).toBe(1)
        expect(unhandled).toHaveLength(0)
    } finally {
        gate.resolve()
        process.removeListener('unhandledRejection', onUnhandled)
        await store.close()
    }
})

it('history still being admitted when close() runs is never scheduled', async () => {
    const instanceId = 'inst-wab1324-h'
    const store: any = await makeEnhancedMongoDBStore({ ...baseStoreConfig(h.uri), instanceId, debounceHistoryEvents: true, debounceDelay: 500, historyWriteConcurrency: 1 } as any)
    const ev = new EventEmitter()
    let calls = 0
    countInFlight(store, async () => { calls++ })
    try {
        store.bind(ev)
        ev.emit('messaging-history.set', {
            chats: [], contacts: [], isLatest: false,
            messages: Array.from({ length: 20 }, (_, i) => historyMessage(`late${i}`)),
        })
        await within(store.close())
        expect(calls).toBe(0)
        await sleep(650)
        expect(calls).toBe(0)
    } finally {
        await store.close()
    }
})

it('a latest-history batch waiting for a clear enqueues nothing after close()', async () => {
    const instanceId = 'inst-wab1324-i'
    const store: any = await makeEnhancedMongoDBStore({ ...baseStoreConfig(h.uri), instanceId, historyWriteConcurrency: 1, clearAllOnHistorySync: true } as any)
    const ev = new EventEmitter()
    const started = deferred()
    const gate = deferred()
    const joined = deferred()
    const originalClear = store.clearAll
    let clearCalls = 0
    let latestCalls = 0
    jest.spyOn(store, 'clearAll').mockImplementation((...args: any[]) => {
        const result = originalClear(...args)
        if (++clearCalls === 2) joined.resolve()
        return result
    })
    countInFlight(store, async (id) => {
        if (id === 'old1') { started.resolve(); await gate.promise } else latestCalls++
    })
    let clearing: Promise<string> | undefined
    try {
        store.bind(ev)
        ev.emit('messaging-history.set', { chats: [], contacts: [], isLatest: false, messages: [historyMessage('old1')] })
        await within(started.promise)
        clearing = store.clearAll({ preserve: ['contacts'] }).then(() => 'resolved', (error: Error) => error.name)
        ev.emit('messaging-history.set', {
            chats: [], contacts: [], isLatest: true,
            messages: Array.from({ length: 20 }, (_, i) => historyMessage(`latest${i}`)),
        })
        await within(joined.promise)
        await within(store.close())
        expect(latestCalls).toBe(0)
        gate.resolve()
        await within(clearing as Promise<string>)
        await sleep(150)
        expect(latestCalls).toBe(0)
    } finally {
        gate.resolve()
        if (clearing) await within(clearing)
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
