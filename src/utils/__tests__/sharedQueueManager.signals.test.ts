// WAB-1155: an application that owns SIGINT/SIGTERM (wabot.js engines) must be able to turn off
// the manager's own signal handler, which shuts the queues down and calls process.exit(0) before
// the application can drain. The default stays as it was, so other users keep their behaviour.
// Redis and BullMQ are fakes: these tests check signal wiring only.
/* eslint-disable @typescript-eslint/no-var-requires */
jest.mock('ioredis', () => {
    class FakeRedis {
        on() { return this }
        setMaxListeners() { return this }
        disconnect() { /* fake */ }
        duplicate() { return new FakeRedis() }
    }
    return { __esModule: true, default: FakeRedis, Redis: FakeRedis }
})
jest.mock('bullmq', () => {
    class Closable {
        on() { return this }
        setMaxListeners() { return this }
        async close() { /* fake */ }
    }
    return { Queue: Closable, Worker: Closable, QueueEvents: Closable, Job: class {} }
})

import { SharedQueueManager, shutdownSharedQueueManager } from '../sharedQueueManager'

const SIGNALS = ['SIGINT', 'SIGTERM'] as const

const newManager = (config: Record<string, unknown> = {}) =>
    new (SharedQueueManager as any)({ redis: { connection: 'redis://127.0.0.1:1/0' }, logLevel: 'none', ...config })

let before: Record<string, Function[]>
beforeEach(() => {
    before = Object.fromEntries(SIGNALS.map((s) => [s, process.listeners(s)]))
})
afterEach(() => {
    for (const s of SIGNALS) {
        for (const l of process.listeners(s)) if (!before[s].includes(l)) process.removeListener(s, l as any)
    }
    SharedQueueManager.resetInstance()
})

const added = (s: string) => process.listeners(s as any).filter((l) => !before[s].includes(l)).length

test('by default the manager installs its own SIGINT and SIGTERM handler (unchanged behaviour)', () => {
    newManager()
    expect(added('SIGINT')).toBe(1)
    expect(added('SIGTERM')).toBe(1)
})

test('handleProcessSignals: false installs no signal handler', () => {
    newManager({ handleProcessSignals: false })
    expect(added('SIGINT')).toBe(0)
    expect(added('SIGTERM')).toBe(0)
})

test('shutdownSharedQueueManager shuts the singleton down once and is a no-op without one', async () => {
    await expect(shutdownSharedQueueManager()).resolves.toBe(false)

    const manager = SharedQueueManager.getInstance({
        redis: { connection: 'redis://127.0.0.1:1/0' }, logLevel: 'none', handleProcessSignals: false,
    })
    const shutdown = jest.spyOn(manager, 'shutdown')
    await expect(shutdownSharedQueueManager()).resolves.toBe(true)
    expect(shutdown).toHaveBeenCalledTimes(1)
})
