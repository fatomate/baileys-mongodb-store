// WAB-866: SharedQueueManager.shutdown() must stop only this manager's workers. It must not
// pause a shared queue for every worker (BullMQ Queue.pause sets meta.paused globally), and it
// must not wait for jobs that other managers hold. Real Redis and BullMQ; managers with their
// own connections, plus one child process stopped by SIGTERM.
// The suite ERASES the selected Redis database (FLUSHDB) before each test, so it runs only with
// explicit consent and a non-default local port:
//   docker run -d --rm --name wab866-redis -p 127.0.0.1:6390:6379 redis:7
//   WAB866_REDIS_DISPOSABLE=1 WAB866_REDIS_URL=redis://127.0.0.1:6390/0 \
//     npx jest src/utils/__tests__/sharedQueueManager.shutdown.int.test.ts
import { spawn } from 'child_process'
import path from 'path'
import Redis from 'ioredis'
import { Queue } from 'bullmq'
import { JobType, SharedQueueManager, SharedQueueName } from '../sharedQueueManager'

const REDIS_URL = process.env.WAB866_REDIS_URL
const localUrl = REDIS_URL?.match(/^redis:\/\/(?:127\.0\.0\.1|localhost):(\d+)\/\d+$/)
// Compare the port as a number: ioredis parses '06379' as 6379.
const isDisposable = process.env.WAB866_REDIS_DISPOSABLE === '1' && !!localUrl && Number(localUrl[1]) !== 6379
const describeIfRedis = isDisposable ? describe : describe.skip

jest.setTimeout(60000)

const META = `bull:${SharedQueueName.MEDIA}:meta`

const newManager = (workerId: string): SharedQueueManager =>
    new (SharedQueueManager as any)({
        redis: { connection: REDIS_URL },
        workerId,
        logLevel: 'none',
        queueConcurrency: { media: 2 },
        ownership: { conflictDelayMs: 100 },
    })

const waitFor = async (probe: () => Promise<boolean> | boolean, timeoutMs = 10000) => {
    const deadline = Date.now() + timeoutMs
    while (!(await probe())) {
        if (Date.now() > deadline) throw new Error('condition not met in time')
        await new Promise((r) => setTimeout(r, 25))
    }
}

const settledWithin = async (p: Promise<unknown>, ms: number) =>
    Promise.race([p.then(() => true), new Promise<boolean>((r) => setTimeout(() => r(false), ms))])

describeIfRedis('WAB-866 SharedQueueManager.shutdown() quiesces only local workers', () => {
    let admin: Redis
    const live: SharedQueueManager[] = []
    const releases: Array<() => void> = []

    const start = (workerId: string) => {
        const m = newManager(workerId)
        live.push(m)
        return m
    }

    // Registers a MEDIA_DOWNLOAD processor that records calls and holds each job until released.
    const holdingProcessor = (m: SharedQueueManager, instanceId: string) => {
        const calls: string[] = []
        const pending: Array<() => void> = []
        m.registerInstanceProcessor(instanceId, JobType.MEDIA_DOWNLOAD, async (job) => {
            calls.push(String(job.data.data?.tag))
            await new Promise<void>((release) => {
                pending.push(release)
                releases.push(release)
            })
            return { ok: true }
        })
        return {
            calls,
            releaseAll: () => pending.splice(0).forEach((r) => r()),
        }
    }

    beforeAll(async () => {
        admin = new Redis(REDIS_URL!)
    })
    afterAll(async () => {
        admin.disconnect()
    })
    beforeEach(async () => {
        await admin.flushdb()
    })
    afterEach(async () => {
        releases.splice(0).forEach((r) => r())
        await Promise.allSettled(live.splice(0).map((m) => m.shutdown()))
    })

    it('stops taking jobs, finishes its active job, and never sets a global pause', async () => {
        const a = start('worker-a')
        const proc = holdingProcessor(a, 'inst-a')

        await a.addJob(JobType.MEDIA_DOWNLOAD, { tag: 'a1' }, 'inst-a')
        await waitFor(() => proc.calls.length === 1)

        const shutdown = a.shutdown()
        await a.addJob(JobType.MEDIA_DOWNLOAD, { tag: 'a2' }, 'inst-a').catch(() => undefined)

        expect(await settledWithin(shutdown, 500)).toBe(false) // waits for its own active job
        proc.releaseAll()
        expect(await settledWithin(shutdown, 10000)).toBe(true)

        expect(proc.calls).toEqual(['a1']) // took no new job after shutdown began
        expect(await admin.hget(META, 'paused')).toBeNull()
    })

    it('does not wait for jobs held by another manager, which keeps processing', async () => {
        const b = start('worker-b')
        const procB = holdingProcessor(b, 'inst-b')
        await b.addJob(JobType.MEDIA_DOWNLOAD, { tag: 'b1' }, 'inst-b')
        await waitFor(() => procB.calls.length === 1)

        const a = start('worker-a')
        holdingProcessor(a, 'inst-a')
        await new Promise((r) => setTimeout(r, 300)) // let A's workers start

        expect(await settledWithin(a.shutdown(), 5000)).toBe(true)
        expect(await admin.hget(META, 'paused')).toBeNull()

        await b.addJob(JobType.MEDIA_DOWNLOAD, { tag: 'b2' }, 'inst-b')
        await waitFor(() => procB.calls.length === 2)
        expect(procB.calls).toEqual(['b1', 'b2'])
    })

    // Backfill guard: shutdown must not resume a pause that an operator set.
    it('keeps a pause that was already set before shutdown', async () => {
        const client = new Redis(REDIS_URL!, { maxRetriesPerRequest: null })
        const queue = new Queue(SharedQueueName.MEDIA, { connection: client })
        try {
            await queue.pause()
        } finally {
            await queue.close()
            client.disconnect()
        }

        const a = start('worker-a')
        holdingProcessor(a, 'inst-a')
        await a.shutdown()

        expect(await admin.hget(META, 'paused')).toBe('1')
    })

    it('on SIGTERM, finishes the active job, exits 0, and leaves the queue unpaused', async () => {
        const child = spawn(process.execPath, [path.join(__dirname, 'helpers', 'shutdownChild.js')], {
            env: { ...process.env, WAB866_REDIS_URL: REDIS_URL },
            stdio: ['ignore', 'pipe', 'inherit'],
        })
        const lines: string[] = []
        child.stdout.on('data', (chunk: Buffer) => lines.push(...chunk.toString().split('\n').filter(Boolean)))
        const exited = new Promise<number | null>((resolve) => child.on('exit', (code) => resolve(code)))

        try {
            await waitFor(() => lines.includes('started'), 20000)
            child.kill('SIGTERM')
            const code = await Promise.race([
                exited,
                new Promise<never>((_, reject) => setTimeout(() => reject(new Error('child did not exit within 20 s')), 20000)),
            ])
            expect(code).toBe(0)
        } finally {
            if (child.exitCode === null && child.signalCode === null) {
                child.kill('SIGKILL')
                await exited
            }
        }

        expect(lines).toEqual(['started', 'finished', 'exit 0'])
        expect(await admin.hget(META, 'paused')).toBeNull()
    })
})
