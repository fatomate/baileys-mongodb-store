// Transport-only doubles for BullMQ and ioredis. The store's real queue processors are
// captured when it constructs a Worker, and queued job payloads are recorded so tests can
// replay them through a JSON round trip, exactly as Redis would deliver them.
export const processors = new Map<string, (job: any) => Promise<any>>()
export const added: { queue: string; name: string; data: any }[] = []

export const reset = () => {
    processors.clear()
    added.length = 0
}

class FakeQueue {
    constructor(public name: string) {}
    async add(name: string, data: any) {
        added.push({ queue: this.name, name, data })
        return { id: String(added.length), name, data }
    }
    async obliterate() {}
    async close() {}
    async removeRepeatableByKey() {}
    async getRepeatableJobs() { return [] }
    on() { return this }
}

class FakeWorker {
    constructor(public name: string, processor: (job: any) => Promise<any>) {
        processors.set(name, processor)
    }
    on() { return this }
    off() { return this }
    setMaxListeners() { return this }
    removeAllListeners() { return this }
    async close() {}
}

export const bullmqMock = { Queue: FakeQueue, Worker: FakeWorker, Job: class {} }

class FakeRedis {
    status = 'ready'
    async ping() { return 'PONG' }
    async config() { return ['maxmemory-policy', 'noeviction'] }
    async hgetall() { return {} }
    async hset() { return 0 }
    async hdel() { return 0 }
    async get() { return null }
    async set() { return 'OK' }
    async del() { return 0 }
    async expire() { return 0 }
    pipeline() { return { hset: () => undefined, hdel: () => undefined, exec: async () => [] } }
    setMaxListeners() { return this }
    on() { return this }
    disconnect() {}
    async quit() { return 'OK' }
}

export const ioredisMock = { __esModule: true, default: FakeRedis, Redis: FakeRedis }

/** Deliver a recorded job the way Redis does: JSON-serialized, then parsed by the worker. */
export const deliver = (data: any) => ({ id: 'job-1', attemptsMade: 0, data: JSON.parse(JSON.stringify(data)) })
