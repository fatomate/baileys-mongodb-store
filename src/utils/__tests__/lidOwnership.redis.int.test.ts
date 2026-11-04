// WAB-1275: the Redis-side lookup fence (LID_CACHE_SCRIPTS) on a real Redis, with two handlers that
// share it as two engine processes would. Real mongod as well.
// The suite ERASES the selected Redis database (FLUSHDB) before each test, so it runs only with
// explicit consent and a non-default local port:
//   docker run -d --rm --name wab1275-redis -p 127.0.0.1:6391:6379 redis:7
//   WAB1275_REDIS_DISPOSABLE=1 WAB1275_REDIS_URL=redis://127.0.0.1:6391/0 \
//     npx jest src/utils/__tests__/lidOwnership.redis.int.test.ts --forceExit
import Redis from 'ioredis'
import { Db, MongoClient } from 'mongodb'
import { MongoMemoryServer } from 'mongodb-memory-server'
import { LID_CACHE_SCRIPTS, LidHandler } from '../lidHandler'

const REDIS_URL = process.env.WAB1275_REDIS_URL
const localUrl = REDIS_URL?.match(/^redis:\/\/(?:127\.0\.0\.1|localhost):(\d+)\/\d+$/)
const isDisposable = process.env.WAB1275_REDIS_DISPOSABLE === '1' && !!localUrl && Number(localUrl[1]) !== 6379
const describeIfRedis = isDisposable ? describe : describe.skip

jest.setTimeout(60000)

const INSTANCE = 'wab1275-redis'
const L = '987650001@lid'
const P1 = '60123450011@s.whatsapp.net'
const P2 = '60123450022@s.whatsapp.net'
const KEY = `lid:${INSTANCE}:${L}`

describeIfRedis('WAB-1275 Redis-side lookup fence (real Redis)', () => {
    let server: MongoMemoryServer
    let mongo: MongoClient
    let db: Db
    let admin: Redis
    const clients: Redis[] = []

    const makeHandler = async () => {
        const client = new Redis(REDIS_URL!)
        clients.push(client)
        const handler = new LidHandler(INSTANCE, { enableCache: true, cacheTTL: 60, redisClient: client, preferReverseLookupFirst: false })
        await handler.initialize(db, 'own_')
        return handler
    }

    beforeAll(async () => {
        server = await MongoMemoryServer.create()
        mongo = await MongoClient.connect(server.getUri())
        db = mongo.db('wab1275_redis')
        await db.collection('own_contacts').createIndex({ instanceId: 1, lid: 1 }, { unique: true, partialFilterExpression: { lid: { $type: 'string' } } })
        await db.collection('own_lidMappings').createIndex({ instanceId: 1, lid: 1 }, { unique: true })
        admin = new Redis(REDIS_URL!)
    })
    afterAll(async () => {
        clients.splice(0).forEach((c) => c.disconnect())
        admin.disconnect()
        await mongo.close()
        await server.stop()
    })
    beforeEach(async () => {
        await admin.flushdb()
        await db.collection('own_contacts').deleteMany({})
        await db.collection('own_lidMappings').deleteMany({})
        await db.collection('own_contacts').insertOne({ instanceId: INSTANCE, id: P1, lid: L })
        await db.collection('own_lidMappings').insertOne({ instanceId: INSTANCE, lid: L, phoneNumber: P1, firstSeen: new Date() })
        for (const level of ['log', 'debug', 'warn'] as const) jest.spyOn(console, level).mockImplementation(() => {})
    })
    afterEach(() => jest.restoreAllMocks())

    it('runs the scripts with hash-tagged keys: publish waits for no write in progress and an unchanged generation', async () => {
        const keys = [KEY, `{${KEY}}:gen`, `{${KEY}}:busy`]
        expect(await admin.eval(LID_CACHE_SCRIPTS.publish, 3, ...keys, '', P1, 60)).toBe(1)
        expect(await admin.get(KEY)).toBe(P1)

        await admin.eval(LID_CACHE_SCRIPTS.beginWrite, 3, ...keys, 3600, 300000)
        expect(await admin.get(KEY)).toBeNull()
        expect(await admin.pttl(keys[2])).toBeGreaterThan(0)
        expect(await admin.ttl(keys[1])).toBeGreaterThan(0)
        expect(await admin.eval(LID_CACHE_SCRIPTS.publish, 3, ...keys, '1', P1, 60)).toBe(0)

        await admin.eval(LID_CACHE_SCRIPTS.endWrite, 3, ...keys, 3600)
        expect(await admin.exists(keys[2])).toBe(0)
        expect(await admin.get(keys[1])).toBe('2')
        expect(await admin.eval(LID_CACHE_SCRIPTS.publish, 3, ...keys, '1', P1, 60)).toBe(0)
        expect(await admin.eval(LID_CACHE_SCRIPTS.publish, 3, ...keys, '2', P2, 60)).toBe(1)
    })

    it('a lookup in one handler that read P1 does not republish it after another handler moves the LID', async () => {
        const reader = await makeHandler()
        const mover = await makeHandler()
        let release!: () => void
        const gate = new Promise<void>((r) => { release = r })
        let reachedResolve!: () => void
        const reached = new Promise<void>((r) => { reachedResolve = r })
        const mappings = (reader as any).lidMappingsCollection
        const original = mappings.findOne.bind(mappings)
        jest.spyOn(mappings, 'findOne').mockImplementation((async (filter: any, options?: any) => {
            const result = await original(filter, options)
            reachedResolve()
            await gate
            return result
        }) as any)

        const lookup = reader.getPhoneNumberFromLid(L)
        await reached
        await mover.handleLidMappingUpdate({ lid: L, pn: P2 })
        release()
        expect(await lookup).toBe(P1) // it read before the move; only its cache write is fenced
        jest.restoreAllMocks()

        expect(await admin.get(KEY)).toBeNull()
        expect(await reader.getPhoneNumberFromLid(L)).toBe(P2)
        expect(await admin.get(KEY)).toBe(P2)
    })

    it('a writer whose begin call fails does not remove the busy marker of a writer in progress', async () => {
        const first = await makeHandler()
        const second = await makeHandler()
        let release!: () => void
        const gate = new Promise<void>((r) => { release = r })
        let reachedResolve!: () => void
        const reached = new Promise<void>((r) => { reachedResolve = r })
        const contacts = (first as any).contactsCollection
        const original = contacts.updateOne.bind(contacts)
        jest.spyOn(contacts, 'updateOne').mockImplementation((async (filter: any, update: any, options?: any) => {
            if (update?.$unset?.lid !== undefined) { reachedResolve(); await gate }
            return original(filter, update, options)
        }) as any)
        const pending = first.handleLidMappingUpdate({ lid: L, pn: P2 })
        await reached

        const secondClient = (second as any).redis as Redis
        const realEval = secondClient.eval.bind(secondClient)
        jest.spyOn(secondClient, 'eval').mockImplementation((async (script: string, ...rest: any[]) => {
            if (script === LID_CACHE_SCRIPTS.beginWrite) throw new Error('begin failed before Redis ran it')
            return (realEval as any)(script, ...rest)
        }) as any)
        await second.handleLidMappingUpdate({ lid: L, pn: '60123450033@s.whatsapp.net' })

        expect(await admin.exists(`{${KEY}}:busy`)).toBe(1)
        release()
        await pending
        expect(await admin.exists(`{${KEY}}:busy`)).toBe(0)
    })
})
