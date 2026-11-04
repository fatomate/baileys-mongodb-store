// WAB-1275: one ownership rule for contacts.lid. Real mongod with the production unique indexes,
// and a Redis double so the lookup cache path is the one production uses.
import { Collection, Db, MongoClient } from 'mongodb'
import { MongoMemoryServer } from 'mongodb-memory-server'
import { LidHandler } from '../lidHandler'
import { flushBurst } from '../burstLog'

const INSTANCE = 'wab1275-a'
const OTHER_INSTANCE = 'wab1275-b'
const L = '987650001@lid'
const P = '60123450001@s.whatsapp.net'
const P1 = '60123450011@s.whatsapp.net'
const P2 = '60123450022@s.whatsapp.net'
const P3 = '60123450033@s.whatsapp.net'
const FAILED = '[LidHandler] Failed to store LID mapping'
const KEPT = '[LidHandler] LID kept by existing owner'

class RedisDouble {
    data = new Map<string, string>()
    down = false
    failSets = 0
    private check() { if (this.down) throw new Error('redis down') }
    async get(key: string) { this.check(); return this.data.has(key) ? this.data.get(key)! : null }
    async set(key: string, value: string) {
        this.check()
        if (this.failSets > 0) { this.failSets--; throw new Error('redis set failed') }
        this.data.set(key, value)
        return 'OK'
    }
    async del(...keys: string[]) { this.check(); let n = 0; for (const k of keys) if (this.data.delete(k)) n++; return n }
}

const deferred = () => {
    let resolve!: () => void
    const promise = new Promise<void>((r) => { resolve = r })
    return { promise, resolve }
}

describe('contacts.lid ownership (WAB-1275)', () => {
    let server: MongoMemoryServer
    let client: MongoClient
    let db: Db
    let redis: RedisDouble
    let errorSpy: jest.SpyInstance
    const contacts = () => db.collection('own_contacts')
    const mappings = () => db.collection('own_lidMappings')

    const makeHandler = async (instanceId = INSTANCE) => {
        const handler = new LidHandler(instanceId, { enableCache: true, cacheTTL: 60, redisClient: redis as any, preferReverseLookupFirst: false })
        await handler.initialize(db, 'own_')
        return handler
    }
    const coll = (handler: LidHandler, name: 'contactsCollection' | 'lidMappingsCollection'): Collection<any> => (handler as any)[name]
    const lines = (label: string) => {
        flushBurst(label)
        return errorSpy.mock.calls.map((c) => String(c[0])).filter((line) => line.startsWith(label))
    }
    const holderOf = async (lid: string, instanceId = INSTANCE) =>
        (await contacts().findOne({ instanceId, lid }, { projection: { _id: 0, id: 1 } }))?.id ?? null
    const mappedPhone = async (lid: string, instanceId = INSTANCE) =>
        (await mappings().findOne({ instanceId, lid }, { projection: { _id: 0, phoneNumber: 1 } }))?.phoneNumber ?? null
    const lidOf = async (id: string) => (await contacts().findOne({ instanceId: INSTANCE, id }))?.lid ?? null
    const selfRef = (lid = L, instanceId = INSTANCE) => contacts().insertOne({ instanceId, id: lid, jid: lid, lid, sqlSubscriber: { mysql_id: 1 } })
    const owner = async (phone: string, withMapping = true) => {
        await contacts().insertOne({ instanceId: INSTANCE, id: phone, lid: L })
        if (withMapping) await mappings().insertOne({ instanceId: INSTANCE, lid: L, phoneNumber: phone, firstSeen: new Date() })
    }
    const redisKey = (type: 'lid' | 'phone', value: string) => `${type}:${INSTANCE}:${value}`

    beforeAll(async () => {
        server = await MongoMemoryServer.create()
        client = await MongoClient.connect(server.getUri())
        db = client.db('wab1275')
        await contacts().createIndex({ instanceId: 1, id: 1 }, { unique: true })
        await contacts().createIndex({ instanceId: 1, lid: 1 }, { unique: true, partialFilterExpression: { lid: { $type: 'string' } } })
        await mappings().createIndex({ instanceId: 1, lid: 1 }, { unique: true })
    })
    afterAll(async () => {
        await client.close()
        await server.stop()
    })
    beforeEach(async () => {
        await contacts().deleteMany({})
        await mappings().deleteMany({})
        redis = new RedisDouble()
        flushBurst(FAILED)
        flushBurst(KEPT)
        errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {})
        jest.spyOn(console, 'log').mockImplementation(() => {})
        jest.spyOn(console, 'warn').mockImplementation(() => {})
        jest.spyOn(console, 'debug').mockImplementation(() => {})
    })
    afterEach(() => jest.restoreAllMocks())

    test('S1: a phone contact takes the LID from a self-referencing holder, with no failure line', async () => {
        const handler = await makeHandler()
        await selfRef()
        await contacts().insertOne({ instanceId: INSTANCE, id: P, name: 'kept' })

        await handler.storeLidMapping(L, P)

        expect(await holderOf(L)).toBe(P)
        expect((await contacts().findOne({ instanceId: INSTANCE, id: L }))?.lid).toBeUndefined()
        expect((await contacts().findOne({ instanceId: INSTANCE, id: P }))?.name).toBe('kept')
        expect(await mappedPhone(L)).toBe(P)
        expect(await handler.getPhoneNumberFromLid(L)).toBe(P)
        expect(await handler.getLidFromPhoneNumber(P)).toBe(L)
        expect(lines(FAILED)).toEqual([])
    })

    test('S2: a missing phone contact is created with the LID', async () => {
        const handler = await makeHandler()
        await selfRef()

        await handler.storeLidMapping(L, P)

        const created = await contacts().findOne({ instanceId: INSTANCE, id: P })
        expect(created?.lid).toBe(L)
        expect(created?.lidFirstSeen).toBeInstanceOf(Date)
        expect(lines(FAILED)).toEqual([])
    })

    test('S3 (regression): the current owner stays without an error', async () => {
        const handler = await makeHandler()
        await owner(P)

        await handler.storeLidMapping(L, P)

        expect(await holderOf(L)).toBe(P)
        expect(await mappedPhone(L)).toBe(P)
        expect(lines(FAILED)).toEqual([])
    })

    test('S4: an ordinary pair does not take the LID from a phone contact', async () => {
        const handler = await makeHandler()
        await owner(P1, false)

        await handler.storeLidMapping(L, P2)

        expect(await holderOf(L)).toBe(P1)
        expect(await contacts().findOne({ instanceId: INSTANCE, id: P2 })).toBeNull()
        expect(await mappedPhone(L)).toBeNull()
        expect(redis.data.get(redisKey('lid', L))).toBeUndefined()
        const kept = lines(KEPT)
        expect(kept).toHaveLength(1)
        expect(kept[0]).not.toMatch(/987650001|6012345|@lid|whatsapp/)
        expect(lines(FAILED)).toEqual([])
    })

    test('S5: an ordinary pair does not disagree with a valid mapping', async () => {
        const handler = await makeHandler()
        await mappings().insertOne({ instanceId: INSTANCE, lid: L, phoneNumber: P1, firstSeen: new Date() })
        await selfRef()

        await handler.storeLidMapping(L, P2)

        expect(await mappedPhone(L)).toBe(P1)
        expect(await lidOf(P2)).toBeNull()
        expect(lines(KEPT)).toHaveLength(1)
    })

    test('S6: lid-mapping.update moves the LID, and contacts, mapping, cache and both lookups agree', async () => {
        const handler = await makeHandler()
        await owner(P1)
        redis.data.set(redisKey('lid', L), P1)
        redis.data.set(redisKey('phone', P1), L)
        redis.data.set(redisKey('phone', P2), '__MISS__')

        await handler.handleLidMappingUpdate({ lid: L, pn: P2 })

        expect(await holderOf(L)).toBe(P2)
        expect(await lidOf(P1)).toBeNull()
        expect(await mappedPhone(L)).toBe(P2)
        expect(redis.data.get(redisKey('phone', P1))).not.toBe(L)
        expect(await handler.getPhoneNumberFromLid(L)).toBe(P2)
        expect(await handler.getLidFromPhoneNumber(P2)).toBe(L)
        expect(await handler.getLidFromPhoneNumber(P1)).toBeNull()

        // autoFill and a later ordinary pair for the old owner do not move it back
        redis.data.clear()
        expect(await handler.getPhoneNumberFromLid(L)).toBe(P2)
        await handler.storeLidMapping(L, P1)
        expect(await holderOf(L)).toBe(P2)
        expect(await mappedPhone(L)).toBe(P2)
    })

    test('S7: a valid pair replaces a lid->lid row; a LID-format phone never writes', async () => {
        const handler = await makeHandler()
        await mappings().insertOne({ instanceId: INSTANCE, lid: L, phoneNumber: L, firstSeen: new Date() })

        await handler.storeLidMapping(L, '987650002@lid')
        expect(await mappedPhone(L)).toBe(L)

        await handler.storeLidMapping(L, P)
        expect(await mappedPhone(L)).toBe(P)
        expect(await holderOf(L)).toBe(P)
    })

    test('S8: another writer takes the LID after the checks: one failure, no retry, nothing published', async () => {
        const handler = await makeHandler()
        const target = coll(handler, 'contactsCollection')
        const original = target.updateOne.bind(target)
        let lidWrites = 0
        jest.spyOn(target, 'updateOne').mockImplementation((async (filter: any, update: any, options?: any) => {
            if (filter?.id === P1 && update?.$set?.lid === L) {
                lidWrites++
                // a writer in another process takes L before this write lands
                if (lidWrites === 1) await contacts().insertOne({ instanceId: INSTANCE, id: P3, lid: L })
            }
            return original(filter, update, options)
        }) as any)

        await handler.storeLidMapping(L, P1)

        expect(lidWrites).toBe(1)
        expect(await holderOf(L)).toBe(P3)
        expect(await lidOf(P1)).toBeNull()
        expect(await mappedPhone(L)).toBeNull()
        expect(redis.data.get(redisKey('lid', L))).toBeUndefined()
        expect(lines(FAILED)).toEqual([`${FAILED}: 1 failure(s) in 5s (MongoServerError:11000×1)`])
    })

    describe('S8b: a failure at each step of a move', () => {
        const failOnce = (handler: LidHandler, name: 'contactsCollection' | 'lidMappingsCollection', match: (filter: any, update: any) => boolean) => {
            const target = coll(handler, name)
            const original = target.updateOne.bind(target)
            let failed = false
            jest.spyOn(target, 'updateOne').mockImplementation((async (filter: any, update: any, options?: any) => {
                if (!failed && match(filter, update)) {
                    failed = true
                    throw new Error('injected failure')
                }
                return original(filter, update, options)
            }) as any)
        }

        test('the mapping write fails: lookups keep P1', async () => {
            const handler = await makeHandler()
            await owner(P1)
            failOnce(handler, 'lidMappingsCollection', (f) => f?.lid === L)

            await handler.handleLidMappingUpdate({ lid: L, pn: P2 })

            expect(await mappedPhone(L)).toBe(P1)
            expect(await holderOf(L)).toBe(P1)
            expect(await handler.getPhoneNumberFromLid(L)).toBe(P1)
        })

        test.each([
            ['release', (f: any, u: any) => f?.lid === L && u?.$unset?.lid !== undefined],
            ['assignment', (f: any, u: any) => f?.id === P2 && u?.$set?.lid === L],
        ])('the %s fails: forward lookups return P2, also after a reverse lookup of P1; a repeat completes the move', async (_step, match) => {
            const handler = await makeHandler()
            await owner(P1)
            failOnce(handler, 'contactsCollection', match)

            await handler.handleLidMappingUpdate({ lid: L, pn: P2 })

            expect(await mappedPhone(L)).toBe(P2)
            await handler.getLidFromPhoneNumber(P1)
            expect(await handler.getPhoneNumberFromLid(L)).toBe(P2)

            await handler.handleLidMappingUpdate({ lid: L, pn: P2 })
            expect(await holderOf(L)).toBe(P2)
            expect(await lidOf(P1)).toBeNull()
        })
    })

    test('S8b (review): with no previous mapping, a failed release keeps the authoritative cutover', async () => {
        const handler = await makeHandler()
        await owner(P1, false)
        const target = coll(handler, 'contactsCollection')
        const original = target.updateOne.bind(target)
        let failed = false
        jest.spyOn(target, 'updateOne').mockImplementation((async (filter: any, update: any, options?: any) => {
            if (!failed && update?.$unset?.lid !== undefined) { failed = true; throw new Error('injected failure') }
            return original(filter, update, options)
        }) as any)

        await handler.handleLidMappingUpdate({ lid: L, pn: P2 })

        expect(await mappedPhone(L)).toBe(P2)
        expect(await handler.getPhoneNumberFromLid(L)).toBe(P2)
        await handler.handleLidMappingUpdate({ lid: L, pn: P2 })
        expect(await holderOf(L)).toBe(P2)
    })

    describe('S8c: Redis and the local cache stay coherent', () => {
        test.each([1, 2])('a failed cache publish (SET %i) leaves MongoDB moved and no stale cache value', async (failingSet) => {
            const handler = await makeHandler()
            await owner(P1)
            redis.data.set(redisKey('lid', L), P1)
            redis.data.set(redisKey('phone', P1), L)
            const realSet = redis.set.bind(redis)
            let sets = 0
            jest.spyOn(redis, 'set').mockImplementation(async (key: string, value: string) => {
                sets++
                if (sets === failingSet) throw new Error('redis set failed')
                return realSet(key, value)
            })

            await handler.handleLidMappingUpdate({ lid: L, pn: P2 })

            expect(await holderOf(L)).toBe(P2)
            expect(await mappedPhone(L)).toBe(P2)
            expect(redis.data.get(redisKey('lid', L))).not.toBe(P1)
            expect(redis.data.get(redisKey('phone', P1))).toBeUndefined()
            expect(await handler.getPhoneNumberFromLid(L)).toBe(P2)
            expect(await handler.getLidFromPhoneNumber(P1)).toBeNull()
            expect(lines(FAILED)).toHaveLength(1)
        })

        test('a Redis outage during a move changes nothing; the repeat after recovery moves the LID', async () => {
            const handler = await makeHandler()
            await owner(P1)
            redis.data.set(redisKey('lid', L), P1)
            redis.down = true

            await handler.handleLidMappingUpdate({ lid: L, pn: P2 })

            expect(await holderOf(L)).toBe(P1)
            expect(await mappedPhone(L)).toBe(P1)
            redis.down = false
            const restarted = await makeHandler()
            expect(await restarted.getPhoneNumberFromLid(L)).toBe(P1)

            await restarted.handleLidMappingUpdate({ lid: L, pn: P2 })
            expect(await restarted.getPhoneNumberFromLid(L)).toBe(P2)
        })

        test('a move with Redis up clears the local fallback, so a later outage does not return P1', async () => {
            const handler = await makeHandler()
            await owner(P1)
            redis.down = true
            expect(await handler.getPhoneNumberFromLid(L)).toBe(P1) // fills the local fallback
            expect(await handler.getLidFromPhoneNumber(P1)).toBe(L)
            redis.down = false

            await handler.handleLidMappingUpdate({ lid: L, pn: P2 })
            redis.down = true

            expect(await handler.getPhoneNumberFromLid(L)).toBe(P2)
            expect(await handler.getLidFromPhoneNumber(P1)).toBeNull()
        })
    })

    test('S8d: an ordinary pair paused after its checks does not overwrite a move that commits meanwhile', async () => {
        const mover = await makeHandler()
        const ordinary = await makeHandler()
        await selfRef()
        const gate = deferred()
        const reached = deferred()
        const target = coll(ordinary, 'lidMappingsCollection')
        const original = target.updateOne.bind(target)
        jest.spyOn(target, 'updateOne').mockImplementation((async (filter: any, update: any, options?: any) => {
            reached.resolve()
            await gate.promise
            return original(filter, update, options)
        }) as any)

        const pending = ordinary.storeLidMapping(L, P1)
        await Promise.race([reached.promise, pending])
        await mover.handleLidMappingUpdate({ lid: L, pn: P2 })
        gate.resolve()
        await pending

        expect(await holderOf(L)).toBe(P2)
        expect(await lidOf(P1)).toBeNull()
        expect(await mappedPhone(L)).toBe(P2)
        expect(redis.data.get(redisKey('lid', L))).not.toBe(P1)
        expect(await ordinary.getPhoneNumberFromLid(L)).toBe(P2)
    })

    test('S8e: a reverse lookup that read a miss before a move does not cache the miss', async () => {
        const handler = await makeHandler()
        await selfRef()
        const gate = deferred()
        const reached = deferred()
        const target = coll(handler, 'contactsCollection')
        const original = target.findOne.bind(target)
        let paused = false
        jest.spyOn(target, 'findOne').mockImplementation((async (filter: any, options?: any) => {
            const result = await original(filter, options)
            if (!paused && filter?.id === P2) {
                paused = true
                reached.resolve()
                await gate.promise
            }
            return result
        }) as any)

        const lookup = handler.getLidFromPhoneNumber(P2)
        await Promise.race([reached.promise, lookup])
        await handler.handleLidMappingUpdate({ lid: L, pn: P2 })
        gate.resolve()
        await lookup

        expect(redis.data.get(redisKey('phone', P2))).not.toBe('__MISS__')
        expect(await handler.getLidFromPhoneNumber(P2)).toBe(L)
    })

    test('S8f: a reverse lookup of the displaced owner during a move does not cache the old LID', async () => {
        const handler = await makeHandler()
        await owner(P1)
        const gate = deferred()
        const reached = deferred()
        const target = coll(handler, 'contactsCollection')
        const original = target.updateOne.bind(target)
        jest.spyOn(target, 'updateOne').mockImplementation((async (filter: any, update: any, options?: any) => {
            if (update?.$unset?.lid !== undefined) { reached.resolve(); await gate.promise }
            return original(filter, update, options)
        }) as any)

        const move = handler.handleLidMappingUpdate({ lid: L, pn: P2 })
        await Promise.race([reached.promise, move])
        expect(await handler.getLidFromPhoneNumber(P1)).toBe(L) // reads P1's contact before the release
        gate.resolve()
        await move

        expect(redis.data.get(redisKey('phone', P1))).toBeUndefined()
        expect(await handler.getLidFromPhoneNumber(P1)).toBeNull()
    })

    test('S8g: an ordinary pair that agreed with the mapping does not take the LID inside a move', async () => {
        const mover = await makeHandler()
        const ordinary = await makeHandler()
        await mappings().insertOne({ instanceId: INSTANCE, lid: L, phoneNumber: P1, firstSeen: new Date() })
        const gate = deferred()
        const reached = deferred()
        // the ordinary write pauses at its first write, after its ownership reads
        for (const name of ['contactsCollection', 'lidMappingsCollection'] as const) {
            const target = coll(ordinary, name)
            const original = target.updateOne.bind(target)
            jest.spyOn(target, 'updateOne').mockImplementation((async (filter: any, update: any, options?: any) => {
                reached.resolve()
                await gate.promise
                return original(filter, update, options)
            }) as any)
        }
        const pending = ordinary.storeLidMapping(L, P1)
        await Promise.race([reached.promise, pending])
        // the move cuts over, then lets the ordinary write finish before it assigns the contact
        const moverContacts = coll(mover, 'contactsCollection')
        const moverOriginal = moverContacts.updateOne.bind(moverContacts)
        jest.spyOn(moverContacts, 'updateOne').mockImplementation((async (filter: any, update: any, options?: any) => {
            if (filter?.id === P2 && update?.$set?.lid === L) {
                gate.resolve()
                await pending
            }
            return moverOriginal(filter, update, options)
        }) as any)

        await mover.handleLidMappingUpdate({ lid: L, pn: P2 })

        expect(await holderOf(L)).toBe(P2)
        expect(await mappedPhone(L)).toBe(P2)
        expect(await lidOf(P1)).toBeNull()
    })

    test('S8i (review): two authoritative moves from different handlers end with contacts and mapping in agreement', async () => {
        const first = await makeHandler()
        const second = await makeHandler()
        await mappings().insertOne({ instanceId: INSTANCE, lid: L, phoneNumber: P1, firstSeen: new Date() })
        const gate = deferred()
        const reached = deferred()
        // the first move pauses at its first write, after its ownership reads
        for (const name of ['contactsCollection', 'lidMappingsCollection'] as const) {
            const target = coll(first, name)
            const original = target.updateOne.bind(target)
            jest.spyOn(target, 'updateOne').mockImplementation((async (filter: any, update: any, options?: any) => {
                reached.resolve()
                await gate.promise
                return original(filter, update, options)
            }) as any)
        }
        const pending = first.handleLidMappingUpdate({ lid: L, pn: P1 })
        await Promise.race([reached.promise, pending])
        // the second move cuts over, then lets the first finish before it assigns the contact
        const secondContacts = coll(second, 'contactsCollection')
        const secondOriginal = secondContacts.updateOne.bind(secondContacts)
        jest.spyOn(secondContacts, 'updateOne').mockImplementation((async (filter: any, update: any, options?: any) => {
            if (filter?.id === P2 && update?.$set?.lid === L) {
                gate.resolve()
                await pending
            }
            return secondOriginal(filter, update, options)
        }) as any)

        await second.handleLidMappingUpdate({ lid: L, pn: P2 })

        const holder = await holderOf(L)
        expect(holder).not.toBeNull()
        expect(await mappedPhone(L)).toBe(holder)
    })

    test('S8h: a holder re-keyed to a phone after the checks is not released by an ordinary pair', async () => {
        const handler = await makeHandler()
        await selfRef()
        const target = coll(handler, 'contactsCollection')
        const original = target.updateOne.bind(target)
        jest.spyOn(target, 'updateOne').mockImplementation((async (filter: any, update: any, options?: any) => {
            // another process re-keys the self-referencing doc to its phone before the release lands
            if (update?.$unset?.lid !== undefined) await contacts().updateOne({ instanceId: INSTANCE, id: L }, { $set: { id: P1, jid: P1 } })
            return original(filter, update, options)
        }) as any)

        await handler.storeLidMapping(L, P2)

        expect(await holderOf(L)).toBe(P1)
        expect(await lidOf(P2)).toBeNull()
        expect(await mappedPhone(L)).toBeNull()
        expect(lines(FAILED)).toHaveLength(1)
    })

    test('S9: concurrent moves of one LID in one handler leave contacts, mapping and cache in agreement', async () => {
        const handler = await makeHandler()
        await owner(P1)

        await Promise.all([
            handler.handleLidMappingUpdate({ lid: L, pn: P2 }),
            handler.handleLidMappingUpdate({ lid: L, pn: P3 }),
        ])

        const holder = await holderOf(L)
        expect([P2, P3]).toContain(holder)
        expect(await mappedPhone(L)).toBe(holder)
        const cached = redis.data.get(redisKey('lid', L))
        if (cached !== undefined) expect(cached).toBe(holder)
        expect(await handler.getPhoneNumberFromLid(L)).toBe(holder)
    })

    test('S10 (regression): a pair for one instance changes no data of another instance', async () => {
        const handler = await makeHandler()
        await selfRef(L, OTHER_INSTANCE)

        await handler.storeLidMapping(L, P)

        expect(await holderOf(L)).toBe(P)
        expect(await holderOf(L, OTHER_INSTANCE)).toBe(L)
    })
})
