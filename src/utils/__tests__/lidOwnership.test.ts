// WAB-1275: one ownership rule for contacts.lid. Real mongod with the production unique indexes,
// and a Redis double so the lookup cache path is the one production uses.
import { Collection, Db, MongoClient } from 'mongodb'
import { MongoMemoryServer } from 'mongodb-memory-server'
import { LID_CACHE_SCRIPTS, LidHandler } from '../lidHandler'
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
    setHook: (() => Promise<void>) | null = null
    async set(key: string, value: string) {
        this.check()
        if (this.setHook) await this.setHook()
        if (this.failSets > 0) { this.failSets--; throw new Error('redis set failed') }
        this.data.set(key, value)
        return 'OK'
    }
    async del(...keys: string[]) { this.check(); let n = 0; for (const k of keys) if (this.data.delete(k)) n++; return n }
    // Emulates the three LID_CACHE_SCRIPTS (TTLs ignored); the real scripts run in lidOwnership.redis.int.test.ts
    evalHook: ((script: string) => Promise<void>) | null = null
    async eval(script: string, numKeys: number, ...args: Array<string | number>) {
        this.check()
        if (this.evalHook) await this.evalHook(script)
        const [key, gen, busy] = args.slice(0, numKeys).map(String)
        const argv = args.slice(numKeys).map(String)
        const incr = (k: string) => this.data.set(k, String(Number(this.data.get(k) ?? 0) + 1))
        if (script === LID_CACHE_SCRIPTS.beginWrite) { this.data.delete(key); incr(gen); incr(busy); return 1 }
        if (script === LID_CACHE_SCRIPTS.endWrite) {
            this.data.delete(key)
            incr(gen)
            const left = Number(this.data.get(busy) ?? 0) - 1
            if (left <= 0) this.data.delete(busy); else this.data.set(busy, String(left))
            return 1
        }
        if (script === LID_CACHE_SCRIPTS.publish) {
            if (this.data.has(busy) || (this.data.get(gen) ?? '') !== argv[0]) return 0
            this.data.set(key, argv[1])
            return 1
        }
        throw new Error('unknown script')
    }
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
        // the write empties every affected key and publishes nothing
        for (const key of [redisKey('lid', L), redisKey('phone', P1), redisKey('phone', P2)]) expect(redis.data.has(key)).toBe(false)
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
        test('a failed end-of-write Redis call leaves MongoDB moved, the keys empty, and no lookup caching while busy', async () => {
            const handler = await makeHandler()
            await owner(P1)
            redis.data.set(redisKey('lid', L), P1)
            redis.evalHook = async (script) => { if (script === LID_CACHE_SCRIPTS.endWrite) throw new Error('redis end failed') }

            await handler.handleLidMappingUpdate({ lid: L, pn: P2 })
            redis.evalHook = null

            expect(await holderOf(L)).toBe(P2)
            expect(await mappedPhone(L)).toBe(P2)
            expect(await handler.getPhoneNumberFromLid(L)).toBe(P2)
            expect(redis.data.has(redisKey('lid', L))).toBe(false)
            expect(lines(FAILED)).toEqual([])
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

    test('S8j (round 1): a lookup in another handler that read P1 does not republish it after a move', async () => {
        const reader = await makeHandler()
        const mover = await makeHandler()
        await owner(P1)
        const gate = deferred()
        const reached = deferred()
        const target = coll(reader, 'lidMappingsCollection')
        const original = target.findOne.bind(target)
        jest.spyOn(target, 'findOne').mockImplementation((async (filter: any, options?: any) => {
            const result = await original(filter, options)
            reached.resolve()
            await gate.promise
            return result
        }) as any)

        const lookup = reader.getPhoneNumberFromLid(L)
        await Promise.race([reached.promise, lookup])
        await mover.handleLidMappingUpdate({ lid: L, pn: P2 })
        gate.resolve()
        await lookup
        jest.restoreAllMocks()

        expect(redis.data.get(redisKey('lid', L))).not.toBe(P1)
        expect(await reader.getPhoneNumberFromLid(L)).toBe(P2)
    })

    test('S8k (round 1): a lookup cache write that fails after a move does not fill the local fallback with P1', async () => {
        const handler = await makeHandler()
        await owner(P1)
        const gate = deferred()
        const reached = deferred()
        let held = false
        // hold the lookup's first cache write (publish script, or a plain SET in older code), then fail it
        const holdFirst = async () => {
            if (held) return
            held = true
            reached.resolve()
            await gate.promise
            throw new Error('delayed publish failure')
        }
        redis.evalHook = async (script) => { if (script === LID_CACHE_SCRIPTS.publish) await holdFirst() }
        redis.setHook = holdFirst

        const lookup = handler.getPhoneNumberFromLid(L)
        await Promise.race([reached.promise, lookup])
        await handler.handleLidMappingUpdate({ lid: L, pn: P2 })
        gate.resolve()
        await lookup
        redis.evalHook = null
        redis.setHook = null
        redis.down = true

        expect(await handler.getPhoneNumberFromLid(L)).toBe(P2)
    })

    test.each([
        ['without an id', {}],
        ['with a null id', { id: null }],
        ['with an empty id', { id: '' }],
    ])('S8l (rounds 1-2): a holder %s is an owner for an ordinary pair and yields only to lid-mapping.update', async (_name, idField) => {
        const handler = await makeHandler()
        await contacts().insertOne({ instanceId: INSTANCE, lid: L, name: 'no id', ...idField })

        await handler.storeLidMapping(L, P2)

        expect((await contacts().findOne({ instanceId: INSTANCE, name: 'no id' }))?.lid).toBe(L)
        expect(await lidOf(P2)).toBeNull()
        expect(await mappedPhone(L)).toBeNull()
        expect(lines(KEPT)).toHaveLength(1)

        await handler.handleLidMappingUpdate({ lid: L, pn: P2 })
        expect(await holderOf(L)).toBe(P2)
    })

    test('S8m (round 2): a failed publish after another handler moved the LID does not seed the local fallback', async () => {
        const reader = await makeHandler()
        const mover = await makeHandler()
        await owner(P1)
        const gate = deferred()
        const reached = deferred()
        let held = false
        const holdFirst = async () => {
            if (held) return
            held = true
            reached.resolve()
            await gate.promise
            throw new Error('delayed publish failure')
        }
        redis.evalHook = async (script) => { if (script === LID_CACHE_SCRIPTS.publish) await holdFirst() }
        redis.setHook = holdFirst

        const lookup = reader.getPhoneNumberFromLid(L)
        await Promise.race([reached.promise, lookup])
        redis.evalHook = null
        redis.setHook = null
        await mover.handleLidMappingUpdate({ lid: L, pn: P2 })
        gate.resolve()
        await lookup
        redis.down = true

        expect(await reader.getPhoneNumberFromLid(L)).toBe(P2)
    })

    test('S8n (round 2): a writer whose Redis begin fails does not remove the busy marker of a writer in progress', async () => {
        const first = await makeHandler()
        const second = await makeHandler()
        const reader = await makeHandler()
        await owner(P1)
        const gate = deferred()
        const reached = deferred()
        // the first writer pauses after its mapping cutover, while its keys are busy
        const target = coll(first, 'contactsCollection')
        const original = target.updateOne.bind(target)
        jest.spyOn(target, 'updateOne').mockImplementation((async (filter: any, update: any, options?: any) => {
            if (update?.$unset?.lid !== undefined) { reached.resolve(); await gate.promise }
            return original(filter, update, options)
        }) as any)
        const pending = first.handleLidMappingUpdate({ lid: L, pn: P2 })
        await Promise.race([reached.promise, pending])

        redis.evalHook = async (script) => { if (script === LID_CACHE_SCRIPTS.beginWrite) throw new Error('redis begin failed') }
        await second.handleLidMappingUpdate({ lid: L, pn: P3 })
        redis.evalHook = null

        expect(redis.data.has(`{${redisKey('lid', L)}}:busy`)).toBe(true)
        expect(await reader.getPhoneNumberFromLid(L)).toBe(P2) // the cutover already happened
        expect(redis.data.has(redisKey('lid', L))).toBe(false) // busy: nothing cached
        gate.resolve()
        await pending
        expect(await reader.getPhoneNumberFromLid(L)).toBe(P2)
        expect(redis.data.get(redisKey('lid', L))).toBe(P2)
    })

    test('S8o (PR review): an authoritative move whose reads are older than another handler\'s completed move still ends consistent', async () => {
        const first = await makeHandler()
        const second = await makeHandler()
        await owner(P1)
        const gate = deferred()
        const reached = deferred()
        // the second move pauses at its mapping write, after it read P1 as the owner
        const target = coll(second, 'lidMappingsCollection')
        const original = target.updateOne.bind(target)
        jest.spyOn(target, 'updateOne').mockImplementation((async (filter: any, update: any, options?: any) => {
            reached.resolve()
            await gate.promise
            return original(filter, update, options)
        }) as any)

        const pending = second.handleLidMappingUpdate({ lid: L, pn: P3 })
        await Promise.race([reached.promise, pending])
        await first.handleLidMappingUpdate({ lid: L, pn: P2 })
        expect(await holderOf(L)).toBe(P2)
        gate.resolve()
        await pending

        expect(await mappedPhone(L)).toBe(P3)
        expect(await holderOf(L)).toBe(P3)
        expect(await lidOf(P2)).toBeNull()
        expect(lines(FAILED)).toEqual([])
    })

    test('S8p (round 4): a move that checked its mapping before another handler completed its own move still ends consistent', async () => {
        const first = await makeHandler()
        const second = await makeHandler()
        await owner(P1)
        const gate = deferred()
        const reached = deferred()
        // the first move pauses inside its reconcile, after the mapping check and before the holder read
        const target = coll(first, 'contactsCollection')
        const original = target.findOne.bind(target)
        let holderReads = 0
        jest.spyOn(target, 'findOne').mockImplementation((async (filter: any, options?: any) => {
            if (filter?.lid === L && filter?.id === undefined && ++holderReads === 2) { reached.resolve(); await gate.promise }
            return original(filter, options)
        }) as any)

        const pending = first.handleLidMappingUpdate({ lid: L, pn: P2 })
        await Promise.race([reached.promise, pending])
        await second.handleLidMappingUpdate({ lid: L, pn: P3 })
        gate.resolve()
        await pending

        const holder = await holderOf(L)
        expect(holder).toBe(await mappedPhone(L))
        expect(lines(FAILED)).toEqual([])
    })

    test('S8q (round 5): a phone that convergence assigns and then releases is not left cached as owning L', async () => {
        const P4 = '60123450044@s.whatsapp.net'
        const a = await makeHandler()
        const b = await makeHandler()
        const c = await makeHandler()
        const reader = await makeHandler()
        await owner(P1)
        // C reads P1 as the owner, then pauses before its mapping write
        const cReady = deferred(), cGate = deferred()
        const cMappings = coll(c, 'lidMappingsCollection')
        const cOriginal = cMappings.updateOne.bind(cMappings)
        jest.spyOn(cMappings, 'updateOne').mockImplementation((async (...args: any[]) => {
            cReady.resolve()
            await cGate.promise
            return (cOriginal as any)(...args)
        }) as any)
        const moveC = c.handleLidMappingUpdate({ lid: L, pn: P4 })
        await Promise.race([cReady.promise, moveC])
        // A pauses before its holder reads in its first and second reconcile attempts, and after it assigns P3
        const firstReady = deferred(), firstGate = deferred()
        const secondReady = deferred(), secondGate = deferred()
        const assignedReady = deferred(), assignedGate = deferred()
        const aContacts = coll(a, 'contactsCollection')
        const aFind = aContacts.findOne.bind(aContacts)
        const aUpdate = aContacts.updateOne.bind(aContacts)
        let holderReads = 0
        jest.spyOn(aContacts, 'findOne').mockImplementation((async (filter: any, ...rest: any[]) => {
            if (filter?.lid === L && filter?.id === undefined) {
                holderReads++
                if (holderReads === 2) { firstReady.resolve(); await firstGate.promise }
                if (holderReads === 3) { secondReady.resolve(); await secondGate.promise }
            }
            return (aFind as any)(filter, ...rest)
        }) as any)
        jest.spyOn(aContacts, 'updateOne').mockImplementation((async (filter: any, update: any, ...rest: any[]) => {
            const result = await (aUpdate as any)(filter, update, ...rest)
            if (filter?.id === P3 && update?.$set?.lid === L) { assignedReady.resolve(); await assignedGate.promise }
            return result
        }) as any)

        const moveA = a.handleLidMappingUpdate({ lid: L, pn: P2 })
        await Promise.race([firstReady.promise, moveA])
        await b.handleLidMappingUpdate({ lid: L, pn: P3 })
        firstGate.resolve()
        await Promise.race([secondReady.promise, moveA])
        cGate.resolve()
        await moveC
        secondGate.resolve()
        await Promise.race([assignedReady.promise, moveA])
        await reader.getLidFromPhoneNumber(P3) // reads the temporary P3 assignment
        assignedGate.resolve()
        await moveA

        expect(await holderOf(L)).toBe(P4)
        expect(await mappedPhone(L)).toBe(P4)
        expect(await lidOf(P3)).toBeNull()
        expect(await reader.getLidFromPhoneNumber(P3)).toBeNull()
        expect([...redis.data.keys()].filter((k) => k.endsWith(':busy'))).toEqual([])
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
