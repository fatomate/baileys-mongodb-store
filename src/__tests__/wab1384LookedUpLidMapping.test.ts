// WAB-1384: a caller that looked up a phone's LID (Baileys getLIDForPN) saves the pair through
// storeLookedUpLidMapping. It is an ordinary write (WAB-1275): it never takes the LID from its current owner,
// and it reports whether the store now gives the LID to that phone. Real mongod with the production indexes.
/* eslint-disable @typescript-eslint/no-require-imports */
jest.mock('baileys', () => require('./helpers/baileysDouble').baileysMock)
jest.mock('baileys/lib/Types/LabelAssociation', () => ({ LabelAssociationType: { Chat: 'label_jid', Message: 'label_message' } }), { virtual: true })

import { DB, Harness, PREFIX, startMongo } from './helpers/storeHarness'
import { makeEnhancedMongoDBStore } from '../makeEnhancedMongoDBStore'
import { ConnectionManager } from '../utils/connectionManager'

jest.setTimeout(60000)

const L = '987651001@lid'
const P1 = '60123451011@s.whatsapp.net'
const P2 = '60123451022@s.whatsapp.net'

let h: Harness
let seq = 0
const stores: any[] = []
const newInstance = () => `wab1384-${++seq}-${Date.now()}`
const contacts = () => h.raw.db(DB).collection(`${PREFIX}contacts`)
const mappings = () => h.raw.db(DB).collection(`${PREFIX}lidMappings`)
const holderOf = async (instanceId: string) => (await contacts().findOne({ instanceId, lid: L }))?.id ?? null
const mappedPhone = async (instanceId: string) => (await mappings().findOne({ instanceId, lid: L }))?.phoneNumber ?? null

const make = async (instanceId: string, extra: Record<string, unknown> = {}) => {
    const store: any = await makeEnhancedMongoDBStore({
        uri: h.uri,
        database: DB,
        instanceId,
        collectionPrefix: PREFIX,
        useSharedConnections: true,
        debounceHistoryEvents: false,
        logLevel: 'error',
        lidHandler: { enableCache: false },
        ...extra,
    } as any)
    stores.push(store)
    return store
}

beforeAll(async () => { h = await startMongo() })
afterAll(async () => {
    await ConnectionManager.getInstance().shutdown()
    await h.stop()
})
beforeEach(() => {
    for (const level of ['error', 'warn', 'log', 'debug'] as const) jest.spyOn(console, level).mockImplementation(() => {})
})
afterEach(async () => {
    while (stores.length) {
        const s = stores.pop()
        await s.close().catch(() => {})
    }
    jest.restoreAllMocks()
})

describe('WAB-1384 storeLookedUpLidMapping', () => {
    test('a free LID is stored in contacts and lidMappings for the phone', async () => {
        const instanceId = newInstance()
        const store = await make(instanceId)
        expect(store.canStoreLidMappings()).toBe(true)
        expect(await store.storeLookedUpLidMapping(L, P1)).toBe('stored')
        expect(await holderOf(instanceId)).toBe(P1)
        expect(await mappedPhone(instanceId)).toBe(P1)
    })

    test('storing the same pair again is stored, with no second mapping row', async () => {
        const instanceId = newInstance()
        const store = await make(instanceId)
        expect(await store.storeLookedUpLidMapping(L, P1)).toBe('stored')
        expect(await store.storeLookedUpLidMapping(L, P1)).toBe('stored')
        expect(await mappings().countDocuments({ instanceId, lid: L })).toBe(1)
    })

    test('a device-specific LID and a c.us phone are normalized before the write', async () => {
        const instanceId = newInstance()
        const store = await make(instanceId)
        expect(await store.storeLookedUpLidMapping('987651001:7@lid', '60123451011@c.us')).toBe('stored')
        expect(await holderOf(instanceId)).toBe(P1)
    })

    test('a LID that another phone owns in contacts and lidMappings is kept for that owner', async () => {
        const instanceId = newInstance()
        const store = await make(instanceId)
        await contacts().insertOne({ instanceId, id: P1, lid: L })
        await mappings().insertOne({ instanceId, lid: L, phoneNumber: P1, firstSeen: new Date() })
        expect(await store.storeLookedUpLidMapping(L, P2)).toBe('kept')
        expect(await holderOf(instanceId)).toBe(P1)
        expect(await mappedPhone(instanceId)).toBe(P1)
        expect((await contacts().findOne({ instanceId, id: P2 }))?.lid).toBeUndefined()
    })

    test('a LID that another phone owns only in lidMappings is kept', async () => {
        const instanceId = newInstance()
        const store = await make(instanceId)
        await mappings().insertOne({ instanceId, lid: L, phoneNumber: P1, firstSeen: new Date() })
        expect(await store.storeLookedUpLidMapping(L, P2)).toBe('kept')
        expect(await mappedPhone(instanceId)).toBe(P1)
        expect(await holderOf(instanceId)).toBeNull()
    })

    test('a LID that another phone holds only in contacts is kept', async () => {
        const instanceId = newInstance()
        const store = await make(instanceId)
        await contacts().insertOne({ instanceId, id: P1, lid: L })
        expect(await store.storeLookedUpLidMapping(L, P2)).toBe('kept')
        expect(await holderOf(instanceId)).toBe(P1)
        expect(await mappedPhone(instanceId)).toBeNull()
    })

    test('the same LID on another instance does not block this instance', async () => {
        const other = newInstance()
        await contacts().insertOne({ instanceId: other, id: P1, lid: L })
        await mappings().insertOne({ instanceId: other, lid: L, phoneNumber: P1, firstSeen: new Date() })
        const instanceId = newInstance()
        const store = await make(instanceId)
        expect(await store.storeLookedUpLidMapping(L, P2)).toBe('stored')
        expect(await holderOf(other)).toBe(P1)
    })

    // Review round 3 (R1-1, R1-2): one owner rule for the pre-check, the write and the read-back. A digits-only
    // phoneNumber is a phone owner, and a legacy LID spelling without the @lid suffix counts as the same LID.
    test('a digits-only mapping of another phone is kept, not replaced', async () => {
        const instanceId = newInstance()
        const store = await make(instanceId)
        await mappings().insertOne({ instanceId, lid: L, phoneNumber: '60123451011', firstSeen: new Date() })
        expect(await store.storeLookedUpLidMapping(L, P2)).toBe('kept')
        expect(await mappedPhone(instanceId)).toBe('60123451011')
        expect(await holderOf(instanceId)).toBeNull()
    })

    test('a digits-only mapping of the same phone is upgraded to the phone JID', async () => {
        const instanceId = newInstance()
        const store = await make(instanceId)
        await mappings().insertOne({ instanceId, lid: L, phoneNumber: '60123451011', firstSeen: new Date() })
        expect(await store.storeLookedUpLidMapping(L, P1)).toBe('stored')
        expect(await mappedPhone(instanceId)).toBe(P1)
        expect(await holderOf(instanceId)).toBe(P1)
    })

    test('a legacy mapping under the LID without its @lid suffix, for another phone, is kept', async () => {
        const instanceId = newInstance()
        const store = await make(instanceId)
        await mappings().insertOne({ instanceId, lid: '987651001', phoneNumber: P1, firstSeen: new Date() })
        expect(await store.storeLookedUpLidMapping(L, P2)).toBe('kept')
        expect(await mappings().countDocuments({ instanceId, lid: L })).toBe(0)
        expect(await holderOf(instanceId)).toBeNull()
    })

    test.each([
        ['a phone as the LID', P1, P2],
        ['a LID as the phone', L, '987651009@lid'],
        ['an empty LID', '', P1],
        ['a group as the phone', L, '1203630@g.us'],
        ['an empty LID user', '@lid', P1],
        ['a non-numeric LID user', 'not-a-number@lid', P1],
        ['a non-numeric device', '987651001:x@lid', P1],
    ])('%s is invalid and writes nothing', async (_label, lid, pn) => {
        const instanceId = newInstance()
        const store = await make(instanceId)
        expect(await store.storeLookedUpLidMapping(lid, pn)).toBe('invalid')
        expect(await contacts().countDocuments({ instanceId })).toBe(0)
        expect(await mappings().countDocuments({ instanceId })).toBe(0)
    })

    test('without the lidHandler option the store cannot save pairs', async () => {
        const instanceId = newInstance()
        const store = await make(instanceId, { lidHandler: undefined })
        expect(store.canStoreLidMappings()).toBe(false)
        expect(await store.storeLookedUpLidMapping(L, P1)).toBe('unavailable')
        expect(await mappings().countDocuments({ instanceId })).toBe(0)
    })

    test('a closing or closed store cannot save pairs', async () => {
        const instanceId = newInstance()
        const store = await make(instanceId)
        const closing = store.close()
        expect(store.canStoreLidMappings()).toBe(false)
        expect(await store.storeLookedUpLidMapping(L, P1)).toBe('unavailable')
        await closing
        expect(store.canStoreLidMappings()).toBe(false)
        expect(await mappings().countDocuments({ instanceId })).toBe(0)
    })

    test('two phones racing for one free LID: exactly one is stored, the other kept', async () => {
        const instanceId = newInstance()
        const store = await make(instanceId)
        const results = await Promise.all([store.storeLookedUpLidMapping(L, P1), store.storeLookedUpLidMapping(L, P2)])
        expect([...results].sort()).toEqual(['kept', 'stored'])
        const winner = results[0] === 'stored' ? P1 : P2
        expect(await holderOf(instanceId)).toBe(winner)
        expect(await mappedPhone(instanceId)).toBe(winner)
    })
})
