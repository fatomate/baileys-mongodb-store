// WAB-1275: the store paths that write contacts.lid follow the one ownership rule.
// Real mongod; the store creates its production indexes (unique instanceId + lid).
/* eslint-disable @typescript-eslint/no-require-imports */
jest.mock('baileys', () => require('./helpers/baileysDouble').baileysMock)
jest.mock('baileys/lib/Types/LabelAssociation', () => ({ LabelAssociationType: { Chat: 'label_jid', Message: 'label_message' } }), { virtual: true })

import { EventEmitter } from 'events'
import { DB, Harness, PREFIX, startMongo } from './helpers/storeHarness'
import { makeEnhancedMongoDBStore } from '../makeEnhancedMongoDBStore'
import { ConnectionManager } from '../utils/connectionManager'

jest.setTimeout(60000)

const L = '987650001@lid'
const P1 = '60123450011@s.whatsapp.net'
const P2 = '60123450022@s.whatsapp.net'

let h: Harness
let seq = 0
const stores: any[] = []
const newInstance = () => `wab1275-${++seq}-${Date.now()}`
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
        events: { 'lid-mapping.update': { enabled: true }, 'contacts.upsert': { enabled: true } },
        ...extra,
    } as any)
    stores.push(store)
    return store
}
const owner = async (instanceId: string) => {
    await contacts().insertOne({ instanceId, id: P1, lid: L })
    await mappings().insertOne({ instanceId, lid: L, phoneNumber: P1, firstSeen: new Date() })
}
const waitFor = async (check: () => Promise<boolean>, timeoutMs = 5000) => {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
        if (await check()) return true
        await new Promise((r) => setTimeout(r, 50))
    }
    return false
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
        try { s.unbind() } catch { /* already unbound */ }
        await s.close().catch(() => {})
    }
    jest.restoreAllMocks()
})

describe('WAB-1275 store ownership paths', () => {
    test('S11: lid-mapping.update moves the LID in contacts and in lidMappings together', async () => {
        const instanceId = newInstance()
        const store = await make(instanceId)
        const ev = new EventEmitter()
        store.bind(ev)
        await owner(instanceId)

        ev.emit('lid-mapping.update', { lid: L, pn: P2 })
        await waitFor(async () => (await holderOf(instanceId)) === P2 && (await mappedPhone(instanceId)) === P2)

        expect(await holderOf(instanceId)).toBe(P2)
        expect(await mappedPhone(instanceId)).toBe(P2)
    })

    test('S12: lid-mapping.update with a LID-format phone changes no contact', async () => {
        const instanceId = newInstance()
        const store = await make(instanceId)
        const ev = new EventEmitter()
        store.bind(ev)
        const otherLid = '987650002@lid'
        await contacts().insertOne({ instanceId, id: otherLid, jid: otherLid })
        const sentinelLid = '987650009@lid'
        const sentinelPhone = '60123450099@s.whatsapp.net'

        ev.emit('lid-mapping.update', { lid: L, pn: otherLid })
        ev.emit('lid-mapping.update', { lid: sentinelLid, pn: sentinelPhone })
        await waitFor(async () => !!(await contacts().findOne({ instanceId, id: sentinelPhone, lid: sentinelLid })))
        await new Promise((r) => setTimeout(r, 200))

        expect((await contacts().findOne({ instanceId, id: otherLid }))?.lid).toBeUndefined()
        expect(await holderOf(instanceId)).toBeNull()
    })

    test('S13: a native LID fetch does not take the LID from its phone owner', async () => {
        const instanceId = newInstance()
        const getLIDsForPNs = jest.fn(async (pns: string[]) => pns.filter((pn) => pn === P2).map((pn) => ({ pn, lid: L })))
        const sock = {
            signalRepository: { lidMapping: { getLIDForPN: async () => null, getLIDsForPNs } },
            profilePictureUrl: async () => null,
        }
        const store = await make(instanceId, { sock, lidConfig: { requestDelay: 0, retryAttempts: 1 } })
        const ev = new EventEmitter()
        store.bind(ev)
        await owner(instanceId)

        ev.emit('contacts.upsert', [{ id: P2, notify: 'New' }])
        await waitFor(async () => getLIDsForPNs.mock.calls.length > 0)
        await new Promise((r) => setTimeout(r, 300))

        expect(getLIDsForPNs).toHaveBeenCalled()
        expect(await holderOf(instanceId)).toBe(P1)
        expect((await contacts().findOne({ instanceId, id: P2 }))?.lid).toBeUndefined()
        expect(await mappedPhone(instanceId)).toBe(P1)
    })

    test('S14 (PR review): without the lidHandler option the store does not fetch LIDs it cannot store', async () => {
        const instanceId = newInstance()
        const getLIDsForPNs = jest.fn(async (pns: string[]) => pns.map((pn) => ({ pn, lid: L })))
        const sock = {
            signalRepository: { lidMapping: { getLIDForPN: async () => null, getLIDsForPNs } },
            profilePictureUrl: async () => null,
        }
        const store = await make(instanceId, { sock, lidHandler: undefined, lidConfig: { requestDelay: 0, retryAttempts: 1 } })
        const ev = new EventEmitter()
        store.bind(ev)

        ev.emit('contacts.upsert', [{ id: P2, notify: 'New' }])
        await waitFor(async () => !!(await contacts().findOne({ instanceId, id: P2 })))
        await new Promise((r) => setTimeout(r, 300))

        expect(getLIDsForPNs).not.toHaveBeenCalled()
        expect(await holderOf(instanceId)).toBeNull()
    })
})
