// WAB-484: writes that change what a Live Chat conversation summary shows (revoke preview,
// grouping JID) must move updatedAt, because the backend list cache reads its delta by updatedAt.
// Real mongod. jest.mock factories are hoisted above imports, so they must use require().
/* eslint-disable @typescript-eslint/no-var-requires */
jest.mock('baileys', () => require('./helpers/baileysDouble').baileysMock)
jest.mock('baileys/lib/Types/LabelAssociation', () => ({ LabelAssociationType: { Chat: 'label_jid', Message: 'label_message' } }), { virtual: true })

import { EventEmitter } from 'events'
import { Harness, INSTANCE, JID, S, baseStoreConfig, messages, startMongo } from './helpers/storeHarness'
import { makeEnhancedMongoDBStore } from '../makeEnhancedMongoDBStore'
import { wrapMessageWrites, writeMessageUpdate } from '../utils/messageTimestamp'

jest.setTimeout(60000)

const LID = '123456789012345@lid'
const PHONE = JID
const REVOKE = 0
const OLD = new Date('2026-01-01T00:00:00Z')

let h: Harness
let store: any
let ev: EventEmitter

const waitFor = async <T>(probe: () => Promise<T | null | undefined | false>, timeoutMs = 5000): Promise<T> => {
    const deadline = Date.now() + timeoutMs
    for (;;) {
        const value = await probe()
        if (value) return value as T
        if (Date.now() > deadline) throw new Error('condition not met in time')
        await new Promise((r) => setTimeout(r, 25))
    }
}

const seed = (id: string, jid: string) => messages(h).insertOne({
    instanceId: INSTANCE,
    jid,
    key: { id, remoteJid: jid, fromMe: false },
    message: { conversation: id },
    messageTimestamp: S,
    updatedAt: OLD,
})
const row = (id: string) => messages(h).findOne({ instanceId: INSTANCE, 'key.id': id })
const revokeOf = (id: string, revokeId: string) => ({
    key: { id: revokeId, remoteJid: PHONE, fromMe: true },
    message: { protocolMessage: { type: REVOKE, key: { id, remoteJid: PHONE, fromMe: false } } },
    messageTimestamp: S + 5,
})

beforeAll(async () => {
    h = await startMongo()
    store = await makeEnhancedMongoDBStore({ ...baseStoreConfig(h.uri), lidHandler: { enableCache: false } } as any)
    ev = new EventEmitter()
    store.bind(ev)
})
afterAll(async () => {
    store?.unbind()
    await store?.close()
    await h.stop()
})
beforeEach(async () => {
    await messages(h).deleteMany({})
    await h.raw.db('wab654').collection('wabot_lidMappings').deleteMany({})
})

describe('revoke moves updatedAt', () => {
    it('upsertMessage marks the revoked message and moves its updatedAt', async () => {
        await seed('rv1', PHONE)
        await store.upsertMessage(PHONE, revokeOf('rv1', 'rv1-revoke'))
        const revoked = await row('rv1')
        expect(revoked).toMatchObject({ revoked: true })
        expect(revoked!.updatedAt.getTime()).toBeGreaterThan(OLD.getTime())
    })

    it('a live messages.upsert revoke moves updatedAt', async () => {
        await seed('rv2', PHONE)
        ev.emit('messages.upsert', { type: 'notify', messages: [revokeOf('rv2', 'rv2-revoke')] })
        const revoked = await waitFor(async () => {
            const value = await row('rv2')
            return value?.revoked ? value : null
        })
        expect(revoked.updatedAt.getTime()).toBeGreaterThan(OLD.getTime())
    })
})

describe('enhanced getMessage key.remoteJid fallback repair moves updatedAt', () => {
    it('moves updatedAt when it rewrites jid to the requested phone', async () => {
        await messages(h).insertOne({
            instanceId: INSTANCE, jid: LID, key: { id: 'fb1', remoteJid: PHONE, fromMe: false },
            message: { conversation: 'fb1' }, messageTimestamp: S, updatedAt: OLD,
        })
        expect(await store.getMessage(PHONE, 'fb1')).toBeTruthy()
        const repaired = await waitFor(async () => {
            const value = await row('fb1')
            return value?.jid === PHONE ? value : null
        })
        expect(repaired.updatedAt.getTime()).toBeGreaterThan(OLD.getTime())
    })
})

describe('writeMessageUpdate timestamp fill moves updatedAt', () => {
    it('stamps updatedAt on the separate messageTimestamp write', async () => {
        await messages(h).insertOne({ instanceId: INSTANCE, jid: PHONE, key: { id: 'ts1', remoteJid: PHONE, fromMe: false }, updatedAt: OLD })
        await writeMessageUpdate(wrapMessageWrites(messages(h)), { instanceId: INSTANCE, jid: PHONE, 'key.id': 'ts1' }, { status: 4, messageTimestamp: S, updatedAt: OLD })
        const filled = await row('ts1')
        expect(filled).toMatchObject({ messageTimestamp: S })
        expect(filled!.updatedAt.getTime()).toBeGreaterThan(OLD.getTime())
    })
})

describe('getMessage LID-to-phone repair moves updatedAt', () => {
    it('moves updatedAt on every LID row it re-keys to the phone', async () => {
        await seed('lid1', LID)
        await seed('lid2', LID)
        const found = await store.getMessage(PHONE, 'lid1')
        expect(found).toBeTruthy()
        const rows = await waitFor(async () => {
            const value = await messages(h).find({ instanceId: INSTANCE, 'key.id': { $in: ['lid1', 'lid2'] } }).toArray()
            return value.every((doc) => doc.key.remoteJid === PHONE) ? value : null
        })
        for (const doc of rows) {
            expect(doc.updatedAt.getTime()).toBeGreaterThan(OLD.getTime())
        }
    })
})
