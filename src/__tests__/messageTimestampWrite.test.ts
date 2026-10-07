// WAB-654 acceptance: messages written through the store's public API are stored with a
// BSON Int32 seconds messageTimestamp, whatever shape the caller passes. Real mongod.
// jest.mock factories are hoisted above imports, so they must use require().
/* eslint-disable @typescript-eslint/no-var-requires */
jest.mock('baileys', () => require('./helpers/baileysDouble').baileysMock)
jest.mock('baileys/lib/Types/LabelAssociation', () => ({ LabelAssociationType: { Chat: 'label_jid', Message: 'label_message' } }), { virtual: true })

import { EventEmitter } from 'events'
import { Collection, Db, Long, MongoClient } from 'mongodb'
import { WebMessageInfo } from './helpers/baileysDouble'
import { Harness, JID, S, baseStoreConfig, messages, nowSeconds, seedMessage, startMongo, storedTimestamp, waitForStored } from './helpers/storeHarness'
import { makeEnhancedMongoDBStore } from '../makeEnhancedMongoDBStore'

jest.setTimeout(60000)

const MESSAGE_EDIT = 14
let h: Harness
let store: any

const plain = (id: string, messageTimestamp: unknown) => ({
    key: { id, remoteJid: JID, fromMe: false },
    message: { conversation: id },
    messageTimestamp,
})

beforeAll(async () => {
    h = await startMongo()
    // monitoringInterval 0 makes the store ping before every operation, so a failed ping
    // drives the real reconnect path (used by the reconnect test).
    store = await makeEnhancedMongoDBStore({ ...baseStoreConfig(h.uri), connectionManager: { monitoringInterval: 0 } } as any)
})
afterAll(async () => {
    await store?.close()
    await h.stop()
})
beforeEach(async () => {
    await messages(h).deleteMany({})
    jest.restoreAllMocks()
})

describe('upsertMessage', () => {
    it('stores a Baileys proto message (uint64 timestamp) as Int32 seconds', async () => {
        const msg = WebMessageInfo.fromObject({ key: { id: 'proto1', remoteJid: JID, fromMe: false }, message: { conversation: 'hi' }, messageTimestamp: S })
        await store.upsertMessage(JID, msg)
        expect(await storedTimestamp(h, 'proto1')).toMatchObject({ type: 'int', value: S })
    })

    it.each([
        ['plain {low, high, unsigned}', { low: S, high: 0, unsigned: true }],
        ['Long', Long.fromNumber(S, true)],
        ['10-digit string', '1759100000'],
        ['milliseconds', 1759100000999],
    ])('stores a %s timestamp as Int32 seconds', async (_label, ts) => {
        await store.upsertMessage(JID, plain('shape1', ts))
        expect(await storedTimestamp(h, 'shape1')).toMatchObject({ type: 'int', value: S })
    })

    it('stores receipt time and the raw value in messageTimestampInvalid for an unconvertible timestamp', async () => {
        const before = nowSeconds()
        await store.upsertMessage(JID, plain('bad1', 'junk'))
        const stored = await storedTimestamp(h, 'bad1')
        expect(stored?.type).toBe('int')
        expect(stored?.value as number).toBeGreaterThanOrEqual(before)
        expect(stored?.value as number).toBeLessThanOrEqual(nowSeconds())
        expect(stored?.doc.messageTimestampInvalid).toBe('junk')
    })

    it('a MESSAGE_EDIT keeps the original message time, normalized from a stored string', async () => {
        await seedMessage(h, 'orig1', '1759100000')
        await store.upsertMessage(JID, {
            ...plain('edit1', S + 50),
            message: { protocolMessage: { type: MESSAGE_EDIT, key: { id: 'orig1', remoteJid: JID } } },
        })
        expect(await storedTimestamp(h, 'edit1')).toMatchObject({ type: 'int', value: S })
    })

    it('a MESSAGE_EDIT over an unconvertible stored time uses the incoming timestamp', async () => {
        await seedMessage(h, 'orig2', 'junk')
        await store.upsertMessage(JID, {
            ...plain('edit2', S + 50),
            message: { protocolMessage: { type: MESSAGE_EDIT, key: { id: 'orig2', remoteJid: JID } } },
        })
        const stored = await storedTimestamp(h, 'edit2')
        expect(stored).toMatchObject({ type: 'int', value: S + 50 })
        expect(stored?.doc).not.toHaveProperty('messageTimestampInvalid')
    })

    it('the duplicate-key fallback stores Int32 seconds', async () => {
        await seedMessage(h, 'dup1', S)
        const duplicate = Object.assign(new Error('E11000 duplicate key error'), { code: 11000 })
        jest.spyOn(Collection.prototype, 'replaceOne').mockRejectedValueOnce(duplicate)
        await store.upsertMessage(JID, plain('dup1', '1759100007'))
        expect(await storedTimestamp(h, 'dup1')).toMatchObject({ type: 'int', value: S + 7 })
    })

    it('still normalizes after the store reconnects to MongoDB', async () => {
        const connect = jest.spyOn(MongoClient.prototype, 'connect')
        const realAdmin = Db.prototype.admin
        jest.spyOn(Db.prototype, 'admin').mockImplementationOnce(function (this: Db) {
            const admin = realAdmin.call(this)
            admin.ping = () => Promise.reject(new Error('simulated ping failure'))
            return admin
        })
        await store.upsertMessage(JID, plain('reconnect1', '1759100000'))
        expect(connect).toHaveBeenCalled()
        expect(await storedTimestamp(h, 'reconnect1')).toMatchObject({ type: 'int', value: S })
    })
})

describe('updateMessage', () => {
    // WAB-859: a non-edit update never changes an existing valid time, so the Long fills a missing one.
    it('stores a Long timestamp as Int32 seconds', async () => {
        await seedMessage(h, 'upd1', undefined)
        await store.updateMessage(JID, 'upd1', { messageTimestamp: Long.fromNumber(S + 3) })
        expect(await storedTimestamp(h, 'upd1')).toMatchObject({ type: 'int', value: S + 3 })
    })

    it('an update without a timestamp keeps the stored timestamp', async () => {
        await seedMessage(h, 'upd2', S)
        await store.updateMessage(JID, 'upd2', { status: 3 })
        const stored = await storedTimestamp(h, 'upd2')
        expect(stored).toMatchObject({ type: 'int', value: S })
        expect(stored?.doc.status).toBe(3)
    })

    it('an unconvertible timestamp in an update is dropped and the stored value kept', async () => {
        await seedMessage(h, 'upd3', S)
        await store.updateMessage(JID, 'upd3', { messageTimestamp: 'junk', status: 4 })
        const stored = await storedTimestamp(h, 'upd3')
        expect(stored).toMatchObject({ type: 'int', value: S })
        expect(stored?.doc.status).toBe(4)
    })
})

describe('messaging-history.set', () => {
    it('stores history-sync proto messages as Int32 seconds', async () => {
        const ev = new EventEmitter()
        store.bind(ev)
        try {
            const msg = WebMessageInfo.fromObject({ key: { id: 'hist1', remoteJid: JID, fromMe: false }, message: { conversation: 'old' }, messageTimestamp: S - 100 })
            ev.emit('messaging-history.set', { chats: [], contacts: [], messages: [msg], isLatest: false })
            expect(await waitForStored(h, 'hist1')).toMatchObject({ type: 'int', value: S - 100 })
        } finally {
            store.unbind()
        }
    })
})
