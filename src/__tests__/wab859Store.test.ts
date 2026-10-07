// WAB-859 acceptance: receipts do not move stored message times, LID-addressed messages are
// stored under the phone jid, and a re-key conflict never drops a new message. Real mongod.
// jest.mock factories are hoisted above imports, so they must use require().
/* eslint-disable @typescript-eslint/no-var-requires */
jest.mock('baileys', () => require('./helpers/baileysDouble').baileysMock)
jest.mock('baileys/lib/Types/LabelAssociation', () => ({ LabelAssociationType: { Chat: 'label_jid', Message: 'label_message' } }), { virtual: true })

import { EventEmitter } from 'events'
import { Collection } from 'mongodb'
import { WebMessageInfo } from './helpers/baileysDouble'
import { Harness, INSTANCE, JID, S, baseStoreConfig, messages, startMongo, storedTimestamp } from './helpers/storeHarness'
import { makeEnhancedMongoDBStore } from '../makeEnhancedMongoDBStore'
import { LidHandler } from '../utils/lidHandler'
import { InstanceAccessContext } from '../utils/auth'

jest.setTimeout(60000)

const LID = '123456789012345@lid'
const LID_DIGITS = '123456789012345'
const PHONE_DIGITS = '60123456789'
const PHONE = JID
const READ = 4

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

const settle = () => new Promise((r) => setTimeout(r, 300))

const findByKeyId = (id: string) => messages(h).find({ instanceId: INSTANCE, 'key.id': id }).toArray()

const seed = async (doc: Record<string, unknown>) => {
    await messages(h).insertOne({ instanceId: INSTANCE, message: { conversation: 'seeded' }, ...doc })
}

const lidRow = (id: string) => ({
    jid: LID,
    key: { id, remoteJid: LID, fromMe: false },
    messageTimestamp: S,
})

const liveMessage = (id: string, key: Record<string, unknown>) => ({
    key: { id, fromMe: false, ...key },
    message: { conversation: id },
    messageTimestamp: S + 10,
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
    jest.restoreAllMocks()
    await messages(h).deleteMany({})
    await h.raw.db('wab654').collection('wabot_contacts').deleteMany({})
    await h.raw.db('wab654').collection('wabot_lidMappings').deleteMany({})
})

describe('receipts and other non-edit updates keep an existing valid time', () => {
    it('an rc14 read receipt through messages.update does not move the stored time', async () => {
        await seed({ jid: PHONE, key: { id: 'r1', remoteJid: PHONE, fromMe: false }, messageTimestamp: S })
        ev.emit('messages.update', [{ key: { id: 'r1', remoteJid: PHONE, fromMe: false }, update: { status: READ, messageTimestamp: S + 3600 } }])
        const stored = await waitFor(async () => {
            const row = await storedTimestamp(h, 'r1')
            return row?.doc.status === READ ? row : null
        })
        expect(stored).toMatchObject({ type: 'int', value: S })
    })

    it('updateMessage with a later time keeps the stored time and applies the status', async () => {
        await seed({ jid: PHONE, key: { id: 'r2', remoteJid: PHONE, fromMe: false }, messageTimestamp: S })
        await store.updateMessage(PHONE, 'r2', { status: READ, messageTimestamp: S + 3600 })
        const stored = await storedTimestamp(h, 'r2')
        expect(stored).toMatchObject({ type: 'int', value: S })
        expect(stored?.doc.status).toBe(READ)
    })

    it('updateMessage with an earlier time keeps the stored time', async () => {
        await seed({ jid: PHONE, key: { id: 'r3', remoteJid: PHONE, fromMe: false }, messageTimestamp: S })
        await store.updateMessage(PHONE, 'r3', { status: READ, messageTimestamp: S - 600 })
        expect(await storedTimestamp(h, 'r3')).toMatchObject({ type: 'int', value: S })
    })

    it('a WAB-654 fallback time (with messageTimestampInvalid) counts as valid and is kept', async () => {
        await seed({ jid: PHONE, key: { id: 'r4', remoteJid: PHONE, fromMe: false }, messageTimestamp: S, messageTimestampInvalid: 'junk' })
        await store.updateMessage(PHONE, 'r4', { status: READ, messageTimestamp: S - 100 })
        expect(await storedTimestamp(h, 'r4')).toMatchObject({ type: 'int', value: S })
    })

    it('an invalid stored time is replaced by a valid incoming time', async () => {
        await seed({ jid: PHONE, key: { id: 'r5', remoteJid: PHONE, fromMe: false }, messageTimestamp: 'junk' })
        await store.updateMessage(PHONE, 'r5', { status: READ, messageTimestamp: S + 5 })
        expect(await storedTimestamp(h, 'r5')).toMatchObject({ type: 'int', value: S + 5 })
    })

    it('a missing stored time is filled from the update', async () => {
        await seed({ jid: PHONE, key: { id: 'r6', remoteJid: PHONE, fromMe: false } })
        await store.updateMessage(PHONE, 'r6', { status: READ, messageTimestamp: S + 6 })
        expect(await storedTimestamp(h, 'r6')).toMatchObject({ type: 'int', value: S + 6 })
    })

    it('an invalid incoming time never replaces the stored time', async () => {
        await seed({ jid: PHONE, key: { id: 'r7', remoteJid: PHONE, fromMe: false }, messageTimestamp: S })
        await store.updateMessage(PHONE, 'r7', { status: READ, messageTimestamp: 'junk' })
        const stored = await storedTimestamp(h, 'r7')
        expect(stored).toMatchObject({ type: 'int', value: S })
        expect(stored?.doc.status).toBe(READ)
    })

    it('a MESSAGE_EDIT update keeps the original time', async () => {
        await seed({ jid: PHONE, key: { id: 'r8', remoteJid: PHONE, fromMe: false }, messageTimestamp: S })
        await store.updateMessage(PHONE, 'r8', { message: { editedMessage: { message: { conversation: 'edited' } } }, messageTimestamp: S + 900 } as any)
        expect(await storedTimestamp(h, 'r8')).toMatchObject({ type: 'int', value: S })
    })

    it('a fill of a missing time does not overwrite a time another writer stored first', async () => {
        await seed({ jid: PHONE, key: { id: 'r9', remoteJid: PHONE, fromMe: false } })
        // The store reads the row while the time is missing; another writer then stores S.
        const realFindOne = Collection.prototype.findOne
        jest.spyOn(Collection.prototype, 'findOne').mockImplementationOnce(async function (this: Collection, ...args: any[]) {
            const stale = await (realFindOne as any).apply(this, args)
            await messages(h).updateOne({ instanceId: INSTANCE, 'key.id': 'r9' }, { $set: { messageTimestamp: S } })
            return stale
        })
        await store.updateMessage(PHONE, 'r9', { status: READ, messageTimestamp: S + 50 })
        const stored = await storedTimestamp(h, 'r9')
        expect(stored).toMatchObject({ type: 'int', value: S })
        expect(stored?.doc.status).toBe(READ)
    })
})

describe('live LID re-key never drops the new message', () => {
    it('saves the new message when an old LID row already has a phone twin', async () => {
        await seed(lidRow('old1'))
        await seed({ jid: PHONE, key: { id: 'old1', remoteJid: PHONE, fromMe: false }, messageTimestamp: S })
        ev.emit('messages.upsert', { type: 'notify', messages: [liveMessage('new1', { remoteJid: PHONE, remoteJidAlt: LID, addressingMode: 'pn' })] })
        const [saved] = await waitFor(async () => {
            const rows = await findByKeyId('new1')
            return rows.length ? rows : null
        })
        expect(saved.jid).toBe(PHONE)
        // The colliding LID row is skipped, not deleted.
        expect((await findByKeyId('old1')).map((r) => r.jid).sort()).toEqual([PHONE, LID].sort())
    })

    it('saves the new message when the re-key fails for another reason', async () => {
        await seed(lidRow('old2'))
        jest.spyOn(Collection.prototype, 'updateMany').mockRejectedValue(new Error('simulated re-key failure'))
        jest.spyOn(Collection.prototype, 'updateOne').mockImplementation(async function () {
            throw new Error('simulated re-key failure')
        } as any)
        ev.emit('messages.upsert', { type: 'notify', messages: [liveMessage('new2', { remoteJid: PHONE, remoteJidAlt: LID, addressingMode: 'pn' })] })
        const [saved] = await waitFor(async () => {
            const rows = await findByKeyId('new2')
            return rows.length ? rows : null
        })
        expect(saved.jid).toBe(PHONE)
    })

    it('re-keys an old LID row with the LID kept as remoteJidAlt', async () => {
        await seed(lidRow('old3'))
        ev.emit('messages.upsert', { type: 'notify', messages: [liveMessage('new3', { remoteJid: PHONE, remoteJidAlt: LID, addressingMode: 'pn' })] })
        const [row] = await waitFor(async () => {
            const rows = await findByKeyId('old3')
            return rows[0]?.jid === PHONE ? rows : null
        })
        expect(row.key).toMatchObject({ remoteJid: PHONE, remoteJidAlt: LID, addressingMode: 'pn' })
    })

    it('re-keys at most 200 old rows for one new message and still saves it', async () => {
        await messages(h).insertMany(Array.from({ length: 250 }, (_, i) => ({ instanceId: INSTANCE, message: { conversation: 'seeded' }, ...lidRow(`bulk${i}`) })))
        ev.emit('messages.upsert', { type: 'notify', messages: [liveMessage('new4', { remoteJid: PHONE, remoteJidAlt: LID, addressingMode: 'pn' })] })
        await waitFor(async () => (await findByKeyId('new4')).length > 0)
        // Wait until the re-key stops moving rows, so it cannot leak into the next test.
        let previous = -1
        const remaining = await waitFor(async () => {
            await settle()
            const count = await messages(h).countDocuments({ instanceId: INSTANCE, jid: LID })
            const stable = count === previous
            previous = count
            return stable ? { count } : null
        }, 20000)
        expect(remaining.count).toBeGreaterThanOrEqual(50)
    })

    it('stores the new message before the re-key writes old rows', async () => {
        await seed(lidRow('old5'))
        const [{ _id: old5Id }] = await findByKeyId('old5')
        const order: string[] = []
        const realReplace = Collection.prototype.replaceOne
        const realUpdateMany = Collection.prototype.updateMany
        const realUpdateOne = Collection.prototype.updateOne
        jest.spyOn(Collection.prototype, 'replaceOne').mockImplementation(function (this: Collection, ...args: any[]) {
            if (this.collectionName.endsWith('messages') && args[0]?.['key.id'] === 'new5') order.push('save')
            return (realReplace as any).apply(this, args)
        } as any)
        const rekeySpy = function (real: any) {
            return function (this: Collection, ...args: any[]) {
                const f = args[0] || {}
                if (this.collectionName.endsWith('messages') && (f._id ? String(f._id) === String(old5Id) : f['key.remoteJid'] === LID || f.jid === LID)) order.push('rekey')
                return real.apply(this, args)
            }
        }
        jest.spyOn(Collection.prototype, 'updateMany').mockImplementation(rekeySpy(realUpdateMany) as any)
        jest.spyOn(Collection.prototype, 'updateOne').mockImplementation(rekeySpy(realUpdateOne) as any)
        ev.emit('messages.upsert', { type: 'notify', messages: [liveMessage('new5', { remoteJid: PHONE, remoteJidAlt: LID, addressingMode: 'pn' })] })
        await waitFor(async () => (await findByKeyId('new5')).length > 0)
        await settle()
        expect(order[0]).toBe('save')
    })
})

describe('messages.upsert identity', () => {
    it('stores the identity the message had when the event fired, even if another listener changes it later', async () => {
        // A LID-addressed message makes the store await mapping I/O before it saves.
        const msg = liveMessage('snap1', { remoteJid: PHONE, remoteJidAlt: LID, addressingMode: 'pn' })
        ev.once('messages.upsert', () => {
            setImmediate(() => {
                msg.key.id = 'mutated'
                ;(msg.key as any).remoteJid = '999@s.whatsapp.net'
            })
        })
        ev.emit('messages.upsert', { type: 'notify', messages: [msg] })
        const [row] = await waitFor(async () => {
            const rows = await messages(h).find({ instanceId: INSTANCE, 'key.id': { $in: ['snap1', 'mutated'] } }).toArray()
            return rows.length ? rows : null
        })
        expect(row.key.id).toBe('snap1')
        expect(row.jid).toBe(PHONE)
    })

    it('Pattern B stores the complete phone-primary key with the LID as remoteJidAlt', async () => {
        ev.emit('messages.upsert', { type: 'notify', messages: [liveMessage('pb1', { remoteJid: LID, remoteJidAlt: PHONE, addressingMode: 'lid' })] })
        const [row] = await waitFor(async () => {
            const rows = await findByKeyId('pb1')
            return rows.length ? rows : null
        })
        expect(row.jid).toBe(PHONE)
        expect(row.key).toMatchObject({ remoteJid: PHONE, remoteJidAlt: LID, addressingMode: 'pn' })
    })

    it.each([
        ['a device suffix', `${PHONE_DIGITS}:12@s.whatsapp.net`],
        ['@c.us', `${PHONE_DIGITS}@c.us`],
    ])('Pattern B canonicalizes a phone with %s', async (_label, alt) => {
        ev.emit('messages.upsert', { type: 'notify', messages: [liveMessage('pb2', { remoteJid: LID, remoteJidAlt: alt, addressingMode: 'lid' })] })
        const [row] = await waitFor(async () => {
            const rows = await findByKeyId('pb2')
            return rows.length ? rows : null
        })
        expect(row.jid).toBe(PHONE)
        expect(row.key.remoteJid).toBe(PHONE)
    })

    it.each([
        ['a non-numeric local part', 'not-a-phone@s.whatsapp.net'],
        ['a wrong domain', `${PHONE_DIGITS}@example.com`],
    ])('Pattern B ignores a remoteJidAlt with %s', async (_label, alt) => {
        ev.emit('messages.upsert', { type: 'notify', messages: [liveMessage('pb3', { remoteJid: LID, remoteJidAlt: alt, addressingMode: 'lid' })] })
        const [row] = await waitFor(async () => {
            const rows = await findByKeyId('pb3')
            return rows.length ? rows : null
        })
        expect(row.jid).toBe(LID)
    })
})

describe('upsertMessage storage jid', () => {
    it('stores a message whose key carries a phone remoteJidAlt under the phone jid', async () => {
        await store.upsertMessage(LID, liveMessage('u1', { remoteJid: LID, remoteJidAlt: PHONE, addressingMode: 'lid' }))
        const [row] = await findByKeyId('u1')
        expect(row.jid).toBe(PHONE)
        expect(row.key).toMatchObject({ remoteJid: PHONE, remoteJidAlt: LID, addressingMode: 'pn' })
    })

    it('leaves a protobuf history message without alternate fields under its LID', async () => {
        const msg = WebMessageInfo.fromObject({ key: { id: 'u2', remoteJid: LID, fromMe: false }, message: { conversation: 'old' }, messageTimestamp: S })
        await store.upsertMessage(LID, msg)
        const [row] = await findByKeyId('u2')
        expect(row.jid).toBe(LID)
    })

    it('leaves a group message unchanged', async () => {
        const group = '120363000000000000@g.us'
        await store.upsertMessage(group, liveMessage('u3', { remoteJid: group, participant: LID, addressingMode: 'lid' }))
        const [row] = await findByKeyId('u3')
        expect(row.jid).toBe(group)
    })

    it('replaces an existing phone row with the same key id and leaves the old LID row', async () => {
        await seed({ jid: PHONE, key: { id: 'u4', remoteJid: PHONE, fromMe: false }, messageTimestamp: S, message: { conversation: 'before' } })
        await seed(lidRow('u4'))
        await store.upsertMessage(LID, liveMessage('u4', { remoteJid: LID, remoteJidAlt: PHONE, addressingMode: 'lid' }))
        const rows = await findByKeyId('u4')
        expect(rows.map((r) => r.jid).sort()).toEqual([PHONE, LID].sort())
        expect(rows.find((r) => r.jid === PHONE)?.message).toEqual({ conversation: 'u4' })
    })

    it('stores under the phone jid through the duplicate-key fallback', async () => {
        await seed({ jid: PHONE, key: { id: 'u5', remoteJid: PHONE, fromMe: false }, messageTimestamp: S })
        const duplicate = Object.assign(new Error('E11000 duplicate key error'), { code: 11000 })
        jest.spyOn(Collection.prototype, 'replaceOne').mockRejectedValueOnce(duplicate)
        await store.upsertMessage(LID, liveMessage('u5', { remoteJid: LID, remoteJidAlt: PHONE, addressingMode: 'lid' }))
        const rows = await findByKeyId('u5')
        expect(rows).toHaveLength(1)
        expect(rows[0].jid).toBe(PHONE)
        expect(rows[0].message).toEqual({ conversation: 'u5' })
    })
})

describe('LID mapping side effects (PR review)', () => {
    it('upsertMessage stores no mapping when the write is not authorized', async () => {
        const spy = jest.spyOn(LidHandler.prototype, 'storeLidMapping')
        jest.spyOn(InstanceAccessContext.prototype, 'validateAccess').mockImplementationOnce(() => {
            throw new Error('Instance not in allowed list')
        })
        await expect(store.upsertMessage(LID, liveMessage('denied1', { remoteJid: LID, remoteJidAlt: PHONE, addressingMode: 'lid' }))).rejects.toThrow('upsertMessage')
        expect(spy).not.toHaveBeenCalled()
        expect(await findByKeyId('denied1')).toHaveLength(0)
    })

    it('a Pattern A event stores the mapping once', async () => {
        const spy = jest.spyOn(LidHandler.prototype, 'storeLidMapping')
        ev.emit('messages.upsert', { type: 'notify', messages: [liveMessage('once1', { remoteJid: PHONE, remoteJidAlt: LID, addressingMode: 'pn' })] })
        await waitFor(async () => (await findByKeyId('once1')).length > 0)
        await settle()
        expect(spy).toHaveBeenCalledTimes(1)
    })

    it('a hook that saves its event message again still stores the mapping', async () => {
        let captured: any = null
        const hooked = await makeEnhancedMongoDBStore({
            ...baseStoreConfig(h.uri),
            lidHandler: { enableCache: false },
            hooks: { afterStore: async (event: string, payload: any) => { if (event === 'messages.upsert') captured = payload } },
        } as any)
        const hookedEv = new EventEmitter()
        hooked.bind(hookedEv)
        try {
            hookedEv.emit('messages.upsert', { type: 'notify', messages: [liveMessage('hook1', { remoteJid: PHONE, remoteJidAlt: LID, addressingMode: 'pn' })] })
            await waitFor(async () => captured)
            const spy = jest.spyOn(LidHandler.prototype, 'storeLidMapping')
            await store.upsertMessage(PHONE, captured)
            expect(spy).toHaveBeenCalledTimes(1)
        } finally {
            hooked.unbind()
            await hooked.close()
        }
    })

    it('a Pattern B event payload matches the stored phone-primary key', async () => {
        ev.emit('messages.upsert', { type: 'notify', messages: [liveMessage('pbh1', { remoteJid: LID, remoteJidAlt: PHONE, addressingMode: 'lid' })] })
        const [row] = await waitFor(async () => {
            const rows = await findByKeyId('pbh1')
            return rows.length ? rows : null
        })
        expect(row.key).toMatchObject({ remoteJid: PHONE, remoteJidAlt: LID, addressingMode: 'pn' })
        expect(row.lidDebug).toMatchObject({ originalRemoteJid: LID, normalized: true })
    })
})

describe('LID handler logs', () => {
    it('do not contain LID or phone numbers', async () => {
        const lines: string[] = []
        for (const method of ['log', 'info', 'warn', 'error', 'debug'] as const) {
            jest.spyOn(console, method).mockImplementation((...args: unknown[]) => {
                lines.push(args.map((a) => (a instanceof Error ? `${a.message} ${JSON.stringify(a)}` : typeof a === 'string' ? a : JSON.stringify(a))).join(' '))
            })
        }
        await seed(lidRow('log0'))
        ev.emit('messages.upsert', { type: 'notify', messages: [liveMessage('log1', { remoteJid: PHONE, remoteJidAlt: LID, addressingMode: 'pn' })] })
        await waitFor(async () => (await findByKeyId('log1')).length > 0)
        await settle()
        const leaked = lines.filter((l) => l.includes(LID_DIGITS) || l.includes(PHONE_DIGITS))
        expect(leaked).toEqual([])
    })
})
