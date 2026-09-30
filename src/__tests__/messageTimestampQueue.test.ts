// WAB-654 acceptance: the store's real queue processors (shared and per-instance) store an
// Int32 seconds messageTimestamp for jobs delivered through Redis. Only the transport is
// doubled; the processors are the store's own callbacks and MongoDB is real.
// jest.mock factories are hoisted above imports, so they must use require().
/* eslint-disable @typescript-eslint/no-var-requires */
jest.mock('baileys', () => require('./helpers/baileysDouble').baileysMock)
jest.mock('baileys/lib/Types/LabelAssociation', () => ({ LabelAssociationType: { Chat: 'label_jid', Message: 'label_message' } }), { virtual: true })
jest.mock('bullmq', () => require('./helpers/queueDouble').bullmqMock)
jest.mock('ioredis', () => require('./helpers/queueDouble').ioredisMock)

import { Collection, Long } from 'mongodb'
import * as queue from './helpers/queueDouble'
import { WebMessageInfo } from './helpers/baileysDouble'
import { Harness, INSTANCE, JID, S, baseStoreConfig, messages, seedMessage, startMongo, storedTimestamp } from './helpers/storeHarness'
import { makeEnhancedMongoDBStore } from '../makeEnhancedMongoDBStore'
import { JobType, SharedQueueManager } from '../utils/sharedQueueManager'

jest.setTimeout(60000)

const MESSAGE_EDIT = 14
const plain = (id: string, messageTimestamp: unknown) => ({
    key: { id, remoteJid: JID, fromMe: false },
    message: { conversation: id },
    messageTimestamp,
})
const edit = (id: string, targetId: string, messageTimestamp: unknown) => ({
    ...plain(id, messageTimestamp),
    message: { protocolMessage: { type: MESSAGE_EDIT, key: { id: targetId, remoteJid: JID } } },
})
const duplicateKey = () => Object.assign(new Error('E11000 duplicate key error'), { code: 11000 })

let h: Harness
beforeAll(async () => {
    h = await startMongo()
})
afterAll(async () => {
    await h.stop()
})

describe('per-instance BullMQ message processor', () => {
    let store: any
    let process: (job: any) => Promise<any>

    beforeAll(async () => {
        queue.reset()
        store = await makeEnhancedMongoDBStore({
            ...baseStoreConfig(h.uri),
            redis: { connection: 'redis://queue-double', useSharedQueues: false },
        } as any)
        const name = [...queue.processors.keys()].find((n) => n.includes('_messages_'))
        if (!name) throw new Error('store did not create a messages worker')
        process = queue.processors.get(name)!
    })
    afterAll(async () => {
        await store?.close()
    })
    beforeEach(async () => {
        await messages(h).deleteMany({})
        queue.added.length = 0
        jest.restoreAllMocks()
    })

    // upsertMessage enqueues JSON.parse(JSON.stringify(message)). It is replayed here directly because
    // per-instance queue setup currently throws at c4fa12e (a missing semicolon before
    // `(worker as any).__eventHandlers` calls the Worker as a function), so the store never
    // enqueues on this path. Production uses shared queues only. Tracked separately from WAB-654.
    it('a JSON-cloned proto message job (string timestamp) is stored as Int32 seconds', async () => {
        const msg = WebMessageInfo.fromObject({ key: { id: 'q1', remoteJid: JID, fromMe: false }, message: { conversation: 'hi' }, messageTimestamp: S })
        const clonedMessage = JSON.parse(JSON.stringify(msg))
        expect(typeof clonedMessage.messageTimestamp).toBe('string')
        await process(queue.deliver({ type: 'upsert', jid: JID, message: clonedMessage, instanceId: INSTANCE, timestamp: Date.now() }))
        expect(await storedTimestamp(h, 'q1')).toMatchObject({ type: 'int', value: S })
    })

    it.each([
        ['plain {low, high, unsigned}', { low: S, high: 0, unsigned: true }],
        ['serialized Long', Long.fromNumber(S, true)],
    ])('stores a %s timestamp from a delivered job as Int32 seconds', async (_label, ts) => {
        await process(queue.deliver({ type: 'upsert', jid: JID, message: plain('q2', ts), instanceId: INSTANCE, timestamp: Date.now() }))
        expect(await storedTimestamp(h, 'q2')).toMatchObject({ type: 'int', value: S })
    })

    it('an edit job keeps the original time, normalized from a stored string', async () => {
        await seedMessage(h, 'q-orig', '1759100000')
        await process(queue.deliver({ type: 'upsert', jid: JID, message: edit('q-edit', 'q-orig', S + 50), instanceId: INSTANCE, timestamp: Date.now() }))
        expect(await storedTimestamp(h, 'q-edit')).toMatchObject({ type: 'int', value: S })
    })

    it('an edit job over an unconvertible stored time uses the incoming timestamp', async () => {
        await seedMessage(h, 'q-orig2', 'junk')
        await process(queue.deliver({ type: 'upsert', jid: JID, message: edit('q-edit2', 'q-orig2', S + 50), instanceId: INSTANCE, timestamp: Date.now() }))
        expect(await storedTimestamp(h, 'q-edit2')).toMatchObject({ type: 'int', value: S + 50 })
    })

    it('the duplicate-key fallback stores Int32 seconds', async () => {
        await seedMessage(h, 'q-dup', S)
        jest.spyOn(Collection.prototype, 'replaceOne').mockRejectedValueOnce(duplicateKey())
        await process(queue.deliver({ type: 'upsert', jid: JID, message: plain('q-dup', '1759100007'), instanceId: INSTANCE, timestamp: Date.now() }))
        expect(await storedTimestamp(h, 'q-dup')).toMatchObject({ type: 'int', value: S + 7 })
    })

    it('an update job stores Int32 seconds', async () => {
        await seedMessage(h, 'q-upd', S)
        await process(queue.deliver({ type: 'update', jid: JID, messageId: 'q-upd', update: { messageTimestamp: Long.fromNumber(S + 3) }, instanceId: INSTANCE, timestamp: Date.now() }))
        expect(await storedTimestamp(h, 'q-upd')).toMatchObject({ type: 'int', value: S + 3 })
    })
})

describe('shared-queue message processor', () => {
    let store: any
    let process: ((job: any) => Promise<any>) | undefined

    const sharedJob = (data: any) => queue.deliver({ instanceId: INSTANCE, type: JobType.MESSAGES, data, timestamp: Date.now() })

    beforeAll(async () => {
        const fakeManager = {
            claimInstanceOwnership: async () => ({ owned: true, ownerId: 'worker-test' }),
            getWorkerId: () => 'worker-test',
            registerInstanceProcessor: (instanceId: string, type: JobType, fn: (job: any) => Promise<any>) => {
                if (instanceId === INSTANCE && type === JobType.MESSAGES) process = fn
            },
            addJob: async () => ({}),
            releaseInstanceOwnership: async () => undefined,
            unregisterInstanceProcessors: () => undefined,
        }
        jest.spyOn(SharedQueueManager, 'getInstance').mockReturnValue(fakeManager as any)
        store = await makeEnhancedMongoDBStore({ ...baseStoreConfig(h.uri), redis: { connection: 'redis://queue-double' } } as any)
        if (!process) throw new Error('store did not register a shared messages processor')
    })
    afterAll(async () => {
        await store?.close()
    })
    beforeEach(async () => {
        await messages(h).deleteMany({})
    })

    it('stores a delivered proto-JSON upsert (string timestamp) as Int32 seconds', async () => {
        const json = WebMessageInfo.fromObject({ key: { id: 's1', remoteJid: JID, fromMe: false }, message: { conversation: 'hi' }, messageTimestamp: S }).toJSON()
        await process!(sharedJob({ type: 'upsert', jid: JID, message: json }))
        expect(await storedTimestamp(h, 's1')).toMatchObject({ type: 'int', value: S })
    })

    it('stores a delivered {low, high} upsert as Int32 seconds', async () => {
        await process!(sharedJob({ type: 'upsert', jid: JID, message: plain('s2', { low: S, high: 0, unsigned: true }) }))
        expect(await storedTimestamp(h, 's2')).toMatchObject({ type: 'int', value: S })
    })

    it('an update job stores Int32 seconds', async () => {
        await seedMessage(h, 's-upd', S)
        await process!(sharedJob({ type: 'update', jid: JID, messageId: 's-upd', update: { messageTimestamp: '1759100004' } }))
        expect(await storedTimestamp(h, 's-upd')).toMatchObject({ type: 'int', value: S + 4 })
    })
})
