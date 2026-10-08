// WAB-866: a Baileys media message is downloaded inline only; the store must not also add a
// MEDIA_DOWNLOAD job to the shared media queue. Real mongod; queue manager and downloader are fakes.
// jest.mock factories are hoisted above imports, so they must use require().
/* eslint-disable @typescript-eslint/no-var-requires */
jest.mock('baileys', () => require('./helpers/baileysDouble').baileysMock)
jest.mock('baileys/lib/Types/LabelAssociation', () => ({ LabelAssociationType: { Chat: 'label_jid', Message: 'label_message' } }), { virtual: true })
jest.mock('../utils/sharedQueueManager', () => {
    const actual = jest.requireActual('../utils/sharedQueueManager')
    const addJob = jest.fn(async () => ({ id: 'fake' }))
    const fake = {
        addJob,
        registerInstanceProcessor: jest.fn(),
        unregisterInstanceProcessors: jest.fn(),
        claimInstanceOwnership: jest.fn(async () => ({ owned: true, ownerId: 'w1' })),
        releaseInstanceOwnership: jest.fn(async () => undefined),
        getWorkerId: () => 'w1',
    }
    return { ...actual, SharedQueueManager: { getInstance: () => fake, resetInstance: () => undefined }, __fake: fake }
})
jest.mock('../utils/media', () => ({
    ...jest.requireActual('../utils/media'),
    downloadMedia: jest.fn(),
    downloadOfficialAPIMedia: jest.fn(),
}))

import { EventEmitter } from 'events'
import { Harness, INSTANCE, JID, S, baseStoreConfig, messages, startMongo } from './helpers/storeHarness'
import { makeEnhancedMongoDBStore } from '../makeEnhancedMongoDBStore'
import { JobType } from '../utils/sharedQueueManager'
import { downloadMedia, downloadOfficialAPIMedia } from '../utils/media'

jest.setTimeout(60000)

const fakeQueue = (require('../utils/sharedQueueManager') as any).__fake
const inlineDownload = downloadMedia as jest.Mock
const officialDownload = downloadOfficialAPIMedia as jest.Mock
const mediaJobs = () => fakeQueue.addJob.mock.calls.filter((c: any[]) => c[0] === JobType.MEDIA_DOWNLOAD)

const SAVED = {
    success: true,
    localPath: '/media/inst/abc.jpg',
    mediaType: 'image',
    fileName: 'abc.jpg',
    fileSize: 1234,
    mediaHash: 'hash-abc',
    reused: false,
}

let h: Harness
const opened: Array<{ store: any }> = []

const openStore = async (mediaEnabled: boolean) => {
    const store = await makeEnhancedMongoDBStore({
        ...baseStoreConfig(h.uri),
        redis: { connection: 'redis://127.0.0.1:1', useSharedQueues: true },
        events: { 'messages.upsert': { enabled: true } },
        media: { enabled: mediaEnabled, baseDir: '/tmp/wab866-media' },
    } as any)
    opened.push({ store })
    const ev = new EventEmitter()
    store.bind(ev)
    return ev
}

const imageMessage = (id: string, extra: Record<string, unknown> = {}) => ({
    key: { id, remoteJid: JID, fromMe: false },
    message: { imageMessage: { mimetype: 'image/jpeg', fileLength: 1234, url: 'https://mmg.example/x', mediaKey: 'k' } },
    messageTimestamp: S,
    ...extra,
})

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
const row = (id: string) => messages(h).findOne({ instanceId: INSTANCE, 'key.id': id })

beforeAll(async () => {
    h = await startMongo()
})
afterAll(async () => {
    for (const { store } of opened) {
        store.unbind?.()
        await store.close?.()
    }
    await h.stop()
})
beforeEach(async () => {
    fakeQueue.addJob.mockClear()
    inlineDownload.mockReset().mockResolvedValue(SAVED)
    officialDownload.mockReset().mockResolvedValue(SAVED)
    await messages(h).deleteMany({})
})

describe('WAB-866 Baileys media is downloaded inline without a queued duplicate', () => {
    it('adds no MEDIA_DOWNLOAD job and saves the media fields inline', async () => {
        const ev = await openStore(true)
        ev.emit('messages.upsert', { type: 'notify', messages: [imageMessage('img1')] })

        const saved = await waitFor(async () => {
            const doc = await row('img1')
            return doc?.mediaUrl ? doc : null
        })
        await settle()

        expect(mediaJobs()).toHaveLength(0)
        expect(downloadMedia).toHaveBeenCalledTimes(1)
        expect(saved).toMatchObject({
            mediaUrl: SAVED.localPath,
            mediaType: 'image',
            mediaFileName: 'abc.jpg',
            mediaFileSize: 1234,
            mediaHash: 'hash-abc',
        })
    })

    // Backfill: these behaviors already hold before the fix.
    it('does nothing for media when media download is disabled', async () => {
        const ev = await openStore(false)
        ev.emit('messages.upsert', { type: 'notify', messages: [imageMessage('img2')] })
        await waitFor(() => row('img2'))
        await settle()

        expect(mediaJobs()).toHaveLength(0)
        expect(downloadMedia).not.toHaveBeenCalled()
        expect((await row('img2'))?.mediaUrl).toBeUndefined()
    })

    it('keeps the message without mediaUrl when the inline download fails', async () => {
        inlineDownload.mockResolvedValue({ success: false, error: 'expired' })
        const ev = await openStore(true)
        ev.emit('messages.upsert', { type: 'notify', messages: [imageMessage('img3')] })
        await waitFor(() => row('img3'))
        await settle()

        expect(downloadMedia).toHaveBeenCalledTimes(1)
        expect(mediaJobs()).toHaveLength(0)
        expect((await row('img3'))?.mediaUrl).toBeUndefined()
    })

    it('leaves Official API media enqueue behavior unchanged', async () => {
        const ev = await openStore(true)
        ev.emit('messages.upsert', { type: 'notify', messages: [imageMessage('off1', { official_api: true })] })
        await waitFor(() => row('off1'))
        await settle()

        // Pins today's Official behavior (upsertMessage enqueue + event-handler enqueue); out of WAB-866 scope.
        expect(mediaJobs()).toHaveLength(2)
    })
})
