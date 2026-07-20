jest.mock('baileys', () => ({
    proto: {
        Message: {
            ProtocolMessage: {
                Type: {
                    REVOKE: 0,
                    MESSAGE_EDIT: 14,
                    HISTORY_SYNC_NOTIFICATION: 5,
                    APP_STATE_SYNC_KEY_SHARE: 6,
                    INITIAL_SECURITY_NOTIFICATION_SETTING_SYNC: 9,
                    APP_STATE_SYNC_KEY_REQUEST: 10
                }
            }
        }
    },
    getAggregateVotesInPollMessage: jest.fn(() => []),
    updateMessageWithReceipt: jest.fn(),
    updateMessageWithReaction: jest.fn()
}))

jest.mock('../../types/baileys-compat.js', () => ({
    LabelAssociationType: {
        Chat: 'label_jid',
        Message: 'label_message'
    }
}))

import { MongoClient } from 'mongodb'
import { MongoMemoryServer } from 'mongodb-memory-server'
import { BoundedThrottle, makeEnhancedMongoDBStore } from '../../makeEnhancedMongoDBStore.js'
import type { EnhancedMongoDBStore } from '../../types-enhanced.js'
import { LidHandler } from '../lidHandler.js'

type AsyncListener = (data: any) => void | Promise<void>

class TestEventEmitter {
    private listeners = new Map<string, AsyncListener[]>()

    on(event: string, listener: AsyncListener): void {
        const listeners = this.listeners.get(event) || []
        listeners.push(listener)
        this.listeners.set(event, listeners)
    }

    off(event: string, listener: AsyncListener): void {
        const listeners = this.listeners.get(event) || []
        this.listeners.set(event, listeners.filter(candidate => candidate !== listener))
    }

    removeAllListeners(event: string): void {
        this.listeners.delete(event)
    }

    emit(event: string, data: any): boolean {
        const listeners = this.listeners.get(event) || []
        for (const listener of listeners) {
            void listener(data)
        }
        return listeners.length > 0
    }

    async emitAsync(event: string, data: any): Promise<void> {
        const listeners = this.listeners.get(event) || []
        await Promise.all(listeners.map(listener => listener(data)))
    }
}

describe('messages.upsert LID write-path hardening', () => {
    jest.setTimeout(60_000)

    let mongoServer: MongoMemoryServer
    let inspectionClient: MongoClient
    let databaseName: string
    let store: EnhancedMongoDBStore | null
    let emitter: TestEventEmitter
    let collectionPrefix: string
    let instanceCounter = 0

    beforeAll(async () => {
        mongoServer = await MongoMemoryServer.create()
        inspectionClient = new MongoClient(mongoServer.getUri())
        await inspectionClient.connect()
        databaseName = 'messages-upsert-lid-hardening'
    })

    afterAll(async () => {
        await inspectionClient.close()
        await mongoServer.stop()
    })

    afterEach(async () => {
        jest.restoreAllMocks()
        if (store) {
            await store.close()
            store = null
        }
    })

    const createStore = async (
        hooks: {
            beforeStore?: (eventType: string, data: any) => boolean | Promise<boolean>
            afterStore?: (eventType: string, data: any) => void | Promise<void>
        } = {}
    ): Promise<EnhancedMongoDBStore> => {
        instanceCounter++
        collectionPrefix = `wab259_${instanceCounter}_`
        emitter = new TestEventEmitter()
        store = await makeEnhancedMongoDBStore({
            uri: mongoServer.getUri(),
            database: databaseName,
            instanceId: `wab259-instance-${instanceCounter}`,
            collectionPrefix,
            useSharedConnections: false,
            enableMetrics: true,
            logLevel: 'warn',
            hooks,
            lidHandler: {
                enableCache: false,
                skipIndexCreation: true,
                lookupsEnabled: false
            },
            indexManagement: {
                enableIndexHealthLogging: false
            }
        })
        store.bind(emitter as any)
        store.resetLidResolutionMetrics()
        return store
    }

    it('stores a private message snapshot when the source is mutated during an awaited filter', async () => {
        let releaseFilter!: () => void
        let signalFilterEntered!: () => void
        const filterEntered = new Promise<void>(resolve => {
            signalFilterEntered = resolve
        })
        const filterRelease = new Promise<void>(resolve => {
            releaseFilter = resolve
        })
        let filteredMessage: any

        await createStore({
            beforeStore: async (eventType, message) => {
                if (eventType === 'messages.upsert') {
                    filteredMessage = message
                    signalFilterEntered()
                    await filterRelease
                }
                return true
            }
        })

        const originalJid = '60111111111@s.whatsapp.net'
        const sourceThumbnail = Buffer.from([1, 2, 3, 4])
        const sourceMessage: any = {
            key: {
                id: 'snapshot-message',
                remoteJid: originalJid,
                fromMe: false
            },
            messageTimestamp: 1_700_000_000,
            message: {
                imageMessage: {
                    caption: 'original caption',
                    jpegThumbnail: sourceThumbnail
                }
            }
        }

        const processing = emitter.emitAsync('messages.upsert', {
            messages: [sourceMessage],
            type: 'notify'
        })

        await filterEntered
        sourceMessage.key.remoteJid = '60999999999@s.whatsapp.net'
        sourceMessage.message.imageMessage.caption = 'mutated caption'
        sourceThumbnail[0] = 9
        releaseFilter()
        await processing

        expect(filteredMessage).not.toBe(sourceMessage)
        expect(filteredMessage.key).not.toBe(sourceMessage.key)
        expect(filteredMessage.message.imageMessage.jpegThumbnail).not.toBe(sourceThumbnail)
        expect(Buffer.from(filteredMessage.message.imageMessage.jpegThumbnail)).toEqual(Buffer.from([1, 2, 3, 4]))

        await expect(store!.getMessage(originalJid, 'snapshot-message')).resolves.toEqual(
            expect.objectContaining({
                key: expect.objectContaining({ remoteJid: originalJid }),
                message: expect.objectContaining({
                    imageMessage: expect.objectContaining({
                        caption: 'original caption',
                        jpegThumbnail: Buffer.from([1, 2, 3, 4])
                    })
                })
            })
        )
        await expect(
            inspectionClient.db(databaseName).collection(`${collectionPrefix}messages`).countDocuments({
                jid: '60999999999@s.whatsapp.net',
                'key.id': 'snapshot-message'
            })
        ).resolves.toBe(0)
    })

    it('snapshots the entire upsert batch before awaiting the first message filter', async () => {
        let releaseFirstFilter!: () => void
        let signalFirstFilterEntered!: () => void
        const firstFilterEntered = new Promise<void>(resolve => {
            signalFirstFilterEntered = resolve
        })
        const firstFilterRelease = new Promise<void>(resolve => {
            releaseFirstFilter = resolve
        })

        await createStore({
            beforeStore: async (eventType, message) => {
                if (eventType === 'messages.upsert' && message.key.id === 'batch-snapshot-first') {
                    signalFirstFilterEntered()
                    await firstFilterRelease
                }
                return true
            }
        })

        const secondJid = '60111111115@s.whatsapp.net'
        const secondMessage: any = {
            key: {
                id: 'batch-snapshot-second',
                remoteJid: secondJid,
                fromMe: false
            },
            messageTimestamp: 1_700_000_001,
            message: { conversation: 'original second content' }
        }
        const processing = emitter.emitAsync('messages.upsert', {
            messages: [
                {
                    key: {
                        id: 'batch-snapshot-first',
                        remoteJid: '60111111114@s.whatsapp.net',
                        fromMe: false
                    },
                    messageTimestamp: 1_700_000_000,
                    message: { conversation: 'first content' }
                },
                secondMessage
            ],
            type: 'notify'
        })

        await firstFilterEntered
        secondMessage.key.id = 'mutated-second-id'
        secondMessage.key.remoteJid = '60999999998@s.whatsapp.net'
        secondMessage.message.conversation = 'mutated second content'
        releaseFirstFilter()
        await processing

        await expect(store!.getMessage(secondJid, 'batch-snapshot-second')).resolves.toEqual(
            expect.objectContaining({
                key: expect.objectContaining({
                    id: 'batch-snapshot-second',
                    remoteJid: secondJid
                }),
                message: { conversation: 'original second content' }
            })
        )
        await expect(store!.getMessage('60999999998@s.whatsapp.net', 'mutated-second-id')).resolves.toBeNull()
    })

    it('clones circular auxiliary message properties without rejecting the batch', async () => {
        const auxiliary = Symbol('auxiliary')
        let filteredMessage: any

        await createStore({
            beforeStore: (eventType, message) => {
                if (eventType === 'messages.upsert') {
                    filteredMessage = message
                }
                return true
            }
        })

        const jid = '60111111112@s.whatsapp.net'
        const sourceMessage: any = {
            key: {
                id: 'circular-auxiliary-message',
                remoteJid: jid,
                fromMe: false
            },
            messageTimestamp: 1_700_000_000,
            message: { conversation: 'circular auxiliary data' }
        }
        sourceMessage[auxiliary] = sourceMessage

        await expect(emitter.emitAsync('messages.upsert', {
            messages: [sourceMessage],
            type: 'notify'
        })).resolves.toBeUndefined()

        expect(filteredMessage).not.toBe(sourceMessage)
        expect(filteredMessage[auxiliary]).toBe(filteredMessage)
        await expect(store!.getMessage(jid, 'circular-auxiliary-message')).resolves.toEqual(
            expect.objectContaining({
                key: expect.objectContaining({ remoteJid: jid }),
                message: { conversation: 'circular auxiliary data' }
            })
        )
    })

    it('normalizes a trusted phone remoteJidAlt even when mapping persistence fails', async () => {
        jest.spyOn(LidHandler.prototype, 'storeLidMapping').mockResolvedValue(false)
        await createStore()

        const lid = '114194640801953@lid'
        const phone = '60196953307@s.whatsapp.net'
        await emitter.emitAsync('messages.upsert', {
            messages: [{
                key: {
                    id: 'mapping-failure-message',
                    remoteJid: lid,
                    remoteJidAlt: phone,
                    addressingMode: 'lid',
                    fromMe: true
                },
                messageTimestamp: 1_700_000_000,
                message: { conversation: 'outgoing' }
            }],
            type: 'notify'
        })

        await expect(store!.getMessage(phone, 'mapping-failure-message')).resolves.toEqual(
            expect.objectContaining({
                key: expect.objectContaining({ remoteJid: phone })
            })
        )
        await expect(
            inspectionClient.db(databaseName).collection(`${collectionPrefix}messages`).countDocuments({
                jid: lid,
                'key.id': 'mapping-failure-message'
            })
        ).resolves.toBe(0)
    })

    it.each([
        ['returns false', jest.fn().mockResolvedValue(false)],
        ['throws', jest.fn().mockRejectedValue(new Error('mapping unavailable'))]
    ])('normalizes trusted legacy senderPn when persistence %s', async (_label, implementation) => {
        jest.spyOn(LidHandler.prototype, 'storeLidMapping').mockImplementation(implementation)
        await createStore()

        const lid = '114194640801955@lid'
        const phone = '60196953308:17@c.us'
        await emitter.emitAsync('messages.upsert', {
            messages: [{
                key: {
                    id: `trusted-sender-pn-${_label}`,
                    remoteJid: lid,
                    senderPn: phone,
                    fromMe: false
                },
                messageTimestamp: 1_700_000_000,
                message: { conversation: 'incoming' }
            }],
            type: 'notify'
        })

        const normalizedPhone = '60196953308@c.us'
        await expect(store!.getMessage(normalizedPhone, `trusted-sender-pn-${_label}`)).resolves.toEqual(
            expect.objectContaining({
                key: expect.objectContaining({ remoteJid: normalizedPhone })
            })
        )
    })

    it('keeps malformed non-fromMe senderPn unresolved', async () => {
        const storeMapping = jest.spyOn(LidHandler.prototype, 'storeLidMapping')
        await createStore()

        const lid = '114194640801956@lid'
        await emitter.emitAsync('messages.upsert', {
            messages: [{
                key: {
                    id: 'malformed-sender-pn',
                    remoteJid: lid,
                    senderPn: 'not-a-phone-jid',
                    fromMe: false
                },
                messageTimestamp: 1_700_000_000,
                message: { conversation: 'incoming' }
            }],
            type: 'notify'
        })

        expect(storeMapping).not.toHaveBeenCalled()
        await expect(store!.getMessage(lid, 'malformed-sender-pn')).resolves.toEqual(
            expect.objectContaining({ key: expect.objectContaining({ remoteJid: lid }) })
        )
    })

    it('normalizes a validated reverse candidate only after conflict-aware persistence succeeds', async () => {
        jest.spyOn(LidHandler.prototype, 'getPhoneNumberFromLid').mockResolvedValue(null)
        jest.spyOn(LidHandler.prototype, 'reversePhoneLookupFromMessages')
            .mockResolvedValue('60196953309:22@s.whatsapp.net')
        const storeMapping = jest.spyOn(LidHandler.prototype, 'storeLidMapping').mockResolvedValue(false)
        await createStore()

        const lid = '114194640801957@lid'
        await emitter.emitAsync('messages.upsert', {
            messages: [{
                key: {
                    id: 'reverse-conflict-message',
                    remoteJid: lid,
                    senderPn: lid,
                    fromMe: true
                },
                messageTimestamp: 1_700_000_000,
                message: { conversation: 'outgoing' }
            }],
            type: 'notify'
        })

        expect(storeMapping).toHaveBeenCalledWith(lid, '60196953309@s.whatsapp.net', undefined)
        await expect(store!.getMessage(lid, 'reverse-conflict-message')).resolves.toEqual(
            expect.objectContaining({ key: expect.objectContaining({ remoteJid: lid }) })
        )
    })

    it('rejects a malformed reverse lookup candidate', async () => {
        jest.spyOn(LidHandler.prototype, 'getPhoneNumberFromLid').mockResolvedValue(null)
        jest.spyOn(LidHandler.prototype, 'reversePhoneLookupFromMessages').mockResolvedValue('customer@example.com')
        const storeMapping = jest.spyOn(LidHandler.prototype, 'storeLidMapping')
        await createStore()

        const lid = '114194640801958@lid'
        await emitter.emitAsync('messages.upsert', {
            messages: [{
                key: {
                    id: 'malformed-reverse-message',
                    remoteJid: lid,
                    senderPn: lid,
                    fromMe: true
                },
                messageTimestamp: 1_700_000_000,
                message: { conversation: 'outgoing' }
            }],
            type: 'notify'
        })

        expect(storeMapping).not.toHaveBeenCalled()
        await expect(store!.getMessage(lid, 'malformed-reverse-message')).resolves.toBeTruthy()
    })

    it('fails closed on contradictory historical pn evidence with a LID remoteJid', async () => {
        await createStore()

        const lid = '114194640801959@lid'
        await inspectionClient.db(databaseName).collection(`${collectionPrefix}messages`).insertOne({
            instanceId: `wab259-instance-${instanceCounter}`,
            jid: lid,
            key: {
                id: 'contradictory-history-message',
                remoteJid: lid,
                remoteJidAlt: lid,
                addressingMode: 'pn',
                senderPn: '60196953310@s.whatsapp.net',
                fromMe: false
            }
        })

        const handler = new LidHandler(`wab259-instance-${instanceCounter}`, {
            enableCache: false,
            skipIndexCreation: true,
            lookupsEnabled: false
        })
        await handler.initialize(inspectionClient.db(databaseName), collectionPrefix)

        await expect(handler.reversePhoneLookupFromMessages(lid)).resolves.toBeNull()
    })

    it('redacts reverse lookup identifiers from logs', async () => {
        await createStore()

        const lid = '114194640801960@lid'
        const phone = '60196953311@s.whatsapp.net'
        await inspectionClient.db(databaseName).collection(`${collectionPrefix}messages`).insertOne({
            instanceId: `wab259-instance-${instanceCounter}`,
            jid: phone,
            key: {
                id: 'validated-history-message',
                remoteJid: phone,
                remoteJidAlt: lid,
                addressingMode: 'pn',
                fromMe: false
            }
        })

        const handler = new LidHandler(`wab259-instance-${instanceCounter}`, {
            enableCache: false,
            skipIndexCreation: true,
            lookupsEnabled: false
        })
        await handler.initialize(inspectionClient.db(databaseName), collectionPrefix)
        const log = jest.spyOn(console, 'log').mockImplementation(() => {})

        try {
            await expect(handler.reversePhoneLookupFromMessages(lid)).resolves.toBe(phone)
            const output = log.mock.calls.flat().join(' ')
            expect(output).not.toContain(lid)
            expect(output).not.toContain(phone)
        } finally {
            log.mockRestore()
        }
    })

    it('preserves Long-like values and own property descriptors in the write-path snapshot', async () => {
        let filteredMessage: any
        await createStore({
            beforeStore: (_eventType, message) => {
                filteredMessage = message
                return true
            }
        })

        const longLike = Object.create({ toString: () => '1700000000' })
        Object.defineProperty(longLike, 'low', {
            value: 1_700_000_000,
            enumerable: false,
            writable: false,
            configurable: false
        })
        const sourceMessage: any = {
            key: {
                id: 'long-descriptor-message',
                remoteJid: '60111111113@s.whatsapp.net',
                fromMe: false
            },
            messageTimestamp: longLike,
            message: { conversation: 'descriptor' }
        }

        await emitter.emitAsync('messages.upsert', { messages: [sourceMessage], type: 'notify' })

        expect(filteredMessage.messageTimestamp).not.toBe(longLike)
        expect(Object.getPrototypeOf(filteredMessage.messageTimestamp)).toBe(Object.getPrototypeOf(longLike))
        expect(Object.getOwnPropertyDescriptor(filteredMessage.messageTimestamp, 'low')).toEqual(
            Object.getOwnPropertyDescriptor(longLike, 'low')
        )
    })

    it('bounds and time-prunes unresolved LID throttle entries without full-cache scans', () => {
        const throttle = new BoundedThrottle(3, 300)

        expect(throttle.shouldRun('a', 0)).toBe(true)
        expect(throttle.shouldRun('b', 1)).toBe(true)
        expect(throttle.shouldRun('c', 2)).toBe(true)
        expect(throttle.shouldRun('d', 3)).toBe(true)
        expect(throttle.size).toBe(3)
        expect(throttle.shouldRun('a', 4)).toBe(true)
        expect(throttle.size).toBe(3)
        expect(throttle.shouldRun('a', 5)).toBe(false)
        expect(throttle.shouldRun('expired', 306)).toBe(true)
        expect(throttle.size).toBeLessThanOrEqual(3)
    })

    it('keeps malformed remoteJidAlt unresolved and records bounded redacted observability', async () => {
        const storeMapping = jest.spyOn(LidHandler.prototype, 'storeLidMapping').mockResolvedValue(true)
        const warn = jest.spyOn(console, 'warn').mockImplementation(() => {})
        await createStore()

        const lid = '114194640801954@lid'
        await emitter.emitAsync('messages.upsert', {
            messages: [{
                key: {
                    id: 'malformed-alt-message',
                    remoteJid: lid,
                    remoteJidAlt: 'not-a-phone-jid',
                    addressingMode: 'lid',
                    fromMe: true
                },
                messageTimestamp: 1_700_000_000,
                message: { conversation: 'outgoing' }
            }],
            type: 'notify'
        })

        expect(storeMapping).not.toHaveBeenCalled()
        await expect(store!.getMessage(lid, 'malformed-alt-message')).resolves.toEqual(
            expect.objectContaining({
                key: expect.objectContaining({ remoteJid: lid })
            })
        )
        expect(store!.getLidResolutionMetrics('messages.upsert.unresolved-lid')).toEqual(
            expect.objectContaining({
                totalErrors: 1
            })
        )

        const warnings = warn.mock.calls.flat().map(String)
        expect(warnings.some(message => message.includes('Unresolved top-level LID'))).toBe(true)
        expect(warnings.join(' ')).not.toContain(lid)

        await emitter.emitAsync('messages.upsert', {
            messages: [{
                key: {
                    id: 'malformed-alt-message-2',
                    remoteJid: lid,
                    remoteJidAlt: 'still-not-a-phone-jid',
                    addressingMode: 'lid',
                    fromMe: true
                },
                messageTimestamp: 1_700_000_001,
                message: { conversation: 'outgoing again' }
            }],
            type: 'notify'
        })

        expect(warn.mock.calls.flat().map(String).filter(message => message.includes('Unresolved top-level LID'))).toHaveLength(1)
    })

    it('does not invoke historical message repair from messages.upsert', async () => {
        const historicalRepair = jest.spyOn(LidHandler.prototype, 'updateExistingMessages')
            .mockResolvedValue(undefined)
        await createStore()

        await emitter.emitAsync('messages.upsert', {
            messages: [
                {
                    key: {
                        id: 'pattern-a-message',
                        remoteJid: '60111111111@s.whatsapp.net',
                        remoteJidAlt: '111111111111111@lid',
                        addressingMode: 'pn',
                        fromMe: false
                    },
                    messageTimestamp: 1_700_000_000,
                    message: { conversation: 'incoming' }
                },
                {
                    key: {
                        id: 'pattern-b-message',
                        remoteJid: '222222222222222@lid',
                        remoteJidAlt: '60222222222@s.whatsapp.net',
                        addressingMode: 'lid',
                        fromMe: true
                    },
                    messageTimestamp: 1_700_000_001,
                    message: { conversation: 'outgoing' }
                }
            ],
            type: 'notify'
        })

        expect(historicalRepair).not.toHaveBeenCalled()
    })
})
