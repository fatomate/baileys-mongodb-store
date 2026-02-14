import { MongoClient, Db } from 'mongodb'
import { MongoMemoryServer } from 'mongodb-memory-server'
import { LidHandler } from '../lidHandler.js'
import { proto } from 'baileys'

describe('LidHandler', () => {
    let mongoServer: MongoMemoryServer
    let client: MongoClient
    let db: Db
    let lidHandler: LidHandler

    beforeAll(async () => {
        // Start in-memory MongoDB
        mongoServer = await MongoMemoryServer.create()
        const uri = mongoServer.getUri()
        
        // Connect to MongoDB
        client = new MongoClient(uri)
        await client.connect()
        db = client.db('test')
    })

    afterAll(async () => {
        await client.close()
        await mongoServer.stop()
    })

    beforeEach(async () => {
        // Clear any existing data first
        await db.collection('test_lidMappings').deleteMany({})
        await db.collection('test_messages').deleteMany({})
        
        // Create new LID handler for each test
        lidHandler = new LidHandler('test-instance', {
            cacheTTL: 60,
            enableCache: true
        })
        await lidHandler.initialize(db, 'test_')
    })

    describe('isLidFormat', () => {
        it('should identify @lid format correctly', () => {
            expect(lidHandler.isLidFormat('114194640801953@lid')).toBe(true)
            expect(lidHandler.isLidFormat('60196953307@s.whatsapp.net')).toBe(false)
            expect(lidHandler.isLidFormat(null)).toBe(false)
            expect(lidHandler.isLidFormat(undefined)).toBe(false)
            expect(lidHandler.isLidFormat('')).toBe(false)
        })
    })

    describe('extractLidInfo', () => {
        it('should extract LID and phone number from message with senderPn', () => {
            const message: proto.IWebMessageInfo = {
                key: {
                    remoteJid: '114194640801953@lid',
                    senderPn: '60196953307@s.whatsapp.net'
                } as any,
                messageTimestamp: 1755232223
            }

            const info = lidHandler.extractLidInfo(message)
            expect(info.lid).toBe('114194640801953@lid')
            expect(info.phoneNumber).toBe('60196953307@s.whatsapp.net')
        })

        it('should extract phone number when remoteJid is not @lid', () => {
            const message: proto.IWebMessageInfo = {
                key: {
                    remoteJid: '60196953307@s.whatsapp.net',
                    fromMe: true
                },
                messageTimestamp: 1755232223
            }

            const info = lidHandler.extractLidInfo(message)
            expect(info.lid).toBeUndefined()
            expect(info.phoneNumber).toBe('60196953307@s.whatsapp.net')
        })

        it('should extract senderLid when present', () => {
            const message: proto.IWebMessageInfo = {
                key: {
                    remoteJid: '60196953307@s.whatsapp.net',
                    senderLid: '114194640801953@lid'
                } as any,
                messageTimestamp: 1755232223
            }

            const info = lidHandler.extractLidInfo(message)
            expect(info.lid).toBe('114194640801953@lid')
            expect(info.phoneNumber).toBe('60196953307@s.whatsapp.net')
        })
    })

    describe('storeLidMapping', () => {
        it('should store LID to phone number mapping', async () => {
            const lid = '114194640801953@lid'
            const phoneNumber = '60196953307@s.whatsapp.net'

            await lidHandler.storeLidMapping(lid, phoneNumber)

            // Check if mapping was stored in database
            const mapping = await db.collection('test_lidMappings').findOne({
                instanceId: 'test-instance',
                lid
            })

            expect(mapping).toBeTruthy()
            expect(mapping?.phoneNumber).toBe(phoneNumber)
            expect(mapping?.firstSeen).toBeInstanceOf(Date)
        })

        it('should not store invalid mappings', async () => {
            // Try to store non-LID format
            await lidHandler.storeLidMapping('60196953307@s.whatsapp.net', '60196953307@s.whatsapp.net')

            const count = await db.collection('test_lidMappings').countDocuments()
            expect(count).toBe(0)
        })

    })

    describe('getPhoneNumberFromLid', () => {
        it('should retrieve phone number from LID', async () => {
            const lid = '114194640801953@lid'
            const phoneNumber = '60196953307@s.whatsapp.net'

            // Store mapping first
            await lidHandler.storeLidMapping(lid, phoneNumber)

            // Retrieve it
            const retrieved = await lidHandler.getPhoneNumberFromLid(lid)
            expect(retrieved).toBe(phoneNumber)
        })

        it('should return null for unknown LID', async () => {
            const retrieved = await lidHandler.getPhoneNumberFromLid('unknown@lid')
            expect(retrieved).toBeNull()
        })

        it('should return input if not LID format', async () => {
            const phoneNumber = '60196953307@s.whatsapp.net'
            const retrieved = await lidHandler.getPhoneNumberFromLid(phoneNumber)
            expect(retrieved).toBe(phoneNumber)
        })

        it('should use cache when available', async () => {
            const lid = '114194640801953@lid'
            const phoneNumber = '60196953307@s.whatsapp.net'

            // Store mapping
            await lidHandler.storeLidMapping(lid, phoneNumber)

            // First retrieval (from DB)
            const retrieved1 = await lidHandler.getPhoneNumberFromLid(lid)
            expect(retrieved1).toBe(phoneNumber)

            // Delete from DB to test cache
            await db.collection('test_lidMappings').deleteMany({})

            // Second retrieval should still work (from cache)
            const retrieved2 = await lidHandler.getPhoneNumberFromLid(lid)
            expect(retrieved2).toBe(phoneNumber)
        })
    })

    describe('getLidFromPhoneNumber', () => {
        it('should retrieve LID from phone number', async () => {
            const lid = '114194640801953@lid'
            const phoneNumber = '60196953307@s.whatsapp.net'

            // Store mapping first
            await lidHandler.storeLidMapping(lid, phoneNumber)

            // Retrieve it
            const retrieved = await lidHandler.getLidFromPhoneNumber(phoneNumber)
            expect(retrieved).toBe(lid)
        })

        it('should return null for unknown phone number', async () => {
            const retrieved = await lidHandler.getLidFromPhoneNumber('unknown@s.whatsapp.net')
            expect(retrieved).toBeNull()
        })
    })

    describe('normalizeJid', () => {
        it('should normalize LID to phone number', async () => {
            const lid = '114194640801953@lid'
            const phoneNumber = '60196953307@s.whatsapp.net'

            // Store mapping
            await lidHandler.storeLidMapping(lid, phoneNumber)

            // Normalize
            const normalized = await lidHandler.normalizeJid(lid)
            expect(normalized).toBe(phoneNumber)
        })

        it('should return original JID if no mapping found', async () => {
            const lid = '999999999@lid' // Use a different LID that won't conflict
            const normalized = await lidHandler.normalizeJid(lid)
            expect(normalized).toBe(lid)
        })

        it('should return phone number as-is', async () => {
            const phoneNumber = '60196953307@s.whatsapp.net'
            const normalized = await lidHandler.normalizeJid(phoneNumber)
            expect(normalized).toBe(phoneNumber)
        })

        it('should handle null/undefined', async () => {
            expect(await lidHandler.normalizeJid(null)).toBeNull()
            expect(await lidHandler.normalizeJid(undefined)).toBeNull()
        })
    })

    describe('processMessage', () => {
        it('should process message with LID and phone number', async () => {
            const message: proto.IWebMessageInfo = {
                key: {
                    remoteJid: '114194640801953@lid',
                    senderPn: '60196953307@s.whatsapp.net'
                } as any,
                messageTimestamp: 1755232223
            }

            const result = await lidHandler.processMessage(message)
            
            expect(result.normalizedJid).toBe('60196953307@s.whatsapp.net')
            expect(result.lidInfo.lid).toBe('114194640801953@lid')
            expect(result.lidInfo.phoneNumber).toBe('60196953307@s.whatsapp.net')
            expect(result.lidInfo.mappingStored).toBe(true)

            // Verify mapping was stored
            const mapping = await db.collection('test_lidMappings').findOne({
                instanceId: 'test-instance',
                lid: '114194640801953@lid'
            })
            expect(mapping?.phoneNumber).toBe('60196953307@s.whatsapp.net')
        })

        it('should process message with only LID', async () => {
            const lid = '114194640801953@lid'
            const phoneNumber = '60196953307@s.whatsapp.net'

            // Pre-store mapping
            await lidHandler.storeLidMapping(lid, phoneNumber)

            const message: proto.IWebMessageInfo = {
                key: {
                    remoteJid: lid
                },
                messageTimestamp: 1755232223
            }

            const result = await lidHandler.processMessage(message)
            
            expect(result.normalizedJid).toBe(phoneNumber)
            expect(result.lidInfo.lid).toBe(lid)
            expect(result.lidInfo.phoneNumber).toBeUndefined()
            expect(result.lidInfo.mappingStored).toBe(false)
        })

        it('should process message with only phone number', async () => {
            const message: proto.IWebMessageInfo = {
                key: {
                    remoteJid: '60196953307@s.whatsapp.net'
                },
                messageTimestamp: 1755232223
            }

            const result = await lidHandler.processMessage(message)
            
            expect(result.normalizedJid).toBe('60196953307@s.whatsapp.net')
            expect(result.lidInfo.lid).toBeUndefined()
            expect(result.lidInfo.phoneNumber).toBe('60196953307@s.whatsapp.net')
            expect(result.lidInfo.mappingStored).toBe(false)
        })

        it('should handle fromMe message with LID using reverse lookup', async () => {
            // First, insert a received message with senderLid
            const insertResult = await db.collection('test_messages').insertOne({
                instanceId: 'test-instance',
                key: {
                    id: 'msg1',
                    fromMe: false,
                    remoteJid: '60196953307@s.whatsapp.net',
                    senderLid: '114194640801953@lid',
                    senderPn: '60196953307@s.whatsapp.net'
                }
            })
            
            // Verify the message was inserted
            expect(insertResult.acknowledged).toBe(true)

            // Now process a sent message with LID remoteJid
            const sentMessage: proto.IWebMessageInfo = {
                key: {
                    remoteJid: '114194640801953@lid',
                    fromMe: true,
                    id: 'msg2'
                } as any,
                messageTimestamp: 1755232223
            }

            const result = await lidHandler.processMessage(sentMessage)
            
            expect(result.normalizedJid).toBe('60196953307@s.whatsapp.net')
            expect(result.lidInfo.lid).toBe('114194640801953@lid')
            expect(result.lidInfo.phoneNumber).toBe('60196953307@s.whatsapp.net')
            expect(result.lidInfo.needsReverseLookup).toBe(true)

            // Verify mapping was discovered and stored
            const mapping = await db.collection('test_lidMappings').findOne({
                instanceId: 'test-instance',
                lid: '114194640801953@lid'
            })
            expect(mapping?.phoneNumber).toBe('60196953307@s.whatsapp.net')
        })

        it('should handle fromMe message when no reverse lookup match found', async () => {
            const sentMessage: proto.IWebMessageInfo = {
                key: {
                    remoteJid: '999999999@lid',
                    fromMe: true,
                    id: 'msg3'
                } as any,
                messageTimestamp: 1755232223
            }

            const result = await lidHandler.processMessage(sentMessage)
            
            // Should return the LID since no phone number was found
            expect(result.normalizedJid).toBe('999999999@lid')
            expect(result.lidInfo.lid).toBe('999999999@lid')
            expect(result.lidInfo.phoneNumber).toBeUndefined()
            expect(result.lidInfo.needsReverseLookup).toBe(true)
        })

        it('should handle fromMe message with LID in both senderLid and senderPn (WhatsApp bug)', async () => {
            // First, insert a received message with correct senderLid
            await db.collection('test_messages').insertOne({
                instanceId: 'test-instance',
                key: {
                    id: 'msg1',
                    fromMe: false,
                    remoteJid: '60196953307@s.whatsapp.net',
                    senderLid: '114194640801953@lid',
                    senderPn: '60196953307@s.whatsapp.net'
                }
            })

            // Now process a sent message where both senderLid and senderPn incorrectly have LID
            const sentMessage: proto.IWebMessageInfo = {
                key: {
                    remoteJid: '114194640801953@lid',
                    fromMe: true,
                    id: 'msg2',
                    senderLid: '114194640801953@lid',
                    senderPn: '114194640801953@lid'  // This is the bug - should be phone number
                } as any,
                messageTimestamp: 1755232223
            }

            const result = await lidHandler.processMessage(sentMessage)
            
            // Should correctly identify the phone number through reverse lookup
            expect(result.normalizedJid).toBe('60196953307@s.whatsapp.net')
            expect(result.lidInfo.lid).toBe('114194640801953@lid')
            expect(result.lidInfo.phoneNumber).toBe('60196953307@s.whatsapp.net')
            expect(result.lidInfo.needsReverseLookup).toBe(true)

            // Verify the correct mapping was stored (not LID->LID)
            const mapping = await db.collection('test_lidMappings').findOne({
                instanceId: 'test-instance',
                lid: '114194640801953@lid'
            })
            expect(mapping?.phoneNumber).toBe('60196953307@s.whatsapp.net')
            expect(mapping?.phoneNumber).not.toBe('114194640801953@lid')
        })

        it('should not store invalid LID->LID mappings', async () => {
            // Try to process a message with LID as both lid and phoneNumber
            const invalidMessage: proto.IWebMessageInfo = {
                key: {
                    remoteJid: '114194640801953@lid',
                    senderLid: '114194640801953@lid',
                    senderPn: '114194640801953@lid'
                } as any,
                messageTimestamp: 1755232223
            }

            const result = await lidHandler.processMessage(invalidMessage)
            
            // Should not store any mapping
            const mappingCount = await db.collection('test_lidMappings').countDocuments({
                instanceId: 'test-instance'
            })
            expect(mappingCount).toBe(0)
            
            // Should still return the LID as normalizedJid
            expect(result.normalizedJid).toBe('114194640801953@lid')
        })
    })

    describe('extractLidInfo - new format (remoteJidAlt + addressingMode)', () => {
        it('should extract LID and phone from incoming message (addressingMode=pn)', () => {
            // Incoming message: customer to bot
            // remoteJid is phone number, remoteJidAlt is LID
            const message: proto.IWebMessageInfo = {
                key: {
                    remoteJid: '60196953307@s.whatsapp.net',
                    remoteJidAlt: '114194640801953@lid',
                    fromMe: false,
                    id: '3EB0927E1937A24BEBE5B6',
                    participant: '',
                    addressingMode: 'pn'
                } as any,
                messageTimestamp: 1765263024,
                pushName: 'Test User'
            }

            const info = lidHandler.extractLidInfo(message)
            expect(info.lid).toBe('114194640801953@lid')
            expect(info.phoneNumber).toBe('60196953307@s.whatsapp.net')
            expect(info.addressingMode).toBe('pn')
            expect(info.needsReverseLookup).toBeFalsy()
        })

        it('should extract LID and phone from outgoing message (addressingMode=lid)', () => {
            // Outgoing message: bot to customer (sent from phone)
            // remoteJid is LID, remoteJidAlt is phone number
            const message: proto.IWebMessageInfo = {
                key: {
                    remoteJid: '114194640801953@lid',
                    remoteJidAlt: '60196953307@s.whatsapp.net',
                    fromMe: true,
                    id: 'A5A396AC133134DCDF3FE694E3978A78',
                    participant: '',
                    addressingMode: 'lid'
                } as any,
                messageTimestamp: 1765263051,
                pushName: 'Bot Name',
                status: 2
            }

            const info = lidHandler.extractLidInfo(message)
            expect(info.lid).toBe('114194640801953@lid')
            expect(info.phoneNumber).toBe('60196953307@s.whatsapp.net')
            expect(info.addressingMode).toBe('lid')
            // Should NOT need reverse lookup since phone is provided in remoteJidAlt
            expect(info.needsReverseLookup).toBeFalsy()
        })

        it('should handle new format with missing remoteJidAlt', () => {
            // Edge case: addressingMode present but remoteJidAlt missing
            const message: proto.IWebMessageInfo = {
                key: {
                    remoteJid: '60196953307@s.whatsapp.net',
                    fromMe: false,
                    id: 'test-id',
                    addressingMode: 'pn'
                } as any,
                messageTimestamp: 1765263024
            }

            const info = lidHandler.extractLidInfo(message)
            // Should fall back to legacy behavior
            expect(info.phoneNumber).toBe('60196953307@s.whatsapp.net')
            expect(info.lid).toBeUndefined()
        })

        it('should prefer new format over legacy format when both present', () => {
            // Mixed message with both new and legacy fields
            const message: proto.IWebMessageInfo = {
                key: {
                    remoteJid: '60196953307@s.whatsapp.net',
                    remoteJidAlt: '114194640801953@lid',
                    senderLid: '999999999@lid', // Different LID from legacy field
                    senderPn: '60111111111@s.whatsapp.net', // Different phone from legacy field
                    fromMe: false,
                    id: 'test-id',
                    addressingMode: 'pn'
                } as any,
                messageTimestamp: 1765263024
            }

            const info = lidHandler.extractLidInfo(message)
            // Should use new format values
            expect(info.lid).toBe('114194640801953@lid')
            expect(info.phoneNumber).toBe('60196953307@s.whatsapp.net')
            expect(info.addressingMode).toBe('pn')
        })
    })

    describe('processMessage - new format', () => {
        it('should process incoming message with new format (addressingMode=pn)', async () => {
            const message: proto.IWebMessageInfo = {
                key: {
                    remoteJid: '60196953307@s.whatsapp.net',
                    remoteJidAlt: '114194640801953@lid',
                    fromMe: false,
                    id: 'test-incoming',
                    addressingMode: 'pn'
                } as any,
                messageTimestamp: 1765263024,
                pushName: 'Test User'
            }

            const result = await lidHandler.processMessage(message)
            
            expect(result.normalizedJid).toBe('60196953307@s.whatsapp.net')
            expect(result.lidInfo.lid).toBe('114194640801953@lid')
            expect(result.lidInfo.phoneNumber).toBe('60196953307@s.whatsapp.net')
            expect(result.lidInfo.addressingMode).toBe('pn')
        })

        it('should process outgoing message with new format (addressingMode=lid)', async () => {
            const message: proto.IWebMessageInfo = {
                key: {
                    remoteJid: '114194640801953@lid',
                    remoteJidAlt: '60196953307@s.whatsapp.net',
                    fromMe: true,
                    id: 'test-outgoing',
                    addressingMode: 'lid'
                } as any,
                messageTimestamp: 1765263051,
                status: 2
            }

            const result = await lidHandler.processMessage(message)
            
            expect(result.normalizedJid).toBe('60196953307@s.whatsapp.net')
            expect(result.lidInfo.lid).toBe('114194640801953@lid')
            expect(result.lidInfo.phoneNumber).toBe('60196953307@s.whatsapp.net')
            expect(result.lidInfo.addressingMode).toBe('lid')
        })

        it('should maintain backward compatibility with legacy format', async () => {
            // Legacy format message (no addressingMode/remoteJidAlt)
            const message: proto.IWebMessageInfo = {
                key: {
                    remoteJid: '114194640801953@lid',
                    senderPn: '60196953307@s.whatsapp.net'
                } as any,
                messageTimestamp: 1755232223
            }

            const result = await lidHandler.processMessage(message)
            
            expect(result.normalizedJid).toBe('60196953307@s.whatsapp.net')
            expect(result.lidInfo.lid).toBe('114194640801953@lid')
            expect(result.lidInfo.phoneNumber).toBe('60196953307@s.whatsapp.net')
            // Legacy messages won't have addressingMode
            expect(result.lidInfo.addressingMode).toBeUndefined()
        })
    })

    describe('clearCache', () => {
        it('should clear cache for instance', async () => {
            const lid = '114194640801953@lid'
            const phoneNumber = '60196953307@s.whatsapp.net'

            // Store mapping (will be cached)
            await lidHandler.storeLidMapping(lid, phoneNumber)

            // Clear cache
            lidHandler.clearCache()

            // Delete from DB
            await db.collection('test_lidMappings').deleteMany({})

            // Should not find in cache anymore
            const retrieved = await lidHandler.getPhoneNumberFromLid(lid)
            expect(retrieved).toBeNull()
        })
    })

    describe('getAllMappings', () => {
        it('should retrieve all mappings for instance', async () => {
            // Store multiple mappings
            await lidHandler.storeLidMapping('111@lid', '601111@s.whatsapp.net')
            await lidHandler.storeLidMapping('222@lid', '602222@s.whatsapp.net')
            await lidHandler.storeLidMapping('333@lid', '603333@s.whatsapp.net')

            const mappings = await lidHandler.getAllMappings()
            
            expect(mappings).toHaveLength(3)
            expect(mappings.map(m => m.lid).sort()).toEqual(['111@lid', '222@lid', '333@lid'])
        })
    })
})

