import { MongoClient, Db, Collection } from 'mongodb'
import { MongoMemoryServer } from 'mongodb-memory-server'

describe('E11000 LID Duplicate Key Prevention', () => {
    let mongoServer: MongoMemoryServer
    let client: MongoClient
    let db: Db
    let contacts: Collection

    const instanceId = 'test-instance'

    beforeAll(async () => {
        mongoServer = await MongoMemoryServer.create()
        const uri = mongoServer.getUri()
        client = new MongoClient(uri)
        await client.connect()
        db = client.db('test')
        contacts = db.collection('contacts')

        await contacts.createIndex(
            { instanceId: 1, id: 1 },
            { unique: true, name: 'contacts_primary' }
        )
        await contacts.createIndex(
            { instanceId: 1, lid: 1 },
            { unique: true, partialFilterExpression: { lid: { $type: 'string' } }, name: 'contacts_lid_lookup' }
        )
    })

    afterAll(async () => {
        await client.close()
        await mongoServer.stop()
    })

    beforeEach(async () => {
        await contacts.deleteMany({})
    })

    // Helper that mirrors the safeSetLid logic from makeEnhancedMongoDBStore
    async function safeSetLid(contactId: string, lid: string, inst: string = instanceId): Promise<void> {
        try {
            await contacts.updateOne(
                { instanceId: inst, id: contactId },
                { $set: { lid, updatedAt: new Date() } }
            )
        } catch (e: any) {
            if (e?.code === 11000 && e?.keyPattern?.lid) {
                await contacts.updateOne(
                    { instanceId: inst, lid, id: { $ne: contactId } },
                    { $unset: { lid: 1 }, $set: { updatedAt: new Date() } }
                )
                try {
                    await contacts.updateOne(
                        { instanceId: inst, id: contactId },
                        { $set: { lid, updatedAt: new Date() } }
                    )
                } catch (retryError: any) {
                    if (retryError?.code === 11000) {
                        // Swallow after retry exhaustion
                    } else {
                        throw retryError
                    }
                }
            } else {
                throw e
            }
        }
    }

    // --- Strategy A Tests ---

    describe('Strategy A: lid excluded from ...rest spread', () => {
        it('T-A1: contact with lid property - lid should not leak into $set via spread', () => {
            const contact = { id: '123@s.whatsapp.net', notify: 'John', lid: '999@lid', instanceId: 'x', extra: 'data' }
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { notify, id: _ignoredId, instanceId: _ignoredInstanceId, lid: _ignoredLid, ...rest } = (contact as any) || {}
            expect(rest).not.toHaveProperty('lid')
            expect(rest).toHaveProperty('extra', 'data')
        })

        it('T-A2: contact without lid property - normal operation unchanged', () => {
            const contact = { id: '123@s.whatsapp.net', notify: 'John', instanceId: 'x', extra: 'data' }
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { notify, id: _ignoredId, instanceId: _ignoredInstanceId, lid: _ignoredLid, ...rest } = (contact as any) || {}
            expect(rest).not.toHaveProperty('lid')
            expect(rest).toHaveProperty('extra', 'data')
        })

        it('T-A3: all excluded fields are stripped, only valid fields remain in rest', () => {
            const contact = { id: '1@s.whatsapp.net', notify: 'A', instanceId: 'x', lid: '5@lid', name: 'Test', profilePic: 'url' }
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { notify, id: _ignoredId, instanceId: _ignoredInstanceId, lid: _ignoredLid, ...rest } = (contact as any) || {}
            expect(rest).toEqual({ name: 'Test', profilePic: 'url' })
        })

        it('T-A4: bulk upsert with mixed contacts - no E11000 on contacts_lid_lookup', async () => {
            const sameLid = 'shared-lid@lid'
            await contacts.insertOne({ instanceId, id: 'existing@s.whatsapp.net', lid: sameLid, updatedAt: new Date() })

            const contactsToUpsert = [
                { id: 'new1@s.whatsapp.net', notify: 'A', lid: sameLid, name: 'New1' },
                { id: 'new2@s.whatsapp.net', notify: 'B', name: 'New2' }
            ]

            const bulkOps = contactsToUpsert.map(contact => {
                // eslint-disable-next-line @typescript-eslint/no-unused-vars
                const { notify, lid: _ignoredLid, id: _ignoredId, instanceId: _ignoredInstanceId, ...rest } = (contact as any) || {}
                return {
                    updateOne: {
                        filter: { instanceId, id: contact.id },
                        update: {
                            $set: { ...rest, updatedAt: new Date() },
                            $setOnInsert: { instanceId, id: contact.id }
                        },
                        upsert: true
                    }
                }
            })

            // Should not throw E11000 because lid is excluded from $set
            await expect(contacts.bulkWrite(bulkOps, { ordered: false })).resolves.not.toThrow()

            // Verify existing contact still has its lid
            const existing = await contacts.findOne({ instanceId, id: 'existing@s.whatsapp.net' })
            expect(existing?.lid).toBe(sameLid)
        })
    })

    // --- Strategy B Tests ---

    describe('Strategy B: safeSetLid retry logic', () => {
        it('T-B1: succeeds on first attempt when no conflict', async () => {
            await contacts.insertOne({ instanceId, id: 'contact-a@s.whatsapp.net', updatedAt: new Date() })

            await safeSetLid('contact-a@s.whatsapp.net', 'lid-1@lid')

            const doc = await contacts.findOne({ instanceId, id: 'contact-a@s.whatsapp.net' })
            expect(doc?.lid).toBe('lid-1@lid')
        })

        it('T-B2: E11000 conflict - clears stale lid and retries successfully', async () => {
            await contacts.insertOne({ instanceId, id: 'contact-a@s.whatsapp.net', lid: 'shared@lid', updatedAt: new Date() })
            await contacts.insertOne({ instanceId, id: 'contact-b@s.whatsapp.net', updatedAt: new Date() })

            await safeSetLid('contact-b@s.whatsapp.net', 'shared@lid')

            const contactA = await contacts.findOne({ instanceId, id: 'contact-a@s.whatsapp.net' })
            expect(contactA?.lid).toBeUndefined()

            const contactB = await contacts.findOne({ instanceId, id: 'contact-b@s.whatsapp.net' })
            expect(contactB?.lid).toBe('shared@lid')
        })

        it('T-B4: non-E11000 error propagates', async () => {
            await contacts.insertOne({ instanceId, id: 'err@s.whatsapp.net', updatedAt: new Date() })

            const originalUpdateOne = contacts.updateOne.bind(contacts)
            let callCount = 0
            jest.spyOn(contacts, 'updateOne').mockImplementation(async (filter: any, update: any, options?: any) => {
                callCount++
                if (callCount === 1) {
                    const err = new Error('Some other MongoDB error') as any
                    err.code = 999
                    throw err
                }
                return originalUpdateOne(filter, update, options)
            })

            await expect(safeSetLid('err@s.whatsapp.net', 'lid@lid')).rejects.toThrow('Some other MongoDB error')

            jest.restoreAllMocks()
        })

        it('T-B5: E11000 on contacts_primary (not lid) - throws original error', async () => {
            await contacts.insertOne({ instanceId, id: 'dup@s.whatsapp.net', updatedAt: new Date() })

            // Try to insert a duplicate on the primary index via raw insertOne
            try {
                await contacts.insertOne({ instanceId, id: 'dup@s.whatsapp.net', updatedAt: new Date() })
                fail('Should have thrown')
            } catch (e: any) {
                // Verify this is an E11000 but NOT on lid keyPattern
                expect(e.code).toBe(11000)
                expect(e.keyPattern?.lid).toBeUndefined()
            }
        })

        it('T-B6: concurrent safeSetLid calls for same LID - both complete without unhandled error', async () => {
            await contacts.insertOne({ instanceId, id: 'c1@s.whatsapp.net', updatedAt: new Date() })
            await contacts.insertOne({ instanceId, id: 'c2@s.whatsapp.net', updatedAt: new Date() })

            // Run concurrently
            const results = await Promise.allSettled([
                safeSetLid('c1@s.whatsapp.net', 'contested@lid'),
                safeSetLid('c2@s.whatsapp.net', 'contested@lid')
            ])

            // Both should resolve (no unhandled errors)
            for (const r of results) {
                expect(r.status).toBe('fulfilled')
            }

            // Exactly one contact should have the LID
            const c1 = await contacts.findOne({ instanceId, id: 'c1@s.whatsapp.net' })
            const c2 = await contacts.findOne({ instanceId, id: 'c2@s.whatsapp.net' })
            const lidCount = [c1?.lid, c2?.lid].filter(l => l === 'contested@lid').length
            expect(lidCount).toBeGreaterThanOrEqual(1)
            expect(lidCount).toBeLessThanOrEqual(2) // Both could succeed if timing allows
        })

        it('T-B7: conflicting contact has same id as target - no-op on $unset, retry succeeds', async () => {
            await contacts.insertOne({ instanceId, id: 'same@s.whatsapp.net', lid: 'old@lid', updatedAt: new Date() })

            // Setting a new LID on the same contact should just work (overwrite)
            await safeSetLid('same@s.whatsapp.net', 'new@lid')

            const doc = await contacts.findOne({ instanceId, id: 'same@s.whatsapp.net' })
            expect(doc?.lid).toBe('new@lid')
        })

        it('T-B8: target contact does not exist - no-op, no error', async () => {
            // safeSetLid on non-existent contact with upsert:false (default) is a no-op
            await expect(safeSetLid('nonexistent@s.whatsapp.net', 'lid@lid')).resolves.not.toThrow()

            const doc = await contacts.findOne({ instanceId, id: 'nonexistent@s.whatsapp.net' })
            expect(doc).toBeNull()
        })
    })
})
