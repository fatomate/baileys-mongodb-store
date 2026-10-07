// Shared real-Mongo harness for WAB-654 store tests.
// Reads go through a separate raw client so assertions observe exactly what MongoDB stored,
// including the BSON type, without passing through the store under test.
import { MongoMemoryServer } from 'mongodb-memory-server'
import { Document, MongoClient } from 'mongodb'

export const PREFIX = 'wabot_'
export const DB = 'wab654'
export const INSTANCE = 'inst-wab654'
export const JID = '60123456789@s.whatsapp.net'
export const S = 1759100000

export type Harness = {
    uri: string
    raw: MongoClient
    stop: () => Promise<void>
}

export const startMongo = async (): Promise<Harness> => {
    const server = await MongoMemoryServer.create()
    const raw = await MongoClient.connect(server.getUri())
    return {
        uri: server.getUri(),
        raw,
        stop: async () => {
            await raw.close()
            await server.stop()
        },
    }
}

export const messages = (h: Harness) => h.raw.db(DB).collection(`${PREFIX}messages`)

/** The stored document for a key id, with the BSON type of messageTimestamp alongside its value. */
export const storedTimestamp = async (h: Harness, keyId: string): Promise<{ type: string; value: unknown; doc: Document } | null> => {
    const [row] = await messages(h).aggregate([
        { $match: { instanceId: INSTANCE, 'key.id': keyId } },
        { $addFields: { __type: { $type: '$messageTimestamp' } } },
        { $limit: 1 },
    ]).toArray()
    return row ? { type: row.__type, value: row.messageTimestamp, doc: row } : null
}

/** Poll until the store has written the message (history sync and queues write asynchronously). */
export const waitForStored = async (h: Harness, keyId: string, timeoutMs = 5000) => {
    const deadline = Date.now() + timeoutMs
    for (;;) {
        const row = await storedTimestamp(h, keyId)
        if (row) return row
        if (Date.now() > deadline) throw new Error(`message ${keyId} was not stored within ${timeoutMs} ms`)
        await new Promise((r) => setTimeout(r, 50))
    }
}

/** Seed a row directly, bypassing the store, to model rows already in production. */
export const seedMessage = async (h: Harness, keyId: string, messageTimestamp: unknown) => {
    await messages(h).insertOne({
        instanceId: INSTANCE,
        jid: JID,
        key: { id: keyId, remoteJid: JID, fromMe: false },
        message: { conversation: 'seeded' },
        messageTimestamp,
    })
}

export const nowSeconds = () => Math.floor(Date.now() / 1000)

export const baseStoreConfig = (uri: string) => ({
    uri,
    database: DB,
    instanceId: INSTANCE,
    collectionPrefix: PREFIX,
    useSharedConnections: false,
    debounceHistoryEvents: false,
    logLevel: 'none' as const,
})
