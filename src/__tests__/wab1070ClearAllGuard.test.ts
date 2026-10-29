// WAB-1070: clearAll must not delete the messages of an instance while it keeps its chats.
// That left chats with counted unread and no messages in the backend. Real mongod.
/* eslint-disable @typescript-eslint/no-var-requires */
jest.mock('baileys', () => require('./helpers/baileysDouble').baileysMock)
jest.mock('baileys/lib/Types/LabelAssociation', () => ({ LabelAssociationType: { Chat: 'label_jid', Message: 'label_message' } }), { virtual: true })

import { DB, Harness, INSTANCE, JID, PREFIX, baseStoreConfig, messages, seedMessage, startMongo } from './helpers/storeHarness'
import { makeEnhancedMongoDBStore } from '../makeEnhancedMongoDBStore'

jest.setTimeout(60000)

let h: Harness
let store: any

const chats = () => h.raw.db(DB).collection(`${PREFIX}chats`)
const contacts = () => h.raw.db(DB).collection(`${PREFIX}contacts`)

beforeAll(async () => {
    h = await startMongo()
    store = await makeEnhancedMongoDBStore({ ...baseStoreConfig(h.uri), lidHandler: { enableCache: false } } as any)
})
afterAll(async () => {
    await store?.close()
    await h.stop()
})
beforeEach(async () => {
    await messages(h).deleteMany({})
    await chats().deleteMany({})
    await contacts().deleteMany({})
    await seedMessage(h, 'm1', 1759100000)
    await chats().insertOne({ instanceId: INSTANCE, id: JID, unreadCount: 2 })
    await contacts().insertOne({ instanceId: INSTANCE, id: JID })
})

it('refuses to delete messages while it keeps the chats', async () => {
    await expect(store.clearAll({ preserve: ['chats'] })).rejects.toThrow(/WAB-1070/)
    expect(await messages(h).countDocuments({ instanceId: INSTANCE })).toBe(1)
    expect(await chats().countDocuments({ instanceId: INSTANCE })).toBe(1)
    expect(await contacts().countDocuments({ instanceId: INSTANCE })).toBe(1)
})

it('still deletes chats and messages together when it keeps the contacts', async () => {
    await store.clearAll({ preserve: ['contacts'] })
    expect(await messages(h).countDocuments({ instanceId: INSTANCE })).toBe(0)
    expect(await chats().countDocuments({ instanceId: INSTANCE })).toBe(0)
    expect(await contacts().countDocuments({ instanceId: INSTANCE })).toBe(1)
})
