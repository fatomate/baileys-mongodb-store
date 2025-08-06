"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.cleanupMongoDBStore = exports.makeMongoDBStore = void 0;
const mongodb_1 = require("mongodb");
const baileys_1 = require("baileys");
const node_cache_1 = __importDefault(require("node-cache"));
const p_queue_1 = __importDefault(require("p-queue"));
const DEFAULT_TTL_DAYS = 30;
let activeConnections = [];
const binaryConversionCache = new node_cache_1.default({ stdTTL: 300, checkperiod: 60 });
const QUEUE_CONCURRENCY = 50;
const BATCH_SIZE = 100;
const BATCH_DELAY = 50;
const performanceMetrics = {
    messagesProcessed: 0,
    labelsProcessed: 0,
    batchesProcessed: 0,
    errors: 0,
    lastResetTime: new Date()
};
const convertBinaryToBuffer = (obj) => {
    try {
        if (!obj || typeof obj !== 'object')
            return obj;
        if (obj.buffer && obj._bsontype === 'Binary') {
            return Buffer.from(obj.buffer);
        }
        if (Array.isArray(obj)) {
            return obj.map(item => convertBinaryToBuffer(item));
        }
        const result = {};
        for (const key in obj) {
            if (Object.prototype.hasOwnProperty.call(obj, key)) {
                result[key] = convertBinaryToBuffer(obj[key]);
            }
        }
        return result;
    }
    catch (error) {
        console.error('Error converting Binary to Buffer:', error);
        return obj;
    }
};
const makeMongoDBStore = async (config) => {
    const { uri, database: dbName, instanceId, ttlDays = DEFAULT_TTL_DAYS, collectionPrefix = 'baileys_' } = config;
    const client = new mongodb_1.MongoClient(uri, {
        maxPoolSize: 100,
        minPoolSize: 10,
        maxIdleTimeMS: 30000,
        writeConcern: { w: 1, j: false }
    });
    await client.connect();
    activeConnections.push({
        client,
        database: dbName,
        instanceId,
        collectionPrefix
    });
    const db = client.db(dbName);
    const messageQueue = new p_queue_1.default({ concurrency: QUEUE_CONCURRENCY });
    const labelQueue = new p_queue_1.default({ concurrency: QUEUE_CONCURRENCY });
    const generalQueue = new p_queue_1.default({ concurrency: QUEUE_CONCURRENCY });
    const labelAssociationBatch = {
        items: [],
        timer: null,
        processing: false
    };
    const messageBatch = {
        items: [],
        timer: null,
        processing: false
    };
    const collections = {
        chats: db.collection(`${collectionPrefix}chats`),
        contacts: db.collection(`${collectionPrefix}contacts`),
        messages: db.collection(`${collectionPrefix}messages`),
        groupMetadata: db.collection(`${collectionPrefix}groupMetadata`),
        state: db.collection(`${collectionPrefix}state`),
        presences: db.collection(`${collectionPrefix}presences`),
        labels: db.collection(`${collectionPrefix}labels`),
        labelAssociations: db.collection(`${collectionPrefix}labelAssociations`)
    };
    const processBatchedLabelAssociations = async () => {
        if (labelAssociationBatch.processing || labelAssociationBatch.items.length === 0)
            return;
        labelAssociationBatch.processing = true;
        const itemsToProcess = [...labelAssociationBatch.items];
        labelAssociationBatch.items = [];
        try {
            const bulkOps = itemsToProcess.map(association => ({
                replaceOne: {
                    filter: {
                        instanceId,
                        chatId: association.chatId,
                        labelId: association.labelId,
                        messageId: 'messageId' in association ? association.messageId : ''
                    },
                    replacement: {
                        ...association,
                        instanceId,
                        updatedAt: new Date()
                    },
                    upsert: true
                }
            }));
            for (let i = 0; i < bulkOps.length; i += BATCH_SIZE) {
                const chunk = bulkOps.slice(i, i + BATCH_SIZE);
                await collections.labelAssociations.bulkWrite(chunk, { ordered: false });
                if (i + BATCH_SIZE < bulkOps.length) {
                    await new Promise(resolve => setTimeout(resolve, BATCH_DELAY));
                }
            }
            performanceMetrics.labelsProcessed += itemsToProcess.length;
            performanceMetrics.batchesProcessed++;
        }
        catch (error) {
            console.error('Error processing label associations batch:', error);
            performanceMetrics.errors++;
        }
        finally {
            labelAssociationBatch.processing = false;
        }
    };
    const processBatchedMessages = async () => {
        if (messageBatch.processing || messageBatch.items.length === 0)
            return;
        messageBatch.processing = true;
        const itemsToProcess = [...messageBatch.items];
        messageBatch.items = [];
        try {
            const bulkOps = itemsToProcess.map(({ jid, ...message }) => ({
                replaceOne: {
                    filter: {
                        instanceId,
                        jid,
                        'key.id': message.key?.id
                    },
                    replacement: {
                        ...message,
                        instanceId,
                        jid,
                        updatedAt: new Date()
                    },
                    upsert: true
                }
            }));
            for (let i = 0; i < bulkOps.length; i += BATCH_SIZE) {
                const chunk = bulkOps.slice(i, i + BATCH_SIZE);
                await collections.messages.bulkWrite(chunk, { ordered: false });
                chunk.forEach(op => {
                    const msgId = op.replaceOne.filter['key.id'];
                    const msgJid = op.replaceOne.filter.jid;
                    const cacheKey = `msg_${instanceId}_${msgJid}_${msgId}`;
                    binaryConversionCache.del(cacheKey);
                });
                if (i + BATCH_SIZE < bulkOps.length) {
                    await new Promise(resolve => setTimeout(resolve, BATCH_DELAY));
                }
            }
            performanceMetrics.messagesProcessed += itemsToProcess.length;
            performanceMetrics.batchesProcessed++;
        }
        catch (error) {
            console.error('Error processing messages batch:', error);
            performanceMetrics.errors++;
        }
        finally {
            messageBatch.processing = false;
        }
    };
    const scheduleLabelBatch = () => {
        if (labelAssociationBatch.timer) {
            clearTimeout(labelAssociationBatch.timer);
        }
        labelAssociationBatch.timer = setTimeout(() => {
            processBatchedLabelAssociations();
        }, 100);
    };
    const scheduleMessageBatch = () => {
        if (messageBatch.timer) {
            clearTimeout(messageBatch.timer);
        }
        messageBatch.timer = setTimeout(() => {
            processBatchedMessages();
        }, 100);
    };
    const createIndexes = async () => {
        const ttlSeconds = ttlDays * 24 * 60 * 60;
        await Promise.all([
            collections.chats.createIndex({ instanceId: 1, id: 1 }, { unique: true }),
            collections.chats.createIndex({ updatedAt: 1 }, { expireAfterSeconds: ttlSeconds }),
            collections.contacts.createIndex({ instanceId: 1, id: 1 }, { unique: true }),
            collections.contacts.createIndex({ updatedAt: 1 }, { expireAfterSeconds: ttlSeconds }),
            collections.messages.createIndex({ instanceId: 1, jid: 1, 'key.id': 1 }, { unique: true }),
            collections.messages.createIndex({ instanceId: 1, jid: 1, messageTimestamp: -1 }),
            collections.messages.createIndex({ updatedAt: 1 }, { expireAfterSeconds: ttlSeconds }),
            collections.groupMetadata.createIndex({ instanceId: 1, id: 1 }, { unique: true }),
            collections.groupMetadata.createIndex({ updatedAt: 1 }, { expireAfterSeconds: ttlSeconds }),
            collections.state.createIndex({ instanceId: 1 }, { unique: true }),
            collections.presences.createIndex({ instanceId: 1, id: 1 }, { unique: true }),
            collections.presences.createIndex({ updatedAt: 1 }, { expireAfterSeconds: ttlSeconds }),
            collections.labels.createIndex({ instanceId: 1, id: 1 }, { unique: true }),
            collections.labels.createIndex({ updatedAt: 1 }, { expireAfterSeconds: ttlSeconds }),
            collections.labelAssociations.createIndex({ instanceId: 1, chatId: 1, labelId: 1, messageId: 1 }, { unique: true }),
            collections.labelAssociations.createIndex({ instanceId: 1, chatId: 1 }),
            collections.labelAssociations.createIndex({ instanceId: 1, messageId: 1 }),
            collections.labelAssociations.createIndex({ updatedAt: 1 }, { expireAfterSeconds: ttlSeconds })
        ]);
    };
    await createIndexes();
    const store = {
        instanceId,
        async getChats() {
            const chats = await collections.chats
                .find({ instanceId })
                .sort({ conversationTimestamp: -1 })
                .toArray();
            return chats.map(({ _id: _1, instanceId: _2, updatedAt: _3, ...chat }) => chat);
        },
        async getChat(jid) {
            const chat = await collections.chats.findOne({ instanceId, id: jid });
            if (!chat)
                return null;
            const { _id: _1, instanceId: _2, updatedAt: _3, ...chatData } = chat;
            return chatData;
        },
        async upsertChats(...chats) {
            if (chats.length === 0)
                return;
            const bulkOps = chats.map(chat => ({
                replaceOne: {
                    filter: { instanceId, id: chat.id },
                    replacement: { ...chat, instanceId, updatedAt: new Date() },
                    upsert: true
                }
            }));
            await collections.chats.bulkWrite(bulkOps);
        },
        async updateChat(jid, update) {
            const result = await collections.chats.updateOne({ instanceId, id: jid }, {
                $set: { ...update, updatedAt: new Date() }
            });
            return result.modifiedCount > 0;
        },
        async deleteChats(jids) {
            await collections.chats.deleteMany({
                instanceId,
                id: { $in: jids }
            });
        },
        async getContacts() {
            const contacts = await collections.contacts
                .find({ instanceId })
                .toArray();
            const contactsMap = {};
            for (const contact of contacts) {
                const { _id: _1, instanceId: _2, updatedAt: _3, ...contactData } = contact;
                contactsMap[contact.id] = contactData;
            }
            return contactsMap;
        },
        async getContact(jid) {
            const contact = await collections.contacts.findOne({ instanceId, id: jid });
            if (!contact)
                return null;
            const { _id: _1, instanceId: _2, updatedAt: _3, ...contactData } = contact;
            return contactData;
        },
        async upsertContacts(contacts) {
            if (contacts.length === 0)
                return;
            const bulkOps = contacts.map(contact => ({
                replaceOne: {
                    filter: { instanceId, id: contact.id },
                    replacement: { ...contact, instanceId, updatedAt: new Date() },
                    upsert: true
                }
            }));
            for (let i = 0; i < bulkOps.length; i += BATCH_SIZE) {
                const chunk = bulkOps.slice(i, i + BATCH_SIZE);
                await generalQueue.add(async () => {
                    await collections.contacts.bulkWrite(chunk, { ordered: false });
                });
                if (i + BATCH_SIZE < bulkOps.length && bulkOps.length > 1000) {
                    await new Promise(resolve => setTimeout(resolve, BATCH_DELAY));
                }
            }
        },
        async getMessages(jid) {
            const messages = await collections.messages
                .find({ instanceId, jid })
                .sort({ messageTimestamp: -1 })
                .toArray();
            return messages.map(({ _id: _1, instanceId: _2, jid: _3, updatedAt: _4, ...msg }) => convertBinaryToBuffer(msg));
        },
        async getMessage(jid, id) {
            const cacheKey = `msg_${instanceId}_${jid}_${id}`;
            const cached = binaryConversionCache.get(cacheKey);
            if (cached)
                return cached;
            const message = await collections.messages.findOne({
                instanceId,
                jid,
                'key.id': id
            });
            if (!message)
                return null;
            const { _id: _1, instanceId: _2, jid: _3, updatedAt: _4, ...msg } = message;
            const converted = convertBinaryToBuffer(msg);
            binaryConversionCache.set(cacheKey, converted);
            return converted;
        },
        async upsertMessage(jid, message, useBatch = false) {
            if (useBatch) {
                messageBatch.items.push({ ...message, jid });
                scheduleMessageBatch();
                if (messageBatch.items.length >= BATCH_SIZE) {
                    await processBatchedMessages();
                }
            }
            else {
                const cacheKey = `msg_${instanceId}_${jid}_${message.key?.id}`;
                binaryConversionCache.del(cacheKey);
                await messageQueue.add(async () => {
                    await collections.messages.replaceOne({
                        instanceId,
                        jid,
                        'key.id': message.key?.id
                    }, {
                        ...message,
                        instanceId,
                        jid,
                        updatedAt: new Date()
                    }, { upsert: true });
                });
            }
        },
        async updateMessage(jid, id, update) {
            const cacheKey = `msg_${instanceId}_${jid}_${id}`;
            binaryConversionCache.del(cacheKey);
            const processedUpdate = convertBinaryToBuffer(update);
            const result = await collections.messages.updateOne({
                instanceId,
                jid,
                'key.id': id
            }, {
                $set: { ...processedUpdate, updatedAt: new Date() }
            });
            return result.modifiedCount > 0;
        },
        async deleteMessages(jid, ids) {
            if (ids && ids.length > 0) {
                ids.forEach(id => {
                    const cacheKey = `msg_${instanceId}_${jid}_${id}`;
                    binaryConversionCache.del(cacheKey);
                });
            }
            else {
                const keys = binaryConversionCache.keys();
                keys.forEach(key => {
                    if (key.startsWith(`msg_${instanceId}_${jid}_`)) {
                        binaryConversionCache.del(key);
                    }
                });
            }
            const filter = { instanceId, jid };
            if (ids && ids.length > 0) {
                filter['key.id'] = { $in: ids };
            }
            await collections.messages.deleteMany(filter);
        },
        async getGroupMetadata(jid) {
            const metadata = await collections.groupMetadata.findOne({ instanceId, id: jid });
            if (!metadata)
                return null;
            const { _id: _1, instanceId: _2, updatedAt: _3, ...metadataData } = metadata;
            return metadataData;
        },
        async upsertGroupMetadata(jid, metadata) {
            await collections.groupMetadata.replaceOne({ instanceId, id: jid }, { ...metadata, instanceId, updatedAt: new Date() }, { upsert: true });
        },
        async getState() {
            const state = await collections.state.findOne({ instanceId });
            if (!state)
                return { connection: 'close' };
            const { _id: _1, instanceId: _2, updatedAt: _3, ...stateData } = state;
            return stateData;
        },
        async updateState(update) {
            await collections.state.updateOne({ instanceId }, {
                $set: { ...update, instanceId, updatedAt: new Date() }
            }, { upsert: true });
        },
        async getPresences() {
            const presences = await collections.presences
                .find({ instanceId })
                .toArray();
            const presencesMap = {};
            for (const presence of presences) {
                presencesMap[presence.id] = presence.presences;
            }
            return presencesMap;
        },
        async updatePresence(id, presences) {
            await collections.presences.updateOne({ instanceId, id }, {
                $set: { presences, updatedAt: new Date() }
            }, { upsert: true });
        },
        async getLabels() {
            const labels = await collections.labels
                .find({ instanceId })
                .toArray();
            const labelsMap = {};
            for (const label of labels) {
                const { _id: _1, instanceId: _2, updatedAt: _3, ...labelData } = label;
                labelsMap[label.id] = labelData;
            }
            return labelsMap;
        },
        async upsertLabel(id, label) {
            await collections.labels.replaceOne({ instanceId, id }, { ...label, instanceId, updatedAt: new Date() }, { upsert: true });
        },
        async deleteLabel(id) {
            await collections.labels.deleteOne({ instanceId, id });
        },
        async getLabelAssociations() {
            const associations = await collections.labelAssociations
                .find({ instanceId })
                .toArray();
            return associations.map(({ _id: _1, instanceId: _2, updatedAt: _3, ...assoc }) => assoc);
        },
        async getChatLabels(chatId) {
            const associations = await collections.labelAssociations
                .find({ instanceId, chatId })
                .toArray();
            return associations.map(({ _id: _1, instanceId: _2, updatedAt: _3, ...assoc }) => assoc);
        },
        async getMessageLabels(messageId) {
            const associations = await collections.labelAssociations
                .find({ instanceId, messageId })
                .toArray();
            return associations.map(assoc => assoc.labelId);
        },
        async upsertLabelAssociation(association) {
            labelAssociationBatch.items.push(association);
            scheduleLabelBatch();
            if (labelAssociationBatch.items.length >= BATCH_SIZE) {
                await processBatchedLabelAssociations();
            }
        },
        async deleteLabelAssociation(association) {
            await collections.labelAssociations.deleteOne({
                instanceId,
                chatId: association.chatId,
                labelId: association.labelId,
                messageId: 'messageId' in association ? association.messageId : ''
            });
        },
        bind(ev) {
            ev.on('connection.update', async (update) => {
                await store.updateState(update);
            });
            ev.on('messaging-history.set', async ({ chats: newChats, contacts: newContacts, messages: newMessages, isLatest }) => {
                if (isLatest) {
                    await store.clearAll();
                }
                const promises = [];
                if (newChats?.length) {
                    promises.push(generalQueue.add(async () => {
                        await store.upsertChats(...newChats);
                    }));
                }
                if (newContacts?.length) {
                    promises.push(generalQueue.add(async () => {
                        await store.upsertContacts(newContacts);
                    }));
                }
                if (newMessages?.length) {
                    for (const msg of newMessages) {
                        const jid = msg.key.remoteJid;
                        await store.upsertMessage(jid, msg, true);
                    }
                    await processBatchedMessages();
                }
                await Promise.all(promises);
            });
            ev.on('contacts.upsert', async (contacts) => {
                await store.upsertContacts(contacts);
            });
            ev.on('contacts.update', async (updates) => {
                for (const update of updates) {
                    const contact = await store.getContact(update.id);
                    if (contact) {
                        Object.assign(contact, update);
                        await store.upsertContacts([contact]);
                    }
                }
            });
            ev.on('chats.upsert', async (newChats) => {
                await store.upsertChats(...newChats);
            });
            ev.on('chats.update', async (updates) => {
                for (const update of updates) {
                    await store.updateChat(update.id, update);
                }
            });
            ev.on('labels.edit', async (label) => {
                if (label.deleted) {
                    await store.deleteLabel(label.id);
                }
                else {
                    await store.upsertLabel(label.id, label);
                }
            });
            ev.on('labels.association', async ({ type, association }) => {
                if (type === 'add') {
                    await store.upsertLabelAssociation(association);
                }
                else if (type === 'remove') {
                    await store.deleteLabelAssociation(association);
                }
            });
            ev.on('presence.update', async ({ id, presences: update }) => {
                const existing = (await store.getPresences())[id] || {};
                Object.assign(existing, update);
                await store.updatePresence(id, existing);
            });
            ev.on('chats.delete', async (deletions) => {
                await store.deleteChats(deletions);
            });
            ev.on('messages.upsert', async ({ messages: newMessages, type }) => {
                for (const msg of newMessages) {
                    const jid = (0, baileys_1.jidNormalizedUser)(msg.key.remoteJid);
                    await store.upsertMessage(jid, msg);
                    if (type === 'notify' && !(await store.getChat(jid))) {
                        await store.upsertChats({
                            id: jid,
                            conversationTimestamp: (0, baileys_1.toNumber)(msg.messageTimestamp),
                            unreadCount: 1
                        });
                    }
                }
            });
            ev.on('messages.update', async (updates) => {
                for (const { update, key } of updates) {
                    const jid = (0, baileys_1.jidNormalizedUser)(key.remoteJid);
                    await store.updateMessage(jid, key.id, update);
                }
            });
            ev.on('messages.delete', async (item) => {
                if ('all' in item) {
                    await store.deleteMessages(item.jid);
                }
                else {
                    const jid = item.keys[0].remoteJid;
                    const ids = item.keys.map(k => k.id);
                    await store.deleteMessages(jid, ids);
                }
            });
            ev.on('groups.update', async (updates) => {
                for (const update of updates) {
                    const metadata = await store.getGroupMetadata(update.id);
                    if (metadata) {
                        Object.assign(metadata, update);
                        await store.upsertGroupMetadata(update.id, metadata);
                    }
                }
            });
            ev.on('group-participants.update', async ({ id, participants, action }) => {
                const metadata = await store.getGroupMetadata(id);
                if (metadata) {
                    switch (action) {
                        case 'add':
                            metadata.participants.push(...participants.map(id => ({
                                id,
                                isAdmin: false,
                                isSuperAdmin: false
                            })));
                            break;
                        case 'demote':
                        case 'promote':
                            for (const participant of metadata.participants) {
                                if (participants.includes(participant.id)) {
                                    participant.isAdmin = action === 'promote';
                                }
                            }
                            break;
                        case 'remove':
                            metadata.participants = metadata.participants.filter(p => !participants.includes(p.id));
                            break;
                    }
                    await store.upsertGroupMetadata(id, metadata);
                }
            });
            ev.on('message-receipt.update', async (updates) => {
                for (const { key, receipt } of updates) {
                    const msg = await store.getMessage(key.remoteJid, key.id);
                    if (msg) {
                        (0, baileys_1.updateMessageWithReceipt)(msg, receipt);
                        await store.updateMessage(key.remoteJid, key.id, msg);
                    }
                }
            });
            ev.on('messages.reaction', async (reactions) => {
                for (const { key, reaction } of reactions) {
                    const msg = await store.getMessage(key.remoteJid, key.id);
                    if (msg) {
                        (0, baileys_1.updateMessageWithReaction)(msg, reaction);
                        await store.updateMessage(key.remoteJid, key.id, msg);
                    }
                }
            });
        },
        async loadMessages(jid, count, cursor) {
            const mode = !cursor || 'before' in cursor ? 'before' : 'after';
            const cursorKey = cursor ? ('before' in cursor ? cursor.before : cursor.after) : undefined;
            let messages = [];
            if (mode === 'before') {
                const query = { instanceId, jid };
                if (cursorKey) {
                    const cursorMsg = await store.getMessage(jid, cursorKey.id);
                    if (cursorMsg) {
                        query.messageTimestamp = { $lt: cursorMsg.messageTimestamp };
                    }
                }
                messages = await collections.messages
                    .find(query)
                    .sort({ messageTimestamp: -1 })
                    .limit(count)
                    .toArray()
                    .then(msgs => msgs.map(({ _id: _1, instanceId: _2, jid: _3, updatedAt: _4, ...msg }) => convertBinaryToBuffer(msg)));
            }
            return messages;
        },
        async loadMessage(jid, id) {
            const msg = await store.getMessage(jid, id);
            return msg || undefined;
        },
        async mostRecentMessage(jid) {
            const messages = await store.getMessages(jid);
            return messages[0];
        },
        async clearAll() {
            const keys = binaryConversionCache.keys();
            keys.forEach(key => {
                if (key.startsWith(`msg_${instanceId}_`)) {
                    binaryConversionCache.del(key);
                }
            });
            await Promise.all([
                collections.chats.deleteMany({ instanceId }),
                collections.contacts.deleteMany({ instanceId }),
                collections.messages.deleteMany({ instanceId }),
                collections.groupMetadata.deleteMany({ instanceId }),
                collections.presences.deleteMany({ instanceId }),
                collections.labels.deleteMany({ instanceId }),
                collections.labelAssociations.deleteMany({ instanceId })
            ]);
        },
        getPerformanceStats() {
            const uptime = Date.now() - performanceMetrics.lastResetTime.getTime();
            return {
                ...performanceMetrics,
                uptime
            };
        },
        resetPerformanceStats() {
            performanceMetrics.messagesProcessed = 0;
            performanceMetrics.labelsProcessed = 0;
            performanceMetrics.batchesProcessed = 0;
            performanceMetrics.errors = 0;
            performanceMetrics.lastResetTime = new Date();
        },
        async close() {
            await processBatchedLabelAssociations();
            await processBatchedMessages();
            if (labelAssociationBatch.timer) {
                clearTimeout(labelAssociationBatch.timer);
            }
            if (messageBatch.timer) {
                clearTimeout(messageBatch.timer);
            }
            await Promise.all([
                messageQueue.onIdle(),
                labelQueue.onIdle(),
                generalQueue.onIdle()
            ]);
            const keys = binaryConversionCache.keys();
            keys.forEach(key => {
                if (key.startsWith(`msg_${instanceId}_`)) {
                    binaryConversionCache.del(key);
                }
            });
            await client.close();
            activeConnections = activeConnections.filter(c => c.client !== client);
        }
    };
    return store;
};
exports.makeMongoDBStore = makeMongoDBStore;
const cleanupMongoDBStore = async (instanceId, deleteData = false) => {
    if (!instanceId) {
        for (const conn of activeConnections) {
            try {
                await conn.client.close();
            }
            catch (error) {
                console.error('Error closing MongoDB connection:', error);
            }
        }
        activeConnections = [];
        return;
    }
    const instanceConnections = activeConnections.filter(c => c.instanceId === instanceId);
    if (instanceConnections.length === 0) {
        console.warn(`No active connections found for instance: ${instanceId}`);
        return;
    }
    for (const conn of instanceConnections) {
        try {
            if (deleteData) {
                const db = conn.client.db(conn.database);
                const collections = [
                    `${conn.collectionPrefix}chats`,
                    `${conn.collectionPrefix}contacts`,
                    `${conn.collectionPrefix}messages`,
                    `${conn.collectionPrefix}groupMetadata`,
                    `${conn.collectionPrefix}state`,
                    `${conn.collectionPrefix}presences`,
                    `${conn.collectionPrefix}labels`,
                    `${conn.collectionPrefix}labelAssociations`
                ];
                for (const collName of collections) {
                    try {
                        await db.collection(collName).deleteMany({ instanceId });
                    }
                    catch (error) {
                        console.error(`Error deleting data from ${collName}:`, error);
                    }
                }
                console.log(`Deleted all data for instance: ${instanceId}`);
            }
            await conn.client.close();
        }
        catch (error) {
            console.error(`Error cleaning up instance ${instanceId}:`, error);
        }
    }
    activeConnections = activeConnections.filter(c => c.instanceId !== instanceId);
};
exports.cleanupMongoDBStore = cleanupMongoDBStore;
//# sourceMappingURL=makeMongoDBStore.js.map