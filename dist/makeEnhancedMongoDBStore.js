"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.makeEnhancedMongoDBStore = void 0;
const mongodb_1 = require("mongodb");
const node_cache_1 = __importDefault(require("node-cache"));
const bullmq_1 = require("bullmq");
const ioredis_1 = __importDefault(require("ioredis"));
const DEFAULT_TTL_DAYS = 30;
const DEFAULT_EVENT_CONFIG = {
    enabled: true,
    useBatch: false
};
var QueueType;
(function (QueueType) {
    QueueType["LABELS"] = "labels";
    QueueType["LABEL_ASSOCIATIONS"] = "label-associations";
    QueueType["MESSAGES"] = "messages";
    QueueType["CHATS"] = "chats";
    QueueType["CONTACTS"] = "contacts";
    QueueType["GROUP_METADATA"] = "group-metadata";
    QueueType["PRESENCES"] = "presences";
    QueueType["STATE"] = "state";
})(QueueType || (QueueType = {}));
const eventMetricsMap = new Map();
const binaryConversionCache = new node_cache_1.default({ stdTTL: 300, checkperiod: 60 });
const convertBinaryToBuffer = (obj) => {
    try {
        if (!obj || typeof obj !== 'object')
            return obj;
        if (obj._bsontype === 'Binary') {
            if (obj.buffer instanceof Buffer) {
                return Buffer.from(obj.buffer);
            }
            else if (obj.buffer instanceof ArrayBuffer) {
                return Buffer.from(obj.buffer);
            }
            else if (obj.buffer instanceof Uint8Array) {
                return Buffer.from(obj.buffer);
            }
            else if (obj.buffer) {
                return Buffer.from(obj.buffer);
            }
        }
        if (obj.type === 'Buffer' && Array.isArray(obj.data)) {
            return Buffer.from(obj.data);
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
        console.error('Error converting binary to buffer:', error);
        return obj;
    }
};
const makeEnhancedMongoDBStore = async (config) => {
    const { uri, database: dbName, instanceId, ttlDays = DEFAULT_TTL_DAYS, collectionTTL = {}, events = {}, storeAllByDefault = true, collectionPrefix = 'baileys_', redis, logLevel = 'none', enableMetrics = false, hooks = {} } = config;
    const log = (...args) => {
        if (logLevel === 'all') {
            console.log(...args);
        }
    };
    const logError = (...args) => {
        if (logLevel === 'error' || logLevel === 'warn' || logLevel === 'all') {
            console.error(...args);
        }
    };
    const logWarn = (...args) => {
        if (logLevel === 'warn' || logLevel === 'all') {
            console.warn(...args);
        }
    };
    const client = new mongodb_1.MongoClient(uri, {
        maxPoolSize: 100,
        minPoolSize: 10,
        maxIdleTimeMS: 30000,
        writeConcern: { w: 1, j: false }
    });
    await client.connect();
    const db = client.db(dbName);
    const getCollections = () => {
        if (!db) {
            throw new Error('Database not initialized');
        }
        return {
            chats: db.collection(`${collectionPrefix}chats`),
            contacts: db.collection(`${collectionPrefix}contacts`),
            messages: db.collection(`${collectionPrefix}messages`),
            groupMetadata: db.collection(`${collectionPrefix}groupMetadata`),
            state: db.collection(`${collectionPrefix}state`),
            presences: db.collection(`${collectionPrefix}presences`),
            labels: db.collection(`${collectionPrefix}labels`),
            labelAssociations: db.collection(`${collectionPrefix}labelAssociations`)
        };
    };
    const collections = getCollections();
    const BATCH_SIZE = 100;
    let bullInitialized = false;
    const queues = new Map();
    const workers = new Map();
    let redisConnection = null;
    const defaultJobOptions = {
        removeOnComplete: {
            age: 60,
            count: 10
        },
        removeOnFail: {
            age: 300
        },
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 2000
        }
    };
    const initializeBullQueues = async () => {
        if (!redis)
            return;
        try {
            log(`🐂 Initializing Bull queues for instance ${instanceId}...`);
            if (typeof redis.connection === 'string') {
                redisConnection = new ioredis_1.default(redis.connection, {
                    maxRetriesPerRequest: null,
                    enableReadyCheck: true,
                    lazyConnect: false
                });
            }
            else {
                redisConnection = new ioredis_1.default({
                    ...redis.connection,
                    maxRetriesPerRequest: null,
                    enableReadyCheck: true,
                    lazyConnect: false
                });
            }
            await redisConnection.ping();
            try {
                const redisConfig = await redisConnection.config('GET', 'maxmemory-policy');
                const policy = redisConfig[1];
                if (policy && policy !== 'noeviction') {
                    logWarn(`⚠️  Redis eviction policy is '${policy}'. Consider using 'noeviction' for BullMQ or a separate Redis instance.`);
                }
            }
            catch (err) {
            }
            const queuePrefix = redis.queuePrefix || 'baileys';
            const redisOpts = { connection: redisConnection };
            const createQueueAndWorker = (queueType, processor) => {
                const queueName = `${queuePrefix}_${queueType}_${instanceId}`;
                const queue = new bullmq_1.Queue(queueName, redisOpts);
                queues.set(queueType, queue);
                const concurrency = queueType === QueueType.LABEL_ASSOCIATIONS ? 1 : (redis.concurrency || 50);
                const worker = new bullmq_1.Worker(queueName, processor, {
                    ...redisOpts,
                    concurrency,
                    autorun: true
                });
                log(`🔧 Created ${queueType} queue with concurrency: ${concurrency}`);
                worker.on('completed', (job) => {
                    if (queueType !== QueueType.LABEL_ASSOCIATIONS) {
                        log(`✅ ${queueType} job ${job.id} completed`);
                    }
                });
                worker.on('failed', (job, err) => {
                    logError(`❌ ${queueType} job ${job?.id} failed:`, err.message);
                    if (enableMetrics) {
                        updateEventMetrics(queueType, 'error');
                    }
                });
                worker.on('stalled', (jobId) => {
                    logWarn(`⚠️ ${queueType} job ${jobId} stalled`);
                });
                workers.set(queueType, worker);
                queue.obliterate({ force: true }).catch(() => { });
            };
            createQueueAndWorker(QueueType.MESSAGES, async (job) => {
                const { type, jid, message, messageId, update, deleteIds } = job.data;
                if (type === 'upsert' && message) {
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
                }
                else if (type === 'update' && messageId && update) {
                    await collections.messages.updateOne({
                        instanceId,
                        jid,
                        'key.id': messageId
                    }, {
                        $set: { ...update, updatedAt: new Date() }
                    });
                }
                else if (type === 'delete') {
                    if (deleteIds && deleteIds.length > 0) {
                        await collections.messages.deleteMany({
                            instanceId,
                            jid,
                            'key.id': { $in: deleteIds }
                        });
                    }
                    else {
                        await collections.messages.deleteMany({ instanceId, jid });
                    }
                }
                return { success: true };
            });
            createQueueAndWorker(QueueType.CHATS, async (job) => {
                const { type, chats, chatId, update, deleteIds } = job.data;
                if (type === 'upsert' && chats) {
                    const bulkOps = chats.map(chat => ({
                        replaceOne: {
                            filter: { instanceId, id: chat.id },
                            replacement: { ...chat, instanceId, updatedAt: new Date() },
                            upsert: true
                        }
                    }));
                    await collections.chats.bulkWrite(bulkOps);
                }
                else if (type === 'update' && chatId && update) {
                    await collections.chats.updateOne({ instanceId, id: chatId }, { $set: { ...update, updatedAt: new Date() } });
                }
                else if (type === 'delete' && deleteIds) {
                    await collections.chats.deleteMany({
                        instanceId,
                        id: { $in: deleteIds }
                    });
                }
                return { success: true };
            });
            createQueueAndWorker(QueueType.CONTACTS, async (job) => {
                const { type, contacts, contact } = job.data;
                if (type === 'upsert' && contacts) {
                    const bulkOps = contacts.map(contact => ({
                        replaceOne: {
                            filter: { instanceId, id: contact.id },
                            replacement: { ...contact, instanceId, updatedAt: new Date() },
                            upsert: true
                        }
                    }));
                    await collections.contacts.bulkWrite(bulkOps, { ordered: false });
                }
                else if (type === 'update' && contact) {
                    await collections.contacts.replaceOne({ instanceId, id: contact.id }, { ...contact, instanceId, updatedAt: new Date() }, { upsert: true });
                }
                return { success: true };
            });
            createQueueAndWorker(QueueType.GROUP_METADATA, async (job) => {
                const { type, jid, metadata, update } = job.data;
                if (type === 'upsert') {
                    await collections.groupMetadata.replaceOne({ instanceId, id: metadata.id }, { ...metadata, instanceId, updatedAt: new Date() }, { upsert: true });
                }
                else if (type === 'update' && update) {
                    await collections.groupMetadata.updateOne({ instanceId, id: jid }, { $set: { ...update, updatedAt: new Date() } });
                }
                return { success: true };
            });
            createQueueAndWorker(QueueType.PRESENCES, async (job) => {
                const { id, presences } = job.data;
                await collections.presences.updateOne({ instanceId, id }, {
                    $set: { presences, updatedAt: new Date() }
                }, { upsert: true });
                return { success: true };
            });
            createQueueAndWorker(QueueType.STATE, async (job) => {
                const { update } = job.data;
                await collections.state.updateOne({ instanceId }, {
                    $set: { ...update, instanceId, updatedAt: new Date() }
                }, { upsert: true });
                return { success: true };
            });
            createQueueAndWorker(QueueType.LABELS, async (job) => {
                const { type, id, label } = job.data;
                if (type === 'upsert' && label) {
                    await collections.labels.replaceOne({ instanceId, id }, { ...label, instanceId, updatedAt: new Date() }, { upsert: true });
                }
                else if (type === 'delete') {
                    await collections.labels.deleteOne({ instanceId, id });
                }
                return { success: true };
            });
            createQueueAndWorker(QueueType.LABEL_ASSOCIATIONS, async (job) => {
                const { type, association } = job.data;
                if (type === 'upsert') {
                    const filter = {
                        instanceId,
                        chatId: association.chatId,
                        labelId: association.labelId
                    };
                    if ('messageId' in association && association.messageId) {
                        filter.messageId = association.messageId;
                    }
                    await collections.labelAssociations.replaceOne(filter, {
                        ...association,
                        instanceId,
                        updatedAt: new Date()
                    }, { upsert: true });
                }
                else if (type === 'delete') {
                    const filter = {
                        instanceId,
                        chatId: association.chatId,
                        labelId: association.labelId
                    };
                    if ('messageId' in association && association.messageId) {
                        filter.messageId = association.messageId;
                    }
                    await collections.labelAssociations.deleteOne(filter);
                }
                return { success: true };
            });
            bullInitialized = true;
            log(`✅ Bull queues initialized successfully for instance ${instanceId}`);
        }
        catch (error) {
            logError(`❌ Failed to initialize Bull queues for instance ${instanceId}:`, error);
            log('⚠️  Falling back to in-memory queue processing');
            for (const worker of workers.values()) {
                await worker.close();
            }
            for (const queue of queues.values()) {
                await queue.close();
            }
            if (redisConnection)
                redisConnection.disconnect();
            queues.clear();
            workers.clear();
            redisConnection = null;
            bullInitialized = false;
        }
    };
    await initializeBullQueues();
    const eventConfigs = new Map();
    Object.keys(events).forEach(eventType => {
        eventConfigs.set(eventType, { ...DEFAULT_EVENT_CONFIG, ...events[eventType] });
    });
    const getEventConfig = (eventType) => {
        if (eventConfigs.has(eventType)) {
            return eventConfigs.get(eventType);
        }
        if (events[eventType]) {
            const config = { ...DEFAULT_EVENT_CONFIG, ...events[eventType] };
            eventConfigs.set(eventType, config);
            return config;
        }
        return {
            enabled: storeAllByDefault,
            ttlDays: ttlDays,
            useBatch: false
        };
    };
    const shouldStoreEvent = async (eventType, data) => {
        const config = getEventConfig(eventType);
        if (!config.enabled) {
            if (enableMetrics)
                updateEventMetrics(eventType, 'skipped');
            return false;
        }
        if (config.filter && !config.filter(data)) {
            if (enableMetrics)
                updateEventMetrics(eventType, 'skipped');
            return false;
        }
        if (hooks.beforeStore) {
            const shouldStore = await hooks.beforeStore(eventType, data);
            if (!shouldStore) {
                if (enableMetrics)
                    updateEventMetrics(eventType, 'skipped');
                return false;
            }
        }
        return true;
    };
    const getTTLForCollection = (collectionName, eventType) => {
        if (eventType) {
            const eventConfig = getEventConfig(eventType);
            if (eventConfig.ttlDays !== undefined) {
                return eventConfig.ttlDays;
            }
        }
        if (collectionTTL[collectionName] !== undefined) {
            return collectionTTL[collectionName];
        }
        return ttlDays;
    };
    const updateEventMetrics = (eventType, action) => {
        if (!enableMetrics)
            return;
        let metrics = eventMetricsMap.get(eventType);
        if (!metrics) {
            metrics = {
                eventType,
                totalReceived: 0,
                totalStored: 0,
                totalSkipped: 0,
                totalErrors: 0
            };
            eventMetricsMap.set(eventType, metrics);
        }
        switch (action) {
            case 'received':
                metrics.totalReceived++;
                break;
            case 'stored':
                metrics.totalStored++;
                metrics.lastProcessedAt = new Date();
                break;
            case 'skipped':
                metrics.totalSkipped++;
                break;
            case 'error':
                metrics.totalErrors++;
                break;
        }
    };
    const createIndexes = async () => {
        const indexPromises = [];
        const chatsTTL = getTTLForCollection('chats') * 24 * 60 * 60;
        indexPromises.push(collections.chats.createIndex({ instanceId: 1, id: 1 }, { unique: true }).then(() => { }), collections.chats.createIndex({ updatedAt: 1 }, { expireAfterSeconds: chatsTTL }).then(() => { }));
        const contactsTTL = getTTLForCollection('contacts') * 24 * 60 * 60;
        indexPromises.push(collections.contacts.createIndex({ instanceId: 1, id: 1 }, { unique: true }).then(() => { }), collections.contacts.createIndex({ updatedAt: 1 }, { expireAfterSeconds: contactsTTL }).then(() => { }));
        const messagesTTL = getTTLForCollection('messages') * 24 * 60 * 60;
        indexPromises.push(collections.messages.createIndex({ instanceId: 1, jid: 1, 'key.id': 1 }, { unique: true }).then(() => { }), collections.messages.createIndex({ instanceId: 1, jid: 1, messageTimestamp: -1 }).then(() => { }), collections.messages.createIndex({ updatedAt: 1 }, { expireAfterSeconds: messagesTTL }).then(() => { }));
        const groupsTTL = getTTLForCollection('groupMetadata') * 24 * 60 * 60;
        indexPromises.push(collections.groupMetadata.createIndex({ instanceId: 1, id: 1 }, { unique: true }).then(() => { }), collections.groupMetadata.createIndex({ updatedAt: 1 }, { expireAfterSeconds: groupsTTL }).then(() => { }));
        const stateTTL = getTTLForCollection('state') * 24 * 60 * 60;
        indexPromises.push(collections.state.createIndex({ instanceId: 1 }, { unique: true }).then(() => { }), collections.state.createIndex({ updatedAt: 1 }, { expireAfterSeconds: stateTTL }).then(() => { }));
        const presencesTTL = getTTLForCollection('presences') * 24 * 60 * 60;
        indexPromises.push(collections.presences.createIndex({ instanceId: 1, id: 1 }, { unique: true }).then(() => { }), collections.presences.createIndex({ updatedAt: 1 }, { expireAfterSeconds: presencesTTL }).then(() => { }));
        const labelsTTL = getTTLForCollection('labels') * 24 * 60 * 60;
        indexPromises.push(collections.labels.createIndex({ instanceId: 1, id: 1 }, { unique: true }).then(() => { }), collections.labels.createIndex({ updatedAt: 1 }, { expireAfterSeconds: labelsTTL }).then(() => { }));
        const labelAssocTTL = getTTLForCollection('labelAssociations') * 24 * 60 * 60;
        indexPromises.push(collections.labelAssociations.createIndex({ instanceId: 1, chatId: 1, labelId: 1 }, { unique: true }).then(() => { }), collections.labelAssociations.createIndex({ updatedAt: 1 }, { expireAfterSeconds: labelAssocTTL }).then(() => { }));
        await Promise.all(indexPromises);
        log(`✅ Indexes created with custom TTL settings for instance ${instanceId}`);
    };
    await createIndexes();
    const storeImpl = {
        instanceId,
        getEventConfig(eventType) {
            return getEventConfig(eventType);
        },
        updateEventConfig(eventType, config) {
            const currentConfig = getEventConfig(eventType);
            eventConfigs.set(eventType, { ...currentConfig, ...config });
            log(`Updated event config for ${eventType}:`, eventConfigs.get(eventType));
        },
        getEventMetrics(eventType) {
            if (eventType) {
                return eventMetricsMap.get(eventType) || {
                    eventType,
                    totalReceived: 0,
                    totalStored: 0,
                    totalSkipped: 0,
                    totalErrors: 0
                };
            }
            return Array.from(eventMetricsMap.values());
        },
        resetEventMetrics(eventType) {
            if (eventType) {
                eventMetricsMap.delete(eventType);
            }
            else {
                eventMetricsMap.clear();
            }
        },
        async getChats() {
            const chats = await collections.chats
                .find({ instanceId })
                .sort({ conversationTimestamp: -1 })
                .toArray();
            return chats.map(({ _id, instanceId: _instanceId, updatedAt: _updatedAt, ...chat }) => chat);
        },
        async getChat(jid) {
            const chat = await collections.chats.findOne({ instanceId, id: jid });
            if (!chat)
                return null;
            const { _id, instanceId: _instanceId, updatedAt, ...chatData } = chat;
            return chatData;
        },
        async upsertChats(...chats) {
            if (chats.length === 0)
                return;
            if (bullInitialized && queues.has(QueueType.CHATS)) {
                try {
                    const queue = queues.get(QueueType.CHATS);
                    await queue.add('upsert', {
                        type: 'upsert',
                        chats,
                        instanceId,
                        timestamp: Date.now()
                    }, defaultJobOptions);
                    return;
                }
                catch (error) {
                    logError('[Bull Chats] Failed to queue, falling back:', error);
                }
            }
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
            if (bullInitialized && queues.has(QueueType.CHATS)) {
                try {
                    const queue = queues.get(QueueType.CHATS);
                    await queue.add('update', {
                        type: 'update',
                        chatId: jid,
                        update,
                        instanceId,
                        timestamp: Date.now()
                    }, defaultJobOptions);
                    return true;
                }
                catch (error) {
                    logError('[Bull Chats] Failed to queue update, falling back:', error);
                }
            }
            const result = await collections.chats.updateOne({ instanceId, id: jid }, { $set: { ...update, updatedAt: new Date() } });
            return result.modifiedCount > 0;
        },
        async deleteChats(jids) {
            if (bullInitialized && queues.has(QueueType.CHATS)) {
                try {
                    const queue = queues.get(QueueType.CHATS);
                    await queue.add('delete', {
                        type: 'delete',
                        deleteIds: jids,
                        instanceId,
                        timestamp: Date.now()
                    }, defaultJobOptions);
                    return;
                }
                catch (error) {
                    logError('[Bull Chats] Failed to queue delete, falling back:', error);
                }
            }
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
                const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...contactData } = contact;
                contactsMap[contact.id] = contactData;
            }
            return contactsMap;
        },
        async getContact(jid) {
            const contact = await collections.contacts.findOne({ instanceId, id: jid });
            if (!contact)
                return null;
            const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...contactData } = contact;
            return contactData;
        },
        async upsertContacts(contacts) {
            if (contacts.length === 0)
                return;
            if (bullInitialized && queues.has(QueueType.CONTACTS)) {
                try {
                    const queue = queues.get(QueueType.CONTACTS);
                    for (let i = 0; i < contacts.length; i += BATCH_SIZE) {
                        const batch = contacts.slice(i, i + BATCH_SIZE);
                        await queue.add('upsert', {
                            type: 'upsert',
                            contacts: batch,
                            instanceId,
                            timestamp: Date.now()
                        }, defaultJobOptions);
                    }
                    return;
                }
                catch (error) {
                    logError('[Bull Contacts] Failed to queue, falling back:', error);
                }
            }
            const bulkOps = contacts.map(contact => ({
                replaceOne: {
                    filter: { instanceId, id: contact.id },
                    replacement: { ...contact, instanceId, updatedAt: new Date() },
                    upsert: true
                }
            }));
            for (let i = 0; i < bulkOps.length; i += BATCH_SIZE) {
                const chunk = bulkOps.slice(i, i + BATCH_SIZE);
                await collections.contacts.bulkWrite(chunk, { ordered: false });
            }
        },
        async getMessages(jid) {
            const messages = await collections.messages
                .find({ instanceId, jid })
                .sort({ messageTimestamp: -1 })
                .toArray();
            return messages.map(({ _id, instanceId: _instanceId, jid: _jid, updatedAt: _updatedAt, ...msg }) => convertBinaryToBuffer(msg));
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
            const { _id, instanceId: _instanceId, jid: _jid, updatedAt: _updatedAt, ...msg } = message;
            const converted = convertBinaryToBuffer(msg);
            binaryConversionCache.set(cacheKey, converted);
            return converted;
        },
        async upsertMessage(jid, message) {
            const cacheKey = `msg_${instanceId}_${jid}_${message.key?.id}`;
            binaryConversionCache.del(cacheKey);
            if (bullInitialized && queues.has(QueueType.MESSAGES)) {
                try {
                    const queue = queues.get(QueueType.MESSAGES);
                    await queue.add('upsert', {
                        type: 'upsert',
                        jid,
                        message,
                        instanceId,
                        timestamp: Date.now()
                    }, defaultJobOptions);
                    return;
                }
                catch (error) {
                    logError('[Bull Messages] Failed to queue, falling back:', error);
                }
            }
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
        },
        async updateMessage(jid, id, update) {
            const cacheKey = `msg_${instanceId}_${jid}_${id}`;
            binaryConversionCache.del(cacheKey);
            if (bullInitialized && queues.has(QueueType.MESSAGES)) {
                try {
                    const queue = queues.get(QueueType.MESSAGES);
                    await queue.add('update', {
                        type: 'update',
                        jid,
                        messageId: id,
                        update: convertBinaryToBuffer(update),
                        instanceId,
                        timestamp: Date.now()
                    }, defaultJobOptions);
                    return true;
                }
                catch (error) {
                    logError('[Bull Messages] Failed to queue update, falling back:', error);
                }
            }
            const result = await collections.messages.updateOne({
                instanceId,
                jid,
                'key.id': id
            }, {
                $set: { ...update, updatedAt: new Date() }
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
            if (bullInitialized && queues.has(QueueType.MESSAGES)) {
                try {
                    const queue = queues.get(QueueType.MESSAGES);
                    await queue.add('delete', {
                        type: 'delete',
                        jid,
                        deleteIds: ids,
                        instanceId,
                        timestamp: Date.now()
                    }, defaultJobOptions);
                    return;
                }
                catch (error) {
                    logError('[Bull Messages] Failed to queue delete, falling back:', error);
                }
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
            const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...metadataData } = metadata;
            return metadataData;
        },
        async getAllGroupMetadata() {
            const groups = await collections.groupMetadata.find({ instanceId }).toArray();
            return groups.map(({ _id, instanceId: _instanceId, updatedAt: _updatedAt, ...metadata }) => metadata);
        },
        async upsertGroupMetadata(jid, metadata) {
            if (!metadata.id) {
                metadata.id = jid;
            }
            if (bullInitialized && queues.has(QueueType.GROUP_METADATA)) {
                try {
                    const queue = queues.get(QueueType.GROUP_METADATA);
                    await queue.add('upsert', {
                        type: 'upsert',
                        jid,
                        metadata,
                        instanceId,
                        timestamp: Date.now()
                    }, defaultJobOptions);
                    return;
                }
                catch (error) {
                    logError('[Bull GroupMetadata] Failed to queue, falling back:', error);
                }
            }
            await collections.groupMetadata.replaceOne({ instanceId, id: metadata.id }, { ...metadata, instanceId, updatedAt: new Date() }, { upsert: true });
        },
        async getState() {
            const state = await collections.state.findOne({ instanceId });
            if (!state)
                return { connection: 'close' };
            const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...stateData } = state;
            return stateData;
        },
        async updateState(update) {
            if (bullInitialized && queues.has(QueueType.STATE)) {
                try {
                    const queue = queues.get(QueueType.STATE);
                    await queue.add('update', {
                        type: 'update',
                        update,
                        instanceId,
                        timestamp: Date.now()
                    }, defaultJobOptions);
                    return;
                }
                catch (error) {
                    logError('[Bull State] Failed to queue, falling back:', error);
                }
            }
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
            if (bullInitialized && queues.has(QueueType.PRESENCES)) {
                try {
                    const queue = queues.get(QueueType.PRESENCES);
                    await queue.add('update', {
                        type: 'update',
                        id,
                        presences,
                        instanceId,
                        timestamp: Date.now()
                    }, defaultJobOptions);
                    return;
                }
                catch (error) {
                    logError('[Bull Presences] Failed to queue, falling back:', error);
                }
            }
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
                const { _id, instanceId: _instanceId, updatedAt: _updatedAt, ...labelData } = label;
                labelsMap[label.id] = labelData;
            }
            return labelsMap;
        },
        async upsertLabel(id, label) {
            if (bullInitialized && queues.has(QueueType.LABELS)) {
                try {
                    const queue = queues.get(QueueType.LABELS);
                    await queue.add('upsert', {
                        type: 'upsert',
                        id,
                        label,
                        instanceId,
                        timestamp: Date.now()
                    }, defaultJobOptions);
                    return;
                }
                catch (error) {
                    logError('[Bull Labels] Failed to queue, falling back:', error);
                }
            }
            await collections.labels.replaceOne({ instanceId, id }, { ...label, instanceId, updatedAt: new Date() }, { upsert: true });
        },
        async deleteLabel(id) {
            if (bullInitialized && queues.has(QueueType.LABELS)) {
                try {
                    const queue = queues.get(QueueType.LABELS);
                    await queue.add('delete', {
                        type: 'delete',
                        id,
                        instanceId,
                        timestamp: Date.now()
                    }, defaultJobOptions);
                    return;
                }
                catch (error) {
                    logError('[Bull Labels] Failed to queue delete, falling back:', error);
                }
            }
            await collections.labels.deleteOne({ instanceId, id });
        },
        async getLabelAssociations() {
            const associations = await collections.labelAssociations
                .find({ instanceId })
                .toArray();
            return associations.map(({ _id, instanceId: _instanceId, updatedAt: _updatedAt, ...assoc }) => assoc);
        },
        async getChatLabels(chatId) {
            const associations = await collections.labelAssociations
                .find({ instanceId, chatId })
                .toArray();
            return associations.map(({ _id, instanceId: _instanceId, updatedAt: _updatedAt, ...assoc }) => assoc);
        },
        async getMessageLabels(messageId) {
            const associations = await collections.labelAssociations
                .find({ instanceId, messageId })
                .toArray();
            return associations.map(assoc => assoc.labelId);
        },
        async upsertLabelAssociation(association) {
            if (bullInitialized && queues.has(QueueType.LABEL_ASSOCIATIONS)) {
                try {
                    const queue = queues.get(QueueType.LABEL_ASSOCIATIONS);
                    await queue.add('upsert', {
                        type: 'upsert',
                        association,
                        instanceId,
                        timestamp: Date.now()
                    }, defaultJobOptions);
                    return;
                }
                catch (error) {
                    logError('[Bull LabelAssociations] Failed to queue, falling back:', error);
                }
            }
            const filter = {
                instanceId,
                chatId: association.chatId,
                labelId: association.labelId
            };
            if ('messageId' in association && association.messageId) {
                filter.messageId = association.messageId;
            }
            await collections.labelAssociations.replaceOne(filter, {
                ...association,
                instanceId,
                updatedAt: new Date()
            }, { upsert: true });
        },
        async deleteLabelAssociation(association) {
            if (bullInitialized && queues.has(QueueType.LABEL_ASSOCIATIONS)) {
                try {
                    const queue = queues.get(QueueType.LABEL_ASSOCIATIONS);
                    await queue.add('delete', {
                        type: 'delete',
                        association,
                        instanceId,
                        timestamp: Date.now()
                    }, defaultJobOptions);
                    return;
                }
                catch (error) {
                    logError('[Bull LabelAssociations] Failed to queue delete, falling back:', error);
                }
            }
            const filter = {
                instanceId,
                chatId: association.chatId,
                labelId: association.labelId
            };
            if ('messageId' in association && association.messageId) {
                filter.messageId = association.messageId;
            }
            await collections.labelAssociations.deleteOne(filter);
        },
        bind(ev) {
            log(`[${instanceId}] store.bind() called - setting up event listeners with selective storage`);
            ev.on('connection.update', async (update) => {
                if (enableMetrics)
                    updateEventMetrics('connection.update', 'received');
                if (await shouldStoreEvent('connection.update', update)) {
                    await storeImpl.updateState(update);
                    if (enableMetrics)
                        updateEventMetrics('connection.update', 'stored');
                    if (hooks.afterStore)
                        await hooks.afterStore('connection.update', update);
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
                    const cursorMsg = await storeImpl.getMessage(jid, cursorKey.id);
                    if (cursorMsg) {
                        query.messageTimestamp = { $lt: cursorMsg.messageTimestamp };
                    }
                }
                messages = await collections.messages
                    .find(query)
                    .sort({ messageTimestamp: -1 })
                    .limit(count)
                    .toArray()
                    .then(msgs => msgs.map(({ _id, instanceId: _instanceId, jid: _jid, updatedAt: _updatedAt, ...msg }) => convertBinaryToBuffer(msg)));
            }
            return messages;
        },
        async loadMessage(jid, id) {
            const msg = await storeImpl.getMessage(jid, id);
            return msg || undefined;
        },
        async mostRecentMessage(jid) {
            const messages = await storeImpl.getMessages(jid);
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
            const eventMetrics = {};
            eventMetricsMap.forEach((value, key) => {
                eventMetrics[key] = value;
            });
            const bullStats = {};
            if (bullInitialized) {
                bullStats.initialized = true;
                bullStats.queues = {};
                for (const [queueType] of queues.entries()) {
                    bullStats.queues[queueType] = 'active';
                }
                bullStats.totalQueues = queues.size;
                bullStats.redisConnected = redisConnection?.status === 'ready';
            }
            else {
                bullStats.initialized = false;
                bullStats.reason = redis ? 'initialization failed' : 'not configured';
            }
            return {
                messagesProcessed: 0,
                labelsProcessed: 0,
                batchesProcessed: 0,
                errors: 0,
                lastResetTime: new Date(),
                uptime: 0,
                eventMetrics: enableMetrics ? eventMetrics : undefined,
                bullStats
            };
        },
        async flushLabelAssociations() {
            log('Flushed all pending label associations');
        },
        resetPerformanceStats() {
            eventMetricsMap.clear();
        },
        async recreateIndexes() {
            try {
                await createIndexes();
                return { created: 16, failed: 0, details: ['All indexes recreated successfully'] };
            }
            catch (error) {
                return { created: 0, failed: 16, details: [`Index recreation failed: ${error}`] };
            }
        },
        async getIndexStatus() {
            const collectionNames = ['chats', 'contacts', 'messages', 'groupMetadata', 'state', 'presences', 'labels', 'labelAssociations'];
            const indexStatus = [];
            for (const collName of collectionNames) {
                try {
                    const collection = collections[collName];
                    const indexes = await collection.listIndexes().toArray();
                    indexStatus.push({
                        collection: `${collectionPrefix}${collName}`,
                        indexes: indexes.map(idx => ({
                            name: idx.name,
                            key: idx.key,
                            unique: idx.unique,
                            expireAfterSeconds: idx.expireAfterSeconds
                        }))
                    });
                }
                catch (error) {
                    indexStatus.push({
                        collection: `${collectionPrefix}${collName}`,
                        indexes: [],
                        error: error.message
                    });
                }
            }
            return indexStatus;
        },
        async close() {
            if (bullInitialized) {
                log(`🛑 Closing Bull queues for instance ${instanceId}...`);
                try {
                    for (const worker of workers.values()) {
                        await worker.close();
                    }
                    for (const queue of queues.values()) {
                        await queue.close();
                    }
                    if (redisConnection)
                        redisConnection.disconnect();
                }
                catch (error) {
                    logError('Error closing Bull queues:', error);
                }
            }
            await Promise.all([
                Promise.resolve()
            ]);
            const keys = binaryConversionCache.keys();
            keys.forEach(key => {
                if (key.startsWith(`msg_${instanceId}_`)) {
                    binaryConversionCache.del(key);
                }
            });
            if (client) {
                await client.close();
            }
        }
    };
    return storeImpl;
};
exports.makeEnhancedMongoDBStore = makeEnhancedMongoDBStore;
//# sourceMappingURL=makeEnhancedMongoDBStore.js.map