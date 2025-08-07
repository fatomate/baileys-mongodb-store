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
const bullmq_1 = require("bullmq");
const ioredis_1 = __importDefault(require("ioredis"));
const DEFAULT_TTL_DAYS = 30;
let activeConnections = [];
const binaryConversionCache = new node_cache_1.default({ stdTTL: 300, checkperiod: 60 });
const QUEUE_CONCURRENCY = 50;
const BATCH_SIZE = 100;
const BATCH_DELAY = 50;
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
        return obj;
    }
};
const makeMongoDBStore = async (config) => {
    const { uri, database: dbName, instanceId, ttlDays = DEFAULT_TTL_DAYS, collectionPrefix = 'baileys_', redis, logLevel = 'none' } = config;
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
    let client;
    let db;
    let isConnected = false;
    let isConnecting = false;
    let connectionError = null;
    let reconnectAttempts = 0;
    const MAX_RECONNECT_ATTEMPTS = 5;
    const RECONNECT_DELAY_BASE = 1000;
    const queues = new Map();
    const workers = new Map();
    let redisConnection = null;
    let bullInitialized = false;
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
    let healthCheckInterval = null;
    let lastHealthCheck = Date.now();
    const HEALTH_CHECK_INTERVAL = 30000;
    const QUEUE_STALE_THRESHOLD = 60000;
    const initializeConnection = async () => {
        try {
            client = new mongodb_1.MongoClient(uri, {
                maxPoolSize: 100,
                minPoolSize: 10,
                maxIdleTimeMS: 30000,
                writeConcern: { w: 1, j: false }
            });
            await client.connect();
            db = client.db(dbName);
            isConnected = true;
            isConnecting = false;
            connectionError = null;
            reconnectAttempts = 0;
            activeConnections = activeConnections.filter(c => c.instanceId !== instanceId);
            activeConnections.push({
                client,
                database: dbName,
                instanceId,
                collectionPrefix
            });
            log(`MongoDB connected successfully for instance ${instanceId}`);
        }
        catch (error) {
            isConnected = false;
            isConnecting = false;
            connectionError = error;
            logError(`MongoDB connection failed for instance ${instanceId}:`, error);
            throw error;
        }
    };
    const ensureConnection = async () => {
        if (isConnected && client) {
            try {
                await client.db(dbName).command({ ping: 1 });
                return;
            }
            catch {
                isConnected = false;
            }
        }
        if (isConnecting) {
            let waitAttempts = 0;
            while (isConnecting && waitAttempts < 50) {
                await new Promise(resolve => setTimeout(resolve, 100));
                waitAttempts++;
            }
            if (isConnected)
                return;
        }
        if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
            throw new Error(`Failed to reconnect after ${MAX_RECONNECT_ATTEMPTS} attempts: ${connectionError?.message}`);
        }
        isConnecting = true;
        const delay = RECONNECT_DELAY_BASE * Math.pow(2, Math.min(reconnectAttempts, 5));
        if (reconnectAttempts > 0) {
            log(`Attempting to reconnect (attempt ${reconnectAttempts + 1}/${MAX_RECONNECT_ATTEMPTS}) after ${delay}ms...`);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
        reconnectAttempts++;
        try {
            await initializeConnection();
        }
        catch (error) {
            isConnecting = false;
            throw error;
        }
    };
    await initializeConnection();
    const pMessageQueue = new p_queue_1.default({ concurrency: QUEUE_CONCURRENCY });
    const pLabelQueue = new p_queue_1.default({ concurrency: QUEUE_CONCURRENCY });
    const generalQueue = new p_queue_1.default({ concurrency: QUEUE_CONCURRENCY });
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
                const config = await redisConnection.config('GET', 'maxmemory-policy');
                const policy = config[1];
                if (policy && policy !== 'noeviction') {
                    logWarn(`⚠️  Redis eviction policy is '${policy}'. Consider using 'noeviction' for BullMQ or a separate Redis instance.`);
                    logWarn(`   Current settings will work but jobs may be lost if Redis memory fills up.`);
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
                    performanceMetrics.errors++;
                });
                worker.on('stalled', (jobId) => {
                    logWarn(`⚠️ ${queueType} job ${jobId} stalled`);
                });
                workers.set(queueType, worker);
                queue.obliterate({ force: true }).catch(() => { });
            };
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
                    const result = await collections.labelAssociations.deleteOne(filter);
                    if (result.deletedCount === 0) {
                        logWarn(`[Bull Label] Warning: No document found to delete - chatId: ${association.chatId}, labelId: ${association.labelId}, messageId: ${association.messageId || 'none'}`);
                    }
                }
                performanceMetrics.labelsProcessed++;
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
                    performanceMetrics.messagesProcessed++;
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
                console.log(`[${instanceId}] Bull worker processing job ${job.id} for group metadata`);
                if (type === 'upsert') {
                    console.log(`[${instanceId}] Processing upsert for group ${metadata.id}`);
                    config.logger?.debug({ instanceId, groupId: metadata.id }, 'Processing group metadata upsert job');
                    try {
                        const result = await collections.groupMetadata.replaceOne({ instanceId, id: metadata.id }, { ...metadata, instanceId, updatedAt: new Date() }, { upsert: true });
                        console.log(`[${instanceId}] Bull worker result for group ${metadata.id}: upserted=${result.upsertedCount}, modified=${result.modifiedCount}`);
                        config.logger?.info({ instanceId, groupId: metadata.id, upserted: result.upsertedCount, modified: result.modifiedCount }, 'Group metadata processed by queue');
                    }
                    catch (error) {
                        console.error(`[${instanceId}] Bull worker failed for group ${metadata.id}:`, error);
                        throw error;
                    }
                }
                else if (type === 'update' && update) {
                    console.log(`[${instanceId}] Processing update for group ${jid}`);
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
            const checkQueuesHealth = async () => {
                try {
                    if (redisConnection?.status !== 'ready') {
                        logError('⚠️ Redis connection lost, attempting to reconnect...');
                        await restartQueues();
                        return;
                    }
                    for (const [queueType, queue] of queues.entries()) {
                        try {
                            const counts = await queue.getJobCounts();
                            const worker = workers.get(queueType);
                            if (counts.waiting > 100) {
                                logWarn(`⚠️ ${queueType} queue has ${counts.waiting} waiting jobs`);
                            }
                            if (worker && !worker.isRunning()) {
                                logError(`❌ ${queueType} worker stopped, restarting...`);
                                await worker.run();
                            }
                            const activeJobs = await queue.getActive();
                            const now = Date.now();
                            for (const job of activeJobs) {
                                const processingTime = now - job.processedOn;
                                if (processingTime > QUEUE_STALE_THRESHOLD) {
                                    logWarn(`⚠️ Stale job detected in ${queueType}: ${job.id} (${processingTime}ms)`);
                                    await job.moveToFailed(new Error('Job stale, moving to failed'), false);
                                }
                            }
                        }
                        catch (error) {
                            logError(`Health check failed for ${queueType}:`, error);
                        }
                    }
                    lastHealthCheck = Date.now();
                }
                catch (error) {
                    logError('Queue health check error:', error);
                    if (Date.now() - lastHealthCheck > QUEUE_STALE_THRESHOLD * 2) {
                        await restartQueues();
                    }
                }
            };
            const restartQueues = async () => {
                log('🔄 Restarting Bull queues...');
                try {
                    for (const worker of workers.values()) {
                        await worker.close();
                    }
                    for (const queue of queues.values()) {
                        await queue.close();
                    }
                    workers.clear();
                    queues.clear();
                    if (redisConnection && redisConnection.status !== 'ready') {
                        redisConnection.disconnect();
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
                    }
                    await initializeBullQueues();
                    log('✅ Queues restarted successfully');
                }
                catch (error) {
                    logError('❌ Failed to restart queues:', error);
                    bullInitialized = false;
                }
            };
            healthCheckInterval = setInterval(checkQueuesHealth, HEALTH_CHECK_INTERVAL);
            setInterval(async () => {
                for (const [, queue] of queues.entries()) {
                    try {
                        await queue.clean(10000, 1000, 'completed');
                        await queue.clean(10000, 1000, 'failed');
                    }
                    catch (error) {
                    }
                }
            }, 60000);
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
    const labelAssociationBatch = {
        items: [],
        timer: null,
        processing: false,
        totalReceived: 0,
        totalProcessed: 0,
        pendingPromises: []
    };
    const messageBatch = {
        items: [],
        timer: null,
        processing: false
    };
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
    let collections = getCollections();
    const withConnection = async (operation) => {
        try {
            await ensureConnection();
            collections = getCollections();
            return await operation();
        }
        catch (error) {
            if (error.message?.includes('Client must be connected') ||
                error.message?.includes('Topology is closed') ||
                error.code === 'ECONNREFUSED') {
                log(`Connection error detected for instance ${instanceId}, attempting reconnection...`);
                isConnected = false;
                await ensureConnection();
                collections = getCollections();
                return await operation();
            }
            throw error;
        }
    };
    const processBatchedLabelAssociations = async () => {
        if (labelAssociationBatch.processing) {
            log(`[Label Batch] Skipping - already processing`);
            return;
        }
        if (labelAssociationBatch.items.length === 0) {
            return;
        }
        labelAssociationBatch.processing = true;
        if (labelAssociationBatch.timer) {
            clearTimeout(labelAssociationBatch.timer);
            labelAssociationBatch.timer = null;
        }
        const itemsToProcess = labelAssociationBatch.items.splice(0);
        const pendingPromises = labelAssociationBatch.pendingPromises?.splice(0, itemsToProcess.length) || [];
        log(`[Label Batch] Processing ${itemsToProcess.length} label associations`);
        if (itemsToProcess.length > BATCH_SIZE * 10) {
            logWarn(`Batch size exceeded for instance ${instanceId} (${itemsToProcess.length} items), processing first ${BATCH_SIZE * 10} items`);
            itemsToProcess.splice(BATCH_SIZE * 10);
        }
        try {
            const bulkOps = itemsToProcess.map(association => {
                const filter = {
                    instanceId,
                    chatId: association.chatId,
                    labelId: association.labelId
                };
                if ('messageId' in association && association.messageId) {
                    filter.messageId = association.messageId;
                }
                return {
                    replaceOne: {
                        filter,
                        replacement: {
                            ...association,
                            instanceId,
                            updatedAt: new Date()
                        },
                        upsert: true
                    }
                };
            });
            for (let i = 0; i < bulkOps.length; i += BATCH_SIZE) {
                const chunk = bulkOps.slice(i, i + BATCH_SIZE);
                await withConnection(() => collections.labelAssociations.bulkWrite(chunk, { ordered: false }));
                if (i + BATCH_SIZE < bulkOps.length) {
                    await new Promise(resolve => setTimeout(resolve, BATCH_DELAY));
                }
            }
            performanceMetrics.labelsProcessed += itemsToProcess.length;
            performanceMetrics.batchesProcessed++;
            labelAssociationBatch.totalProcessed = (labelAssociationBatch.totalProcessed || 0) + itemsToProcess.length;
            pendingPromises.forEach(p => p.resolve());
            console.log(`[Label Batch] Successfully processed ${itemsToProcess.length} label associations (total processed: ${labelAssociationBatch.totalProcessed}/${labelAssociationBatch.totalReceived})`);
        }
        catch (error) {
            console.error('Error processing label associations batch:', error);
            performanceMetrics.errors++;
            pendingPromises.forEach(p => p.reject(error));
            labelAssociationBatch.items.unshift(...itemsToProcess);
            console.log(`[Label Batch] Re-queued ${itemsToProcess.length} items after error`);
        }
        finally {
            labelAssociationBatch.processing = false;
            if (labelAssociationBatch.items.length > 0) {
                console.log(`[Label Batch] ${labelAssociationBatch.items.length} new items accumulated, scheduling next batch`);
                scheduleLabelBatch();
            }
        }
    };
    const processBatchedMessages = async () => {
        if (messageBatch.processing || messageBatch.items.length === 0)
            return;
        messageBatch.processing = true;
        const itemsToProcess = [...messageBatch.items];
        messageBatch.items = [];
        if (itemsToProcess.length > BATCH_SIZE * 10) {
            console.warn(`Message batch size exceeded for instance ${instanceId} (${itemsToProcess.length} items), processing first ${BATCH_SIZE * 10} items`);
            itemsToProcess.splice(BATCH_SIZE * 10);
        }
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
                await withConnection(() => collections.messages.bulkWrite(chunk, { ordered: false }));
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
        if (labelAssociationBatch.processing) {
            return;
        }
        if (labelAssociationBatch.timer) {
            clearTimeout(labelAssociationBatch.timer);
        }
        labelAssociationBatch.timer = setTimeout(() => {
            labelAssociationBatch.timer = null;
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
        const criticalIndexes = [
            { collection: 'chats', spec: { instanceId: 1, id: 1 }, options: { unique: true }, name: 'chats_primary' },
            { collection: 'contacts', spec: { instanceId: 1, id: 1 }, options: { unique: true }, name: 'contacts_primary' },
            { collection: 'messages', spec: { instanceId: 1, jid: 1, 'key.id': 1 }, options: { unique: true }, name: 'messages_primary' },
            { collection: 'messages', spec: { instanceId: 1, jid: 1, messageTimestamp: -1 }, options: {}, name: 'messages_query' },
            { collection: 'groupMetadata', spec: { instanceId: 1, id: 1 }, options: { unique: true }, name: 'groups_primary' },
            { collection: 'state', spec: { instanceId: 1 }, options: { unique: true }, name: 'state_primary' },
            { collection: 'presences', spec: { instanceId: 1, id: 1 }, options: { unique: true }, name: 'presences_primary' },
            { collection: 'labels', spec: { instanceId: 1, id: 1 }, options: { unique: true }, name: 'labels_primary' },
            { collection: 'labelAssociations', spec: { instanceId: 1, chatId: 1, labelId: 1 }, options: { unique: true }, name: 'label_assoc_primary' }
        ];
        const optimizationIndexes = [
            { collection: 'labelAssociations', spec: { instanceId: 1, chatId: 1 }, options: {}, name: 'label_assoc_chat' },
            { collection: 'labelAssociations', spec: { instanceId: 1, messageId: 1 }, options: {}, name: 'label_assoc_message' }
        ];
        const ttlIndexes = [
            { collection: 'chats', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds }, name: 'chats_ttl' },
            { collection: 'contacts', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds }, name: 'contacts_ttl' },
            { collection: 'messages', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds }, name: 'messages_ttl' },
            { collection: 'groupMetadata', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds }, name: 'groups_ttl' },
            { collection: 'presences', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds }, name: 'presences_ttl' },
            { collection: 'labels', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds }, name: 'labels_ttl' },
            { collection: 'labelAssociations', spec: { updatedAt: 1 }, options: { expireAfterSeconds: ttlSeconds }, name: 'label_assoc_ttl' }
        ];
        const createIndexWithRetry = async (indexDef, maxRetries = 3) => {
            const { collection, spec, options, name } = indexDef;
            for (let attempt = 1; attempt <= maxRetries; attempt++) {
                try {
                    await withConnection(() => collections[collection].createIndex(spec, options));
                    console.log(`✅ Index created: ${name} (attempt ${attempt})`);
                    return { success: true };
                }
                catch (error) {
                    const delay = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
                    console.warn(`❌ Index creation failed: ${name} (attempt ${attempt}/${maxRetries}):`, error);
                    if (attempt < maxRetries) {
                        console.log(`⏳ Retrying ${name} in ${delay}ms...`);
                        await new Promise(resolve => setTimeout(resolve, delay));
                    }
                    else {
                        return { success: false, error: error };
                    }
                }
            }
            return { success: false };
        };
        console.log(`🔧 Creating critical indexes for instance ${instanceId}...`);
        const criticalResults = await Promise.allSettled(criticalIndexes.map(idx => createIndexWithRetry(idx, 5)));
        const failedCritical = criticalResults
            .map((result, i) => ({ result, index: criticalIndexes[i] }))
            .filter(({ result }) => result.status === 'rejected' || (result.status === 'fulfilled' && !result.value.success));
        if (failedCritical.length > 0) {
            const errorDetails = failedCritical.map(({ index }) => index.name).join(', ');
            throw new Error(`Critical indexes failed to create: ${errorDetails}. Query performance will be severely impacted. Please check MongoDB permissions and server status.`);
        }
        console.log(`⚡ Creating optimization indexes for instance ${instanceId}...`);
        const optimizationResults = await Promise.allSettled(optimizationIndexes.map(idx => createIndexWithRetry(idx, 2)));
        const failedOptimization = optimizationResults
            .map((result, i) => ({ result, index: optimizationIndexes[i] }))
            .filter(({ result }) => result.status === 'rejected' || (result.status === 'fulfilled' && !result.value.success));
        if (failedOptimization.length > 0) {
            console.warn(`⚠️  Some optimization indexes failed: ${failedOptimization.map(({ index }) => index.name).join(', ')}`);
        }
        log(`🗑️  Creating TTL indexes for instance ${instanceId}...`);
        const ttlResults = await Promise.allSettled(ttlIndexes.map(idx => createIndexWithRetry(idx, 2)));
        const failedTTL = ttlResults
            .map((result, i) => ({ result, index: ttlIndexes[i] }))
            .filter(({ result }) => result.status === 'rejected' || (result.status === 'fulfilled' && !result.value.success));
        if (failedTTL.length > 0) {
            console.warn(`⚠️  Some TTL indexes failed: ${failedTTL.map(({ index }) => index.name).join(', ')} - automatic data cleanup may not work`);
        }
        const totalCreated = criticalIndexes.length + optimizationIndexes.length + ttlIndexes.length - failedOptimization.length - failedTTL.length;
        console.log(`✅ Index creation completed for instance ${instanceId}: ${totalCreated}/${criticalIndexes.length + optimizationIndexes.length + ttlIndexes.length} indexes created`);
    };
    const checkAndFixLabelAssociationsIndex = async () => {
        try {
            const existingIndexes = await collections.labelAssociations.indexes();
            const oldIndex = existingIndexes.find(idx => {
                const keys = Object.keys(idx.key || {});
                return keys.includes('instanceId') &&
                    keys.includes('chatId') &&
                    keys.includes('labelId') &&
                    keys.includes('messageId') &&
                    keys.length === 4;
            });
            if (oldIndex) {
                console.log(`🔧 Found old labelAssociations index with messageId field, updating...`);
                const indexName = oldIndex.name || 'instanceId_1_chatId_1_labelId_1_messageId_1';
                try {
                    await collections.labelAssociations.dropIndex(indexName);
                    console.log(`✅ Dropped old index: ${indexName}`);
                }
                catch (dropError) {
                    console.warn(`⚠️  Failed to drop old index ${indexName}:`, dropError);
                }
            }
        }
        catch (error) {
            console.warn('⚠️  Could not check existing labelAssociations indexes:', error);
        }
    };
    await checkAndFixLabelAssociationsIndex();
    await createIndexes();
    const createStoreProxy = (target) => {
        return new Proxy(target, {
            get(obj, prop) {
                const value = obj[prop];
                if (typeof value === 'function' && prop !== 'bind' && prop !== 'close') {
                    return async (...args) => {
                        const methodsWithConnection = new Set(['getChats', 'getChat', 'updateState']);
                        if (methodsWithConnection.has(prop)) {
                            return value.apply(obj, args);
                        }
                        return withConnection(() => value.apply(obj, args));
                    };
                }
                return value;
            }
        });
    };
    const storeImpl = {
        instanceId,
        async getChats() {
            return withConnection(async () => {
                const chats = await collections.chats
                    .find({ instanceId })
                    .sort({ conversationTimestamp: -1 })
                    .toArray();
                return chats.map(({ _id, instanceId: _instanceId, updatedAt: _updatedAt, ...chat }) => chat);
            });
        },
        async getChat(jid) {
            return withConnection(async () => {
                const chat = await collections.chats.findOne({ instanceId, id: jid });
                if (!chat)
                    return null;
                const { _id, instanceId: _instanceId, updatedAt, ...chatData } = chat;
                return chatData;
            });
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
            const result = await collections.chats.updateOne({ instanceId, id: jid }, {
                $set: { ...update, updatedAt: new Date() }
            });
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
        async upsertMessage(jid, message, useBatch = false) {
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
                    const cacheKey = `msg_${instanceId}_${jid}_${message.key?.id}`;
                    binaryConversionCache.del(cacheKey);
                    return;
                }
                catch (error) {
                    logError('[Bull Messages] Failed to queue, falling back:', error);
                }
            }
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
                await pMessageQueue.add(async () => {
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
                config.logger?.error({ instanceId, jid, metadata }, 'GroupMetadata missing id field');
                throw new Error(`GroupMetadata missing id field for jid: ${jid}`);
            }
            console.log(`[${instanceId}] upsertGroupMetadata called for group ${metadata.id}`);
            console.log(`[${instanceId}] Bull initialized: ${bullInitialized}, Has queue: ${queues.has(QueueType.GROUP_METADATA)}`);
            config.logger?.debug({ instanceId, groupId: metadata.id, jid }, 'Upserting group metadata');
            if (bullInitialized && queues.has(QueueType.GROUP_METADATA)) {
                try {
                    console.log(`[${instanceId}] Using Bull queue for group ${metadata.id}`);
                    const queue = queues.get(QueueType.GROUP_METADATA);
                    const job = await queue.add('upsert', {
                        type: 'upsert',
                        jid,
                        metadata,
                        instanceId,
                        timestamp: Date.now()
                    }, defaultJobOptions);
                    console.log(`[${instanceId}] Job queued with ID: ${job.id} for group ${metadata.id}`);
                    config.logger?.debug({ instanceId, groupId: metadata.id }, 'Group metadata queued for processing');
                    return;
                }
                catch (error) {
                    console.error(`[${instanceId}] Bull queue failed for group ${metadata.id}:`, error);
                    logError('[Bull GroupMetadata] Failed to queue, falling back:', error);
                }
            }
            console.log(`[${instanceId}] Using direct MongoDB write for group ${metadata.id}`);
            try {
                const result = await collections.groupMetadata.replaceOne({ instanceId, id: metadata.id }, { ...metadata, instanceId, updatedAt: new Date() }, { upsert: true });
                console.log(`[${instanceId}] Direct write result for group ${metadata.id}: upserted=${result.upsertedCount}, modified=${result.modifiedCount}`);
                config.logger?.info({ instanceId, groupId: metadata.id, upserted: result.upsertedCount, modified: result.modifiedCount }, 'Group metadata saved directly to MongoDB');
            }
            catch (error) {
                console.error(`[${instanceId}] Direct MongoDB write failed for group ${metadata.id}:`, error);
                throw error;
            }
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
            await withConnection(() => collections.state.updateOne({ instanceId }, {
                $set: { ...update, instanceId, updatedAt: new Date() }
            }, { upsert: true }));
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
                    const job = await queue.add('upsert', {
                        type: 'upsert',
                        association,
                        instanceId,
                        timestamp: Date.now()
                    }, defaultJobOptions);
                    labelAssociationBatch.totalReceived = (labelAssociationBatch.totalReceived || 0) + 1;
                    log(`[Bull Label] Job ${job.id} queued - chatId: ${association.chatId}, labelId: ${association.labelId}`);
                    return;
                }
                catch (error) {
                    logError('[Bull Label] Failed to queue job, falling back to in-memory:', error);
                }
            }
            return new Promise((resolve, reject) => {
                labelAssociationBatch.totalReceived = (labelAssociationBatch.totalReceived || 0) + 1;
                labelAssociationBatch.items.push(association);
                labelAssociationBatch.pendingPromises?.push({ resolve, reject });
                const currentBatchSize = labelAssociationBatch.items.length;
                const totalReceived = labelAssociationBatch.totalReceived;
                const totalProcessed = labelAssociationBatch.totalProcessed || 0;
                log(`[Label Association] #${totalReceived} Added to batch (queue: ${currentBatchSize}, received: ${totalReceived}, processed: ${totalProcessed}) - chatId: ${association.chatId}, labelId: ${association.labelId}, messageId: ${association.messageId || 'none'}`);
                if (currentBatchSize >= BATCH_SIZE) {
                    log(`[Label Association] Batch full (${currentBatchSize}/${BATCH_SIZE}), processing immediately`);
                    if (labelAssociationBatch.timer) {
                        clearTimeout(labelAssociationBatch.timer);
                        labelAssociationBatch.timer = null;
                    }
                    processBatchedLabelAssociations().catch(error => {
                        logError('[Label Association] Error in batch processing:', error);
                    });
                }
                else {
                    scheduleLabelBatch();
                }
            });
        },
        async deleteLabelAssociation(association) {
            if (bullInitialized && queues.has(QueueType.LABEL_ASSOCIATIONS)) {
                try {
                    const queue = queues.get(QueueType.LABEL_ASSOCIATIONS);
                    const job = await queue.add('delete', {
                        type: 'delete',
                        association,
                        instanceId,
                        timestamp: Date.now()
                    }, defaultJobOptions);
                    log(`[Bull Label] Delete job ${job.id} queued - chatId: ${association.chatId}, labelId: ${association.labelId}`);
                    return;
                }
                catch (error) {
                    logError('[Bull Label] Failed to queue delete job, falling back to direct deletion:', error);
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
            const result = await collections.labelAssociations.deleteOne(filter);
            if (result.deletedCount === 0) {
                logWarn(`[Direct Delete] Warning: No label association found to delete - chatId: ${association.chatId}, labelId: ${association.labelId}, messageId: ${association.messageId || 'none'}`);
            }
        },
        bind(ev) {
            console.log(`[${instanceId}] store.bind() called - setting up event listeners`);
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
                    const deleteResult = await collections.labelAssociations.deleteMany({
                        instanceId,
                        labelId: label.id
                    });
                    if (deleteResult.deletedCount > 0) {
                        log(`Deleted ${deleteResult.deletedCount} label associations for deleted label ${label.id}`);
                    }
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
                const stats = store.getPerformanceStats();
                if (stats.labelStats && stats.labelStats.totalReceived % 50 === 0 && stats.labelStats.totalReceived > 0) {
                    log(`[Label Event] Periodic status - received: ${stats.labelStats.totalReceived}, processed: ${stats.labelStats.totalProcessed}, queued: ${stats.labelStats.currentQueueSize}`);
                    if (stats.labelStats.currentQueueSize > BATCH_SIZE * 2) {
                        log('[Label Event] Queue backlog detected, forcing flush');
                        store.flushLabelAssociations().catch(err => logError('[Label Event] Flush error:', err));
                    }
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
                console.log(`[${instanceId}] groups.update event received with ${updates.length} updates`);
                config.logger?.info({ instanceId, count: updates.length }, 'Processing groups.update event');
                for (const update of updates) {
                    if (update.participants && Array.isArray(update.participants)) {
                        config.logger?.debug({ instanceId, groupId: update.id }, 'Saving complete group metadata from groups.update');
                        await store.upsertGroupMetadata(update.id, update);
                    }
                    else {
                        const existingMetadata = await store.getGroupMetadata(update.id);
                        if (existingMetadata) {
                            config.logger?.debug({ instanceId, groupId: update.id }, 'Merging group update with existing metadata');
                            Object.assign(existingMetadata, update);
                            await store.upsertGroupMetadata(update.id, existingMetadata);
                        }
                        else {
                            config.logger?.debug({ instanceId, groupId: update.id }, 'Creating new group metadata from update');
                            const newMetadata = {
                                id: update.id,
                                subject: update.subject || '',
                                participants: [],
                                ...update
                            };
                            await store.upsertGroupMetadata(update.id, newMetadata);
                        }
                    }
                }
            });
            ev.on('groups.upsert', async (groups) => {
                console.log(`[${instanceId}] groups.upsert event received with ${groups.length} groups`);
                config.logger?.info({ instanceId, count: groups.length }, 'Processing groups.upsert event');
                for (const group of groups) {
                    await store.upsertGroupMetadata(group.id, group);
                    config.logger?.debug({ instanceId, groupId: group.id }, 'Group metadata upserted');
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
                    .then(msgs => msgs.map(({ _id, instanceId: _instanceId, jid: _jid, updatedAt: _updatedAt, ...msg }) => convertBinaryToBuffer(msg)));
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
            const stats = {
                ...performanceMetrics,
                uptime,
                labelStats: {
                    totalReceived: labelAssociationBatch.totalReceived || 0,
                    totalProcessed: labelAssociationBatch.totalProcessed || 0,
                    currentQueueSize: labelAssociationBatch.items.length,
                    isProcessing: labelAssociationBatch.processing
                }
            };
            if (bullInitialized) {
                const queueStats = {};
                for (const [queueType] of queues.entries()) {
                    queueStats[queueType] = 'active';
                }
                stats.bullStats = {
                    initialized: true,
                    queues: queueStats,
                    totalQueues: queues.size,
                    redisConnected: redisConnection?.status === 'ready'
                };
            }
            else {
                stats.bullStats = {
                    initialized: false,
                    reason: redis ? 'initialization failed' : 'not configured'
                };
            }
            return stats;
        },
        async flushLabelAssociations() {
            log(`[Label Flush] Forcing flush of ${labelAssociationBatch.items.length} pending label associations`);
            if (labelAssociationBatch.timer) {
                clearTimeout(labelAssociationBatch.timer);
                labelAssociationBatch.timer = null;
            }
            while (labelAssociationBatch.processing) {
                log('[Label Flush] Waiting for current batch to complete...');
                await new Promise(resolve => setTimeout(resolve, 50));
            }
            while (labelAssociationBatch.items.length > 0) {
                await processBatchedLabelAssociations();
                await new Promise(resolve => setTimeout(resolve, 10));
            }
            log(`[Label Flush] Flush complete. Total processed: ${labelAssociationBatch.totalProcessed}/${labelAssociationBatch.totalReceived}`);
        },
        resetPerformanceStats() {
            performanceMetrics.messagesProcessed = 0;
            performanceMetrics.labelsProcessed = 0;
            performanceMetrics.batchesProcessed = 0;
            performanceMetrics.errors = 0;
            performanceMetrics.lastResetTime = new Date();
        },
        async recreateIndexes() {
            const results = [];
            try {
                await createIndexes();
                const totalIndexes = 18;
                results.push(`Index recreation completed successfully`);
                return { created: totalIndexes, failed: 0, details: results };
            }
            catch (error) {
                results.push(`Index recreation failed: ${error}`);
                throw error;
            }
        },
        async getIndexStatus() {
            return withConnection(async () => {
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
            });
        },
        async close() {
            isConnected = false;
            reconnectAttempts = 0;
            if (store.flushLabelAssociations) {
                await store.flushLabelAssociations();
            }
            else {
                await processBatchedLabelAssociations();
            }
            await processBatchedMessages();
            if (labelAssociationBatch.timer) {
                clearTimeout(labelAssociationBatch.timer);
            }
            if (messageBatch.timer) {
                clearTimeout(messageBatch.timer);
            }
            if (bullInitialized) {
                console.log(`🛑 Closing Bull queues for instance ${instanceId}...`);
                try {
                    if (healthCheckInterval) {
                        clearInterval(healthCheckInterval);
                        healthCheckInterval = null;
                    }
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
                    console.error('Error closing Bull queues:', error);
                }
            }
            await Promise.all([
                pMessageQueue.onIdle(),
                pLabelQueue.onIdle(),
                generalQueue.onIdle()
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
            activeConnections = activeConnections.filter(c => c.instanceId !== instanceId);
        }
    };
    const store = createStoreProxy(storeImpl);
    return store;
};
exports.makeMongoDBStore = makeMongoDBStore;
const cleanupMongoDBStore = async (instanceId, deleteData = false) => {
    if (!instanceId) {
        const connections = [...activeConnections];
        activeConnections = [];
        for (const conn of connections) {
            try {
                if (conn.client) {
                    try {
                        await conn.client.db(conn.database).command({ ping: 1 });
                        await conn.client.close();
                    }
                    catch {
                        try {
                            await conn.client.close();
                        }
                        catch {
                        }
                    }
                }
            }
            catch (error) {
                console.error('Error closing MongoDB connection:', error);
            }
        }
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