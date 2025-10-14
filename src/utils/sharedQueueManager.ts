import { Queue, Worker, QueueEvents, Job, JobsOptions } from 'bullmq'
import Redis from 'ioredis'
import { EventEmitter } from 'events'
import { randomUUID } from 'crypto'

// Queue names
export enum SharedQueueName {
    HIGH_PRIORITY = 'baileys_high_priority_shared',
    DATA_SYNC = 'baileys_data_sync_shared',
    MEDIA = 'baileys_media_shared',
    LOW_PRIORITY = 'baileys_low_priority_shared'
}

// Job types that map to queues
export enum JobType {
    // High priority
    MESSAGES = 'messages',
    
    // Data sync
    CONTACTS = 'contacts',
    CHATS = 'chats',
    GROUP_METADATA = 'group_metadata',
    
    // Media
    PROFILE_PICTURES = 'profile_pictures',
    MEDIA_DOWNLOAD = 'media_download',
    
    // Low priority
    STATE = 'state',
    PRESENCES = 'presences',
    LABELS = 'labels',
    LABEL_ASSOCIATIONS = 'label_associations'
}

// Base job data structure for all shared jobs
export interface SharedJobData {
    instanceId: string
    type: JobType
    data: any
    priority?: number // 0-10, higher = more urgent
    timestamp: number
    requeueCount?: number
    lastError?: string
    ownerId?: string
}

// Queue configuration
interface QueueConfig {
    concurrency: number
    jobTypes: JobType[]
}

// Manager configuration
export interface SharedQueueManagerConfig {
    redis: {
        host?: string
        port?: number
        password?: string
        db?: number
        connection?: Redis | string | any
    }
    queueConcurrency?: {
        highPriority?: number
        dataSync?: number
        media?: number
        lowPriority?: number
    }
    /**
     * Optional identifier to distinguish this worker process.
     * Defaults to a random UUID if not provided.
     */
    workerId?: string
    enableMetrics?: boolean
    logLevel?: 'none' | 'error' | 'warn' | 'info' | 'debug' | 'all'
    missingProcessorHandling?: {
        /**
         * Maximum number of times to re-attempt a job when no processor is registered
         */
        maxAttempts?: number
        /**
         * Base delay (in ms) applied before retrying a job without a processor
         */
        initialDelayMs?: number
        /**
         * Upper bound (in ms) for the backoff delay applied to re-queued jobs
         */
        maxDelayMs?: number
    }
    ownership?: {
        /**
         * Key prefix for ownership tracking entries in Redis.
         * Defaults to `baileys_shared_queue`.
         */
        keyPrefix?: string
        /**
         * TTL (in ms) applied to ownership claims. When the TTL expires,
         * another worker can take over the instance.
         */
        claimTtlMs?: number
        /**
         * Interval (in ms) for refreshing ownership claims.
         * Defaults to half of `claimTtlMs`, clamped to >= 5000ms.
         */
        heartbeatIntervalMs?: number
        /**
         * Delay (in ms) applied when rescheduling jobs because of ownership conflicts.
         */
        conflictDelayMs?: number
    }
}

interface OwnershipConfig {
    keyPrefix: string
    claimTtlMs: number
    heartbeatIntervalMs: number
    conflictDelayMs: number
}

// Metrics tracking
interface QueueMetrics {
    processed: number
    failed: number
    avgProcessingTime: number
    lastProcessedAt?: Date
}

export class SharedQueueManager extends EventEmitter {
    private static instance: SharedQueueManager | null = null
    private redis: Redis
    private queues: Map<SharedQueueName, Queue<SharedJobData>>
    private workers: Map<SharedQueueName, Worker<SharedJobData>>
    private queueEvents: Map<SharedQueueName, QueueEvents>
    private metrics: Map<SharedQueueName, QueueMetrics>
    // Changed to support per-instance processors to fix the singleton overwrite issue
    private instanceProcessors: Map<JobType, Map<string, (job: Job<SharedJobData>) => Promise<any>>>
    private config: SharedQueueManagerConfig
    private isShuttingDown: boolean = false
    private readonly queueConfigs: Map<SharedQueueName, QueueConfig>
    private readonly workerId: string
    private readonly ownershipConfig: OwnershipConfig
    private claimedInstances: Set<string>
    private ownershipHeartbeats: Map<string, NodeJS.Timeout>
    
    private constructor(config: SharedQueueManagerConfig) {
        super()
        this.config = config
        this.queues = new Map()
        this.workers = new Map()
        this.queueEvents = new Map()
        this.metrics = new Map()
        this.instanceProcessors = new Map() // Initialize instance processors map
        this.redis = null as any // Will be initialized in initializeRedis()
        this.workerId = config.workerId || this.safeRandomId()
        this.ownershipConfig = this.buildOwnershipConfig(config.ownership)
        this.claimedInstances = new Set()
        this.ownershipHeartbeats = new Map()
        
        // Define queue configurations
        this.queueConfigs = new Map([
            [SharedQueueName.HIGH_PRIORITY, {
                concurrency: config.queueConcurrency?.highPriority || 400,
                jobTypes: [JobType.MESSAGES]
            }],
            [SharedQueueName.DATA_SYNC, {
                concurrency: config.queueConcurrency?.dataSync || 200,
                jobTypes: [JobType.CONTACTS, JobType.CHATS, JobType.GROUP_METADATA]
            }],
            [SharedQueueName.MEDIA, {
                concurrency: config.queueConcurrency?.media || 30,
                jobTypes: [JobType.PROFILE_PICTURES, JobType.MEDIA_DOWNLOAD]
            }],
            [SharedQueueName.LOW_PRIORITY, {
                concurrency: config.queueConcurrency?.lowPriority || 10,
                jobTypes: [JobType.STATE, JobType.PRESENCES, JobType.LABELS, JobType.LABEL_ASSOCIATIONS]
            }]
        ])
        
        // Initialize Redis connection
        this.initializeRedis()
        
        // Initialize queues
        this.initializeQueues()
        
        // Set up graceful shutdown handlers
        this.setupShutdownHandlers()
    }
    
    private safeRandomId(): string {
        try {
            return randomUUID()
        } catch {
            return `worker-${Date.now()}-${Math.random().toString(36).slice(2)}`
        }
    }
    
    /**
     * Get singleton instance of SharedQueueManager
     */
    static getInstance(config?: SharedQueueManagerConfig): SharedQueueManager {
        if (!SharedQueueManager.instance) {
            if (!config) {
                throw new Error('SharedQueueManager: Configuration required for first initialization')
            }
            SharedQueueManager.instance = new SharedQueueManager(config)
        }
        return SharedQueueManager.instance
    }
    
    /**
     * Reset singleton instance (mainly for testing)
     */
    static resetInstance(): void {
        if (SharedQueueManager.instance) {
            SharedQueueManager.instance.shutdown().catch(console.error)
            SharedQueueManager.instance = null
        }
    }
    
    private initializeRedis(): void {
        const { redis } = this.config
        
        if (typeof redis.connection === 'string') {
            this.redis = new Redis(redis.connection, {
                maxRetriesPerRequest: null,
                enableReadyCheck: true,
                lazyConnect: false
            })
        } else if (redis.connection instanceof Redis) {
            this.redis = redis.connection
        } else if (redis.connection) {
            this.redis = new Redis({
                ...redis.connection,
                maxRetriesPerRequest: null,
                enableReadyCheck: true,
                lazyConnect: false
            })
        } else {
            this.redis = new Redis({
                host: redis.host || 'localhost',
                port: redis.port || 6379,
                password: redis.password,
                db: redis.db || 0,
                maxRetriesPerRequest: null,
                enableReadyCheck: true,
                lazyConnect: false
            })
        }
        
        // Fix EventEmitter memory leak warning
        this.redis.setMaxListeners(0)
        
        // Set up Redis event handlers
        this.redis.on('connect', () => {
            this.log('info', '✅ SharedQueueManager: Redis connected')
            this.emit('redis:connected')
        })
        
        this.redis.on('error', (error) => {
            this.log('error', '❌ SharedQueueManager: Redis error', error)
            this.emit('redis:error', error)
        })
        
        this.redis.on('close', () => {
            this.log('warn', '⚠️ SharedQueueManager: Redis connection closed')
            this.emit('redis:closed')
        })
    }
    
    private initializeQueues(): void {
        const connection = this.redis
        
        // Default job options for automatic cleanup
        const defaultJobOptions = {
            removeOnComplete: {
                age: 60,    // Keep completed jobs for 60 seconds
                count: 100  // Keep max 100 completed jobs per queue
            },
            removeOnFail: {
                age: 300,   // Keep failed jobs for 5 minutes
                count: 50   // Keep max 50 failed jobs per queue
            }
        }
        
        // Create queues and workers
        for (const [queueName, config] of this.queueConfigs) {
            // Create queue
            const queue = new Queue<SharedJobData>(queueName, {
                connection,
                defaultJobOptions
            })
            this.queues.set(queueName, queue)
            
            // Initialize metrics
            this.metrics.set(queueName, {
                processed: 0,
                failed: 0,
                avgProcessingTime: 0
            })
            
            // Create worker
            const worker = new Worker<SharedJobData>(
                queueName,
                async (job) => this.processJob(job, queueName),
                {
                    connection,
                    concurrency: config.concurrency,
                    autorun: true
                }
            )
            this.workers.set(queueName, worker)
            
            // Create queue events listener
            const queueEvents = new QueueEvents(queueName, { connection })
            this.queueEvents.set(queueName, queueEvents)
            
            // Set up event handlers
            this.setupQueueEventHandlers(queueName, worker, queueEvents)
            
            this.log('info', `📦 Initialized queue ${queueName} with concurrency ${config.concurrency}`)
        }
        
        this.log('info', '✅ SharedQueueManager: All queues initialized')
    }
    
    private setupQueueEventHandlers(
        queueName: SharedQueueName,
        worker: Worker<SharedJobData>,
        queueEvents: QueueEvents
    ): void {
        // Worker events
        worker.on('completed', (job) => {
            const metrics = this.metrics.get(queueName)!
            metrics.processed++
            metrics.lastProcessedAt = new Date()
            
            if (this.config.enableMetrics) {
                const processingTime = Date.now() - job.data.timestamp
                metrics.avgProcessingTime = 
                    (metrics.avgProcessingTime * (metrics.processed - 1) + processingTime) / metrics.processed
            }
            
            this.log('debug', `✅ ${queueName}: Job ${job.id} completed for instance ${job.data.instanceId}`)
            this.emit('job:completed', { queue: queueName, job: job.data })
        })
        
        worker.on('failed', (job, error) => {
            const metrics = this.metrics.get(queueName)!
            metrics.failed++
            
            this.log('error', `❌ ${queueName}: Job ${job?.id} failed for instance ${job?.data.instanceId}`, error)
            this.emit('job:failed', { queue: queueName, job: job?.data, error })
        })
        
        worker.on('stalled', (jobId) => {
            this.log('warn', `⚠️ ${queueName}: Job ${jobId} stalled`)
            this.emit('job:stalled', { queue: queueName, jobId })
        })
        
        // Queue events
        queueEvents.on('waiting', ({ jobId }) => {
            this.log('debug', `⏳ ${queueName}: Job ${jobId} waiting`)
        })
        
        queueEvents.on('progress', ({ jobId, data }) => {
            this.log('debug', `📊 ${queueName}: Job ${jobId} progress`, data)
        })
    }
    
    private async processJob(job: Job<SharedJobData>, queueName: SharedQueueName): Promise<any> {
        const { type, instanceId } = job.data
        
        // Check if we're shutting down
        if (this.isShuttingDown) {
            throw new Error('Queue manager is shutting down')
        }

        // Ensure this worker is responsible for the instance before processing
        const ownershipResult = await this.ensureOwnershipForJob(job, queueName)
        if (ownershipResult.action === 'skip') {
            return {
                skipped: true,
                reason: ownershipResult.reason
            }
        }
        
        // Get processor for this specific instance and job type
        const processors = this.instanceProcessors.get(type)
        if (!processors || !processors.has(instanceId)) {
            const errorMessage = `No processor registered for instance ${instanceId} and job type ${type}`
            await this.handleMissingProcessor(job, queueName, errorMessage)
            // Avoid throwing to prevent noisy failures when the job landed on a non-owner worker.
            // We re-queued or delayed the job already in handleMissingProcessor.
            this.log('warn', `⤴️ ${queueName}: ${errorMessage}. Job ${job.id} rescheduled.`)
            return { skipped: true, reason: errorMessage }
        }
        
        const processor = processors.get(instanceId)!
        
        try {
            // Add processing metadata
            const startTime = Date.now()
            
            // Execute processor
            const result = await processor(job)

            // Refresh ownership TTL so the claim stays active while work continues
            await this.extendOwnership(instanceId)
            
            const processingTime = Date.now() - startTime
            this.log('debug', `✅ Processed ${type} job for instance ${instanceId} in ${processingTime}ms`)
            
            return {
                success: true,
                processingTime,
                result
            }
        } catch (error) {
            this.log('error', `❌ Failed to process ${type} job for instance ${instanceId}`, error)
            throw error
        }
    }

    private async ensureOwnershipForJob(
        job: Job<SharedJobData>,
        queueName: SharedQueueName
    ): Promise<{ action: 'process' } | { action: 'skip'; reason: string }> {
        const instanceId = job.data.instanceId

        // Fast path: we already believe we own the instance
        if (this.claimedInstances.has(instanceId)) {
            if (job.data.ownerId !== this.workerId) {
                job.data.ownerId = this.workerId
                try {
                    await (job as any).update(job.data)
                } catch (error) {
                    this.log('warn', `⚠️ Failed to update job ${job.id} with local ownership`, error)
                }
            }
            await this.extendOwnership(instanceId)
            return { action: 'process' }
        }

        const jobOwnerId = job.data.ownerId
        if (jobOwnerId && jobOwnerId !== this.workerId) {
            await this.rescheduleForOwnershipMismatch(job, queueName, jobOwnerId)
            return { action: 'skip', reason: `Job owned by ${jobOwnerId}` }
        }

        const claimResult = await this.claimInstanceOwnership(instanceId)

        if (!claimResult.owned && claimResult.ownerId && claimResult.ownerId !== this.workerId) {
            await this.rescheduleForOwnershipMismatch(job, queueName, claimResult.ownerId)
            return { action: 'skip', reason: `Ownership held by ${claimResult.ownerId}` }
        }

        if (claimResult.owned) {
            if (job.data.ownerId !== this.workerId) {
                job.data.ownerId = this.workerId
                try {
                    await (job as any).update(job.data)
                } catch (error) {
                    this.log('warn', `⚠️ Failed to stamp ownership on job ${job.id}`, error)
                }
            }
            await this.extendOwnership(instanceId)
        }

        return { action: 'process' }
    }

    private async rescheduleForOwnershipMismatch(
        job: Job<SharedJobData>,
        queueName: SharedQueueName,
        ownerId: string
    ): Promise<void> {
        const requeueCount = (job.data.requeueCount ?? 0) + 1
        const delay = Math.min(
            this.ownershipConfig.conflictDelayMs * requeueCount,
            this.ownershipConfig.conflictDelayMs * 5
        )

        try {
            await (job as any).update({
                ...job.data,
                ownerId,
                requeueCount,
                lastError: `Ownership mismatch: job belongs to ${ownerId}, worker ${this.workerId}`
            })

            if ((job as any).token) {
                const scheduledFor = Date.now() + delay
                await job.moveToDelayed(scheduledFor, (job as any).token)
            } else {
                const queue = this.queues.get(queueName)
                if (queue) {
                    await queue.add(
                        `${job.data.type}_${job.data.instanceId}_${Date.now()}`,
                        {
                            ...job.data,
                            ownerId,
                            requeueCount,
                            lastError: `Ownership mismatch: job belongs to ${ownerId}`
                        },
                        {
                            priority: job.opts.priority ?? Math.max(0, Math.min(10 - (job.data.priority ?? 5), 10)),
                            delay
                        }
                    )
                }
            }

            this.log(
                'debug',
                `↩️ Rescheduled job ${job.id} for instance ${job.data.instanceId} (owned by ${ownerId})`
            )
        } catch (error) {
            this.log('warn', `⚠️ Failed to reschedule job ${job.id} for ownership mismatch`, error)
        }
    }

    async claimInstanceOwnership(instanceId: string): Promise<{ owned: boolean; ownerId: string | null }> {
        const ownerKey = this.getInstanceOwnerKey(instanceId)
        try {
            const existingOwner = await this.redis.get(ownerKey)

            if (existingOwner === this.workerId) {
                await this.extendOwnership(instanceId, ownerKey)
                return { owned: true, ownerId: this.workerId }
            }

            if (existingOwner && existingOwner !== this.workerId) {
                return { owned: false, ownerId: existingOwner }
            }

            const setResult = await this.redis.set(
                ownerKey,
                this.workerId,
                'PX',
                this.ownershipConfig.claimTtlMs,
                'NX'
            )

            if (setResult === 'OK') {
                this.claimedInstances.add(instanceId)
                this.startOwnershipHeartbeat(instanceId, ownerKey)
                return { owned: true, ownerId: this.workerId }
            }

            const postSetOwner = await this.redis.get(ownerKey)
            return {
                owned: postSetOwner === this.workerId,
                ownerId: postSetOwner
            }
        } catch (error) {
            this.log('error', `❌ Failed to claim ownership for instance ${instanceId}`, error)
            return { owned: false, ownerId: null }
        }
    }

    async releaseInstanceOwnership(instanceId: string): Promise<void> {
        const ownerKey = this.getInstanceOwnerKey(instanceId)
        try {
            const script = `
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("del", KEYS[1])
                end
                return 0
            `
            await this.redis.eval(script, 1, ownerKey, this.workerId)
        } catch (error) {
            this.log('warn', `⚠️ Failed to release ownership for instance ${instanceId}`, error)
        } finally {
            this.stopOwnershipHeartbeat(instanceId)
            this.claimedInstances.delete(instanceId)
        }
    }

    private async extendOwnership(instanceId: string, ownerKey?: string): Promise<void> {
        const key = ownerKey ?? this.getInstanceOwnerKey(instanceId)
        try {
            const result = await this.redis.set(
                key,
                this.workerId,
                'PX',
                this.ownershipConfig.claimTtlMs,
                'XX'
            )

            if (result === 'OK') {
                this.claimedInstances.add(instanceId)
                this.startOwnershipHeartbeat(instanceId, key)
            } else {
                const currentOwner = await this.redis.get(key)
                if (currentOwner !== this.workerId) {
                    this.stopOwnershipHeartbeat(instanceId)
                    this.claimedInstances.delete(instanceId)
                }
            }
        } catch (error) {
            this.log('warn', `⚠️ Failed to extend ownership for instance ${instanceId}`, error)
        }
    }

    private getInstanceOwnerKey(instanceId: string): string {
        return `${this.ownershipConfig.keyPrefix}:owner:${instanceId}`
    }

    private async getInstanceOwner(instanceId: string): Promise<string | null> {
        try {
            return await this.redis.get(this.getInstanceOwnerKey(instanceId))
        } catch (error) {
            this.log('warn', `⚠️ Failed to read ownership for instance ${instanceId}`, error)
            return null
        }
    }

    private startOwnershipHeartbeat(instanceId: string, ownerKey?: string): void {
        if (this.ownershipHeartbeats.has(instanceId)) {
            return
        }

        const key = ownerKey ?? this.getInstanceOwnerKey(instanceId)
        const intervalMs = this.ownershipConfig.heartbeatIntervalMs

        const timer = setInterval(async () => {
            try {
                const result = await this.redis.set(
                    key,
                    this.workerId,
                    'PX',
                    this.ownershipConfig.claimTtlMs,
                    'XX'
                )
                if (result !== 'OK') {
                    const currentOwner = await this.redis.get(key)
                    if (currentOwner !== this.workerId) {
                        this.log('warn', `⚠️ Ownership heartbeat lost for instance ${instanceId}`)
                        this.stopOwnershipHeartbeat(instanceId)
                        this.claimedInstances.delete(instanceId)
                    }
                }
            } catch (error) {
                this.log('warn', `⚠️ Ownership heartbeat error for instance ${instanceId}`, error)
            }
        }, intervalMs)

        if (typeof (timer as any).unref === 'function') {
            (timer as any).unref()
        }

        this.ownershipHeartbeats.set(instanceId, timer)
    }

    private stopOwnershipHeartbeat(instanceId: string): void {
        const timer = this.ownershipHeartbeats.get(instanceId)
        if (timer) {
            clearInterval(timer)
            this.ownershipHeartbeats.delete(instanceId)
        }
    }

    private buildOwnershipConfig(ownership?: SharedQueueManagerConfig['ownership']): OwnershipConfig {
        const claimTtlMs = ownership?.claimTtlMs ?? 60000
        const heartbeatIntervalMs = ownership?.heartbeatIntervalMs ?? Math.max(5000, Math.floor(claimTtlMs / 2))
        return {
            keyPrefix: ownership?.keyPrefix || 'baileys_shared_queue',
            claimTtlMs,
            heartbeatIntervalMs,
            conflictDelayMs: ownership?.conflictDelayMs ?? 2000
        }
    }
    
    /**
     * Register a processor for a specific job type and instance
     * This fixes the singleton overwrite issue by maintaining per-instance processors
     */
    registerInstanceProcessor(instanceId: string, type: JobType, processor: (job: Job<SharedJobData>) => Promise<any>): void {
        if (!this.instanceProcessors.has(type)) {
            this.instanceProcessors.set(type, new Map())
        }
        this.instanceProcessors.get(type)!.set(instanceId, processor)
        this.log('info', `📝 Registered processor for instance ${instanceId} and job type ${type}`)
        
        if (!this.claimedInstances.has(instanceId)) {
            this.claimInstanceOwnership(instanceId).catch(error => {
                this.log('warn', `⚠️ Failed to initiate ownership claim for instance ${instanceId}`, error)
            })
        }
    }
    
    /**
     * Legacy method - kept for backward compatibility but now registers for a default instance
     * @deprecated Use registerInstanceProcessor instead
     */
    registerProcessor(type: JobType, processor: (job: Job<SharedJobData>) => Promise<any>): void {
        this.log('warn', `⚠️ Using deprecated registerProcessor for job type ${type}. Use registerInstanceProcessor instead.`)
        // Register as a default processor - this will be overwritten if called multiple times!
        this.registerInstanceProcessor('default', type, processor)
    }
    
    /**
     * Unregister all processors for a specific instance
     * Call this when an instance shuts down to clean up
     */
    unregisterInstanceProcessors(instanceId: string): void {
        let removedCount = 0
        for (const [, processors] of this.instanceProcessors) {
            if (processors.has(instanceId)) {
                processors.delete(instanceId)
                removedCount++
            }
        }
        if (removedCount > 0) {
            this.log('info', `🗑️ Unregistered ${removedCount} processors for instance ${instanceId}`)
        }
    }
    
    /**
     * Add a job to the appropriate queue
     */
    async addJob(
        type: JobType,
        data: any,
        instanceId: string,
        priority: number = 5,
        options?: {
            delayMs?: number
            attempts?: number
            backoff?: JobsOptions['backoff']
        }
    ): Promise<Job<SharedJobData>> {
        // Find the appropriate queue for this job type
        let targetQueue: SharedQueueName | undefined
        
        for (const [queueName, config] of this.queueConfigs) {
            if (config.jobTypes.includes(type)) {
                targetQueue = queueName
                break
            }
        }
        
        if (!targetQueue) {
            throw new Error(`No queue configured for job type ${type}`)
        }
        
        const queue = this.queues.get(targetQueue)
        if (!queue) {
            throw new Error(`Queue ${targetQueue} not initialized`)
        }
        
        // Determine owner for this instance so the correct worker handles the job
        let ownerId: string | null = null
        if (this.claimedInstances.has(instanceId)) {
            ownerId = this.workerId
            await this.extendOwnership(instanceId)
        } else {
            ownerId = await this.getInstanceOwner(instanceId)
            if (!ownerId) {
                const claimAttempt = await this.claimInstanceOwnership(instanceId)
                ownerId = claimAttempt.ownerId
            }
        }
        
        // Create job data
        const jobData: SharedJobData = {
            instanceId,
            type,
            data,
            priority,
            timestamp: Date.now(),
            requeueCount: 0
        }
        if (ownerId) {
            jobData.ownerId = ownerId
        }
        
        const jobOptions: JobsOptions = {
            priority: Math.max(0, Math.min(10 - priority, 10)), // BullMQ uses lower numbers for higher priority
            delay: options?.delayMs ?? 0
        }

        if (options?.attempts && options.attempts > 0) {
            jobOptions.attempts = options.attempts
        }

        if (options?.backoff) {
            jobOptions.backoff = options.backoff
        }

        // Add job with priority and optional retry configuration
        const job = await queue.add(
            `${type}_${instanceId}_${Date.now()}`,
            jobData,
            jobOptions
        )
        
        this.log('debug', `📨 Added ${type} job to ${targetQueue} for instance ${instanceId}`)
        
        return job
    }
    
    /**
     * Get queue metrics
     */
    async getMetrics(): Promise<Map<SharedQueueName, QueueMetrics & { waiting: number; active: number; completed: number; failed: number }>> {
        const results = new Map()
        
        for (const [queueName, queue] of this.queues) {
            const metrics = this.metrics.get(queueName)!
            const counts = await queue.getJobCounts()
            
            results.set(queueName, {
                ...metrics,
                waiting: counts.waiting,
                active: counts.active,
                completed: counts.completed,
                failed: counts.failed
            })
        }
        
        return results
    }
    
    /**
     * Graceful shutdown with job draining
     */
    async shutdown(): Promise<void> {
        if (this.isShuttingDown) {
            return
        }
        
        this.isShuttingDown = true
        this.log('info', '🛑 SharedQueueManager: Starting graceful shutdown...')
        
        try {
            // Stop accepting new jobs
            for (const queue of this.queues.values()) {
                await queue.pause()
            }
            
            // Wait for active jobs to complete (max 30 seconds)
            const shutdownTimeout = 30000
            const startTime = Date.now()
            
            while (Date.now() - startTime < shutdownTimeout) {
                let hasActiveJobs = false
                
                for (const [queueName] of this.workers) {
                    // Check for active jobs in the queue
                    const queue = this.queues.get(queueName)!
                    const counts = await queue.getJobCounts()
                    if (counts.active > 0) {
                        hasActiveJobs = true
                        break
                    }
                }
                
                if (!hasActiveJobs) {
                    break
                }
                
                this.log('info', '⏳ Waiting for active jobs to complete...')
                await new Promise(resolve => setTimeout(resolve, 1000))
            }
            
            // Close workers
            for (const worker of this.workers.values()) {
                await worker.close()
            }
            
            // Close queue events
            for (const queueEvents of this.queueEvents.values()) {
                await queueEvents.close()
            }
            
            // Close queues
            for (const queue of this.queues.values()) {
                await queue.close()
            }

            for (const timer of this.ownershipHeartbeats.values()) {
                clearInterval(timer)
            }
            this.ownershipHeartbeats.clear()
            this.claimedInstances.clear()
            
            // Close Redis connection
            this.redis.disconnect()
            
            this.log('info', '✅ SharedQueueManager: Shutdown complete')
            this.emit('shutdown')
        } catch (error) {
            this.log('error', '❌ Error during shutdown', error)
            throw error
        }
    }
    
    private setupShutdownHandlers(): void {
        // Handle process termination signals
        const shutdownHandler = async (signal: string) => {
            this.log('info', `📛 Received ${signal}, initiating graceful shutdown...`)
            await this.shutdown()
            process.exit(0)
        }
        
        process.once('SIGTERM', () => shutdownHandler('SIGTERM'))
        process.once('SIGINT', () => shutdownHandler('SIGINT'))
    }
    
    private log(level: string, ...args: any[]): void {
        const { logLevel } = this.config
        
        if (logLevel === 'none') return

        const normalizedLogLevel = logLevel === 'all' ? 'debug' : logLevel
        
        const levelPriority: Record<string, number> = {
            debug: 0,
            info: 1,
            warn: 2,
            error: 3
        }
        
        const configuredPriority = levelPriority[normalizedLogLevel || 'none'] ?? 4
        const messagePriority = levelPriority[level] ?? 0
        
        if (messagePriority >= configuredPriority) {
            const logFn = console[level as 'debug' | 'info' | 'warn' | 'error']
            if (logFn) {
                logFn(...args)
            } else {
                console.log(...args)
            }
        }
    }

    private getMissingProcessorConfig(): {
        maxAttempts: number
        initialDelayMs: number
        maxDelayMs: number
    } {
        const defaults = {
            maxAttempts: 5,
            initialDelayMs: 2000,
            maxDelayMs: 15000
        }
        const overrides = this.config.missingProcessorHandling || {}
        return {
            maxAttempts: (overrides.maxAttempts && overrides.maxAttempts > 0)
                ? overrides.maxAttempts
                : defaults.maxAttempts,
            initialDelayMs: overrides.initialDelayMs ?? defaults.initialDelayMs,
            maxDelayMs: overrides.maxDelayMs ?? defaults.maxDelayMs
        }
    }

    private async handleMissingProcessor(
        job: Job<SharedJobData>,
        queueName: SharedQueueName,
        reason: string
    ): Promise<void> {
        const { maxAttempts, initialDelayMs, maxDelayMs } = this.getMissingProcessorConfig()
        const currentAttempt = job.data.requeueCount ?? 0
        const nextAttempt = currentAttempt + 1
        
        if (Number.isFinite(maxAttempts) && nextAttempt > maxAttempts) {
            this.log('error', `❌ ${reason}. Reached max attempts (${maxAttempts}), leaving job failed.`)
            return
        }

        const queue = this.queues.get(queueName)
        if (!queue) {
            this.log('error', `❌ Attempted to requeue job ${job.id} on missing queue ${queueName}`)
            return
        }

        const delay = Math.min(initialDelayMs * Math.pow(2, currentAttempt), maxDelayMs)
        const priority = job.data.priority ?? 5
        const attemptLabel = Number.isFinite(maxAttempts)
            ? `${nextAttempt}/${maxAttempts}`
            : `${nextAttempt}`

        this.log(
            'warn',
            `⚠️ ${reason}. Re-queuing (attempt ${attemptLabel}) with ${delay}ms delay.`
        )

        // Prefer delaying the same job to avoid duplications
        try {
            await (job as any).update({
                ...job.data,
                requeueCount: nextAttempt,
                lastError: reason,
                timestamp: job.data.timestamp ?? Date.now()
            })
            if ((job as any).token) {
                await job.moveToDelayed(Date.now() + delay, (job as any).token)
                return
            }
        } catch (err) {
            // Fall through to re-add as new job if update/move fails
        }

        const nextJobId = `${job.data.type}_${job.data.instanceId}_${Date.now()}`
        await queue.add(
            nextJobId,
            {
                ...job.data,
                requeueCount: nextAttempt,
                lastError: reason,
                timestamp: job.data.timestamp ?? Date.now()
            },
            {
                priority: 10 - priority,
                delay
            }
        )
    }
    
    /**
     * Check if manager is initialized
     */
    static isInitialized(): boolean {
        return SharedQueueManager.instance !== null
    }
    
    /**
     * Get queue for a specific job type
     */
    getQueueForJobType(type: JobType): SharedQueueName | undefined {
        for (const [queueName, config] of this.queueConfigs) {
            if (config.jobTypes.includes(type)) {
                return queueName
            }
        }
        return undefined
    }

    /**
     * Expose the worker identifier for logging/debug purposes
     */
    getWorkerId(): string {
        return this.workerId
    }
    
    /**
     * Clean all queues (remove all jobs)
     */
    async cleanAllQueues(): Promise<void> {
        this.log('info', '🧹 Cleaning all queues...')
        
        for (const [queueName, queue] of this.queues) {
            await queue.obliterate({ force: true })
            this.log('info', `✅ Cleaned queue ${queueName}`)
        }
    }
}
