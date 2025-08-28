import { Queue, Worker, Job, QueueEvents } from 'bullmq'
import Redis from 'ioredis'
import { EventEmitter } from 'events'

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
    enableMetrics?: boolean
    logLevel?: 'none' | 'error' | 'warn' | 'info' | 'debug'
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
    
    private constructor(config: SharedQueueManagerConfig) {
        super()
        this.config = config
        this.queues = new Map()
        this.workers = new Map()
        this.queueEvents = new Map()
        this.metrics = new Map()
        this.instanceProcessors = new Map() // Initialize instance processors map
        this.redis = null as any // Will be initialized in initializeRedis()
        
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
    
    private async processJob(job: Job<SharedJobData>, _queueName: SharedQueueName): Promise<any> {
        const { type, instanceId } = job.data
        
        // Check if we're shutting down
        if (this.isShuttingDown) {
            throw new Error('Queue manager is shutting down')
        }
        
        // Get processor for this specific instance and job type
        const processors = this.instanceProcessors.get(type)
        if (!processors || !processors.has(instanceId)) {
            this.log('warn', `⚠️ No processor registered for instance ${instanceId} and job type ${type}`)
            return { success: false, error: `No processor for instance ${instanceId}` }
        }
        
        const processor = processors.get(instanceId)!
        
        try {
            // Add processing metadata
            const startTime = Date.now()
            
            // Execute processor
            const result = await processor(job)
            
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
        for (const [jobType, processors] of this.instanceProcessors) {
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
    async addJob(type: JobType, data: any, instanceId: string, priority: number = 5): Promise<Job<SharedJobData>> {
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
        
        // Create job data
        const jobData: SharedJobData = {
            instanceId,
            type,
            data,
            priority,
            timestamp: Date.now()
        }
        
        // Add job with priority
        const job = await queue.add(
            `${type}_${instanceId}_${Date.now()}`,
            jobData,
            {
                priority: 10 - priority, // BullMQ uses lower numbers for higher priority
                delay: 0
            }
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
        
        const levelPriority: Record<string, number> = {
            debug: 0,
            info: 1,
            warn: 2,
            error: 3
        }
        
        const configuredPriority = levelPriority[logLevel || 'none'] ?? 4
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