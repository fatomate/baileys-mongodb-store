/**
 * Memory management utilities for batch processing and resource monitoring
 */

import { performance } from 'perf_hooks'

export interface MemoryConfig {
    /**
     * Maximum memory threshold in MB before triggering backpressure
     */
    maxMemoryMB?: number
    
    /**
     * Maximum batch size allowed
     */
    maxBatchSize?: number
    
    /**
     * Time window for batch accumulation in ms
     */
    batchTimeWindowMs?: number
    
    /**
     * Enable memory monitoring
     */
    enableMonitoring?: boolean
}

export interface MemoryMetrics {
    heapUsed: number
    heapTotal: number
    external: number
    rss: number
    timestamp: Date
}

export interface BatchMetrics {
    itemsProcessed: number
    batchesProcessed: number
    avgBatchSize: number
    maxBatchSize: number
    totalProcessingTime: number
    memoryPeakMB: number
}

// Default configuration
const DEFAULT_MEMORY_CONFIG: Required<MemoryConfig> = {
    maxMemoryMB: 512, // 512MB default threshold
    maxBatchSize: 1000, // Maximum 1000 items per batch
    batchTimeWindowMs: 100, // 100ms batch window
    enableMonitoring: true
}

/**
 * Memory monitor for tracking resource usage
 */
export class MemoryMonitor {
    private config: Required<MemoryConfig>
    private batchMetrics: BatchMetrics = {
        itemsProcessed: 0,
        batchesProcessed: 0,
        avgBatchSize: 0,
        maxBatchSize: 0,
        totalProcessingTime: 0,
        memoryPeakMB: 0
    }
    
    constructor(config: MemoryConfig = {}) {
        this.config = { ...DEFAULT_MEMORY_CONFIG, ...config }
    }
    
    /**
     * Get current memory usage
     */
    getCurrentMemory(): MemoryMetrics {
        const memUsage = process.memoryUsage()
        return {
            heapUsed: memUsage.heapUsed,
            heapTotal: memUsage.heapTotal,
            external: memUsage.external,
            rss: memUsage.rss,
            timestamp: new Date()
        }
    }
    
    /**
     * Check if memory usage is within limits
     */
    isMemoryWithinLimits(): boolean {
        const current = this.getCurrentMemory()
        const usedMB = current.heapUsed / 1024 / 1024
        return usedMB < this.config.maxMemoryMB
    }
    
    /**
     * Get memory pressure (0-1, where 1 is maximum pressure)
     */
    getMemoryPressure(): number {
        const current = this.getCurrentMemory()
        const usedMB = current.heapUsed / 1024 / 1024
        return Math.min(usedMB / this.config.maxMemoryMB, 1)
    }
    
    /**
     * Record batch processing metrics
     */
    recordBatch(batchSize: number, processingTime: number): void {
        this.batchMetrics.itemsProcessed += batchSize
        this.batchMetrics.batchesProcessed++
        this.batchMetrics.totalProcessingTime += processingTime
        this.batchMetrics.maxBatchSize = Math.max(this.batchMetrics.maxBatchSize, batchSize)
        this.batchMetrics.avgBatchSize = this.batchMetrics.itemsProcessed / this.batchMetrics.batchesProcessed
        
        const currentMB = this.getCurrentMemory().heapUsed / 1024 / 1024
        this.batchMetrics.memoryPeakMB = Math.max(this.batchMetrics.memoryPeakMB, currentMB)
    }
    
    /**
     * Get batch processing metrics
     */
    getBatchMetrics(): BatchMetrics {
        return { ...this.batchMetrics }
    }
    
    /**
     * Reset metrics
     */
    resetMetrics(): void {
        this.batchMetrics = {
            itemsProcessed: 0,
            batchesProcessed: 0,
            avgBatchSize: 0,
            maxBatchSize: 0,
            totalProcessingTime: 0,
            memoryPeakMB: 0
        }
    }
    
    /**
     * Force garbage collection if available
     */
    forceGC(): void {
        if (global.gc) {
            global.gc()
        }
    }
}

/**
 * Backpressure controller for managing processing flow
 */
export class BackpressureController {
    private memoryMonitor: MemoryMonitor
    private processingPaused = false
    private pauseCallbacks: (() => void)[] = []
    private resumeCallbacks: (() => void)[] = []
    
    constructor(memoryConfig?: MemoryConfig) {
        this.memoryMonitor = new MemoryMonitor(memoryConfig)
    }
    
    /**
     * Check if processing should be paused due to memory pressure
     */
    shouldPause(): boolean {
        const pressure = this.memoryMonitor.getMemoryPressure()
        
        // Pause if pressure > 0.8 (80% of max memory)
        if (pressure > 0.8 && !this.processingPaused) {
            this.processingPaused = true
            this.pauseCallbacks.forEach(cb => cb())
            return true
        }
        
        // Resume if pressure < 0.6 (60% of max memory)
        if (pressure < 0.6 && this.processingPaused) {
            this.processingPaused = false
            this.resumeCallbacks.forEach(cb => cb())
        }
        
        return this.processingPaused
    }
    
    /**
     * Register pause callback
     */
    onPause(callback: () => void): void {
        this.pauseCallbacks.push(callback)
    }
    
    /**
     * Register resume callback
     */
    onResume(callback: () => void): void {
        this.resumeCallbacks.push(callback)
    }
    
    /**
     * Get current memory pressure
     */
    getPressure(): number {
        return this.memoryMonitor.getMemoryPressure()
    }
    
    /**
     * Get memory monitor instance
     */
    getMonitor(): MemoryMonitor {
        return this.memoryMonitor
    }
}

/**
 * Batch processor with memory management
 */
export class MemoryAwareBatchProcessor<T> {
    private items: T[] = []
    private timer: NodeJS.Timeout | null = null
    private processing = false
    private backpressure: BackpressureController
    private config: Required<MemoryConfig>
    private processCallback: (items: T[]) => Promise<void>
    
    constructor(
        processCallback: (items: T[]) => Promise<void>,
        config: MemoryConfig = {}
    ) {
        this.config = { ...DEFAULT_MEMORY_CONFIG, ...config }
        this.processCallback = processCallback
        this.backpressure = new BackpressureController(config)
    }
    
    /**
     * Add items to the batch
     */
    async add(items: T[]): Promise<void> {
        // Check if we should apply backpressure
        if (this.backpressure.shouldPause()) {
            // Wait for memory pressure to reduce
            await new Promise(resolve => {
                const checkInterval = setInterval(() => {
                    if (!this.backpressure.shouldPause()) {
                        clearInterval(checkInterval)
                        resolve(undefined)
                    }
                }, 100)
            })
        }
        
        // Add items respecting max batch size
        const remainingCapacity = this.config.maxBatchSize - this.items.length
        const itemsToAdd = items.slice(0, remainingCapacity)
        this.items.push(...itemsToAdd)
        
        // Process immediately if batch is full
        if (this.items.length >= this.config.maxBatchSize) {
            await this.processBatch()
        } else {
            // Schedule batch processing
            this.scheduleBatch()
        }
        
        // If there are remaining items, recursively add them
        if (items.length > itemsToAdd.length) {
            await this.add(items.slice(itemsToAdd.length))
        }
    }
    
    /**
     * Schedule batch processing
     */
    private scheduleBatch(): void {
        if (this.timer) {
            clearTimeout(this.timer)
        }
        
        this.timer = setTimeout(() => {
            this.processBatch()
        }, this.config.batchTimeWindowMs)
    }
    
    /**
     * Process the current batch
     */
    private async processBatch(): Promise<void> {
        if (this.processing || this.items.length === 0) {
            return
        }
        
        this.processing = true
        const batch = this.items.splice(0, this.config.maxBatchSize)
        const startTime = performance.now()
        
        try {
            await this.processCallback(batch)
            
            const processingTime = performance.now() - startTime
            this.backpressure.getMonitor().recordBatch(batch.length, processingTime)
        } finally {
            this.processing = false
            
            // Process remaining items if any
            if (this.items.length > 0) {
                await this.processBatch()
            }
        }
    }
    
    /**
     * Flush all remaining items
     */
    async flush(): Promise<void> {
        if (this.timer) {
            clearTimeout(this.timer)
            this.timer = null
        }
        
        while (this.items.length > 0) {
            await this.processBatch()
        }
    }
    
    /**
     * Get current metrics
     */
    getMetrics(): BatchMetrics {
        return this.backpressure.getMonitor().getBatchMetrics()
    }
    
    /**
     * Get current queue size
     */
    getQueueSize(): number {
        return this.items.length
    }
}

/**
 * Calculate optimal batch size based on memory pressure
 */
export function calculateOptimalBatchSize(
    baseBatchSize: number,
    memoryPressure: number
): number {
    // Reduce batch size as memory pressure increases
    // At 0% pressure: 100% of base batch size
    // At 50% pressure: 75% of base batch size
    // At 80% pressure: 25% of base batch size
    // At 100% pressure: 10% of base batch size
    
    const scaleFactor = 1 - (memoryPressure * 0.9)
    return Math.max(Math.floor(baseBatchSize * scaleFactor), Math.floor(baseBatchSize * 0.1))
}

/**
 * Memory usage formatter
 */
export function formatMemoryUsage(bytes: number): string {
    const mb = bytes / 1024 / 1024
    return `${mb.toFixed(2)} MB`
}

/**
 * Check if system has sufficient memory for operation
 */
export function hassufficientMemory(requiredMB: number): boolean {
    const current = process.memoryUsage()
    const availableMB = (current.heapTotal - current.heapUsed) / 1024 / 1024
    return availableMB >= requiredMB
}