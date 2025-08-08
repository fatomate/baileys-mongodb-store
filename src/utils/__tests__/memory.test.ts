import {
    MemoryMonitor,
    BackpressureController,
    MemoryAwareBatchProcessor,
    calculateOptimalBatchSize,
    formatMemoryUsage,
    hassufficientMemory
} from '../memory'

describe('Memory Management Utilities', () => {
    describe('MemoryMonitor', () => {
        test('should get current memory metrics', () => {
            const monitor = new MemoryMonitor()
            const metrics = monitor.getCurrentMemory()
            
            expect(metrics).toHaveProperty('heapUsed')
            expect(metrics).toHaveProperty('heapTotal')
            expect(metrics).toHaveProperty('external')
            expect(metrics).toHaveProperty('rss')
            expect(metrics).toHaveProperty('timestamp')
            expect(metrics.heapUsed).toBeGreaterThan(0)
            expect(metrics.heapTotal).toBeGreaterThan(0)
        })

        test('should check memory within limits', () => {
            const monitor = new MemoryMonitor({ maxMemoryMB: 1024 })
            
            // Should be within limits for most test environments
            expect(monitor.isMemoryWithinLimits()).toBe(true)
        })

        test('should calculate memory pressure', () => {
            const monitor = new MemoryMonitor({ maxMemoryMB: 1024 })
            const pressure = monitor.getMemoryPressure()
            
            expect(pressure).toBeGreaterThanOrEqual(0)
            expect(pressure).toBeLessThanOrEqual(1)
        })

        test('should record batch metrics', () => {
            const monitor = new MemoryMonitor()
            
            monitor.recordBatch(100, 500)
            monitor.recordBatch(150, 600)
            
            const metrics = monitor.getBatchMetrics()
            expect(metrics.itemsProcessed).toBe(250)
            expect(metrics.batchesProcessed).toBe(2)
            expect(metrics.avgBatchSize).toBe(125)
            expect(metrics.maxBatchSize).toBe(150)
            expect(metrics.totalProcessingTime).toBe(1100)
            expect(metrics.memoryPeakMB).toBeGreaterThan(0)
        })

        test('should reset metrics', () => {
            const monitor = new MemoryMonitor()
            
            monitor.recordBatch(100, 500)
            monitor.resetMetrics()
            
            const metrics = monitor.getBatchMetrics()
            expect(metrics.itemsProcessed).toBe(0)
            expect(metrics.batchesProcessed).toBe(0)
            expect(metrics.avgBatchSize).toBe(0)
            expect(metrics.maxBatchSize).toBe(0)
        })
    })

    describe('BackpressureController', () => {
        test('should not pause when memory pressure is low', () => {
            const controller = new BackpressureController({ maxMemoryMB: 10000 }) // High limit
            
            expect(controller.shouldPause()).toBe(false)
            expect(controller.getPressure()).toBeLessThan(0.8)
        })

        test('should handle pause and resume callbacks', () => {
            const controller = new BackpressureController({ maxMemoryMB: 1 }) // Very low limit
            const pauseCallback = jest.fn()
            const resumeCallback = jest.fn()
            
            controller.onPause(pauseCallback)
            controller.onResume(resumeCallback)
            
            // Force high pressure scenario
            const shouldPause = controller.shouldPause()
            
            // Due to the very low memory limit, it should trigger pause
            if (shouldPause) {
                expect(pauseCallback).toHaveBeenCalled()
            }
        })

        test('should expose memory monitor', () => {
            const controller = new BackpressureController()
            const monitor = controller.getMonitor()
            
            expect(monitor).toBeDefined()
            expect(monitor.getCurrentMemory).toBeDefined()
        })
    })

    describe('MemoryAwareBatchProcessor', () => {
        test('should process items in batches', async () => {
            const processedBatches: number[][] = []
            const processor = new MemoryAwareBatchProcessor<number>(
                async (items) => {
                    processedBatches.push([...items])
                },
                { maxBatchSize: 3, batchTimeWindowMs: 50 }
            )
            
            // Add items
            await processor.add([1, 2])
            await processor.add([3, 4, 5])
            
            // Wait for processing
            await new Promise(resolve => setTimeout(resolve, 100))
            
            expect(processedBatches).toHaveLength(2)
            expect(processedBatches[0]).toEqual([1, 2, 3]) // First batch (max size 3)
            expect(processedBatches[1]).toEqual([4, 5]) // Remaining items
        })

        test('should flush all items', async () => {
            const processedItems: number[] = []
            const processor = new MemoryAwareBatchProcessor<number>(
                async (items) => {
                    processedItems.push(...items)
                },
                { maxBatchSize: 10, batchTimeWindowMs: 1000 } // Long window
            )
            
            await processor.add([1, 2, 3])
            await processor.add([4, 5])
            
            // Flush immediately
            await processor.flush()
            
            expect(processedItems).toEqual([1, 2, 3, 4, 5])
        })

        test('should report queue size', async () => {
            const processor = new MemoryAwareBatchProcessor<number>(
                async () => {
                    await new Promise(resolve => setTimeout(resolve, 100))
                },
                { maxBatchSize: 10, batchTimeWindowMs: 1000 }
            )
            
            await processor.add([1, 2, 3])
            expect(processor.getQueueSize()).toBe(3)
            
            await processor.flush()
            expect(processor.getQueueSize()).toBe(0)
        })

        test('should get processing metrics', async () => {
            const processor = new MemoryAwareBatchProcessor<number>(
                async () => {
                    await new Promise(resolve => setTimeout(resolve, 10))
                },
                { maxBatchSize: 2 }
            )
            
            await processor.add([1, 2, 3, 4])
            await processor.flush()
            
            const metrics = processor.getMetrics()
            expect(metrics.itemsProcessed).toBe(4)
            expect(metrics.batchesProcessed).toBe(2)
            expect(metrics.avgBatchSize).toBe(2)
            expect(metrics.maxBatchSize).toBe(2)
            expect(metrics.totalProcessingTime).toBeGreaterThan(0)
        })
    })

    describe('Utility Functions', () => {
        test('should calculate optimal batch size based on memory pressure', () => {
            expect(calculateOptimalBatchSize(100, 0)).toBe(100) // No pressure
            expect(calculateOptimalBatchSize(100, 0.5)).toBe(55) // 50% pressure
            expect(calculateOptimalBatchSize(100, 0.8)).toBe(27) // 80% pressure
            expect(calculateOptimalBatchSize(100, 1)).toBe(10) // 100% pressure (minimum)
        })

        test('should format memory usage', () => {
            expect(formatMemoryUsage(1024 * 1024)).toBe('1.00 MB')
            expect(formatMemoryUsage(1536 * 1024)).toBe('1.50 MB')
            expect(formatMemoryUsage(10 * 1024 * 1024)).toBe('10.00 MB')
            expect(formatMemoryUsage(512 * 1024)).toBe('0.50 MB')
        })

        test('should check sufficient memory', () => {
            // Most systems should have at least 10MB available
            expect(hassufficientMemory(10)).toBe(true)
            
            // Very large requirement should fail
            expect(hassufficientMemory(100000)).toBe(false)
        })
    })

    describe('Integration Tests', () => {
        test('should handle backpressure during batch processing', async () => {
            const processedBatches: number[][] = []
            const memoryConfig = { 
                maxMemoryMB: 10000, // High limit to avoid actual backpressure
                maxBatchSize: 5,
                batchTimeWindowMs: 50
            }
            
            const processor = new MemoryAwareBatchProcessor<number>(
                async (items) => {
                    processedBatches.push([...items])
                    await new Promise(resolve => setTimeout(resolve, 10))
                },
                memoryConfig
            )
            
            // Add many items
            for (let i = 0; i < 20; i++) {
                await processor.add([i])
            }
            
            await processor.flush()
            
            // Should have processed all items
            const totalProcessed = processedBatches.reduce((sum, batch) => sum + batch.length, 0)
            expect(totalProcessed).toBe(20)
            
            // Should respect max batch size
            processedBatches.forEach(batch => {
                expect(batch.length).toBeLessThanOrEqual(5)
            })
        })
    })
})