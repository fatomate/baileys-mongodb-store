import { Db } from 'mongodb'
import { EventEmitter } from 'events'

export interface HealthMetrics {
    isHealthy: boolean
    lastCheck: Date
    lastSuccessfulOperation: Date
    failedAttempts: number
    successfulOperations: number
    failedOperations: number
    averageResponseTime: number
    connectionState: string
}

export interface HealthCheckOptions {
    checkInterval?: number // milliseconds
    unhealthyThreshold?: number // number of consecutive failures before marking unhealthy
    healthyThreshold?: number // number of consecutive successes before marking healthy
}

export class ConnectionHealthMonitor extends EventEmitter {
    private metrics: HealthMetrics
    private checkInterval: NodeJS.Timeout | null = null
    private options: Required<HealthCheckOptions>
    private consecutiveFailures = 0
    private consecutiveSuccesses = 0
    private responseTimes: number[] = []
    private readonly maxResponseTimeSamples = 100
    
    constructor(options?: HealthCheckOptions) {
        super()
        
        this.options = {
            checkInterval: options?.checkInterval ?? 30000, // 30 seconds
            unhealthyThreshold: options?.unhealthyThreshold ?? 3,
            healthyThreshold: options?.healthyThreshold ?? 2
        }
        
        this.metrics = {
            isHealthy: true,
            lastCheck: new Date(),
            lastSuccessfulOperation: new Date(),
            failedAttempts: 0,
            successfulOperations: 0,
            failedOperations: 0,
            averageResponseTime: 0,
            connectionState: 'unknown'
        }
    }
    
    /**
     * Start health monitoring
     */
    startMonitoring(db: Db): void {
        if (this.checkInterval) {
            return // Already monitoring
        }
        
        // Perform initial health check
        this.performHealthCheck(db)
        
        // Set up periodic health checks
        this.checkInterval = setInterval(() => {
            this.performHealthCheck(db)
        }, this.options.checkInterval)
    }
    
    /**
     * Stop health monitoring
     */
    stopMonitoring(): void {
        if (this.checkInterval) {
            clearInterval(this.checkInterval)
            this.checkInterval = null
        }
    }
    
    /**
     * Perform a health check
     */
    private async performHealthCheck(db: Db): Promise<void> {
        const startTime = Date.now()
        
        try {
            // Ping the database
            await db.admin().ping()
            
            const responseTime = Date.now() - startTime
            this.recordSuccess(responseTime)
            
            this.consecutiveSuccesses++
            this.consecutiveFailures = 0
            
            // Mark as healthy if threshold met
            if (!this.metrics.isHealthy && this.consecutiveSuccesses >= this.options.healthyThreshold) {
                this.markHealthy()
            }
            
            this.metrics.lastCheck = new Date()
            this.emit('healthCheck', { healthy: true, responseTime })
        } catch (error) {
            this.recordFailure()
            
            this.consecutiveFailures++
            this.consecutiveSuccesses = 0
            
            // Mark as unhealthy if threshold met
            if (this.metrics.isHealthy && this.consecutiveFailures >= this.options.unhealthyThreshold) {
                this.markUnhealthy(error)
            }
            
            this.metrics.lastCheck = new Date()
            this.emit('healthCheck', { healthy: false, error })
        }
    }
    
    /**
     * Record a successful operation
     */
    recordSuccess(responseTime?: number): void {
        this.metrics.successfulOperations++
        this.metrics.lastSuccessfulOperation = new Date()
        
        if (responseTime !== undefined) {
            this.responseTimes.push(responseTime)
            if (this.responseTimes.length > this.maxResponseTimeSamples) {
                this.responseTimes.shift()
            }
            
            // Calculate average response time
            const sum = this.responseTimes.reduce((a, b) => a + b, 0)
            this.metrics.averageResponseTime = sum / this.responseTimes.length
        }
    }
    
    /**
     * Record a failed operation
     */
    recordFailure(): void {
        this.metrics.failedOperations++
        this.metrics.failedAttempts++
    }
    
    /**
     * Mark connection as healthy
     */
    private markHealthy(): void {
        const wasUnhealthy = !this.metrics.isHealthy
        this.metrics.isHealthy = true
        this.metrics.connectionState = 'healthy'
        
        if (wasUnhealthy) {
            this.emit('healthy')
        }
    }
    
    /**
     * Mark connection as unhealthy
     */
    private markUnhealthy(error: any): void {
        const wasHealthy = this.metrics.isHealthy
        this.metrics.isHealthy = false
        this.metrics.connectionState = 'unhealthy'
        
        if (wasHealthy) {
            this.emit('unhealthy', error)
        }
    }
    
    /**
     * Update connection state
     */
    updateConnectionState(state: string): void {
        this.metrics.connectionState = state
    }
    
    /**
     * Get current health metrics
     */
    getMetrics(): HealthMetrics {
        return { ...this.metrics }
    }
    
    /**
     * Check if connection is healthy
     */
    isHealthy(): boolean {
        return this.metrics.isHealthy
    }
    
    /**
     * Get average response time
     */
    getAverageResponseTime(): number {
        return this.metrics.averageResponseTime
    }
}