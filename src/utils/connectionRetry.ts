
export interface RetryOptions {
    maxAttempts?: number
    initialDelay?: number
    maxDelay?: number
    factor?: number
    jitter?: boolean
    shouldRetry?: (error: any) => boolean
}

export interface RetryResult<T> {
    success: boolean
    result?: T
    error?: Error
    attempts: number
}

const hasErrorLabel = (error: any, label: string): boolean => {
    if (!error) {
        return false
    }
    if (Array.isArray(error.errorLabels) && error.errorLabels.includes(label)) {
        return true
    }
    if (error.errorLabelSet instanceof Set && error.errorLabelSet.has(label)) {
        return true
    }
    return false
}

const DEFAULT_RETRY_OPTIONS: Required<RetryOptions> = {
    maxAttempts: 5,
    initialDelay: 100,
    maxDelay: 10000,
    factor: 2,
    jitter: true,
    shouldRetry: (error: any) => {
        // Retry on connection errors and transient failures
        const errorMessage = error.message || ''
        const retryableErrors = [
            'Client must be connected',
            'Topology is closed',
            'Connection pool closed',
            'closed connection pool',
            'client was closed', // covers "Operation interrupted because client was closed"
            'ECONNREFUSED',
            'ETIMEDOUT',
            'ENETUNREACH',
            'MongoNetworkError',
            'MongoNotConnectedError',
            'MongoExpiredSessionError',
            'Cannot use a session that has ended',
            'session has ended',
            'Given transaction number',
            'NoSuchTransaction',
            'connection timed out',
            'socket hang up'
        ]
        
        return retryableErrors.some(msg => errorMessage.includes(msg)) ||
               error.code === 'ECONNREFUSED' ||
               error.code === 'ETIMEDOUT' ||
               error.code === 'ENETUNREACH' ||
               error.code === 251 ||
               error.name === 'MongoNetworkError' ||
               error.name === 'MongoNotConnectedError' ||
               error.name === 'MongoExpiredSessionError' ||
               error.name === 'MongoClientClosedError' ||
               error.name === 'MongoPoolClosedError' ||
               error.codeName === 'NoSuchTransaction' ||
               hasErrorLabel(error, 'RetryableWriteError') ||
               hasErrorLabel(error, 'TransientTransactionError')
    }
}

export async function retryWithBackoff<T>(
    operation: () => Promise<T>,
    options: RetryOptions = {},
    onRetry?: (attempt: number, error: any, delay: number) => void
): Promise<RetryResult<T>> {
    const config = { ...DEFAULT_RETRY_OPTIONS, ...options }
    let lastError: Error | undefined
    
    for (let attempt = 1; attempt <= config.maxAttempts; attempt++) {
        try {
            const result = await operation()
            return {
                success: true,
                result,
                attempts: attempt
            }
        } catch (error: any) {
            lastError = error
            
            // Check if we should retry
            if (!config.shouldRetry(error) || attempt === config.maxAttempts) {
                return {
                    success: false,
                    error: lastError,
                    attempts: attempt
                }
            }
            
            // Calculate delay with exponential backoff
            let delay = Math.min(
                config.initialDelay * Math.pow(config.factor, attempt - 1),
                config.maxDelay
            )
            
            // Add jitter to prevent thundering herd
            if (config.jitter) {
                delay = delay * (0.5 + Math.random() * 0.5)
            }
            
            // Notify about retry if callback provided
            if (onRetry) {
                onRetry(attempt, error, delay)
            }
            
            // Wait before next attempt
            await new Promise(resolve => setTimeout(resolve, delay))
        }
    }
    
    return {
        success: false,
        error: lastError || new Error('Max retry attempts reached'),
        attempts: config.maxAttempts
    }
}

export function isRetryableError(error: any): boolean {
    return DEFAULT_RETRY_OPTIONS.shouldRetry(error)
}

export function getRetryDelay(attempt: number, options: RetryOptions = {}): number {
    const config = { ...DEFAULT_RETRY_OPTIONS, ...options }
    let delay = Math.min(
        config.initialDelay * Math.pow(config.factor, attempt - 1),
        config.maxDelay
    )
    
    if (config.jitter) {
        delay = delay * (0.5 + Math.random() * 0.5)
    }
    
    return delay
}
