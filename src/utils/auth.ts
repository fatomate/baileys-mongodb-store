import { createHmac, randomBytes } from 'crypto'
import { AuthorizationError } from './security.js'

// Re-export for convenience
export { AuthorizationError } from './security.js'

/**
 * Authentication and authorization utilities
 */

export interface AuthConfig {
    /**
     * Secret key for signing tokens (should be from environment)
     */
    secretKey?: string
    
    /**
     * Enable strict instance isolation
     */
    strictIsolation?: boolean
    
    /**
     * Allowed instance IDs (whitelist)
     */
    allowedInstances?: string[]
    
    /**
     * Enable API key authentication
     */
    enableApiKey?: boolean
    
    /**
     * API key to instance mapping
     */
    apiKeys?: Map<string, string>
}

export interface AccessToken {
    instanceId: string
    permissions: string[]
    expiresAt: Date
    signature: string
}

/**
 * Default permissions for instances
 */
export const DEFAULT_PERMISSIONS = [
    'read:own',
    'write:own',
    'delete:own'
]

/**
 * Admin permissions
 */
export const ADMIN_PERMISSIONS = [
    ...DEFAULT_PERMISSIONS,
    'read:all',
    'write:all',
    'delete:all',
    'admin:instances'
]

/**
 * Creates an access token for an instance
 * @param instanceId - The instance ID
 * @param permissions - Array of permissions
 * @param secretKey - Secret key for signing
 * @param expiresInHours - Token expiration in hours
 * @returns Access token
 */
export const createAccessToken = (
    instanceId: string,
    permissions: string[],
    secretKey: string,
    expiresInHours: number = 24
): AccessToken => {
    const expiresAt = new Date()
    expiresAt.setHours(expiresAt.getHours() + expiresInHours)
    
    const tokenData = {
        instanceId,
        permissions,
        expiresAt: expiresAt.toISOString()
    }
    
    const signature = createHmac('sha256', secretKey)
        .update(JSON.stringify(tokenData))
        .digest('hex')
    
    return {
        instanceId,
        permissions,
        expiresAt,
        signature
    }
}

/**
 * Verifies an access token
 * @param token - The token to verify
 * @param secretKey - Secret key for verification
 * @throws AuthorizationError if invalid
 * @returns true if valid
 */
export const verifyAccessToken = (token: AccessToken, secretKey: string): boolean => {
    // Check expiration
    if (new Date() > new Date(token.expiresAt)) {
        throw new AuthorizationError('Token expired')
    }
    
    // Verify signature
    const tokenData = {
        instanceId: token.instanceId,
        permissions: token.permissions,
        expiresAt: token.expiresAt.toISOString()
    }
    
    const expectedSignature = createHmac('sha256', secretKey)
        .update(JSON.stringify(tokenData))
        .digest('hex')
    
    if (token.signature !== expectedSignature) {
        throw new AuthorizationError('Invalid token signature')
    }
    
    return true
}

/**
 * Checks if an instance has permission for an operation
 * @param instanceId - The instance ID
 * @param targetInstanceId - The target instance ID
 * @param operation - The operation (read, write, delete)
 * @param permissions - Array of permissions
 * @throws AuthorizationError if not authorized
 */
export const checkPermission = (
    instanceId: string,
    targetInstanceId: string,
    operation: string,
    permissions: string[]
): void => {
    // Check for admin permission
    if (permissions.includes(`${operation}:all`)) {
        return
    }
    
    // Check for own instance permission
    if (instanceId === targetInstanceId && permissions.includes(`${operation}:own`)) {
        return
    }
    
    throw new AuthorizationError(`Permission denied for ${operation} operation`)
}

/**
 * Generates a secure API key
 * @returns A secure random API key
 */
export const generateApiKey = (): string => {
    return `bmdb_${randomBytes(32).toString('hex')}`
}

/**
 * Validates an API key format
 * @param apiKey - The API key to validate
 * @returns true if valid format
 */
export const isValidApiKey = (apiKey: string): boolean => {
    return /^bmdb_[a-f0-9]{64}$/.test(apiKey)
}

/**
 * Creates an instance access context
 */
export class InstanceAccessContext {
    private instanceId: string
    private permissions: string[]
    private config: AuthConfig
    
    constructor(instanceId: string, permissions: string[], config: AuthConfig = {}) {
        this.instanceId = instanceId
        this.permissions = permissions
        this.config = config
    }
    
    /**
     * Validates access to a resource
     * @param resourceInstanceId - The instance ID of the resource
     * @param operation - The operation being performed
     * @throws AuthorizationError if access denied
     */
    validateAccess(resourceInstanceId: string, operation: 'read' | 'write' | 'delete'): void {
        // Check if instance is in whitelist
        if (this.config.allowedInstances && !this.config.allowedInstances.includes(this.instanceId)) {
            throw new AuthorizationError('Instance not in allowed list')
        }
        
        // Check permissions
        checkPermission(this.instanceId, resourceInstanceId, operation, this.permissions)
    }
    
    /**
     * Filters data based on access permissions
     * @param data - Array of data with instanceId property
     * @returns Filtered data
     */
    filterData<T extends { instanceId: string }>(data: T[]): T[] {
        if (this.permissions.includes('read:all')) {
            return data
        }
        
        return data.filter(item => item.instanceId === this.instanceId)
    }
    
    /**
     * Gets the current instance ID
     */
    getInstanceId(): string {
        return this.instanceId
    }
    
    /**
     * Checks if user has a specific permission
     */
    hasPermission(permission: string): boolean {
        return this.permissions.includes(permission)
    }
}

/**
 * Middleware for validating API keys
 * @param apiKey - The API key from request
 * @param apiKeyMap - Map of API keys to instance IDs
 * @throws AuthorizationError if invalid
 * @returns Instance ID associated with the API key
 */
export const validateApiKey = (apiKey: string, apiKeyMap: Map<string, string>): string => {
    if (!isValidApiKey(apiKey)) {
        throw new AuthorizationError('Invalid API key format')
    }
    
    const instanceId = apiKeyMap.get(apiKey)
    if (!instanceId) {
        throw new AuthorizationError('API key not found')
    }
    
    return instanceId
}

/**
 * Creates a secure instance session
 */
export class SecureSession {
    private static sessions = new Map<string, { instanceId: string, expiresAt: Date }>()
    
    /**
     * Creates a new session
     * @param instanceId - The instance ID
     * @param ttlMinutes - Session TTL in minutes
     * @returns Session ID
     */
    static create(instanceId: string, ttlMinutes: number = 60): string {
        const sessionId = randomBytes(32).toString('hex')
        const expiresAt = new Date()
        expiresAt.setMinutes(expiresAt.getMinutes() + ttlMinutes)
        
        this.sessions.set(sessionId, { instanceId, expiresAt })
        
        // Clean up expired sessions
        this.cleanup()
        
        return sessionId
    }
    
    /**
     * Validates a session
     * @param sessionId - The session ID
     * @throws AuthorizationError if invalid
     * @returns Instance ID
     */
    static validate(sessionId: string): string {
        const session = this.sessions.get(sessionId)
        
        if (!session) {
            throw new AuthorizationError('Invalid session')
        }
        
        if (new Date() > session.expiresAt) {
            this.sessions.delete(sessionId)
            throw new AuthorizationError('Session expired')
        }
        
        return session.instanceId
    }
    
    /**
     * Destroys a session
     * @param sessionId - The session ID
     */
    static destroy(sessionId: string): void {
        this.sessions.delete(sessionId)
    }
    
    /**
     * Cleans up expired sessions
     */
    private static cleanup(): void {
        const now = new Date()
        for (const [sessionId, session] of this.sessions.entries()) {
            if (now > session.expiresAt) {
                this.sessions.delete(sessionId)
            }
        }
    }
}