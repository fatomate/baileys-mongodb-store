import {
    createAccessToken,
    verifyAccessToken,
    checkPermission,
    generateApiKey,
    isValidApiKey,
    validateApiKey,
    InstanceAccessContext,
    SecureSession,
    AuthorizationError,
    DEFAULT_PERMISSIONS,
    ADMIN_PERMISSIONS
} from '../auth.js'

describe('Authentication and Authorization', () => {
    const SECRET_KEY = 'test-secret-key-12345'
    
    describe('Access Tokens', () => {
        test('should create valid access token', () => {
            const token = createAccessToken('instance-1', DEFAULT_PERMISSIONS, SECRET_KEY, 1)
            
            expect(token.instanceId).toBe('instance-1')
            expect(token.permissions).toEqual(DEFAULT_PERMISSIONS)
            expect(token.expiresAt).toBeInstanceOf(Date)
            expect(token.signature).toBeTruthy()
            expect(token.signature).toHaveLength(64) // SHA256 hex length
        })

        test('should verify valid token', () => {
            const token = createAccessToken('instance-1', DEFAULT_PERMISSIONS, SECRET_KEY, 1)
            
            expect(() => verifyAccessToken(token, SECRET_KEY)).not.toThrow()
            expect(verifyAccessToken(token, SECRET_KEY)).toBe(true)
        })

        test('should reject expired token', () => {
            const token = createAccessToken('instance-1', DEFAULT_PERMISSIONS, SECRET_KEY, -1) // Expired 1 hour ago
            
            expect(() => verifyAccessToken(token, SECRET_KEY)).toThrow(AuthorizationError)
            expect(() => verifyAccessToken(token, SECRET_KEY)).toThrow('Token expired')
        })

        test('should reject token with invalid signature', () => {
            const token = createAccessToken('instance-1', DEFAULT_PERMISSIONS, SECRET_KEY, 1)
            token.signature = 'invalid-signature'
            
            expect(() => verifyAccessToken(token, SECRET_KEY)).toThrow(AuthorizationError)
            expect(() => verifyAccessToken(token, SECRET_KEY)).toThrow('Invalid token signature')
        })

        test('should reject token with wrong secret', () => {
            const token = createAccessToken('instance-1', DEFAULT_PERMISSIONS, SECRET_KEY, 1)
            
            expect(() => verifyAccessToken(token, 'wrong-secret')).toThrow(AuthorizationError)
        })
    })

    describe('Permission Checking', () => {
        test('should allow own instance access with correct permission', () => {
            expect(() => checkPermission('instance-1', 'instance-1', 'read', ['read:own'])).not.toThrow()
            expect(() => checkPermission('instance-1', 'instance-1', 'write', ['write:own'])).not.toThrow()
            expect(() => checkPermission('instance-1', 'instance-1', 'delete', ['delete:own'])).not.toThrow()
        })

        test('should deny own instance access without permission', () => {
            expect(() => checkPermission('instance-1', 'instance-1', 'write', ['read:own'])).toThrow(AuthorizationError)
            expect(() => checkPermission('instance-1', 'instance-1', 'delete', ['read:own', 'write:own'])).toThrow(AuthorizationError)
        })

        test('should deny different instance access with own permission', () => {
            expect(() => checkPermission('instance-1', 'instance-2', 'read', ['read:own'])).toThrow(AuthorizationError)
        })

        test('should allow any instance access with admin permission', () => {
            expect(() => checkPermission('instance-1', 'instance-2', 'read', ['read:all'])).not.toThrow()
            expect(() => checkPermission('instance-1', 'instance-3', 'write', ['write:all'])).not.toThrow()
            expect(() => checkPermission('instance-1', 'instance-4', 'delete', ['delete:all'])).not.toThrow()
        })

        test('should work with full admin permissions', () => {
            expect(() => checkPermission('admin', 'any-instance', 'read', ADMIN_PERMISSIONS)).not.toThrow()
            expect(() => checkPermission('admin', 'any-instance', 'write', ADMIN_PERMISSIONS)).not.toThrow()
            expect(() => checkPermission('admin', 'any-instance', 'delete', ADMIN_PERMISSIONS)).not.toThrow()
        })
    })

    describe('API Key Management', () => {
        test('should generate valid API key', () => {
            const apiKey = generateApiKey()
            
            expect(apiKey).toMatch(/^bmdb_[a-f0-9]{64}$/)
            expect(apiKey).toHaveLength(69) // bmdb_ (5) + 64 hex chars
        })

        test('should generate unique API keys', () => {
            const key1 = generateApiKey()
            const key2 = generateApiKey()
            
            expect(key1).not.toBe(key2)
        })

        test('should validate API key format', () => {
            const validKey = 'bmdb_' + 'a'.repeat(64)
            const invalidKeys = [
                'wrong_prefix_' + 'a'.repeat(64),
                'bmdb_' + 'a'.repeat(63), // too short
                'bmdb_' + 'a'.repeat(65), // too long
                'bmdb_' + 'g'.repeat(64), // invalid hex char
                'bmdb_UPPERCASE' + 'a'.repeat(52), // uppercase not allowed
                'not-an-api-key'
            ]
            
            expect(isValidApiKey(validKey)).toBe(true)
            invalidKeys.forEach(key => {
                expect(isValidApiKey(key)).toBe(false)
            })
        })

        test('should validate API key with mapping', () => {
            const apiKeyMap = new Map([
                ['bmdb_abc123' + '0'.repeat(58), 'instance-1'],
                ['bmdb_def456' + '0'.repeat(58), 'instance-2']
            ])
            
            expect(validateApiKey('bmdb_abc123' + '0'.repeat(58), apiKeyMap)).toBe('instance-1')
            expect(validateApiKey('bmdb_def456' + '0'.repeat(58), apiKeyMap)).toBe('instance-2')
        })

        test('should reject invalid API key format', () => {
            const apiKeyMap = new Map([['bmdb_valid' + '0'.repeat(59), 'instance-1']])
            
            expect(() => validateApiKey('invalid-key', apiKeyMap)).toThrow(AuthorizationError)
            expect(() => validateApiKey('invalid-key', apiKeyMap)).toThrow('Invalid API key format')
        })

        test('should reject unknown API key', () => {
            const apiKeyMap = new Map([['bmdb_' + '0'.repeat(64), 'instance-1']])
            const unknownKey = 'bmdb_' + 'f'.repeat(64)
            
            expect(() => validateApiKey(unknownKey, apiKeyMap)).toThrow(AuthorizationError)
            expect(() => validateApiKey(unknownKey, apiKeyMap)).toThrow('API key not found')
        })
    })

    describe('InstanceAccessContext', () => {
        test('should validate access for same instance', () => {
            const context = new InstanceAccessContext('instance-1', DEFAULT_PERMISSIONS)
            
            expect(() => context.validateAccess('instance-1', 'read')).not.toThrow()
            expect(() => context.validateAccess('instance-1', 'write')).not.toThrow()
            expect(() => context.validateAccess('instance-1', 'delete')).not.toThrow()
        })

        test('should deny access to different instance', () => {
            const context = new InstanceAccessContext('instance-1', DEFAULT_PERMISSIONS)
            
            expect(() => context.validateAccess('instance-2', 'read')).toThrow(AuthorizationError)
        })

        test('should filter data by instance', () => {
            const context = new InstanceAccessContext('instance-1', DEFAULT_PERMISSIONS)
            
            const data = [
                { id: 1, instanceId: 'instance-1', name: 'Item 1' },
                { id: 2, instanceId: 'instance-2', name: 'Item 2' },
                { id: 3, instanceId: 'instance-1', name: 'Item 3' },
                { id: 4, instanceId: 'instance-3', name: 'Item 4' }
            ]
            
            const filtered = context.filterData(data)
            
            expect(filtered).toHaveLength(2)
            expect(filtered.every(item => item.instanceId === 'instance-1')).toBe(true)
        })

        test('should not filter data with admin permissions', () => {
            const context = new InstanceAccessContext('admin', ['read:all'])
            
            const data = [
                { id: 1, instanceId: 'instance-1' },
                { id: 2, instanceId: 'instance-2' },
                { id: 3, instanceId: 'instance-3' }
            ]
            
            const filtered = context.filterData(data)
            
            expect(filtered).toHaveLength(3)
        })

        test('should check for allowed instances', () => {
            const authConfig = {
                allowedInstances: ['instance-1', 'instance-2']
            }
            const context = new InstanceAccessContext('instance-3', DEFAULT_PERMISSIONS, authConfig)
            
            expect(() => context.validateAccess('instance-3', 'read')).toThrow(AuthorizationError)
            expect(() => context.validateAccess('instance-3', 'read')).toThrow('Instance not in allowed list')
        })

        test('should check specific permissions', () => {
            const context = new InstanceAccessContext('instance-1', ['read:own', 'custom:permission'])
            
            expect(context.hasPermission('read:own')).toBe(true)
            expect(context.hasPermission('custom:permission')).toBe(true)
            expect(context.hasPermission('write:own')).toBe(false)
        })

        test('should get instance ID', () => {
            const context = new InstanceAccessContext('my-instance', DEFAULT_PERMISSIONS)
            
            expect(context.getInstanceId()).toBe('my-instance')
        })
    })

    describe('SecureSession', () => {
        beforeEach(() => {
            // Clear any existing sessions
            SecureSession['sessions'].clear()
        })

        test('should create new session', () => {
            const sessionId = SecureSession.create('instance-1', 60)
            
            expect(sessionId).toBeTruthy()
            expect(sessionId).toHaveLength(64) // 32 bytes in hex
        })

        test('should validate valid session', () => {
            const sessionId = SecureSession.create('instance-1', 60)
            
            expect(SecureSession.validate(sessionId)).toBe('instance-1')
        })

        test('should reject invalid session', () => {
            expect(() => SecureSession.validate('invalid-session-id')).toThrow(AuthorizationError)
            expect(() => SecureSession.validate('invalid-session-id')).toThrow('Invalid session')
        })

        test('should reject expired session', async () => {
            // Create a session with very short TTL
            const sessionId = SecureSession.create('instance-1', 0.001) // Expires in 0.001 minutes (60ms)
            
            // Validate immediately (should work)
            expect(SecureSession.validate(sessionId)).toBe('instance-1')
            
            // Wait for it to expire
            await new Promise(resolve => setTimeout(resolve, 100))
            
            // Now it should be expired
            expect(() => SecureSession.validate(sessionId)).toThrow(AuthorizationError)
            // The error message could be either 'Invalid session' (if cleaned up) or 'Session expired'
            expect(() => SecureSession.validate(sessionId)).toThrow(/Invalid session|Session expired/)
        })

        test('should destroy session', () => {
            const sessionId = SecureSession.create('instance-1', 60)
            
            SecureSession.destroy(sessionId)
            
            expect(() => SecureSession.validate(sessionId)).toThrow(AuthorizationError)
            expect(() => SecureSession.validate(sessionId)).toThrow('Invalid session')
        })

        test('should create unique sessions', () => {
            const session1 = SecureSession.create('instance-1', 60)
            const session2 = SecureSession.create('instance-1', 60)
            
            expect(session1).not.toBe(session2)
        })

        test('should clean up expired sessions automatically', () => {
            // Create one valid and one expired session
            const validSession = SecureSession.create('instance-1', 60)
            const expiredSession = SecureSession.create('instance-2', -1)
            
            // Try to validate expired session (triggers cleanup)
            expect(() => SecureSession.validate(expiredSession)).toThrow()
            
            // Valid session should still work
            expect(SecureSession.validate(validSession)).toBe('instance-1')
            
            // Expired session should be gone
            expect(() => SecureSession.validate(expiredSession)).toThrow('Invalid session')
        })
    })
})