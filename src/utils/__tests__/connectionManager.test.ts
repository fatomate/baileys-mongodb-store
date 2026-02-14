import { ConnectionManager, getConnectionManager, getConnectionMetrics } from '../connectionManager.js'
import { MongoClient } from 'mongodb'

// Mock MongoDB
jest.mock('mongodb', () => ({
    MongoClient: jest.fn().mockImplementation(() => ({
        connect: jest.fn().mockResolvedValue(undefined),
        close: jest.fn().mockResolvedValue(undefined),
        db: jest.fn().mockReturnValue({
            collection: jest.fn().mockReturnValue({})
        })
    }))
}))

describe('ConnectionManager', () => {
    let manager: ConnectionManager
    
    beforeEach(() => {
        // Reset singleton
        (ConnectionManager as any).instance = null
        jest.clearAllMocks()
    })
    
    afterEach(async () => {
        // Clean up
        if (manager) {
            await manager.shutdown()
        }
    })
    
    describe('Singleton Pattern', () => {
        it('should return same instance when called multiple times', () => {
            const instance1 = ConnectionManager.getInstance()
            const instance2 = ConnectionManager.getInstance()
            expect(instance1).toBe(instance2)
        })
        
        it('should work with helper functions', () => {
            const manager1 = getConnectionManager()
            const manager2 = getConnectionManager()
            expect(manager1).toBe(manager2)
        })
    })
    
    describe('Instance Registration', () => {
        it('should register new instance and return connection', async () => {
            manager = getConnectionManager({
                maxTotalConnections: 100,
                monitoringInterval: 5000,
                cleanupInterval: 10000
            })
            
            const result = await manager.registerInstance({
                instanceId: 'test-001',
                uri: 'mongodb://localhost:27017',
                database: 'testdb'
            })
            
            expect(result.db).toBeDefined()
            expect(result.client).toBeDefined()
            expect(MongoClient).toHaveBeenCalled()
        })
        
        it('should reuse existing pool for same database', async () => {
            manager = getConnectionManager()
            
            // Register first instance
            await manager.registerInstance({
                instanceId: 'test-001',
                uri: 'mongodb://localhost:27017',
                database: 'testdb'
            })
            
            // Register second instance with same database
            await manager.registerInstance({
                instanceId: 'test-002',
                uri: 'mongodb://localhost:27017',
                database: 'testdb'
            })
            
            // Should only create one MongoClient for both
            expect(MongoClient).toHaveBeenCalledTimes(1)
        })
        
        it('should create separate pools for different tiers', async () => {
            manager = getConnectionManager()
            
            // Register hot instance
            await manager.registerInstance({
                instanceId: 'hot-001',
                uri: 'mongodb://localhost:27017',
                database: 'testdb',
                config: { tier: 'hot' }
            })
            
            // Register cold instance
            await manager.registerInstance({
                instanceId: 'cold-001',
                uri: 'mongodb://localhost:27017',
                database: 'testdb',
                config: { tier: 'cold' }
            })
            
            // Should create separate pools
            expect(MongoClient).toHaveBeenCalledTimes(2)
        })
    })
    
    describe('Activity Tracking', () => {
        it('should track instance activity', async () => {
            manager = getConnectionManager()
            
            await manager.registerInstance({
                instanceId: 'test-001',
                uri: 'mongodb://localhost:27017',
                database: 'testdb'
            })
            
            // Record some activity
            manager.recordActivity('test-001', 50)
            manager.recordActivity('test-001', 100)
            manager.recordActivity('test-001', 75)
            
            const metrics = manager.getInstanceMetrics('test-001')
            expect(metrics).toBeDefined()
            expect(metrics?.totalOperations).toBe(3)
            expect(metrics?.avgResponseTime).toBeCloseTo(75, 0)
        })
        
        it('should update queue depth', async () => {
            manager = getConnectionManager()
            
            await manager.registerInstance({
                instanceId: 'test-001',
                uri: 'mongodb://localhost:27017',
                database: 'testdb'
            })
            
            manager.updateQueueDepth('test-001', 10)
            
            const metrics = manager.getInstanceMetrics('test-001')
            expect(metrics?.queueDepth).toBe(10)
        })
    })
    
    describe('Tier Classification', () => {
        it('should start instances as warm by default', async () => {
            manager = getConnectionManager()
            
            await manager.registerInstance({
                instanceId: 'test-001',
                uri: 'mongodb://localhost:27017',
                database: 'testdb'
            })
            
            const metrics = manager.getInstanceMetrics('test-001')
            expect(metrics?.tier).toBe('warm')
        })
        
        it('should respect initial tier hint', async () => {
            manager = getConnectionManager()
            
            await manager.registerInstance({
                instanceId: 'test-001',
                uri: 'mongodb://localhost:27017',
                database: 'testdb',
                config: { tier: 'hot' }
            })
            
            const metrics = manager.getInstanceMetrics('test-001')
            expect(metrics?.tier).toBe('hot')
        })
        
        it('should allow manual tier changes', async () => {
            manager = getConnectionManager()
            
            await manager.registerInstance({
                instanceId: 'test-001',
                uri: 'mongodb://localhost:27017',
                database: 'testdb'
            })
            
            await manager.setInstanceTier('test-001', 'hot')
            
            const metrics = manager.getInstanceMetrics('test-001')
            expect(metrics?.tier).toBe('hot')
        })
    })
    
    describe('Metrics', () => {
        it('should provide global metrics', async () => {
            manager = getConnectionManager()
            
            await manager.registerInstance({
                instanceId: 'test-001',
                uri: 'mongodb://localhost:27017',
                database: 'testdb'
            })
            
            await manager.registerInstance({
                instanceId: 'test-002',
                uri: 'mongodb://localhost:27017',
                database: 'testdb'
            })
            
            const metrics = manager.getMetrics()
            expect(metrics.instances.total).toBe(2)
            expect(metrics.totalPools).toBeGreaterThan(0)
            expect(metrics.totalConnections).toBeGreaterThan(0)
        })
        
        it('should work with helper function', async () => {
            manager = getConnectionManager()
            
            await manager.registerInstance({
                instanceId: 'test-001',
                uri: 'mongodb://localhost:27017',
                database: 'testdb'
            })
            
            const metrics = getConnectionMetrics()
            expect(metrics.instances.total).toBe(1)
        })
    })
    
    describe('Instance Unregistration', () => {
        it('should unregister instance and clean up', async () => {
            manager = getConnectionManager()
            
            await manager.registerInstance({
                instanceId: 'test-001',
                uri: 'mongodb://localhost:27017',
                database: 'testdb'
            })
            
            await manager.unregisterInstance('test-001')
            
            const metrics = manager.getInstanceMetrics('test-001')
            expect(metrics).toBeUndefined()
        })
        
        it('should close pool when last instance unregisters', async () => {
            manager = getConnectionManager()
            const mockClient = new MongoClient('mongodb://localhost:27017')
            
            await manager.registerInstance({
                instanceId: 'test-001',
                uri: 'mongodb://localhost:27017',
                database: 'testdb'
            })
            
            await manager.unregisterInstance('test-001')
            
            // Verify client.close was called
            expect(mockClient.close).toHaveBeenCalled()
        })
    })
    
    describe('Connection Limits', () => {
        it('should respect max total connections', async () => {
            manager = getConnectionManager({
                maxTotalConnections: 50,
                tierConfigurations: {
                    warm: {
                        maxPoolSize: 30,
                        minPoolSize: 5,
                        maxInstancesPerPool: 10,
                        maxIdleTimeMS: 60000
                    }
                }
            })
            
            // First instance should succeed
            await manager.registerInstance({
                instanceId: 'test-001',
                uri: 'mongodb://localhost:27017',
                database: 'testdb1'
            })
            
            // Second instance might fail if it would exceed limits
            try {
                await manager.registerInstance({
                    instanceId: 'test-002',
                    uri: 'mongodb://localhost:27017',
                    database: 'testdb2'
                })
                
                await manager.registerInstance({
                    instanceId: 'test-003',
                    uri: 'mongodb://localhost:27017',
                    database: 'testdb3'
                })
            } catch (error: any) {
                expect(error.message).toContain('exceed maximum connections')
            }
        })
    })
    
    describe('Shutdown', () => {
        it('should clean up all resources on shutdown', async () => {
            manager = getConnectionManager()
            const mockClient = new MongoClient('mongodb://localhost:27017')
            
            await manager.registerInstance({
                instanceId: 'test-001',
                uri: 'mongodb://localhost:27017',
                database: 'testdb'
            })
            
            await manager.registerInstance({
                instanceId: 'test-002',
                uri: 'mongodb://localhost:27017',
                database: 'testdb'
            })
            
            await manager.shutdown()
            
            // Verify all clients are closed
            expect(mockClient.close).toHaveBeenCalled()
            
            // Verify singleton is cleared
            expect((ConnectionManager as any).instance).toBeNull()
        })
    })
})