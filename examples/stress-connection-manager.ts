import { getConnectionManager } from '../src/utils/connectionManager'
import type { ConnectionManagerConfig } from '../src/types/connection'

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

async function main() {
    const uri = process.env.MONGO_URI
    const database = process.env.MONGO_DB || 'baileys'
    const instanceCount = Number(process.env.INSTANCE_COUNT || 2000)
    const registrationBatch = Number(process.env.INSTANCE_BATCH || 50)
    const monitorWindowMs = Number(process.env.MONITOR_WINDOW_MS || 5000)

    if (!uri) {
        console.error('MONGO_URI env var is required for stress harness')
        process.exit(1)
    }

    const managerConfig: ConnectionManagerConfig = {
        maxTotalConnections: Number(process.env.MAX_CONNECTIONS || 900),
        tierConfigurations: {
            hot: {
                maxPoolSize: 60,
                minPoolSize: 24,
                maxInstancesPerPool: 25,
                maxIdleTimeMS: 60000
            },
            warm: {
                maxPoolSize: 24,
                minPoolSize: 8,
                maxInstancesPerPool: 60,
                maxIdleTimeMS: 120000
            },
            cold: {
                maxPoolSize: 10,
                minPoolSize: 2,
                maxInstancesPerPool: 150,
                maxIdleTimeMS: 300000
            }
        },
        logLevel: 'info',
        enableMetrics: true
    }

    const manager = getConnectionManager(managerConfig)

    console.log(`Registering ${instanceCount} instances with batch size ${registrationBatch}`)

    const start = Date.now()
    const registrations: Promise<unknown>[] = []
    for (let i = 0; i < instanceCount; i++) {
        const instanceId = `stress-${i.toString().padStart(4, '0')}`
        registrations.push(
            manager.registerInstance({
                instanceId,
                uri,
                database,
                config: { tier: 'warm' }
            })
        )

        if (registrations.length >= registrationBatch) {
            await Promise.all(registrations)
            registrations.length = 0
        }
    }

    if (registrations.length) {
        await Promise.all(registrations)
    }

    const duration = Date.now() - start
    console.log(`Registration completed in ${duration}ms`)

    const metrics = manager.getMetrics()
    console.log('Connection metrics after registration:', JSON.stringify(metrics, null, 2))

    console.log('Monitoring utilisation...')
    await sleep(monitorWindowMs)
    console.log('Final metrics:', JSON.stringify(manager.getMetrics(), null, 2))

    console.log('Cleaning up stress instances...')
    const unregisterBatch = Number(process.env.UNREGISTER_BATCH || registrationBatch)
    const unregisterPromises: Promise<void>[] = []
    for (let i = 0; i < instanceCount; i++) {
        const instanceId = `stress-${i.toString().padStart(4, '0')}`
        unregisterPromises.push(manager.unregisterInstance(instanceId))
        if (unregisterPromises.length >= unregisterBatch) {
            await Promise.all(unregisterPromises)
            unregisterPromises.length = 0
        }
    }
    if (unregisterPromises.length) {
        await Promise.all(unregisterPromises)
    }

    await manager.shutdown()
    console.log('Stress test finished')
}

main().catch(error => {
    console.error('Stress harness failed', error)
    process.exit(1)
})
