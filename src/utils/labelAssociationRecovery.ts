import { MongoClient, Collection, Db } from 'mongodb'

interface LabelOperation {
    instanceId: string
    chatId: string
    normalizedChatId: string
    addLabelIds: string[]
    removeLabelIds: string[]
    updatedAt: Date
    ttl: Date
}

interface LabelAssociation {
    type: 'label_jid' | 'label_message'
    chatId: string
    labelId: string
    messageId?: string
}

interface RecoveryStats {
    operationsProcessed: number
    associationsRecovered: number
    associationsAlreadyExists: number
    errors: string[]
    processed: {
        chatId: string
        addedLabels: string[]
        skippedLabels: string[]
    }[]
}

export class LabelAssociationRecovery {
    private db: Db
    private collectionPrefix: string
    private instanceId: string

    constructor(db: Db, instanceId: string, collectionPrefix = 'baileys_') {
        this.db = db
        this.instanceId = instanceId
        this.collectionPrefix = collectionPrefix
    }

    /**
     * Recovers missing label associations from labelOperations collection
     */
    async recoverLabelAssociations(dryRun: boolean = true): Promise<RecoveryStats> {
        const stats: RecoveryStats = {
            operationsProcessed: 0,
            associationsRecovered: 0,
            associationsAlreadyExists: 0,
            errors: [],
            processed: []
        }

        try {
            const labelOperations: Collection<LabelOperation> = this.db.collection(`${this.collectionPrefix}labelOperations`)
            const labelAssociations: Collection<LabelAssociation & { instanceId: string; updatedAt: Date }> = 
                this.db.collection(`${this.collectionPrefix}labelAssociations`)

            console.log(`🔄 Starting label association recovery for instance ${this.instanceId}`)
            console.log(`Mode: ${dryRun ? 'DRY RUN (no changes will be made)' : 'LIVE RECOVERY (changes will be applied)'}`)

            // Get all label operations for this instance
            const operations = await labelOperations.find({ instanceId: this.instanceId }).toArray()
            console.log(`📊 Found ${operations.length} label operations to process`)

            for (const operation of operations) {
                try {
                    stats.operationsProcessed++
                    const processed = {
                        chatId: operation.chatId,
                        addedLabels: [] as string[],
                        skippedLabels: [] as string[]
                    }

                    // Process add operations (create missing associations)
                    for (const labelId of operation.addLabelIds || []) {
                        const association: LabelAssociation = {
                            type: 'label_jid', // Default to label_jid for chat associations
                            chatId: operation.chatId,
                            labelId: labelId
                        }

                        // Check if association already exists
                        const existing = await labelAssociations.findOne({
                            instanceId: this.instanceId,
                            type: association.type,
                            chatId: association.chatId,
                            labelId: association.labelId
                        })

                        if (existing) {
                            stats.associationsAlreadyExists++
                            processed.skippedLabels.push(labelId)
                            console.log(`⚠️  Association already exists: ${association.chatId} -> ${association.labelId}`)
                        } else {
                            // Create missing association
                            if (!dryRun) {
                                await labelAssociations.insertOne({
                                    ...association,
                                    instanceId: this.instanceId,
                                    updatedAt: new Date()
                                })
                            }
                            
                            stats.associationsRecovered++
                            processed.addedLabels.push(labelId)
                            console.log(`✅ ${dryRun ? '[DRY RUN] Would create' : 'Created'} association: ${association.chatId} -> ${association.labelId}`)
                        }
                    }

                    stats.processed.push(processed)
                    
                } catch (error) {
                    const errorMsg = `Failed to process operation for ${operation.chatId}: ${error instanceof Error ? error.message : 'Unknown error'}`
                    stats.errors.push(errorMsg)
                    console.error(`❌ ${errorMsg}`)
                }
            }

            // Print summary
            console.log('\n📈 Recovery Summary:')
            console.log(`Operations processed: ${stats.operationsProcessed}`)
            console.log(`Associations ${dryRun ? 'that would be' : ''} recovered: ${stats.associationsRecovered}`)
            console.log(`Associations already existing: ${stats.associationsAlreadyExists}`)
            console.log(`Errors encountered: ${stats.errors.length}`)

            if (stats.errors.length > 0) {
                console.log('\n❌ Errors:')
                stats.errors.forEach(error => console.log(`  - ${error}`))
            }

            if (dryRun) {
                console.log('\n🚨 This was a DRY RUN - no changes were made to the database')
                console.log('To apply changes, call recoverLabelAssociations(false)')
            }

        } catch (error) {
            const errorMsg = `Recovery failed: ${error instanceof Error ? error.message : 'Unknown error'}`
            stats.errors.push(errorMsg)
            console.error(`💥 ${errorMsg}`)
            throw error
        }

        return stats
    }

    /**
     * Analyzes the consistency between labelOperations and labelAssociations
     */
    async analyzeConsistency(): Promise<{
        operationsCount: number
        associationsCount: number
        missingAssociations: { chatId: string, labelId: string }[]
        orphanedAssociations: { chatId: string, labelId: string }[]
    }> {
        const labelOperations: Collection<LabelOperation> = this.db.collection(`${this.collectionPrefix}labelOperations`)
        const labelAssociations: Collection<LabelAssociation & { instanceId: string }> = 
            this.db.collection(`${this.collectionPrefix}labelAssociations`)

        // Get all operations and associations
        const operations = await labelOperations.find({ instanceId: this.instanceId }).toArray()
        const associations = await labelAssociations.find({ instanceId: this.instanceId }).toArray()

        console.log(`📊 Analysis for instance ${this.instanceId}:`)
        console.log(`Label Operations: ${operations.length} documents`)
        console.log(`Label Associations: ${associations.length} documents`)

        // Build expected associations from operations
        const expectedAssociations = new Set<string>()
        for (const operation of operations) {
            for (const labelId of operation.addLabelIds || []) {
                expectedAssociations.add(`${operation.chatId}:${labelId}`)
            }
        }

        // Build actual associations
        const actualAssociations = new Set<string>()
        for (const association of associations) {
            actualAssociations.add(`${association.chatId}:${association.labelId}`)
        }

        // Find missing associations (in operations but not in associations)
        const missingAssociations = Array.from(expectedAssociations)
            .filter(key => !actualAssociations.has(key))
            .map(key => {
                const [chatId, labelId] = key.split(':')
                return { chatId, labelId }
            })

        // Find orphaned associations (in associations but not in operations)
        const orphanedAssociations = Array.from(actualAssociations)
            .filter(key => !expectedAssociations.has(key))
            .map(key => {
                const [chatId, labelId] = key.split(':')
                return { chatId, labelId }
            })

        console.log(`\n🔍 Consistency Analysis:`)
        console.log(`Expected associations: ${expectedAssociations.size}`)
        console.log(`Actual associations: ${actualAssociations.size}`)
        console.log(`Missing associations: ${missingAssociations.length}`)
        console.log(`Orphaned associations: ${orphanedAssociations.length}`)

        return {
            operationsCount: operations.length,
            associationsCount: associations.length,
            missingAssociations,
            orphanedAssociations
        }
    }
}

/**
 * Standalone recovery script that can be run independently
 */
export async function runRecovery(
    mongoUri: string, 
    database: string, 
    instanceId: string, 
    collectionPrefix: string = 'baileys_',
    dryRun: boolean = true
): Promise<void> {
    const client = new MongoClient(mongoUri)
    
    try {
        await client.connect()
        const db = client.db(database)
        
        const recovery = new LabelAssociationRecovery(db, instanceId, collectionPrefix)
        
        // First analyze consistency
        console.log('🔍 Analyzing consistency...\n')
        await recovery.analyzeConsistency()
        
        console.log('\n' + '='.repeat(60))
        
        // Then run recovery
        console.log('🔧 Running recovery...\n')
        await recovery.recoverLabelAssociations(dryRun)
        
    } finally {
        await client.close()
    }
}

// Example usage if run directly
if (require.main === module) {
    const mongoUri = process.env.MONGO_URI || 'mongodb://localhost:27017'
    const database = process.env.MONGO_DB || 'baileys_store'
    const instanceId = process.env.INSTANCE_ID || 'your-instance-id'
    const dryRun = process.env.DRY_RUN !== 'false' // Default to dry run
    
    runRecovery(mongoUri, database, instanceId, 'baileys_', dryRun)
        .then(() => {
            console.log('\n✅ Recovery completed')
            process.exit(0)
        })
        .catch(error => {
            console.error('\n💥 Recovery failed:', error)
            process.exit(1)
        })
}