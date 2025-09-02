const { makeEnhancedMongoDBStore } = require('./dist')
const { MongoClient } = require('mongodb')

async function testIndexFix() {
    console.log('Testing index creation fix...\n')
    
    const mongoUrl = process.env.MONGODB_URL || 'mongodb://localhost:27017/wabot-test'
    const client = new MongoClient(mongoUrl)
    
    try {
        await client.connect()
        console.log('✅ Connected to MongoDB')
        
        const db = client.db()
        const instanceId = 'TEST_INSTANCE_' + Date.now()
        
        // Create the store which will trigger index creation
        console.log('\n📝 Creating store and indexes...')
        const store = await makeEnhancedMongoDBStore({
            db,
            instanceId,
            options: {
                prefix: 'test_',
                debug: true
            }
        })
        
        console.log('\n✅ Store created successfully')
        
        // Use the new verifyExpectedIndexes method
        console.log('\n🔍 Verifying expected indexes...')
        const verification = await store.verifyExpectedIndexes()
        
        // Check labelAssociations specifically
        const labelAssocVerification = verification.find(v => 
            v.collection.includes('labelAssociations')
        )
        
        if (labelAssocVerification) {
            console.log('\n📊 LabelAssociations Index Status:')
            console.log('  Collection:', labelAssocVerification.collection)
            console.log('  ✅ Correct indexes:', labelAssocVerification.correct)
            console.log('  ⚠️  Missing indexes:', labelAssocVerification.missing)
            console.log('  ❓ Unexpected indexes:', labelAssocVerification.unexpected)
            
            if (labelAssocVerification.missing.length === 0 && 
                labelAssocVerification.correct.includes('label_jid_unique_partial') &&
                labelAssocVerification.correct.includes('label_message_unique_partial')) {
                console.log('\n🎉 SUCCESS: All expected partial unique indexes are present!')
            } else {
                console.log('\n⚠️  WARNING: Some expected indexes are missing')
            }
        }
        
        // Get detailed index status
        console.log('\n📋 Detailed Index Status:')
        const indexStatus = await store.getIndexStatus()
        const labelAssocStatus = indexStatus.find(s => 
            s.collection.includes('labelAssociations')
        )
        
        if (labelAssocStatus) {
            console.log('\nLabelAssociations indexes:')
            labelAssocStatus.indexes.forEach(idx => {
                console.log(`  - ${idx.name}: ${JSON.stringify(idx.key)}`)
                if (idx.unique) console.log('    (unique)')
                if (idx.partialFilterExpression) {
                    console.log('    Partial filter:', JSON.stringify(idx.partialFilterExpression))
                }
            })
        }
        
        // Clean up test collections
        console.log('\n🧹 Cleaning up test collections...')
        const collections = await db.listCollections({ name: { $regex: '^test_' } }).toArray()
        for (const coll of collections) {
            await db.dropCollection(coll.name)
            console.log(`  Dropped: ${coll.name}`)
        }
        
        console.log('\n✅ Test completed successfully!')
        
    } catch (error) {
        console.error('\n❌ Test failed:', error)
        process.exit(1)
    } finally {
        await client.close()
    }
}

// Run the test
testIndexFix().catch(console.error)