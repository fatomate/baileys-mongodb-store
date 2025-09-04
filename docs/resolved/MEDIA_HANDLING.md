# Media Handling Feature

## Overview

The Enhanced MongoDB Store now includes an optional media download feature that automatically downloads and stores media files from WhatsApp messages to your server's file system. Downloaded media URLs are then stored in the MongoDB database for easy reference.

## Features

- **Automatic Media Download**: Automatically downloads media from incoming messages
- **Instance-based Storage**: Organizes media files by instance ID
- **Type-based Organization**: Separates media by type (image, video, audio, document, sticker)
- **Database Integration**: Updates message documents with local media URLs
- **Configurable Limits**: Set file size limits and allowed media types
- **Retry Logic**: Automatic retry on download failures
- **Cleanup Utilities**: Remove old media files to manage disk space
- **Statistics**: Track media storage usage per instance

## Configuration

### Basic Setup

```typescript
import { makeEnhancedMongoDBStore } from '@baileys/mongodb-store'

const store = await makeEnhancedMongoDBStore({
    uri: 'mongodb://localhost:27017',
    database: 'whatsapp_db',
    instanceId: 'instance_123',
    media: {
        enabled: true,
        baseDir: '/path/to/media/storage',
        maxSizeInMB: 50,
        allowedTypes: ['image', 'video', 'audio', 'document'],
        skipGroupMessages: false,
        maxRetries: 3,
        retryDelay: 1000,
        downloadTimeout: 30000
    }
})
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | - | Enable/disable media download functionality |
| `baseDir` | string | - | Base directory for storing media files |
| `maxSizeInMB` | number | 0 (unlimited) | Maximum file size in MB |
| `allowedTypes` | MediaType[] | all types | Types of media to download |
| `skipGroupMessages` | boolean | false | Skip downloading media from group messages |
| `maxRetries` | number | 3 | Number of retry attempts for failed downloads |
| `retryDelay` | number | 1000 | Delay between retries in milliseconds |
| `downloadTimeout` | number | 30000 | Timeout for download operations in milliseconds |

### Media Types

- `'image'` - Images (JPEG, PNG, etc.)
- `'video'` - Video files
- `'audio'` - Audio messages and files
- `'document'` - Documents (PDF, DOC, etc.)
- `'sticker'` - WhatsApp stickers

## File Organization

Media files are organized in the following structure:

```
baseDir/
├── instance_123/
│   ├── image/
│   │   ├── 1234567890_msg123_image.jpg
│   │   └── 1234567891_msg124_image.png
│   ├── video/
│   │   └── 1234567892_msg125_video.mp4
│   ├── audio/
│   │   └── 1234567893_msg126_audio.mp3
│   ├── document/
│   │   └── 1234567894_msg127_report.pdf
│   └── sticker/
│       └── 1234567895_msg128_sticker.webp
└── instance_456/
    └── ...
```

## Database Schema

When media is downloaded, the message document in MongoDB is updated with the following fields:

```typescript
{
    // ... existing message fields
    mediaUrl: string,           // Relative path to downloaded file
    mediaType: string,          // Type of media (image, video, etc.)
    mediaFileName: string,      // Generated filename
    mediaFileSize: number,      // File size in bytes
    mediaDownloadedAt: Date     // Timestamp of download
}
```

## API Methods

### Download Media for Specific Message

```typescript
const result = await store.downloadMessageMedia(
    'user@s.whatsapp.net',  // JID
    'MESSAGE_ID_123'        // Message ID
)

if (result.success) {
    console.log('Media downloaded to:', result.localPath)
} else {
    console.error('Download failed:', result.error)
}
```

### Get Media Statistics

```typescript
const stats = await store.getMediaStats()

console.log('Total files:', stats.totalFiles)
console.log('Total size:', stats.totalSize)
console.log('By type:', stats.byType)
// Output:
// {
//   image: { count: 150, size: 52428800 },
//   video: { count: 20, size: 209715200 },
//   ...
// }
```

### Cleanup Old Media

```typescript
// Remove media files older than 30 days
const result = await store.cleanupOldMedia(30)

console.log('Deleted files:', result.deleted)
console.log('Errors:', result.errors)
```

## Usage Examples

### Example 1: Basic Media Download

```typescript
const store = await makeEnhancedMongoDBStore({
    uri: 'mongodb://localhost:27017',
    database: 'whatsapp_db',
    instanceId: 'business_account',
    media: {
        enabled: true,
        baseDir: '/var/whatsapp/media'
    }
})

// Media will be automatically downloaded when messages are received
sock.ev.on('messages.upsert', async ({ messages }) => {
    // Media download happens automatically via the store
    for (const msg of messages) {
        console.log('Message received:', msg.key.id)
    }
})
```

### Example 2: Selective Media Download

```typescript
const store = await makeEnhancedMongoDBStore({
    uri: 'mongodb://localhost:27017',
    database: 'whatsapp_db',
    instanceId: 'selective_account',
    media: {
        enabled: true,
        baseDir: '/var/whatsapp/media',
        maxSizeInMB: 10,                    // Only files under 10MB
        allowedTypes: ['image', 'document'], // Only images and documents
        skipGroupMessages: true              // Skip group media
    }
})
```

### Example 3: Media Management Script

```typescript
// Periodic cleanup script
async function mediaManagement() {
    const store = await makeEnhancedMongoDBStore({
        uri: 'mongodb://localhost:27017',
        database: 'whatsapp_db',
        instanceId: 'managed_account',
        media: {
            enabled: true,
            baseDir: '/var/whatsapp/media'
        }
    })
    
    // Get current statistics
    const stats = await store.getMediaStats()
    console.log('Current media usage:', {
        files: stats.totalFiles,
        sizeGB: (stats.totalSize / (1024 * 1024 * 1024)).toFixed(2)
    })
    
    // Cleanup old files (older than 15 days)
    const cleanup = await store.cleanupOldMedia(15)
    console.log('Cleanup completed:', cleanup)
    
    // Get updated statistics
    const newStats = await store.getMediaStats()
    console.log('After cleanup:', {
        files: newStats.totalFiles,
        sizeGB: (newStats.totalSize / (1024 * 1024 * 1024)).toFixed(2)
    })
    
    await store.close()
}

// Run daily
setInterval(mediaManagement, 24 * 60 * 60 * 1000)
```

## Integration with Application Code

Based on your application code pattern, here's how to integrate with the enhanced store:

```typescript
// In your application
async function handleMediaMessage(message, instance_id) {
    // The store automatically handles media download
    // You can access the downloaded media URL from the database
    
    const db = await MongoClient.connect(uri)
    const collection = db.collection('baileys_messages')
    
    const storedMessage = await collection.findOne({
        instanceId: instance_id,
        'key.id': message.key.id
    })
    
    if (storedMessage?.mediaUrl) {
        console.log('Media available at:', storedMessage.mediaUrl)
        // You can now serve this file via your API
        // or process it further as needed
    }
}
```

## Best Practices

1. **Storage Planning**: Ensure adequate disk space for media storage
2. **Regular Cleanup**: Implement periodic cleanup to manage disk usage
3. **Access Control**: Secure the media directory with appropriate permissions
4. **Backup Strategy**: Include media directories in your backup plan
5. **Monitoring**: Monitor disk usage and set up alerts for low space
6. **CDN Integration**: Consider serving media through a CDN for better performance

## Security Considerations

1. **File Permissions**: Set appropriate permissions on the media directory
2. **Path Validation**: The system validates and sanitizes file paths
3. **File Type Validation**: Only configured media types are downloaded
4. **Size Limits**: Enforce size limits to prevent storage abuse
5. **Access Control**: Implement proper authentication when serving media files

## Troubleshooting

### Media Not Downloading

1. Check if media download is enabled in configuration
2. Verify the base directory exists and is writable
3. Check file size limits and allowed types
4. Review logs for download errors

### Storage Issues

1. Monitor disk space regularly
2. Adjust cleanup schedule based on usage
3. Consider increasing storage or using external storage
4. Implement compression for older files

### Performance Optimization

1. Use SSD storage for better I/O performance
2. Consider implementing a queue for large media files
3. Use separate storage volumes for media
4. Implement caching for frequently accessed files

## Migration Guide

If you're upgrading from a version without media support:

1. Update your configuration to include media settings
2. Create the media storage directory structure
3. Set appropriate permissions
4. Update your application code to utilize media URLs
5. Consider downloading historical media if needed

## API Reference

### MediaConfig Interface

```typescript
interface MediaConfig {
    enabled: boolean
    baseDir: string
    maxSizeInMB?: number
    allowedTypes?: MediaType[]
    skipGroupMessages?: boolean
    maxRetries?: number
    retryDelay?: number
    downloadTimeout?: number
}
```

### MediaDownloadResult Interface

```typescript
interface MediaDownloadResult {
    success: boolean
    localPath?: string
    mediaType?: MediaType
    fileName?: string
    fileSize?: number
    error?: string
    retries?: number
}
```

### MediaType

```typescript
type MediaType = 'image' | 'video' | 'audio' | 'document' | 'sticker'
```