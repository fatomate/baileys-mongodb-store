import { downloadContentFromMessage } from 'baileys'
import type { proto } from 'baileys'
import { createWriteStream, promises as fs } from 'fs'
import { join } from 'path'
import { pipeline } from 'stream/promises'
import type { Logger } from 'pino'
import axios from 'axios'

export interface MediaConfig {
    /**
     * Enable media download functionality
     */
    enabled: boolean
    
    /**
     * Base directory for storing media files
     */
    baseDir: string
    
    /**
     * Maximum file size in MB (0 = unlimited)
     */
    maxSizeInMB?: number
    
    /**
     * File types to download (empty = all types)
     */
    allowedTypes?: MediaType[]
    
    /**
     * Skip group messages
     */
    skipGroupMessages?: boolean
    
    /**
     * Number of retry attempts for failed downloads
     */
    maxRetries?: number
    
    /**
     * Delay between retries in milliseconds
     */
    retryDelay?: number
    
    /**
     * Timeout for download operations in milliseconds
     */
    downloadTimeout?: number
    
    /**
     * Official WhatsApp API configuration
     */
    officialAPI?: {
        /**
         * Function to get account data for a given instance
         */
        getAccountData?: (instanceId: string) => Promise<OfficialAPIAccountData | null>
    }
}

export interface OfficialAPIAccountData {
    loginType: number
    status: number
    tmp: string
    data: string
}

export type MediaType = 'image' | 'video' | 'audio' | 'document' | 'sticker'

export interface MediaDownloadResult {
    success: boolean
    localPath?: string
    mediaType?: MediaType
    fileName?: string
    fileSize?: number
    error?: string
    retries?: number
    mediaHash?: string
    reused?: boolean
}

export interface MediaInfo {
    type: MediaType
    message: proto.Message.IImageMessage | proto.Message.IVideoMessage | proto.Message.IAudioMessage | proto.Message.IDocumentMessage | proto.Message.IStickerMessage
    mimetype?: string
    filename?: string
    caption?: string
}

/**
 * Extract media information from a WhatsApp message
 */
export function extractMediaInfo(message: proto.IWebMessageInfo): MediaInfo | null {
    const msg = message.message
    if (!msg) return null
    
    // Handle documentWithCaptionMessage (nested structure)
    if (msg.documentWithCaptionMessage?.message?.documentMessage) {
        const docMsg = msg.documentWithCaptionMessage.message.documentMessage
        return {
            type: 'document',
            message: docMsg,
            mimetype: docMsg.mimetype || undefined,
            filename: docMsg.fileName || undefined,
            caption: docMsg.caption || undefined
        }
    }
    
    // Handle regular image message
    if (msg.imageMessage) {
        return {
            type: 'image',
            message: msg.imageMessage,
            mimetype: msg.imageMessage.mimetype || undefined,
            caption: msg.imageMessage.caption || undefined
        }
    }
    
    // Handle quoted image message
    if (msg.extendedTextMessage?.contextInfo?.quotedMessage?.imageMessage) {
        const imgMsg = msg.extendedTextMessage.contextInfo.quotedMessage.imageMessage
        return {
            type: 'image',
            message: imgMsg,
            mimetype: imgMsg.mimetype || undefined,
            caption: imgMsg.caption || undefined
        }
    }
    
    // Handle regular video message
    if (msg.videoMessage) {
        return {
            type: 'video',
            message: msg.videoMessage,
            mimetype: msg.videoMessage.mimetype || undefined,
            caption: msg.videoMessage.caption || undefined
        }
    }
    
    // Handle quoted video message
    if (msg.extendedTextMessage?.contextInfo?.quotedMessage?.videoMessage) {
        const vidMsg = msg.extendedTextMessage.contextInfo.quotedMessage.videoMessage
        return {
            type: 'video',
            message: vidMsg,
            mimetype: vidMsg.mimetype || undefined,
            caption: vidMsg.caption || undefined
        }
    }
    
    if (msg.audioMessage) {
        return {
            type: 'audio',
            message: msg.audioMessage,
            mimetype: msg.audioMessage.mimetype || undefined
        }
    }
    
    if (msg.documentMessage) {
        return {
            type: 'document',
            message: msg.documentMessage,
            mimetype: msg.documentMessage.mimetype || undefined,
            filename: msg.documentMessage.fileName || undefined
        }
    }
    
    if (msg.stickerMessage) {
        return {
            type: 'sticker',
            message: msg.stickerMessage,
            mimetype: msg.stickerMessage.mimetype || undefined
        }
    }
    
    return null
}

/**
 * Get file extension based on mimetype
 */
function getExtension(mimetype?: string, mediaType?: MediaType): string {
    if (mimetype) {
        // Map common mimetypes to proper extensions
        const mimeToExt: Record<string, string> = {
            // Microsoft Office
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx',
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
            'application/vnd.openxmlformats-officedocument.presentationml.presentation': '.pptx',
            'application/vnd.ms-excel': '.xls',
            'application/msword': '.doc',
            'application/vnd.ms-powerpoint': '.ppt',
            
            // Images
            'image/jpeg': '.jpg',
            'image/png': '.png',
            'image/gif': '.gif',
            'image/webp': '.webp',
            'image/svg+xml': '.svg',
            
            // Videos
            'video/mp4': '.mp4',
            'video/mpeg': '.mpeg',
            'video/quicktime': '.mov',
            'video/x-msvideo': '.avi',
            'video/webm': '.webm',
            
            // Audio
            'audio/mpeg': '.mp3',
            'audio/wav': '.wav',
            'audio/ogg': '.ogg',
            'audio/opus': '.opus',
            'audio/aac': '.aac',
            
            // Documents
            'application/pdf': '.pdf',
            'text/plain': '.txt',
            'text/csv': '.csv',
            'application/zip': '.zip',
            'application/x-rar-compressed': '.rar',
            'application/x-7z-compressed': '.7z',
            'application/json': '.json',
            'application/xml': '.xml',
            'text/html': '.html'
        }
        
        // Check if we have a known mapping
        if (mimeToExt[mimetype]) {
            return mimeToExt[mimetype]
        }
        
        // For unknown mimetypes, try to extract a reasonable extension
        const parts = mimetype.split('/')
        if (parts.length === 2) {
            const subtype = parts[1].split(';')[0]
            // Avoid long extensions from complex mimetypes
            if (subtype && subtype.length <= 10 && !subtype.includes('.')) {
                return `.${subtype}`
            }
        }
    }
    
    // Fallback extensions based on media type
    switch (mediaType) {
        case 'image': return '.jpg'
        case 'video': return '.mp4'
        case 'audio': return '.mp3'
        case 'document': return '.pdf'
        case 'sticker': return '.webp'
        default: return '.bin'
    }
}

/**
 * Generate unique filename for media
 */
function generateFileName(
    messageId: string,
    mediaType: MediaType,
    extension: string,
    originalFilename?: string
): string {
    const timestamp = Date.now()
    const sanitizedId = messageId.replace(/[^a-zA-Z0-9]/g, '_')
    
    if (originalFilename && mediaType === 'document') {
        // Keep original filename for documents but ensure proper extension
        let nameWithoutExt = originalFilename
        
        // Remove any existing extension from the original filename
        const lastDotIndex = originalFilename.lastIndexOf('.')
        if (lastDotIndex > 0) {
            const existingExt = originalFilename.substring(lastDotIndex)
            // Only remove if it looks like a valid extension (not too long)
            if (existingExt.length <= 10) {
                nameWithoutExt = originalFilename.substring(0, lastDotIndex)
            }
        }
        
        // Sanitize the filename to avoid path traversal issues
        nameWithoutExt = nameWithoutExt.replace(/[^a-zA-Z0-9_-]/g, '_')
        
        return `${timestamp}_${sanitizedId}_${nameWithoutExt}${extension}`
    }
    
    return `${timestamp}_${sanitizedId}_${mediaType}${extension}`
}

/**
 * Ensure directory exists
 */
async function ensureDir(dirPath: string): Promise<void> {
    try {
        await fs.access(dirPath)
    } catch {
        await fs.mkdir(dirPath, { recursive: true })
    }
}

/**
 * Download media with retry logic
 */
async function downloadWithRetry(
    mediaMessage: proto.Message.IImageMessage | proto.Message.IVideoMessage | proto.Message.IAudioMessage | proto.Message.IDocumentMessage | proto.Message.IStickerMessage,
    mediaType: MediaType,
    outputPath: string,
    config: MediaConfig,
    logger?: Logger
): Promise<void> {
    const maxRetries = config.maxRetries || 3
    const retryDelay = config.retryDelay || 1000
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            const stream = await downloadContentFromMessage(mediaMessage, mediaType)
            const writeStream = createWriteStream(outputPath)
            
            // Set timeout if configured
            if (config.downloadTimeout) {
                const timeout = setTimeout(() => {
                    writeStream.destroy(new Error('Download timeout'))
                }, config.downloadTimeout)
                
                writeStream.on('finish', () => clearTimeout(timeout))
                writeStream.on('error', () => clearTimeout(timeout))
            }
            
            await pipeline(stream, writeStream)
            return
        } catch (error) {
            if (attempt === maxRetries) {
                throw error
            }
            
            logger?.warn({
                error: error instanceof Error ? error.message : 'Unknown error',
                attempt,
                maxRetries,
                mediaType
            }, 'Media download failed, retrying...')
            
            await new Promise(resolve => setTimeout(resolve, retryDelay * attempt))
        }
    }
}

/**
 * Get media SHA256 hash from message
 */
function getMediaHash(mediaInfo: MediaInfo): string | undefined {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const message = mediaInfo.message as any
    return message.fileSha256 ? Buffer.from(message.fileSha256).toString('base64') : undefined
}

/**
 * Main function to download and save media from a WhatsApp message
 */
export async function downloadMedia(
    message: proto.IWebMessageInfo,
    instanceId: string,
    config: MediaConfig,
    logger?: Logger,
    checkExisting?: (hash: string) => Promise<string | null>
): Promise<MediaDownloadResult> {
    try {
        // Check if media download is enabled
        if (!config.enabled) {
            return { success: false, error: 'Media download disabled' }
        }
        
        // Skip group messages if configured
        if (config.skipGroupMessages && message.key.remoteJid?.includes('@g.us')) {
            return { success: false, error: 'Group message skipped' }
        }
        
        // Extract media info
        const mediaInfo = extractMediaInfo(message)
        if (!mediaInfo) {
            return { success: false, error: 'No media found in message' }
        }
        
        // Check if we already have this media file by hash
        const mediaHash = getMediaHash(mediaInfo)
        if (mediaHash && checkExisting) {
            const existingPath = await checkExisting(mediaHash)
            if (existingPath) {
                logger?.info({
                    messageId: message.key.id,
                    hash: mediaHash,
                    existingPath
                }, 'Media already exists, reusing file')
                
                // Return the existing path without downloading
                return {
                    success: true,
                    localPath: existingPath,
                    mediaType: mediaInfo.type,
                    fileName: existingPath.split('/').pop(),
                    fileSize: 0, // We don't need to check size for existing files
                    reused: true
                }
            }
        }
        
        // Check allowed types
        if (config.allowedTypes && config.allowedTypes.length > 0) {
            if (!config.allowedTypes.includes(mediaInfo.type)) {
                return { success: false, error: `Media type ${mediaInfo.type} not allowed` }
            }
        }
        
        // Check file size if available
        if (config.maxSizeInMB && mediaInfo.message.fileLength) {
            const fileSizeMB = Number(mediaInfo.message.fileLength) / (1024 * 1024)
            if (fileSizeMB > config.maxSizeInMB) {
                return { 
                    success: false, 
                    error: `File size ${fileSizeMB.toFixed(2)}MB exceeds limit of ${config.maxSizeInMB}MB` 
                }
            }
        }
        
        // Prepare directory structure
        const instanceDir = join(config.baseDir, instanceId)
        const typeDir = join(instanceDir, mediaInfo.type)
        await ensureDir(typeDir)
        
        // Generate filename
        const extension = getExtension(mediaInfo.mimetype, mediaInfo.type)
        const fileName = generateFileName(
            message.key.id || 'unknown',
            mediaInfo.type,
            extension,
            mediaInfo.filename
        )
        const filePath = join(typeDir, fileName)
        
        // Download media
        await downloadWithRetry(
            mediaInfo.message,
            mediaInfo.type,
            filePath,
            config,
            logger
        )
        
        // Get file stats
        const stats = await fs.stat(filePath)
        
        // Return relative path from base directory
        const relativePath = join(instanceId, mediaInfo.type, fileName)
        
        return {
            success: true,
            localPath: relativePath,
            mediaType: mediaInfo.type,
            fileName,
            fileSize: stats.size,
            mediaHash: mediaHash
        }
    } catch (error) {
        logger?.error({
            error: error instanceof Error ? error.message : 'Unknown error',
            messageId: message.key.id,
            instanceId
        }, 'Failed to download media')
        
        return {
            success: false,
            error: error instanceof Error ? error.message : 'Unknown error occurred'
        }
    }
}

/**
 * Download media from Official WhatsApp API
 */
async function downloadFromOfficialAPI(
    mediaId: string,
    accountData: OfficialAPIAccountData,
    filePath: string,
    logger?: Logger
): Promise<void> {
    try {
        const { access_token, phone_number_id } = JSON.parse(accountData.tmp)
        const isWabotPro = accountData.data === 'wabot_pro'
        
        // Step 1: Get media URL from WhatsApp API
        const apiUrl = isWabotPro
            ? `https://crm.wabot.pro/api/meta/v19.0/${mediaId}?phone_number_id=${phone_number_id}`
            : `https://graph.facebook.com/v23.0/${mediaId}?phone_number_id=${phone_number_id}`
        
        const firstResponse = await axios.get(
            apiUrl,
            {
                headers: {
                    'Authorization': `Bearer ${access_token}`
                },
                responseType: 'stream',
                timeout: 10000
            }
        )
        
        // Check if the response is JSON (contains URL) or direct media
        const contentType = firstResponse.headers['content-type'] || ''
        
        if (contentType.includes('application/json')) {
            // This is a JSON response with URL - need second request
            // Convert stream to JSON
            let data = ''
            for await (const chunk of firstResponse.data) {
                data += chunk.toString()
            }
            const jsonResponse = JSON.parse(data)
            
            if (!jsonResponse?.url) {
                throw new Error('No media URL returned from WhatsApp API')
            }
            
            // Step 2: Download the actual media file
            const mediaResponse = await axios.get(
                jsonResponse.url,
                {
                    headers: {
                        'Authorization': `Bearer ${access_token}`
                    },
                    responseType: 'stream',
                    timeout: 60000
                }
            )
            
            // Write to file
            const writeStream = createWriteStream(filePath)
            await pipeline(mediaResponse.data, writeStream)
        } else {
            // This is the direct media response (wabot_pro proxy behavior)
            // Write directly to file
            const writeStream = createWriteStream(filePath)
            await pipeline(firstResponse.data, writeStream)
        }
        
        logger?.info({
            mediaId,
            filePath,
            contentType
        }, 'Successfully downloaded Official API media')
        
    } catch (error) {
        logger?.error({
            error: error instanceof Error ? error.message : 'Unknown error',
            mediaId
        }, 'Failed to download Official API media')
        throw error
    }
}

/**
 * Download and save media from an Official WhatsApp API message
 */
export async function downloadOfficialAPIMedia(
    message: proto.IWebMessageInfo,
    instanceId: string,
    config: MediaConfig,
    logger?: Logger,
    checkExisting?: (hash: string) => Promise<string | null>
): Promise<MediaDownloadResult> {
    try {
        // Check if media download is enabled
        if (!config.enabled) {
            return { success: false, error: 'Media download disabled' }
        }
        
        // Check if Official API is configured
        if (!config.officialAPI?.getAccountData) {
            return { success: false, error: 'Official API not configured' }
        }
        
        // Skip group messages if configured
        if (config.skipGroupMessages && message.key.remoteJid?.includes('@g.us')) {
            return { success: false, error: 'Group message skipped' }
        }
        
        // Extract media info
        const mediaInfo = extractMediaInfo(message)
        if (!mediaInfo) {
            return { success: false, error: 'No media found in message' }
        }
        
        // Get the media ID from the message
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const mediaMessage = mediaInfo.message as any
        const mediaId = mediaMessage.id
        
        if (!mediaId) {
            return { success: false, error: 'No media ID found in Official API message' }
        }
        
        // Check if we already have this media file by ID (using ID as hash for Official API)
        if (checkExisting) {
            const existingPath = await checkExisting(mediaId)
            if (existingPath) {
                logger?.info({
                    messageId: message.key.id,
                    mediaId,
                    existingPath
                }, 'Official API media already exists, reusing file')
                
                return {
                    success: true,
                    localPath: existingPath,
                    mediaType: mediaInfo.type,
                    fileName: existingPath.split('/').pop(),
                    fileSize: 0,
                    mediaHash: mediaId,
                    reused: true
                }
            }
        }
        
        // Get account data for this instance
        const accountData = await config.officialAPI.getAccountData(instanceId)
        if (!accountData || accountData.loginType !== 1 || accountData.status !== 1) {
            return { success: false, error: 'Invalid account for Official API' }
        }
        
        // Check allowed types
        if (config.allowedTypes && config.allowedTypes.length > 0) {
            if (!config.allowedTypes.includes(mediaInfo.type)) {
                return { success: false, error: `Media type ${mediaInfo.type} not allowed` }
            }
        }
        
        // Prepare directory structure
        const instanceDir = join(config.baseDir, instanceId)
        const typeDir = join(instanceDir, mediaInfo.type)
        await ensureDir(typeDir)
        
        // Generate filename
        const timestamp = Date.now()
        const extension = getExtension(mediaInfo.mimetype || 'application/octet-stream', mediaInfo.type)
        const fileName = mediaInfo.filename || `${timestamp}.${extension}`
        const filePath = join(typeDir, fileName)
        
        // Download the media with retry logic
        const maxRetries = config.maxRetries || 3
        const retryDelay = config.retryDelay || 1000
        
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                await downloadFromOfficialAPI(mediaId, accountData, filePath, logger)
                break
            } catch (error) {
                if (attempt === maxRetries) {
                    throw error
                }
                
                logger?.warn({
                    error: error instanceof Error ? error.message : 'Unknown error',
                    attempt,
                    maxRetries,
                    mediaId
                }, 'Official API media download failed, retrying...')
                
                await new Promise(resolve => setTimeout(resolve, retryDelay * attempt))
            }
        }
        
        // Get file stats
        const stats = await fs.stat(filePath)
        
        // Return relative path from base directory
        const relativePath = join(instanceId, mediaInfo.type, fileName)
        
        return {
            success: true,
            localPath: relativePath,
            mediaType: mediaInfo.type,
            fileName,
            fileSize: stats.size,
            mediaHash: mediaId
        }
    } catch (error) {
        logger?.error({
            error: error instanceof Error ? error.message : 'Unknown error',
            messageId: message.key.id,
            instanceId
        }, 'Failed to download Official API media')
        
        return {
            success: false,
            error: error instanceof Error ? error.message : 'Unknown error occurred'
        }
    }
}

/**
 * Clean up old media files
 */
export async function cleanupOldMedia(
    instanceId: string,
    config: MediaConfig,
    daysToKeep: number = 30,
    logger?: Logger
): Promise<{ deleted: number; errors: number }> {
    const result = { deleted: 0, errors: 0 }
    
    try {
        const instanceDir = join(config.baseDir, instanceId)
        const cutoffTime = Date.now() - (daysToKeep * 24 * 60 * 60 * 1000)
        
        // Process each media type directory
        const mediaTypes: MediaType[] = ['image', 'video', 'audio', 'document', 'sticker']
        
        for (const mediaType of mediaTypes) {
            const typeDir = join(instanceDir, mediaType)
            
            try {
                const files = await fs.readdir(typeDir)
                
                for (const file of files) {
                    const filePath = join(typeDir, file)
                    
                    try {
                        const stats = await fs.stat(filePath)
                        
                        if (stats.mtimeMs < cutoffTime) {
                            await fs.unlink(filePath)
                            result.deleted++
                        }
                    } catch (err) {
                        logger?.warn({
                            error: err instanceof Error ? err.message : 'Unknown error',
                            file: filePath
                        }, 'Failed to process media file')
                        result.errors++
                    }
                }
            } catch (err) {
                // Directory might not exist
                if ((err as NodeJS.ErrnoException).code !== 'ENOENT') {
                    logger?.warn({
                        error: err instanceof Error ? err.message : 'Unknown error',
                        directory: typeDir
                    }, 'Failed to read media directory')
                }
            }
        }
        
        logger?.info({
            instanceId,
            deleted: result.deleted,
            errors: result.errors,
            daysToKeep
        }, 'Media cleanup completed')
        
    } catch (error) {
        logger?.error({
            error: error instanceof Error ? error.message : 'Unknown error',
            instanceId
        }, 'Failed to cleanup media')
    }
    
    return result
}

/**
 * Get media statistics for an instance
 */
export async function getMediaStats(
    instanceId: string,
    config: MediaConfig,
    logger?: Logger
): Promise<{
    totalFiles: number
    totalSize: number
    byType: Record<MediaType, { count: number; size: number }>
}> {
    const stats = {
        totalFiles: 0,
        totalSize: 0,
        byType: {} as Record<MediaType, { count: number; size: number }>
    }
    
    try {
        const instanceDir = join(config.baseDir, instanceId)
        const mediaTypes: MediaType[] = ['image', 'video', 'audio', 'document', 'sticker']
        
        for (const mediaType of mediaTypes) {
            stats.byType[mediaType] = { count: 0, size: 0 }
            
            const typeDir = join(instanceDir, mediaType)
            
            try {
                const files = await fs.readdir(typeDir)
                
                for (const file of files) {
                    const filePath = join(typeDir, file)
                    
                    try {
                        const fileStat = await fs.stat(filePath)
                        
                        if (fileStat.isFile()) {
                            stats.totalFiles++
                            stats.totalSize += fileStat.size
                            stats.byType[mediaType].count++
                            stats.byType[mediaType].size += fileStat.size
                        }
                    } catch (err) {
                        logger?.warn({
                            error: err instanceof Error ? err.message : 'Unknown error',
                            file: filePath
                        }, 'Failed to stat media file')
                    }
                }
            } catch (err) {
                // Directory might not exist, which is fine
                if ((err as NodeJS.ErrnoException).code !== 'ENOENT') {
                    logger?.warn({
                        error: err instanceof Error ? err.message : 'Unknown error',
                        directory: typeDir
                    }, 'Failed to read media directory')
                }
            }
        }
    } catch (error) {
        logger?.error({
            error: error instanceof Error ? error.message : 'Unknown error',
            instanceId
        }, 'Failed to get media stats')
    }
    
    return stats
}