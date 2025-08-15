import { createHash } from 'crypto'
import { normalizeJidForComparison } from './jidUtils'

/**
 * Security utilities for input validation and sanitization
 */

// JID validation regex patterns for WhatsApp
const WHATSAPP_JID_REGEX = /^[0-9]+(:[0-9]+)?@s\.whatsapp\.net$/ // Individual users (with optional :XX suffix)
const GROUP_JID_REGEX = /^[0-9]+(-[0-9]+)?@g\.us$/ // Groups (with optional hyphen segment)
const BROADCAST_JID_REGEX = /^[0-9]+@broadcast$/ // Broadcast lists
const LID_JID_REGEX = /^[0-9]+(:[0-9]+)?@lid$/ // Linked devices (with optional :XX suffix)
const CONTACT_JID_REGEX = /^[0-9]+@c\.us$/ // Contacts
const STATUS_JID = 'status@broadcast' // Status updates

// Message ID validation
// WhatsApp message IDs can be:
// - Classic format: 16-32 uppercase alphanumeric (e.g., "3EB0ABC123DEF456")
// - Numeric format: Any digit strings (e.g., "1679765488", "58384821")
// - Numeric with hyphen: Digits with optional hyphen (e.g., "1688785978376286-1")
// - Official API format: Base64-like strings with = and + (e.g., "wamid.HBgL...AA==")
// - Mixed format: Alphanumeric with special chars (various lengths)
const MESSAGE_ID_REGEX = /^([A-Z0-9]{16,32}|[0-9]+(-[0-9]+)?|[A-Za-z0-9+/=._-]{3,})$/

// Custom error classes for security
export class ValidationError extends Error {
    constructor(message: string) {
        super(message)
        this.name = 'ValidationError'
    }
}

export class AuthorizationError extends Error {
    constructor(message: string) {
        super(message)
        this.name = 'AuthorizationError'
    }
}

export class SecurityError extends Error {
    constructor(message: string) {
        super(message)
        this.name = 'SecurityError'
    }
}

/**
 * Validates a WhatsApp JID
 * @param jid - The JID to validate
 * @returns true if valid, false otherwise
 */
export const isValidJID = (jid: string): boolean => {
    if (!jid || typeof jid !== 'string') return false
    
    // Check for special JIDs
    if (jid === STATUS_JID) return true
    
    // Check all WhatsApp JID patterns
    return WHATSAPP_JID_REGEX.test(jid) || 
           GROUP_JID_REGEX.test(jid) || 
           BROADCAST_JID_REGEX.test(jid) ||
           LID_JID_REGEX.test(jid) ||
           CONTACT_JID_REGEX.test(jid)
}

/**
 * Validates and sanitizes a JID
 * @param jid - The JID to validate
 * @throws ValidationError if invalid
 * @returns The validated JID
 */
export const validateJID = (jid: string): string => {
    const trimmedJid = jid?.trim()
    if (!isValidJID(trimmedJid)) {
        throw new ValidationError(`Invalid JID format: ${trimmedJid?.substring(0, 20)}...`)
    }
    return trimmedJid
}

/**
 * Validates a message ID
 * @param id - The message ID to validate
 * @returns true if valid, false otherwise
 */
export const isValidMessageId = (id: string): boolean => {
    if (!id || typeof id !== 'string') return false
    const trimmedId = id.trim()
    // Very lenient validation - just check for basic safety
    return trimmedId.length >= 3 && !trimmedId.includes('$') && !trimmedId.includes('{') && !trimmedId.includes('}')
}

/**
 * Validates and sanitizes a message ID
 * @param id - The message ID to validate
 * @param isOfficialAPI - Whether this is an Official API message
 * @throws ValidationError if invalid
 * @returns The validated message ID
 */
export const validateMessageId = (id: string, isOfficialAPI: boolean = false): string => {
    // Ensure id is a string and trim it first
    const trimmedId = id?.toString().trim()
    
    // Basic validation - must have some content
    if (!trimmedId || trimmedId.length === 0) {
        throw new ValidationError('Message ID cannot be empty')
    }
    
    // For Official API messages, we're more lenient as they use different ID formats
    if (isOfficialAPI && trimmedId.length >= 3) {
        return trimmedId // Accept any Official API ID with reasonable length
    }
    
    // For regular messages, be very lenient
    // Accept any ID that's at least 3 characters and doesn't contain obvious injection attempts
    if (trimmedId.length >= 3 && !trimmedId.includes('$') && !trimmedId.includes('{') && !trimmedId.includes('}')) {
        return trimmedId
    }
    
    // If it still doesn't pass our lenient check, log and reject
    console.error('Message ID validation failed:', {
        original: id,
        trimmed: trimmedId,
        type: typeof id,
        length: trimmedId?.length,
        regex: MESSAGE_ID_REGEX.source,
        isOfficialAPI
    })
    throw new ValidationError(`Invalid message ID format: ${trimmedId?.substring(0, 50)}`)
}

/**
 * Validates an instance ID
 * @param instanceId - The instance ID to validate
 * @returns true if valid, false otherwise
 */
export const isValidInstanceId = (instanceId: string): boolean => {
    if (!instanceId || typeof instanceId !== 'string') return false
    // Instance ID should be alphanumeric with hyphens/underscores, 3-50 chars
    return /^[a-zA-Z0-9_-]{3,50}$/.test(instanceId)
}

/**
 * Validates and sanitizes an instance ID
 * @param instanceId - The instance ID to validate
 * @throws ValidationError if invalid
 * @returns The validated instance ID
 */
export const validateInstanceId = (instanceId: string): string => {
    if (!isValidInstanceId(instanceId)) {
        throw new ValidationError('Invalid instance ID format')
    }
    return instanceId.trim()
}

/**
 * Validates a label ID
 * @param labelId - The label ID to validate
 * @returns true if valid, false otherwise
 */
export const isValidLabelId = (labelId: string): boolean => {
    if (!labelId || typeof labelId !== 'string') return false
    // Label IDs are typically numeric strings
    return /^[0-9]{1,20}$/.test(labelId)
}

/**
 * Validates and sanitizes a label ID
 * @param labelId - The label ID to validate
 * @throws ValidationError if invalid
 * @returns The validated label ID
 */
export const validateLabelId = (labelId: string): string => {
    if (!isValidLabelId(labelId)) {
        throw new ValidationError('Invalid label ID format')
    }
    return labelId.trim()
}

/**
 * Sanitizes a string for safe logging (removes sensitive data)
 * @param str - The string to sanitize
 * @param maxLength - Maximum length to show
 * @returns Sanitized string
 */
export const sanitizeForLogging = (str: string, maxLength: number = 50): string => {
    if (str === null || str === undefined) return '[empty]'
    if (typeof str !== 'string') return '[non-string]'
    if (str === '') return '[empty]'
    
    const sanitized = str.substring(0, maxLength)
    return str.length > maxLength ? `${sanitized}...` : sanitized
}

/**
 * Creates a hash of sensitive data for logging purposes
 * @param data - The data to hash
 * @returns A short hash suitable for correlation
 */
export const hashForLogging = (data: string): string => {
    return createHash('sha256').update(data).digest('hex').substring(0, 8)
}

/**
 * Access control check for instance-based data access
 * @param requestInstanceId - The instance making the request
 * @param dataInstanceId - The instance ID of the data
 * @throws AuthorizationError if access denied
 */
export const checkInstanceAccess = (requestInstanceId: string, dataInstanceId: string): void => {
    if (requestInstanceId !== dataInstanceId) {
        throw new AuthorizationError('Access denied: instance mismatch')
    }
}

/**
 * Validates pagination parameters
 * @param limit - The limit parameter
 * @param offset - The offset parameter
 * @returns Validated parameters
 */
export const validatePagination = (limit?: number, offset?: number): { limit: number, offset: number } => {
    const validatedLimit = Math.min(Math.max(1, limit || 100), 1000) // Max 1000 items
    const validatedOffset = Math.max(0, offset || 0)
    
    return {
        limit: validatedLimit,
        offset: validatedOffset
    }
}

/**
 * Validates a collection name to prevent injection
 * @param name - The collection name
 * @returns true if valid, false otherwise
 */
export const isValidCollectionName = (name: string): boolean => {
    if (!name || typeof name !== 'string') return false
    // Only allow specific collection names
    const allowedNames = [
        'chats', 'contacts', 'messages', 'groupMetadata', 
        'state', 'presences', 'labels', 'labelAssociations'
    ]
    return allowedNames.includes(name)
}

/**
 * Rate limiting key generator
 * @param instanceId - The instance ID
 * @param operation - The operation being performed
 * @returns A rate limit key
 */
export const getRateLimitKey = (instanceId: string, operation: string): string => {
    return `ratelimit:${instanceId}:${operation}`
}

/**
 * Validates MongoDB query operators to prevent injection
 * @param query - The query object to validate
 * @throws SecurityError if dangerous operators found
 */
export const validateMongoQuery = (query: Record<string, unknown>): void => {
    const dangerousOperators = ['$where', '$expr', '$function', '$accumulator', '$regex']
    
    const checkObject = (obj: Record<string, unknown>): void => {
        if (!obj || typeof obj !== 'object') return
        
        for (const key in obj) {
            if (dangerousOperators.includes(key)) {
                throw new SecurityError(`Dangerous operator not allowed: ${key}`)
            }
            
            if (typeof obj[key] === 'object' && obj[key] !== null) {
                checkObject(obj[key] as Record<string, unknown>)
            }
        }
    }
    
    checkObject(query)
}

/**
 * Safe validation wrapper for JIDs that logs warnings instead of throwing
 * @param jid - The JID to validate
 * @returns The JID (validated or not)
 */
export const safeValidateJID = (jid: string): string => {
    const trimmedJid = jid?.trim()
    if (!isValidJID(trimmedJid)) {
        // Try with normalized JID (removing :XX suffixes)
        const normalized = normalizeJidForComparison(trimmedJid)
        if (isValidJID(normalized)) {
            return normalized
        }
        console.warn(`Invalid JID format (processing anyway): ${trimmedJid?.substring(0, 30)}`)
    }
    return trimmedJid || ''
}

/**
 * Normalize a JID for safe comparison
 * Removes :XX suffixes while maintaining safety
 */
export const safeNormalizeJid = (jid: string): string => {
    return normalizeJidForComparison(jid)
}

/**
 * Safe validation wrapper for message IDs that logs warnings instead of throwing
 * @param id - The message ID to validate
 * @param isOfficialAPI - Whether this is an Official API message
 * @returns The message ID (validated or not)
 */
export const safeValidateMessageId = (id: string, _isOfficialAPI: boolean = false): string => {
    // Handle undefined/null/empty IDs gracefully
    if (!id || id === 'undefined' || id === 'null') {
        console.warn('Empty or invalid message ID encountered (processing with placeholder)')
        return 'PLACEHOLDER_' + Date.now()
    }
    
    const trimmedId = id.toString().trim()
    
    // Basic validation - just warn if suspicious
    if (trimmedId.length < 3 || trimmedId.includes('$') || trimmedId.includes('{') || trimmedId.includes('}')) {
        console.warn(`Suspicious message ID format (processing anyway): ${trimmedId?.substring(0, 30)}`)
    }
    
    return trimmedId
}

/**
 * Creates a safe error message for external consumption
 * @param error - The original error
 * @param operation - The operation that failed
 * @returns A safe error message
 */
export const createSafeErrorMessage = (error: Error, operation: string): string => {
    // Map specific error types to safe messages
    if (error instanceof ValidationError) {
        return `Validation failed for ${operation}`
    }
    
    if (error instanceof AuthorizationError) {
        return 'Access denied'
    }
    
    if (error.message?.includes('E11000')) {
        return 'Duplicate entry detected'
    }
    
    if (error.message?.includes('connection') || error.message?.includes('ECONNREFUSED')) {
        return 'Service temporarily unavailable'
    }
    
    // Generic error message
    return `Operation failed: ${operation}`
}

/**
 * Validates event data before storage
 * @param eventType - The type of event
 * @param data - The event data
 * @throws ValidationError if invalid
 */
export const validateEventData = (eventType: string, data: Record<string, unknown>): void => {
    const allowedEventTypes = [
        'messages.upsert', 'messages.update', 'messages.delete',
        'chats.upsert', 'chats.update', 'chats.delete',
        'contacts.upsert', 'contacts.update',
        'groups.upsert', 'groups.update',
        'labels.upsert', 'labels.update', 'labels.delete',
        'label-associations.upsert', 'label-associations.delete'
    ]
    
    if (!allowedEventTypes.includes(eventType)) {
        throw new ValidationError(`Invalid event type: ${eventType}`)
    }
    
    if (!data || typeof data !== 'object') {
        throw new ValidationError('Invalid event data')
    }
}