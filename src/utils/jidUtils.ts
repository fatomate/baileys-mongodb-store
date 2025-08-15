/**
 * JID Utility Functions for WhatsApp
 * Handles normalization and comparison of WhatsApp JIDs with various formats
 */

/**
 * Normalize a JID by removing the :XX suffix for comparison
 * Examples:
 * - 114194640801953:73@lid -> 114194640801953@lid
 * - 60196953307:73@s.whatsapp.net -> 60196953307@s.whatsapp.net
 * - 114194640801953@lid -> 114194640801953@lid (unchanged)
 */
export const normalizeJidForComparison = (jid: string | undefined | null): string => {
    if (!jid) return ''
    // Remove :XX suffix before @ symbol
    return jid.replace(/:[0-9]+@/, '@')
}

/**
 * Extract the base JID without any suffixes
 * Examples:
 * - 114194640801953:73@lid -> 114194640801953
 * - 60196953307@s.whatsapp.net -> 60196953307
 */
export const extractBaseJid = (jid: string | undefined | null): string => {
    if (!jid) return ''
    // Extract everything before @ or : symbols
    const match = jid.match(/^([0-9]+)/)
    return match ? match[1] : ''
}

/**
 * Get the JID domain/suffix
 * Examples:
 * - 114194640801953:73@lid -> lid
 * - 60196953307@s.whatsapp.net -> s.whatsapp.net
 */
export const getJidDomain = (jid: string | undefined | null): string => {
    if (!jid) return ''
    const parts = jid.split('@')
    return parts.length > 1 ? parts[1] : ''
}

/**
 * Check if two JIDs are equivalent (ignoring :XX suffixes)
 * Examples:
 * - areJidsEquivalent('114194640801953:73@lid', '114194640801953@lid') -> true
 * - areJidsEquivalent('60196953307:73@s.whatsapp.net', '60196953307@s.whatsapp.net') -> true
 */
export const areJidsEquivalent = (jid1: string | undefined | null, jid2: string | undefined | null): boolean => {
    if (!jid1 || !jid2) return false
    return normalizeJidForComparison(jid1) === normalizeJidForComparison(jid2)
}

/**
 * Check if a JID is in LID format (with or without suffix)
 */
export const isLidFormat = (jid: string | undefined | null): boolean => {
    if (!jid) return false
    // Check if it ends with @lid (after removing any :XX suffix)
    const normalized = normalizeJidForComparison(jid)
    return normalized.endsWith('@lid')
}

/**
 * Check if a JID is a phone number format (with or without suffix)
 */
export const isPhoneNumberFormat = (jid: string | undefined | null): boolean => {
    if (!jid) return false
    const normalized = normalizeJidForComparison(jid)
    return normalized.endsWith('@s.whatsapp.net') || normalized.endsWith('@c.us')
}

/**
 * Check if two JIDs form a LID-Phone pair
 * One should be @lid format and the other should be phone format
 */
export const isLidAndPhonePair = (jid1: string | undefined | null, jid2: string | undefined | null): boolean => {
    if (!jid1 || !jid2) return false
    
    const isJid1Lid = isLidFormat(jid1)
    const isJid1Phone = isPhoneNumberFormat(jid1)
    const isJid2Lid = isLidFormat(jid2)
    const isJid2Phone = isPhoneNumberFormat(jid2)
    
    // One must be LID and the other must be phone
    return (isJid1Lid && isJid2Phone) || (isJid1Phone && isJid2Lid)
}

/**
 * Extract LID and phone number from a potential pair
 * Returns null if not a valid pair
 */
export const extractLidPhonePair = (jid1: string, jid2: string): { lid: string; phoneNumber: string } | null => {
    if (!isLidAndPhonePair(jid1, jid2)) return null
    
    const norm1 = normalizeJidForComparison(jid1)
    const norm2 = normalizeJidForComparison(jid2)
    
    if (isLidFormat(norm1)) {
        return { lid: norm1, phoneNumber: norm2 }
    } else {
        return { lid: norm2, phoneNumber: norm1 }
    }
}

/**
 * Normalize a JID for storage (removes suffixes and ensures consistency)
 */
export const normalizeJidForStorage = (jid: string | undefined | null): string => {
    if (!jid) return ''
    return normalizeJidForComparison(jid)
}

/**
 * Check if two JIDs might be related (same base number)
 * Useful for detecting potential LID-phone relationships
 */
export const mightBeRelatedJids = (jid1: string | undefined | null, jid2: string | undefined | null): boolean => {
    if (!jid1 || !jid2) return false
    
    // If they're already equivalent, they're definitely related
    if (areJidsEquivalent(jid1, jid2)) return true
    
    // Check if they have the same base number
    const base1 = extractBaseJid(jid1)
    const base2 = extractBaseJid(jid2)
    
    // If bases are the same and one is LID and other is phone, they might be related
    if (base1 === base2 && base1 !== '') {
        return isLidAndPhonePair(jid1, jid2)
    }
    
    return false
}

/**
 * Create all possible JID variations for searching
 * Useful for finding messages that might be stored with different formats
 */
export const getJidVariations = (jid: string): string[] => {
    const variations = new Set<string>()
    
    // Add original
    variations.add(jid)
    
    // Add normalized version
    const normalized = normalizeJidForComparison(jid)
    variations.add(normalized)
    
    // If it has a suffix, try without it
    if (jid.includes(':') && jid.includes('@')) {
        variations.add(normalized)
    }
    
    // If it's a phone number, try common variations
    const base = extractBaseJid(jid)
    if (base) {
        if (isPhoneNumberFormat(jid)) {
            variations.add(`${base}@s.whatsapp.net`)
            variations.add(`${base}@c.us`)
        } else if (isLidFormat(jid)) {
            variations.add(`${base}@lid`)
        }
    }
    
    return Array.from(variations)
}

/**
 * Format a JID for display (consistent format)
 */
export const formatJidForDisplay = (jid: string | undefined | null): string => {
    if (!jid) return 'Unknown'
    return normalizeJidForComparison(jid)
}

/**
 * Check if a JID is a group format
 */
export const isGroupJid = (jid: string | undefined | null): boolean => {
    if (!jid) return false
    const normalized = normalizeJidForComparison(jid)
    return normalized.endsWith('@g.us')
}

/**
 * Check if a JID is a broadcast format
 */
export const isBroadcastJid = (jid: string | undefined | null): boolean => {
    if (!jid) return false
    const normalized = normalizeJidForComparison(jid)
    return normalized.endsWith('@broadcast') || normalized === 'status@broadcast'
}

/**
 * Get a cache key for a JID (normalized for consistency)
 */
export const getJidCacheKey = (jid: string): string => {
    return normalizeJidForComparison(jid).replace(/[^a-zA-Z0-9]/g, '_')
}