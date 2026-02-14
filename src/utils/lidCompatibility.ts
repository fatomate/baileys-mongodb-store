import type { LidHandler } from './lidHandler.js'

/**
 * Baileys v7 LID Mapping type
 */
export interface BaileysLIDMapping {
    pn: string  // Phone Number JID (@s.whatsapp.net)
    lid: string // LID JID (@lid)
}

/**
 * Interface for Baileys v7 LIDMappingStore
 * Available via sock.signalRepository.lidMapping
 */
export interface BaileysLIDMappingStore {
    getLIDForPN(pn: string): Promise<string | null>
    getLIDsForPNs(pns: string[]): Promise<BaileysLIDMapping[] | null>
    getPNForLID(lid: string): Promise<string | null>
    storeLIDPNMapping?(lid: string, pn: string): Promise<void>
    storeLIDPNMappings?(pairs: BaileysLIDMapping[]): Promise<void>
}

/**
 * Unified LID resolver interface that works with both Baileys v6 and v7
 */
export interface LIDResolver {
    /**
     * Get LID for a phone number JID
     */
    getLIDForPN(pn: string): Promise<string | null>
    
    /**
     * Get LIDs for multiple phone number JIDs
     */
    getLIDsForPNs(pns: string[]): Promise<Map<string, string>>
    
    /**
     * Get phone number JID for a LID
     */
    getPNForLID(lid: string): Promise<string | null>
    
    /**
     * Check if using Baileys v7 native API
     */
    isUsingNativeStore(): boolean
    
    /**
     * Get the underlying Baileys LID mapping store (v7 only)
     */
    getNativeStore(): BaileysLIDMappingStore | null
}

/**
 * Check if a socket has the Baileys v7 LID mapping API
 */
export function hasBaileysV7LidMapping(sock: any): boolean {
    return !!(sock?.signalRepository?.lidMapping?.getLIDForPN)
}

/**
 * Check if a socket has the legacy onWhatsApp API (v6.x and earlier)
 */
export function hasLegacyOnWhatsApp(sock: any): boolean {
    return typeof sock?.onWhatsApp === 'function'
}

/**
 * Get the Baileys v7 LID mapping store from a socket
 */
export function getBaileysLIDMappingStore(sock: any): BaileysLIDMappingStore | null {
    if (hasBaileysV7LidMapping(sock)) {
        return sock.signalRepository.lidMapping as BaileysLIDMappingStore
    }
    return null
}

/**
 * Create a unified LID resolver that works with both Baileys v6 and v7
 * 
 * Resolution priority:
 * 1. Baileys v7 native LIDMappingStore (sock.signalRepository.lidMapping)
 * 2. Baileys v6 onWhatsApp API
 * 3. MongoDB fallback via LidHandler
 * 
 * @param sock - Baileys socket instance (optional)
 * @param fallbackHandler - LidHandler for MongoDB fallback (optional)
 * @returns LIDResolver instance
 */
export function createLIDResolver(sock: any, fallbackHandler?: LidHandler | null): LIDResolver {
    const v7Store = getBaileysLIDMappingStore(sock)
    
    // Baileys v7 native API
    if (v7Store) {
        return {
            async getLIDForPN(pn: string): Promise<string | null> {
                try {
                    const lid = await v7Store.getLIDForPN(pn)
                    
                    // Also store in MongoDB for persistence
                    if (lid && fallbackHandler) {
                        try {
                            await fallbackHandler.storeLidMapping(lid, pn)
                        } catch (err) {
                            console.debug('[LIDResolver] Failed to persist mapping to MongoDB:', (err as Error).message)
                        }
                    }
                    
                    return lid
                } catch (err) {
                    console.debug('[LIDResolver] v7 getLIDForPN failed:', (err as Error).message)
                    // Fallback to MongoDB
                    return fallbackHandler?.getLidFromPhoneNumber(pn) || null
                }
            },
            
            async getLIDsForPNs(pns: string[]): Promise<Map<string, string>> {
                const map = new Map<string, string>()
                
                try {
                    const result = await v7Store.getLIDsForPNs(pns)
                    if (result) {
                        for (const { pn, lid } of result) {
                            map.set(pn, lid)
                            
                            // Persist to MongoDB
                            if (fallbackHandler) {
                                try {
                                    await fallbackHandler.storeLidMapping(lid, pn)
                                } catch (err) {
                                    console.debug('[LIDResolver] Failed to persist mapping to MongoDB:', (err as Error).message)
                                }
                            }
                        }
                    }
                } catch (err) {
                    console.debug('[LIDResolver] v7 getLIDsForPNs failed:', (err as Error).message)
                    
                    // Fallback to MongoDB for missing PNs
                    if (fallbackHandler) {
                        for (const pn of pns) {
                            if (!map.has(pn)) {
                                try {
                                    const lid = await fallbackHandler.getLidFromPhoneNumber(pn)
                                    if (lid) map.set(pn, lid)
                                } catch {
                                    // continue
                                }
                            }
                        }
                    }
                }
                
                return map
            },
            
            async getPNForLID(lid: string): Promise<string | null> {
                try {
                    const pn = await v7Store.getPNForLID(lid)
                    
                    // Also store in MongoDB for persistence
                    if (pn && fallbackHandler) {
                        try {
                            await fallbackHandler.storeLidMapping(lid, pn)
                        } catch (err) {
                            console.debug('[LIDResolver] Failed to persist mapping to MongoDB:', (err as Error).message)
                        }
                    }
                    
                    return pn
                } catch (err) {
                    console.debug('[LIDResolver] v7 getPNForLID failed:', (err as Error).message)
                    // Fallback to MongoDB
                    return fallbackHandler?.getPhoneNumberFromLid(lid) || null
                }
            },
            
            isUsingNativeStore(): boolean {
                return true
            },
            
            getNativeStore(): BaileysLIDMappingStore | null {
                return v7Store
            }
        }
    }
    
    // Baileys v6 legacy onWhatsApp API
    if (hasLegacyOnWhatsApp(sock)) {
        return {
            async getLIDForPN(pn: string): Promise<string | null> {
                try {
                    const result = await sock.onWhatsApp(pn) as Array<{ jid: string; exists: boolean; lid?: string }> | undefined
                    const lid = result?.[0]?.lid || null
                    
                    // Persist to MongoDB
                    if (lid && fallbackHandler) {
                        try {
                            await fallbackHandler.storeLidMapping(lid, pn)
                        } catch (err) {
                            console.debug('[LIDResolver] Failed to persist mapping to MongoDB:', (err as Error).message)
                        }
                    }
                    
                    return lid
                } catch (err) {
                    console.debug('[LIDResolver] Legacy onWhatsApp failed:', (err as Error).message)
                    // Fallback to MongoDB
                    return fallbackHandler?.getLidFromPhoneNumber(pn) || null
                }
            },
            
            async getLIDsForPNs(pns: string[]): Promise<Map<string, string>> {
                const map = new Map<string, string>()
                
                for (const pn of pns) {
                    try {
                        const result = await sock.onWhatsApp(pn) as Array<{ jid: string; exists: boolean; lid?: string }> | undefined
                        if (result?.[0]?.lid) {
                            map.set(pn, result[0].lid)
                            
                            // Persist to MongoDB
                            if (fallbackHandler) {
                                try {
                                    await fallbackHandler.storeLidMapping(result[0].lid, pn)
                                } catch (err) {
                                    console.debug('[LIDResolver] Failed to persist mapping to MongoDB:', (err as Error).message)
                                }
                            }
                        }
                    } catch (err) {
                        console.debug('[LIDResolver] Legacy onWhatsApp failed for', pn, ':', (err as Error).message)
                        
                        // Fallback to MongoDB
                        if (fallbackHandler) {
                            try {
                                const lid = await fallbackHandler.getLidFromPhoneNumber(pn)
                                if (lid) map.set(pn, lid)
                            } catch {
                                // continue
                            }
                        }
                    }
                }
                
                return map
            },
            
            async getPNForLID(lid: string): Promise<string | null> {
                // Legacy API doesn't support reverse lookup
                // Use MongoDB fallback only
                return fallbackHandler?.getPhoneNumberFromLid(lid) || null
            },
            
            isUsingNativeStore(): boolean {
                return false
            },
            
            getNativeStore(): BaileysLIDMappingStore | null {
                return null
            }
        }
    }
    
    // No socket or unsupported - MongoDB fallback only
    return {
        async getLIDForPN(pn: string): Promise<string | null> {
            return fallbackHandler?.getLidFromPhoneNumber(pn) || null
        },
        
        async getLIDsForPNs(pns: string[]): Promise<Map<string, string>> {
            const map = new Map<string, string>()
            
            if (fallbackHandler) {
                for (const pn of pns) {
                    try {
                        const lid = await fallbackHandler.getLidFromPhoneNumber(pn)
                        if (lid) map.set(pn, lid)
                    } catch {
                        // continue
                    }
                }
            }
            
            return map
        },
        
        async getPNForLID(lid: string): Promise<string | null> {
            return fallbackHandler?.getPhoneNumberFromLid(lid) || null
        },
        
        isUsingNativeStore(): boolean {
            return false
        },
        
        getNativeStore(): BaileysLIDMappingStore | null {
            return null
        }
    }
}

/**
 * Sync MongoDB LID mappings to Baileys v7 native store
 * Useful for restoring mappings after reconnection
 * 
 * @param store - Baileys v7 LIDMappingStore
 * @param mappings - Array of LID mappings to sync
 */
export async function syncMappingsToNativeStore(
    store: BaileysLIDMappingStore,
    mappings: Array<{ lid: string; phoneNumber: string }>
): Promise<{ synced: number; failed: number }> {
    let synced = 0
    let failed = 0
    
    if (!store.storeLIDPNMappings) {
        // Fall back to individual storage if batch not available
        for (const { lid, phoneNumber } of mappings) {
            try {
                if (store.storeLIDPNMapping) {
                    await store.storeLIDPNMapping(lid, phoneNumber)
                    synced++
                }
            } catch (err) {
                console.debug('[syncMappingsToNativeStore] Failed to sync:', lid, '->', phoneNumber, (err as Error).message)
                failed++
            }
        }
    } else {
        try {
            await store.storeLIDPNMappings(
                mappings.map(({ lid, phoneNumber }) => ({ lid, pn: phoneNumber }))
            )
            synced = mappings.length
        } catch (err) {
            console.error('[syncMappingsToNativeStore] Batch sync failed:', (err as Error).message)
            failed = mappings.length
        }
    }
    
    return { synced, failed }
}
