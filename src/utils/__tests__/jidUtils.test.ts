import {
    normalizeJidForComparison,
    extractBaseJid,
    getJidDomain,
    areJidsEquivalent,
    isLidFormat,
    isPhoneNumberFormat,
    isLidAndPhonePair,
    extractLidPhonePair,
    mightBeRelatedJids,
    getJidVariations,
    formatJidForDisplay,
    isGroupJid,
    isBroadcastJid,
    getJidCacheKey
} from '../jidUtils'

describe('JID Utilities', () => {
    describe('normalizeJidForComparison', () => {
        it('should remove :XX suffix from JID', () => {
            expect(normalizeJidForComparison('114194640801953:73@lid')).toBe('114194640801953@lid')
            expect(normalizeJidForComparison('60196953307:45@s.whatsapp.net')).toBe('60196953307@s.whatsapp.net')
        })

        it('should return unchanged JID if no suffix', () => {
            expect(normalizeJidForComparison('114194640801953@lid')).toBe('114194640801953@lid')
            expect(normalizeJidForComparison('60196953307@s.whatsapp.net')).toBe('60196953307@s.whatsapp.net')
        })

        it('should handle null/undefined', () => {
            expect(normalizeJidForComparison(null)).toBe('')
            expect(normalizeJidForComparison(undefined)).toBe('')
            expect(normalizeJidForComparison('')).toBe('')
        })
    })

    describe('extractBaseJid', () => {
        it('should extract base number from JID', () => {
            expect(extractBaseJid('114194640801953:73@lid')).toBe('114194640801953')
            expect(extractBaseJid('60196953307@s.whatsapp.net')).toBe('60196953307')
            expect(extractBaseJid('123456789@g.us')).toBe('123456789')
        })

        it('should handle null/undefined', () => {
            expect(extractBaseJid(null)).toBe('')
            expect(extractBaseJid(undefined)).toBe('')
        })
    })

    describe('getJidDomain', () => {
        it('should extract domain from JID', () => {
            expect(getJidDomain('114194640801953:73@lid')).toBe('lid')
            expect(getJidDomain('60196953307@s.whatsapp.net')).toBe('s.whatsapp.net')
            expect(getJidDomain('123456789@g.us')).toBe('g.us')
        })

        it('should return empty for invalid JID', () => {
            expect(getJidDomain('no-at-sign')).toBe('')
            expect(getJidDomain(null)).toBe('')
        })
    })

    describe('areJidsEquivalent', () => {
        it('should identify equivalent JIDs with different suffixes', () => {
            expect(areJidsEquivalent('114194640801953:73@lid', '114194640801953@lid')).toBe(true)
            expect(areJidsEquivalent('60196953307:45@s.whatsapp.net', '60196953307@s.whatsapp.net')).toBe(true)
            expect(areJidsEquivalent('114194640801953:73@lid', '114194640801953:99@lid')).toBe(true)
        })

        it('should identify non-equivalent JIDs', () => {
            expect(areJidsEquivalent('114194640801953@lid', '60196953307@lid')).toBe(false)
            expect(areJidsEquivalent('114194640801953@lid', '114194640801953@s.whatsapp.net')).toBe(false)
        })

        it('should handle null/undefined', () => {
            expect(areJidsEquivalent(null, '114194640801953@lid')).toBe(false)
            expect(areJidsEquivalent('114194640801953@lid', null)).toBe(false)
            expect(areJidsEquivalent(null, null)).toBe(false)
        })
    })

    describe('isLidFormat', () => {
        it('should identify LID format', () => {
            expect(isLidFormat('114194640801953@lid')).toBe(true)
            expect(isLidFormat('114194640801953:73@lid')).toBe(true)
            expect(isLidFormat('123@lid')).toBe(true)
        })

        it('should reject non-LID format', () => {
            expect(isLidFormat('60196953307@s.whatsapp.net')).toBe(false)
            expect(isLidFormat('123456789@g.us')).toBe(false)
            expect(isLidFormat('status@broadcast')).toBe(false)
        })

        it('should handle null/undefined', () => {
            expect(isLidFormat(null)).toBe(false)
            expect(isLidFormat(undefined)).toBe(false)
            expect(isLidFormat('')).toBe(false)
        })
    })

    describe('isPhoneNumberFormat', () => {
        it('should identify phone number format', () => {
            expect(isPhoneNumberFormat('60196953307@s.whatsapp.net')).toBe(true)
            expect(isPhoneNumberFormat('60196953307:45@s.whatsapp.net')).toBe(true)
            expect(isPhoneNumberFormat('123456789@c.us')).toBe(true)
        })

        it('should reject non-phone format', () => {
            expect(isPhoneNumberFormat('114194640801953@lid')).toBe(false)
            expect(isPhoneNumberFormat('123456789@g.us')).toBe(false)
            expect(isPhoneNumberFormat('status@broadcast')).toBe(false)
        })
    })

    describe('isLidAndPhonePair', () => {
        it('should identify valid LID-phone pairs', () => {
            expect(isLidAndPhonePair('114194640801953@lid', '60196953307@s.whatsapp.net')).toBe(true)
            expect(isLidAndPhonePair('60196953307@s.whatsapp.net', '114194640801953@lid')).toBe(true)
            expect(isLidAndPhonePair('114194640801953:73@lid', '60196953307@c.us')).toBe(true)
        })

        it('should reject invalid pairs', () => {
            expect(isLidAndPhonePair('114194640801953@lid', '222222@lid')).toBe(false)
            expect(isLidAndPhonePair('60196953307@s.whatsapp.net', '111111@s.whatsapp.net')).toBe(false)
            expect(isLidAndPhonePair('123456789@g.us', '987654321@g.us')).toBe(false)
        })

        it('should handle null/undefined', () => {
            expect(isLidAndPhonePair(null, '114194640801953@lid')).toBe(false)
            expect(isLidAndPhonePair('114194640801953@lid', null)).toBe(false)
        })
    })

    describe('extractLidPhonePair', () => {
        it('should extract LID and phone from valid pair', () => {
            const pair1 = extractLidPhonePair('114194640801953@lid', '60196953307@s.whatsapp.net')
            expect(pair1).toEqual({
                lid: '114194640801953@lid',
                phoneNumber: '60196953307@s.whatsapp.net'
            })

            const pair2 = extractLidPhonePair('60196953307@s.whatsapp.net', '114194640801953:73@lid')
            expect(pair2).toEqual({
                lid: '114194640801953@lid',
                phoneNumber: '60196953307@s.whatsapp.net'
            })
        })

        it('should return null for invalid pair', () => {
            expect(extractLidPhonePair('114194640801953@lid', '222222@lid')).toBeNull()
            expect(extractLidPhonePair('60196953307@s.whatsapp.net', '111111@s.whatsapp.net')).toBeNull()
        })
    })

    describe('mightBeRelatedJids', () => {
        it('should identify potentially related JIDs', () => {
            // Same JID with different suffixes
            expect(mightBeRelatedJids('114194640801953:73@lid', '114194640801953@lid')).toBe(true)
            
            // Different domains but same base - only if LID/phone pair
            expect(mightBeRelatedJids('60196953307@lid', '60196953307@s.whatsapp.net')).toBe(true)
        })

        it('should reject unrelated JIDs', () => {
            expect(mightBeRelatedJids('114194640801953@lid', '222222@lid')).toBe(false)
            expect(mightBeRelatedJids('60196953307@s.whatsapp.net', '111111@s.whatsapp.net')).toBe(false)
        })
    })

    describe('getJidVariations', () => {
        it('should generate variations for JID', () => {
            const variations1 = getJidVariations('114194640801953:73@lid')
            expect(variations1).toContain('114194640801953:73@lid')
            expect(variations1).toContain('114194640801953@lid')

            const variations2 = getJidVariations('60196953307@s.whatsapp.net')
            expect(variations2).toContain('60196953307@s.whatsapp.net')
            expect(variations2).toContain('60196953307@c.us')
        })
    })

    describe('formatJidForDisplay', () => {
        it('should format JID for display', () => {
            expect(formatJidForDisplay('114194640801953:73@lid')).toBe('114194640801953@lid')
            expect(formatJidForDisplay('60196953307@s.whatsapp.net')).toBe('60196953307@s.whatsapp.net')
            expect(formatJidForDisplay(null)).toBe('Unknown')
        })
    })

    describe('isGroupJid', () => {
        it('should identify group JIDs', () => {
            expect(isGroupJid('123456789-987654321@g.us')).toBe(true)
            expect(isGroupJid('123456789@g.us')).toBe(true)
        })

        it('should reject non-group JIDs', () => {
            expect(isGroupJid('60196953307@s.whatsapp.net')).toBe(false)
            expect(isGroupJid('114194640801953@lid')).toBe(false)
        })
    })

    describe('isBroadcastJid', () => {
        it('should identify broadcast JIDs', () => {
            expect(isBroadcastJid('status@broadcast')).toBe(true)
            expect(isBroadcastJid('123456789@broadcast')).toBe(true)
        })

        it('should reject non-broadcast JIDs', () => {
            expect(isBroadcastJid('60196953307@s.whatsapp.net')).toBe(false)
            expect(isBroadcastJid('114194640801953@lid')).toBe(false)
        })
    })

    describe('getJidCacheKey', () => {
        it('should generate cache key from JID', () => {
            expect(getJidCacheKey('114194640801953:73@lid')).toBe('114194640801953_lid')
            expect(getJidCacheKey('60196953307@s.whatsapp.net')).toBe('60196953307_s_whatsapp_net')
        })
    })
})