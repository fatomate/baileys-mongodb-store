import { ObjectId } from 'mongodb'
import {
    normalizeMessageTimestamp,
    resolvePreservedMessageTimestamp
} from '../messageTimestamp'

describe('message timestamp preservation', () => {
    const nowSeconds = 1750000000

    it('keeps the earliest valid timestamp when a message is re-upserted', () => {
        const timestamp = resolvePreservedMessageTimestamp({
            existingTimestamp: 1700000000,
            incomingTimestamp: 1750000000,
            nowSeconds
        })

        expect(timestamp).toBe(1700000000)
    })

    it('normalizes millisecond, string, and Long-like timestamps to numeric seconds', () => {
        expect(normalizeMessageTimestamp('1750000000000', nowSeconds)).toBe(1750000000)
        expect(normalizeMessageTimestamp({ toNumber: () => 1740000000123 }, nowSeconds)).toBe(1740000000)
        expect(normalizeMessageTimestamp({ toString: () => '1730000000' }, nowSeconds)).toBe(1730000000)
    })

    it('falls back when timestamps are invalid, zero, or future-ish', () => {
        const timestamp = resolvePreservedMessageTimestamp({
            existingTimestamp: 0,
            incomingTimestamp: nowSeconds + 3600,
            fallbackTimestamp: 1690000000,
            nowSeconds
        })

        expect(timestamp).toBe(1690000000)
        expect(normalizeMessageTimestamp('not-a-number', nowSeconds)).toBeNull()
    })

    it('normalizes MESSAGE_EDIT preservation from the original message timestamp', () => {
        const editTimestamp = resolvePreservedMessageTimestamp({
            existingTimestamp: '1700000000000',
            incomingTimestamp: 1750000000,
            nowSeconds
        })

        expect(editTimestamp).toBe(1700000000)
    })

    it('uses ObjectId generation time/current seconds for duplicate fallback when needed', () => {
        const objectId = ObjectId.createFromTime(1680000000)

        const timestamp = resolvePreservedMessageTimestamp({
            existingTimestamp: 'invalid',
            incomingTimestamp: null,
            fallbackTimestamp: objectId,
            nowSeconds
        })

        expect(timestamp).toBe(1680000000)
    })
})
