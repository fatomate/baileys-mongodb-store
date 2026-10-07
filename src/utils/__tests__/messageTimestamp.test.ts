import { Decimal128, Double, Int32, Long } from 'mongodb'
import { normalizeMessageTimestamp } from '../messageTimestamp'

// WAB-654 contract: a positive Int32 count of Unix seconds, or undefined. Never 0, NaN or a fraction.
const S = 1759100000

describe('normalizeMessageTimestamp', () => {
    describe('accepts every shape seen in wabot_messages', () => {
        it.each([
            ['int seconds', S],
            ['10-digit string (protobufjs longs:String)', '1759100000'],
            ['string with surrounding whitespace', ' 1759100000 '],
            ['plain {low, high, unsigned} (JSON of a Long)', { low: S, high: 0, unsigned: true }],
            ['{low, high} with BSON Int32 wrappers (promoteValues:false)', { low: new Int32(S), high: new Int32(0), unsigned: true }],
            ['Long', Long.fromNumber(S, true)],
            ['signed Long', Long.fromNumber(S)],
            ['{$numberLong}', { $numberLong: '1759100000' }],
            ['bigint', BigInt(S)],
            ['BSON Int32', new Int32(S)],
            ['BSON Double', new Double(S)],
            ['Decimal128', Decimal128.fromString('1759100000')],
        ])('%s', (_label, value) => {
            expect(normalizeMessageTimestamp(value)).toBe(S)
        })
    })

    describe('converts milliseconds to seconds by value, truncating', () => {
        it.each([
            ['ms number', 1759100000999, S],
            ['13-digit ms string', '1759100000999', S],
            ['ms Long', Long.fromNumber(1759100000999), S],
            ['ms {low, high}', (() => { const l = Long.fromNumber(1759100000999, true); return { low: l.low, high: l.high, unsigned: true } })(), S],
        ])('%s', (_label, value, expected) => {
            expect(normalizeMessageTimestamp(value)).toBe(expected)
        })

        it('keeps a leading-zero seconds string in seconds (unit is decided by value, not length)', () => {
            expect(normalizeMessageTimestamp('0001759100000')).toBe(S)
        })

        it('treats exactly 9999999999 as seconds, which is then out of Int32 range', () => {
            expect(normalizeMessageTimestamp(9999999999)).toBeUndefined()
        })
    })

    describe('truncates fractions exactly', () => {
        it.each([
            ['fractional number', 1759100000.9],
            ['decimal string that Number() would round up', '1759100000.999999999'],
            ['Decimal128 that Number() would round up', Decimal128.fromString('1759100000.999999999')],
        ])('%s', (_label, value) => {
            expect(normalizeMessageTimestamp(value)).toBe(S)
        })
    })

    describe('rejects values with no trustworthy seconds value', () => {
        it.each([
            ['undefined', undefined],
            ['null', null],
            ['empty string', ''],
            ['junk string', 'junk'],
            ['negative string', '-1759100000'],
            ['exponent string', '1.7591e9'],
            ['Decimal128 in exponent form', Decimal128.fromString('17591E5')],
            ['zero', 0],
            ['string zero', '0'],
            ['negative number', -5],
            ['NaN', Number.NaN],
            ['Infinity', Number.POSITIVE_INFINITY],
            ['unsafe integer', Number.MAX_SAFE_INTEGER + 2],
            ['unsafe Long', Long.MAX_UNSIGNED_VALUE],
            ['over Int32 seconds (after 2038-01-19)', 2147483648],
            ['{low, high} with a non-integer part', { low: 1.5, high: 0 }],
            ['{low, high} outside int32', { low: 2 ** 32, high: 0 }],
            ['object with other keys', { seconds: S }],
            ['array holding a valid value', [S]],
            ['boolean', true],
            ['Date', new Date(S * 1000)],
            // JSON-compatible junk that only claims to be a BSON wrapper must not throw.
            ['fake Long tag', { _bsontype: 'Long' }],
            ['fake Int32 tag with no valueOf', { _bsontype: 'Int32', valueOf: null }],
            ['fake Decimal128 tag', { _bsontype: 'Decimal128', toString: null }],
            ['{low, high} with a fake Int32 part', { low: { _bsontype: 'Int32', valueOf: null }, high: 0 }],
        ])('%s', (_label, value) => {
            expect(normalizeMessageTimestamp(value)).toBeUndefined()
        })
    })

    it('always returns an Int32-safe integer', () => {
        for (const value of [S, '1759100000', { low: S, high: 0 }, Long.fromNumber(S), 1759100000999, '1759100000.5']) {
            const result = normalizeMessageTimestamp(value)
            expect(Number.isInteger(result)).toBe(true)
            expect(result).toBeGreaterThan(0)
            expect(result).toBeLessThanOrEqual(2147483647)
        }
    })
})
