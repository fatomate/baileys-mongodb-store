import {
    validateJID,
    validateMessageId,
    validateInstanceId,
    isValidJID,
    isValidMessageId,
    isValidInstanceId,
    isValidLabelId,
    ValidationError,
    AuthorizationError,
    SecurityError,
    createSafeErrorMessage,
    sanitizeForLogging,
    hashForLogging,
    checkInstanceAccess,
    validatePagination,
    validateMongoQuery
} from '../security'

describe('Security Utilities', () => {
    describe('JID Validation', () => {
        describe('isValidJID', () => {
            // Valid JIDs
            test('should accept valid WhatsApp user JID', () => {
                expect(isValidJID('1234567890@s.whatsapp.net')).toBe(true)
                expect(isValidJID('911234567890@s.whatsapp.net')).toBe(true)
            })

            test('should accept valid group JID', () => {
                expect(isValidJID('1234567890-1234567890@g.us')).toBe(true)
                expect(isValidJID('120363999999999999-1609999999@g.us')).toBe(true)
                expect(isValidJID('120363322411650683@g.us')).toBe(true) // Groups without hyphen
                expect(isValidJID('1234567890@g.us')).toBe(true) // Simple group format
            })

            test('should accept valid broadcast JID', () => {
                expect(isValidJID('1234567890@broadcast')).toBe(true)
                expect(isValidJID('status@broadcast')).toBe(true)
            })

            test('should accept valid linked device JID', () => {
                expect(isValidJID('1234567890@lid')).toBe(true)
            })

            test('should accept valid contact JID', () => {
                expect(isValidJID('1234567890@c.us')).toBe(true)
            })

            // Invalid JIDs
            test('should reject invalid JID formats', () => {
                expect(isValidJID('invalid@jid')).toBe(false)
                expect(isValidJID('user@example.com')).toBe(false)
                expect(isValidJID('not-a-number@s.whatsapp.net')).toBe(false)
                expect(isValidJID('1234567890')).toBe(false)
                expect(isValidJID('@s.whatsapp.net')).toBe(false)
                expect(isValidJID('')).toBe(false)
                expect(isValidJID(null as any)).toBe(false)
                expect(isValidJID(undefined as any)).toBe(false)
            })
        })

        describe('validateJID', () => {
            test('should return valid JID unchanged', () => {
                const validJid = '1234567890@s.whatsapp.net'
                expect(validateJID(validJid)).toBe(validJid)
            })

            test('should throw ValidationError for invalid JID', () => {
                expect(() => validateJID('invalid@jid')).toThrow(ValidationError)
                expect(() => validateJID('invalid@jid')).toThrow('Invalid JID format')
            })

            test('should trim whitespace from valid JID', () => {
                expect(validateJID('  1234567890@s.whatsapp.net  ')).toBe('1234567890@s.whatsapp.net')
            })
        })
    })

    describe('Message ID Validation', () => {
        describe('isValidMessageId', () => {
            test('should accept message IDs that meet the lenient safety rules', () => {
                expect(isValidMessageId('3EB0ABC123DEF456')).toBe(true)
                expect(isValidMessageId('BAE5ABC123DEF456789012')).toBe(true)
                expect(isValidMessageId('3A2B4C6D8E0F1A2B4C6D8E0F1A2B4C6D')).toBe(true) // 32 chars
                expect(isValidMessageId('1679765488')).toBe(true) // Numeric 10 digits
                expect(isValidMessageId('1234567890123456')).toBe(true) // Numeric 16 digits
                expect(isValidMessageId('too-short')).toBe(true) // Length/charset are intentionally lenient
                expect(isValidMessageId('lowercase123456')).toBe(true)
                expect(isValidMessageId('INVALID-CHARS!@#')).toBe(true)
                expect(isValidMessageId('123456789')).toBe(true)
            })

            test('should reject unsafe message IDs', () => {
                expect(isValidMessageId('ab')).toBe(false)
                expect(isValidMessageId('contains$dollar')).toBe(false)
                expect(isValidMessageId('contains{brace')).toBe(false)
                expect(isValidMessageId('contains}brace')).toBe(false)
                expect(isValidMessageId('')).toBe(false)
            })
        })

        describe('validateMessageId', () => {
            test('should return valid message ID unchanged', () => {
                const validId = '3EB0ABC123DEF456'
                expect(validateMessageId(validId)).toBe(validId)
            })

            test('should throw ValidationError for unsafe message ID', () => {
                expect(() => validateMessageId('invalid$id')).toThrow(ValidationError)
                expect(() => validateMessageId('invalid$id')).toThrow('Invalid message ID format')
            })
        })
    })

    describe('Instance ID Validation', () => {
        describe('isValidInstanceId', () => {
            test('should accept valid instance IDs', () => {
                expect(isValidInstanceId('instance-1')).toBe(true)
                expect(isValidInstanceId('my_instance_123')).toBe(true)
                expect(isValidInstanceId('abc')).toBe(true) // 3 chars minimum
                expect(isValidInstanceId('a'.repeat(50))).toBe(true) // 50 chars maximum
            })

            test('should reject invalid instance IDs', () => {
                expect(isValidInstanceId('ab')).toBe(false) // too short
                expect(isValidInstanceId('a'.repeat(51))).toBe(false) // too long
                expect(isValidInstanceId('instance@123')).toBe(false) // invalid chars
                expect(isValidInstanceId('instance!@#')).toBe(false)
                expect(isValidInstanceId('')).toBe(false)
            })
        })

        describe('validateInstanceId', () => {
            test('should return valid instance ID unchanged', () => {
                const validId = 'my-instance-123'
                expect(validateInstanceId(validId)).toBe(validId)
            })

            test('should throw ValidationError for invalid instance ID', () => {
                expect(() => validateInstanceId('a')).toThrow(ValidationError)
                expect(() => validateInstanceId('a')).toThrow('Invalid instance ID format')
            })
        })
    })

    describe('Label ID Validation', () => {
        describe('isValidLabelId', () => {
            test('should accept valid label IDs', () => {
                expect(isValidLabelId('1')).toBe(true)
                expect(isValidLabelId('123')).toBe(true)
                expect(isValidLabelId('12345678901234567890')).toBe(true) // 20 digits max
            })

            test('should reject invalid label IDs', () => {
                expect(isValidLabelId('abc')).toBe(false) // not numeric
                expect(isValidLabelId('123abc')).toBe(false)
                expect(isValidLabelId('123456789012345678901')).toBe(false) // too long
                expect(isValidLabelId('')).toBe(false)
            })
        })
    })

    describe('Error Handling', () => {
        describe('createSafeErrorMessage', () => {
            test('should return safe message for ValidationError', () => {
                const error = new ValidationError('Invalid input: secret data')
                expect(createSafeErrorMessage(error, 'testOp')).toBe('Validation failed for testOp')
            })

            test('should return safe message for AuthorizationError', () => {
                const error = new AuthorizationError('User 123 cannot access resource 456')
                expect(createSafeErrorMessage(error, 'testOp')).toBe('Access denied')
            })

            test('should handle MongoDB duplicate key error', () => {
                const error = new Error('E11000 duplicate key error')
                expect(createSafeErrorMessage(error, 'testOp')).toBe('Duplicate entry detected')
            })

            test('should handle connection errors', () => {
                const error = new Error('ECONNREFUSED: connection refused')
                expect(createSafeErrorMessage(error, 'testOp')).toBe('Service temporarily unavailable')
            })

            test('should return generic message for unknown errors', () => {
                const error = new Error('Some internal error with sensitive data')
                expect(createSafeErrorMessage(error, 'testOp')).toBe('Operation failed: testOp')
            })
        })

        describe('sanitizeForLogging', () => {
            test('should truncate long strings', () => {
                const longString = 'a'.repeat(100)
                expect(sanitizeForLogging(longString, 10)).toBe('aaaaaaaaaa...')
            })

            test('should handle empty strings', () => {
                expect(sanitizeForLogging('')).toBe('[empty]')
            })

            test('should handle non-string values', () => {
                expect(sanitizeForLogging(null as any)).toBe('[empty]')
                expect(sanitizeForLogging(123 as any)).toBe('[non-string]')
            })

            test('should not truncate short strings', () => {
                expect(sanitizeForLogging('short', 50)).toBe('short')
            })
        })

        describe('hashForLogging', () => {
            test('should create consistent hash', () => {
                const data = 'sensitive-data'
                const hash1 = hashForLogging(data)
                const hash2 = hashForLogging(data)
                expect(hash1).toBe(hash2)
                expect(hash1).toHaveLength(8)
            })

            test('should create different hashes for different data', () => {
                const hash1 = hashForLogging('data1')
                const hash2 = hashForLogging('data2')
                expect(hash1).not.toBe(hash2)
            })
        })
    })

    describe('Access Control', () => {
        describe('checkInstanceAccess', () => {
            test('should allow access to same instance', () => {
                expect(() => checkInstanceAccess('instance-1', 'instance-1')).not.toThrow()
            })

            test('should deny access to different instance', () => {
                expect(() => checkInstanceAccess('instance-1', 'instance-2')).toThrow(AuthorizationError)
                expect(() => checkInstanceAccess('instance-1', 'instance-2')).toThrow('Access denied: instance mismatch')
            })
        })
    })

    describe('Pagination Validation', () => {
        test('should validate pagination parameters', () => {
            expect(validatePagination(10, 20)).toEqual({ limit: 10, offset: 20 })
        })

        test('should enforce maximum limit', () => {
            expect(validatePagination(5000, 0)).toEqual({ limit: 1000, offset: 0 })
        })

        test('should enforce minimum values', () => {
            expect(validatePagination(-10, -20)).toEqual({ limit: 1, offset: 0 })
        })

        test('should provide defaults', () => {
            expect(validatePagination()).toEqual({ limit: 100, offset: 0 })
        })
    })

    describe('MongoDB Query Validation', () => {
        test('should allow safe queries', () => {
            expect(() => validateMongoQuery({ id: '123' })).not.toThrow()
            expect(() => validateMongoQuery({ name: { $eq: 'test' } })).not.toThrow()
            expect(() => validateMongoQuery({ age: { $gte: 18 } })).not.toThrow()
        })

        test('should block $where operator', () => {
            expect(() => validateMongoQuery({ $where: 'this.age > 18' })).toThrow(SecurityError)
            expect(() => validateMongoQuery({ $where: 'this.age > 18' })).toThrow('Dangerous operator not allowed: $where')
        })

        test('should block $expr operator', () => {
            expect(() => validateMongoQuery({ $expr: { $gt: ['$age', 18] } })).toThrow(SecurityError)
        })

        test('should block nested dangerous operators', () => {
            expect(() => validateMongoQuery({
                nested: {
                    field: {
                        $where: 'malicious code'
                    }
                }
            })).toThrow(SecurityError)
        })

        test('should block $function operator', () => {
            expect(() => validateMongoQuery({ 
                $function: {
                    body: 'function() { return true; }',
                    args: [],
                    lang: 'js'
                }
            })).toThrow(SecurityError)
        })
    })
})