import { burstTotal, errorKind, flushBurst, reportBurst } from '../burstLog'

// WAB-1248: write errors have no codeName, so the numeric code is the only clue to the cause
describe('errorKind', () => {
    it('adds the numeric Mongo code to the kind', () => {
        expect(errorKind({ name: 'MongoServerError', code: 11000 })).toBe('MongoServerError:11000')
        expect(errorKind({ name: 'MongoServerError', codeName: 'WriteConflict', code: 112 })).toBe('WriteConflict:112')
    })

    it('keeps the kind when there is no numeric code', () => {
        expect(errorKind({ name: 'MongoPoolClosedError' })).toBe('MongoPoolClosedError')
        expect(errorKind({ name: 'Error', code: 'ECONNRESET' })).toBe('Error')
        expect(errorKind(undefined)).toBe('Error')
        expect(errorKind('boom')).toBe('Error')
    })
})

describe('WAB-978 burst log summary', () => {
    let errorSpy: jest.SpyInstance

    beforeEach(() => {
        jest.useFakeTimers()
        errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {})
    })
    afterEach(() => {
        jest.useRealTimers()
        errorSpy.mockRestore()
    })

    it('logs one summary line with the count for a burst of failures', () => {
        const label = '[LidHandler] Failed to store LID mapping (test-1)'
        for (let i = 0; i < 998; i++) reportBurst(label, 'MongoPoolClosedError')
        reportBurst(label, 'MongoNotConnectedError')
        reportBurst(label, 'MongoNotConnectedError')

        expect(errorSpy).not.toHaveBeenCalled()
        jest.advanceTimersByTime(5000)

        expect(errorSpy).toHaveBeenCalledTimes(1)
        expect(errorSpy.mock.calls[0][0]).toBe(
            `${label}: 1000 failure(s) in 5s (MongoPoolClosedError×998, MongoNotConnectedError×2)`
        )
        expect(burstTotal(label)).toBe(1000)
    })

    it('starts a new window after a summary', () => {
        const label = 'test-2'
        reportBurst(label, 'A')
        jest.advanceTimersByTime(5000)
        reportBurst(label, 'A')
        jest.advanceTimersByTime(5000)

        expect(errorSpy).toHaveBeenCalledTimes(2)
        flushBurst(label)
        expect(errorSpy).toHaveBeenCalledTimes(2)
    })
})
