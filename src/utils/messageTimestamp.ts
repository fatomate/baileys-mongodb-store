const FUTURE_TIMESTAMP_LEEWAY_SECONDS = 5 * 60
const MILLISECOND_TIMESTAMP_THRESHOLD = 100000000000

type TimestampInput = unknown

const currentSeconds = (): number => Math.floor(Date.now() / 1000)

const toNumericTimestamp = (value: TimestampInput): number | null => {
    if (value === null || value === undefined) {
        return null
    }

    if (typeof value === 'number') {
        return value
    }

    if (typeof value === 'string') {
        const trimmed = value.trim()
        if (!trimmed) {
            return null
        }
        return Number(trimmed)
    }

    if (value instanceof Date) {
        return value.getTime()
    }

    if (typeof value === 'object') {
        const timestampLike = value as {
            toNumber?: () => number
            toString?: () => string
            getTimestamp?: () => Date
        }

        if (typeof timestampLike.toNumber === 'function') {
            return timestampLike.toNumber()
        }

        if (typeof timestampLike.getTimestamp === 'function') {
            return timestampLike.getTimestamp().getTime()
        }

        if (typeof timestampLike.toString === 'function') {
            const stringValue = timestampLike.toString()
            if (stringValue && stringValue !== '[object Object]') {
                return Number(stringValue)
            }
        }
    }

    return null
}

export const normalizeMessageTimestamp = (
    value: TimestampInput,
    nowSeconds: number = currentSeconds()
): number | null => {
    const numeric = toNumericTimestamp(value)
    if (numeric === null || !Number.isFinite(numeric) || numeric <= 0) {
        return null
    }

    const seconds = Math.floor(
        numeric >= MILLISECOND_TIMESTAMP_THRESHOLD
            ? numeric / 1000
            : numeric
    )

    if (seconds <= 0 || seconds > nowSeconds + FUTURE_TIMESTAMP_LEEWAY_SECONDS) {
        return null
    }

    return seconds
}

export const resolvePreservedMessageTimestamp = ({
    existingTimestamp,
    incomingTimestamp,
    fallbackTimestamp,
    nowSeconds = currentSeconds()
}: {
    existingTimestamp?: TimestampInput
    incomingTimestamp?: TimestampInput
    fallbackTimestamp?: TimestampInput
    nowSeconds?: number
}): number => {
    const existing = normalizeMessageTimestamp(existingTimestamp, nowSeconds)
    const incoming = normalizeMessageTimestamp(incomingTimestamp, nowSeconds)
    const fallback = normalizeMessageTimestamp(fallbackTimestamp, nowSeconds) ?? nowSeconds

    if (existing !== null) {
        return incoming !== null ? Math.min(existing, incoming) : existing
    }

    return incoming ?? fallback
}
