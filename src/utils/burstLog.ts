// WAB-978: a pool close can fail thousands of writes in one second. Log one summary line
// per label and window instead of one line per failure.

const DEFAULT_WINDOW_MS = 5000

type Bucket = { counts: Map<string, number>; timer: NodeJS.Timeout | null; windowMs: number }

const buckets = new Map<string, Bucket>()
const totals = new Map<string, number>()

/** Count one failure. The first failure in a window schedules one summary line for the window. */
export function reportBurst(label: string, kind: string, windowMs: number = DEFAULT_WINDOW_MS): void {
    totals.set(label, (totals.get(label) ?? 0) + 1)
    let bucket = buckets.get(label)
    if (!bucket) {
        bucket = { counts: new Map(), timer: null, windowMs }
        buckets.set(label, bucket)
    }
    bucket.counts.set(kind, (bucket.counts.get(kind) ?? 0) + 1)
    if (!bucket.timer) {
        bucket.windowMs = windowMs
        bucket.timer = setTimeout(() => flushBurst(label), windowMs)
        bucket.timer.unref?.()
    }
}

/** Write the summary line for a label now, if it has unreported failures. */
export function flushBurst(label: string): void {
    const bucket = buckets.get(label)
    if (!bucket) return
    buckets.delete(label)
    if (bucket.timer) clearTimeout(bucket.timer)
    let total = 0
    const kinds: string[] = []
    for (const [kind, count] of bucket.counts) {
        total += count
        kinds.push(`${kind}×${count}`)
    }
    if (total === 0) return
    console.error(`${label}: ${total} failure(s) in ${Math.round(bucket.windowMs / 1000)}s (${kinds.join(', ')})`)
}

/** Failures counted for a label since the process started. */
export function burstTotal(label: string): number {
    return totals.get(label) ?? 0
}

export function errorKind(error: unknown): string {
    const e = error as any
    return e?.codeName || e?.name || 'Error'
}
