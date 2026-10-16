// WAB-978: first-in-first-out limiter for bursty event persistence.

export type TaskLimiter = {
    run<T>(task: () => Promise<T>): Promise<T>
    /** Resolves when no task is queued or running. */
    idle(): Promise<void>
    /** Drop tasks that have not started. Returns the count. Their promises reject. */
    clear(reason: Error): number
    readonly active: number
    readonly queued: number
}

export function createTaskLimiter(concurrency: number): TaskLimiter {
    const limit = Number.isInteger(concurrency) && concurrency > 0 ? concurrency : 1
    const queue: Array<{ start: () => void; cancel: (e: Error) => void }> = []
    let active = 0
    let idleWaiters: Array<() => void> = []

    const settle = () => {
        if (active === 0 && queue.length === 0) {
            const waiters = idleWaiters
            idleWaiters = []
            waiters.forEach((resolve) => resolve())
        }
    }

    const next = () => {
        while (active < limit && queue.length > 0) {
            queue.shift()!.start()
        }
        settle()
    }

    return {
        run<T>(task: () => Promise<T>): Promise<T> {
            return new Promise<T>((resolve, reject) => {
                queue.push({
                    start: () => {
                        active++
                        task().then(resolve, reject).finally(() => {
                            active--
                            next()
                        })
                    },
                    cancel: reject,
                })
                next()
            })
        },
        idle(): Promise<void> {
            if (active === 0 && queue.length === 0) return Promise.resolve()
            return new Promise((resolve) => idleWaiters.push(resolve))
        },
        clear(reason: Error): number {
            const dropped = queue.splice(0)
            dropped.forEach((item) => item.cancel(reason))
            settle()
            return dropped.length
        },
        get active() { return active },
        get queued() { return queue.length },
    }
}
