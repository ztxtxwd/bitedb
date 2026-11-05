import { BiteDBError } from './errors.js'

interface Pending<TInput, TResult> {
  input: TInput
  resolve: (value: TResult) => void
  reject: (reason: unknown) => void
}

export interface BatcherOptions<TInput, TResult> {
  windowMs: number
  maxSize: number
  flush: (key: string, inputs: TInput[]) => Promise<TResult[]>
}

/**
 * Coalesces writes that arrive in the same event-loop window into one
 * batch_create call. One API request instead of N, one rate-limit unit.
 */
export class Batcher<TInput, TResult> {
  private readonly queues = new Map<string, Pending<TInput, TResult>[]>()
  private readonly timers = new Map<string, ReturnType<typeof setTimeout>>()
  private readonly inFlight = new Set<Promise<void>>()
  private closed = false

  constructor(private readonly options: BatcherOptions<TInput, TResult>) {}

  add(key: string, input: TInput): Promise<TResult> {
    if (this.closed) return Promise.reject(new BiteDBError('Batcher is closed'))
    return new Promise<TResult>((resolve, reject) => {
      let queue = this.queues.get(key)
      if (!queue) {
        queue = []
        this.queues.set(key, queue)
      }
      queue.push({ input, resolve, reject })

      if (queue.length >= this.options.maxSize) {
        void this.flushKey(key)
        return
      }
      if (!this.timers.has(key)) {
        const timer = setTimeout(() => {
          this.timers.delete(key)
          void this.flushKey(key)
        }, this.options.windowMs)
        // Keep the timer referenced: a caller awaiting insertOne() should keep
        // the process alive until the batch actually flushes. Unref'ing it lets
        // Node exit while the write is still buffered.
        this.timers.set(key, timer)
      }
    })
  }

  async flushKey(key: string): Promise<void> {
    const timer = this.timers.get(key)
    if (timer) {
      clearTimeout(timer)
      this.timers.delete(key)
    }
    const queue = this.queues.get(key)
    if (!queue || queue.length === 0) return
    this.queues.delete(key)

    const promise = this.run(key, queue)
    this.inFlight.add(promise)
    try {
      await promise
    } finally {
      this.inFlight.delete(promise)
    }
  }

  private async run(key: string, queue: Array<Pending<TInput, TResult>>): Promise<void> {
    try {
      const results = await this.options.flush(
        key,
        queue.map((item) => item.input),
      )
      if (results.length !== queue.length) {
        throw new BiteDBError(
          `Batch flush expected ${queue.length} results but got ${results.length}`,
        )
      }
      queue.forEach((item, index) => item.resolve(results[index]!))
    } catch (err) {
      queue.forEach((item) => item.reject(err))
    }
  }

  async flushAll(): Promise<void> {
    await Promise.all([...this.timers.keys()].map((key) => this.flushKey(key)))
    while (this.inFlight.size > 0) {
      await Promise.all([...this.inFlight])
    }
    // A flush may have queued more work through callbacks.
    if (this.queues.size > 0) await this.flushAll()
  }

  get pendingCount(): number {
    let count = 0
    for (const queue of this.queues.values()) count += queue.length
    return count
  }

  async close(): Promise<void> {
    this.closed = true
    await this.flushAll()
  }
}
