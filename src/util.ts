import { createHash, randomBytes, randomUUID } from 'node:crypto'

export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

export { randomUUID }

const CROCKFORD = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

/** Monotonic-ish, lexicographically sortable ULID. 26 chars, no dependencies. */
export function generateId(now = Date.now()): string {
  let time = now
  let timePart = ''
  for (let i = 0; i < 10; i++) {
    timePart = CROCKFORD[time % 32]! + timePart
    time = Math.floor(time / 32)
  }
  let randomPart = ''
  for (const byte of randomBytes(16)) {
    randomPart += CROCKFORD[byte % 32]
  }
  return (timePart + randomPart).slice(0, 26)
}

export function sha256(input: string | Uint8Array): string {
  return createHash('sha256').update(input).digest('hex')
}

export function isPlainObject(value: unknown): value is Record<string, any> {
  if (value === null || typeof value !== 'object') return false
  const proto = Object.getPrototypeOf(value)
  return proto === Object.prototype || proto === null
}

export function isBinary(value: unknown): value is Buffer | Uint8Array | ArrayBuffer {
  return (
    Buffer.isBuffer(value) ||
    value instanceof Uint8Array ||
    value instanceof ArrayBuffer
  )
}

export function toBuffer(value: Buffer | Uint8Array | ArrayBuffer | string): Buffer {
  if (typeof value === 'string') return Buffer.from(value, 'utf8')
  if (Buffer.isBuffer(value)) return value
  if (value instanceof ArrayBuffer) return Buffer.from(value)
  return Buffer.from(value.buffer, value.byteOffset, value.byteLength)
}

export function byteLength(value: unknown): number {
  if (typeof value === 'string') return Buffer.byteLength(value)
  if (isBinary(value)) return toBuffer(value).byteLength
  return Buffer.byteLength(JSON.stringify(value))
}

export function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value))
}

export class Mutex {
  private tail: Promise<unknown> = Promise.resolve()

  runExclusive<T>(fn: () => Promise<T> | T): Promise<T> {
    const run = this.tail.then(fn, fn)
    this.tail = run.catch(() => undefined)
    return run
  }
}

/** Simple token bucket used to stay under Feishu rate limits. */
export class TokenBucket {
  private tokens: number
  private lastRefill: number

  constructor(
    private readonly ratePerSecond: number,
    private readonly burst = Math.max(1, Math.ceil(ratePerSecond)),
  ) {
    this.tokens = burst
    this.lastRefill = Date.now()
  }

  async take(count = 1): Promise<void> {
    if (this.ratePerSecond <= 0) return
    while (true) {
      this.refill()
      if (this.tokens >= count) {
        this.tokens -= count
        return
      }
      const needed = count - this.tokens
      const waitMs = Math.max(5, Math.ceil((needed / this.ratePerSecond) * 1000))
      await sleep(waitMs)
    }
  }

  private refill(): void {
    const now = Date.now()
    const elapsed = now - this.lastRefill
    if (elapsed <= 0) return
    this.tokens = Math.min(this.burst, this.tokens + (elapsed / 1000) * this.ratePerSecond)
    this.lastRefill = now
  }
}

/** Limit concurrent async operations. */
export class Semaphore {
  private active = 0
  private queue: Array<() => void> = []

  constructor(private readonly concurrency: number) {}

  async run<T>(fn: () => Promise<T>): Promise<T> {
    if (this.active >= this.concurrency) {
      await new Promise<void>((resolve) => this.queue.push(resolve))
    }
    this.active++
    try {
      return await fn()
    } finally {
      this.active--
      this.queue.shift()?.()
    }
  }
}

export function truncate(value: string, max: number): string {
  if (value.length <= max) return value
  return value.slice(0, max)
}

export function normalizeUrl(url: string): string {
  return url.replace(/\/+$/, '')
}


export function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value))
}

const UNSAFE_PROPERTY_KEYS = new Set(['__proto__', 'constructor', 'prototype'])

export function isSafePropertyKey(key: string): boolean {
  return !UNSAFE_PROPERTY_KEYS.has(key)
}

/** Define an own data property even for keys like `__proto__`. */
export function setOwn(target: Record<string, any>, key: string, value: unknown): void {
  Object.defineProperty(target, key, {
    value,
    enumerable: true,
    configurable: true,
    writable: true,
  })
}
