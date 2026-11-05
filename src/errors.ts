export class BiteDBError extends Error {
  readonly code?: string | number
  readonly cause?: unknown

  constructor(message: string, options: { code?: string | number; cause?: unknown } = {}) {
    super(message)
    this.name = new.target.name
    this.code = options.code
    this.cause = options.cause
    Error.captureStackTrace?.(this, new.target)
  }
}

export class ConfigError extends BiteDBError {}

export class ValidationError extends BiteDBError {}

export class NotFoundError extends BiteDBError {}

export class UnsupportedError extends BiteDBError {}

/** An error returned by the Feishu/Lark Open Platform. */
export class FeishuApiError extends BiteDBError {
  readonly status: number
  readonly apiCode: number
  readonly apiMsg: string
  readonly method: string
  readonly path: string
  readonly errorLogId?: string
  readonly retryable: boolean
  readonly response?: unknown

  constructor(options: {
    message?: string
    status: number
    code: number
    msg: string
    method: string
    path: string
    errorLogId?: string
    retryable?: boolean
    response?: unknown
    cause?: unknown
  }) {
    super(options.message ?? `Feishu API ${options.method} ${options.path} failed: [${options.code}] ${options.msg}`, {
      code: options.code,
      cause: options.cause,
    })
    this.status = options.status
    this.apiCode = options.code
    this.apiMsg = options.msg
    this.method = options.method
    this.path = options.path
    this.errorLogId = options.errorLogId
    this.retryable = options.retryable ?? (isRetryableCode(options.code) || options.status === 429 || options.status >= 500)
    this.response = options.response
  }
}

/**
 * Codes that are safe to retry. Rate limits and write conflicts are documented
 * as transient; 1255040 / 1254607 are Bitable server-side timeouts.
 */
const RETRYABLE_CODES = new Set([
  99991400, // global rate limit
  1254290, // bitable rate limit
  1254291, // write conflict / concurrent write
  1255040, // server timeout
  1254607, // transient server error
  1254608, // duplicate idempotency request (for the same client_token)
  1061045, // drive/media upload rate limit
])

export function isRetryableCode(code: number): boolean {
  return RETRYABLE_CODES.has(code)
}

export function isTableNotFound(err: unknown): boolean {
  if (err instanceof FeishuApiError) {
    return err.apiCode === 1254040 || err.apiCode === 1254041 || err.apiCode === 1254002
  }
  return false
}

export function isFieldNameConflict(err: unknown): boolean {
  return err instanceof FeishuApiError && err.apiCode === 1254014
}
