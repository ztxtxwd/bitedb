import type { BiteDBLogger } from './types.js'

const noop = () => undefined

export class Logger {
  readonly debug: (...args: any[]) => void
  readonly info: (...args: any[]) => void
  readonly warn: (...args: any[]) => void
  readonly error: (...args: any[]) => void

  constructor(logger?: BiteDBLogger) {
    this.debug = logger?.debug?.bind(logger) ?? noop
    this.info = logger?.info?.bind(logger) ?? noop
    this.warn = logger?.warn?.bind(logger) ?? noop
    this.error = logger?.error?.bind(logger) ?? noop
  }
}
