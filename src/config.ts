import { ConfigError } from './errors.js'
import type { BiteDBConfig, HydrateMode } from './types.js'
import { clamp } from './util.js'

export interface ResolvedConfig {
  appId?: string
  appSecret?: string
  clientId?: string
  clientSecret?: string
  accessToken?: string | (() => string | Promise<string>)
  domain: string
  folderToken?: string
  catalogName: string
  catalogBaseToken?: string
  databasePrefix: string
  batchWindowMs: number
  maxBatchSize: number
  maxRowsPerShard: number
  maxTablesPerBase: number
  blobThresholdBytes: number
  hydrate: HydrateMode
  requestTimeoutMs: number
  retries: number
  bitableQps: number
  driveQps: number
  catalogFlushMs: number
  fetch?: typeof fetch
}

export function resolveConfig(config: BiteDBConfig): ResolvedConfig {
  if (!config || typeof config !== 'object') throw new ConfigError('bitedb config object is required')
  const appId = config.appId ?? config.clientId
  const appSecret = config.appSecret ?? config.clientSecret
  if (!config.accessToken && (!appId || !appSecret)) {
    throw new ConfigError(
      'Provide appId + appSecret (or clientId + clientSecret), or an accessToken/accessToken provider',
    )
  }

  const catalogName = config.catalogName ?? '__bitedb_catalog__'
  if (!catalogName.trim()) throw new ConfigError('catalogName cannot be empty')

  const hydrate = config.hydrate ?? 'auto'
  if (hydrate !== 'auto' && hydrate !== 'eager' && hydrate !== 'never') {
    throw new ConfigError('hydrate must be "auto", "eager" or "never"')
  }

  return {
    appId,
    appSecret,
    clientId: config.clientId,
    clientSecret: config.clientSecret,
    accessToken: config.accessToken,
    domain: config.domain ?? 'https://open.feishu.cn',
    folderToken: config.folderToken,
    catalogName,
    catalogBaseToken: config.catalogBaseToken,
    databasePrefix: config.databasePrefix ?? 'bitedb:',
    batchWindowMs: clamp(Math.floor(config.batchWindowMs ?? 50), 0, 5000),
    maxBatchSize: clamp(Math.floor(config.maxBatchSize ?? 500), 1, 1000),
    maxRowsPerShard: clamp(Math.floor(config.maxRowsPerShard ?? 20000), 1, 20000),
    maxTablesPerBase: clamp(Math.floor(config.maxTablesPerBase ?? 90), 2, 100),
    blobThresholdBytes: Math.max(0, Math.floor(config.blobThresholdBytes ?? 32 * 1024)),
    hydrate,
    requestTimeoutMs: Math.max(1000, Math.floor(config.requestTimeoutMs ?? 30000)),
    retries: clamp(Math.floor(config.retries ?? 3), 0, 10),
    bitableQps: Math.max(1, config.bitableQps ?? 18),
    driveQps: Math.max(1, config.driveQps ?? 4),
    catalogFlushMs: Math.max(0, Math.floor(config.catalogFlushMs ?? 1000)),
    fetch: config.fetch,
  }
}
