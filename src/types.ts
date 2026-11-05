/**
 * Public types for bitedb.
 *
 * bitedb treats Feishu/Lark Bitable as a headless storage engine. Everything a
 * developer touches lives in this file: connection config, schemas, documents,
 * queries and blob handles.
 */

/** Semantic field types understood by bitedb. */
export type FieldType =
  | 'text'
  | 'number'
  | 'boolean'
  | 'date'
  | 'json'
  | 'blob'
  | 'attachment'
  | 'select'
  | 'multiSelect'
  | 'link'
  | 'computed'
  | 'raw'

/** `string` is accepted as an alias of `text` in schema declarations. */
export type SchemaFieldType = FieldType | 'string'

export type ResultType = 'text' | 'number' | 'boolean' | 'date' | 'json'

export interface SelectOption {
  name: string
  color?: number
  id?: string
}

/** Declarative description of one field. */
export interface FieldDefinition {
  type: SchemaFieldType
  /** Physical field name in Bitable. Defaults to the schema key. */
  name?: string
  /** Human readable description attached to the Bitable field. */
  description?: string
  /**
   * For `text` / `json` fields: externalize values larger than this many bytes
   * to Feishu Drive and keep only a small reference in the cell. `true` uses
   * the client's `blobThresholdBytes`. Defaults to true for inferred JSON.
   */
  spill?: boolean | number
  /** Formula expression for `computed` fields, e.g. `"price * count"`. */
  expr?: string
  /** Declared result type of a computed field. */
  resultType?: ResultType
  /** Target logical table name for `link` fields. */
  table?: string
  /** Whether a link can point to more than one record. Defaults to true. */
  multiple?: boolean
  /** Use a DuplexLink (server-maintained back-reference). Defaults to true. */
  duplex?: boolean
  /** Name of the reciprocal field created in the target table. */
  backFieldName?: string
  /** Options for select / multiSelect fields. */
  options?: Array<string | SelectOption>
  /**
   * Escape hatch: create the raw Bitable field with this numeric type and
   * property object. Useful for advanced Bitable field types.
   */
  feishuType?: number
  property?: Record<string, unknown>
  uiType?: string
}

export type SchemaDefinition = Record<string, SchemaFieldType | FieldDefinition>

/** A plain document. bitedb adds a stable string `_id`. */
export type Document = Record<string, any>

export type WithId<T extends Document = Document> = T & { _id: string }

export type UpdateSpec<T extends Document = Document> =
  | Partial<T>
  | {
      $set?: Partial<T>
      $unset?: Array<keyof T & string>
      $inc?: Partial<Record<keyof T & string, number>>
      $push?: Partial<Record<keyof T & string, unknown>>
    }

export type SortDirection = 'asc' | 'desc'

export type SortSpec =
  | string
  | [string, SortDirection]
  | Array<string | [string, SortDirection]>
  | Record<string, SortDirection>

/**
 * Mongo-ish filter. Top-level keys are ANDed. Supported operators:
 * `$eq $ne $gt $gte $lt $lte $in $nin $contains $notContains $exists`,
 * plus `$and` / `$or`.
 */
export type Filter = Record<string, unknown>

export interface QueryOptions {
  /**
   * Maximum number of rows to return. Defaults to **100**, and rows beyond the
   * limit are silently dropped. Pass an explicit limit (or use
   * `Query.page()` / `Table.findAfter()`) when the result set may be larger.
   */
  limit?: number
  /**
   * Rows to skip. Offsets are applied after fetching: every shard is searched
   * for up to `offset + limit` rows before the final slice, so deep offsets get
   * progressively more expensive. Prefer cursor pagination with
   * `Table.findAfter()` for large result sets.
   */
  offset?: number
  sort?: SortSpec
  /** Restrict returned fields. `_id` is always returned. */
  fields?: string[]
}

/** Options accepted by `updateOne` / `updateById` / `updateMany`. */
export interface UpdateOptions {
  /**
   * Materialise the document around the update. `'before'` returns the matched
   * row as it was before the write, `'after'` returns it after the write. Omit
   * to skip document materialisation (the default, and the fastest option).
   */
  returnDocument?: 'before' | 'after'
}

/** How attachment payloads are materialised when reading. */
export type HydrateMode = 'auto' | 'eager' | 'never'

export interface BiteDBLogger {
  debug?: (...args: any[]) => void
  info?: (...args: any[]) => void
  warn?: (...args: any[]) => void
  error?: (...args: any[]) => void
}

export interface BiteDBConfig {
  /** Feishu/Lark app id. Required unless `accessToken` is provided. */
  appId?: string
  /** Feishu/Lark app secret. Required unless `accessToken` is provided. */
  appSecret?: string
  /** Alias for `appId`. */
  clientId?: string
  /** Alias for `appSecret`. */
  clientSecret?: string
  /**
   * Pre-acquired tenant/user access token, or an async provider. When a
   * provider is supplied it is called for every request (the caller owns
   * caching/refresh).
   */
  accessToken?: string | (() => string | Promise<string>)
  /** Open platform domain. Defaults to https://open.feishu.cn */
  domain?: string
  /** Folder that owns created Bases. Defaults to the cloud-space root. */
  folderToken?: string
  /** Name of the system catalog Base. Defaults to `__bitedb_catalog__`. */
  catalogName?: string
  /** Pin the catalog Base token, skipping discovery. Also readable from BITEDB_CATALOG_BASE_TOKEN. */
  catalogBaseToken?: string
  /** Prefix for auto-created database Base names. Defaults to `bitedb:`. */
  databasePrefix?: string

  /** Coalescing window for writes, in ms. Defaults to 50. */
  batchWindowMs?: number
  /** Max records per batch_create call. Defaults to 500, capped at 1000. */
  maxBatchSize?: number
  /** Records per physical table before a new shard is created. Defaults to 20000. */
  maxRowsPerShard?: number
  /** Max number of physical tables (shards) per database Base. Defaults to 90. */
  maxTablesPerBase?: number

  /** Values above this byte size are externalized to Drive. Defaults to 32768. */
  blobThresholdBytes?: number
  /** Attachment materialisation strategy. Defaults to `auto`. */
  hydrate?: HydrateMode

  /** Per-request timeout in ms. Defaults to 30000. */
  requestTimeoutMs?: number
  /** Retries for retryable failures. Defaults to 3. */
  retries?: number
  /** Bitable request rate limit. Defaults to 18 requests/second. */
  bitableQps?: number
  /** Drive upload/download rate limit. Defaults to 4 requests/second. */
  driveQps?: number

  /** Optional logger. */
  logger?: BiteDBLogger
  /** How often dirty catalog row counts are flushed, in ms. Defaults to 1000. */
  catalogFlushMs?: number
  /** Custom fetch implementation (testing, proxies, edge runtimes). */
  fetch?: typeof fetch
}

export interface UpdateResult<T extends Document = Document> {
  matchedCount: number
  /** Rows whose fields actually changed. */
  modifiedCount: number
  /**
   * The updated document when `returnDocument` was requested on
   * `updateOne` / `updateById`.
   */
  document?: WithId<T>
  /**
   * One entry per matched row when `returnDocument` was requested on
   * `updateMany`, in the order the rows were matched.
   */
  documents?: Array<WithId<T>>
}

export interface DeleteResult {
  deletedCount: number
}

export interface TableInfo {
  name: string
  shards: number
  rows: number
  fields: number
}

export interface DatabaseInfo {
  name: string
  token: string
  url?: string
}

export interface TableStats {
  name: string
  shards: Array<{ index: number; tableId: string; rows: number; capacity: number }>
  totalRows: number
  fields: string[]
}

/** Attachment metadata returned by Bitable. */
export interface AttachmentMetadata {
  file_token: string
  name: string
  size: number
  type?: string
  url?: string
  tmp_url?: string
}
