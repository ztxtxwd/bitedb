import type { FieldType, ResultType, SelectOption } from './types.js'

/** A field as persisted in the system catalog. */
export interface StoredField {
  /** Logical key used by developers. */
  key: string
  /** Physical Bitable field name (defaults to key). */
  name: string
  kind: FieldType
  /** Numeric Bitable field type. */
  feishuType: number
  fieldId?: string
  primary?: boolean
  /** Text field that stores JSON. */
  json?: boolean
  /** Spill threshold in bytes; 0/undefined disables spilling. */
  spill?: number
  expr?: string
  resultType?: ResultType
  target?: string
  multiple?: boolean
  duplex?: boolean
  backFieldName?: string
  options?: SelectOption[]
  property?: Record<string, unknown>
  uiType?: string
  description?: string
  existing?: boolean
  inferred?: boolean
}

export interface ShardMeta {
  index: number
  tableId: string
  /** Physical table name, used for diagnostics. */
  name: string
  rows: number
}

export interface TableMeta {
  database: string
  name: string
  fields: StoredField[]
  shards: ShardMeta[]
  createdAt: number
  updatedAt: number
  /** Catalog record id (not persisted inside the JSON payload). */
  recordId?: string
}

export interface DatabaseMeta {
  name: string
  appToken: string
  url?: string
  createdAt: number
  recordId?: string
}

export function fieldByKeyOrName(meta: TableMeta, keyOrName: string): StoredField | undefined {
  return meta.fields.find((field) => field.key === keyOrName || field.name === keyOrName)
}

export function totalRows(meta: TableMeta): number {
  return meta.shards.reduce((sum, shard) => sum + (shard.rows || 0), 0)
}
