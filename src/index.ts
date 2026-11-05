export { BiteDB, createClient } from './client.js'
export { Database } from './database.js'
export { Table } from './table.js'
export { Query } from './query-builder.js'
export { BlobRef } from './blob.js'
export {
  BiteDBError,
  ConfigError,
  ValidationError,
  NotFoundError,
  UnsupportedError,
  FeishuApiError,
} from './errors.js'
export type {
  AttachmentMetadata,
  BiteDBConfig,
  BiteDBLogger,
  DatabaseInfo,
  DeleteResult,
  Document,
  FieldDefinition,
  FieldType,
  Filter,
  HydrateMode,
  QueryOptions,
  ResultType,
  SchemaDefinition,
  SchemaFieldType,
  SelectOption,
  SortDirection,
  SortSpec,
  TableInfo,
  TableStats,
  UpdateOptions,
  UpdateResult,
  UpdateSpec,
  WithId,
} from './types.js'
export type { DatabaseMeta, ShardMeta, StoredField, TableMeta } from './meta.js'
