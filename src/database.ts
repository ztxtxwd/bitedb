import type { Catalog } from './catalog.js'
import type { ResolvedConfig } from './config.js'
import type { FeishuClient } from './feishu.js'
import type { Logger } from './logger.js'
import type { DatabaseMeta, TableMeta } from './meta.js'
import type { BlobManager } from './blob.js'
import { Table } from './table.js'
import type { DatabaseInfo, Document, SchemaDefinition, TableInfo } from './types.js'
import { totalRows } from './meta.js'

export interface DatabaseContext {
  client: FeishuClient
  catalog: Catalog
  config: ResolvedConfig
  logger: Logger
  blobs: BlobManager
}

export class Database {
  readonly name: string
  private readonly ctx: DatabaseContext
  private readonly tables = new Map<string, Table<any>>()
  private ensurePromise?: Promise<DatabaseMeta>

  constructor(context: DatabaseContext, name: string) {
    if (!name || typeof name !== 'string') throw new Error('Database name must be a non-empty string')
    if (name.length > 200) throw new Error('Database name must be 200 characters or fewer')
    this.ctx = context
    this.name = name
  }

  get client(): FeishuClient {
    return this.ctx.client
  }

  get catalog(): Catalog {
    return this.ctx.catalog
  }

  get config(): ResolvedConfig {
    return this.ctx.config
  }

  get blobs(): BlobManager {
    return this.ctx.blobs
  }

  get logger(): Logger {
    return this.ctx.logger
  }

  async ensure(): Promise<DatabaseMeta> {
    if (!this.ensurePromise) {
      this.ensurePromise = this.ctx.catalog.ensureDatabase(this.name).catch((err) => {
        this.ensurePromise = undefined
        throw err
      })
    }
    return this.ensurePromise
  }

  /**
   * Eagerly ensure this Base and every table handle registered on it. Tables
   * are provisioned concurrently, which shortens cold starts where several
   * independent tables would otherwise be created one after another.
   */
  async prewarm(): Promise<void> {
    await this.ensure()
    await Promise.all(this.tableHandles().map((table) => table.ensure()))
  }

  async getToken(): Promise<string> {
    return (await this.ensure()).appToken
  }

  async info(): Promise<DatabaseInfo> {
    const meta = await this.ensure()
    return { name: this.name, token: meta.appToken, url: meta.url }
  }

  defineTable<T extends Document = Document>(name: string, schema: SchemaDefinition = {}): Table<T> {
    const table = this.table<T>(name)
    table.defineSchema(schema)
    return table
  }

  table<T extends Document = Document>(name: string, schema?: SchemaDefinition): Table<T> {
    if (!name || typeof name !== 'string') throw new Error('Table name must be a non-empty string')
    let table = this.tables.get(name) as Table<T> | undefined
    if (!table) {
      table = new Table<T>(this, name, schema)
      this.tables.set(name, table as Table<any>)
    } else if (schema) {
      table.defineSchema(schema)
    }
    return table
  }

  collection<T extends Document = Document>(name: string, schema?: SchemaDefinition): Table<T> {
    return this.table<T>(name, schema)
  }

  /** Synchronous access for an already constructed handle; used by link fields. */
  tableHandle(name: string): Table {
    return this.table(name)
  }

  async listTables(): Promise<TableInfo[]> {
    const metas = await this.ctx.catalog.listTables(this.name)
    return metas.map((meta) => ({
      name: meta.name,
      shards: meta.shards.length,
      rows: totalRows(meta),
      fields: meta.fields.length,
    }))
  }

  async tableMetadata(name: string): Promise<TableMeta | undefined> {
    return this.ctx.catalog.getTable(this.name, name)
  }

  async dropTable(name: string): Promise<void> {
    const table = this.tables.get(name)
    if (table) {
      await table.drop()
      this.tables.delete(name)
    } else {
      const meta = await this.ctx.catalog.getTable(this.name, name)
      if (!meta) return
      await this.ctx.client.deleteTables((await this.ensure()).appToken, meta.shards.map((s) => s.tableId))
      await this.ctx.catalog.deleteTable(this.name, name)
    }
  }

  async refresh(): Promise<void> {
    this.ensurePromise = undefined
    this.ctx.catalog.invalidateDatabase(this.name)
    for (const table of this.tables.values()) {
      table.invalidate()
    }
  }

  /** @internal */
  get context(): DatabaseContext {
    return this.ctx
  }

  /** @internal */
  tableHandles(): Table<any>[] {
    return [...this.tables.values()]
  }
}
