import { FeishuApiError, NotFoundError, ValidationError } from './errors.js'
import type { FeishuClient } from './feishu.js'
import type { Logger } from './logger.js'
import type { DatabaseMeta, TableMeta } from './meta.js'
import { Mutex, randomUUID, sleep } from './util.js'

export interface CatalogOptions {
  client: FeishuClient
  logger: Logger
  catalogName: string
  catalogBaseToken?: string
  folderToken?: string
  databasePrefix: string
  flushMs: number
}

interface SystemTables {
  databases: string
  tables: string
}

interface CatalogRecord<T> {
  recordId: string
  value: T
}

const DATABASE_FIELDS = [
  { field_name: 'name', type: 1 },
  { field_name: 'app_token', type: 1 },
  { field_name: 'url', type: 1 },
  { field_name: 'created_at', type: 2 },
]

const TABLE_FIELDS = [
  { field_name: 'key', type: 1 },
  { field_name: 'database', type: 1 },
  { field_name: 'name', type: 1 },
  { field_name: 'meta', type: 1 },
  { field_name: 'updated_at', type: 2 },
]

const catalogKey = (database: string, table: string) => JSON.stringify([database, table])

export class Catalog {
  private readonly client: FeishuClient
  private readonly logger: Logger
  private readonly options: CatalogOptions
  private readonly initMutex = new Mutex()
  private readonly writeMutex = new Mutex()

  private ready?: Promise<void>
  private baseToken?: string
  private systemTables?: SystemTables

  private readonly databaseIndex = new Map<string, DatabaseMeta>()
  private readonly tableCache = new Map<string, TableMeta>()
  private readonly pendingSaves = new Map<string, { meta: TableMeta; epoch: number }>()
  // Bumped whenever a table is deleted, so an in-flight coalesced flush that
  // snapshotted pre-delete metadata cannot resurrect the catalog record.
  private readonly tableEpochs = new Map<string, number>()
  private flushTimer?: ReturnType<typeof setTimeout>
  private flushPromise?: Promise<void>

  constructor(options: CatalogOptions) {
    this.options = options
    this.client = options.client
    this.logger = options.logger
  }

  async init(): Promise<void> {
    if (!this.ready) {
      this.ready = this.initMutex.runExclusive(() => this.doInit()).catch((err) => {
        this.ready = undefined
        throw err
      })
    }
    return this.ready
  }

  private async doInit(): Promise<void> {
    this.baseToken = await this.resolveCatalogBase()
    this.systemTables = await this.ensureSystemTables()
    await this.loadDatabaseIndex()
    this.logger.debug?.(`[bitedb] catalog ready at ${this.baseToken}`)
  }

  get catalogToken(): string | undefined {
    return this.baseToken
  }

  private async resolveCatalogBase(): Promise<string> {
    const explicit = this.options.catalogBaseToken ?? process.env.BITEDB_CATALOG_BASE_TOKEN
    if (explicit) return explicit

    const files = await this.client.listFiles(this.options.folderToken)
    const matches = files.filter((file) => {
      const type = file.type === 'shortcut' ? file.shortcut_info?.target_type : file.type
      return type === 'bitable' && file.name === this.options.catalogName
    })
    if (matches.length > 1) {
      this.logger.warn?.(
        `[bitedb] found ${matches.length} catalog Bases named "${this.options.catalogName}". Using the first; pin catalogBaseToken to avoid ambiguity.`,
      )
    }
    if (matches[0]) {
      const token =
        matches[0].type === 'shortcut'
          ? matches[0].shortcut_info?.target_token
          : matches[0].token
      if (!token) throw new NotFoundError('Catalog shortcut has no target token')
      return token
    }

    this.logger.info?.(`[bitedb] creating system catalog Base "${this.options.catalogName}"`)
    const app = await this.client.createBase(this.options.catalogName, this.options.folderToken)
    return app.app_token
  }

  private async ensureSystemTables(): Promise<SystemTables> {
    if (!this.baseToken) throw new NotFoundError('Catalog Base is not initialised')
    const existing = await this.client.listTables(this.baseToken)
    const find = (name: string) => existing.find((table) => table.name === name)

    let databases = find('databases')
    if (!databases) {
      const created = await this.client.createTable(this.baseToken, {
        name: 'databases',
        default_view_name: 'all',
        fields: DATABASE_FIELDS,
      })
      databases = { table_id: created.tableId, name: 'databases' }
    }
    let tables = find('tables')
    if (!tables) {
      const created = await this.client.createTable(this.baseToken, {
        name: 'tables',
        default_view_name: 'all',
        fields: TABLE_FIELDS,
      })
      tables = { table_id: created.tableId, name: 'tables' }
    }

    await this.ensureCatalogFields(databases.table_id, DATABASE_FIELDS)
    await this.ensureCatalogFields(tables.table_id, TABLE_FIELDS)

    return { databases: databases.table_id, tables: tables.table_id }
  }

  private async ensureCatalogFields(
    tableId: string,
    specs: Array<{ field_name: string; type: number }>,
  ): Promise<void> {
    if (!this.baseToken) return
    const fields = await this.client.listFields(this.baseToken, tableId)
    const names = new Set(fields.map((field) => field.field_name))
    for (const spec of specs) {
      if (!names.has(spec.field_name)) {
        await this.client.createField(this.baseToken, tableId, spec)
      }
    }
  }

  private async loadDatabaseIndex(): Promise<void> {
    if (!this.systemTables || !this.baseToken) return
    const { items } = await this.client.searchAllRecords(this.baseToken, this.systemTables.databases, {
      automatic_fields: false,
    })
    for (const item of items) {
      const name = textValue(item.fields.name)
      const token = textValue(item.fields.app_token)
      if (!name || !token) continue
      this.databaseIndex.set(name, {
        name,
        appToken: token,
        url: textValue(item.fields.url) || undefined,
        createdAt: Number(item.fields.created_at ?? 0) || 0,
        recordId: item.record_id,
      })
    }
  }

  async refreshDatabases(): Promise<DatabaseMeta[]> {
    await this.init()
    this.databaseIndex.clear()
    await this.loadDatabaseIndex()
    return [...this.databaseIndex.values()]
  }

  async listDatabases(): Promise<DatabaseMeta[]> {
    return this.refreshDatabases()
  }

  async getDatabase(name: string): Promise<DatabaseMeta | undefined> {
    await this.init()
    const cached = this.databaseIndex.get(name)
    if (cached) return cached
    const found = await this.findDatabaseInCatalog(name)
    if (found) {
      this.databaseIndex.set(name, found)
      return found
    }
    return undefined
  }

  private async findDatabaseInCatalog(name: string): Promise<DatabaseMeta | undefined> {
    if (!this.systemTables || !this.baseToken) return undefined
    const { items } = await this.client.searchAllRecords(this.baseToken, this.systemTables.databases, {
      filter: {
        conjunction: 'and',
        conditions: [{ field_name: 'name', operator: 'is', value: [name] }],
      },
      automatic_fields: false,
    })
    const item = items[0]
    if (!item) return undefined
    const token = textValue(item.fields.app_token)
    if (!token) return undefined
    return {
      name,
      appToken: token,
      url: textValue(item.fields.url) || undefined,
      createdAt: Number(item.fields.created_at ?? 0) || 0,
      recordId: item.record_id,
    }
  }

  async ensureDatabase(name: string): Promise<DatabaseMeta> {
    await this.init()
    const existing = await this.getDatabase(name)
    if (existing) return existing

    return this.writeMutex.runExclusive(async () => {
      const second = await this.getDatabase(name)
      if (second) return second

      const baseName = `${this.options.databasePrefix}${name}`
      if (baseName.length > 255) {
        throw new ValidationError('Database Base name exceeds 255 characters; shorten the name or databasePrefix')
      }
      const files = await this.client.listFiles(this.options.folderToken)
      const adopted = files.find((file) => {
        const type = file.type === 'shortcut' ? file.shortcut_info?.target_type : file.type
        return type === 'bitable' && file.name === baseName
      })
      let appToken: string | undefined
      let url: string | undefined
      if (adopted) {
        appToken =
          adopted.type === 'shortcut' ? adopted.shortcut_info?.target_token : adopted.token
        this.logger.info?.(`[bitedb] adopting existing Base "${baseName}"`)
      } else {
        this.logger.info?.(`[bitedb] creating database Base "${baseName}"`)
        const base = await this.client.createBase(baseName, this.options.folderToken)
        appToken = base.app_token
        url = base.url
      }
      if (!appToken) throw new NotFoundError(`Could not create or find database Base for "${name}"`)

      const meta: DatabaseMeta = {
        name,
        appToken,
        url,
        createdAt: Date.now(),
      }
      const created = await this.createDatabaseRecord(meta)
      this.databaseIndex.set(name, created)
      return created
    })
  }

  private async createDatabaseRecord(meta: DatabaseMeta): Promise<DatabaseMeta> {
    if (!this.systemTables || !this.baseToken) throw new NotFoundError('Catalog is not initialised')
    const record = await this.client.createRecord(
      this.baseToken,
      this.systemTables.databases,
      {
        name: meta.name,
        app_token: meta.appToken,
        url: meta.url ?? '',
        created_at: meta.createdAt,
      },
      randomUUID(),
    )
    return { ...meta, recordId: record.record_id }
  }

  async updateDatabase(meta: DatabaseMeta): Promise<void> {
    if (!this.systemTables || !this.baseToken) throw new NotFoundError('Catalog is not initialised')
    if (!meta.recordId) {
      const created = await this.createDatabaseRecord(meta)
      Object.assign(meta, created)
      this.databaseIndex.set(meta.name, meta)
      return
    }
    await this.client.updateRecord(this.baseToken, this.systemTables.databases, meta.recordId, {
      app_token: meta.appToken,
      url: meta.url ?? '',
    })
    this.databaseIndex.set(meta.name, meta)
  }

  async listTables(database: string): Promise<TableMeta[]> {
    await this.init()
    if (!this.systemTables || !this.baseToken) return []
    const { items } = await this.client.searchAllRecords(this.baseToken, this.systemTables.tables, {
      filter: {
        conjunction: 'and',
        conditions: [{ field_name: 'database', operator: 'is', value: [database] }],
      },
      automatic_fields: false,
    })
    const metas: TableMeta[] = []
    for (const item of items) {
      const meta = this.parseTableRecord(item.record_id, item.fields)
      if (meta) {
        metas.push(meta)
        this.tableCache.set(`${meta.database}\u0001${meta.name}`, meta)
      }
    }
    return metas
  }

  async getTable(database: string, table: string): Promise<TableMeta | undefined> {
    await this.init()
    const key = `${database}\u0001${table}`
    const cached = this.tableCache.get(key)
    if (cached) return cached

    const found = await this.findTableInCatalog(database, table)
    if (found) this.tableCache.set(key, found)
    return found
  }

  async refreshTable(database: string, table: string): Promise<TableMeta | undefined> {
    const key = `${database}\u0001${table}`
    this.tableCache.delete(key)
    return this.getTable(database, table)
  }

  private async findTableInCatalog(database: string, table: string): Promise<TableMeta | undefined> {
    if (!this.systemTables || !this.baseToken) return undefined
    const key = catalogKey(database, table)
    const { items } = await this.client.searchAllRecords(this.baseToken, this.systemTables.tables, {
      filter: {
        conjunction: 'and',
        conditions: [{ field_name: 'key', operator: 'is', value: [key] }],
      },
      automatic_fields: false,
    })
    const item = items[0]
    if (!item) return undefined
    return this.parseTableRecord(item.record_id, item.fields)
  }

  private parseTableRecord(recordId: string, fields: Record<string, any>): TableMeta | undefined {
    const serialized = textValue(fields.meta)
    if (!serialized) return undefined
    try {
      const meta = JSON.parse(serialized) as TableMeta
      meta.recordId = recordId
      meta.fields ??= []
      meta.shards ??= []
      return meta
    } catch (err) {
      this.logger.error?.('[bitedb] failed to parse catalog table metadata:', err)
      return undefined
    }
  }

  /** Insert or update a logical table row. Awaits the catalog write. */
  async saveTable(meta: TableMeta): Promise<TableMeta> {
    await this.init()
    const key = `${meta.database}\u0001${meta.name}`
    const snapshot = cloneMeta(meta)
    return this.writeMutex.runExclusive(async () => {
      const saved = await this.persistTable(snapshot)
      this.tableCache.set(key, saved)
      return saved
    })
  }

  private epochOf(key: string): number {
    return this.tableEpochs.get(key) ?? 0
  }

  /** Coalesced row-count persistence. */
  scheduleTableSave(meta: TableMeta): void {
    const key = `${meta.database}\u0001${meta.name}`
    // Keep the live object so recordId assignments are visible at flush time.
    this.pendingSaves.set(key, { meta, epoch: this.epochOf(key) })
    if (!this.flushTimer) {
      this.flushTimer = setTimeout(() => {
        this.flushTimer = undefined
        void this.flush()
      }, this.options.flushMs)
      this.flushTimer.unref?.()
    }
  }

  async flush(): Promise<void> {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer)
      this.flushTimer = undefined
    }
    if (this.flushPromise) {
      await this.flushPromise
      if (this.pendingSaves.size === 0) return
    }
    if (this.pendingSaves.size === 0) return

    const snapshots = [...this.pendingSaves.entries()].map(([key, entry]) => ({
      key,
      epoch: entry.epoch,
      meta: cloneMeta(entry.meta),
    }))
    this.pendingSaves.clear()
    this.flushPromise = this.writeMutex
      .runExclusive(async () => {
        for (const { key, epoch, meta } of snapshots) {
          // The table was deleted after this snapshot was taken; persisting it
          // would recreate the catalog row for a physical table that is gone.
          if (this.epochOf(key) !== epoch) continue
          try {
            const saved = await this.persistTable(meta)
            this.tableCache.set(key, saved)
          } catch (err) {
            this.logger.warn?.('[bitedb] failed to persist table metadata:', err)
          }
        }
      })
      .finally(() => {
        this.flushPromise = undefined
      })
    await this.flushPromise
  }

  private async persistTable(meta: TableMeta): Promise<TableMeta> {
    if (!this.systemTables || !this.baseToken) throw new NotFoundError('Catalog is not initialised')
    meta.updatedAt = Date.now()
    const payload = {
      key: catalogKey(meta.database, meta.name),
      database: meta.database,
      name: meta.name,
      meta: JSON.stringify(stripRuntimeMeta(meta)),
      updated_at: meta.updatedAt,
    }
    if (meta.recordId) {
      try {
        await this.client.updateRecord(this.baseToken, this.systemTables.tables, meta.recordId, payload)
        return meta
      } catch (err) {
        if (!isMissingRecord(err)) throw err
        meta.recordId = undefined
      }
    }
    const record = await this.client.createRecord(
      this.baseToken,
      this.systemTables.tables,
      payload,
      randomUUID(),
    )
    meta.recordId = record.record_id
    return meta
  }

  async deleteTable(database: string, table: string): Promise<void> {
    await this.init()
    const key = `${database}\u0001${table}`
    const cached = this.tableCache.get(key) ?? (await this.findTableInCatalog(database, table))
    this.pendingSaves.delete(key)
    await this.writeMutex.runExclusive(async () => {
      // Serialize with metadata flushes and invalidate any snapshot that was
      // taken before this point.
      this.tableEpochs.set(key, this.epochOf(key) + 1)
      this.tableCache.delete(key)
      if (!cached?.recordId || !this.systemTables || !this.baseToken) return
      await this.client.deleteRecord(this.baseToken, this.systemTables.tables, cached.recordId)
    })
  }

  invalidateDatabase(name: string): void {
    this.databaseIndex.delete(name)
  }

  async close(): Promise<void> {
    await this.flush()
    // Let an in-flight flush settle before returning.
    while (this.flushPromise) {
      await this.flushPromise
      await sleep(0)
      if (!this.flushPromise) break
    }
  }
}

function cloneMeta(meta: TableMeta): TableMeta {
  return {
    ...meta,
    fields: meta.fields.map((field) => ({ ...field })),
    shards: meta.shards.map((shard) => ({ ...shard })),
  }
}

function stripRuntimeMeta(meta: TableMeta): Omit<TableMeta, 'recordId'> {
  const { recordId: _recordId, ...rest } = meta
  return rest
}

function textValue(value: unknown): string {
  if (typeof value === 'string') return value
  if (Array.isArray(value)) {
    return value.map((item) => (typeof item === 'string' ? item : item?.text ?? '')).join('')
  }
  if (value === undefined || value === null) return ''
  return String(value)
}

function isMissingRecord(err: unknown): boolean {
  return err instanceof FeishuApiError && (err.apiCode === 1254002 || err.apiCode === 1254040 || err.status === 404)
}
