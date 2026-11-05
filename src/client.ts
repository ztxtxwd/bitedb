import { BlobManager } from './blob.js'
import { Catalog } from './catalog.js'
import { resolveConfig, type ResolvedConfig } from './config.js'
import { Database } from './database.js'
import { FeishuClient } from './feishu.js'
import { Logger } from './logger.js'
import type { BiteDBConfig, DatabaseInfo } from './types.js'

/**
 * bitedb entry point.
 *
 * ```ts
 * const db = createClient({ appId, appSecret })
 * const orders = db.useDatabase('production').collection('orders')
 * await orders.insertOne({ sku: 'A1', price: 100, count: 3 })
 * ```
 */
export class BiteDB {
  readonly config: ResolvedConfig
  readonly logger: Logger
  private readonly client: FeishuClient
  private readonly catalog: Catalog
  private readonly blobs: BlobManager
  private readonly databases = new Map<string, Database>()

  constructor(config: BiteDBConfig) {
    this.config = resolveConfig(config)
    this.logger = new Logger(config.logger)
    this.client = new FeishuClient({
      appId: this.config.appId,
      appSecret: this.config.appSecret,
      accessToken: this.config.accessToken,
      domain: this.config.domain,
      timeoutMs: this.config.requestTimeoutMs,
      retries: this.config.retries,
      bitableQps: this.config.bitableQps,
      driveQps: this.config.driveQps,
      fetch: this.config.fetch,
      logger: this.logger,
    })
    this.catalog = new Catalog({
      client: this.client,
      logger: this.logger,
      catalogName: this.config.catalogName,
      catalogBaseToken: this.config.catalogBaseToken,
      folderToken: this.config.folderToken,
      databasePrefix: this.config.databasePrefix,
      flushMs: this.config.catalogFlushMs,
    })
    this.blobs = new BlobManager(this.client, {
      hydrate: this.config.hydrate,
      logger: this.logger,
    })
  }

  /** Get a lazy database handle. The Base is created on first operation. */
  useDatabase(name: string): Database {
    if (!name || typeof name !== 'string') {
      throw new TypeError('useDatabase(name) requires a non-empty string')
    }
    let database = this.databases.get(name)
    if (!database) {
      database = new Database(
        {
          client: this.client,
          catalog: this.catalog,
          config: this.config,
          logger: this.logger,
          blobs: this.blobs,
        },
        name,
      )
      this.databases.set(name, database)
    }
    return database
  }

  /** Alias for useDatabase. */
  database(name: string): Database {
    return this.useDatabase(name)
  }

  /** Eagerly create (or adopt) a database and return its handle. */
  async createDatabase(name: string): Promise<Database> {
    const database = this.useDatabase(name)
    await database.ensure()
    return database
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    const metas = await this.catalog.listDatabases()
    return metas.map((meta) => ({ name: meta.name, token: meta.appToken, url: meta.url }))
  }

  /** Token of the system catalog Base, useful for pinning across processes. */
  async catalogBaseToken(): Promise<string> {
    await this.catalog.init()
    return this.catalog.catalogToken!
  }

  /** Flush pending batches and metadata counters. */
  async flush(): Promise<void> {
    for (const database of this.databases.values()) {
      for (const table of database.tableHandles()) {
        await table.flush()
      }
    }
    await this.catalog.flush()
  }

  async close(): Promise<void> {
    for (const database of this.databases.values()) {
      for (const table of database.tableHandles()) {
        await table.flush()
      }
    }
    await this.catalog.close()
  }
}

export function createClient(config: BiteDBConfig): BiteDB {
  return new BiteDB(config)
}
