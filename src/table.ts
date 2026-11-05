import { Batcher } from './batching.js'
import {
  decodeFieldValue,
  decodeRecord,
  encodeDocument,
  type CodecContext,
  type LinkResolver,
} from './codec.js'
import type { Database } from './database.js'
import { BiteDBError, NotFoundError, UnsupportedError, ValidationError } from './errors.js'
import type { FeishuFieldInfo, FeishuRecordInfo } from './feishu.js'
import type { StoredField, TableMeta } from './meta.js'
import { totalRows } from './meta.js'
import {
  compileFilter,
  compareDocuments,
  mapFieldNames,
  mergeSorted,
  normalizeQueryOptions,
  parseSort,
} from './query.js'
import { Query } from './query-builder.js'
import {
  compileFormula,
  feishuFieldPayload,
  formulaUsesFieldIds,
  inferField,
  normalizeSchema,
} from './schema.js'
import type {
  DeleteResult,
  Document,
  Filter,
  QueryOptions,
  SchemaDefinition,
  TableStats,
  UpdateOptions,
  UpdateResult,
  UpdateSpec,
  WithId,
} from './types.js'
import { Mutex, Semaphore, generateId, isSafePropertyKey, randomUUID, setOwn, truncate } from './util.js'

interface RecordLocation {
  shardIndex: number
  recordId: string
  logicalId?: string
}

interface LocatedRecord extends RecordLocation {
  fields: Record<string, any>
  shardTableId: string
}

interface InsertItem {
  id: string
  document: Document
  fields: Record<string, any>
  shardIndex: number
}

type InsertResult = WithId<Document>

const NOT_FOUND_CODES = new Set([1254002, 1254040, 1254041])

export class Table<T extends Document = Document> {
  readonly database: Database
  readonly name: string

  private declaredSchema: SchemaDefinition
  private meta?: TableMeta
  private token?: string

  private ensurePromise?: Promise<TableMeta>
  private baseEnsurePromise?: Promise<TableMeta>
  private refreshRequested = false

  private readonly baseMutex = new Mutex()
  private readonly ddlMutex = new Mutex()
  private readonly shardMutex = new Mutex()
  private readonly writeSemaphore = new Semaphore(4)

  private readonly idCache = new Map<string, RecordLocation>()
  private readonly recordCache = new Map<string, RecordLocation>()
  private readonly maxCacheSize = 10000

  private insertBatcher: Batcher<InsertItem, InsertResult>
  private readonly linkResolver: LinkResolver
  private closed = false

  constructor(database: Database, name: string, schema: SchemaDefinition = {}) {
    if (!name || typeof name !== 'string') throw new Error('Table name must be a non-empty string')
    if (name.length > 90) throw new Error('Table name must be 90 characters or fewer')
    if (/#\d+$/.test(name)) {
      throw new Error('Table names must not end with "#<number>"; this suffix is reserved for shards')
    }
    this.database = database
    this.name = name
    this.declaredSchema = schema
    this.insertBatcher = this.createBatcher()
    this.linkResolver = {
      resolveOutgoing: (field, ids) => this.resolveLinkToPhysical(field, ids),
      resolveIncoming: (field, ids) => this.resolveLinkFromPhysical(field, ids),
    }
  }

  private createBatcher(): Batcher<InsertItem, InsertResult> {
    return new Batcher<InsertItem, InsertResult>({
      windowMs: this.database.config.batchWindowMs,
      maxSize: this.database.config.maxBatchSize,
      flush: (key, items) => this.flushInserts(Number(key), items),
    })
  }

  // ---------------------------------------------------------------------------
  // Schema lifecycle
  // ---------------------------------------------------------------------------

  defineSchema(schema: SchemaDefinition): this {
    normalizeSchema(schema, { blobThresholdBytes: this.database.config.blobThresholdBytes })
    this.declaredSchema = schema
    this.ensurePromise = undefined
    return this
  }

  async ensure(): Promise<TableMeta> {
    if (!this.ensurePromise) {
      this.ensurePromise = this.runEnsure().catch((err) => {
        this.ensurePromise = undefined
        throw err
      })
    }
    return this.ensurePromise
  }

  /** Ensure only the base physical table exists (used during link creation). */
  async ensureBase(): Promise<TableMeta> {
    if (!this.baseEnsurePromise) {
      this.baseEnsurePromise = this.runEnsureBase().catch((err) => {
        this.baseEnsurePromise = undefined
        throw err
      })
    }
    return this.baseEnsurePromise
  }

  private async runEnsure(): Promise<TableMeta> {
    const meta = await this.ensureBase()
    await this.ddlMutex.runExclusive(async () => {
      await this.syncDeclaredAndPhysical(meta)
      await this.ensureAuxFields(meta)
    })
    return meta
  }

  private async runEnsureBase(): Promise<TableMeta> {
    await this.database.ensure()
    this.token = await this.database.getToken()

    if (this.refreshRequested) {
      this.refreshRequested = false
      const refreshed = await this.database.catalog.refreshTable(this.database.name, this.name)
      if (refreshed) return this.adoptMeta(refreshed)
    }

    const existing = await this.database.catalog.getTable(this.database.name, this.name)
    if (existing) return this.adoptMeta(existing)

    return this.baseMutex.runExclusive(async () => {
      const second = await this.database.catalog.getTable(this.database.name, this.name)
      if (second) return this.adoptMeta(second)

      const physicalTables = await this.database.client.listTables(this.token!)
      const adopted = physicalTables
        .map((table) => ({ table, index: shardIndexFromName(table.name, this.name) }))
        .filter((entry): entry is { table: (typeof physicalTables)[number]; index: number } => entry.index >= 0)
        .sort((a, b) => a.index - b.index)

      let meta: TableMeta
      if (adopted.length > 0) {
        const tableNames = new Map(
          physicalTables.map((table) => [table.table_id, logicalTableNameFromPhysical(table.name)]),
        )
        meta = await this.buildAdoptedMeta(
          adopted.map((entry) => ({ tableId: entry.table.table_id, name: entry.table.name, index: entry.index })),
          (tableId) => tableNames.get(tableId),
        )
        this.database.logger.info?.(
          `[bitedb] adopted existing physical table "${this.name}" with ${meta.shards.length} shard(s)`,
        )
      } else {
        const count = await this.physicalTableCount()
        if (count >= this.database.config.maxTablesPerBase) {
          throw new BiteDBError(
            `Database "${this.database.name}" reached maxTablesPerBase=${this.database.config.maxTablesPerBase}. ` +
              'Bitable allows at most 100 tables per Base.',
          )
        }
        const fields = normalizeSchema(this.declaredSchema, {
          blobThresholdBytes: this.database.config.blobThresholdBytes,
        })
        meta = {
          database: this.database.name,
          name: this.name,
          fields,
          shards: [],
          createdAt: Date.now(),
          updatedAt: Date.now(),
        }
        const shard = await this.createPhysicalTable(meta, 0)
        meta.shards.push(shard)
        this.database.logger.info?.(`[bitedb] created table "${this.name}" (${shard.tableId})`)
      }
      this.meta = meta
      const saved = await this.database.catalog.saveTable(meta)
      meta.recordId = saved.recordId
      return meta
    })
  }

  private adoptMeta(meta: TableMeta): TableMeta {
    this.meta = meta
    if (!meta.fields.some((field) => field.key === '_id')) {
      meta.fields.unshift({
        key: '_id',
        name: '_id',
        kind: 'text',
        feishuType: 1,
        primary: true,
        description: 'bitedb document id',
      })
    }
    return meta
  }

  private requireMeta(): TableMeta {
    if (!this.meta) throw new BiteDBError('Table is not initialised')
    return this.meta
  }

  private codecContext(hydrate?: 'auto' | 'eager' | 'never'): CodecContext {
    return {
      blobs: this.database.blobs,
      baseToken: this.token!,
      hydrate: hydrate ?? this.database.config.hydrate,
      links: this.linkResolver,
      logger: this.database.logger,
    }
  }

  // ---------------------------------------------------------------------------
  // DDL
  // ---------------------------------------------------------------------------

  private async syncDeclaredAndPhysical(meta: TableMeta): Promise<void> {
    let changed = false

    // Reconcile physical tables that are missing from the catalog. This
    // recovers shards created by a concurrent process or a lost catalog write.
    const physicalTables = await this.database.client.listTables(this.token!)
    const tableNameById = new Map(
      physicalTables.map((table) => [table.table_id, logicalTableNameFromPhysical(table.name)]),
    )
    const resolveTableName = (tableId: string) => tableNameById.get(tableId)
    const knownShardNames = new Set(meta.shards.map((shard) => shard.name))
    for (const table of physicalTables) {
      const index = shardIndexFromName(table.name, this.name)
      if (index < 0 || knownShardNames.has(table.name)) continue
      if (meta.shards.some((shard) => shard.index === index)) continue
      meta.shards.push({
        index,
        tableId: table.table_id,
        name: table.name,
        rows: await this.countShardRows(table.table_id),
      })
      knownShardNames.add(table.name)
      changed = true
    }
    meta.shards.sort((a, b) => a.index - b.index)

    const declared = normalizeSchema(this.declaredSchema, {
      blobThresholdBytes: this.database.config.blobThresholdBytes,
    })

    // Merge declarations into the catalog metadata.
    for (const field of declared) {
      if (field.key === '_id') continue
      const existing = meta.fields.find((item) => item.key === field.key || item.name === field.name)
      if (!existing) {
        meta.fields.push(field)
        changed = true
        continue
      }
      if (existing.kind !== field.kind) {
        if (
          (existing.kind === 'raw' || existing.inferred) &&
          existing.feishuType === field.feishuType
        ) {
          Object.assign(existing, { ...field, fieldId: existing.fieldId, existing: existing.existing })
          changed = true
        } else {
          this.database.logger.warn?.(
            `[bitedb] field "${field.key}" already exists as ${existing.kind}; keeping it instead of ${field.kind}`,
          )
        }
      } else {
        const fieldId = existing.fieldId
        const wasExisting = existing.existing
        Object.assign(existing, {
          ...field,
          fieldId,
          existing: wasExisting,
          inferred: existing.inferred,
        })
      }
    }

    if (!meta.fields.some((field) => field.key === '_id')) {
      meta.fields.unshift({
        key: '_id',
        name: '_id',
        kind: 'text',
        feishuType: 1,
        primary: true,
        description: 'bitedb document id',
      })
      changed = true
    }

    for (const shard of meta.shards) {
      const live = await this.liveFields(shard.tableId)
      const byName = new Map(live.map((field) => [field.field_name, field]))

      for (const field of meta.fields) {
        if (field.kind === 'link' || field.kind === 'computed') continue
        const physical = byName.get(field.name)
        if (!physical) {
          const created = await this.createFieldOnShard(meta, shard, field)
          byName.set(created.field_name, created)
          live.push(created)
          changed = true
          if (shard.index === 0 && !field.fieldId) field.fieldId = created.field_id
        } else if (shard.index === 0 && !field.fieldId) {
          field.fieldId = physical.field_id
          changed = true
        }
      }

      if (shard.index === 0) {
        const knownNames = new Set(meta.fields.map((field) => field.name))
        for (const physical of live) {
          if (knownNames.has(physical.field_name)) continue
          const adopted = adoptPhysicalField(physical, meta.fields, resolveTableName)
          meta.fields.push(adopted)
          knownNames.add(physical.field_name)
          changed = true
        }
      }
    }

    if (changed) {
      const saved = await this.database.catalog.saveTable(meta)
      meta.recordId = saved.recordId
    }
  }

  private async ensureAuxFields(meta: TableMeta): Promise<void> {
    const linkFields = meta.fields.filter((field) => field.kind === 'link')
    const computedFields = meta.fields.filter((field) => field.kind === 'computed')

    if (linkFields.length > 0 || computedFields.length > 0) {
      let changed = false
      for (const shard of meta.shards) {
        const live = await this.liveFields(shard.tableId)
        const byName = new Map(live.map((field) => [field.field_name, field]))

        // Links first: computed formulas can reference them.
        for (const field of linkFields) {
          const existing = byName.get(field.name)
          if (existing) {
            if (shard.index === 0) {
              if (!field.fieldId) {
                field.fieldId = existing.field_id
                changed = true
              }
              await this.registerBackField(meta, shard, field, existing)
            }
            continue
          }
          const created = await this.createLinkField(meta, shard, field)
          byName.set(created.field_name, created)
          live.push(created)
          changed = true
          if (shard.index === 0 && !field.fieldId) {
            field.fieldId = created.field_id
            await this.registerBackField(meta, shard, field, created)
          }
        }

        // Computed fields may reference each other; retry until no progress.
        const pending = [...computedFields]
        while (pending.length > 0) {
          let progress = false
          for (let index = pending.length - 1; index >= 0; index--) {
            const field = pending[index]!
            if (byName.has(field.name)) {
              const physical = byName.get(field.name)!
              if (shard.index === 0 && !field.fieldId) {
                field.fieldId = physical.field_id
                changed = true
              }
              // A previous attempt may have created the field but failed to
              // apply the formula (two-step creation). Repair empty formulas.
              if (!physical.property?.formula_expression) {
                await this.applyComputedFormula(meta, shard, field, physical, byName)
              }
              pending.splice(index, 1)
              continue
            }
            let created: FeishuFieldInfo
            try {
              created = await this.createComputedField(meta, shard, field, byName)
            } catch (err) {
              if (isMissingDependency(err)) continue
              throw err
            }
            byName.set(created.field_name, created)
            live.push(created)
            pending.splice(index, 1)
            progress = true
            changed = true
            if (shard.index === 0 && !field.fieldId) field.fieldId = created.field_id
          }
          if (!progress && pending.length > 0) {
            throw new UnsupportedError(
              `Could not resolve computed field dependencies in "${this.name}": ${pending
                .map((field) => field.key)
                .join(', ')}`,
            )
          }
        }
      }

      if (changed) {
        const saved = await this.database.catalog.saveTable(meta)
        meta.recordId = saved.recordId
      }
    }
  }

  private async createFieldOnShard(
    meta: TableMeta,
    shard: TableMeta['shards'][number],
    field: StoredField,
  ): Promise<FeishuFieldInfo> {
    const payload = feishuFieldPayload(field)
    try {
      return await this.database.client.createField(this.token!, shard.tableId, payload)
    } catch (err) {
      if (isFieldNameConflict(err)) {
        const live = await this.liveFields(shard.tableId)
        const existing = live.find((item) => item.field_name === field.name)
        if (existing) return existing
      }
      throw err
    }
  }

  private async createLinkField(
    meta: TableMeta,
    shard: TableMeta['shards'][number],
    field: StoredField,
  ): Promise<FeishuFieldInfo> {
    if (!field.target) throw new ValidationError(`Link field "${field.key}" has no target table`)
    const targetTable = this.database.table(field.target)
    const targetMeta = await targetTable.ensureBase()
    const targetShard = targetMeta.shards[0]
    if (!targetShard) throw new BiteDBError(`Target table "${field.target}" has no physical shard`)

    if (!field.backFieldName) {
      field.backFieldName = truncate(
        `${this.name}_${field.key}_${targetShard.tableId.slice(-4)}_back`,
        100,
      )
    }

    const physicalField: StoredField =
      shard.index === 0
        ? { ...field, feishuType: field.duplex === false ? 18 : 21 }
        : { ...field, feishuType: 18, duplex: false }
    const payload = feishuFieldPayload(physicalField, { targetTableId: targetShard.tableId })
    try {
      return await this.database.client.createField(this.token!, shard.tableId, payload)
    } catch (err) {
      if (isFieldNameConflict(err)) {
        const live = await this.liveFields(shard.tableId)
        const existing = live.find((item) => item.field_name === field.name)
        if (existing) return existing
      }
      throw err
    }
  }

  private async applyComputedFormula(
    meta: TableMeta,
    shard: TableMeta['shards'][number],
    field: StoredField,
    physical: FeishuFieldInfo,
    liveByName: Map<string, FeishuFieldInfo>,
  ): Promise<void> {
    if (!field.expr) return
    const liveMetaFields = meta.fields.map((item) => ({
      ...item,
      fieldId: liveByName.get(item.name)?.field_id ?? (shard.index === 0 ? item.fieldId : undefined),
    }))
    const expression = formulaUsesFieldIds(field.expr)
      ? field.expr
      : compileFormula(field.expr, shard.tableId, liveMetaFields)
    await this.database.client.updateField(
      this.token!,
      shard.tableId,
      physical.field_id,
      feishuFieldPayload(field, { formulaExpression: expression }),
    )
  }

  private async createComputedField(
    meta: TableMeta,
    shard: TableMeta['shards'][number],
    field: StoredField,
    liveByName: Map<string, FeishuFieldInfo>,
  ): Promise<FeishuFieldInfo> {
    if (!field.expr) throw new ValidationError(`Computed field "${field.key}" has no expression`)
    const liveMetaFields = meta.fields.map((item) => ({
      ...item,
      fieldId: liveByName.get(item.name)?.field_id ?? (shard.index === 0 ? item.fieldId : undefined),
    }))
    const expression = formulaUsesFieldIds(field.expr)
      ? // User supplied machine syntax verbatim.
        field.expr
      : compileFormula(field.expr, shard.tableId, liveMetaFields)
    const payload = feishuFieldPayload(field, { formulaExpression: expression })

    let created: FeishuFieldInfo
    try {
      created = await this.database.client.createField(this.token!, shard.tableId, payload)
    } catch (err) {
      // Some Bitable deployments reject formula_expression on create. Create
      // an empty formula field first, then apply the expression with PUT.
      if (!isFormulaCreateRejected(err)) throw err
      const withoutExpression = {
        ...payload,
        property: payload.property
          ? Object.fromEntries(
              Object.entries(payload.property).filter(([key]) => key !== 'formula_expression'),
            )
          : undefined,
      }
      created = await this.database.client.createField(this.token!, shard.tableId, withoutExpression)
    }

    // The documented two-step path: full-field update applies the formula.
    return this.database.client.updateField(this.token!, shard.tableId, created.field_id, payload)
  }

  private async registerBackField(
    meta: TableMeta,
    shard: TableMeta['shards'][number],
    field: StoredField,
    created: FeishuFieldInfo,
  ): Promise<void> {
    if (field.duplex === false || !field.target) return
    const targetTable = this.database.table(field.target)
    const targetMeta = await targetTable.ensureBase()
    if (!targetMeta.shards[0]) return
    if (!field.backFieldName) {
      field.backFieldName = truncate(
        `${this.name}_${field.key}_${targetMeta.shards[0].tableId.slice(-4)}_back`,
        100,
      )
    }
    const live = await targetTable.liveFields(targetMeta.shards[0].tableId)
    const back =
      live.find((item) => item.field_name === field.backFieldName) ??
      live.find((item) => {
        const tableId = extractLinkTableId(item)
        return tableId === shard.tableId
      })
    if (!back) return
    await targetTable.adoptBackField(back, this.name)
  }

  /** @internal Called by the source table when Feishu creates a duplex back field. */
  async adoptBackField(physical: FeishuFieldInfo, sourceTable: string): Promise<void> {
    const meta = await this.ensureBase()
    const existing = meta.fields.find((field) => field.name === physical.field_name)
    if (existing) {
      if (
        existing.kind !== 'link' &&
        (existing.kind === 'raw' || existing.inferred) &&
        (existing.feishuType === 18 || existing.feishuType === 21)
      ) {
        existing.kind = 'link'
        existing.target = sourceTable
        existing.multiple = true
        existing.duplex = true
        existing.fieldId ??= physical.field_id
        existing.existing = true
        const saved = await this.database.catalog.saveTable(meta)
        meta.recordId = saved.recordId
      }
      return
    }
    let key = physical.field_name
    let suffix = 1
    while (meta.fields.some((field) => field.key === key)) {
      key = `${physical.field_name}_${suffix++}`
    }
    meta.fields.push({
      key,
      name: physical.field_name,
      kind: 'link',
      feishuType: physical.type,
      fieldId: physical.field_id,
      target: sourceTable,
      multiple: true,
      duplex: true,
      existing: true,
      inferred: true,
    })
    const saved = await this.database.catalog.saveTable(meta)
    meta.recordId = saved.recordId
  }

  private async addInferredFields(meta: TableMeta, fields: StoredField[]): Promise<void> {
    let changed = false
    const liveByShard = new Map<number, Map<string, FeishuFieldInfo>>()
    for (const shard of meta.shards) {
      const live = await this.liveFields(shard.tableId)
      liveByShard.set(shard.index, new Map(live.map((field) => [field.field_name, field])))
    }
    try {
      for (const field of fields) {
        if (meta.fields.some((item) => item.key === field.key || item.name === field.name)) continue
        if (meta.fields.some((item) => item.name === field.name)) {
          throw new ValidationError(`Field name collision while inferring "${field.key}"`)
        }
        meta.fields.push(field)
        changed = true
        for (const shard of meta.shards) {
          const live = liveByShard.get(shard.index)!
          const existing = live.get(field.name)
          if (existing) {
            if (shard.index === 0 && !field.fieldId) field.fieldId = existing.field_id
            continue
          }
          const created = await this.createFieldOnShard(meta, shard, field)
          live.set(created.field_name, created)
          if (shard.index === 0) field.fieldId = created.field_id
        }
      }
    } catch (err) {
      // Force the next operation through full schema reconciliation so a
      // partially-created field is completed on every shard.
      this.ensurePromise = undefined
      throw err
    }
    if (changed) {
      const saved = await this.database.catalog.saveTable(meta)
      meta.recordId = saved.recordId
    }
  }

  private async ensureInferredFields(meta: TableMeta, doc: Document): Promise<void> {
    const missing: StoredField[] = []
    for (const [key, value] of Object.entries(doc)) {
      if (key === '_id') continue
      if (meta.fields.some((field) => field.key === key)) continue
      if (meta.fields.some((field) => field.name === key)) continue
      const inferred = inferField(key, value, {
        blobThresholdBytes: this.database.config.blobThresholdBytes,
      })
      if (inferred) missing.push(inferred)
    }
    if (missing.length === 0) return
    await this.ddlMutex.runExclusive(async () => {
      // Another insert may have added some fields while we waited.
      const stillMissing = missing.filter(
        (field) =>
          !meta.fields.some((item) => item.key === field.key || item.name === field.name),
      )
      if (stillMissing.length > 0) await this.addInferredFields(meta, stillMissing)
    })
  }

  private async liveFields(tableId: string): Promise<FeishuFieldInfo[]> {
    return this.database.client.listFields(this.token!, tableId)
  }

  private async physicalTableCount(): Promise<number> {
    // Count real Bitable tables (including its default table) rather than only
    // catalogued shards, so we respect the platform's per-Base limit.
    return (await this.database.client.listTables(this.token!)).length
  }

  private async buildAdoptedMeta(
    tables: Array<{ tableId: string; name: string; index: number }>,
    resolveTableName?: (tableId: string) => string | undefined,
  ): Promise<TableMeta> {
    this.token = await this.database.getToken()
    const primary = tables[0]!
    const live = await this.liveFields(primary.tableId)
    const fields: StoredField[] = []
    for (const physical of live) {
      fields.push(adoptPhysicalField(physical, fields, resolveTableName))
    }
    if (!fields.some((field) => field.key === '_id')) {
      fields.unshift({
        key: '_id',
        name: '_id',
        kind: 'text',
        feishuType: 1,
        primary: true,
        description: 'bitedb document id',
      })
    }
    const shards: TableMeta['shards'] = []
    for (const table of tables) {
      shards.push({
        index: table.index,
        tableId: table.tableId,
        name: table.name,
        rows: await this.countShardRows(table.tableId),
      })
    }
    return {
      database: this.database.name,
      name: this.name,
      fields,
      shards,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    }
  }

  private async countShardRows(tableId: string): Promise<number> {
    const page = await this.database.client.searchRecords(
      this.token!,
      tableId,
      { automatic_fields: false },
      { pageSize: 1 },
    )
    return page.total ?? 0
  }

  // ---------------------------------------------------------------------------
  // Physical tables and shards
  // ---------------------------------------------------------------------------

  private async createPhysicalTable(
    meta: TableMeta,
    index: number,
  ): Promise<TableMeta['shards'][number]> {
    const baseFields = meta.fields.filter(
      (field) => field.kind !== 'link' && field.kind !== 'computed',
    )
    const payloadFields = baseFields.map((field) => feishuFieldPayload(field))
    const created = await this.database.client.createTable(this.token!, {
      name: this.physicalTableName(index),
      default_view_name: 'all',
      fields: payloadFields,
    })
    const shard = {
      index,
      tableId: created.tableId,
      name: this.physicalTableName(index),
      rows: 0,
    }
    if (created.fieldIds.length === payloadFields.length) {
      baseFields.forEach((field, fieldIndex) => {
        if (index === 0) field.fieldId = created.fieldIds[fieldIndex]
      })
    } else {
      const live = await this.liveFields(created.tableId)
      const byName = new Map(live.map((field) => [field.field_name, field]))
      for (const field of baseFields) {
        const physical = byName.get(field.name)
        if (physical && index === 0) field.fieldId = physical.field_id
      }
    }
    return shard
  }

  private physicalTableName(index: number): string {
    // Keep the full logical name so catalog-loss adoption can match it.
    return index === 0 ? this.name : `${this.name}#${index}`
  }

  private async createShard(meta: TableMeta): Promise<TableMeta['shards'][number]> {
    const count = await this.physicalTableCount()
    if (count >= this.database.config.maxTablesPerBase) {
      throw new BiteDBError(
        `Database "${this.database.name}" reached maxTablesPerBase=${this.database.config.maxTablesPerBase}. ` +
          'Bitable allows at most 100 tables per Base; split logical tables or raise the limit.',
      )
    }
    const index = meta.shards.reduce((max, shard) => Math.max(max, shard.index), -1) + 1
    const shard = await this.createPhysicalTable(meta, index)
    meta.shards.push(shard)
    try {
      await this.populateAuxFields(meta, shard)
    } catch (err) {
      meta.shards = meta.shards.filter((candidate) => candidate !== shard)
      this.ensurePromise = undefined
      throw err
    }
    const saved = await this.database.catalog.saveTable(meta)
    meta.recordId = saved.recordId
    this.database.logger.info?.(
      `[bitedb] created shard ${index} for "${this.name}" (${shard.tableId})`,
    )
    return shard
  }

  /** Create link/computed fields on a newly created physical shard. */
  private async populateAuxFields(
    meta: TableMeta,
    shard: TableMeta['shards'][number],
  ): Promise<void> {
    const live = await this.liveFields(shard.tableId)
    const byName = new Map(live.map((field) => [field.field_name, field]))

    for (const field of meta.fields) {
      if (field.kind !== 'link') continue
      if (byName.has(field.name)) continue
      const created = await this.createLinkField(meta, shard, field)
      byName.set(created.field_name, created)
      if (shard.index === 0) {
        if (!field.fieldId) field.fieldId = created.field_id
        await this.registerBackField(meta, shard, field, created)
      }
    }

    const pending = meta.fields.filter((field) => field.kind === 'computed')
    while (pending.length > 0) {
      let progress = false
      for (let index = pending.length - 1; index >= 0; index--) {
        const field = pending[index]!
        if (byName.has(field.name)) {
          pending.splice(index, 1)
          continue
        }
        let created: FeishuFieldInfo
        try {
          created = await this.createComputedField(meta, shard, field, byName)
        } catch (err) {
          if (isMissingDependency(err)) continue
          throw err
        }
        byName.set(created.field_name, created)
        pending.splice(index, 1)
        progress = true
        if (shard.index === 0 && !field.fieldId) field.fieldId = created.field_id
      }
      if (!progress && pending.length > 0) {
        throw new UnsupportedError(
          `Could not resolve computed field dependencies in "${this.name}"`,
        )
      }
    }
  }

  private async reserveShard(meta: TableMeta): Promise<TableMeta['shards'][number]> {
    return this.shardMutex.runExclusive(async () => {
      let target = meta.shards.find((shard) => shard.rows < this.database.config.maxRowsPerShard)
      if (!target) {
        target = await this.createShard(meta)
      }
      target.rows++
      return target
    })
  }

  // ---------------------------------------------------------------------------
  // Writes
  // ---------------------------------------------------------------------------

  async insertOne(doc: T): Promise<WithId<T>> {
    if (this.closed) throw new BiteDBError(`Table "${this.name}" is closed`)
    if (!doc || typeof doc !== 'object' || Array.isArray(doc) || doc instanceof Date || Buffer.isBuffer(doc)) {
      throw new ValidationError('insertOne expects a plain document object')
    }
    const meta = await this.ensure()
    const providedId = (doc as Document)._id
    if (providedId !== undefined && typeof providedId !== 'string') {
      throw new ValidationError('_id must be a string')
    }
    const id = providedId && providedId.length > 0 ? providedId : generateId()
    if (this.idCache.has(id)) {
      throw new ValidationError(`Duplicate _id "${id}" in table "${this.name}"`)
    }
    const document: Document = { ...(doc as Document), _id: id }
    await this.ensureInferredFields(meta, document)
    const fields = await encodeDocument(document, meta, this.codecContext())
    const idField = meta.fields.find((field) => field.key === '_id')!
    fields[idField.name] = id

    const shard = await this.reserveShard(meta)
    try {
      const result = await this.insertBatcher.add(String(shard.index), {
        id,
        document,
        fields,
        shardIndex: shard.index,
      })
      return result as WithId<T>
    } catch (err) {
      shard.rows = Math.max(0, shard.rows - 1)
      throw err
    }
  }

  async insertMany(docs: T[]): Promise<WithId<T>[]> {
    if (!Array.isArray(docs)) throw new ValidationError('insertMany expects an array of documents')
    return Promise.all(docs.map((doc) => this.insertOne(doc)))
  }

  /** Alias for insertOne. */
  async insert(doc: T): Promise<WithId<T>> {
    return this.insertOne(doc)
  }

  private async flushInserts(shardIndex: number, items: InsertItem[]): Promise<InsertResult[]> {
    const meta = this.requireMeta()
    const shard = meta.shards.find((candidate) => candidate.index === shardIndex)
    if (!shard) throw new BiteDBError(`Shard ${shardIndex} not found in metadata`)
    try {
      const records = await this.database.client.batchCreateRecords(
        this.token!,
        shard.tableId,
        items.map((item) => ({ fields: item.fields })),
        randomUUID(),
      )
      if (records.length !== items.length) {
        throw new BiteDBError(
          `Bitable batch_create returned ${records.length} records for ${items.length} inputs`,
        )
      }
      const idFieldName = meta.fields.find((field) => field.key === '_id')?.name ?? '_id'
      const byLogicalId = new Map<string, FeishuRecordInfo>()
      for (const record of records) {
        const logical = record.fields?.[idFieldName]
        if (typeof logical === 'string' && logical.length > 0) {
          byLogicalId.set(logical, record)
        }
      }
      const results: InsertResult[] = []
      items.forEach((item, index) => {
        // Prefer the echoed logical _id; fall back to documented request order.
        const record = byLogicalId.get(item.id) ?? records[index]!
        this.rememberLocation({
          shardIndex,
          recordId: record.record_id,
          logicalId: item.id,
        })
        results.push(publicDocument(item.document, meta, item.id))
      })
      this.database.catalog.scheduleTableSave(meta)
      return results
    } catch (err) {
      // The individual insertOne callers roll back their own reservation. A
      // batch_create call is all-or-nothing, so this avoids double counting.
      this.database.catalog.scheduleTableSave(meta)
      throw err
    }
  }

  // ---------------------------------------------------------------------------
  // Read
  // ---------------------------------------------------------------------------

  find(filter: Filter = {}, options: QueryOptions = {}): Query<T> {
    return new Query<T>(this, filter, options)
  }

  /**
   * Cursor pagination helper. Shorthand for
   * `find({}, options).after(field, lastValue)`; use `Query.after()` directly
   * to combine a cursor with a filter. The cursor field should be unique
   * (`_id` is a good choice); rows sharing a cursor value may be skipped.
   */
  findAfter(field: string, lastValue: unknown, options: QueryOptions = {}): Query<T> {
    return this.find({}, options).after(field, lastValue)
  }

  async findOne(filter: Filter = {}, options: QueryOptions = {}): Promise<WithId<T> | null> {
    const docs = await this.executeFind(filter, { ...options, limit: 1, offset: 0 })
    return (docs[0] as WithId<T> | undefined) ?? null
  }

  async get(id: string): Promise<WithId<T> | null> {
    if (typeof id !== 'string' || !id) throw new ValidationError('get expects a non-empty id')
    const located = await this.tryLocate(id)
    if (!located) return null
    if (Object.keys(located.fields).length === 0) {
      const record = await this.database.client.getRecord(
        this.token!,
        located.shardTableId,
        located.recordId,
      )
      located.fields = record.fields
    }
    const doc = await decodeRecord(
      located.fields,
      this.requireMeta(),
      this.codecContext(),
      located.recordId,
    )
    this.rememberLocation({
      shardIndex: located.shardIndex,
      recordId: located.recordId,
      logicalId: typeof doc._id === 'string' ? doc._id : undefined,
    })
    return doc as WithId<T>
  }

  async countDocuments(filter: Filter = {}): Promise<number> {
    const meta = await this.ensure()
    const compiled = compileFilter(await this.translateFilter(filter, meta), meta)
    if (compiled.impossible) return 0
    const counts = await Promise.all(
      meta.shards.map((shard) =>
        this.writeSemaphore.run(async () => {
          const page = await this.database.client.searchRecords(
            this.token!,
            shard.tableId,
            { filter: compiled.filter, automatic_fields: false },
            { pageSize: 1 },
          )
          if (page.total !== undefined) return page.total
          const { items } = await this.database.client.searchAllRecords(
            this.token!,
            shard.tableId,
            { filter: compiled.filter, automatic_fields: false },
          )
          return items.length
        }),
      ),
    )
    return counts.reduce((sum, count) => sum + count, 0)
  }

  async count(filter: Filter = {}): Promise<number> {
    return this.countDocuments(filter)
  }

  /** @internal used by Query */
  async executeFind(filter: Filter = {}, options: QueryOptions = {}): Promise<Document[]> {
    return (await this.executeFindPage(filter, options)).documents
  }

  /** @internal used by Query.page() */
  async executeFindPage(
    filter: Filter = {},
    options: QueryOptions = {},
  ): Promise<{ documents: Document[]; hasMore: boolean }> {
    const meta = await this.ensure()
    const compiled = compileFilter(await this.translateFilter(filter, meta), meta)
    if (compiled.impossible) return { documents: [], hasMore: false }
    const { limit, offset } = normalizeQueryOptions(options)
    if (limit === 0) return { documents: [], hasMore: false }
    const sort = parseSort(options.sort, meta)
    const sortSpec = options.sort
    const fieldNames = mapFieldNames(options.fields, meta)
    const need = offset + limit
    // Read one extra row beyond the page. This is enough to tell whether more
    // results exist without a separate count query.
    const probe = need + 1

    const perShard = await Promise.all(
      meta.shards.map((shard) =>
        this.writeSemaphore.run(async () => {
          const { items } = await this.database.client.searchAllRecords(
            this.token!,
            shard.tableId,
            {
              filter: compiled.filter,
              ...(sort.length > 0 ? { sort } : {}),
              ...(fieldNames ? { field_names: fieldNames } : {}),
              automatic_fields: false,
            },
            { maxRecords: probe },
          )
          const docs: Document[] = []
          for (const item of items) {
            const doc = await decodeRecord(
              item.fields,
              meta,
              this.codecContext(),
              item.record_id,
            )
            this.rememberLocation({
              shardIndex: shard.index,
              recordId: item.record_id,
              logicalId: typeof doc._id === 'string' ? doc._id : undefined,
            })
            docs.push(doc)
          }
          return { index: shard.index, docs }
        }),
      ),
    )

    let merged: Document[]
    if (sort.length > 0) {
      merged = mergeSorted(
        perShard.map((entry) => entry.docs),
        (a, b) => compareDocuments(a, b, sortSpec!),
        probe,
      )
    } else {
      perShard.sort((a, b) => a.index - b.index)
      merged = perShard.flatMap((entry) => entry.docs)
    }

    const hasMore = merged.length > need
    if (hasMore && options.limit === undefined) {
      this.database.logger.debug(
        `[bitedb] query on "${this.name}" hit the default limit of ${limit} rows; ` +
          'results were truncated. Pass limit() explicitly or use page() to detect more rows.',
      )
    }
    return { documents: merged.slice(offset, offset + limit), hasMore }
  }

  // ---------------------------------------------------------------------------
  // Update
  // ---------------------------------------------------------------------------

  async updateOne(
    target: string | Filter,
    update: UpdateSpec<T>,
    options: UpdateOptions = {},
  ): Promise<UpdateResult<T>> {
    if (typeof target === 'string' && target.length === 0) {
      throw new ValidationError('updateOne id must be a non-empty string')
    }
    const returnDocument = normalizeReturnDocument(options)
    const meta = await this.ensure()
    const located =
      typeof target === 'string'
        ? await this.tryLocate(target)
        : await this.locateFirst(target, meta)
    if (!located) return { matchedCount: 0, modifiedCount: 0 }
    const patch = await this.buildPatch(meta, update, located.fields)
    if (Object.keys(patch).length === 0) {
      const result: UpdateResult<T> = { matchedCount: 1, modifiedCount: 0 }
      if (returnDocument) {
        result.document = (await this.decodeStoredRecord(meta, located, located.fields)) as WithId<T>
      }
      return result
    }
    await this.database.client.updateRecord(
      this.token!,
      located.shardTableId,
      located.recordId,
      patch,
    )
    const result: UpdateResult<T> = { matchedCount: 1, modifiedCount: 1 }
    if (returnDocument === 'before') {
      result.document = (await this.decodeStoredRecord(meta, located, located.fields)) as WithId<T>
    } else if (returnDocument === 'after') {
      result.document = (await this.decodePatchedRecord(
        meta,
        located,
        located.fields,
        patch,
      )) as WithId<T>
    }
    return result
  }

  async updateById(
    id: string,
    update: UpdateSpec<T>,
    options: UpdateOptions = {},
  ): Promise<UpdateResult<T>> {
    return this.updateOne(id, update, options)
  }

  async updateMany(
    filter: Filter,
    update: UpdateSpec<T>,
    options: UpdateOptions = {},
  ): Promise<UpdateResult<T>> {
    const returnDocument = normalizeReturnDocument(options)
    const meta = await this.ensure()
    const compiled = compileFilter(await this.translateFilter(filter, meta), meta)
    if (compiled.impossible) return { matchedCount: 0, modifiedCount: 0 }
    const staticPatch = hasRuntimeUpdateOperators(update)
      ? undefined
      : await this.buildPatch(meta, update, {})
    let matchedCount = 0
    let modifiedCount = 0
    const documents: Array<WithId<Document>> = []

    for (const shard of meta.shards) {
      const { items } = await this.database.client.searchAllRecords(this.token!, shard.tableId, {
        filter: compiled.filter,
        automatic_fields: false,
      })
      if (items.length === 0) continue
      const updates: Array<{ record_id: string; fields: Record<string, any> }> = []
      const patches = new Map<string, Record<string, any>>()
      for (const item of items) {
        matchedCount++
        const patch = staticPatch ?? (await this.buildPatch(meta, update, item.fields))
        if (Object.keys(patch).length === 0) continue
        updates.push({ record_id: item.record_id, fields: patch })
        patches.set(item.record_id, patch)
      }
      for (const chunk of chunkArray(updates, this.database.config.maxBatchSize)) {
        if (chunk.length === 0) continue
        await this.database.client.batchUpdateRecords(this.token!, shard.tableId, chunk)
        modifiedCount += chunk.length
      }
      if (returnDocument) {
        for (const item of items) {
          const location = { shardIndex: shard.index, recordId: item.record_id }
          const patch = patches.get(item.record_id)
          documents.push(
            returnDocument === 'after' && patch
              ? await this.decodePatchedRecord(meta, location, item.fields, patch)
              : await this.decodeStoredRecord(meta, location, item.fields),
          )
        }
      }
    }
    const result: UpdateResult<T> = { matchedCount, modifiedCount }
    if (returnDocument) result.documents = documents as Array<WithId<T>>
    return result
  }

  private async decodeStoredRecord(
    meta: TableMeta,
    location: RecordLocation,
    fields: Record<string, any>,
  ): Promise<WithId<Document>> {
    const doc = await decodeRecord(fields, meta, this.codecContext(), location.recordId)
    this.rememberLocation({
      shardIndex: location.shardIndex,
      recordId: location.recordId,
      logicalId: typeof doc._id === 'string' ? doc._id : undefined,
    })
    return doc as WithId<Document>
  }

  /**
   * Build the post-update document locally from the pre-update row and the
   * encoded patch, mirroring what a fresh `get()` would return without an
   * extra read. Server-computed formula values are whatever the row held
   * before the write; Feishu recalculates them asynchronously.
   */
  private async decodePatchedRecord(
    meta: TableMeta,
    location: RecordLocation,
    beforeFields: Record<string, any>,
    patch: Record<string, any>,
  ): Promise<WithId<Document>> {
    const doc = await decodeRecord(beforeFields, meta, this.codecContext(), location.recordId)
    for (const [name, raw] of Object.entries(patch)) {
      const field = meta.fields.find((item) => item.name === name)
      if (!field) continue
      const value = await decodeFieldValue(field, raw, this.codecContext())
      if (value === undefined) delete doc[field.key]
      else setOwn(doc, field.key, value)
    }
    this.rememberLocation({
      shardIndex: location.shardIndex,
      recordId: location.recordId,
      logicalId: typeof doc._id === 'string' ? doc._id : undefined,
    })
    return doc as WithId<Document>
  }

  private async buildPatch(
    meta: TableMeta,
    update: UpdateSpec<T>,
    rawFields: Record<string, any>,
  ): Promise<Record<string, any>> {
    if (!update || typeof update !== 'object' || Array.isArray(update)) {
      throw new ValidationError('update must be an object')
    }
    const setValues: Record<string, unknown> = Object.create(null)
    const updateRecord = update as Record<string, unknown>
    const operatorKeys = Object.keys(updateRecord).filter((key) => key.startsWith('$'))
    const directKeys = Object.keys(updateRecord).filter((key) => !key.startsWith('$'))
    for (const key of directKeys) {
      if (!isSafePropertyKey(key)) throw new ValidationError(`Unsafe field key "${key}"`)
    }

    for (const key of directKeys) setValues[key] = updateRecord[key]

    if (operatorKeys.length > 0) {
      const $set = updateRecord.$set
      if ($set !== undefined) {
        if (!isRecord($set)) throw new ValidationError('$set expects an object')
        Object.assign(setValues, $set)
      }
      const $unset = updateRecord.$unset
      if ($unset !== undefined) {
        if (!Array.isArray($unset)) throw new ValidationError('$unset expects an array of field names')
        for (const key of $unset) setValues[String(key)] = null
      }
      const $inc = updateRecord.$inc
      if ($inc !== undefined) {
        if (!isRecord($inc)) throw new ValidationError('$inc expects an object')
        for (const [key, delta] of Object.entries($inc)) {
          if (typeof delta !== 'number' || !Number.isFinite(delta)) {
            throw new ValidationError('$inc values must be finite numbers')
          }
          const current = await this.readRawLogicalValue(meta, rawFields, key)
          const base = typeof current === 'number' ? current : Number(current ?? 0)
          if (!Number.isFinite(base)) {
            throw new ValidationError(`Cannot $inc non-numeric field "${key}"`)
          }
          setValues[key] = base + delta
        }
      }
      const $push = updateRecord.$push
      if ($push !== undefined) {
        if (!isRecord($push)) throw new ValidationError('$push expects an object')
        for (const [key, value] of Object.entries($push)) {
          const current = await this.readRawLogicalValue(meta, rawFields, key)
          const array = Array.isArray(current) ? current : []
          setValues[key] = [...array, value]
        }
      }
      for (const key of operatorKeys) {
        if (!['$set', '$unset', '$inc', '$push'].includes(key)) {
          throw new ValidationError(`Unsupported update operator "${key}"`)
        }
      }
    }

    if (Object.prototype.hasOwnProperty.call(setValues, '_id')) {
      throw new ValidationError('_id is immutable')
    }
    for (const key of Object.keys(setValues)) {
      const field = meta.fields.find((item) => item.key === key || item.name === key)
      if (field?.kind === 'computed') {
        throw new ValidationError(`Field "${key}" is computed and cannot be updated`)
      }
    }

    await this.ensureInferredFields(meta, setValues)
    return encodeDocument(setValues, meta, this.codecContext())
  }

  private async readRawLogicalValue(
    meta: TableMeta,
    rawFields: Record<string, any>,
    key: string,
  ): Promise<unknown> {
    const field = meta.fields.find((item) => item.key === key || item.name === key)
    if (!field) return undefined
    const raw = rawFields[field.name]
    if (raw === undefined) return undefined
    return decodeFieldValue(field, raw, this.codecContext('never'))
  }

  // ---------------------------------------------------------------------------
  // Delete
  // ---------------------------------------------------------------------------

  async deleteOne(target: string | Filter): Promise<DeleteResult> {
    if (typeof target === 'string' && target.length === 0) {
      throw new ValidationError('deleteOne id must be a non-empty string')
    }
    const meta = await this.ensure()
    const located =
      typeof target === 'string' ? await this.tryLocate(target) : await this.locateFirst(target, meta)
    if (!located) return { deletedCount: 0 }
    await this.database.client.deleteRecord(this.token!, located.shardTableId, located.recordId)
    this.forgetLocation(located)
    const shard = meta.shards.find((item) => item.tableId === located.shardTableId)
    if (shard) shard.rows = Math.max(0, shard.rows - 1)
    this.database.catalog.scheduleTableSave(meta)
    return { deletedCount: 1 }
  }

  async deleteMany(filter: Filter = {}): Promise<DeleteResult> {
    const meta = await this.ensure()
    const compiled = compileFilter(await this.translateFilter(filter, meta), meta)
    if (compiled.impossible) return { deletedCount: 0 }
    let deletedCount = 0
    try {
      for (const shard of meta.shards) {
        const { items } = await this.database.client.searchAllRecords(this.token!, shard.tableId, {
          filter: compiled.filter,
          automatic_fields: false,
        })
        for (const chunk of chunkArray(items, Math.min(500, this.database.config.maxBatchSize))) {
          if (chunk.length === 0) continue
          await this.database.client.batchDeleteRecords(
            this.token!,
            shard.tableId,
            chunk.map((item) => item.record_id),
          )
          for (const item of chunk) {
            this.forgetLocation({ shardIndex: shard.index, recordId: item.record_id })
          }
          deletedCount += chunk.length
          shard.rows = Math.max(0, shard.rows - chunk.length)
        }
      }
    } finally {
      if (deletedCount > 0) this.database.catalog.scheduleTableSave(meta)
    }
    return { deletedCount }
  }

  // ---------------------------------------------------------------------------
  // Record location / links
  // ---------------------------------------------------------------------------

  private async locateFirst(
    filter: Filter,
    meta: TableMeta,
  ): Promise<LocatedRecord | null> {
    const compiled = compileFilter(await this.translateFilter(filter, meta), meta)
    if (compiled.impossible) return null
    for (const shard of meta.shards) {
      const page = await this.database.client.searchRecords(
        this.token!,
        shard.tableId,
        { filter: compiled.filter, automatic_fields: false },
        { pageSize: 1 },
      )
      const item = page.items[0]
      if (item) {
        return {
          shardIndex: shard.index,
          recordId: item.record_id,
          fields: item.fields,
          shardTableId: shard.tableId,
        }
      }
    }
    return null
  }

  private async tryLocate(id: string): Promise<LocatedRecord | null> {
    const meta = await this.ensure()
    const cached = this.idCache.get(id)
    if (cached) {
      const shard = meta.shards.find((item) => item.index === cached.shardIndex)
      if (shard) {
        try {
          const record = await this.database.client.getRecord(this.token!, shard.tableId, cached.recordId)
          return {
            ...cached,
            shardTableId: shard.tableId,
            fields: record.fields,
          }
        } catch (err) {
          if (!isNotFound(err)) throw err
          this.idCache.delete(id)
          this.recordCache.delete(cached.recordId)
        }
      }
    }

    const idField = meta.fields.find((field) => field.key === '_id')
    if (idField) {
      for (const shard of meta.shards) {
        const page = await this.database.client.searchRecords(
          this.token!,
          shard.tableId,
          {
            filter: {
              conjunction: 'and',
              conditions: [{ field_name: idField.name, operator: 'is', value: [id] }],
            },
            automatic_fields: false,
          },
          { pageSize: 1 },
        )
        const item = page.items[0]
        if (item) {
          this.rememberLocation({
            shardIndex: shard.index,
            recordId: item.record_id,
            logicalId: id,
          })
          return {
            shardIndex: shard.index,
            recordId: item.record_id,
            logicalId: id,
            fields: item.fields,
            shardTableId: shard.tableId,
          }
        }
      }
    }

    // Allow raw Bitable record ids as a convenience.
    if (id.startsWith('rec')) {
      const byRecord = await this.findLocationByRecordId(id)
      if (byRecord) {
        return {
          ...byRecord,
          shardTableId: byRecord.shardTableId,
        }
      }
    }
    return null
  }

  async findLocationByLogicalId(id: string): Promise<LocatedRecord | null> {
    return this.tryLocate(id)
  }

  private async findLocationByRecordId(recordId: string): Promise<LocatedRecord | null> {
    const meta = await this.ensure()
    const cached = this.recordCache.get(recordId)
    if (cached) {
      const shard = meta.shards.find((item) => item.index === cached.shardIndex)
      if (shard) {
        try {
          const record = await this.database.client.getRecord(this.token!, shard.tableId, cached.recordId)
          return {
            ...cached,
            shardTableId: shard.tableId,
            fields: record.fields,
          }
        } catch (err) {
          if (!isNotFound(err)) throw err
          this.recordCache.delete(recordId)
          if (cached.logicalId) this.idCache.delete(cached.logicalId)
        }
      }
    }
    for (const shard of meta.shards) {
      try {
        const record = await this.database.client.getRecord(this.token!, shard.tableId, recordId)
        const idField = meta.fields.find((field) => field.key === '_id')
        const logical = idField ? record.fields[idField.name] : undefined
        const location = {
          shardIndex: shard.index,
          recordId,
          ...(typeof logical === 'string' && logical ? { logicalId: logical } : {}),
        }
        this.rememberLocation(location)
        return {
          ...location,
          fields: record.fields,
          shardTableId: shard.tableId,
        }
      } catch (err) {
        if (!isNotFound(err)) throw err
      }
    }
    return null
  }

  /**
   * Link fields store physical Bitable record ids, while developers query by
   * logical document ids. Rewrite filter values before compiling them.
   */
  private async translateFilter(filter: Filter | undefined, meta: TableMeta): Promise<Filter | undefined> {
    if (!filter) return filter
    const out: Filter = {}
    for (const [key, value] of Object.entries(filter)) {
      if ((key === '$and' || key === '$or') && Array.isArray(value)) {
        out[key] = await Promise.all(
          value.map((item) => this.translateFilter(item as Filter, meta)),
        )
        continue
      }
      if (key.startsWith('$')) {
        out[key] = value
        continue
      }
      const field = meta.fields.find((item) => item.key === key || item.name === key)
      if (!field || field.kind !== 'link' || !field.target) {
        out[key] = value
        continue
      }
      out[key] = await this.translateLinkFilterValue(field, value)
    }
    return out
  }

  private async translateLinkFilterValue(field: StoredField, value: unknown): Promise<unknown> {
    if (value === null || value === undefined) return value
    if (typeof value === 'string') {
      const [physical] = await this.linkResolver.resolveOutgoing(field, [value])
      return physical
    }
    if (Array.isArray(value)) {
      if (value.every((item) => typeof item === 'string')) {
        return this.linkResolver.resolveOutgoing(field, value)
      }
      return value
    }
    if (isRecord(value)) {
      const out: Record<string, unknown> = {}
      for (const [operator, operand] of Object.entries(value)) {
        if (operator === '$exists' || typeof operand === 'boolean' || operand === null) {
          out[operator] = operand
        } else if (Array.isArray(operand)) {
          out[operator] = operand.every((item) => typeof item === 'string')
            ? await this.linkResolver.resolveOutgoing(field, operand)
            : operand
        } else if (typeof operand === 'string') {
          const [physical] = await this.linkResolver.resolveOutgoing(field, [operand])
          out[operator] = physical
        } else {
          out[operator] = operand
        }
      }
      return out
    }
    return value
  }

  private async resolveLinkToPhysical(field: StoredField, ids: string[]): Promise<string[]> {
    if (!field.target) return ids
    const target = this.database.table(field.target)
    await target.ensure()
    const out: string[] = []
    for (const id of ids) {
      if (id.startsWith('rec')) {
        const known = target.recordLocation(id)
        if (!known) {
          const located = await target.findLocationByRecordId(id)
          if (!located) {
            out.push(id)
            continue
          }
          this.assertLinkableShard(target, located.shardIndex, id)
          out.push(located.recordId)
          continue
        }
        this.assertLinkableShard(target, known.shardIndex, id)
        out.push(known.recordId)
        continue
      }
      const cached = target.logicalLocation(id)
      if (cached) {
        this.assertLinkableShard(target, cached.shardIndex, id)
        out.push(cached.recordId)
        continue
      }
      const located = await target.findLocationByLogicalId(id)
      if (!located) {
        throw new NotFoundError(`Link target "${field.target}" has no document with _id "${id}"`)
      }
      this.assertLinkableShard(target, located.shardIndex, id)
      out.push(located.recordId)
    }
    return out
  }

  private assertLinkableShard(target: Table<any>, shardIndex: number, id: string): void {
    const targetMeta = target.meta
    if (targetMeta && targetMeta.shards.length > 1 && shardIndex !== 0) {
      throw new UnsupportedError(
        `Native Bitable links can only reference the first shard of "${target.name}". ` +
          `Document "${id}" lives in shard ${shardIndex}.`,
      )
    }
  }

  private async resolveLinkFromPhysical(field: StoredField, recordIds: string[]): Promise<string[]> {
    if (!field.target) return recordIds
    const target = this.database.table(field.target)
    const targetMeta = await target.ensure()
    const idField = targetMeta.fields.find((item) => item.key === '_id')
    const out: string[] = []
    for (const recordId of recordIds) {
      const cached = target.recordLocation(recordId)
      if (cached?.logicalId) {
        out.push(cached.logicalId)
        continue
      }
      const located = await target.findLocationByRecordId(recordId)
      if (!located) {
        out.push(recordId)
        continue
      }
      const logical = idField ? located.fields[idField.name] : undefined
      out.push(typeof logical === 'string' && logical ? logical : recordId)
    }
    return out
  }

  /** @internal */
  logicalLocation(id: string): RecordLocation | undefined {
    return this.idCache.get(id)
  }

  /** @internal */
  recordLocation(recordId: string): RecordLocation | undefined {
    return this.recordCache.get(recordId)
  }

  private rememberLocation(location: RecordLocation): void {
    if (location.logicalId) {
      this.idCache.delete(location.logicalId)
      this.idCache.set(location.logicalId, {
        shardIndex: location.shardIndex,
        recordId: location.recordId,
        logicalId: location.logicalId,
      })
      this.trimCache(this.idCache)
    }
    this.recordCache.delete(location.recordId)
    this.recordCache.set(location.recordId, {
      shardIndex: location.shardIndex,
      recordId: location.recordId,
      logicalId: location.logicalId,
    })
    this.trimCache(this.recordCache)
  }

  private forgetLocation(location: RecordLocation): void {
    const cached = this.recordCache.get(location.recordId)
    const logical = location.logicalId ?? cached?.logicalId
    if (logical) this.idCache.delete(logical)
    this.recordCache.delete(location.recordId)
  }

  private trimCache(cache: Map<string, RecordLocation>): void {
    while (cache.size > this.maxCacheSize) {
      const first = cache.keys().next().value
      if (first === undefined) break
      cache.delete(first)
    }
  }

  // ---------------------------------------------------------------------------
  // Introspection / lifecycle
  // ---------------------------------------------------------------------------

  async schema(): Promise<StoredField[]> {
    const meta = await this.ensure()
    return meta.fields.map((field) => ({ ...field }))
  }

  fields(): string[] {
    return this.meta ? this.meta.fields.map((field) => field.key) : []
  }

  async stats(): Promise<TableStats> {
    const meta = await this.ensure()
    return {
      name: this.name,
      shards: meta.shards.map((shard) => ({
        index: shard.index,
        tableId: shard.tableId,
        rows: shard.rows,
        capacity: this.database.config.maxRowsPerShard,
      })),
      totalRows: totalRows(meta),
      fields: meta.fields.map((field) => field.key),
    }
  }

  /** Re-read true row counts from Bitable (useful after crashes or multi-writer use). */
  async recount(): Promise<TableStats> {
    const meta = await this.ensure()
    for (const shard of meta.shards) {
      shard.rows = await this.countShardRows(shard.tableId)
    }
    const saved = await this.database.catalog.saveTable(meta)
    meta.recordId = saved.recordId
    return this.stats()
  }

  async flush(): Promise<void> {
    await this.insertBatcher.flushAll()
    await this.database.catalog.flush()
  }

  invalidate(): void {
    this.meta = undefined
    this.token = undefined
    this.ensurePromise = undefined
    this.baseEnsurePromise = undefined
    this.refreshRequested = true
  }

  async drop(): Promise<void> {
    this.closed = true
    try {
      await this.insertBatcher.close()
      if (this.meta?.shards.length) {
        try {
          await this.database.client.deleteTables(
            this.token!,
            this.meta.shards.map((shard) => shard.tableId),
          )
        } catch (err) {
          if (!isLastTableError(err)) {
            this.database.logger.warn?.(`[bitedb] failed to delete physical tables for "${this.name}":`, err)
          }
        }
      }
      await this.database.catalog.deleteTable(this.database.name, this.name)
      this.meta = undefined
      this.token = undefined
      this.ensurePromise = undefined
      this.baseEnsurePromise = undefined
      this.refreshRequested = false
      this.idCache.clear()
      this.recordCache.clear()
    } finally {
      // A dropped Table handle can be used again; the next operation recreates
      // the physical table and starts with a fresh write buffer.
      this.insertBatcher = this.createBatcher()
      this.closed = false
    }
  }

}

function adoptPhysicalField(
  physical: FeishuFieldInfo,
  existing: StoredField[],
  resolveTableName?: (tableId: string) => string | undefined,
): StoredField {
  let key = physical.field_name
  let suffix = 1
  while (existing.some((field) => field.key === key)) key = `${physical.field_name}_${suffix++}`
  const base: StoredField = {
    key,
    name: physical.field_name,
    kind: 'raw',
    feishuType: physical.type,
    fieldId: physical.field_id,
    existing: true,
    inferred: true,
  }

  if (physical.type === 18 || physical.type === 21) {
    const tableId = physical.property?.table_id
    const target = typeof tableId === 'string' ? resolveTableName?.(tableId) : undefined
    if (target) {
      return {
        ...base,
        kind: 'link',
        target,
        multiple: physical.property?.multiple !== false,
        duplex: physical.type === 21,
      }
    }
  }

  if (physical.type === 20) {
    const expr = physical.property?.formula_expression
    if (typeof expr === 'string' && expr.length > 0) {
      const dataType = Number(physical.property?.type?.data_type)
      return {
        ...base,
        kind: 'computed',
        expr,
        resultType:
          dataType === 2
            ? 'number'
            : dataType === 5
              ? 'date'
              : dataType === 7
                ? 'boolean'
                : dataType === 1
                  ? 'text'
                  : undefined,
      }
    }
  }

  const { kind, json, spill } = kindFromFeishu(physical)
  return {
    ...base,
    kind,
    ...(json ? { json: true } : {}),
    ...(spill ? { spill } : {}),
  }
}

function kindFromFeishu(physical: FeishuFieldInfo): {
  kind: StoredField['kind']
  json?: boolean
  spill?: number
} {
  switch (physical.type) {
    case 1:
      return { kind: 'text' }
    case 2:
      return { kind: 'number' }
    case 3:
      return { kind: 'select' }
    case 4:
      return { kind: 'multiSelect' }
    case 5:
      return { kind: 'date' }
    case 7:
      return { kind: 'boolean' }
    case 17:
      return { kind: 'attachment' }
    case 18:
    case 20:
    case 21:
    default:
      return { kind: 'raw' }
  }
}

function extractLinkTableId(field: FeishuFieldInfo): string | undefined {
  const tableId = field.property?.table_id
  return typeof tableId === 'string' ? tableId : undefined
}

function publicDocument(doc: Document, meta: TableMeta, id: string): WithId<Document> {
  const out: Document = { _id: id }
  const computed = new Set(meta.fields.filter((field) => field.kind === 'computed').map((field) => field.key))
  for (const [key, value] of Object.entries(doc)) {
    if (key === '_id' || computed.has(key)) continue
    setOwn(out, key, value)
  }
  return out as WithId<Document>
}

function chunkArray<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = []
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size))
  }
  return chunks
}

function hasRuntimeUpdateOperators(update: UpdateSpec<any>): boolean {
  if (!update || typeof update !== 'object') return false
  return Object.prototype.hasOwnProperty.call(update, '$inc') || Object.prototype.hasOwnProperty.call(update, '$push')
}

function normalizeReturnDocument(options: UpdateOptions | undefined): 'before' | 'after' | undefined {
  const value = options?.returnDocument
  if (value === undefined) return undefined
  if (value === 'before' || value === 'after') return value
  throw new ValidationError('returnDocument must be "before" or "after"')
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value)
}

function isNotFound(err: unknown): boolean {
  const code = (err as any)?.apiCode ?? (err as any)?.code
  return NOT_FOUND_CODES.has(Number(code)) || (err as any)?.status === 404
}

function isFieldNameConflict(err: unknown): boolean {
  return Number((err as any)?.apiCode) === 1254014
}

function isMissingDependency(err: unknown): boolean {
  return err instanceof ValidationError && /field id is not available|Unknown field/i.test(err.message)
}

function isFormulaCreateRejected(err: unknown): boolean {
  const status = Number((err as any)?.status)
  const code = Number((err as any)?.apiCode)
  return status === 400 || code === 1254015 || code === 1254016 || code === 1254066
}

function isLastTableError(err: unknown): boolean {
  const code = Number((err as any)?.apiCode)
  return code === 1254046 || code === 1254047 || /last table/i.test(String((err as any)?.message))
}

function shardIndexFromName(name: string, logicalName: string): number {
  if (name === logicalName) return 0
  const prefix = `${logicalName}#`
  if (!name.startsWith(prefix)) return -1
  const suffix = name.slice(prefix.length)
  return /^\d+$/.test(suffix) ? Number(suffix) : -1
}

function logicalTableNameFromPhysical(name: string): string {
  return name.replace(/#\d+$/, '')
}
