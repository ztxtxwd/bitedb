/* An in-memory approximation of the Feishu Open Platform endpoints bitedb uses. */
import http from 'node:http'
import type { AddressInfo } from 'node:net'

interface FakeField {
  field_id: string
  field_name: string
  type: number
  property?: Record<string, any>
  is_primary?: boolean
}

interface FakeRecord {
  record_id: string
  fields: Record<string, any>
  created_time: number
  last_modified_time: number
}

interface FakeTable {
  table_id: string
  name: string
  fields: FakeField[]
  records: Map<string, FakeRecord>
}

interface FakeBase {
  app_token: string
  name: string
  url: string
  tables: Map<string, FakeTable>
}

interface FakeMedia {
  name: string
  bytes: Buffer
}

export interface FakeCall {
  method: string
  path: string
  query: Record<string, string>
  body?: any
}

export class FakeFeishu {
  readonly bases = new Map<string, FakeBase>()
  readonly media = new Map<string, FakeMedia>()
  readonly calls: FakeCall[] = []
  readonly token = 'fake-tenant-token'
  rejectFormulaOnCreate = false

  private server?: http.Server
  private counters = { base: 0, table: 0, field: 0, record: 0, file: 0, view: 0 }
  private uploads = new Map<
    string,
    {
      name: string
      size: number
      blockSize: number
      blockNum: number
      parts: Map<number, Buffer>
    }
  >()
  url = ''

  async start(): Promise<this> {
    this.server = http.createServer((req, res) => {
      void this.handle(req, res).catch((err) => {
        this.send(res, 500, { code: 500, msg: String(err?.stack ?? err) })
      })
    })
    await new Promise<void>((resolve) => this.server!.listen(0, '127.0.0.1', resolve))
    const address = this.server.address() as AddressInfo
    this.url = `http://127.0.0.1:${address.port}`
    return this
  }

  async stop(): Promise<void> {
    if (!this.server) return
    await new Promise<void>((resolve, reject) =>
      this.server!.close((err) => (err ? reject(err) : resolve())),
    )
    this.server = undefined
  }

  callsMatching(method: string, fragment: string): FakeCall[] {
    return this.calls.filter((call) => call.method === method && call.path.includes(fragment))
  }

  clearCalls(): void {
    this.calls.length = 0
  }

  private async handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const url = new URL(req.url ?? '/', this.url || 'http://127.0.0.1')
    const method = req.method ?? 'GET'
    const chunks: Buffer[] = []
    for await (const chunk of req) chunks.push(Buffer.from(chunk))
    const rawBody = Buffer.concat(chunks)
    const contentType = String(req.headers['content-type'] ?? '')

    if (url.pathname === '/open-apis/auth/v3/tenant_access_token/internal' && method === 'POST') {
      const body = JSON.parse(rawBody.toString('utf8') || '{}')
      this.calls.push({ method, path: url.pathname, query: {}, body })
      if (body.app_id !== 'test-app' || body.app_secret !== 'test-secret') {
        this.send(res, 200, { code: 10003, msg: 'invalid app credentials' })
        return
      }
      this.send(res, 200, {
        code: 0,
        msg: 'success',
        tenant_access_token: this.token,
        expire: 7200,
      })
      return
    }

    if (req.headers.authorization !== `Bearer ${this.token}`) {
      this.send(res, 401, { code: 99991663, msg: 'Invalid access token' })
      return
    }

    let parsedBody: any
    if (contentType.includes('application/json') && rawBody.length > 0) {
      parsedBody = JSON.parse(rawBody.toString('utf8'))
    } else if (contentType.includes('multipart/form-data')) {
      parsedBody = await new Response(rawBody, { headers: { 'content-type': contentType } }).formData()
    }
    const query: Record<string, string> = {}
    for (const [key, value] of url.searchParams) query[key] = value
    this.calls.push({ method, path: url.pathname, query, body: parsedBody })

    // Drive -----------------------------------------------------------------
    if (url.pathname === '/open-apis/drive/v1/files' && method === 'GET') {
      const files = [...this.bases.values()].map((base) => ({
        name: base.name,
        token: base.app_token,
        type: 'bitable',
        parent_token: 'fldr_root',
        url: base.url,
      }))
      this.ok(res, { files, has_more: false })
      return
    }

    if (url.pathname === '/open-apis/drive/v1/medias/upload_all' && method === 'POST') {
      const form = parsedBody as FormData
      const file = form.get('file')
      const name = String(form.get('file_name') ?? 'blob')
      if (!(file instanceof Blob)) {
        this.apiError(res, 1254000, 'missing file')
        return
      }
      const bytes = Buffer.from(await file.arrayBuffer())
      const token = `box${String(++this.counters.file).padStart(8, '0')}`
      this.media.set(token, { name, bytes })
      this.ok(res, { file_token: token })
      return
    }

    if (url.pathname === '/open-apis/drive/v1/medias/upload_prepare' && method === 'POST') {
      const body = parsedBody ?? {}
      const uploadId = `upl${++this.counters.file}`
      const blockSize = 4 * 1024 * 1024
      const blockNum = Math.max(1, Math.ceil(Number(body.size) / blockSize))
      this.uploads.set(uploadId, {
        name: String(body.file_name ?? 'blob'),
        size: Number(body.size),
        blockSize,
        blockNum,
        parts: new Map(),
      })
      this.ok(res, { upload_id: uploadId, block_size: blockSize, block_num: blockNum })
      return
    }

    if (url.pathname === '/open-apis/drive/v1/medias/upload_part' && method === 'POST') {
      const form = parsedBody as FormData
      const uploadId = String(form.get('upload_id'))
      const upload = this.uploads.get(uploadId)
      const file = form.get('file')
      if (!upload || !(file instanceof Blob)) {
        this.apiError(res, 1254000, 'invalid upload part')
        return
      }
      upload.parts.set(Number(form.get('seq')), Buffer.from(await file.arrayBuffer()))
      this.ok(res, {})
      return
    }

    if (url.pathname === '/open-apis/drive/v1/medias/upload_finish' && method === 'POST') {
      const body = parsedBody ?? {}
      const upload = this.uploads.get(String(body.upload_id))
      if (!upload) {
        this.apiError(res, 1254000, 'unknown upload')
        return
      }
      const chunks: Buffer[] = []
      for (let seq = 0; seq < Number(body.block_num ?? upload.blockNum); seq++) {
        const part = upload.parts.get(seq)
        if (!part) {
          this.apiError(res, 1254000, `missing part ${seq}`)
          return
        }
        chunks.push(part)
      }
      const token = `box${String(++this.counters.file).padStart(8, '0')}`
      this.media.set(token, { name: upload.name, bytes: Buffer.concat(chunks) })
      this.uploads.delete(String(body.upload_id))
      this.ok(res, { file_token: token })
      return
    }

    const download = /^\/open-apis\/drive\/v1\/medias\/([^/]+)\/download$/.exec(url.pathname)
    if (download && method === 'GET') {
      const media = this.media.get(decodeURIComponent(download[1]!))
      if (!media) {
        this.send(res, 404, { code: 1254002, msg: 'media not found' })
        return
      }
      res.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-length': String(media.bytes.byteLength),
      })
      res.end(media.bytes)
      return
    }

    // Bitable ---------------------------------------------------------------
    if (url.pathname === '/open-apis/bitable/v1/apps' && method === 'POST') {
      const body = parsedBody ?? {}
      const base = this.createBase(body.name ?? 'untitled')
      this.ok(res, {
        app: {
          app_token: base.app_token,
          name: base.name,
          url: base.url,
          default_table_id: [...base.tables.keys()][0],
        },
      })
      return
    }

    const tablesMatch = /^\/open-apis\/bitable\/v1\/apps\/([^/]+)\/tables$/.exec(url.pathname)
    if (tablesMatch) {
      const base = this.requireBase(res, tablesMatch[1]!)
      if (!base) return
      if (method === 'GET') {
        this.ok(res, {
          items: [...base.tables.values()].map((table) => ({
            table_id: table.table_id,
            name: table.name,
            revision: 1,
          })),
          has_more: false,
          total: base.tables.size,
        })
        return
      }
      if (method === 'POST') {
        const payload = parsedBody?.table ?? {}
        const table = this.createTable(base, payload.name ?? 'table', payload.fields ?? [])
        this.ok(res, {
          table_id: table.table_id,
          default_view_id: `vew${++this.counters.view}`,
          field_id_list: table.fields.map((field) => field.field_id),
        })
        return
      }
    }

    const batchDeleteTables = /^\/open-apis\/bitable\/v1\/apps\/([^/]+)\/tables\/batch_delete$/.exec(url.pathname)
    if (batchDeleteTables && method === 'POST') {
      const base = this.requireBase(res, batchDeleteTables[1]!)
      if (!base) return
      for (const tableId of parsedBody?.table_ids ?? []) base.tables.delete(tableId)
      this.ok(res, {})
      return
    }

    const fieldList = /^\/open-apis\/bitable\/v1\/apps\/([^/]+)\/tables\/([^/]+)\/fields$/.exec(url.pathname)
    if (fieldList) {
      const table = this.requireTable(res, fieldList[1]!, fieldList[2]!)
      if (!table) return
      if (method === 'GET') {
        this.ok(res, {
          items: table.fields,
          has_more: false,
          total: table.fields.length,
        })
        return
      }
      if (method === 'POST') {
        if (
          this.rejectFormulaOnCreate &&
          parsedBody?.type === 20 &&
          parsedBody?.property?.formula_expression
        ) {
          this.apiError(res, 1254015, 'formula_expression is not accepted on create')
          return
        }
        const field = this.createField(table, parsedBody)
        if (!field) {
          this.apiError(res, 1254014, 'field name already exists')
          return
        }
        this.ok(res, { field })
        return
      }
    }

    const fieldOne = /^\/open-apis\/bitable\/v1\/apps\/([^/]+)\/tables\/([^/]+)\/fields\/([^/]+)$/.exec(url.pathname)
    if (fieldOne && method === 'PUT') {
      const table = this.requireTable(res, fieldOne[1]!, fieldOne[2]!)
      if (!table) return
      const field = table.fields.find((item) => item.field_id === fieldOne[3])
      if (!field) {
        this.apiError(res, 1254041, 'field not found')
        return
      }
      field.field_name = parsedBody.field_name ?? field.field_name
      field.type = parsedBody.type ?? field.type
      field.property = parsedBody.property ?? field.property
      this.ok(res, { field })
      return
    }

    const search = /^\/open-apis\/bitable\/v1\/apps\/([^/]+)\/tables\/([^/]+)\/records\/search$/.exec(url.pathname)
    if (search && method === 'POST') {
      const base = this.requireBase(res, search[1]!)
      if (!base) return
      const table = this.requireTable(res, search[1]!, search[2]!)
      if (!table) return
      this.searchRecords(res, base, table, parsedBody ?? {}, query)
      return
    }

    const batchCreate = /^\/open-apis\/bitable\/v1\/apps\/([^/]+)\/tables\/([^/]+)\/records\/batch_create$/.exec(url.pathname)
    if (batchCreate && method === 'POST') {
      const table = this.requireTable(res, batchCreate[1]!, batchCreate[2]!)
      if (!table) return
      const records = parsedBody?.records ?? []
      if (records.length > 1000) {
        this.apiError(res, 1254104, 'batch size exceeds 1000')
        return
      }
      const created = records.map((record: any) => this.createRecord(table, record.fields ?? {})) as FakeRecord[]
      this.ok(res, {
        records: created.map((record) => ({
          record_id: record.record_id,
          id: record.record_id,
          fields: record.fields,
        })),
      })
      return
    }

    const batchUpdate = /^\/open-apis\/bitable\/v1\/apps\/([^/]+)\/tables\/([^/]+)\/records\/batch_update$/.exec(url.pathname)
    if (batchUpdate && method === 'POST') {
      const table = this.requireTable(res, batchUpdate[1]!, batchUpdate[2]!)
      if (!table) return
      const records = (parsedBody?.records ?? []).map((item: any) => {
        const record = table.records.get(item.record_id)
        if (!record) return { record_id: item.record_id, id: item.record_id, fields: {} }
        Object.assign(record.fields, item.fields ?? {})
        record.last_modified_time = Date.now()
        return { record_id: record.record_id, id: record.record_id, fields: record.fields }
      })
      this.ok(res, { records })
      return
    }

    const batchDelete = /^\/open-apis\/bitable\/v1\/apps\/([^/]+)\/tables\/([^/]+)\/records\/batch_delete$/.exec(url.pathname)
    if (batchDelete && method === 'POST') {
      const table = this.requireTable(res, batchDelete[1]!, batchDelete[2]!)
      if (!table) return
      for (const recordId of parsedBody?.records ?? []) table.records.delete(recordId)
      this.ok(res, {})
      return
    }

    const createOne = /^\/open-apis\/bitable\/v1\/apps\/([^/]+)\/tables\/([^/]+)\/records$/.exec(url.pathname)
    if (createOne && method === 'POST') {
      const base = this.requireBase(res, createOne[1]!)
      if (!base) return
      const table = this.requireTable(res, createOne[1]!, createOne[2]!)
      if (!table) return
      const record = this.createRecord(table, parsedBody?.fields ?? {})
      this.ok(res, { record: this.serializeRecord(base, table, record) })
      return
    }

    const recordOne = /^\/open-apis\/bitable\/v1\/apps\/([^/]+)\/tables\/([^/]+)\/records\/([^/]+)$/.exec(url.pathname)
    if (recordOne) {
      const base = this.requireBase(res, recordOne[1]!)
      if (!base) return
      const table = this.requireTable(res, recordOne[1]!, recordOne[2]!)
      if (!table) return
      const recordId = recordOne[3]!
      if (method === 'GET') {
        const record = table.records.get(recordId)
        if (!record) {
          this.apiError(res, 1254040, 'record not found')
          return
        }
        this.ok(res, { record: this.serializeRecord(base, table, record) })
        return
      }
      if (method === 'PUT') {
        const record = table.records.get(recordId)
        if (!record) {
          this.apiError(res, 1254040, 'record not found')
          return
        }
        Object.assign(record.fields, parsedBody?.fields ?? {})
        record.last_modified_time = Date.now()
        this.ok(res, { record: this.serializeRecord(base, table, record) })
        return
      }
      if (method === 'DELETE') {
        table.records.delete(recordId)
        this.ok(res, { deleted: true })
        return
      }
    }

    this.send(res, 404, { code: 404, msg: `unhandled ${method} ${url.pathname}` })
  }

  // ---------------------------------------------------------------------------
  // Fake data model
  // ---------------------------------------------------------------------------

  private createBase(name: string): FakeBase {
    const appToken = `bascn${String(++this.counters.base).padStart(8, '0')}`
    const base: FakeBase = {
      app_token: appToken,
      name,
      url: `https://fake.feishu/${appToken}`,
      tables: new Map(),
    }
    this.createTable(base, '数据表', [{ field_name: '文本', type: 1 }])
    this.bases.set(appToken, base)
    return base
  }

  private createTable(base: FakeBase, name: string, fields: any[]): FakeTable {
    const table: FakeTable = {
      table_id: `tbl${String(++this.counters.table).padStart(8, '0')}`,
      name,
      fields: [],
      records: new Map(),
    }
    base.tables.set(table.table_id, table)
    for (const field of fields) {
      this.createField(table, field, base)
    }
    return table
  }

  private createField(table: FakeTable, payload: any, base?: FakeBase): FakeField | undefined {
    if (table.fields.some((field) => field.field_name === payload.field_name)) return undefined
    const field: FakeField = {
      field_id: `fld${String(++this.counters.field).padStart(8, '0')}`,
      field_name: payload.field_name,
      type: payload.type,
      property: payload.property ? structuredClone(payload.property) : undefined,
      is_primary: table.fields.length === 0,
    }
    table.fields.push(field)

    if (field.type === 21 && field.property?.table_id && field.property?.back_field_name) {
      const resolvedBase =
        base ??
        [...this.bases.values()].find((candidate) => candidate.tables.has(field.property!.table_id))
      const target = resolvedBase?.tables.get(field.property.table_id)
      if (resolvedBase && target && !target.fields.some((item) => item.field_name === field.property!.back_field_name)) {
        const back = this.createField(
          target,
          {
            field_name: field.property.back_field_name,
            type: 21,
            property: {
              table_id: table.table_id,
              _back_source: { tableId: table.table_id, fieldId: field.field_id },
            },
          },
          resolvedBase,
        )
        void back
      }
    }
    return field
  }

  private createRecord(table: FakeTable, fields: Record<string, any>): FakeRecord {
    const record: FakeRecord = {
      record_id: `rec${String(++this.counters.record).padStart(8, '0')}`,
      fields: structuredClone(fields),
      created_time: Date.now(),
      last_modified_time: Date.now(),
    }
    table.records.set(record.record_id, record)
    return record
  }

  private serializeRecord(
    base: FakeBase,
    table: FakeTable,
    record: FakeRecord,
  ): { record_id: string; id: string; fields: Record<string, any>; created_time: number; last_modified_time: number } {
    return {
      record_id: record.record_id,
      id: record.record_id,
      fields: this.materializeFields(base, table, record),
      created_time: record.created_time,
      last_modified_time: record.last_modified_time,
    }
  }

  private materializeFields(
    base: FakeBase,
    table: FakeTable,
    record: FakeRecord,
  ): Record<string, any> {
    const out: Record<string, any> = structuredClone(record.fields)
    const byId = new Map(table.fields.map((field) => [field.field_id, field]))
    const evaluating = new Set<string>()

    const valueOf = (field: FakeField): unknown => {
      if (field.type === 20) {
        if (evaluating.has(field.field_id)) return null
        evaluating.add(field.field_id)
        const expression = String(field.property?.formula_expression ?? '')
        const replaced = expression.replace(
          /bitable::\$table\[([^\]]+)\]\.\$field\[([^\]]+)\]/g,
          (_match, _tableId, fieldId) => {
            const referenced = byId.get(fieldId)
            if (!referenced) return 'null'
            const value = valueOf(referenced)
            if (value === null || value === undefined) return 'null'
            if (typeof value === 'string') return JSON.stringify(value)
            if (typeof value === 'boolean' || typeof value === 'number') return String(value)
            if (Array.isArray(value)) return JSON.stringify(value)
            return 'null'
          },
        )
        evaluating.delete(field.field_id)
        try {
          // Test-only evaluator for arithmetic formulas.
          const result = Function(`"use strict"; return (${replaced});`)()
          return result === undefined ? null : result
        } catch {
          return null
        }
      }
      if (field.property?._back_source) {
        const backSource = field.property._back_source as { tableId: string; fieldId: string }
        const sourceTable = base.tables.get(backSource.tableId)
        const sourceField = sourceTable?.fields.find((item) => item.field_id === backSource.fieldId)
        if (!sourceTable || !sourceField) return []
        return [...sourceTable.records.values()]
          .filter((source) => {
            const refs = source.fields[sourceField.field_name]
            return Array.isArray(refs) && refs.includes(record.record_id)
          })
          .map((source) => source.record_id)
      }
      return record.fields[field.field_name]
    }

    for (const field of table.fields) {
      if (field.type === 20 || field.property?._back_source) {
        out[field.field_name] = valueOf(field)
      }
    }
    return out
  }

  private searchRecords(
    res: http.ServerResponse,
    base: FakeBase,
    table: FakeTable,
    body: any,
    query: Record<string, string>,
  ): void {
    const materialized = [...table.records.values()].map((record) => ({
      record,
      fields: this.materializeFields(base, table, record),
    }))
    const filter = body.filter
    let matched = materialized
    if (filter) matched = matched.filter((entry) => matchesFilter(entry.fields, filter))

    if (Array.isArray(body.sort) && body.sort.length > 0) {
      matched = [...matched].sort((a, b) => {
        for (const sort of body.sort) {
          const comparison = compareValues(a.fields[sort.field_name], b.fields[sort.field_name])
          if (comparison !== 0) return sort.desc ? -comparison : comparison
        }
        return 0
      })
    }

    const total = matched.length
    const pageSize = Math.min(Number(query.page_size ?? 100), 500)
    const offset = query.page_token ? Number(Buffer.from(query.page_token, 'base64url').toString()) : 0
    const page = matched.slice(offset, offset + pageSize)
    const nextOffset = offset + page.length
    const hasMore = nextOffset < total
    const pageToken = hasMore ? Buffer.from(String(nextOffset)).toString('base64url') : undefined

    const fieldNames: string[] | undefined = body.field_names
    // Real Bitable search responses default to text_field_as_array=true; the
    // record GET endpoint returns plain strings. Keep the difference here so
    // tests cover both response shapes.
    const textAsArray = query.text_field_as_array !== 'false'
    const textFieldNames = table.fields
      .filter((field) => field.type === 1)
      .map((field) => field.field_name)
    let items = page.map((entry) => {
      const serialized = this.serializeRecord(base, table, entry.record)
      if (fieldNames) {
        const projected: Record<string, any> = {}
        for (const name of fieldNames) projected[name] = serialized.fields[name]
        serialized.fields = projected
      }
      if (textAsArray) {
        for (const name of textFieldNames) {
          const value = serialized.fields[name]
          if (typeof value === 'string') {
            serialized.fields[name] = [{ text: value, type: 'text' }]
          }
        }
      }
      return serialized
    })

    this.ok(res, {
      items,
      total,
      has_more: hasMore,
      page_token: pageToken,
    })
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private requireBase(res: http.ServerResponse, token: string): FakeBase | undefined {
    const base = this.bases.get(token)
    if (!base) this.apiError(res, 1254002, 'base not found')
    return base
  }

  private requireTable(
    res: http.ServerResponse,
    baseToken: string,
    tableId: string,
  ): FakeTable | undefined {
    const base = this.bases.get(baseToken)
    const table = base?.tables.get(tableId)
    if (!table) this.apiError(res, 1254040, 'table not found')
    return table
  }

  private ok(res: http.ServerResponse, data: unknown): void {
    this.send(res, 200, { code: 0, msg: 'success', data })
  }

  private apiError(res: http.ServerResponse, code: number, msg: string): void {
    this.send(res, 200, { code, msg })
  }

  private send(res: http.ServerResponse, status: number, body: unknown): void {
    res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' })
    res.end(JSON.stringify(body))
  }
}

function matchesFilter(fields: Record<string, any>, filter: any): boolean {
  const conjunction = filter.conjunction === 'or' ? 'or' : 'and'
  const conditions: any[] = filter.conditions ?? []
  const conditionResults = conditions.map((condition) => matchCondition(fields, condition))
  const base = combine(conditionResults, conjunction)
  const children: any[] = filter.children ?? []
  return base && children.every((child) => child && matchesFilter(fields, child))
}

function combine(results: boolean[], conjunction: 'and' | 'or'): boolean {
  if (results.length === 0) return true
  return conjunction === 'and' ? results.every(Boolean) : results.some(Boolean)
}

function matchCondition(fields: Record<string, any>, condition: any): boolean {
  const actual = fields[condition.field_name]
  const expected: string[] = condition.value ?? []
  switch (condition.operator) {
    case 'isEmpty':
      return isEmptyValue(actual)
    case 'isNotEmpty':
      return !isEmptyValue(actual)
    case 'is':
      return expected.some((value) => isEqualValue(actual, value))
    case 'isNot':
      return !expected.some((value) => isEqualValue(actual, value))
    case 'contains':
      return expected.some((value) => containsValue(actual, value))
    case 'doesNotContain':
      return !expected.some((value) => containsValue(actual, value))
    case 'isGreater':
      return compareToValue(actual, expected[0]) > 0
    case 'isGreaterEqual':
      return compareToValue(actual, expected[0]) >= 0
    case 'isLess':
      return compareToValue(actual, expected[0]) < 0
    case 'isLessEqual':
      return compareToValue(actual, expected[0]) <= 0
    default:
      return false
  }
}

function isEmptyValue(value: unknown): boolean {
  if (value === null || value === undefined || value === '') return true
  if (Array.isArray(value)) return value.length === 0
  return false
}

function isEqualValue(actual: unknown, expected: string): boolean {
  if (Array.isArray(actual)) return actual.some((item) => isEqualValue(item, expected))
  if (typeof actual === 'number') return actual === Number(expected)
  if (typeof actual === 'boolean') return String(actual) === expected
  if (actual === null || actual === undefined) return expected === ''
  return String(actual) === expected
}

function containsValue(actual: unknown, expected: string): boolean {
  if (Array.isArray(actual)) return actual.some((item) => String(item) === expected)
  if (typeof actual === 'string') return actual.includes(expected)
  return false
}

function compareToValue(actual: unknown, expected: string | undefined): number {
  if (expected === undefined) return 0
  if (actual === null || actual === undefined || actual === '') return -1
  const a = typeof actual === 'number' ? actual : Number(actual)
  const b = Number(expected)
  if (!Number.isNaN(a) && !Number.isNaN(b)) return a === b ? 0 : a < b ? -1 : 1
  return String(actual).localeCompare(expected)
}

function compareValues(a: unknown, b: unknown): number {
  if (a === b) return 0
  if (a === null || a === undefined) return 1
  if (b === null || b === undefined) return -1
  if (typeof a === 'number' && typeof b === 'number') return a < b ? -1 : 1
  return String(a).localeCompare(String(b))
}
