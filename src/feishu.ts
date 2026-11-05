import { FeishuApiError, BiteDBError, ConfigError, isRetryableCode } from './errors.js'
import type { Logger } from './logger.js'
import { TokenBucket, clamp, normalizeUrl, randomUUID, sleep } from './util.js'

export interface FeishuClientOptions {
  appId?: string
  appSecret?: string
  accessToken?: string | (() => string | Promise<string>)
  domain?: string
  timeoutMs: number
  retries: number
  bitableQps: number
  driveQps: number
  fetch?: typeof fetch
  logger: Logger
  userAgent?: string
}

export interface FeishuTableInfo {
  table_id: string
  name: string
  revision?: number
}

export interface FeishuFieldInfo {
  field_id: string
  field_name: string
  type: number
  ui_type?: string
  property?: Record<string, any>
  is_primary?: boolean
  description?: { text?: string; disable_sync?: boolean } | string
}

export interface FeishuAttachmentInfo {
  file_token: string
  name?: string
  size?: number
  type?: string
  url?: string
  tmp_url?: string
}

export interface FeishuRecordInfo {
  record_id: string
  fields: Record<string, any>
  created_time?: number
  last_modified_time?: number
  created_by?: Record<string, any>
  last_modified_by?: Record<string, any>
  shared_url?: string
  record_url?: string
}

export interface FeishuRecordPage {
  items: FeishuRecordInfo[]
  total?: number
  hasMore: boolean
  pageToken?: string
}

export interface FeishuFilterCondition {
  field_name: string
  operator:
    | 'is'
    | 'isNot'
    | 'contains'
    | 'doesNotContain'
    | 'isEmpty'
    | 'isNotEmpty'
    | 'isGreater'
    | 'isGreaterEqual'
    | 'isLess'
    | 'isLessEqual'
  value?: string[]
}

export interface FeishuFilter {
  conjunction: 'and' | 'or'
  conditions?: FeishuFilterCondition[]
  children?: Array<{
    conjunction: 'and' | 'or'
    conditions: FeishuFilterCondition[]
  }>
}

export interface FeishuRecordSearchBody {
  view_id?: string
  field_names?: string[]
  sort?: Array<{ field_name: string; desc?: boolean }>
  filter?: FeishuFilter
  automatic_fields?: boolean
}

export interface FeishuDriveFile {
  name: string
  token: string
  type: string
  parent_token?: string
  shortcut_info?: { target_token?: string; target_type?: string }
}

export interface CreateTablePayload {
  name?: string
  default_view_name?: string
  fields?: Array<{
    field_name: string
    type: number
    ui_type?: string
    property?: Record<string, unknown>
    description?: { text?: string; disable_sync?: boolean }
  }>
}

interface RequestOptions {
  method?: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE'
  path: string
  query?: Record<string, string | number | boolean | undefined | Array<string | number>>
  body?: unknown
  headers?: Record<string, string>
  category?: 'default' | 'drive'
  /** Safe to replay after a network error / 5xx. Defaults to true for GET/PUT/DELETE. */
  idempotent?: boolean
  timeoutMs?: number
}

const DEFAULT_DOMAIN = 'https://open.feishu.cn'
const MAX_SIMPLE_UPLOAD = 20 * 1024 * 1024

function isNetworkError(err: unknown): boolean {
  if (err instanceof BiteDBError) return false
  if (err instanceof Error) {
    return (
      err.name === 'AbortError' ||
      err.name === 'TimeoutError' ||
      err.name === 'TypeError' ||
      /fetch failed|network|socket|ECONN|ETIMEDOUT|EAI_AGAIN/i.test(err.message)
    )
  }
  return false
}

function logIdFromHeaders(headers: Headers): string | undefined {
  return (
    headers.get('x-tt-logid') ??
    headers.get('x-request-id') ??
    headers.get('x-tt-trace-id') ??
    undefined
  )
}

export class FeishuClient {
  private readonly baseUrl: string
  private readonly timeoutMs: number
  private readonly retries: number
  private readonly fetchImpl: typeof fetch
  private readonly logger: Logger
  private readonly userAgent: string
  private readonly bitableBucket: TokenBucket
  private readonly driveBucket: TokenBucket

  private readonly appId?: string
  private readonly appSecret?: string
  private readonly accessTokenOption?: string | (() => string | Promise<string>)
  private tokenCache?: { token: string; expiresAt: number }
  private tokenPromise?: Promise<string>

  constructor(options: FeishuClientOptions) {
    this.baseUrl = normalizeUrl(options.domain ?? DEFAULT_DOMAIN)
    this.timeoutMs = options.timeoutMs
    this.retries = clamp(options.retries, 0, 10)
    this.fetchImpl = options.fetch ?? globalThis.fetch
    if (typeof this.fetchImpl !== 'function') {
      throw new ConfigError('No fetch implementation available. Use Node.js >= 18 or pass `fetch` in config.')
    }
    this.logger = options.logger
    this.userAgent = options.userAgent ?? 'bitedb/0.1.0'
    this.bitableBucket = new TokenBucket(Math.max(1, options.bitableQps))
    this.driveBucket = new TokenBucket(Math.max(1, options.driveQps))
    this.appId = options.appId
    this.appSecret = options.appSecret
    this.accessTokenOption = options.accessToken

    if (!this.accessTokenOption && (!this.appId || !this.appSecret)) {
      throw new ConfigError('bitedb requires `appId` + `appSecret`, or an `accessToken`/`accessToken` provider.')
    }
  }

  // ---------------------------------------------------------------------------
  // Auth
  // ---------------------------------------------------------------------------

  async getAccessToken(force = false): Promise<string> {
    if (typeof this.accessTokenOption === 'function') return this.accessTokenOption()
    if (typeof this.accessTokenOption === 'string') return this.accessTokenOption
    if (!force && this.tokenCache && this.tokenCache.expiresAt > Date.now()) return this.tokenCache.token
    if (this.tokenPromise) return this.tokenPromise

    this.tokenPromise = this.fetchTenantToken().finally(() => {
      this.tokenPromise = undefined
    })
    return this.tokenPromise
  }

  private async fetchTenantToken(): Promise<string> {
    const url = `${this.baseUrl}/open-apis/auth/v3/tenant_access_token/internal`
    let lastError: unknown
    for (let attempt = 0; attempt <= this.retries; attempt++) {
      try {
        const res = await this.fetchImpl(url, {
          method: 'POST',
          headers: { 'content-type': 'application/json; charset=utf-8', 'user-agent': this.userAgent },
          body: JSON.stringify({ app_id: this.appId, app_secret: this.appSecret }),
          signal: AbortSignal.timeout(this.timeoutMs),
        })
        const text = await res.text()
        let json: any
        try {
          json = text ? JSON.parse(text) : {}
        } catch {
          throw new FeishuApiError({
            status: res.status,
            code: res.status,
            msg: text.slice(0, 500) || res.statusText,
            method: 'POST',
            path: '/open-apis/auth/v3/tenant_access_token/internal',
          })
        }
        if (!res.ok || json.code !== 0 || !json.tenant_access_token) {
          throw new FeishuApiError({
            status: res.status,
            code: typeof json.code === 'number' ? json.code : res.status,
            msg: String(json.msg ?? res.statusText),
            method: 'POST',
            path: '/open-apis/auth/v3/tenant_access_token/internal',
            errorLogId: logIdFromHeaders(res.headers),
            response: json,
          })
        }
        const expire = Number(json.expire ?? 7200)
        // Refresh one minute early. Some Feishu responses use `expire` in seconds.
        this.tokenCache = {
          token: String(json.tenant_access_token),
          expiresAt: Date.now() + Math.max(60, expire - 60) * 1000,
        }
        this.logger.debug?.('[bitedb] acquired tenant access token')
        return this.tokenCache.token
      } catch (err) {
        lastError = err
        if (!this.shouldRetry(err, attempt, true)) break
        await sleep(this.backoffMs(attempt))
      }
    }
    throw lastError instanceof Error
      ? lastError
      : new BiteDBError('Failed to acquire a tenant access token', { cause: lastError })
  }

  // ---------------------------------------------------------------------------
  // Transport
  // ---------------------------------------------------------------------------

  async request<T>(options: RequestOptions): Promise<T> {
    const method = options.method ?? 'GET'
    const idempotent = options.idempotent ?? (method === 'GET' || method === 'PUT' || method === 'DELETE')
    const bucket = options.category === 'drive' ? this.driveBucket : this.bitableBucket
    let refreshedAuth = false
    let lastError: unknown

    for (let attempt = 0; attempt <= this.retries; attempt++) {
      await bucket.take()
      let token: string | undefined
      try {
        token = await this.getAccessToken(refreshedAuth)
        const res = await this.fetchImpl(this.buildUrl(options), {
          method,
          headers: {
            authorization: `Bearer ${token}`,
            'user-agent': this.userAgent,
            ...(options.body !== undefined && !(options.body instanceof FormData)
              ? { 'content-type': 'application/json; charset=utf-8' }
              : {}),
            ...options.headers,
          },
          body:
            options.body === undefined
              ? undefined
              : options.body instanceof FormData
                ? options.body
                : JSON.stringify(options.body),
          signal: AbortSignal.timeout(options.timeoutMs ?? this.timeoutMs),
        })

        if (res.status === 401 && !refreshedAuth && !this.accessTokenOption) {
          refreshedAuth = true
          this.tokenCache = undefined
          attempt--
          continue
        }

        const json = await this.parseJson<T>(res, method, options.path)
        this.logger.debug?.(`[bitedb] ${method} ${options.path} -> ${res.status}`)
        return json
      } catch (err) {
        lastError = err
        if (!this.shouldRetry(err, attempt, idempotent)) break
        const delay = this.retryDelay(err, attempt)
        this.logger.warn?.(`[bitedb] retrying ${method} ${options.path} in ${delay}ms:`, (err as Error).message)
        await sleep(delay)
      }
    }

    throw lastError instanceof Error
      ? lastError
      : new BiteDBError(`Feishu request failed: ${method} ${options.path}`, { cause: lastError })
  }

  private buildUrl(options: RequestOptions): string {
    const url = new URL(options.path.startsWith('http') ? options.path : `${this.baseUrl}${options.path}`)
    if (options.query) {
      for (const [key, value] of Object.entries(options.query)) {
        if (value === undefined || value === null || value === '') continue
        if (Array.isArray(value)) {
          for (const item of value) url.searchParams.append(key, String(item))
        } else {
          url.searchParams.set(key, String(value))
        }
      }
    }
    return url.toString()
  }

  private async parseJson<T>(res: Response, method: string, path: string): Promise<T> {
    const text = await res.text()
    let json: any = undefined
    if (text) {
      try {
        json = JSON.parse(text)
      } catch {
        if (!res.ok) {
          throw new FeishuApiError({
            status: res.status,
            code: res.status,
            msg: text.slice(0, 1000) || res.statusText,
            method,
            path,
            errorLogId: logIdFromHeaders(res.headers),
          })
        }
        return text as T
      }
    }
    const code = typeof json?.code === 'number' ? json.code : 0
    if (!res.ok || code !== 0) {
      const msg = String(json?.msg ?? res.statusText ?? 'Unknown Feishu error')
      const apiCode = typeof json?.code === 'number' ? json.code : res.status
      throw new FeishuApiError({
        status: res.status,
        code: apiCode,
        msg,
        method,
        path,
        errorLogId: json?.error?.log_id ?? logIdFromHeaders(res.headers),
        response: json,
      })
    }
    // Feishu wraps successful payloads in `data`; auth is the exception and
    // never goes through this path.
    return (json && typeof json === 'object' && 'data' in json ? json.data : json) as T
  }

  private shouldRetry(err: unknown, attempt: number, idempotent: boolean): boolean {
    if (attempt >= this.retries) return false
    if (err instanceof FeishuApiError) {
      // Rate limits / documented transient codes only. A 5xx POST may have
      // executed server-side, so it is only replayed when marked idempotent.
      if (isRetryableCode(err.apiCode) || err.status === 429) return true
      if (err.status >= 500 && idempotent) return true
      return false
    }
    if (isNetworkError(err)) return idempotent
    return false
  }

  private backoffMs(attempt: number): number {
    return Math.min(8000, 250 * 2 ** attempt) + Math.floor(Math.random() * 150)
  }

  private retryDelay(err: unknown, attempt: number): number {
    if (err instanceof FeishuApiError && err.status === 429) {
      // Without exposing headers on the error we still use a conservative
      // floor for rate limit retries.
      return Math.max(1000, this.backoffMs(attempt))
    }
    return this.backoffMs(attempt)
  }

  // ---------------------------------------------------------------------------
  // Drive
  // ---------------------------------------------------------------------------

  async listFiles(folderToken?: string): Promise<FeishuDriveFile[]> {
    const files: FeishuDriveFile[] = []
    let pageToken: string | undefined
    do {
      const data = await this.request<{
        files?: FeishuDriveFile[]
        has_more?: boolean
        next_page_token?: string
      }>({
        path: '/open-apis/drive/v1/files',
        query: {
          folder_token: folderToken,
          page_size: 200,
          page_token: pageToken,
        },
      })
      files.push(...(data.files ?? []))
      // Root folder listing explicitly does not support pagination.
      pageToken = folderToken && data.has_more ? data.next_page_token : undefined
    } while (pageToken)
    return files
  }

  async createBase(name: string, folderToken?: string): Promise<{ app_token: string; url?: string; name: string }> {
    const data = await this.request<{
      app: { app_token: string; url?: string; name: string; default_table_id?: string }
    }>({
      method: 'POST',
      path: '/open-apis/bitable/v1/apps',
      body: {
        name,
        ...(folderToken ? { folder_token: folderToken } : {}),
      },
      // Creating a Base is not idempotent, but rate limit retries are safe.
      idempotent: false,
    })
    return data.app
  }

  async uploadMedia(options: {
    parentToken: string
    bytes: Buffer
    fileName: string
    parentType?: 'bitable_file' | 'bitable_image'
  }): Promise<{ fileToken: string; fileName: string; size: number }> {
    const { parentToken, bytes, fileName } = options
    const parentType = options.parentType ?? 'bitable_file'
    const size = bytes.byteLength

    if (size <= MAX_SIMPLE_UPLOAD) {
      const form = new FormData()
      form.append('file_name', fileName)
      form.append('parent_type', parentType)
      form.append('parent_node', parentToken)
      form.append('size', String(size))
      form.append('file', new Blob([bytes]), fileName)
      const data = await this.request<{ file_token?: string }>({
        method: 'POST',
        path: '/open-apis/drive/v1/medias/upload_all',
        body: form,
        category: 'drive',
      })
      if (!data?.file_token) throw new BiteDBError('Feishu media upload returned no file_token')
      return { fileToken: data.file_token, fileName, size }
    }

    return this.uploadMediaChunked({ parentToken, bytes, fileName, parentType })
  }

  private async uploadMediaChunked(options: {
    parentToken: string
    bytes: Buffer
    fileName: string
    parentType: 'bitable_file' | 'bitable_image'
  }): Promise<{ fileToken: string; fileName: string; size: number }> {
    const { parentToken, bytes, fileName, parentType } = options
    const prepared = await this.request<{
      upload_id?: string
      block_size?: number
      block_num?: number
    }>({
      method: 'POST',
      path: '/open-apis/drive/v1/medias/upload_prepare',
      body: {
        file_name: fileName,
        parent_type: parentType,
        parent_node: parentToken,
        size: bytes.byteLength,
      },
      category: 'drive',
      idempotent: false,
    })
    if (!prepared?.upload_id || !prepared.block_size || !prepared.block_num) {
      throw new BiteDBError('Feishu chunked upload prepare returned an invalid strategy')
    }

    const uploadId = prepared.upload_id
    const blockSize = Number(prepared.block_size)
    const blockNum = Number(prepared.block_num)
    if (!Number.isFinite(blockSize) || !Number.isFinite(blockNum) || blockSize <= 0 || blockNum <= 0) {
      throw new BiteDBError('Feishu chunked upload prepare returned an invalid strategy')
    }
    for (let seq = 0; seq < blockNum; seq++) {
      const start = seq * blockSize
      const chunk = bytes.subarray(start, Math.min(bytes.byteLength, start + blockSize))
      if (chunk.byteLength === 0) continue
      const form = new FormData()
      form.append('upload_id', uploadId)
      form.append('seq', String(seq))
      form.append('size', String(chunk.byteLength))
      form.append('file', new Blob([chunk]), fileName)
      await this.request({
        method: 'POST',
        path: '/open-apis/drive/v1/medias/upload_part',
        body: form,
        category: 'drive',
      })
    }

    const finished = await this.request<{ file_token?: string }>({
      method: 'POST',
      path: '/open-apis/drive/v1/medias/upload_finish',
      body: { upload_id: uploadId, block_num: blockNum },
      category: 'drive',
      idempotent: false,
    })
    if (!finished?.file_token) throw new BiteDBError('Feishu chunked upload returned no file_token')
    return { fileToken: finished.file_token, fileName, size: bytes.byteLength }
  }

  /** Download a signed/Feishu-provided URL, without attaching the tenant token. */
  async downloadUrl(rawUrl: string): Promise<Buffer> {
    let url: URL
    try {
      url = new URL(rawUrl)
    } catch {
      throw new BiteDBError('Invalid media URL')
    }
    if (url.protocol !== 'https:' && url.protocol !== 'http:') {
      throw new BiteDBError('Unsupported media URL protocol')
    }
    await this.driveBucket.take()
    const res = await this.fetchImpl(url.toString(), {
      headers: { 'user-agent': this.userAgent },
      signal: AbortSignal.timeout(this.timeoutMs),
    })
    if (!res.ok) {
      const text = await res.text()
      throw new FeishuApiError({
        status: res.status,
        code: res.status,
        msg: text.slice(0, 1000) || res.statusText,
        method: 'GET',
        path: url.pathname,
        errorLogId: logIdFromHeaders(res.headers),
      })
    }
    return Buffer.from(await res.arrayBuffer())
  }

  async downloadMedia(fileToken: string): Promise<Buffer> {
    let token = await this.getAccessToken()
    let refreshed = false
    for (let attempt = 0; attempt <= this.retries; attempt++) {
      await this.driveBucket.take()
      try {
        const res = await this.fetchImpl(
          this.buildUrl({ path: `/open-apis/drive/v1/medias/${encodeURIComponent(fileToken)}/download` }),
          {
            headers: { authorization: `Bearer ${token}`, 'user-agent': this.userAgent },
            signal: AbortSignal.timeout(this.timeoutMs),
          },
        )
        if (res.status === 401 && !refreshed && !this.accessTokenOption) {
          refreshed = true
          token = await this.getAccessToken(true)
          attempt--
          continue
        }
        if (!res.ok) {
          const text = await res.text()
          let code = res.status
          let msg = text.slice(0, 1000) || res.statusText
          try {
            const json = JSON.parse(text)
            code = json.code ?? code
            msg = json.msg ?? msg
          } catch {
            /* raw body */
          }
          throw new FeishuApiError({
            status: res.status,
            code,
            msg,
            method: 'GET',
            path: `/open-apis/drive/v1/medias/${fileToken}/download`,
            errorLogId: logIdFromHeaders(res.headers),
          })
        }
        const buffer = Buffer.from(await res.arrayBuffer())
        // Some failures arrive as HTTP 200 JSON envelopes. A real JSON file is
        // still returned verbatim when no error code is present.
        if ((res.headers.get('content-type') ?? '').includes('application/json')) {
          try {
            const json = JSON.parse(buffer.toString('utf8'))
            if (json && typeof json.code === 'number' && json.code !== 0) {
              throw new FeishuApiError({
                status: 200,
                code: json.code,
                msg: String(json.msg ?? 'media download failed'),
                method: 'GET',
                path: `/open-apis/drive/v1/medias/${fileToken}/download`,
                errorLogId: json.error?.log_id ?? logIdFromHeaders(res.headers),
                response: json,
              })
            }
          } catch (err) {
            if (err instanceof FeishuApiError) throw err
          }
        }
        return buffer
      } catch (err) {
        if (!this.shouldRetry(err, attempt, true)) throw err
        await sleep(this.backoffMs(attempt))
      }
    }
    throw new BiteDBError(`Failed to download media ${fileToken}`)
  }

  // ---------------------------------------------------------------------------
  // Bitable metadata
  // ---------------------------------------------------------------------------

  async listTables(appToken: string): Promise<FeishuTableInfo[]> {
    const tables: FeishuTableInfo[] = []
    let pageToken: string | undefined
    do {
      const data = await this.request<{
        items?: FeishuTableInfo[]
        has_more?: boolean
        page_token?: string
      }>({
        path: `/open-apis/bitable/v1/apps/${encodeURIComponent(appToken)}/tables`,
        query: { page_size: 100, page_token: pageToken },
      })
      tables.push(...(data.items ?? []))
      pageToken = data.has_more ? data.page_token : undefined
    } while (pageToken)
    return tables
  }

  async createTable(
    appToken: string,
    table: CreateTablePayload,
  ): Promise<{ tableId: string; viewId?: string; fieldIds: string[] }> {
    const data = await this.request<{
      table_id?: string
      default_view_id?: string
      field_id_list?: string[]
    }>({
      method: 'POST',
      path: `/open-apis/bitable/v1/apps/${encodeURIComponent(appToken)}/tables`,
      body: { table },
      idempotent: false,
    })
    if (!data?.table_id) throw new BiteDBError('Feishu createTable returned no table_id')
    return { tableId: data.table_id, viewId: data.default_view_id, fieldIds: data.field_id_list ?? [] }
  }

  async deleteTables(appToken: string, tableIds: string[]): Promise<void> {
    if (tableIds.length === 0) return
    await this.request({
      method: 'POST',
      path: `/open-apis/bitable/v1/apps/${encodeURIComponent(appToken)}/tables/batch_delete`,
      body: { table_ids: tableIds },
      idempotent: true,
    })
  }

  async listFields(appToken: string, tableId: string): Promise<FeishuFieldInfo[]> {
    const fields: FeishuFieldInfo[] = []
    let pageToken: string | undefined
    do {
      const data = await this.request<{
        items?: FeishuFieldInfo[]
        has_more?: boolean
        page_token?: string
      }>({
        path: `/open-apis/bitable/v1/apps/${encodeURIComponent(appToken)}/tables/${encodeURIComponent(tableId)}/fields`,
        query: { page_size: 100, page_token: pageToken, text_field_as_array: false },
      })
      fields.push(...(data.items ?? []))
      pageToken = data.has_more ? data.page_token : undefined
    } while (pageToken)
    return fields
  }

  async createField(
    appToken: string,
    tableId: string,
    field: {
      field_name: string
      type: number
      ui_type?: string
      property?: Record<string, unknown>
      description?: { text?: string; disable_sync?: boolean }
    },
  ): Promise<FeishuFieldInfo> {
    const data = await this.request<{ field?: FeishuFieldInfo }>({
      method: 'POST',
      path: `/open-apis/bitable/v1/apps/${encodeURIComponent(appToken)}/tables/${encodeURIComponent(tableId)}/fields`,
      query: { client_token: randomUUID() },
      body: field,
      idempotent: true,
    })
    if (!data?.field) throw new BiteDBError(`Feishu createField(${field.field_name}) returned no field`)
    return data.field
  }

  async updateField(
    appToken: string,
    tableId: string,
    fieldId: string,
    field: {
      field_name: string
      type: number
      property?: Record<string, unknown>
      description?: { text?: string; disable_sync?: boolean }
    },
  ): Promise<FeishuFieldInfo> {
    const data = await this.request<{ field?: FeishuFieldInfo }>({
      method: 'PUT',
      path: `/open-apis/bitable/v1/apps/${encodeURIComponent(appToken)}/tables/${encodeURIComponent(tableId)}/fields/${encodeURIComponent(fieldId)}`,
      body: field,
      idempotent: true,
    })
    if (!data?.field) throw new BiteDBError(`Feishu updateField(${fieldId}) returned no field`)
    return data.field
  }

  // ---------------------------------------------------------------------------
  // Records
  // ---------------------------------------------------------------------------

  async searchRecords(
    appToken: string,
    tableId: string,
    body: FeishuRecordSearchBody,
    options: { pageSize?: number; pageToken?: string; fieldNames?: string[] } = {},
  ): Promise<FeishuRecordPage> {
    const data = await this.request<{
      items?: FeishuRecordInfo[]
      total?: number
      has_more?: boolean
      page_token?: string
    }>({
      method: 'POST',
      path: `/open-apis/bitable/v1/apps/${encodeURIComponent(appToken)}/tables/${encodeURIComponent(tableId)}/records/search`,
      query: {
        page_size: clamp(options.pageSize ?? 100, 1, 500),
        page_token: options.pageToken,
      },
      body,
    })
    return {
      items: data.items ?? [],
      total: data.total,
      hasMore: Boolean(data.has_more),
      pageToken: data.page_token,
    }
  }

  async searchAllRecords(
    appToken: string,
    tableId: string,
    body: FeishuRecordSearchBody,
    options: { maxRecords?: number; pageSize?: number } = {},
  ): Promise<{ items: FeishuRecordInfo[]; total?: number }> {
    const maxRecords = options.maxRecords ?? Infinity
    const pageSize = clamp(options.pageSize ?? 500, 1, 500)
    const items: FeishuRecordInfo[] = []
    let pageToken: string | undefined
    let total: number | undefined
    do {
      const page = await this.searchRecords(appToken, tableId, body, {
        pageSize: Math.min(pageSize, maxRecords - items.length),
        pageToken,
      })
      items.push(...page.items)
      if (total === undefined) total = page.total
      pageToken = page.hasMore && items.length < maxRecords ? page.pageToken : undefined
      if (page.items.length === 0) break
    } while (pageToken)
    return { items, total }
  }

  async batchCreateRecords(
    appToken: string,
    tableId: string,
    records: Array<{ fields: Record<string, any> }>,
    clientToken?: string,
  ): Promise<FeishuRecordInfo[]> {
    if (records.length === 0) return []
    const data = await this.request<{ records?: FeishuRecordInfo[] }>({
      method: 'POST',
      path: `/open-apis/bitable/v1/apps/${encodeURIComponent(appToken)}/tables/${encodeURIComponent(tableId)}/records/batch_create`,
      query: { client_token: clientToken ?? randomUUID() },
      body: { records },
      idempotent: Boolean(clientToken),
    })
    return data?.records ?? []
  }

  async createRecord(
    appToken: string,
    tableId: string,
    fields: Record<string, any>,
    clientToken?: string,
  ): Promise<FeishuRecordInfo> {
    const data = await this.request<{ record?: FeishuRecordInfo }>({
      method: 'POST',
      path: `/open-apis/bitable/v1/apps/${encodeURIComponent(appToken)}/tables/${encodeURIComponent(tableId)}/records`,
      query: { client_token: clientToken ?? randomUUID() },
      body: { fields },
      idempotent: Boolean(clientToken),
    })
    if (!data?.record) throw new BiteDBError('Feishu createRecord returned no record')
    return data.record
  }

  async updateRecord(
    appToken: string,
    tableId: string,
    recordId: string,
    fields: Record<string, any>,
  ): Promise<FeishuRecordInfo> {
    const data = await this.request<{ record?: FeishuRecordInfo }>({
      method: 'PUT',
      path: `/open-apis/bitable/v1/apps/${encodeURIComponent(appToken)}/tables/${encodeURIComponent(tableId)}/records/${encodeURIComponent(recordId)}`,
      body: { fields },
      idempotent: true,
    })
    if (!data?.record) throw new BiteDBError(`Feishu updateRecord(${recordId}) returned no record`)
    return data.record
  }

  async batchUpdateRecords(
    appToken: string,
    tableId: string,
    records: Array<{ record_id: string; fields: Record<string, any> }>,
  ): Promise<FeishuRecordInfo[]> {
    if (records.length === 0) return []
    const data = await this.request<{ records?: FeishuRecordInfo[] }>({
      method: 'POST',
      path: `/open-apis/bitable/v1/apps/${encodeURIComponent(appToken)}/tables/${encodeURIComponent(tableId)}/records/batch_update`,
      body: { records },
      idempotent: true,
    })
    return data?.records ?? []
  }

  async getRecord(appToken: string, tableId: string, recordId: string): Promise<FeishuRecordInfo> {
    const data = await this.request<{ record?: FeishuRecordInfo }>({
      method: 'GET',
      path: `/open-apis/bitable/v1/apps/${encodeURIComponent(appToken)}/tables/${encodeURIComponent(tableId)}/records/${encodeURIComponent(recordId)}`,
    })
    if (!data?.record) throw new BiteDBError(`Feishu getRecord(${recordId}) returned no record`)
    return data.record
  }

  async deleteRecord(appToken: string, tableId: string, recordId: string): Promise<void> {
    await this.request({
      method: 'DELETE',
      path: `/open-apis/bitable/v1/apps/${encodeURIComponent(appToken)}/tables/${encodeURIComponent(tableId)}/records/${encodeURIComponent(recordId)}`,
      idempotent: true,
    })
  }

  async batchDeleteRecords(appToken: string, tableId: string, recordIds: string[]): Promise<void> {
    if (recordIds.length === 0) return
    await this.request({
      method: 'POST',
      path: `/open-apis/bitable/v1/apps/${encodeURIComponent(appToken)}/tables/${encodeURIComponent(tableId)}/records/batch_delete`,
      body: { records: recordIds },
      idempotent: true,
    })
  }
}
