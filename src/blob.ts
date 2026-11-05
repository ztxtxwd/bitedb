import { ValidationError } from './errors.js'
import type { FeishuClient } from './feishu.js'
import type { Logger } from './logger.js'
import type { AttachmentMetadata, HydrateMode } from './types.js'
import { isBinary, sha256, toBuffer } from './util.js'

export type BlobKind = 'json' | 'text' | 'binary'

export interface UploadedBlob {
  token: string
  name: string
  size: number
  kind: BlobKind
}

export interface SpillMarker {
  v: 1
  kind: BlobKind
  file_token: string
  name: string
  size: number
}

const SPILL_PREFIX = '{"$bitedb_blob":'
const ENVELOPE_PREFIX = '{"$bitedb_json":'
const BLOB_NAME_RE = /^bitedb\.v1\.(json|text|binary)\.([a-f0-9]{8,64})(?:\.[A-Za-z0-9._-]{1,40})?\.(json|txt|bin)$/

export function isSpillMarkerString(value: unknown): value is string {
  return typeof value === 'string' && value.startsWith(SPILL_PREFIX)
}

export function parseSpillMarker(value: unknown): SpillMarker | undefined {
  if (!isSpillMarkerString(value)) return undefined
  try {
    const parsed = JSON.parse(value)
    const marker = parsed?.$bitedb_blob
    if (
      marker &&
      marker.v === 1 &&
      (marker.kind === 'json' || marker.kind === 'text' || marker.kind === 'binary') &&
      typeof marker.file_token === 'string'
    ) {
      return {
        v: 1,
        kind: marker.kind,
        file_token: marker.file_token,
        name: String(marker.name ?? ''),
        size: Number(marker.size ?? 0),
      }
    }
  } catch {
    /* not a marker */
  }
  return undefined
}

export function makeSpillMarker(marker: SpillMarker): string {
  return `${SPILL_PREFIX}${JSON.stringify(marker)}}`
}

export function makeJsonEnvelope(value: unknown): string {
  return `${ENVELOPE_PREFIX}${JSON.stringify(value)}}`
}

export function parseJsonEnvelope(value: unknown): { found: boolean; value?: unknown } {
  if (typeof value !== 'string' || !value.startsWith(ENVELOPE_PREFIX)) return { found: false }
  try {
    const parsed = JSON.parse(value)
    if (parsed && Object.prototype.hasOwnProperty.call(parsed, '$bitedb_json')) {
      return { found: true, value: parsed.$bitedb_json }
    }
  } catch {
    /* not an envelope */
  }
  return { found: false }
}

export class BlobRef {
  readonly fileToken: string
  readonly name: string
  readonly size: number
  readonly type?: string
  readonly url?: string
  readonly tmpUrl?: string
  private readonly manager: BlobManager

  constructor(manager: BlobManager, metadata: AttachmentMetadata) {
    this.manager = manager
    this.fileToken = metadata.file_token
    this.name = metadata.name ?? ''
    this.size = Number(metadata.size ?? 0)
    this.type = metadata.type
    this.url = metadata.url
    this.tmpUrl = metadata.tmp_url
  }

  async bytes(): Promise<Buffer> {
    return this.manager.download(this.fileToken, this.tmpUrl ?? this.url)
  }

  async arrayBuffer(): Promise<ArrayBuffer> {
    const buffer = await this.bytes()
    return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength) as ArrayBuffer
  }

  async text(): Promise<string> {
    return (await this.bytes()).toString('utf8')
  }

  async json<T = unknown>(): Promise<T> {
    return JSON.parse(await this.text()) as T
  }

  get kind(): BlobKind | undefined {
    return blobKindFromName(this.name)
  }

  toJSON(): Record<string, unknown> {
    return {
      $bitedb: 'blob',
      file_token: this.fileToken,
      name: this.name,
      size: this.size,
      type: this.type,
    }
  }

  toString(): string {
    return `[BlobRef ${this.name || this.fileToken} (${this.size} bytes)]`
  }

  get [Symbol.toStringTag](): string {
    return 'BlobRef'
  }
}

export function blobKindFromName(name: string | undefined): BlobKind | undefined {
  if (!name) return undefined
  const match = BLOB_NAME_RE.exec(name)
  if (!match) return undefined
  return match[1] as BlobKind
}

export interface BlobManagerOptions {
  hydrate: HydrateMode
  logger: Logger
}

export class BlobManager {
  private readonly client: FeishuClient
  private readonly hydrate: HydrateMode
  private readonly logger: Logger
  private readonly uploadCache = new Map<string, UploadedBlob>()
  private readonly downloadCache = new Map<string, Promise<Buffer>>()
  private readonly maxCacheEntries = 128

  constructor(client: FeishuClient, options: BlobManagerOptions) {
    this.client = client
    this.hydrate = options.hydrate
    this.logger = options.logger
  }

  get hydrateMode(): HydrateMode {
    return this.hydrate
  }

  async uploadBytes(
    baseToken: string,
    bytes: Buffer,
    kind: BlobKind,
    nameHint?: string,
  ): Promise<UploadedBlob> {
    const digest = sha256(bytes)
    const cacheKey = `${baseToken}:${kind}:${digest}`
    const cached = this.uploadCache.get(cacheKey)
    if (cached) return cached

    const name = makeBlobFileName(kind, digest, nameHint)
    const uploaded = await this.client.uploadMedia({ parentToken: baseToken, bytes, fileName: name })
    const result: UploadedBlob = {
      token: uploaded.fileToken,
      name,
      size: bytes.byteLength,
      kind,
    }
    this.remember(this.uploadCache, cacheKey, result)
    this.logger.debug?.(`[bitedb] uploaded ${kind} blob ${name} (${bytes.byteLength} bytes)`)
    return result
  }

  async spill(baseToken: string, value: unknown, nameHint?: string): Promise<string> {
    const { bytes, kind } = await serializeBlobValueAsync(value)
    const uploaded = await this.uploadBytes(baseToken, bytes, kind, nameHint)
    return makeSpillMarker({
      v: 1,
      kind,
      file_token: uploaded.token,
      name: uploaded.name,
      size: uploaded.size,
    })
  }

  async encodeAttachment(
    baseToken: string,
    value: unknown,
    nameHint?: string,
  ): Promise<Array<{ file_token: string }>> {
    const values = Array.isArray(value) ? value : [value]
    const out: Array<{ file_token: string }> = []
    for (const item of values) {
      if (item === null || item === undefined) continue
      if (item instanceof BlobRef) {
        out.push({ file_token: item.fileToken })
        continue
      }
      if (isAttachmentMetadata(item)) {
        out.push({ file_token: item.file_token })
        continue
      }
      if (typeof item === 'string' && /^(box|file)[A-Za-z0-9_-]{8,}$/.test(item)) {
        // Caller passed an existing file_token.
        out.push({ file_token: item })
        continue
      }
      if (typeof item === 'object' && item !== null && 'file_token' in (item as any)) {
        out.push({ file_token: String((item as any).file_token) })
        continue
      }
      const { bytes, kind } = await serializeBlobValueAsync(item)
      const uploaded = await this.uploadBytes(baseToken, bytes, kind, nameHint)
      out.push({ file_token: uploaded.token })
    }
    return out
  }

  ref(metadata: AttachmentMetadata): BlobRef {
    return new BlobRef(this, metadata)
  }

  async download(fileToken: string, url?: string): Promise<Buffer> {
    const cached = this.downloadCache.get(fileToken)
    if (cached) return cached
    const promise = (async () => {
      if (url) {
        try {
          return await this.client.downloadUrl(url)
        } catch (err) {
          this.logger.debug?.(`[bitedb] signed media URL failed, falling back to file token:`, err)
        }
      }
      return this.client.downloadMedia(fileToken)
    })()
    this.remember(this.downloadCache, fileToken, promise)
    try {
      return await promise
    } catch (err) {
      this.downloadCache.delete(fileToken)
      throw err
    }
  }

  async materializeMarker(marker: SpillMarker, hydrate: HydrateMode = this.hydrate): Promise<unknown> {
    if (hydrate === 'never') {
      return this.ref({
        file_token: marker.file_token,
        name: marker.name,
        size: marker.size,
        type: marker.kind,
      })
    }
    if (marker.kind === 'binary' && hydrate !== 'eager') {
      return this.ref({
        file_token: marker.file_token,
        name: marker.name,
        size: marker.size,
        type: 'application/octet-stream',
      })
    }
    const bytes = await this.download(marker.file_token)
    if (marker.kind === 'json') {
      try {
        return JSON.parse(bytes.toString('utf8'))
      } catch (err) {
        throw new ValidationError(`Failed to parse spilled JSON blob ${marker.file_token}`, { cause: err })
      }
    }
    if (marker.kind === 'text') return bytes.toString('utf8')
    return bytes
  }

  async decodeAttachmentMetadata(
    metadata: AttachmentMetadata,
    options: { semanticBlob: boolean; hydrate?: HydrateMode },
  ): Promise<unknown> {
    const hydrate = options.hydrate ?? this.hydrate
    if (!options.semanticBlob) return this.ref(metadata)
    const kind = blobKindFromName(metadata.name) ?? 'binary'
    if (hydrate === 'never') return this.ref(metadata)
    if (kind === 'binary' && hydrate !== 'eager') return this.ref(metadata)
    const bytes = await this.download(metadata.file_token, metadata.tmp_url ?? metadata.url)
    if (kind === 'json') {
      try {
        return JSON.parse(bytes.toString('utf8'))
      } catch (err) {
        throw new ValidationError(`Failed to parse JSON attachment ${metadata.file_token}`, { cause: err })
      }
    }
    if (kind === 'text') return bytes.toString('utf8')
    return bytes
  }

  private remember<T>(map: Map<string, T>, key: string, value: T): void {
    if (map.size >= this.maxCacheEntries) {
      const oldest = map.keys().next().value
      if (oldest !== undefined) map.delete(oldest)
    }
    map.set(key, value)
  }
}

export function isAttachmentMetadata(value: unknown): value is AttachmentMetadata {
  return Boolean(value && typeof value === 'object' && typeof (value as any).file_token === 'string')
}

export function isBlobLike(value: unknown): boolean {
  return Boolean(
    value &&
      typeof value === 'object' &&
      typeof (value as any).arrayBuffer === 'function' &&
      !isBinary(value) &&
      !Array.isArray(value),
  )
}

export function serializeBlobValue(value: unknown): { bytes: Buffer; kind: BlobKind } {
  if (isBinary(value)) return { bytes: toBuffer(value), kind: 'binary' }
  if (typeof value === 'string') return { bytes: Buffer.from(value, 'utf8'), kind: 'text' }
  if (value && typeof value === 'object' && typeof (value as any).arrayBuffer === 'function' && !Array.isArray(value)) {
    // Blob/File-like objects are handled asynchronously in serializeBlobValueAsync.
    throw new ValidationError('Blob-like values must be encoded asynchronously')
  }
  try {
    return { bytes: Buffer.from(JSON.stringify(value, jsonReplacer), 'utf8'), kind: 'json' }
  } catch (err) {
    throw new ValidationError('Value cannot be serialized to JSON for blob storage', { cause: err })
  }
}

export async function serializeBlobValueAsync(value: unknown): Promise<{ bytes: Buffer; kind: BlobKind }> {
  if (isBlobLike(value)) {
    const bytes = Buffer.from(await (value as any).arrayBuffer())
    return { bytes, kind: 'binary' }
  }
  return serializeBlobValue(value)
}

export function jsonReplacer(_key: string, value: unknown): unknown {
  if (typeof value === 'bigint') return value.toString()
  return value
}

export function safeJsonStringify(value: unknown): string {
  try {
    const json = JSON.stringify(value, jsonReplacer)
    if (json === undefined) throw new Error('value produced undefined')
    return json
  } catch (err) {
    throw new ValidationError('Value cannot be serialized to JSON', { cause: err })
  }
}

function makeBlobFileName(kind: BlobKind, digest: string, hint?: string): string {
  const shortDigest = digest.slice(0, 16)
  const ext = kind === 'json' ? 'json' : kind === 'text' ? 'txt' : 'bin'
  const safeHint =
    hint && /^[A-Za-z0-9._-]{1,40}$/.test(hint)
      ? hint.replace(/[^A-Za-z0-9._-]/g, '_')
      : ''
  const middle = safeHint ? `${shortDigest}.${safeHint}` : shortDigest
  return `bitedb.v1.${kind}.${middle}.${ext}`
}

