import {
  BlobManager,
  isAttachmentMetadata,
  isBlobLike,
  isSpillMarkerString,
  makeJsonEnvelope,
  parseJsonEnvelope,
  parseSpillMarker,
  safeJsonStringify,
} from './blob.js'
import { ValidationError } from './errors.js'
import type { AttachmentMetadata, Document, HydrateMode } from './types.js'
import type { StoredField, TableMeta } from './meta.js'
import { isBinary, isPlainObject, setOwn } from './util.js'

export interface LinkResolver {
  /** Map logical link values to physical Bitable record ids. */
  resolveOutgoing(field: StoredField, ids: string[]): Promise<string[]>
  /** Map physical Bitable record ids back to logical document ids. */
  resolveIncoming(field: StoredField, recordIds: string[]): Promise<string[]>
}

export interface CodecContext {
  blobs: BlobManager
  baseToken: string
  hydrate?: HydrateMode
  links?: LinkResolver
}

const MARKER_LIKE = /^\{"\$bitedb_(blob|json)":/

/**
 * Encode a document (or a patch) into Bitable record fields.
 * Unknown keys are ignored here; callers must have ensured DDL first.
 */
export async function encodeDocument(
  doc: Document,
  meta: TableMeta,
  ctx: CodecContext,
  options: { skipUndefined?: boolean; fieldsToEncode?: string[] } = {},
): Promise<Record<string, any>> {
  const out: Record<string, any> = {}
  const only = options.fieldsToEncode ? new Set(options.fieldsToEncode) : undefined
  const byKey = new Map(meta.fields.map((field) => [field.key, field]))

  for (const [key, value] of Object.entries(doc)) {
    if (key === '_id') continue
    if (only && !only.has(key)) continue
    const field = byKey.get(key)
    if (!field) {
      // There is no physical field to write. Null/undefined values carry no
      // type information, so treat them as a no-op (e.g. $unset of an unknown
      // key); any concrete value is a programming error.
      if (value === undefined || value === null) continue
      throw new ValidationError(`Unknown field "${key}" for table "${meta.name}"`)
    }
    if (field.kind === 'computed') continue
    if (value === undefined && options.skipUndefined !== false) continue
    const encoded = await encodeFieldValue(field, value, ctx)
    if (encoded !== undefined) setOwn(out, field.name, encoded)
  }
  return out
}

export async function encodeFieldValue(
  field: StoredField,
  value: unknown,
  ctx: CodecContext,
): Promise<unknown> {
  if (value === null) return null
  if (value === undefined) return undefined

  if (field.kind === 'computed') {
    throw new ValidationError(`Field "${field.key}" is computed and cannot be written`)
  }

  const spillThreshold = field.spill ?? 0
  const spillIfLarge = (bytes: number) => spillThreshold > 0 && bytes >= spillThreshold

  switch (field.kind) {
    case 'text': {
      if (isBinary(value) || isBlobLike(value)) {
        return ctx.blobs.spill(ctx.baseToken, value)
      }
      if (typeof value === 'bigint') value = value.toString()
      if (typeof value === 'string') {
        if (spillIfLarge(Buffer.byteLength(value))) {
          return ctx.blobs.spill(ctx.baseToken, value)
        }
        // Prevent user strings from being mistaken for internal markers.
        return MARKER_LIKE.test(value) ? makeJsonEnvelope(value) : value
      }
      if (!field.json && !field.inferred) {
        // Explicit text field: store a textual representation.
        return typeof value === 'object' ? safeJsonStringify(value) : String(value)
      }
      if (spillIfLarge(estimateBytes(value))) {
        return ctx.blobs.spill(ctx.baseToken, value)
      }
      return makeJsonEnvelope(value)
    }

    case 'json': {
      if (isBinary(value) || isBlobLike(value)) return ctx.blobs.spill(ctx.baseToken, value)
      const json = safeJsonStringify(value)
      if (spillIfLarge(Buffer.byteLength(json))) return ctx.blobs.spill(ctx.baseToken, value)
      return json
    }

    case 'number': {
      const number = typeof value === 'string' ? Number(value) : value
      if (typeof number === 'bigint') {
        if (number > BigInt(Number.MAX_SAFE_INTEGER) || number < BigInt(Number.MIN_SAFE_INTEGER)) {
          throw new ValidationError(
            `Field "${field.key}" is a number field but the bigint exceeds JavaScript's safe integer range`,
          )
        }
        return Number(number)
      }
      if (typeof number !== 'number' || !Number.isFinite(number)) {
        throw new ValidationError(`Field "${field.key}" expects a finite number`)
      }
      if (Number.isInteger(number) && !Number.isSafeInteger(number)) {
        throw new ValidationError(
          `Field "${field.key}" exceeds JavaScript's safe integer range. Use a text/json field to preserve it.`,
        )
      }
      return number
    }

    case 'boolean': {
      if (typeof value === 'boolean') return value
      if (value === 1 || value === 0) return value === 1
      if (value === 'true' || value === 'false') return value === 'true'
      throw new ValidationError(`Field "${field.key}" expects a boolean`)
    }

    case 'date': {
      if (value instanceof Date) {
        if (Number.isNaN(value.getTime())) throw new ValidationError(`Field "${field.key}" has an invalid Date`)
        return value.getTime()
      }
      if (typeof value === 'number' && Number.isFinite(value)) return value
      if (typeof value === 'string') {
        const parsed = Date.parse(value)
        if (!Number.isNaN(parsed)) return parsed
      }
      throw new ValidationError(`Field "${field.key}" expects a Date, epoch milliseconds, or ISO string`)
    }

    case 'select': {
      if (typeof value !== 'string') throw new ValidationError(`Field "${field.key}" expects a string option`)
      return value
    }

    case 'multiSelect': {
      if (typeof value === 'string') return [value]
      if (!Array.isArray(value) || value.some((item) => typeof item !== 'string')) {
        throw new ValidationError(`Field "${field.key}" expects an array of option strings`)
      }
      return value
    }

    case 'attachment':
    case 'blob': {
      return ctx.blobs.encodeAttachment(ctx.baseToken, value)
    }

    case 'link': {
      const ids = normalizeLinkInput(value)
      if (field.multiple === false && ids.length > 1) {
        throw new ValidationError(`Link field "${field.key}" accepts at most one document id`)
      }
      if (!ctx.links) {
        throw new ValidationError(`Link field "${field.key}" cannot be written without a link resolver`)
      }
      const physical = await ctx.links.resolveOutgoing(field, ids)
      return physical
    }

    case 'raw':
      return value

    default:
      throw new ValidationError(`Unsupported field kind "${field.kind}"`)
  }
}

/** Decode Bitable record fields back into a document. */
export async function decodeRecord(
  rawFields: Record<string, any> | undefined,
  meta: TableMeta,
  ctx: CodecContext,
  recordId: string,
): Promise<Document> {
  const fields = rawFields ?? {}
  const out: Document = {}
  let logicalId: string | undefined

  for (const field of meta.fields) {
    const raw = fields[field.name]
    if (field.key === '_id') {
      const decoded = typeof raw === 'string' && raw.length > 0 ? raw : recordId
      logicalId = decoded
      setOwn(out, '_id', decoded)
      continue
    }
    if (raw === undefined) continue
    const value = await decodeFieldValue(field, raw, ctx)
    if (value !== undefined) setOwn(out, field.key, value)
  }

  if (out._id === undefined) out._id = recordId
  return out
}

export async function decodeFieldValue(
  field: StoredField,
  raw: unknown,
  ctx: CodecContext,
): Promise<unknown> {
  if (raw === null || raw === undefined) return undefined

  if (field.kind !== 'raw') {
    const marker = parseSpillMarker(raw)
    if (marker) return ctx.blobs.materializeMarker(marker, ctx.hydrate)
    const envelope = parseJsonEnvelope(raw)
    if (envelope.found) return envelope.value
  }

  switch (field.kind) {
    case 'text': {
      if (Array.isArray(raw)) {
        // text_field_as_array=true responses.
        return raw.map((item) => (typeof item === 'string' ? item : item?.text ?? '')).join('')
      }
      return typeof raw === 'string' ? raw : raw
    }

    case 'json': {
      if (typeof raw !== 'string') return raw
      try {
        return JSON.parse(raw)
      } catch {
        return raw
      }
    }

    case 'number': {
      const n = typeof raw === 'number' ? raw : Number(raw)
      return Number.isNaN(n) ? raw : n
    }

    case 'boolean':
      return raw === true || raw === 'true' || raw === 1

    case 'date': {
      if (raw instanceof Date) return raw
      if (typeof raw === 'number') return new Date(raw)
      if (typeof raw === 'string') {
        const parsed = Date.parse(raw)
        if (!Number.isNaN(parsed)) return new Date(parsed)
      }
      return raw
    }

    case 'select':
      return raw

    case 'multiSelect':
      return Array.isArray(raw) ? raw : [raw]

    case 'attachment': {
      const list = normalizeAttachments(raw)
      return list.map((metadata) => ctx.blobs.ref(metadata))
    }

    case 'blob': {
      const list = normalizeAttachments(raw)
      if (list.length === 0) return []
      const decoded = await Promise.all(
        list.map((metadata) =>
          ctx.blobs.decodeAttachmentMetadata(metadata, { semanticBlob: true, hydrate: ctx.hydrate }),
        ),
      )
      return decoded.length === 1 ? decoded[0] : decoded
    }

    case 'link': {
      const ids = extractLinkIds(raw)
      if (ids.length === 0) return field.multiple === false ? null : []
      const logical = ctx.links ? await ctx.links.resolveIncoming(field, ids) : ids
      return field.multiple === false ? (logical[0] ?? null) : logical
    }

    case 'computed': {
      return decodeComputed(field, raw)
    }

    case 'raw':
      return raw

    default:
      return raw
  }
}

function decodeComputed(field: StoredField, raw: unknown): unknown {
  switch (field.resultType) {
    case 'number': {
      const n = typeof raw === 'number' ? raw : Number(raw)
      return Number.isNaN(n) ? raw : n
    }
    case 'boolean':
      return raw === true || raw === 'true' || raw === 1
    case 'date': {
      if (typeof raw === 'number') return new Date(raw)
      if (typeof raw === 'string') {
        const parsed = Date.parse(raw)
        if (!Number.isNaN(parsed)) return new Date(parsed)
      }
      return raw
    }
    case 'text':
      if (Array.isArray(raw)) return raw.map((item) => item?.text ?? item).join('')
      return typeof raw === 'string' ? raw : String(raw)
    case 'json':
      if (typeof raw !== 'string') return raw
      try {
        return JSON.parse(raw)
      } catch {
        return raw
      }
    default:
      if (isSpillMarkerString(raw)) return raw
      return raw
  }
}

export function normalizeLinkInput(value: unknown): string[] {
  if (value === null || value === undefined) return []
  if (typeof value === 'string') return [value]
  if (Array.isArray(value)) {
    return value.flatMap((item) => {
      if (typeof item === 'string') return [item]
      if (item && typeof item === 'object') {
        if (typeof item._id === 'string') return [item._id]
        if (typeof item.record_id === 'string') return [item.record_id]
        if (typeof item.id === 'string') return [item.id]
      }
      throw new ValidationError('Link values must be document ids, record ids, or objects with _id')
    })
  }
  if (typeof value === 'object' && value !== null) {
    const id = (value as any)._id ?? (value as any).record_id ?? (value as any).id
    if (typeof id === 'string') return [id]
  }
  throw new ValidationError('Link values must be strings, arrays, or objects with _id')
}

export function extractLinkIds(raw: unknown): string[] {
  if (raw === null || raw === undefined) return []
  if (typeof raw === 'string') return [raw]
  if (Array.isArray(raw)) {
    return raw.flatMap((item) => extractLinkIds(item))
  }
  if (typeof raw === 'object') {
    const obj = raw as Record<string, unknown>
    if (Array.isArray(obj.link_record_ids)) return obj.link_record_ids.map(String)
    if (Array.isArray(obj.record_ids)) return obj.record_ids.map(String)
    if (typeof obj.record_id === 'string') return [obj.record_id]
    if (typeof obj.id === 'string') return [obj.id]
  }
  return []
}

export function normalizeAttachments(raw: unknown): AttachmentMetadata[] {
  if (!raw) return []
  const list = Array.isArray(raw) ? raw : [raw]
  const result: AttachmentMetadata[] = []
  for (const item of list) {
    if (isAttachmentMetadata(item)) {
      result.push({
        file_token: item.file_token,
        name: String(item.name ?? ''),
        size: Number(item.size ?? 0),
        type: item.type,
        url: item.url,
        tmp_url: item.tmp_url,
      })
    }
  }
  return result
}

function estimateBytes(value: unknown): number {
  if (typeof value === 'string') return Buffer.byteLength(value)
  if (isBinary(value)) return value.byteLength
  if (isPlainObject(value) || Array.isArray(value)) {
    try {
      return Buffer.byteLength(JSON.stringify(value))
    } catch {
      return 0
    }
  }
  return Buffer.byteLength(String(value))
}
