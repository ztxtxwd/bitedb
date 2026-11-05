import { ValidationError } from './errors.js'
import type { StoredField } from './meta.js'
import type {
  FieldDefinition,
  ResultType,
  SchemaDefinition,
  SchemaFieldType,
  SelectOption,
} from './types.js'
import { isBlobLike } from './blob.js'
import { byteLength, isBinary, isPlainObject, isSafePropertyKey } from './util.js'

export interface SchemaOptions {
  blobThresholdBytes: number
}

const DATE_PROPERTY = { date_formatter: 'yyyy/MM/dd HH:mm', auto_fill: false }

const RESULT_DATA_TYPES: Record<ResultType, number> = {
  text: 1,
  number: 2,
  boolean: 7,
  date: 5,
  json: 1,
}

const FEISHU_TYPE_BY_KIND: Record<string, number> = {
  text: 1,
  json: 1,
  number: 2,
  boolean: 7,
  date: 5,
  select: 3,
  multiSelect: 4,
  attachment: 17,
  blob: 17,
  link: 21,
  computed: 20,
}

export function normalizeSchema(schema: SchemaDefinition | undefined, options: SchemaOptions): StoredField[] {
  const fields: StoredField[] = []
  const keys = new Set<string>()
  const names = new Set<string>()

  for (const [key, raw] of Object.entries(schema ?? {})) {
    if (key === '_id') continue
    if (!key) throw new ValidationError('Field keys cannot be empty')
    if (keys.has(key)) throw new ValidationError(`Duplicate field key: ${key}`)
    const def: FieldDefinition = typeof raw === 'string' ? { type: raw } : raw
    if (!def || typeof def !== 'object' || !def.type) {
      throw new ValidationError(`Field "${key}" must be a type name or a FieldDefinition object`)
    }
    const field = normalizeField(key, def, options)
    if (names.has(field.name)) {
      throw new ValidationError(`Duplicate physical field name "${field.name}" (from key "${key}")`)
    }
    keys.add(key)
    names.add(field.name)
    fields.push(field)
  }

  if (names.has('_id')) {
    throw new ValidationError('"_id" is reserved for the document id; do not map another field to it')
  }

  fields.unshift({
    key: '_id',
    name: '_id',
    kind: 'text',
    feishuType: 1,
    primary: true,
    description: 'bitedb document id',
  })
  return fields
}

export function normalizeField(key: string, def: FieldDefinition, options: SchemaOptions): StoredField {
  const type = normalizeTypeName(def.type)
  const name = def.name ?? key
  if (!isSafePropertyKey(key) || !isSafePropertyKey(name)) {
    throw new ValidationError(`Unsafe field key or physical name "${key}"`)
  }
  if (!name) throw new ValidationError(`Field "${key}" has an empty physical name`)
  if (name.length > 100) throw new ValidationError(`Field "${key}" physical name exceeds 100 characters`)
  if (def.feishuType !== undefined && type !== 'raw') {
    throw new ValidationError('`feishuType` is only supported with type "raw"')
  }

  const base: StoredField = {
    key,
    name,
    kind: type,
    feishuType: def.feishuType ?? FEISHU_TYPE_BY_KIND[type] ?? 1,
    description: def.description,
    primary: false,
  }

  const spillThreshold =
    def.spill === false
      ? 0
      : typeof def.spill === 'number'
        ? def.spill
        : type === 'json'
          ? options.blobThresholdBytes
          : 0

  switch (type) {
    case 'text':
      base.spill = spillThreshold || undefined
      break
    case 'json':
      base.json = true
      base.spill = spillThreshold || undefined
      break
    case 'number':
    case 'boolean':
    case 'blob':
    case 'attachment':
      break
    case 'date':
      base.property = { ...DATE_PROPERTY, ...(def.property ?? {}) }
      break
    case 'select':
    case 'multiSelect':
      base.options = normalizeOptions(def.options)
      base.property = { options: base.options }
      break
    case 'link': {
      if (!def.table) throw new ValidationError(`Link field "${key}" requires a target table`)
      base.target = def.table
      base.multiple = def.multiple ?? true
      base.duplex = def.duplex ?? true
      base.feishuType = base.duplex ? 21 : 18
      base.backFieldName = def.backFieldName
      break
    }
    case 'computed': {
      if (!def.expr) throw new ValidationError(`Computed field "${key}" requires an expression`)
      base.expr = def.expr
      base.resultType = def.resultType
      base.property = { ...(def.property ?? {}) }
      break
    }
    case 'raw': {
      if (!def.feishuType) throw new ValidationError(`Raw field "${key}" requires feishuType`)
      base.feishuType = def.feishuType
      base.property = def.property ? { ...def.property } : undefined
      base.uiType = def.uiType
      break
    }
    default:
      throw new ValidationError(`Unsupported field type "${String(type)}" for "${key}"`)
  }

  if (def.property) base.property = { ...(base.property ?? {}), ...def.property }
  if (def.uiType) base.uiType = def.uiType
  return base
}

export function inferField(key: string, value: unknown, options: SchemaOptions): StoredField | undefined {
  if (value === undefined || value === null) return undefined
  if (!isSafePropertyKey(key)) throw new ValidationError(`Unsafe field key "${key}"`)

  if (isBinary(value) || isBlobLike(value)) {
    return { key, name: key, kind: 'blob', feishuType: 17, inferred: true }
  }
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) throw new ValidationError(`Field "${key}" contains an invalid Date`)
    return {
      key,
      name: key,
      kind: 'date',
      feishuType: 5,
      property: { ...DATE_PROPERTY },
      inferred: true,
    }
  }
  if (typeof value === 'boolean') {
    return { key, name: key, kind: 'boolean', feishuType: 7, inferred: true }
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new ValidationError(`Field "${key}" contains a non-finite number`)
    if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
      throw new ValidationError(
        `Field "${key}" exceeds JavaScript's safe integer range; store it as a string or bigint instead`,
      )
    }
    return { key, name: key, kind: 'number', feishuType: 2, inferred: true }
  }
  if (typeof value === 'bigint') {
    return { key, name: key, kind: 'text', feishuType: 1, inferred: true }
  }
  if (typeof value === 'string') {
    return {
      key,
      name: key,
      kind: 'text',
      feishuType: 1,
      spill: options.blobThresholdBytes || undefined,
      inferred: true,
    }
  }
  if (typeof value === 'function' || typeof value === 'symbol') {
    throw new ValidationError(`Field "${key}" has an unsupported value type: ${typeof value}`)
  }
  if (isPlainObject(value) || Array.isArray(value)) {
    return {
      key,
      name: key,
      kind: 'json',
      feishuType: 1,
      json: true,
      spill: options.blobThresholdBytes || undefined,
      inferred: true,
    }
  }
  // Class instances and other objects: persist as JSON text.
  return {
    key,
    name: key,
    kind: 'json',
    feishuType: 1,
    json: true,
    spill: options.blobThresholdBytes || undefined,
    inferred: true,
  }
}

export function feishuFieldPayload(
  field: StoredField,
  options: { tableId?: string; targetTableId?: string; formulaExpression?: string } = {},
): {
  field_name: string
  type: number
  ui_type?: string
  property?: Record<string, unknown>
  description?: { text?: string; disable_sync?: boolean }
} {
  let property: Record<string, unknown> | undefined = field.property ? { ...field.property } : undefined

  if (field.kind === 'link') {
    if (!options.targetTableId) {
      throw new ValidationError(`Cannot create link field "${field.key}" without a target table id`)
    }
    property = {
      table_id: options.targetTableId,
      multiple: field.multiple ?? true,
      ...(field.duplex && field.backFieldName ? { back_field_name: field.backFieldName } : {}),
      ...(property ?? {}),
    }
  }

  if (field.kind === 'computed') {
    const expression = options.formulaExpression ?? field.expr
    if (!expression) throw new ValidationError(`Computed field "${field.key}" has no expression`)
    property = {
      ...(property ?? {}),
      formula_expression: expression,
    }
    if (field.resultType) {
      const dataType = RESULT_DATA_TYPES[field.resultType]
      property.type = {
        data_type: dataType,
        ...(field.resultType === 'date'
          ? { ui_property: { ui_type: 'DateTime', date_formatter: 'yyyy/MM/dd HH:mm' } }
          : field.resultType === 'number'
            ? { ui_property: { ui_type: 'Number' } }
            : {}),
      }
    }
  }

  if (field.kind === 'select' || field.kind === 'multiSelect') {
    property = {
      ...(property ?? {}),
      options: (field.options ?? []).map((option) => ({ name: option.name, ...(option.color !== undefined ? { color: option.color } : {}) })),
    }
  }

  return {
    field_name: field.name,
    type: field.feishuType,
    ...(field.uiType ? { ui_type: field.uiType } : {}),
    ...(property ? { property } : {}),
    ...(field.description ? { description: { text: field.description, disable_sync: false } } : {}),
  }
}

/** Thrown when a formula references an unknown field. */
function formulaReference(tableId: string, field: StoredField): string {
  if (!field.fieldId) {
    throw new ValidationError(
      `Formula references field "${field.key}", but its Bitable field id is not available yet`,
    )
  }
  return `bitable::$table[${tableId}].$field[${field.fieldId}]`
}

const IDENTIFIER_START = /[A-Za-z_一-鿿]/
const IDENTIFIER_PART = /[A-Za-z0-9_一-鿿]/

/**
 * Compile a developer-friendly formula (`price * count`) into Bitable's
 * machine formula syntax (`bitable::$table[tbl].$field[fld] * ...`).
 *
 * Field names containing spaces or symbols can be referenced as `{order total}`.
 * Existing `bitable::$table[...]` expressions are passed through untouched.
 */
export function compileFormula(expression: string, tableId: string, fields: StoredField[]): string {
  const trimmed = expression.trim()
  if (!trimmed) throw new ValidationError('Formula expression is empty')
  if (trimmed.includes('bitable::$')) return trimmed

  const byKey = new Map<string, StoredField>()
  for (const field of fields) {
    byKey.set(field.key, field)
    byKey.set(field.name, field)
  }

  let out = ''
  let i = 0
  while (i < trimmed.length) {
    const char = trimmed[i]!
    if (char === '"' || char === "'") {
      const quote = char
      out += char
      i++
      while (i < trimmed.length) {
        const c = trimmed[i]!
        out += c
        i++
        if (c === '\\' && i < trimmed.length) {
          out += trimmed[i]!
          i++
          continue
        }
        if (c === quote) break
      }
      continue
    }
    if (char === '{') {
      const end = trimmed.indexOf('}', i + 1)
      if (end === -1) throw new ValidationError('Unclosed "{" in formula expression')
      const name = trimmed.slice(i + 1, end).trim()
      const field = byKey.get(name)
      if (!field) throw new ValidationError(`Unknown field "${name}" in formula expression`)
      out += formulaReference(tableId, field)
      i = end + 1
      continue
    }
    if (IDENTIFIER_START.test(char)) {
      let end = i + 1
      while (end < trimmed.length && IDENTIFIER_PART.test(trimmed[end]!)) end++
      const identifier = trimmed.slice(i, end)
      let lookahead = end
      while (lookahead < trimmed.length && /\s/.test(trimmed[lookahead]!)) lookahead++
      const isFunction = trimmed[lookahead] === '('
      const isLiteral = identifier === 'true' || identifier === 'false' || identifier === 'null'
      if (isFunction || isLiteral) {
        out += identifier
      } else {
        const field = byKey.get(identifier)
        if (!field) {
          throw new ValidationError(
            `Unknown field "${identifier}" in formula expression. Use {field name} for names with spaces.`,
          )
        }
        out += formulaReference(tableId, field)
      }
      i = end
      continue
    }
    out += char
    i++
  }
  return out
}

export function formulaUsesFieldIds(expression: string): boolean {
  return expression.includes('bitable::$')
}

export function normalizeOptions(options: Array<string | SelectOption> | undefined): SelectOption[] {
  return (options ?? []).map((option) => (typeof option === 'string' ? { name: option } : { ...option }))
}

function normalizeTypeName(type: SchemaFieldType): Exclude<SchemaFieldType, 'string'> {
  return type === 'string' ? 'text' : type
}

export function isJsonLikeField(field: StoredField): boolean {
  return field.json === true || field.kind === 'json'
}


export function estimateValueBytes(value: unknown): number {
  if (typeof value === 'bigint') return Buffer.byteLength(value.toString())
  if (typeof value === 'string') return byteLength(value)
  if (isBinary(value)) return byteLength(value)
  try {
    return byteLength(JSON.stringify(value) ?? '')
  } catch {
    return 0
  }
}
