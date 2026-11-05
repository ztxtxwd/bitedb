import { UnsupportedError, ValidationError } from './errors.js'
import type { FeishuFilter, FeishuFilterCondition } from './feishu.js'
import type { StoredField, TableMeta } from './meta.js'
import type { Document, Filter, QueryOptions, SortDirection, SortSpec } from './types.js'

const OPERATOR_MAP = {
  $eq: 'is',
  $ne: 'isNot',
  $gt: 'isGreater',
  $gte: 'isGreaterEqual',
  $lt: 'isLess',
  $lte: 'isLessEqual',
  $contains: 'contains',
  $notContains: 'doesNotContain',
  $doesNotContain: 'doesNotContain',
  $exists: 'isNotEmpty',
} as const

type FeishuOperator = FeishuFilterCondition['operator']

export interface CompiledFilter {
  filter?: FeishuFilter
  /** True when the filter has a condition that Feishu cannot express. */
  impossible?: boolean
}

export function compileFilter(filter: Filter | undefined, meta: TableMeta): CompiledFilter {
  if (filter === undefined || filter === null) return {}
  if (typeof filter !== 'object' || Array.isArray(filter)) {
    throw new ValidationError('filter must be an object')
  }
  if (Object.keys(filter).length === 0) return {}

  const rootConditions: FeishuFilterCondition[] = []
  const childGroups: NonNullable<FeishuFilter['children']> = []

  for (const [key, value] of Object.entries(filter)) {
    if (key === '$and') {
      if (!Array.isArray(value)) throw new ValidationError('$and expects an array')
      for (const item of value) {
        if (!isPlainRecord(item)) throw new ValidationError('$and entries must be filter objects')
        const compiled = compileFilter(item as Filter, meta)
        if (compiled.filter?.conditions) rootConditions.push(...compiled.filter.conditions)
        if (compiled.filter?.children) childGroups.push(...compiled.filter.children)
      }
      continue
    }
    if (key === '$or') {
      if (!Array.isArray(value)) throw new ValidationError('$or expects an array')
      const orConditions: FeishuFilterCondition[] = []
      for (const item of value) {
        if (!isPlainRecord(item)) throw new ValidationError('$or entries must be filter objects')
        const compiled = compileFilter(item as Filter, meta)
        if (compiled.filter?.children?.length) {
          throw new UnsupportedError(
            'Bitable record filters support only one level of boolean nesting. Simplify the filter.',
          )
        }
        orConditions.push(...(compiled.filter?.conditions ?? []))
      }
      if (orConditions.length > 0) {
        childGroups.push({ conjunction: 'or', conditions: orConditions })
      }
      continue
    }
    if (key.startsWith('$')) {
      throw new UnsupportedError(`Unsupported logical operator "${key}"`)
    }
    const compiledField = compileFieldConditions(key, value, meta)
    rootConditions.push(...compiledField.conditions)
    for (const group of compiledField.orGroups) {
      childGroups.push({ conjunction: 'or', conditions: group })
    }
  }

  if (childGroups.length > 1) {
    throw new UnsupportedError(
      'Bitable record filters support a single OR group per query. Combine $or clauses or filter client-side.',
    )
  }

  if (rootConditions.length === 0 && childGroups.length === 0) return { impossible: true }

  // An OR-only query can be expressed directly on the root filter. This is
  // more widely supported than a root with children and no conditions.
  if (rootConditions.length === 0 && childGroups.length === 1) {
    return {
      filter: {
        conjunction: childGroups[0]!.conjunction,
        conditions: childGroups[0]!.conditions,
      },
    }
  }

  return {
    filter: {
      conjunction: 'and',
      ...(rootConditions.length > 0 ? { conditions: rootConditions } : {}),
      ...(childGroups.length > 0 ? { children: childGroups } : {}),
    },
  }
}

interface CompiledFieldConditions {
  conditions: FeishuFilterCondition[]
  orGroups: FeishuFilterCondition[][]
}

function compileFieldConditions(
  key: string,
  value: unknown,
  meta: TableMeta,
): CompiledFieldConditions {
  const field = findField(meta, key)
  if (!field) {
    throw new ValidationError(`Cannot filter on unknown field "${key}" in table "${meta.name}"`)
  }
  const name = field.name
  if (value === null) return { conditions: [condition(name, 'isEmpty')], orGroups: [] }
  if (Array.isArray(value)) {
    if (value.length === 0) throw new ValidationError(`Empty array filter on "${key}"`)
    return {
      conditions: [],
      orGroups: [value.map((item) => condition(name, 'is', [stringifyFilterValue(item, field)]))],
    }
  }
  if (!isPlainRecord(value) || !Object.keys(value).some((op) => op.startsWith('$'))) {
    return { conditions: [condition(name, 'is', [stringifyFilterValue(value, field)])], orGroups: [] }
  }

  const conditions: FeishuFilterCondition[] = []
  const orGroups: FeishuFilterCondition[][] = []
  for (const [op, operand] of Object.entries(value)) {
    if (!op.startsWith('$')) {
      throw new ValidationError(
        `Mixed filter object for "${key}". Use either an equality value or operators, not both.`,
      )
    }
    if (op === '$in' || op === '$nin') {
      if (!Array.isArray(operand) || operand.length === 0) {
        throw new ValidationError(`${op} expects a non-empty array`)
      }
      const values = operand.map((item) => stringifyFilterValue(item, field))
      if (op === '$in') {
        if (values.length === 1) {
          conditions.push(condition(name, 'is', values))
        } else {
          // Feishu has no isAnyOf; express the set as an OR group.
          orGroups.push(values.map((v) => condition(name, 'is', [v])))
        }
      } else {
        for (const v of values) conditions.push(condition(name, 'isNot', [v]))
      }
      continue
    }
    if (op === '$exists') {
      conditions.push(condition(name, operand ? 'isNotEmpty' : 'isEmpty'))
      continue
    }
    if (operand === null && (op === '$eq' || op === '$ne')) {
      conditions.push(condition(name, op === '$eq' ? 'isEmpty' : 'isNotEmpty'))
      continue
    }
    const mapped = OPERATOR_MAP[op as keyof typeof OPERATOR_MAP]
    if (!mapped) {
      throw new UnsupportedError(
        `Unsupported filter operator "${op}" on "${key}". Supported: $eq $ne $gt $gte $lt $lte $in $nin $contains $notContains $exists`,
      )
    }
    if (mapped === 'isNotEmpty') {
      conditions.push(condition(name, mapped))
      continue
    }
    if (Array.isArray(operand)) {
      conditions.push(condition(name, mapped, operand.map((item) => stringifyFilterValue(item, field))))
    } else {
      conditions.push(condition(name, mapped, [stringifyFilterValue(operand, field)]))
    }
  }
  if (conditions.length === 0 && orGroups.length === 0) {
    return { conditions: [condition(name, 'isEmpty')], orGroups: [] }
  }
  return { conditions, orGroups }
}

function condition(
  fieldName: string,
  operator: FeishuOperator,
  value?: string[],
): FeishuFilterCondition {
  return { field_name: fieldName, operator, ...(value ? { value } : {}) }
}

export function stringifyFilterValue(value: unknown, field?: StoredField): string {
  if (value instanceof Date) return String(value.getTime())
  if (typeof value === 'boolean') return value ? 'true' : 'false'
  if (typeof value === 'number' || typeof value === 'bigint') return String(value)
  if (typeof value === 'string') return value
  if (value === null || value === undefined) return ''
  if (field?.kind === 'json' && isPlainRecord(value)) {
    try {
      return JSON.stringify(value)
    } catch {
      /* fall through */
    }
  }
  throw new ValidationError(`Unsupported filter value: ${String(value)}`)
}

function findField(meta: TableMeta, key: string): StoredField | undefined {
  return meta.fields.find((field) => field.key === key || field.name === key)
}

export function parseSort(
  sort: SortSpec | undefined,
  meta: TableMeta,
): Array<{ field_name: string; desc?: boolean }> {
  if (!sort) return []
  const entries: Array<[string, SortDirection]> = []
  if (typeof sort === 'string') {
    entries.push([sort, 'asc'])
  } else if (Array.isArray(sort)) {
    if (sort.length === 2 && typeof sort[0] === 'string' && (sort[1] === 'asc' || sort[1] === 'desc')) {
      entries.push([sort[0], sort[1]])
    } else {
      for (const item of sort as Array<string | [string, SortDirection]>) {
        if (typeof item === 'string') entries.push([item, 'asc'])
        else if (Array.isArray(item) && typeof item[0] === 'string' && (item[1] === 'asc' || item[1] === 'desc')) {
          entries.push([item[0], item[1]])
        } else {
          throw new ValidationError('Invalid sort specification')
        }
      }
    }
  } else if (isPlainRecord(sort)) {
    for (const [key, direction] of Object.entries(sort)) {
      if (direction !== 'asc' && direction !== 'desc') throw new ValidationError('Sort direction must be asc or desc')
      entries.push([key, direction])
    }
  } else {
    throw new ValidationError('Invalid sort specification')
  }

  return entries.map(([key, direction]) => {
    const field = findField(meta, key)
    if (!field) throw new ValidationError(`Cannot sort on unknown field "${key}" in table "${meta.name}"`)
    return { field_name: field.name, ...(direction === 'desc' ? { desc: true } : {}) }
  })
}

export function compareDocuments(a: Document, b: Document, sort: SortSpec): number {
  for (const [key, direction] of sortEntries(sort)) {
    const aValue = a[key]
    const bValue = b[key]
    const cmp = compareValues(aValue, bValue)
    if (cmp !== 0) return direction === 'desc' ? -cmp : cmp
  }
  return 0
}

export function sortDirectionFor(
  sort: SortSpec | undefined,
  field: string,
): SortDirection | undefined {
  for (const [key, direction] of sortEntries(sort)) {
    if (key === field) return direction
  }
  return undefined
}

function sortEntries(sort: SortSpec | undefined): Array<[string, SortDirection]> {
  if (!sort) return []
  if (typeof sort === 'string') return [[sort, 'asc']]
  if (Array.isArray(sort)) {
    if (sort.length === 2 && typeof sort[0] === 'string' && (sort[1] === 'asc' || sort[1] === 'desc')) {
      return [[sort[0], sort[1]]]
    }
    return (sort as Array<string | [string, SortDirection]>).map((item) =>
      typeof item === 'string' ? [item, 'asc'] : [item[0], item[1]],
    )
  }
  return Object.entries(sort).map(([key, direction]) => [key, direction])
}

export function compareValues(a: unknown, b: unknown): number {
  if (a === b) return 0
  if (a === null || a === undefined) return 1
  if (b === null || b === undefined) return -1
  if (a instanceof Date || b instanceof Date) {
    const av = a instanceof Date ? a.getTime() : new Date(a as any).getTime()
    const bv = b instanceof Date ? b.getTime() : new Date(b as any).getTime()
    return av === bv ? 0 : av < bv ? -1 : 1
  }
  if (typeof a === 'number' && typeof b === 'number') return a < b ? -1 : 1
  if (typeof a === 'boolean' && typeof b === 'boolean') return a === b ? 0 : a ? 1 : -1
  const as = String(a)
  const bs = String(b)
  return as === bs ? 0 : as < bs ? -1 : 1
}

export function mergeSorted<T>(
  perShard: T[][],
  compare: (a: T, b: T) => number,
  limit: number,
): T[] {
  const heads = perShard.map((list) => ({ list, index: 0 }))
  const out: T[] = []
  while (out.length < limit) {
    let best: { list: T[]; index: number } | undefined
    for (const head of heads) {
      if (head.index >= head.list.length) continue
      if (!best) {
        best = head
        continue
      }
      if (compare(head.list[head.index]!, best.list[best.index]!) < 0) best = head
    }
    if (!best) break
    out.push(best.list[best.index]!)
    best.index++
  }
  return out
}

export function mapFieldNames(fields: string[] | undefined, meta: TableMeta): string[] | undefined {
  if (!fields || fields.length === 0) return undefined
  const names = fields.map((key) => {
    const field = findField(meta, key)
    if (!field) throw new ValidationError(`Unknown field "${key}" in projection for table "${meta.name}"`)
    return field.name
  })
  if (!names.includes('_id')) {
    const idField = meta.fields.find((field) => field.key === '_id')
    if (idField && !names.includes(idField.name)) names.push(idField.name)
  }
  return names
}

export function isPlainRecord(value: unknown): value is Record<string, any> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value) && !(value instanceof Date)
}

export interface NormalizedQueryOptions extends QueryOptions {
  limit: number
  offset: number
}

export function normalizeQueryOptions(options: QueryOptions | undefined): NormalizedQueryOptions {
  const limit = options?.limit === undefined ? 100 : Math.max(0, Math.floor(options.limit))
  const offset = options?.offset === undefined ? 0 : Math.max(0, Math.floor(options.offset))
  return { ...options, limit, offset }
}
