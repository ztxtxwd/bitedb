import { ValidationError } from './errors.js'
import { sortDirectionFor } from './query.js'
import type { Table } from './table.js'
import type { Document, Filter, QueryOptions, SortDirection, SortSpec, WithId } from './types.js'

export class Query<T extends Document = Document> {
  private readonly table: Table<T>
  private filter: Filter
  private options: QueryOptions

  constructor(table: Table<T>, filter: Filter = {}, options: QueryOptions = {}) {
    this.table = table
    this.filter = filter
    this.options = { ...options }
  }

  sort(spec: SortSpec): this
  sort(field: string, direction: SortDirection): this
  sort(specOrField: SortSpec | string, direction?: SortDirection): this {
    this.options.sort =
      direction !== undefined ? [String(specOrField), direction] : (specOrField as SortSpec)
    return this
  }

  limit(limit: number): this {
    this.options.limit = limit
    return this
  }

  offset(offset: number): this {
    this.options.offset = offset
    return this
  }

  skip(offset: number): this {
    return this.offset(offset)
  }

  fields(...fields: Array<string | string[]>): this {
    this.options.fields = fields.flat()
    return this
  }

  projection(fields: string[]): this {
    this.options.fields = fields
    return this
  }

  /**
   * Cursor pagination helper. Adds a condition for rows strictly after
   * `value` on `field`, in the direction the query sorts that field
   * (ascending by default). The cursor field should be unique (`_id` is a
   * good choice); rows sharing a cursor value may be skipped.
   *
   * @example
   * const page1 = await table.find({ status: 'paid' }).sort('_id', 'asc').limit(100).toArray()
   * const page2 = await table
   *   .find({ status: 'paid' })
   *   .sort('_id', 'asc')
   *   .limit(100)
   *   .after('_id', page1.at(-1)!._id)
   *   .toArray()
   */
  after(field: string, value: unknown): this {
    const sortedDirection = sortDirectionFor(this.options.sort, field)
    if (this.options.sort !== undefined && sortedDirection === undefined) {
      throw new ValidationError(`Cursor pagination requires the sort to order by "${field}"`)
    }
    const direction = sortedDirection ?? 'asc'
    if (value !== undefined) {
      if (Object.prototype.hasOwnProperty.call(this.filter, field)) {
        throw new ValidationError(`Cursor pagination conflicts with an existing filter on "${field}"`)
      }
      this.filter = {
        ...this.filter,
        [field]: { [direction === 'desc' ? '$lt' : '$gt']: value },
      }
    }
    if (sortedDirection === undefined) this.options.sort = [[field, direction]]
    return this
  }

  async toArray(): Promise<Array<WithId<T>>> {
    return (await this.table.executeFind(this.filter, this.options)) as Array<WithId<T>>
  }

  /**
   * Like `toArray()`, but also reports whether more rows exist beyond the
   * requested page. `toArray()` alone cannot distinguish "exactly `limit`
   * rows matched" from "the page was truncated".
   */
  async page(): Promise<{ documents: Array<WithId<T>>; hasMore: boolean }> {
    const result = await this.table.executeFindPage(this.filter, this.options)
    return { documents: result.documents as Array<WithId<T>>, hasMore: result.hasMore }
  }

  async first(): Promise<WithId<T> | null> {
    const docs = await this.table.executeFind(this.filter, { ...this.options, limit: 1, offset: 0 })
    return (docs[0] as WithId<T> | undefined) ?? null
  }

  async count(): Promise<number> {
    return this.table.countDocuments(this.filter)
  }
}
