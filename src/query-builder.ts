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

  async toArray(): Promise<Array<WithId<T>>> {
    return (await this.table.executeFind(this.filter, this.options)) as Array<WithId<T>>
  }

  async first(): Promise<WithId<T> | null> {
    const docs = await this.table.executeFind(this.filter, { ...this.options, limit: 1, offset: 0 })
    return (docs[0] as WithId<T> | undefined) ?? null
  }

  async count(): Promise<number> {
    return this.table.countDocuments(this.filter)
  }
}
