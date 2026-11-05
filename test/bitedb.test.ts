import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { after, before, test } from 'node:test'
import { BlobRef, ConfigError, createClient, type BiteDB, type BiteDBConfig } from '../src/index.js'
import { FakeFeishu } from './fake-feishu.js'

let fake: FakeFeishu
const clients: BiteDB[] = []

function makeClient(overrides: Partial<BiteDBConfig> = {}): BiteDB {
  const client = createClient({
    appId: 'test-app',
    appSecret: 'test-secret',
    domain: fake.url,
    batchWindowMs: 5,
    maxBatchSize: 1000,
    maxRowsPerShard: 20000,
    blobThresholdBytes: 1024,
    catalogFlushMs: 0,
    ...overrides,
  })
  clients.push(client)
  return client
}

before(async () => {
  fake = await new FakeFeishu().start()
})

after(async () => {
  for (const client of clients) {
    try {
      await client.close()
    } catch {
      /* ignore */
    }
  }
  await fake.stop()
})

test('zero provisioning: creates catalog, database and physical table', async () => {
  const client = makeClient()
  const database = client.useDatabase('zero_provision')
  const orders = database.defineTable('orders', {
    price: 'number',
    count: 'number',
    total: { type: 'computed', expr: 'price * count' },
  })

  const inserted = await orders.insertOne({ price: 100, count: 3 })
  assert.equal(typeof inserted._id, 'string')
  assert.equal(inserted.total, undefined)

  const read = await orders.get(inserted._id)
  assert.ok(read)
  assert.equal(read.price, 100)
  assert.equal(read.count, 3)
  assert.equal(read.total, 300)

  assert.ok([...fake.bases.values()].some((base) => base.name === '__bitedb_catalog__'))
  assert.ok([...fake.bases.values()].some((base) => base.name === 'bitedb:zero_provision'))

  const formulaCalls = fake.callsMatching('POST', '/fields').filter((call) => call.body?.field_name === 'total')
  assert.equal(formulaCalls.length, 1)
  assert.match(formulaCalls[0]!.body.property.formula_expression, /^bitable::\$table\[tbl/)
  assert.match(formulaCalls[0]!.body.property.formula_expression, /\$field\[fld/)
})

test('find/findOne return the same logical _id as insertOne and get', async () => {
  const client = makeClient()
  const cards = client.useDatabase('logical_ids').defineTable('cards', {
    owner: 'text',
    title: 'text',
  })
  const inserted = await cards.insertOne({ owner: 'ada', title: 'first' })

  const found = await cards.findOne({ title: 'first' })
  assert.ok(found)
  assert.equal(found._id, inserted._id)
  assert.equal(found.owner, 'ada')

  const rows = await cards.find({ owner: 'ada' }).toArray()
  assert.equal(rows.length, 1)
  assert.equal(rows[0]!._id, inserted._id)

  const got = await cards.get(inserted._id)
  assert.ok(got)
  assert.equal(got._id, inserted._id)
})

test('an unreadable _id falls back to the record id and logs a warning', async () => {
  const warnings: string[] = []
  const client = makeClient({
    logger: { warn: (...args: any[]) => warnings.push(args.map(String).join(' ')) },
  })
  const table = client.useDatabase('bad_ids').defineTable('items', { name: 'text' })
  const inserted = await table.insertOne({ name: 'x' })

  const base = [...fake.bases.values()].find((entry) => entry.name === 'bitedb:bad_ids')!
  const physical = [...base.tables.values()].find((entry) => entry.name === 'items')!
  const record = [...physical.records.values()].find((entry) => entry.fields._id === inserted._id)!
  record.fields._id = { unexpected: true }

  const found = await table.findOne({ name: 'x' })
  assert.ok(found)
  assert.equal(found._id, record.record_id)
  assert.ok(warnings.some((message) => message.includes('unsupported _id')))
})

test('database.prewarm provisions every declared table', async () => {
  const client = makeClient()
  const database = client.useDatabase('prewarm')
  const users = database.defineTable('users', { name: 'text' })
  const posts = database.defineTable('posts', {
    title: 'text',
    author: { type: 'link', table: 'users' },
  })
  database.defineTable('audit', { action: 'text' })

  await database.prewarm()

  const names = (await database.listTables()).map((table) => table.name).sort()
  assert.deepEqual(names, ['audit', 'posts', 'users'])

  const user = await users.insertOne({ name: 'Ada' })
  const post = await posts.insertOne({ title: 'Hello', author: user._id })
  assert.equal((await posts.get(post._id))!.title, 'Hello')
  assert.deepEqual((await posts.find({ author: user._id }).toArray())[0]!._id, post._id)
})

test('reconnects from catalog discovery and reads existing data', async () => {
  const client = makeClient()
  const orders = client.useDatabase('zero_provision').table('orders')
  const all = await orders.find({}).toArray()
  assert.equal(all.length, 1)
  assert.equal(all[0]!.total, 300)

  const databases = await client.listDatabases()
  assert.ok(databases.some((database) => database.name === 'zero_provision'))
})

test('schema-less inserts infer Bitable fields and preserve types', async () => {
  const client = makeClient()
  const events = client.useDatabase('schemaless').table('events')
  const when = new Date('2024-01-02T03:04:05.000Z')
  const inserted = await events.insertOne({
    title: 'launch',
    amount: 42,
    active: true,
    payload: { tags: ['a', 'b'], nested: { n: 1 } },
    when,
  })
  const read = await events.get(inserted._id)
  assert.ok(read)
  assert.equal(read.title, 'launch')
  assert.equal(read.amount, 42)
  assert.equal(read.active, true)
  assert.deepEqual(read.payload, { tags: ['a', 'b'], nested: { n: 1 } })
  assert.ok(read.when instanceof Date)
  assert.equal(read.when.toISOString(), when.toISOString())

  const schema = await events.schema()
  assert.deepEqual(
    schema.filter((field) => field.key !== '_id').map((field) => field.kind).sort(),
    ['boolean', 'date', 'json', 'number', 'text'],
  )
})

test('insert batching coalesces parallel writes into one batch_create', async () => {
  const client = makeClient()
  const table = client.useDatabase('batching').defineTable('metrics', { value: 'number' })
  await table.ensure()
  fake.clearCalls()

  const docs = await Promise.all(
    Array.from({ length: 25 }, (_, index) => table.insertOne({ value: index })),
  )
  assert.equal(docs.length, 25)
  const batchCalls = fake.callsMatching('POST', '/records/batch_create')
  assert.equal(batchCalls.length, 1)
  assert.equal(batchCalls[0]!.body.records.length, 25)
  assert.equal(await table.countDocuments(), 25)
})

test('filters, sort and computed fields are executed server-side', async () => {
  const client = makeClient()
  const orders = client.useDatabase('queries').defineTable('orders', {
    sku: 'text',
    price: 'number',
    count: 'number',
    total: { type: 'computed', expr: 'price * count' },
  })
  await orders.insertMany([
    { sku: 'a', price: 10, count: 1 },
    { sku: 'b', price: 20, count: 3 },
    { sku: 'c', price: 5, count: 10 },
    { sku: 'd', price: 100, count: 2 },
  ])

  const expensive = await orders
    .find({ total: { $gte: 100 } })
    .sort('total', 'desc')
    .limit(2)
    .toArray()
  assert.deepEqual(expensive.map((order) => order.sku), ['d'])

  const inFilter = await orders.find({ sku: { $in: ['a', 'd'] } }).toArray()
  assert.deepEqual(inFilter.map((order) => order.sku).sort(), ['a', 'd'])

  const count = await orders.countDocuments({ total: { $gt: 30 } })
  assert.equal(count, 3)

  const either = await orders
    .find({ $or: [{ sku: 'a' }, { sku: 'd' }] })
    .sort('sku', 'asc')
    .toArray()
  assert.deepEqual(either.map((order) => order.sku), ['a', 'd'])

  const projected = await orders.find({ sku: 'b' }).fields('sku').first()
  assert.ok(projected)
  assert.deepEqual(Object.keys(projected).sort(), ['_id', 'sku'])
})

test('row sharding is transparent across physical tables', async () => {
  const client = makeClient({ maxRowsPerShard: 2 })
  const logs = client.useDatabase('sharded').defineTable('logs', {
    seq: 'number',
    doubled: { type: 'computed', expr: 'seq * 2' },
  })
  await logs.insertMany(Array.from({ length: 5 }, (_, index) => ({ seq: index + 1 })))

  const stats = await logs.stats()
  assert.equal(stats.shards.length, 3)
  assert.equal(stats.totalRows, 5)
  assert.equal(await logs.countDocuments(), 5)

  const all = await logs.find({}).sort('seq', 'asc').toArray()
  assert.deepEqual(all.map((doc) => doc.seq), [1, 2, 3, 4, 5])
  assert.deepEqual(all.map((doc) => doc.doubled), [2, 4, 6, 8, 10])

  const page = await logs.find({}).sort('seq', 'desc').offset(1).limit(2).toArray()
  assert.deepEqual(page.map((doc) => doc.seq), [4, 3])

  // New fields are DDL'd onto every existing shard.
  const late = await logs.insertOne({ seq: 6, note: 'late field' })
  const lateRead = await logs.get(late._id)
  assert.equal(lateRead!.note, 'late field')
})

test('large values spill to Drive; blobs hydrate lazily or transparently', async () => {
  const client = makeClient()
  const files = client.useDatabase('blobs').defineTable('files', {
    name: 'text',
    avatar: { type: 'blob' },
    logs: { type: 'json', spill: 64 },
  })
  const avatar = Buffer.from('binary-avatar-content')
  const logs = { entries: Array.from({ length: 20 }, (_, index) => ({ index, message: 'x'.repeat(10) })) }
  const inserted = await files.insertOne({ name: 'one', avatar, logs })
  fake.clearCalls()

  const read = await files.get(inserted._id)
  assert.ok(read)
  assert.equal(read.name, 'one')
  assert.ok(read.avatar instanceof BlobRef)
  assert.deepEqual(await read.avatar.bytes(), avatar)
  assert.deepEqual(read.logs, logs)
  assert.equal(fake.callsMatching('GET', '/download').length, 2)

  const eagerClient = makeClient({ hydrate: 'eager' })
  const eagerFiles = eagerClient.useDatabase('blobs').table('files')
  const eager = await eagerFiles.get(inserted._id)
  assert.ok(eager)
  assert.ok(Buffer.isBuffer(eager.avatar))
  assert.deepEqual(eager.avatar, avatar)
})

test('duplex links resolve logical ids and expose the reverse side', async () => {
  const client = makeClient()
  const database = client.useDatabase('graph')
  const users = database.defineTable('users', { name: 'text' })
  const posts = database.defineTable('posts', {
    title: 'text',
    author: { type: 'link', table: 'users' },
  })

  const user = await users.insertOne({ name: 'Ada' })
  const post = await posts.insertOne({ title: 'Hello', author: user._id })

  const readPost = await posts.get(post._id)
  assert.ok(readPost)
  assert.deepEqual(readPost.author, [user._id])

  const userSchema = await users.schema()
  const reverse = userSchema.find((field) => field.kind === 'link' && field.target === 'posts')
  assert.ok(reverse, 'reverse link field should be added to the users schema')
  const readUser = await users.get(user._id)
  assert.ok(readUser)
  assert.deepEqual(readUser[reverse.key], [post._id])

  const linked = await posts.find({ author: user._id }).toArray()
  assert.equal(linked.length, 1)
  assert.equal(linked[0]!.title, 'Hello')

  // Catalog loss must not turn the reverse side back into raw record ids.
  const recoveredClient = makeClient({ catalogName: '__bitedb_catalog_graph__' })
  const recoveredUsers = recoveredClient
    .useDatabase('graph')
    .defineTable('users', { name: 'text' })
  const recoveredUser = await recoveredUsers.get(user._id)
  assert.ok(recoveredUser)
  const recoveredReverse = (await recoveredUsers.schema()).find(
    (field) => field.kind === 'link' && field.target === 'posts',
  )
  assert.ok(recoveredReverse)
  assert.deepEqual(recoveredUser[recoveredReverse.key], [post._id])
})

test('updates, upserts by id, deletes and counts stay consistent', async () => {
  const client = makeClient()
  const table = client.useDatabase('crud').defineTable('items', {
    name: 'text',
    qty: 'number',
    status: 'text',
  })
  const docs = await table.insertMany([
    { name: 'a', qty: 1, status: 'new' },
    { name: 'b', qty: 2, status: 'new' },
    { name: 'c', qty: 3, status: 'new' },
    { name: 'd', qty: 4, status: 'new' },
  ])

  const inc = await table.updateById(docs[0]!._id, { $inc: { qty: 10 } })
  assert.deepEqual(inc, { matchedCount: 1, modifiedCount: 1 })
  assert.equal((await table.get(docs[0]!._id))!.qty, 11)

  const updated = await table.updateMany({ qty: { $gte: 3 } }, { $set: { status: 'hot' } })
  assert.equal(updated.matchedCount, 3)
  assert.equal(await table.countDocuments({ status: 'hot' }), 3)

  const removed = await table.deleteMany({ status: 'new' })
  assert.equal(removed.deletedCount, 1)
  assert.equal(await table.countDocuments(), 3)

  const one = await table.deleteOne(docs[3]!._id)
  assert.equal(one.deletedCount, 1)
  assert.equal(await table.countDocuments(), 2)

  const unset = await table.updateOne(docs[0]!._id, { $unset: ['status'] })
  assert.equal(unset.modifiedCount, 1)
  const afterUnset = await table.get(docs[0]!._id)
  assert.equal(afterUnset!.status, undefined)
})

test('adopts an existing Base and physical tables when the catalog is lost', async () => {
  const first = makeClient()
  const sourceTable = first.useDatabase('recoverable').defineTable('orders', {
    sku: 'text',
    qty: 'number',
    total: { type: 'computed', expr: 'qty * 2' },
  })
  await sourceTable.insertOne({ sku: 'x', qty: 2 })

  // Simulate total catalog loss by pointing a client at a brand-new catalog.
  const second = makeClient({ catalogName: '__bitedb_catalog_lost__' })
  const recovered = second.useDatabase('recoverable').defineTable('orders', {
    sku: 'text',
    qty: 'number',
    total: { type: 'computed', expr: 'qty * 2' },
  })
  const rows = await recovered.find({}).toArray()
  assert.equal(rows.length, 1)
  assert.equal(rows[0]!.sku, 'x')
  assert.equal(rows[0]!.total, 4)

  const inserted = await recovered.insertOne({ sku: 'y', qty: 3 })
  assert.equal((await recovered.get(inserted._id))!.total, 6)
  assert.equal(await recovered.countDocuments(), 2)
})

test('long logical table names survive catalog-loss adoption', async () => {
  const tableName = `t_${'x'.repeat(80)}`
  const first = makeClient()
  const source = first.useDatabase('longname').defineTable(tableName, { value: 'number' })
  await source.insertOne({ value: 7 })

  const second = makeClient({ catalogName: '__bitedb_catalog_long__' })
  const recovered = second.useDatabase('longname').table(tableName)
  const rows = await recovered.find({}).toArray()
  assert.equal(rows.length, 1)
  assert.equal(rows[0]!.value, 7)
})

test('credential aliases and pre-acquired access tokens work', async () => {
  const viaAliases = createClient({
    clientId: 'test-app',
    clientSecret: 'test-secret',
    domain: fake.url,
    batchWindowMs: 5,
  })
  clients.push(viaAliases)
  const table = viaAliases.useDatabase('credentials').defineTable('items', { name: 'text' })
  await table.insertOne({ name: 'alias' })

  const viaToken = createClient({
    accessToken: fake.token,
    domain: fake.url,
    batchWindowMs: 5,
  })
  clients.push(viaToken)
  const read = await viaToken.useDatabase('credentials').table('items').find({}).toArray()
  assert.equal(read.length, 1)
  assert.equal(read[0]!.name, 'alias')

  assert.throws(
    () => createClient({ domain: fake.url } as BiteDBConfig),
    (err) => err instanceof ConfigError,
  )
})

test('more filter operators and boolean/date values', async () => {
  const client = makeClient()
  const table = client.useDatabase('operators').defineTable('records', {
    name: 'text',
    tag: 'text',
    score: 'number',
    active: 'boolean',
    at: 'date',
  })
  await table.insertMany([
    { name: 'alpha', tag: 'x', score: 10, active: true, at: new Date('2024-01-01T00:00:00Z') },
    { name: 'beta', tag: 'y', score: 20, active: false, at: new Date('2024-06-01T00:00:00Z') },
    { name: 'gamma', score: 30, active: true },
  ])

  const contains = await table.find({ name: { $contains: 'amm' } }).toArray()
  assert.deepEqual(contains.map((doc) => doc.name), ['gamma'])

  const notIn = await table.find({ tag: { $nin: ['x'] } }).sort('name', 'asc').toArray()
  assert.deepEqual(notIn.map((doc) => doc.name), ['beta', 'gamma'])

  const active = await table.find({ active: true }).sort('score', 'asc').toArray()
  assert.deepEqual(active.map((doc) => doc.score), [10, 30])

  const exists = await table.find({ tag: { $exists: true } }).toArray()
  assert.deepEqual(exists.map((doc) => doc.name).sort(), ['alpha', 'beta'])

  const recent = await table
    .find({ at: { $gte: new Date('2024-03-01T00:00:00Z') } })
    .toArray()
  assert.deepEqual(recent.map((doc) => doc.name), ['beta'])
})

test('attachment fields keep raw attachment semantics', async () => {
  const client = makeClient()
  const table = client.useDatabase('attachments').defineTable('files', {
    receipts: 'attachment',
  })
  const inserted = await table.insertOne({ receipts: Buffer.from('receipt-bytes') })
  const read = await table.get(inserted._id)
  assert.ok(read)
  assert.ok(Array.isArray(read.receipts))
  assert.equal(read.receipts.length, 1)
  assert.ok(read.receipts[0] instanceof BlobRef)
  assert.equal(await read.receipts[0].text(), 'receipt-bytes')
})

test('a dropped table handle can be reused to recreate the table', async () => {
  const client = makeClient()
  const table = client.useDatabase('drop_reuse').defineTable('items', { name: 'text' })
  await table.insertOne({ name: 'first' })
  assert.equal(await table.countDocuments(), 1)

  await table.drop()
  assert.equal(await table.countDocuments(), 0)

  const recreated = await table.insertOne({ name: 'second' })
  assert.equal((await table.get(recreated._id))!.name, 'second')
  assert.equal(await table.countDocuments(), 1)
})

test('formula fields survive deployments that reject expressions on create', async () => {
  fake.rejectFormulaOnCreate = true
  try {
    const client = makeClient()
    const table = client.useDatabase('formula_fallback').defineTable('metrics', {
      value: 'number',
      tripled: { type: 'computed', expr: 'value * 3' },
    })
    const inserted = await table.insertOne({ value: 2 })
    const read = await table.get(inserted._id)
    assert.equal(read!.tripled, 6)
    const creates = fake
      .callsMatching('POST', '/fields')
      .filter((call) => call.body?.field_name === 'tripled')
    assert.equal(creates.length, 2)
    assert.ok(fake.callsMatching('PUT', '/fields/').length >= 1)
  } finally {
    fake.rejectFormulaOnCreate = false
  }
})

test('chunked upload path stores media larger than 20 MB', async () => {
  const client = makeClient()
  const table = client.useDatabase('chunked').defineTable('assets', { data: { type: 'blob' } })
  const payload = Buffer.alloc(21 * 1024 * 1024, 'ab')
  const inserted = await table.insertOne({ data: payload })
  assert.ok(fake.callsMatching('POST', '/upload_prepare').length >= 1)

  const read = await table.get(inserted._id)
  assert.ok(read)
  assert.ok(read.data instanceof BlobRef)
  const restored = await read.data.bytes()
  assert.equal(restored.length, payload.length)
  const hash = (buffer: Buffer) => createHash('sha256').update(buffer).digest('hex')
  assert.equal(hash(restored), hash(payload))
})

test('json columns decode segment arrays from the search endpoint', async () => {
  const client = makeClient()
  const cards = client.useDatabase('json_segments').defineTable('cards', {
    owner: 'text',
    dsl: { type: 'json' },
  })
  const dsl = {
    schema: '2.0',
    body: { elements: [{ tag: 'markdown', content: 'hi' }] },
  }
  const inserted = await cards.insertOne({ owner: 'u1', dsl })

  const found = await cards.findOne({ owner: 'u1' })
  assert.ok(found)
  assert.deepEqual(found.dsl, dsl)
  assert.ok(!Array.isArray(found.dsl))

  const rows = await cards.find({ owner: 'u1' }).toArray()
  assert.deepEqual(rows[0]!.dsl, dsl)

  const got = await cards.get(inserted._id)
  assert.deepEqual(got!.dsl, dsl)

  // A JSON value that is itself an array must not be confused with the
  // rich-text segment array returned for text columns.
  await cards.insertOne({ owner: 'u2', dsl: [1, 2, { a: true }] })
  const list = await cards.findOne({ owner: 'u2' })
  assert.deepEqual(list!.dsl, [1, 2, { a: true }])
})

test('updateOne / updateById can return the document before or after the write', async () => {
  const client = makeClient()
  const table = client.useDatabase('update_return').defineTable('items', {
    name: 'text',
    qty: 'number',
    meta: { type: 'json' },
  })
  const inserted = await table.insertOne({ name: 'a', qty: 1, meta: { tags: ['x'] } })

  const plain = await table.updateOne(inserted._id, { $inc: { qty: 2 } })
  assert.deepEqual(plain, { matchedCount: 1, modifiedCount: 1 })

  const before = await table.updateById(
    inserted._id,
    { $set: { name: 'b' } },
    { returnDocument: 'before' },
  )
  assert.equal(before.document!._id, inserted._id)
  assert.equal(before.document!.name, 'a')

  const after = await table.updateOne(
    inserted._id,
    { $set: { name: 'c' }, $inc: { qty: 3 } },
    { returnDocument: 'after' },
  )
  assert.equal(after.document!.name, 'c')
  assert.equal(after.document!.qty, 6)
  assert.deepEqual(after.document!.meta, { tags: ['x'] })

  // An empty patch matches the row without writing it; the document is still
  // available.
  const noop = await table.updateById(inserted._id, {}, { returnDocument: 'after' })
  assert.equal(noop.matchedCount, 1)
  assert.equal(noop.modifiedCount, 0)
  assert.equal(noop.document!.name, 'c')

  await assert.rejects(
    table.updateOne(inserted._id, { $set: { name: 'd' } }, { returnDocument: 'yes' as any }),
    /returnDocument/,
  )
})

test('updateMany can return the matched documents across shards', async () => {
  const client = makeClient({ maxRowsPerShard: 2 })
  const table = client.useDatabase('update_many_return').defineTable('items', {
    name: 'text',
    qty: 'number',
  })
  const docs = await table.insertMany([
    { name: 'a', qty: 1 },
    { name: 'b', qty: 2 },
    { name: 'c', qty: 3 },
  ])
  assert.equal((await table.stats()).shards.length, 2)

  const after = await table.updateMany(
    { qty: { $gte: 2 } },
    { $inc: { qty: 10 } },
    { returnDocument: 'after' },
  )
  assert.equal(after.matchedCount, 2)
  assert.equal(after.modifiedCount, 2)
  assert.deepEqual(
    after.documents!.map((doc) => [doc.name, doc.qty]).sort(),
    [
      ['b', 12],
      ['c', 13],
    ],
  )

  const before = await table.updateMany(
    { name: 'a' },
    { $set: { qty: 100 } },
    { returnDocument: 'before' },
  )
  assert.equal(before.documents!.length, 1)
  assert.equal(before.documents![0]!._id, docs[0]!._id)
  assert.equal(before.documents![0]!.qty, 1)
})

test('Query.page reports truncation and findAfter pages by cursor', async () => {
  const client = makeClient({ maxRowsPerShard: 2 })
  const table = client.useDatabase('paging').defineTable('events', {
    seq: 'number',
    kind: 'text',
  })
  await table.insertMany(
    Array.from({ length: 5 }, (_, index) => ({
      seq: index + 1,
      kind: index % 2 === 0 ? 'odd' : 'even',
    })),
  )

  const complete = await table.find({}).sort('seq', 'asc').page()
  assert.equal(complete.documents.length, 5)
  assert.equal(complete.hasMore, false)

  const first = await table.find({}).sort('seq', 'asc').limit(2).page()
  assert.deepEqual(first.documents.map((doc) => doc.seq), [1, 2])
  assert.equal(first.hasMore, true)

  const last = await table.find({}).sort('seq', 'asc').limit(2).offset(4).page()
  assert.deepEqual(last.documents.map((doc) => doc.seq), [5])
  assert.equal(last.hasMore, false)

  const page1 = await table.findAfter('seq', undefined, { limit: 2 }).toArray()
  assert.deepEqual(page1.map((doc) => doc.seq), [1, 2])
  const page2 = await table.findAfter('seq', page1.at(-1)!.seq, { limit: 2 }).toArray()
  assert.deepEqual(page2.map((doc) => doc.seq), [3, 4])
  const page3 = await table.findAfter('seq', page2.at(-1)!.seq, { limit: 2 }).toArray()
  assert.deepEqual(page3.map((doc) => doc.seq), [5])

  const descending = await table
    .findAfter('seq', 4, { limit: 2, sort: [['seq', 'desc']] })
    .toArray()
  assert.deepEqual(descending.map((doc) => doc.seq), [3, 2])

  // Query.after() combines a cursor with an existing filter.
  const filtered = await table
    .find({ kind: 'odd' })
    .sort('seq', 'asc')
    .after('seq', 1)
    .limit(10)
    .toArray()
  assert.deepEqual(filtered.map((doc) => doc.seq), [3, 5])

  assert.throws(
    () => table.find({}).sort('kind', 'asc').after('seq', 1),
    /requires the sort to order by "seq"/,
  )
  assert.throws(
    () => table.find({ seq: 1 }).after('seq', 1),
    /conflicts with an existing filter/,
  )
})

test('hitting the default limit is visible to a debug logger', async () => {
  const messages: string[] = []
  const client = makeClient({
    maxRowsPerShard: 60,
    logger: { debug: (...args: any[]) => messages.push(args.map(String).join(' ')) },
  })
  const table = client.useDatabase('default_limit').defineTable('rows', { seq: 'number' })
  await table.insertMany(Array.from({ length: 101 }, (_, index) => ({ seq: index })))

  const page = await table.find({}).page()
  assert.equal(page.documents.length, 100)
  assert.equal(page.hasMore, true)
  assert.ok(messages.some((message) => message.includes('default limit of 100')))
})
