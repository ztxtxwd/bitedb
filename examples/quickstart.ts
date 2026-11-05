/**
 * Run with:
 *   FEISHU_APP_ID=cli_xxx FEISHU_APP_SECRET=xxx npx tsx examples/quickstart.ts
 */
import { BlobRef, createClient } from 'bitedb'

const db = createClient({
  appId: process.env.FEISHU_APP_ID!,
  appSecret: process.env.FEISHU_APP_SECRET!,
})

const database = db.useDatabase('bitedb_demo')
const users = database.defineTable('users', {
  name: 'text',
  email: 'text',
})
const orders = database.defineTable('orders', {
  sku: 'text',
  price: 'number',
  count: 'number',
  total: { type: 'computed', expr: 'price * count' },
  status: { type: 'select', options: ['new', 'paid', 'shipped'] },
  owner: { type: 'link', table: 'users' },
  notes: { type: 'json', spill: true },
  attachments: { type: 'blob' },
})

const owner = await users.insertOne({ name: 'Ada', email: 'ada@example.com' })
await orders.insertOne({
  sku: 'A-1',
  price: 100,
  count: 3,
  status: 'paid',
  owner: owner._id,
  notes: { source: 'demo', tags: ['fresh'] },
  attachments: Buffer.from('hello from bitedb'),
})

const top = await orders
  .find({ total: { $gte: 200 } })
  .sort('total', 'desc')
  .limit(10)
  .toArray()

console.log(top)

const one = await orders.get(top[0]!._id)
if (one?.attachments instanceof BlobRef) {
  console.log('attachment:', await one.attachments.text())
}

await db.close()
