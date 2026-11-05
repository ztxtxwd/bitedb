# bitedb

[![npm](https://img.shields.io/npm/v/bitedb.svg)](https://www.npmjs.com/package/bitedb)
[![CI](https://github.com/ztxtxwd/bitedb/actions/workflows/ci.yml/badge.svg)](https://github.com/ztxtxwd/bitedb/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

**把飞书 / Lark 多维表格变成一个无头（headless）的 Serverless NoSQL 数据库。**

你只提供 `appId` + `appSecret`。建 Base、建表、建字段、读写、分片、大文件转储、双向关联、公式列全部由代码完成。开发者不需要打开多维表格，也不需要知道 `app_token`、`table_id`、`field_id` 长什么样。

```ts
import { createClient } from 'bitedb'

const db = createClient({
  appId: process.env.FEISHU_APP_ID!,
  appSecret: process.env.FEISHU_APP_SECRET!,
})

const orders = db.useDatabase('production').defineTable('orders', {
  sku: 'text',
  price: 'number',
  count: 'number',
  total: { type: 'computed', expr: 'price * count' },
  contract: { type: 'json', spill: true },
  attachments: { type: 'blob' },
})

const order = await orders.insertOne({ sku: 'A-1', price: 100, count: 3 })

const expensive = await orders
  .find({ total: { $gte: 200 } })
  .sort('total', 'desc')
  .limit(20)
  .toArray()
```

零运行时依赖。Node.js >= 18.17。ESM 与 CommonJS 均可。

> bitedb 不是飞书官方产品。它只是飞书开放平台 API 之上的一层数据库语义封装。
>
> 当前版本 0.1.0：核心能力已可用，API 仍可能随实践演进。

---

## 它到底做了什么

| 能力 | 开发者看到的 | bitedb 在底层做的事 |
| --- | --- | --- |
| 零预置 | `client.useDatabase('prod')` | 在云空间查找名为 `bitedb:prod` 的 Base；不存在则调用 API 静默创建 |
| 系统目录 | 无感知 | 自动维护 `__bitedb_catalog__` Base，保存库名 ↔ Base token、表 ↔ 物理表、分片与行数 |
| 动态 DDL | 直接 `insert({ a: 1, b: 'x' })` | 不存在表就建表；不存在字段就按 JS 类型建 Bitable 字段 |
| 公式列 | `{ total: { type: 'computed', expr: 'price * count' } }` | 创建 Formula 字段，表达式编译成 Bitable 机器语法；筛选/排序可直接使用 |
| 写缓冲 | 50ms 内并发 20 次 `insertOne` | 合并成 1 次 `records/batch_create`，消耗 1 次配额 |
| 自动分片 | 仍然查询 `logs` 一张表 | 单表接近 20,000 行时自动建 `logs#1`、`logs#2`；查询自动合并 |
| Blob | `insert({ avatar: buffer })` | 上传到飞书云盘，单元格只存 file_token；超过 20MB 自动分片上传 |
| 双向关联 | `{ author: { type: 'link', table: 'users' } }` | 创建 DuplexLink；反向字段自动出现在目标表；读写使用逻辑 `_id` |
| 目录丢失恢复 | 继续用同一套代码 | 按 Base 名和物理表名重新收养已有数据，而不是重建 |

---

## 安装

```bash
npm install bitedb
```

---

## 飞书应用配置

1. 在[飞书开放平台](https://open.feishu.cn/app)创建企业自建应用。
2. 开通权限（任选粗细粒度）：
   - `bitable:app`：多维表格完整读写；或使用更细的 `base:app:create`、`base:table:*`、`base:record:*`、`base:field:*` 等。
   - `drive:drive`：列出云空间文件、创建 Base、上传/下载素材。
3. 发布应用版本，并由企业管理员审批。
4. 把 `appId` / `appSecret` 放在服务端环境变量里。**永远不要把 secret 放进浏览器代码。**

如果使用 `folderToken`，请确保应用有该文件夹的编辑权限。使用 tenant access token 时，应用只能操作自己创建的文件夹；最简单的做法是省略 `folderToken`，让 bitedb 在云空间根目录创建自己的文件。

Lark 国际版把 `domain` 改成 `https://open.larksuite.com`。

也支持直接传 `accessToken`，或传一个每次请求都会调用的 token provider；此时无需 `appId` / `appSecret`。

---

## 快速开始

```ts
import { createClient } from 'bitedb'

const db = createClient({
  appId: process.env.FEISHU_APP_ID!,
  appSecret: process.env.FEISHU_APP_SECRET!,
})

// 没有就自动创建；有就复用
const database = db.useDatabase('production_order_db')

const users = database.defineTable('users', {
  name: 'text',
  email: 'text',
  created_at: 'date',
})

const orders = database.defineTable('orders', {
  sku: 'text',
  price: 'number',
  count: 'number',
  status: { type: 'select', options: ['new', 'paid', 'shipped'] },
  total: { type: 'computed', expr: 'price * count' },
  owner: { type: 'link', table: 'users' },
  contract: { type: 'json', spill: true },
  attachments: { type: 'blob' },
})

const ada = await users.insertOne({ name: 'Ada', email: 'ada@example.com' })
const order = await orders.insertOne({
  sku: 'A-1',
  price: 100,
  count: 3,
  status: 'paid',
  owner: ada._id,
  contract: { terms: 'net-30', files: ['a.pdf'] },
  attachments: Buffer.from('...'),
})

const rows = await orders
  .find({ total: { $gte: 200 }, status: 'paid' })
  .sort('total', 'desc')
  .limit(20)
  .toArray()
```

`insertOne` 返回的文档立即包含 `_id`，但 Formula 列由飞书异步计算，因此不在写入返回值里；再次读取时可见。

---

## 客户端与句柄

```ts
const db = createClient({
  appId: 'cli_xxx',
  appSecret: 'xxx',

  // 或使用别名
  // clientId: 'cli_xxx',
  // clientSecret: 'xxx',

  // 常用可选项
  domain: 'https://open.feishu.cn',
  folderToken: 'fldrxxx',
  databasePrefix: 'bitedb:',
  batchWindowMs: 50,
  maxBatchSize: 500,
  maxRowsPerShard: 20000,
  blobThresholdBytes: 32 * 1024,
  hydrate: 'auto', // 'auto' | 'eager' | 'never'
  bitableQps: 18,
  driveQps: 4,
})

const database = db.useDatabase('production')  // 懒加载
await database.ensure()                        // 只创建 / 发现 Base

const table = database.defineTable('orders', schema)
const same = database.table('orders')          // 同一句柄
const alias = database.collection('orders')    // 别名

// 已声明的表会在首次读写时逐个创建；需要提前预热时，并行创建它们
await database.prewarm()

await db.listDatabases()
const catalogToken = await db.catalogBaseToken()
await db.flush()
await db.close()
```

`useDatabase()` 是同步的，不会发请求。第一次真正的读写才会触发 Base / 表 / 字段的创建。

---

## Schema

```ts
type SchemaDefinition = Record<string, FieldType | FieldDefinition>
```

### 字段类型

| `type` | Bitable 类型 | JS 读写行为 |
| --- | --- | --- |
| `text` / `string` | 文本（1） | string；非字符串值会被编码保存并在读取时还原 |
| `number` | 数字（2） | number |
| `boolean` | 复选框（7） | boolean |
| `date` | 日期（5） | 写入 `Date` / 毫秒时间戳 / ISO 字符串；读取 `Date` |
| `json` | 文本（1），内容为 JSON | 任意可序列化值；大值可自动转储 |
| `blob` | 附件（17） | Buffer / string / object 自动上传；读取见 Blob 一节 |
| `attachment` | 附件（17） | 返回 `BlobRef[]`，保留原始附件语义 |
| `select` | 单选（3） | string |
| `multiSelect` | 多选（4） | string[] |
| `link` | 双向/单向关联（21/18） | 逻辑 `_id`（默认数组） |
| `computed` | 公式（20） | 只读；由飞书计算 |
| `raw` | 任意 | 原样透传，需提供 `feishuType` |

### 字段选项

```ts
{
  type: 'number',
  name: 'order_total',      // 物理字段名，默认等于 key
  description: '含税总价',
  spill: true,              // text/json：超过阈值的值转储到云盘
}
```

### 计算列（computed）

```ts
const orders = database.defineTable('orders', {
  price: 'number',
  count: 'number',
  total: { type: 'computed', expr: 'price * count', resultType: 'number' },
})
```

- `expr` 中的裸标识符按字段 key / 物理字段名解析；带空格的字段用 `{unit price}` 引用。
- bitedb 会把表达式编译成 Bitable 机器语法 `bitable::$table[tbl...].$field[fld...]`，所以字段改名/重排不会影响引用。
- 也可以直接传 `bitable::$table[...].$field[...]` 原始表达式，bitedb 会原样透传。
- 不同部署对公式字段的创建行为不完全一致；bitedb 会创建公式字段，再通过更新字段接口写入表达式；若创建阶段不接受表达式，会自动降级为“先建空公式字段、再更新”的两步路径。
- 某些 Base 的公式引擎要求显式声明结果类型；此时设置 `resultType: 'number' | 'text' | 'boolean' | 'date' | 'json'`。
- 计算列只读，写入时会被忽略；写入返回的文档不包含刚算出的公式值，重新读取即可获得。
- 当前不支持跨表公式引用。

### 无 Schema 写入

没有 `defineTable` 也可以直接写：

```ts
const events = db.useDatabase('logs').table('events')

await events.insertOne({
  message: 'boot',
  level: 2,
  ok: true,
  payload: { region: 'cn' },
  at: new Date(),
})
```

bitedb 会按值的类型自动创建字段：string → 文本、number → 数字、boolean → 复选框、Date → 日期、对象/数组 → JSON 文本、Buffer → 附件。之后同名字段持续复用；新出现的字段会动态追加到所有分片。

`_id` 自动生成（26 位、按时间可排序的 ULID 风格字符串），也可以自己传入。当前没有服务端唯一约束：重复 `_id` 只能在同一进程的缓存范围内被发现。

---

## 查询

```ts
await orders.find({ status: 'paid' }).toArray()
await orders.find({ total: { $gte: 200 } }).sort('total', 'desc').limit(20).toArray()
await orders.find({}).sort([['status', 'asc'], ['total', 'desc']]).offset(40).limit(20).toArray()
await orders.find({ sku: { $in: ['A-1', 'A-2'] } }).toArray()
await orders.find({ $or: [{ status: 'paid' }, { total: { $gt: 1000 } }] }).toArray()
await orders.findOne({ sku: 'A-1' })
await orders.get(orderId)
await orders.countDocuments({ status: 'paid' })
await orders.find({}).fields('sku', 'total').limit(10).toArray()

// 分页：page() 额外返回 hasMore，用于判断结果是否被 limit 截断
const { documents, hasMore } = await orders.find({ status: 'paid' }).limit(50).page()

// 游标分页：按唯一字段（推荐 _id）翻页，开销不随页码增长
const next = await orders.findAfter('_id', lastId, { limit: 50 }).toArray()

// 已有筛选条件时直接用 Query.after() 追加游标
await orders
  .find({ status: 'paid' })
  .sort('_id', 'asc')
  .after('_id', lastId)
  .limit(50)
  .toArray()
```

支持的筛选操作符：

| 操作符 | 含义 |
| --- | --- |
| `$eq` / 直接值 | 等于 |
| `$ne` | 不等于 |
| `$gt` `$gte` `$lt` `$lte` | 比较 |
| `$in` / `$nin` | 集合 |
| `$contains` / `$notContains` | 文本包含 / 多选包含 |
| `$exists` | 非空 / 为空 |
| `$and` / `$or` | 逻辑组合 |

说明：

- 条件由 Bitable 服务端执行；`total` 这类公式列可以直接筛选和排序。
- Bitable 的 filter 只支持一层布尔嵌套，并且每次查询最多一个 OR 分组。更复杂的查询请拆分，或拉取后在应用层过滤。
- 日期按毫秒时间戳比较；数组条件表示“任意一个匹配”。
- 默认 `limit` 为 100，超出部分会被丢弃，`toArray()` 不会报错。需要更多请显式 `limit()`；或用 `Query.page()` 拿到 `hasMore`；配置了 `logger.debug` 时，命中默认上限会输出一条提示。
- `offset` 是“先拉取后切片”：每个分片最多拉取 `offset + limit` 行，深分页成本随 offset 线性增长。大数据量翻页请用 `findAfter()` / `Query.after()` 做游标分页；游标字段需要唯一（推荐 `_id`），重复值可能被跳过。
- 跨分片排序会在客户端做最终归并。
- 未指定排序时，顺序是“分片顺序 + 每个分片内的 API 顺序”，不是稳定业务顺序。
- 字符串排序的最终归并使用 JS 比较；当分片内存在中文等复杂排序时，可能与飞书视图的排序规则略有差异。

---

## 写入、更新、删除

```ts
await table.insertOne(doc)
await table.insertMany(docs)     // 自动走批处理
await table.insert(doc)          // insertOne 别名

await table.updateOne(id, { $set: { status: 'shipped' } })
await table.updateOne({ sku: 'A-1' }, { status: 'shipped' })
await table.updateById(id, { $inc: { count: 2 } })
await table.updateMany({ status: 'new' }, { $set: { status: 'queued' } })

// 直接拿到更新后的文档，省去一次回读
const result = await table.updateById(id, { $set: { status: 'shipped' } }, { returnDocument: 'after' })
result.document // => 更新后的完整文档

// updateMany 返回按匹配顺序排列的 documents
await table.updateMany(
  { status: 'new' },
  { $set: { status: 'queued' } },
  { returnDocument: 'after' },
)

await table.deleteOne(id)
await table.deleteMany({ status: 'cancelled' })
```

支持的更新操作符：`$set`、`$unset`、`$inc`、`$push`。也可以直接传字段补丁。

`returnDocument: 'before' | 'after'` 让更新返回文档：`updateOne` / `updateById` 放在 `document`，`updateMany` 放在按匹配顺序排列的 `documents`。默认不返回（零额外开销）。`after` 文档由“更新前的行 + 补丁”在本地组装，不会多发一次读请求；公式列在飞书异步重算前可能仍是旧值。

`$inc` / `$push` 是“读取当前值 → 计算 → 写回”，不是原子操作。并发写同一行可能丢失更新。

### 写缓冲

同一进程内，`batchWindowMs` 窗口内到达的写入会按物理分片合并：

```ts
await Promise.all(
  Array.from({ length: 200 }, (_, i) => orders.insertOne({ seq: i })),
)
// 通常只产生 1 次 batch_create（受 maxBatchSize 限制）
```

`batch_create` 单次上限 1000 条；默认 `maxBatchSize` 为 500。一次批量请求要么整体成功，要么整体失败；如果失败，本次批量中的调用都会 reject。

---

## Blob、附件与自动转储

### `blob` 字段

```ts
await files.insertOne({
  avatar: Buffer.from(...),        // 二进制
  article: '# hello',              // 文本
  state: { nested: ['tree'] },     // JSON
})
```

- Buffer / Uint8Array / ArrayBuffer → 二进制附件
- string → 文本文件
- object / array → JSON 文件
- 单个文件 ≤ 20MB 走 `medias/upload_all`；> 20MB 自动走 `upload_prepare` → `upload_part`（4MB 分片，`seq` 从 0）→ `upload_finish`
- 读取时：
  - `hydrate: 'auto'`（默认）：文本/JSON 自动还原；二进制返回 `BlobRef`
  - `hydrate: 'eager'`：二进制也下载成 Buffer
  - `hydrate: 'never'`：全部返回 `BlobRef`

```ts
const read = await files.get(id)
if (read.avatar instanceof BlobRef) {
  const bytes = await read.avatar.bytes()
  const buffer = await read.avatar.arrayBuffer()
  const text = await read.avatar.text()
  const json = await read.avatar.json()
}
```

### `attachment` 字段

保留附件的原始语义，读取总是返回 `BlobRef[]`，不会自动下载：

```ts
const refs = read.receipts
for (const ref of refs) {
  console.log(ref.fileToken, ref.name, ref.size, ref.url)
  const bytes = await ref.bytes()
}
```

### `text` / `json` 自动转储

`json` 字段默认在 `blobThresholdBytes`（32KB）以上自动上传到云盘，单元格里只留一个小引用。`text` 字段也可以开启：

```ts
{ type: 'text', spill: true }
```

读取时默认自动取回并还原，调用方无感。适合几十 MB 的日志快照、AST、JSON 状态树；二进制超大对象请直接用 `blob`。

> 飞书素材上传限流约为 5 QPS、10,000 次/天，bitedb 默认用 4 QPS 的令牌桶节流，并对同进程内相同内容做去重。

---

## 关联（Links）

```ts
const users = database.defineTable('users', { name: 'text' })
const posts = database.defineTable('posts', {
  title: 'text',
  author: { type: 'link', table: 'users' },             // 默认双向、可关联多条
  reviewer: { type: 'link', table: 'users', multiple: false },
})

const ada = await users.insertOne({ name: 'Ada' })
await posts.insertOne({ title: 'Hello', author: ada._id })
```

- 默认创建 DuplexLink（类型 21）。飞书会在目标表自动建立反向字段；bitedb 会把它收养进目标表的 schema。
- 反向字段的 key 使用物理字段名，可通过 `await users.schema()` 找到 `kind === 'link'` 且 `target === 'posts'` 的字段。
- 读写都使用逻辑 `_id`：写入时自动解析为 Bitable `record_id`，读取时反向解析；按关联字段过滤时也会自动翻译。
- 只需在一侧声明关联；两侧都声明会产生两条独立关联。
- 原生 Bitable 关联只能指向目标表的分片 0。如果目标记录落在 `#1` 及以后分片，写入关联会抛出明确错误。
- 反向关联由目标表的分片 0 维护；源表落在后续分片的记录不会出现在反向字段中。
- `multiple: false` 时读取返回单个 `_id` 或 `null`，其余情况返回数组。

---

## 自动分片

Bitable 单表有行数上限。bitedb 在物理层自动分片：

- 默认每个分片最多 20,000 行（`maxRowsPerShard`，上限 20,000）。
- 物理表命名：`orders`、`orders#1`、`orders#2` …
- 逻辑 API 不变：`database.table('orders')`。
- 查询会并发访问各分片、合并结果；排序查询在客户端做最终归并。
- 插入时选择第一个有余量的分片；行数计数器持久化在系统目录中，批量写入时合并刷新。
- Bitable 每个 Base 最多 100 张表。bitedb 默认 `maxTablesPerBase: 90`，为目录/默认表留余量。

限制：

- 分片行数计数是尽力而为的。多进程/多实例同时写入时可能短暂超出单分片容量；需要时调用 `table.recount()` 从 Bitable 重新读取真实行数。
- 没有服务端全局分页游标；请用 `findAfter()` / `Query.after()` 基于唯一字段（推荐 `_id`）做游标分页。大偏移量查询会让每个分片拉取 `offset + limit` 行。
- 关联字段与分片组合有上述分片 0 限制。

---

## 系统目录与灾难恢复

bitedb 在云空间根目录（或 `folderToken`）维护：

```text
__bitedb_catalog__          <- 系统 Base
  databases                 <- 库名 -> Base token
  tables                    <- 库/表 -> schema、物理分片、行数
```

数据库 Base 默认命名为 `bitedb:<databaseName>`，可通过 `databasePrefix` 修改。

- **目录发现**：客户端启动时先按名称找到目录 Base；找不到就创建。`databasePrefix` 是恢复时匹配数据库 Base 的依据，请保持稳定。
- **固定目录**：多进程/Serverless 环境建议固定 `catalogBaseToken`，或设置环境变量 `BITEDB_CATALOG_BASE_TOKEN`，避免并发首次启动时创建多个目录。
- **目录丢失恢复**：如果目录记录丢失，bitedb 会按 `bitedb:<name>` 收养已有数据库 Base，并按物理表名（含 `#N`）收养已有分片；同时从 Bitable 重新读取字段与真实行数。数据不会因为目录丢失而重建。
- **冷启动**：表与字段默认在首次读写时按需创建。提前 `defineTable()` 后调用 `database.prewarm()` 会并行初始化所有已注册的表句柄；关联/公式字段仍按依赖顺序创建。字段级 DDL 不并行，以免同一张表上的并发建字段互相冲突。
- **默认表**：飞书创建 Base 时会自带一张默认表，bitedb 不依赖、也不会自动删除它。

---

## 配置项

| 选项 | 默认值 | 说明 |
| --- | --- | --- |
| `appId` / `appSecret` | — | 飞书应用凭证 |
| `clientId` / `clientSecret` | — | 上述二者的别名 |
| `accessToken` | — | 直接提供 token，或 `() => string \| Promise<string>` |
| `domain` | `https://open.feishu.cn` | Lark 国际版用 `https://open.larksuite.com` |
| `folderToken` | 根目录 | 创建与发现 Base 的目标文件夹；请保持稳定，或固定 `catalogBaseToken` |
| `catalogName` | `__bitedb_catalog__` | 系统目录 Base 名称 |
| `catalogBaseToken` | — | 固定目录 token（也支持 `BITEDB_CATALOG_BASE_TOKEN`） |
| `databasePrefix` | `bitedb:` | 自动创建的数据库 Base 名称前缀 |
| `batchWindowMs` | `50` | 写入合并窗口 |
| `maxBatchSize` | `500` | 单次 `batch_create` 条数，最大 1000 |
| `maxRowsPerShard` | `20000` | 单物理表行数上限，最大 20000 |
| `maxTablesPerBase` | `90` | bitedb 在单个 Base 中创建的最大物理表数，最大 100 |
| `blobThresholdBytes` | `32768` | 自动转储阈值 |
| `hydrate` | `auto` | `auto` / `eager` / `never` |
| `requestTimeoutMs` | `30000` | 单请求超时 |
| `retries` | `3` | 可重试失败的重试次数 |
| `bitableQps` | `18` | Bitable 请求令牌桶速率 |
| `driveQps` | `4` | 云盘上传/下载令牌桶速率 |
| `catalogFlushMs` | `1000` | 行数等目录元数据的合并落盘间隔 |
| `logger` | 静默 | `{ debug?, info?, warn?, error? }` |
| `fetch` | 全局 fetch | 自定义 fetch（代理、测试、边缘运行时） |

---

## API 速查

```ts
createClient(config): BiteDB

class BiteDB {
  useDatabase(name): Database
  database(name): Database
  createDatabase(name): Promise<Database>
  listDatabases(): Promise<DatabaseInfo[]>
  catalogBaseToken(): Promise<string>
  flush(): Promise<void>
  close(): Promise<void>
}

class Database {
  defineTable<T>(name, schema?): Table<T>
  table<T>(name, schema?): Table<T>
  collection<T>(name, schema?): Table<T>
  listTables(): Promise<TableInfo[]>
  dropTable(name): Promise<void>
  refresh(): Promise<void>
  ensure(): Promise<DatabaseMeta>
  prewarm(): Promise<void>
}

class Table<T> {
  insertOne(doc): Promise<WithId<T>>
  insertMany(docs): Promise<WithId<T>[]>
  insert(doc): Promise<WithId<T>>
  find(filter?, options?): Query<T>
  findAfter(field, lastValue, options?): Query<T>
  findOne(filter?, options?): Promise<WithId<T> | null>
  get(id): Promise<WithId<T> | null>
  countDocuments(filter?): Promise<number>
  updateOne(idOrFilter, update, options?): Promise<UpdateResult>
  updateById(id, update, options?): Promise<UpdateResult>
  updateMany(filter, update, options?): Promise<UpdateResult>
  deleteOne(idOrFilter): Promise<DeleteResult>
  deleteMany(filter?): Promise<DeleteResult>
  defineSchema(schema): this
  schema(): Promise<StoredField[]>
  stats(): Promise<TableStats>
  recount(): Promise<TableStats>
  flush(): Promise<void>
  drop(): Promise<void>
}

class Query<T> {
  sort(field, direction?): this
  sort(spec): this
  limit(n): this
  offset(n): this
  skip(n): this
  fields(...names): this
  after(field, value): this
  toArray(): Promise<WithId<T>[]>
  page(): Promise<{ documents: WithId<T>[]; hasMore: boolean }>
  first(): Promise<WithId<T> | null>
  count(): Promise<number>
}
```

`UpdateOptions` 支持 `returnDocument: 'before' | 'after'`；`UpdateResult` 相应提供可选的 `document` / `documents`。

错误类型：`BiteDBError`、`ConfigError`、`ValidationError`、`NotFoundError`、`UnsupportedError`、`FeishuApiError`。

`FeishuApiError` 暴露 `status`、`apiCode`、`apiMsg`、`method`、`path`、`errorLogId`、`response`，便于排查权限和字段类型问题。

---

## 边界与一致性

bitedb 把 Bitable 当作存储引擎，但 Bitable 不是事务数据库。请明确这些边界：

- **Schema 变更是增量式的**：bitedb 自动建表、加字段、建公式/关联；不会自动修改字段类型、重命名或删除字段。需要大规模迁移时，建议新表 + 双写/回填。
- **没有事务 / 原子多行更新**。`insertMany` 可能部分成功；跨分片操作不是原子的。
- **没有服务端唯一索引**。`_id` 重复仅在进程缓存里尽量发现。
- **`$inc` / `$push` 非原子**，并发写同一行可能丢失更新。
- **多进程 DDL/分片计数是尽力而为的**。bitedb 会在初始化时扫描并收养目录里缺失的物理分片，但目录写入本身没有 CAS。建议同一个逻辑表只有一个写入者；或者接受偶发的容量偏差。
- **Formula 由飞书异步计算**。刚写入后立刻读取可能读到空值。
- **原生关联只能指向目标表分片 0**，反向关联也只覆盖源表分片 0。
- **筛选/排序依赖 Bitable 的查询能力**。Bitable 自身维护查询所需的底层索引，bitedb 不额外创建二级索引；复杂布尔嵌套会抛 `UnsupportedError`，不会静默返回错误结果。
- **高级权限**：开启多维表格高级权限后，调用身份必须拥有相应记录的可见/可管理权限，否则 API 可能返回空数据。
- **速率**：Bitable 约 20 QPS，素材上传约 5 QPS / 10,000 次/天（以飞书当前文档为准）。bitedb 通过批处理、令牌桶和重试降低触发限流的概率，但不能突破平台限制。
- **默认查询上限** 为 100 条；需要更多请显式 `limit()`，或用 `page()` 检查 `hasMore`。
- **进程退出前调用 `await db.flush()` 或 `await db.close()`**。写入合并窗口和目录行数落盘都有定时器；Serverless 函数应在返回前等待写入 Promise，必要时显式 flush。
- **高级权限 Base 的素材下载**：bitedb 会优先使用附件元数据里的 `url` / `tmp_url`（其中包含高级权限所需的 `extra`），失败再回退到按 `file_token` 下载。文本单元格里的自动转储标记没有 URL，在开启高级权限的 Base 上可能无法下载；这类 Base 建议用显式的 `blob` / `attachment` 字段。
- **已转储的大值**在 Bitable 单元格里只是一个内部引用。基于该单元格内容的服务端筛选/排序，以及引用该字段的 Formula，看到的都是引用而不是原始内容；需要计算/筛选的字段请保持小值或关闭 `spill`。
- **文本字段的混合类型**通过内部 JSON 信封保存，以便读回原类型；这类单元格在飞书 UI 中看起来是 JSON 字符串。
- **Bitable 限额**：每个 Base 100 张表；单表约 20,000 行；单次批量 1000 条；单次搜索 500 条；单文件简单上传 20MB。平台可能调整这些数字。

---

## 架构

```text
你的代码
  │  createClient / useDatabase / table / insert / find
  ▼
┌──────────────────────────────────────────────────────────┐
│ bitedb                                                   │
│                                                          │
│  Database ── Table ── Query                              │
│      │         │                                         │
│      │         ├─ 动态 DDL（建表 / 加字段 / 公式 / 关联） │
│      │         ├─ Batcher（50ms 合并 → batch_create）     │
│      │         ├─ Shard Allocator（20k 行自动分片）       │
│      │         ├─ Codec（类型 / 日期 / JSON / 链接）      │
│      │         └─ BlobManager（附件 / 转储 / 分片上传）   │
│      │                                                   │
│      └─ Catalog（databases / tables / schema / shards）  │
│                                                          │
│  FeishuClient（auth、重试、令牌桶、分页）                 │
└──────────────────────────────────────────────────────────┘
  │
  ▼
飞书开放平台 OpenAPI  ──  Bitable / Drive
```

---

## 开发与测试

```bash
npm install
npm test        # 使用内存版飞书 API，无需真实凭证
npm run build   # ESM + CJS + d.ts
npm run typecheck
```

### 性能基准

```bash
FEISHU_APP_ID=cli_xxx FEISHU_APP_SECRET=xxx npm run bench
npm run bench:mock   # 内存 fake，仅验证脚本与本地开销
```

`bench/perf.ts` 会依次测量冷/热启动、批量与逐条写入、各类查询、更新和删除，
输出 ops/s、p50/p95/p99、实际 API 调用次数，并把原始结果写入 `bench/results.json`
（使用 `--json` 指定路径）。参数、场景说明和一次参考结果见
[`bench/README.md`](./bench/README.md)。

测试覆盖：零预置建库、目录发现与重连、目录丢失恢复（含长表名与反向关联）、动态 DDL、写入合并、公式列（含创建阶段降级路径）、筛选操作符、跨分片查询、Blob 转储与 21MB 分片上传、附件语义、双向关联、CRUD、drop 后复用。

---

## 发布

发版统一通过 GitHub Actions 完成，使用 npm Trusted Publishing（OIDC），仓库和 CI 中不保存任何 npm token。

1. 更新 `package.json` 中的 `version`，提交并推送到 `main`。
2. 在 GitHub 上创建与版本号一致的标签和 Release，例如 `v0.2.0`。
3. `Publish` workflow 会自动执行 typecheck、测试、构建，并通过 OIDC 发布到 npm（附带 provenance）。

也可以在 Actions 页面手动触发 `Publish` workflow，勾选 `dry-run` 可只验证打包、不真正发布。

首次配置：在 npm 包的 **Settings → Trusted publishing** 中添加 GitHub 仓库 `ztxtxwd/bitedb`，workflow 文件名填写 `publish.yml`。

---

## License

MIT
