/**
 * bitedb performance benchmark.
 *
 * Real Feishu / Lark:
 *   FEISHU_APP_ID=cli_xxx FEISHU_APP_SECRET=xxx npx tsx bench/perf.ts
 *
 * Local in-memory Feishu API (no network, no credentials):
 *   npx tsx bench/perf.ts --mock
 *
 * Run `npx tsx bench/perf.ts --help` for all options.
 */
import { performance } from 'node:perf_hooks'
import { writeFile } from 'node:fs/promises'
import { createClient, FeishuApiError, type BiteDB } from '../src/index.js'
import type { SchemaDefinition, WithId } from '../src/index.js'

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

interface Options {
  docs: number
  sequential: number
  readOps: number
  concurrency: number
  qps?: number
  batchSize: number
  batchWindowMs: number
  database: string
  table: string
  fresh: boolean
  keepData: boolean
  json?: string
  mock: boolean
  domain?: string
  appId?: string
  appSecret?: string
  help: boolean
}

const DEFAULTS: Options = {
  docs: 2000,
  sequential: 20,
  readOps: 60,
  concurrency: 8,
  qps: undefined,
  batchSize: 500,
  batchWindowMs: 50,
  database: 'bitedb_perf',
  table: 'bench',
  fresh: false,
  keepData: false,
  json: undefined,
  mock: false,
  domain: undefined,
  appId: undefined,
  appSecret: undefined,
  help: false,
}

function parseOptions(argv: string[]): Options {
  const opts: Options = { ...DEFAULTS }
  const int = (raw: string, flag: string): number => {
    const value = Number(raw)
    if (!Number.isFinite(value) || value < 0) throw new Error(`${flag} expects a non-negative number`)
    return Math.floor(value)
  }

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]!
    const value = (): string => {
      const v = argv[++i]
      if (v === undefined) throw new Error(`${arg} requires a value`)
      return v
    }
    switch (arg) {
      case '--docs':
        opts.docs = int(value(), arg)
        break
      case '--sequential':
        opts.sequential = int(value(), arg)
        break
      case '--read-ops':
        opts.readOps = int(value(), arg)
        break
      case '--concurrency':
        opts.concurrency = Math.max(1, int(value(), arg))
        break
      case '--qps':
        opts.qps = Math.max(1, int(value(), arg))
        break
      case '--batch-size':
        opts.batchSize = Math.max(1, int(value(), arg))
        break
      case '--batch-window':
        opts.batchWindowMs = int(value(), arg)
        break
      case '--database':
        opts.database = value()
        break
      case '--table':
        opts.table = value()
        break
      case '--domain':
        opts.domain = value()
        break
      case '--app-id':
        opts.appId = value()
        break
      case '--app-secret':
        opts.appSecret = value()
        break
      case '--json':
        opts.json = value()
        break
      case '--fresh':
        opts.fresh = true
        break
      case '--keep-data':
        opts.keepData = true
        break
      case '--mock':
        opts.mock = true
        break
      case '-h':
      case '--help':
        opts.help = true
        break
      default:
        throw new Error(`Unknown option: ${arg}`)
    }
  }
  return opts
}

function printUsage(): void {
  console.log(`bitedb performance benchmark

Usage:
  FEISHU_APP_ID=cli_xxx FEISHU_APP_SECRET=xxx npx tsx bench/perf.ts [options]
  npx tsx bench/perf.ts --mock [options]

Options:
  --docs N            Documents to insert in the burst write (default 2000)
  --sequential N      Documents inserted one-by-one without batching (default 20)
  --read-ops N        Operations per read/update scenario (default 60)
  --concurrency N     Concurrent in-flight operations (default 8)
  --qps N             Bitable request rate limit (default 18; unlimited in --mock)
  --batch-size N      maxBatchSize passed to bitedb (default 500)
  --batch-window N    batchWindowMs passed to bitedb (default 50)
  --database NAME     Database name (default bitedb_perf)
  --table NAME        Table name (default bench)
  --fresh             Use a unique database name for a cold-start measurement
  --keep-data         Do not delete benchmark rows at the end
  --json PATH         Write raw results as JSON
  --domain URL        open.feishu.cn (default) or https://open.larksuite.com
  --app-id / --app-secret   Override FEISHU_APP_ID / FEISHU_APP_SECRET
  --mock              Run against the in-memory fake Feishu API
  -h, --help          Show this help
`)
}

// ---------------------------------------------------------------------------
// Document model
// ---------------------------------------------------------------------------

interface BenchDoc {
  run: string
  seq: number
  sku: string
  price: number
  count: number
  active: boolean
  created_at: Date
  status: string
  payload: { region: string; tags: string[]; i: number }
  note: string
}

const SCHEMA: SchemaDefinition = {
  run: 'text',
  seq: 'number',
  sku: 'text',
  price: 'number',
  count: 'number',
  active: 'boolean',
  created_at: 'date',
  status: { type: 'select', options: ['new', 'paid', 'shipped'] },
  payload: 'json',
  note: 'text',
}

const REGIONS = ['cn', 'us', 'eu']
const STATUSES = ['new', 'paid', 'shipped']

function makeDoc(run: string, i: number): BenchDoc {
  return {
    run,
    seq: i,
    sku: `SKU-${String(i).padStart(6, '0')}`,
    price: (i % 100) + 0.5,
    count: (i % 7) + 1,
    active: i % 2 === 0,
    created_at: new Date(Date.now() - i * 1000),
    status: STATUSES[i % STATUSES.length]!,
    payload: { region: REGIONS[i % REGIONS.length]!, tags: ['perf', `batch-${i % 10}`], i },
    note: `benchmark row ${i}`,
  }
}

// ---------------------------------------------------------------------------
// Request accounting
// ---------------------------------------------------------------------------

let totalCalls = 0
const callCounts = new Map<string, number>()

function endpointBucket(path: string): string {
  if (path.includes('tenant_access_token') || path.includes('app_access_token')) return 'auth.token'
  if (path.endsWith('/records/search')) return 'records.search'
  if (path.endsWith('/records/batch_create')) return 'records.batch_create'
  if (path.endsWith('/records/batch_update')) return 'records.batch_update'
  if (path.endsWith('/records/batch_delete')) return 'records.batch_delete'
  if (/\/records\/[^/]+$/.test(path)) return 'records.item'
  if (/\/fields(\/[^/]+)?$/.test(path)) return 'fields'
  if (/\/tables(\/[^/]+)?$/.test(path)) return 'tables'
  if (/\/apps(\/[^/]+)?$/.test(path)) return 'apps'
  if (path.includes('/files')) return 'drive.files'
  if (path.includes('/medias')) return 'drive.medias'
  return path
}

function countCall(path: string): void {
  const bucket = endpointBucket(path)
  callCounts.set(bucket, (callCounts.get(bucket) ?? 0) + 1)
  totalCalls++
}

const countingFetch: typeof fetch = async (input, init) => {
  try {
    const url =
      input instanceof Request ? input.url : typeof input === 'string' ? input : input.href
    countCall(new URL(url).pathname)
  } catch {
    /* not a URL we can parse; ignore */
  }
  return globalThis.fetch(input, init)
}

// ---------------------------------------------------------------------------
// Timing helpers
// ---------------------------------------------------------------------------

type Group = 'write' | 'read' | 'update' | 'delete'

interface ScenarioResult {
  group: Group
  name: string
  ops: number
  wallMs: number
  latencies: number[]
  apiCalls: number
  failed?: string
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1))
  return sorted[index]!
}

async function runPool<T>(
  count: number,
  concurrency: number,
  fn: (i: number) => Promise<T>,
): Promise<{ results: T[]; latencies: number[] }> {
  const results = new Array<T>(count)
  const latencies = new Array<number>(count)
  let cursor = 0

  const worker = async (): Promise<void> => {
    while (true) {
      const index = cursor++
      if (index >= count) return
      const start = performance.now()
      results[index] = await fn(index)
      latencies[index] = performance.now() - start
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(Math.max(1, concurrency), Math.max(1, count)) }, worker),
  )
  return { results, latencies }
}

async function measure(
  group: Group,
  name: string,
  ops: number,
  concurrency: number,
  fn: (i: number) => Promise<unknown>,
  results: ScenarioResult[],
): Promise<void> {
  const callsBefore = totalCalls
  const start = performance.now()
  try {
    const pool = await runPool(ops, concurrency, fn)
    const wallMs = performance.now() - start
    results.push({
      group,
      name,
      ops,
      wallMs,
      latencies: pool.latencies,
      apiCalls: totalCalls - callsBefore,
    })
  } catch (err) {
    const wallMs = performance.now() - start
    results.push({
      group,
      name,
      ops: 0,
      wallMs,
      latencies: [],
      apiCalls: totalCalls - callsBefore,
      failed: err instanceof Error ? err.message : String(err),
    })
    throw err
  }
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

function pad(value: string | number, width: number, align: 'left' | 'right' = 'left'): string {
  const text = String(value)
  return align === 'left' ? text.padEnd(width) : text.padStart(width)
}

function printResults(
  results: ScenarioResult[],
  bootstrap: { ms: number; calls: number },
  opts: Options,
  dbName: string,
  domain: string,
): void {
  const groups: Array<[Group, string]> = [
    ['write', 'WRITE'],
    ['read', 'READ'],
    ['update', 'UPDATE'],
    ['delete', 'DELETE'],
  ]

  const header = [
    pad('scenario', 42),
    pad('ops', 7, 'right'),
    pad('wall(s)', 9, 'right'),
    pad('ops/s', 9, 'right'),
    pad('p50(ms)', 9, 'right'),
    pad('p95(ms)', 9, 'right'),
    pad('p99(ms)', 9, 'right'),
    pad('api', 6, 'right'),
  ].join(' ')
  const rule = '-'.repeat(header.length)

  console.log('')
  console.log('bitedb performance benchmark')
  console.log(rule)
  console.log(`domain      ${domain}`)
  console.log(`database    ${dbName}`)
  console.log(`table       ${opts.table}`)
  console.log(`docs        ${opts.docs} burst + ${opts.sequential} sequential`)
  console.log(`read ops    ${opts.readOps} per scenario, concurrency ${opts.concurrency}`)
  console.log(`qps limit   ${opts.qps ?? (opts.mock ? 'unlimited' : 18)}`)
  console.log(rule)
  console.log(
    `bootstrap (catalog + database + table): ${bootstrap.ms.toFixed(1)} ms, ${bootstrap.calls} API calls`,
  )

  for (const [group, title] of groups) {
    const rows = results.filter((r) => r.group === group)
    if (rows.length === 0) continue
    console.log('')
    console.log(title)
    console.log(header)
    for (const row of rows) {
      if (row.failed) {
        console.log(`${pad(row.name, 42)} FAILED: ${row.failed}`)
        continue
      }
      const sorted = [...row.latencies].sort((a, b) => a - b)
      const opsPerSecond = row.wallMs > 0 ? (row.ops / row.wallMs) * 1000 : 0
      console.log(
        [
          pad(row.name, 42),
          pad(row.ops, 7, 'right'),
          pad((row.wallMs / 1000).toFixed(2), 9, 'right'),
          pad(opsPerSecond.toFixed(1), 9, 'right'),
          pad(percentile(sorted, 50).toFixed(1), 9, 'right'),
          pad(percentile(sorted, 95).toFixed(1), 9, 'right'),
          pad(percentile(sorted, 99).toFixed(1), 9, 'right'),
          pad(row.apiCalls, 6, 'right'),
        ].join(' '),
      )
    }
  }

  console.log('')
  console.log('API calls by endpoint')
  console.log(rule)
  const entries = [...callCounts.entries()].sort((a, b) => b[1] - a[1])
  for (const [name, count] of entries) {
    console.log(`${pad(name, 30)} ${pad(count, 8, 'right')}`)
  }
  console.log(`${pad('total', 30)} ${pad(totalCalls, 8, 'right')}`)
  console.log('')
}

function printFeishuHelp(err: unknown, appId: string, domain: string): boolean {
  if (!(err instanceof FeishuApiError)) return false
  console.error(`\nFeishu API ${err.method} ${err.path} failed: [${err.apiCode}] ${err.apiMsg}`)
  const violations =
    (err.response as any)?.error?.permission_violations ??
    (err.response as any)?.data?.error?.permission_violations ??
    []
  const scopes = violations
    .map((item: any) => item?.subject)
    .filter((item: unknown): item is string => typeof item === 'string' && item.length > 0)
  if (err.apiCode === 99991672 || scopes.length > 0) {
    const host = new URL(domain).host
    const query = scopes.length > 0 ? encodeURIComponent(scopes.join(',')) : 'bitable:app'
    console.error('\nThe app is missing required scopes:')
    for (const scope of scopes.length > 0 ? scopes : ['bitable:app']) console.error(`  - ${scope}`)
    console.error('\nEnable them in the developer console:')
    console.error(`  https://${host}/app/${appId}/auth?q=${query}&op_from=openapi&token_type=tenant`)
    console.error('Then create and publish a new app version (an admin may need to approve it).')
  }
  return true
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const opts = parseOptions(process.argv.slice(2))
  if (opts.help) {
    printUsage()
    return
  }

  const startedAt = new Date()
  const runId = `${startedAt.getTime().toString(36)}-${Math.random().toString(36).slice(2, 6)}`

  let fake: { url: string; stop(): Promise<void> } | undefined
  let domain = opts.domain ?? process.env.FEISHU_DOMAIN ?? 'https://open.feishu.cn'
  let appId = opts.appId ?? process.env.FEISHU_APP_ID
  let appSecret = opts.appSecret ?? process.env.FEISHU_APP_SECRET

  if (opts.mock) {
    const { FakeFeishu } = await import('../test/fake-feishu.js')
    fake = await new FakeFeishu().start()
    domain = fake.url
    appId = 'test-app'
    appSecret = 'test-secret'
  } else if (!appId || !appSecret) {
    console.error('FEISHU_APP_ID and FEISHU_APP_SECRET are required (or use --mock).')
    process.exitCode = 1
    return
  }

  const dbName = opts.fresh
    ? `${opts.database}_${startedAt.toISOString().replace(/[-:T.Z]/g, '').slice(0, 14)}`
    : opts.database

  let db: BiteDB | undefined
  const results: ScenarioResult[] = []
  const bootstrap = { ms: 0, calls: 0 }
  let tableStats: unknown

  try {
    db = createClient({
      appId,
      appSecret,
      domain,
      batchWindowMs: opts.batchWindowMs,
      maxBatchSize: opts.batchSize,
      bitableQps: opts.qps ?? (opts.mock ? 1000 : 18),
      catalogFlushMs: opts.mock ? 0 : 1000,
      fetch: countingFetch,
    })

    const database = db.useDatabase(dbName)
    const table = database.defineTable<BenchDoc>(opts.table, SCHEMA)

    // -- bootstrap -----------------------------------------------------------
    const bootstrapCalls = totalCalls
    const bootstrapStart = performance.now()
    await table.ensure()
    bootstrap.ms = performance.now() - bootstrapStart
    bootstrap.calls = totalCalls - bootstrapCalls

    // -- write: burst --------------------------------------------------------
    const burstStart = performance.now()
    const burstCalls = totalCalls
    const burst = await runPool(opts.docs, opts.docs, (i) => table.insertOne(makeDoc(runId, i)))
    results.push({
      group: 'write',
      name: `insertOne burst (${opts.docs} concurrent)`,
      ops: opts.docs,
      wallMs: performance.now() - burstStart,
      latencies: burst.latencies,
      apiCalls: totalCalls - burstCalls,
    })
    const ids = burst.results.map((doc) => doc._id)

    // -- write: sequential ---------------------------------------------------
    if (opts.sequential > 0) {
      const sequentialStart = performance.now()
      const sequentialCalls = totalCalls
      const sequential = await runPool(opts.sequential, 1, (i) =>
        table.insertOne(makeDoc(runId, opts.docs + i)),
      )
      results.push({
        group: 'write',
        name: `insertOne sequential (${opts.sequential})`,
        ops: opts.sequential,
        wallMs: performance.now() - sequentialStart,
        latencies: sequential.latencies,
        apiCalls: totalCalls - sequentialCalls,
      })
      ids.push(...sequential.results.map((doc) => doc._id))
    }

    await table.flush()
    await db.flush()
    tableStats = await table.stats()

    const pickId = (i: number): string => ids[Math.floor(Math.random() * ids.length)]!
    const pickSeq = (i: number): number => Math.floor(Math.random() * opts.docs)

    // -- reads ---------------------------------------------------------------
    await measure('read', 'get(_id)', opts.readOps, opts.concurrency, (i) => table.get(pickId(i)), results)
    await measure(
      'read',
      'findOne({ sku })',
      opts.readOps,
      opts.concurrency,
      (i) => table.findOne({ sku: `SKU-${String(pickSeq(i)).padStart(6, '0')}` }),
      results,
    )
    await measure(
      'read',
      'find limit 100',
      opts.readOps,
      opts.concurrency,
      () => table.find({}).limit(100).toArray(),
      results,
    )
    await measure(
      'read',
      'find sort seq desc limit 100',
      opts.readOps,
      opts.concurrency,
      () => table.find({}).sort('seq', 'desc').limit(100).toArray(),
      results,
    )
    await measure(
      'read',
      'find price>=50 sort limit 100',
      opts.readOps,
      opts.concurrency,
      () => table.find({ price: { $gte: 50 } }).sort('seq', 'desc').limit(100).toArray(),
      results,
    )
    await measure(
      'read',
      'find offset 200 limit 100',
      opts.readOps,
      opts.concurrency,
      () => table.find({}).offset(200).limit(100).toArray(),
      results,
    )
    if (opts.docs >= 1000) {
      await measure(
        'read',
        'find limit 1000 (paged)',
        Math.max(10, Math.floor(opts.readOps / 2)),
        opts.concurrency,
        () => table.find({}).limit(1000).toArray(),
        results,
      )
    }
    await measure(
      'read',
      'countDocuments({ run })',
      opts.readOps,
      opts.concurrency,
      () => table.countDocuments({ run: runId }),
      results,
    )

    // -- updates -------------------------------------------------------------
    await measure(
      'update',
      'updateOne $set by _id',
      opts.readOps,
      Math.min(6, opts.concurrency),
      (i) => table.updateOne(pickId(i), { $set: { note: `updated-${i}` } }),
      results,
    )

    {
      const callsBefore = totalCalls
      const start = performance.now()
      const updated = await table.updateMany(
        { run: runId, active: true },
        { $set: { note: 'bulk-updated' } },
      )
      results.push({
        group: 'update',
        name: 'updateMany active=true',
        ops: updated.matchedCount,
        wallMs: performance.now() - start,
        latencies: [performance.now() - start],
        apiCalls: totalCalls - callsBefore,
      })
    }

    // -- delete --------------------------------------------------------------
    if (opts.keepData) {
      console.log(`\n--keep-data: ${opts.docs + opts.sequential} rows left in "${dbName}.${opts.table}" (run=${runId})`)
    } else {
      const callsBefore = totalCalls
      const start = performance.now()
      const deleted = await table.deleteMany({ run: runId })
      results.push({
        group: 'delete',
        name: 'deleteMany({ run })',
        ops: deleted.deletedCount,
        wallMs: performance.now() - start,
        latencies: [performance.now() - start],
        apiCalls: totalCalls - callsBefore,
      })
      const remaining = await table.countDocuments({ run: runId })
      console.log(`\ncleanup: deleted ${deleted.deletedCount} rows, remaining ${remaining}`)
    }
  } catch (err) {
    if (!printFeishuHelp(err, appId ?? 'cli_xxx', domain)) console.error(err)
    process.exitCode = 2
  } finally {
    try {
      await db?.close()
    } catch {
      /* ignore close failures after an error */
    }
    await fake?.stop()
  }

  printResults(results, bootstrap, opts, dbName, domain)

  if (opts.json) {
    const payload = {
      startedAt: startedAt.toISOString(),
      database: dbName,
      table: opts.table,
      domain,
      // Never persist credentials passed via --app-id/--app-secret.
      options: { ...opts, appSecret: opts.appSecret ? '[redacted]' : undefined },
      bootstrap,
      results: results.map((row) => {
        const sorted = [...row.latencies].sort((a, b) => a - b)
        return {
          group: row.group,
          name: row.name,
          ops: row.ops,
          wallMs: row.wallMs,
          opsPerSecond: row.wallMs > 0 ? (row.ops / row.wallMs) * 1000 : 0,
          p50: percentile(sorted, 50),
          p95: percentile(sorted, 95),
          p99: percentile(sorted, 99),
          apiCalls: row.apiCalls,
          failed: row.failed,
        }
      }),
      apiCalls: Object.fromEntries(callCounts),
      tableStats,
    }
    await writeFile(opts.json, JSON.stringify(payload, null, 2))
    console.log(`JSON results written to ${opts.json}`)
  }
}

main().catch((err) => {
  console.error(err)
  process.exitCode = 1
})
