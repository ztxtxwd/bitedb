# bitedb benchmark

Measures the end-to-end performance of bitedb against a real Feishu/Lark tenant,
or against the in-memory fake API used by the test suite.

## Run against Feishu

```bash
FEISHU_APP_ID=cli_xxx FEISHU_APP_SECRET=xxx npm run bench
```

The app needs `bitable:app` (or the granular `base:table:*`, `base:field:*`,
`base:record:*` scopes) plus `drive:drive`. If a scope is missing, the script
prints the permission list and a developer-console link.

## Run against the mock

```bash
npm run bench:mock
```

The mock has no network latency. It is useful for validating the benchmark and
for measuring bitedb's local overhead (codec, batching, merge), not platform
performance.

## What it does

| Phase | Scenario |
| --- | --- |
| bootstrap | catalog discovery + database/table/field reconciliation |
| write | 2000 concurrent `insertOne` (coalesced into `batch_create`) |
| write | 20 sequential `insertOne` (one request per document) |
| read | `get(_id)`, `findOne({sku})`, `find limit 100`, sorted/filtered finds, `offset`, 1000-row paged find, `countDocuments` |
| update | `updateOne` by `_id`, `updateMany` over ~1000 rows |
| delete | `deleteMany` over all benchmark rows |

Every row is tagged with a per-run `run` value, so cleanup only touches data
written by that run. The final `deleteMany` is measured and then verified with
`countDocuments`.

## Options

```bash
npm run bench -- --help
```

| Option | Default | Meaning |
| --- | --- | --- |
| `--docs N` | 2000 | documents inserted concurrently |
| `--sequential N` | 20 | documents inserted one at a time |
| `--read-ops N` | 60 | operations per read/update scenario |
| `--concurrency N` | 8 | concurrent in-flight operations |
| `--qps N` | 18 | bitedb's bitable token-bucket rate |
| `--batch-size N` | 500 | `maxBatchSize` |
| `--batch-window N` | 50 | `batchWindowMs` |
| `--database NAME` | `bitedb_perf` | logical database name |
| `--table NAME` | `bench` | logical table name |
| `--fresh` | off | unique database name (cold provisioning) |
| `--keep-data` | off | keep rows and print the `run` id |
| `--json PATH` | off | write raw results |
| `--mock` | off | use the in-memory fake Feishu API |

## Reading the output

- **ops/s** — end-to-end operations per second, including batching windows,
  token-bucket waits, HTTP round trips and retries.
- **p50/p95/p99** — per-operation latency measured at the caller. For the
  concurrent insert row, all calls start together, so latency reflects the
  batch flush rather than one document's round trip.
- **api** — actual HTTP requests issued during the scenario. Compare
  `insertOne burst` (one `batch_create` per `maxBatchSize`) with
  `insertOne sequential` (one request per document) to see the batching effect.
- **updateOne** issues two requests per operation: a `GET` to locate/read the
  record, then a `PUT`. That is expected.

## Reference run

2026-11-05, `open.feishu.cn`, 2000 documents, concurrency 16, default QPS 18:

| scenario | ops | wall (s) | ops/s | p50 (ms) | api |
| --- | ---: | ---: | ---: | ---: | ---: |
| insertOne burst (2000 concurrent) | 2000 | 6.38 | 313.4 | 4477 | 5 |
| insertOne sequential (20) | 20 | 60.82 | 0.3 | 3001 | 40 |
| get(_id) | 60 | 3.55 | 16.9 | 736 | 70 |
| find limit 100 | 60 | 11.53 | 5.2 | 3004 | 60 |
| find offset 200 limit 100 | 60 | 13.67 | 4.4 | 3486 | 60 |
| find limit 1000 (paged) | 30 | 15.45 | 1.9 | 7720 | 60 |
| countDocuments | 60 | 10.64 | 5.6 | 2638 | 60 |
| updateOne $set by _id | 60 | 31.83 | 1.9 | 2995 | 120 |
| updateMany (1010 rows) | 1010 | 10.29 | 98.2 | — | 6 |
| deleteMany (2020 rows) | 2020 | 20.53 | 98.4 | — | 10 |

Absolute numbers depend heavily on the tenant's region and the distance to the
Feishu API; compare runs from the same machine.
