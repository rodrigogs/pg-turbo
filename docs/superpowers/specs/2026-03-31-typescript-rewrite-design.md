# pg_resilient TypeScript Rewrite — Design Specification

**Date**: 2026-03-31
**Status**: Draft
**Scope**: Full rewrite of pg_resilient from Bash to TypeScript with new architecture

## 1. Motivation

The current Bash implementation (~2,700 lines) is buggy and hard to maintain. The rewrite targets:

- **Developer experience**: Type safety, structured error handling, testable modules
- **Performance**: Direct COPY protocol replaces per-table pg_dump spawning; sub-table parallelism enables splitting large tables across workers
- **Resilience**: Chunk-level retry (250MB) instead of table-level retry (potentially 500GB)

This is NOT a port. It is a new architecture that uses direct PostgreSQL COPY streaming instead of spawning pg_dump/pg_restore processes for data transfer.

## 2. Architecture Overview

```
CLI (commander)
  │
  ├── dump command
  │     ├── Connection manager (pg Client pool, snapshot coordinator)
  │     ├── Schema dumper (pg_dump --schema-only, plain SQL)
  │     ├── Table discoverer (pg_catalog query)
  │     ├── Chunk planner (PK range / ctid splitting)
  │     ├── Parallel COPY pipeline (N workers, each: COPY TO → lz4 → file)
  │     └── Manifest writer (manifest.json)
  │
  └── restore command
        ├── Manifest reader
        ├── Pre-data restore (pg_restore --section=pre-data)
        ├── Parallel COPY FROM pipeline (N workers, each: file → lz4 decompress → COPY FROM)
        ├── Post-data restore (pg_restore --section=post-data)
        └── Sequence reset
```

### Data Flow — Dump

```
Coordinator Connection
  BEGIN REPEATABLE READ
  pg_export_snapshot() → snapshot_id
  (holds transaction open for consistency)
       │
       ├── Worker 1 (own Client)
       │   SET TRANSACTION SNAPSHOT '...'
       │   COPY public.users TO STDOUT
       │     → lz4.createEncoderStream()
       │     → fs.createWriteStream('data/public.users/chunk_0000.copy.lz4')
       │
       ├── Worker 2 (own Client)
       │   SET TRANSACTION SNAPSHOT '...'
       │   COPY (SELECT * FROM huge WHERE id BETWEEN 0 AND 1M) TO STDOUT
       │     → lz4 → file
       │
       └── Worker N ...
```

All streams connected via Node.js `pipeline()` for end-to-end backpressure.

### Data Flow — Restore

```
1. Read manifest.json
2. pg_restore --section=pre-data schema/ddl.dump (tables, types, sequences — NO indexes)
3. Parallel COPY FROM:
     file → lz4.createDecoderStream() → pg COPY FROM STDIN
     Multiple chunks into same table concurrently (ROW EXCLUSIVE is self-compatible)
4. pg_restore --section=post-data schema/ddl.dump (indexes, constraints, triggers)
5. Reset sequences: setval(seq, max(pk))
```

## 3. Project Structure

```
pg_resilient/
  ts/                               # TypeScript project subdirectory
    package.json
    tsconfig.json
    vitest.config.ts
    src/
      cli/
        index.ts                    # Entry point — command router
        dump.ts                     # dump command orchestration
        restore.ts                  # restore command orchestration
        args.ts                     # argument parsing (commander)
        ui.ts                       # progress dashboard, banners, colors
      core/
        connection.ts               # pg clients, snapshot management, health checks
        copy-stream.ts              # COPY TO/FROM pipeline with compression
        chunker.ts                  # PK range / ctid table splitting
        queue.ts                    # parallel worker orchestration
        manifest.ts                 # read/write manifest.json
        schema.ts                   # pg_dump --schema-only wrapper (DDL only)
        retry.ts                    # retry with exponential backoff
        format.ts                   # human_size, elapsed_time, progress_bar
      types/
        index.ts                    # shared types
    tests/
      unit/
        connection.test.ts
        chunker.test.ts
        copy-stream.test.ts
        queue.test.ts
        manifest.test.ts
        format.test.ts
        retry.test.ts
        args.test.ts
      integration/
        dump.test.ts
        restore.test.ts
        docker-compose.yml
        fixtures.sql
    bin/
      pg-resilient.ts              # shebang entry
```

## 4. Dependency Stack

### Runtime Dependencies

| Package | Version | Deps | Size | Purpose |
|---------|---------|------|------|---------|
| `commander` | 14.x | 0 | 204KB | CLI framework |
| `@commander-js/extra-typings` | 14.x | 0 | peer | Type inference for commander |
| `pg` | latest | ~5 | — | PostgreSQL client (pure JS, NOT pg-native) |
| `pg-copy-streams` | 7.x | 0 | — | COPY TO/FROM streaming |
| `lz4` | 0.6.x | native | — | Streaming lz4 compression/decompression (`createEncoderStream`/`createDecoderStream`) |
| `picocolors` | 1.1.x | 0 | 6KB | Terminal colors |
| `nanospinner` | 1.2.x | 1 | 10KB | Spinners |
| `log-update` | 7.2.x | 5 | 16KB | Live dashboard rendering |
| `nano-spawn` | 2.0.x | 0 | — | Typed child_process wrapper (for pg_dump) |

Total transitive runtime deps: ~12. Extremely lean.

### Dev Dependencies

| Package | Purpose |
|---------|---------|
| `typescript` 5.x | Type checking (`noEmit: true`) |
| `tsdown` 0.21.x | Bundler (successor to deprecated tsup) |
| `vitest` | Unit + integration tests |
| `tsx` | Run TS directly during development |
| `@types/pg` | pg type definitions |
| `@types/pg-copy-streams` | pg-copy-streams type definitions |

### Package Manager

`pnpm` — strict dependency resolution prevents phantom imports.

## 5. TypeScript Configuration

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "noImplicitOverride": true,
    "verbatimModuleSyntax": true,
    "isolatedModules": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "outDir": "dist",
    "rootDir": "src",
    "noEmit": true
  },
  "include": ["src"]
}
```

Key decisions:
- `module: "NodeNext"` — enforces correct ESM imports with `.js` extensions
- `target: "ES2022"` — Node 20 supports everything in ES2022
- `verbatimModuleSyntax: true` — forces `import type` syntax
- `noEmit: true` — tsdown/esbuild handles compilation; tsc is type-checking only
- `noUncheckedIndexedAccess: true` — catches unsafe array access

`package.json`: `"type": "module"`, `"engines": { "node": ">=20" }`

## 6. Core Modules — Detailed Design

### 6.1 Connection Manager (`core/connection.ts`)

**Why not pg.Pool for workers**: Pool connections are lazily created and reused. You cannot guarantee snapshot setup on every connection. Use individual `Client` instances with manual lifecycle.

**Coordinator pattern**:
```typescript
// Coordinator: dedicated long-lived Client
const coordinator = new Client({ connectionString })
await coordinator.connect()
await coordinator.query('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ')
const { rows } = await coordinator.query('SELECT pg_export_snapshot() AS snapshot_id')
const snapshotId = rows[0].snapshot_id
// Coordinator stays open until all workers finish

// Workers: individual Clients, each imports the snapshot
async function createWorker(connectionString: string, snapshotId: string): Promise<Client> {
  const client = new Client({ connectionString })
  await client.connect()
  await client.query('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ')
  await client.query(`SET TRANSACTION SNAPSHOT '${snapshotId}'`)
  return client
}
```

**Connection string tuning** (appended automatically):
```
keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=5&tcp_user_timeout=30000&connect_timeout=10
```
Detects dead connections in ~60s instead of default 2 hours.

**Snapshot behavior on failure**: Workers that already imported the snapshot are self-contained. If the coordinator dies, active workers continue their current COPY. Only new workers can't join. On coordinator reconnect, a new snapshot is exported and remaining work uses the new snapshot (accept minor inconsistency for resilience, or fail explicitly with `--strict-consistency` flag).

**For read replicas**: Skip snapshot entirely (no writes = no consistency concern). Each worker opens its own independent REPEATABLE READ transaction. Simpler and more resilient. Detect read replica via `pg_is_in_recovery()`.

### 6.2 COPY Pipeline (`core/copy-stream.ts`)

**Memory per stream**: ~128KB (64KB highWaterMark + 64KB protocol buffer). 8 parallel streams = ~1MB total.

**Dump pipeline**:
```typescript
import { pipeline } from 'node:stream/promises'
import { to as copyTo } from 'pg-copy-streams'
import lz4 from 'lz4'
import { createWriteStream } from 'node:fs'

async function dumpChunk(client: Client, copyQuery: string, outputPath: string): Promise<number> {
  const copyStream = client.query(copyTo(copyQuery))
  const compressor = lz4.createEncoderStream({ blockMaxSize: 4 * 1024 * 1024 })
  const fileStream = createWriteStream(outputPath)

  await pipeline(copyStream, compressor, fileStream)
  return copyStream.rowCount
}
```

**Restore pipeline**:
```typescript
import { from as copyFrom } from 'pg-copy-streams'

async function restoreChunk(client: Client, table: string, columns: string[], inputPath: string): Promise<void> {
  const colList = columns.join(', ')
  const copyStream = client.query(copyFrom(`COPY ${table} (${colList}) FROM STDIN`))
  const decompressor = lz4.createDecoderStream()
  const fileStream = createReadStream(inputPath)

  await pipeline(fileStream, decompressor, copyStream)
}
```

**Error handling**: After a connection-level error during COPY, the client is in undefined state (`_queryable = false`). Must be destroyed, never reused:
```typescript
try {
  await pipeline(copyStream, compressor, fileStream)
  client.release?.() // or tracked for reuse
} catch (err) {
  client.end()  // destroy — do not reuse
  throw err
}
```

**pg-copy-streams requirement**: Must use pure JS pg bindings (not pg-native). The library intercepts the connection's raw TCP stream data handler.

### 6.3 Table Chunking (`core/chunker.ts`)

Two strategies, selected automatically per table:

**Strategy 1 — PK range splitting** (preferred):
- Detect single-column integer PK (smallint/int/bigint) via `pg_index` + `pg_attribute`
- Query `min(pk), max(pk)` — fast on btree index
- Generate chunks: `COPY (SELECT * FROM t WHERE id BETWEEN $start AND $end) TO STDOUT`
- First chunk includes `WHERE pk IS NULL OR pk BETWEEN $start AND $end` (handles NULL PKs)

**Strategy 2 — ctid range splitting** (fallback):
- Use `pg_class.relpages * block_size` for total byte estimate
- Generate chunks: `COPY (SELECT * FROM t WHERE ctid >= '($start,0)'::tid AND ctid < '($end,0)'::tid) TO STDOUT`
- Last chunk is open-ended: `WHERE ctid >= '($start,0)'::tid`
- **Requires PostgreSQL 14+** for Tid Range Scan operator. On PG <14, ctid filters trigger full seq scan — fall back to single chunk.

**No chunking**: Tables smaller than `--split-threshold` (default: 1GB) are dumped as a single chunk with plain `COPY table TO STDOUT`.

**Chunk count formula**: `ceil(table_bytes / split_threshold)`, capped by `--max-chunks-per-table` (default: 32).

**Edge cases**:
- Empty tables: skip data dump (COPY produces no output), still listed in manifest
- Generated columns (`pg_attribute.attgenerated <> ''`): excluded from COPY column list
- Partitioned tables (`relkind = 'p'`): skip parent, dump leaf partitions (`relkind = 'r'`) directly
- Materialized views (`relkind = 'm'`): included, treated as regular tables
- Foreign tables (`relkind = 'f'`): skipped
- Tables never ANALYZEd (`relpages = 0`): use `pg_table_size()` as fallback

### 6.4 Table Discovery (`core/schema.ts`)

Single efficient query against `pg_catalog`:

```sql
SELECT
    c.oid,
    n.nspname AS schema_name,
    c.relname AS table_name,
    c.relkind,
    c.relpages,
    c.reltuples::bigint AS estimated_rows,
    pg_table_size(c.oid) AS actual_bytes,
    pkeys.attname AS pk_column,
    pkeys.typname AS pk_type
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
LEFT JOIN LATERAL (
    SELECT a.attname, t.typname
    FROM pg_index x
    JOIN pg_class i ON i.oid = x.indexrelid
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = x.indkey[0]
    JOIN pg_type t ON t.oid = a.atttypid
    WHERE x.indrelid = c.oid
      AND (x.indisprimary OR x.indisunique)
      AND array_length(x.indkey::integer[], 1) = 1
      AND a.atttypid IN ('smallint'::regtype, 'int'::regtype, 'bigint'::regtype)
    ORDER BY NOT x.indisprimary, NOT x.indisunique
    LIMIT 1
) AS pkeys ON true
WHERE c.relkind IN ('r', 'm')
  AND c.relpersistence IN ('p', 'u')
  AND n.nspname NOT LIKE 'pg_%'
  AND n.nspname <> 'information_schema'
ORDER BY pg_table_size(c.oid) DESC NULLS LAST;
```

**Schema filter** (`-n`): adds `AND n.nspname = $1`.

**Generated column detection** (separate query per table, only when needed):
```sql
SELECT attname FROM pg_attribute
WHERE attrelid = $oid AND attgenerated <> '' AND NOT attisdropped AND attnum > 0;
```

**DDL extraction**: Delegates to `pg_dump --schema-only` in custom format (single file, supports `--section` on restore):
```
pg_dump --schema-only --format=custom -f schema/ddl.dump
```

Custom format is used (not plain SQL) because `pg_restore --section=pre-data` and `--section=post-data` require it. A single file supports both section-based restore phases.

Uses `--snapshot $snapshotId` for consistency with data dump (when available).

### 6.5 Parallel Queue (`core/queue.ts`)

Async worker pool using `Promise.all` + shared async queue:

```typescript
interface ChunkJob {
  table: TableInfo
  chunkIndex: number
  totalChunks: number
  copyQuery: string
  outputPath: string
}

async function runWorkerPool(
  jobs: ChunkJob[],
  workerCount: number,
  createWorkerClient: () => Promise<Client>,
  onProgress: (event: ProgressEvent) => void
): Promise<WorkerResult[]> {
  const queue = [...jobs]  // sorted by estimated_bytes DESC (largest first)
  const results: WorkerResult[] = []

  async function worker(workerId: number): Promise<void> {
    const client = await createWorkerClient()
    try {
      while (queue.length > 0) {
        const job = queue.shift()!
        // Check resume marker
        if (existsSync(`${job.outputPath}.done`)) {
          onProgress({ type: 'skipped', workerId, job })
          continue
        }
        try {
          await dumpChunk(client, job.copyQuery, job.outputPath)
          writeFileSync(`${job.outputPath}.done`, '')
          onProgress({ type: 'completed', workerId, job })
          results.push({ job, status: 'ok' })
        } catch (err) {
          // Retry logic: return job to queue
          if (job.attempt < maxRetries) {
            job.attempt++
            queue.push(job)
          } else {
            results.push({ job, status: 'failed', error: err })
          }
          // Destroy broken client, create fresh one
          await client.end().catch(() => {})
          client = await createWorkerClient()
        }
      }
    } finally {
      await client.query('COMMIT').catch(() => {})
      await client.end().catch(() => {})
    }
  }

  await Promise.all(Array.from({ length: workerCount }, (_, i) => worker(i)))
  return results
}
```

Jobs are sorted **largest first** for optimal scheduling — avoids the pathological case where small jobs finish early and one worker is stuck on the biggest table.

### 6.6 Dump Format (`core/manifest.ts`)

```
dump_dir/
  manifest.json
  schema/
    ddl.dump                  # pg_dump custom format (supports --section on restore)
  data/
    public.users/
      chunk_0000.copy.lz4
      chunk_0000.copy.lz4.done
      chunk_0001.copy.lz4
      chunk_0001.copy.lz4.done
    public.orders/
      chunk_0000.copy.lz4
      chunk_0000.copy.lz4.done
```

**manifest.json schema**:
```json
{
  "version": 1,
  "tool": "pg-resilient",
  "created_at": "2026-03-31T05:00:00.000Z",
  "pg_version": "16.2",
  "database": "mydb",
  "snapshot_id": "00000003-0000001B-1",
  "compression": "lz4",
  "options": {
    "schema_filter": null,
    "split_threshold_bytes": 1073741824,
    "jobs": 4
  },
  "tables": [
    {
      "schema": "public",
      "name": "users",
      "oid": 16385,
      "relkind": "r",
      "estimated_bytes": 2147483648,
      "estimated_rows": 10000000,
      "pk_column": "id",
      "pk_type": "int8",
      "chunk_strategy": "pk_range",
      "columns": ["id", "name", "email", "created_at"],
      "generated_columns": [],
      "chunks": [
        { "index": 0, "file": "data/public.users/chunk_0000.copy.lz4", "range_start": 1, "range_end": 2500000 },
        { "index": 1, "file": "data/public.users/chunk_0001.copy.lz4", "range_start": 2500001, "range_end": 5000000 },
        { "index": 2, "file": "data/public.users/chunk_0002.copy.lz4", "range_start": 5000001, "range_end": 7500000 },
        { "index": 3, "file": "data/public.users/chunk_0003.copy.lz4", "range_start": 7500001, "range_end": 10000000 }
      ]
    },
    {
      "schema": "public",
      "name": "config",
      "oid": 16390,
      "relkind": "r",
      "estimated_bytes": 8192,
      "estimated_rows": 5,
      "pk_column": null,
      "pk_type": null,
      "chunk_strategy": "none",
      "columns": ["key", "value"],
      "generated_columns": [],
      "chunks": [
        { "index": 0, "file": "data/public.config/chunk_0000.copy.lz4" }
      ]
    }
  ],
  "sequences": [
    { "schema": "public", "name": "users_id_seq", "last_value": 10000000, "is_called": true }
  ]
}
```

### 6.7 Retry (`core/retry.ts`)

Exponential backoff with jitter:
```
delay = min(base_delay * 2^attempt + random_jitter, max_delay)
```

Default: `base_delay=5s`, `max_delay=60s`, `max_retries=5`.

On COPY failure:
1. Destroy the broken client
2. Create a fresh client (reimport snapshot if coordinator still alive)
3. Delete partial chunk file
4. Re-run COPY for that chunk

### 6.8 Progress Dashboard (`cli/ui.ts`)

Live-updating via `log-update`:
```
[████████████░░░░░░░░] 62% (1.2 GB / 1.9 GB) — 145 MB/s — ETA: 4s
  Worker 1: public.users chunk 3/4 (52 MB/s)
  Worker 2: public.orders chunk 1/1 (38 MB/s)
  Worker 3: public.events chunk 7/12 (55 MB/s)
  Worker 4: idle
```

Progress tracked via stream `data` events (counting bytes through the pipeline). No background monitor processes needed — streams emit events naturally.

## 7. CLI Interface

### dump

```
pg-resilient dump -d <connection_string> --output <dir> [options]

Required:
  -d, --dbname              PostgreSQL connection string
  --output                  Output directory

Optional:
  -n, --schema              Dump only this schema
  -j, --jobs                Parallel workers (default: 4)
  --split-threshold         Chunk tables larger than this (default: "1GB")
  --max-chunks-per-table    Max chunks per table (default: 32)
  --retries                 Max retries per chunk (default: 5)
  --retry-delay             Base retry delay in seconds (default: 5)
  --no-snapshot             Skip snapshot (for read replicas)
  --dry-run                 Preview without dumping
  --help                    Show help
  [-- pg_dump_args...]      Passthrough to pg_dump (for DDL only)
```

### restore

```
pg-resilient restore -d <connection_string> --input <dir> [options]

Required:
  -d, --dbname              PostgreSQL connection string
  --input                   Input directory (from dump)

Optional:
  -n, --schema              Restore only tables in this schema
  -t, --table               Restore a single table
  -j, --jobs                Parallel workers (default: 4)
  -c, --clean               DROP + CREATE schema before restore
  -a, --data-only           Skip DDL, restore only table data
  --retries                 Max retries per chunk (default: 5)
  --retry-delay             Base retry delay in seconds (default: 5)
  --dry-run                 Preview without restoring
  --help                    Show help
  [-- pg_restore_args...]   Passthrough to pg_restore (for DDL only)
```

## 8. Testing Strategy

### Unit Tests (vitest)

Each core module tested in isolation with mocked pg connections:

| Module | Key test scenarios |
|--------|--------------------|
| `connection.ts` | Snapshot export/import, connection string tuning, health check, error recovery |
| `chunker.ts` | PK range calculation, ctid range calculation, edge cases (empty, no PK, generated cols) |
| `copy-stream.ts` | Pipeline assembly, error mid-stream, backpressure |
| `queue.ts` | Job scheduling, retry on failure, resume markers, worker lifecycle |
| `manifest.ts` | Serialize/deserialize, version validation |
| `format.ts` | human_size, elapsed_time (ported from Bash tests) |
| `retry.ts` | Exponential backoff, max retries, jitter |
| `args.ts` | Argument parsing, blocked flags, passthrough |

### Integration Tests (vitest + Docker)

Docker Compose with PostgreSQL 16 Alpine (same as current):
- Dump a test database, verify manifest and chunk files
- Restore from dump, verify data integrity
- Parallel dump/restore with multiple workers
- Resume after simulated interruption
- Chunked tables (large table with integer PK)

### Vitest Configuration

```typescript
// vitest.config.ts
import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['tests/unit/**/*.test.ts'],
    coverage: {
      provider: 'v8',
      include: ['src/**/*.ts'],
      exclude: ['src/types/**'],
    },
  },
})
```

Separate config for integration:
```typescript
// vitest.integration.config.ts
export default defineConfig({
  test: {
    include: ['tests/integration/**/*.test.ts'],
    testTimeout: 120_000,
    hookTimeout: 60_000,
  },
})
```

## 9. External Dependencies

The tool still requires these system binaries:
- `pg_dump` — for DDL extraction only (schema-only, plain SQL format)
- `pg_restore` — for DDL restoration only (pre-data and post-data sections)
- `psql` — NOT required (all queries use `pg` client directly)

Data transfer uses direct COPY protocol via `pg-copy-streams`. No external binaries needed for data.

## 10. Constraints and Limitations

- **PostgreSQL 14+ recommended** for ctid range splitting (Tid Range Scan operator). PG 12-13 supported but large tables without integer PKs cannot be chunked.
- **Node.js >= 20** required for stable streams, `pipeline()`, `fs/promises`.
- **Pure JS pg bindings only** — pg-native is incompatible with pg-copy-streams.
- **No backward compatibility** with Bash version dump format — clean break.
- **DDL depends on pg_dump/pg_restore** — not reimplemented in TypeScript.
- **Binary COPY format not used** — text format is portable across PG versions and compresses well with lz4. Binary gives only 5-15% server-side speedup at the cost of version portability.
- **Large Objects** (pg_largeobject) are not handled — only regular tables and materialized views.
