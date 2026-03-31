# Phase 3: Schema Discovery, Chunker & COPY Stream Pipeline

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the data pipeline: discover tables from pg_catalog, plan chunk splits, and stream COPY data through lz4 compression.

**Architecture:** Schema module queries pg_catalog and spawns pg_dump for DDL. Chunker generates COPY queries with PK/ctid ranges. Copy-stream module assembles the `pipeline(COPY TO → lz4 → file)` and `pipeline(file → lz4 → COPY FROM)`.

**Tech Stack:** pg, pg-copy-streams, lz4, nano-spawn

**Depends on:** Phase 2 (connection, types)

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `ts/src/core/schema.ts` | Table discovery (pg_catalog), DDL extraction (pg_dump), PG version, sequences |
| Create | `ts/tests/unit/schema.test.ts` | Tests for SQL builders and result parsing |
| Create | `ts/src/core/chunker.ts` | PK range / ctid range splitting logic |
| Create | `ts/tests/unit/chunker.test.ts` | Tests for chunk planning |
| Create | `ts/src/core/copy-stream.ts` | COPY TO/FROM pipelines with lz4 |
| Create | `ts/tests/unit/copy-stream.test.ts` | Tests for pipeline assembly |

---

### Task 8: Schema discovery

**Files:**
- Create: `ts/src/core/schema.ts`
- Create: `ts/tests/unit/schema.test.ts`

- [ ] **Step 1: Write failing tests for SQL builders and result parsers**

```typescript
// ts/tests/unit/schema.test.ts
import { describe, it, expect, vi } from 'vitest'
import {
  buildTableDiscoveryQuery,
  buildGeneratedColumnsQuery,
  parseTableRows,
  buildDdlDumpArgs,
  buildSequenceQuery,
} from '../../src/core/schema.js'

describe('buildTableDiscoveryQuery', () => {
  it('builds query without schema filter', () => {
    const sql = buildTableDiscoveryQuery(undefined)
    expect(sql).toContain('pg_catalog.pg_class')
    expect(sql).toContain("n.nspname NOT LIKE 'pg_%'")
    expect(sql).not.toContain('n.nspname =')
  })

  it('builds query with schema filter', () => {
    const sql = buildTableDiscoveryQuery('public')
    expect(sql).toContain("n.nspname = 'public'")
  })
})

describe('buildGeneratedColumnsQuery', () => {
  it('returns query with oid parameter', () => {
    const sql = buildGeneratedColumnsQuery()
    expect(sql).toContain('pg_attribute')
    expect(sql).toContain('attgenerated')
    expect(sql).toContain('$1')
  })
})

describe('parseTableRows', () => {
  it('parses rows into TableInfo array', () => {
    const rows = [
      {
        oid: 16385,
        schema_name: 'public',
        table_name: 'users',
        relkind: 'r',
        relpages: 1000,
        estimated_rows: 50000,
        actual_bytes: '8388608',
        pk_column: 'id',
        pk_type: 'int8',
      },
    ]
    const tables = parseTableRows(rows)
    expect(tables).toHaveLength(1)
    expect(tables[0]).toEqual({
      oid: 16385,
      schemaName: 'public',
      tableName: 'users',
      relkind: 'r',
      relpages: 1000,
      estimatedRows: 50000,
      actualBytes: 8388608,
      pkColumn: 'id',
      pkType: 'int8',
      columns: [],
      generatedColumns: [],
    })
  })

  it('handles null PK', () => {
    const rows = [
      {
        oid: 16390,
        schema_name: 'public',
        table_name: 'config',
        relkind: 'r',
        relpages: 1,
        estimated_rows: 5,
        actual_bytes: '8192',
        pk_column: null,
        pk_type: null,
      },
    ]
    const tables = parseTableRows(rows)
    expect(tables[0]!.pkColumn).toBeNull()
    expect(tables[0]!.pkType).toBeNull()
  })
})

describe('buildDdlDumpArgs', () => {
  it('builds args for full database', () => {
    const args = buildDdlDumpArgs('pg://h/db', '/out/schema/ddl.dump', undefined, null, [])
    expect(args).toContain('--schema-only')
    expect(args).toContain('--format=custom')
    expect(args).toContain('--no-owner')
    expect(args).toContain('--no-privileges')
  })

  it('adds schema filter', () => {
    const args = buildDdlDumpArgs('pg://h/db', '/out/schema/ddl.dump', 'public', null, [])
    expect(args).toContain('--schema=public')
  })

  it('adds snapshot', () => {
    const args = buildDdlDumpArgs('pg://h/db', '/out/schema/ddl.dump', undefined, 'snap-123', [])
    expect(args).toContain('--snapshot=snap-123')
  })

  it('appends extra args', () => {
    const args = buildDdlDumpArgs('pg://h/db', '/out/schema/ddl.dump', undefined, null, ['--no-comments'])
    expect(args).toContain('--no-comments')
  })
})

describe('buildSequenceQuery', () => {
  it('builds query to fetch sequence values', () => {
    const sql = buildSequenceQuery(undefined)
    expect(sql).toContain('pg_sequences')
    expect(sql).toContain('last_value')
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm test`

Expected: FAIL — module not found.

- [ ] **Step 3: Implement schema module**

```typescript
// ts/src/core/schema.ts
import type { TableInfo, SequenceInfo } from '../types/index.js'

export function buildTableDiscoveryQuery(schemaFilter: string | undefined): string {
  const schemaClause = schemaFilter
    ? `AND n.nspname = '${schemaFilter}'`
    : `AND n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema'`

  return `
    SELECT
      c.oid,
      n.nspname AS schema_name,
      c.relname AS table_name,
      c.relkind,
      c.relpages,
      c.reltuples::bigint AS estimated_rows,
      pg_table_size(c.oid)::text AS actual_bytes,
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
      ${schemaClause}
    ORDER BY pg_table_size(c.oid) DESC NULLS LAST
  `
}

export function buildGeneratedColumnsQuery(): string {
  return `
    SELECT attname
    FROM pg_attribute
    WHERE attrelid = $1
      AND attgenerated <> ''
      AND NOT attisdropped
      AND attnum > 0
  `
}

export function buildColumnsQuery(): string {
  return `
    SELECT attname
    FROM pg_attribute
    WHERE attrelid = $1
      AND NOT attisdropped
      AND attnum > 0
      AND attgenerated = ''
    ORDER BY attnum
  `
}

interface TableRow {
  oid: number
  schema_name: string
  table_name: string
  relkind: string
  relpages: number
  estimated_rows: number
  actual_bytes: string
  pk_column: string | null
  pk_type: string | null
}

export function parseTableRows(rows: TableRow[]): TableInfo[] {
  return rows.map(r => ({
    oid: r.oid,
    schemaName: r.schema_name,
    tableName: r.table_name,
    relkind: r.relkind as 'r' | 'm',
    relpages: r.relpages,
    estimatedRows: r.estimated_rows,
    actualBytes: parseInt(r.actual_bytes, 10),
    pkColumn: r.pk_column,
    pkType: r.pk_type as TableInfo['pkType'],
    columns: [],
    generatedColumns: [],
  }))
}

export function buildDdlDumpArgs(
  connectionString: string,
  outputPath: string,
  schemaFilter: string | undefined,
  snapshotId: string | null,
  extraArgs: string[],
): string[] {
  const args = [
    connectionString,
    '--schema-only',
    '--format=custom',
    '--no-owner',
    '--no-privileges',
    '--verbose',
    `-f`, outputPath,
  ]
  if (schemaFilter) args.push(`--schema=${schemaFilter}`)
  if (snapshotId) args.push(`--snapshot=${snapshotId}`)
  args.push(...extraArgs)
  return args
}

export function buildSequenceQuery(schemaFilter: string | undefined): string {
  const schemaClause = schemaFilter
    ? `WHERE schemaname = '${schemaFilter}'`
    : `WHERE schemaname NOT LIKE 'pg_%' AND schemaname <> 'information_schema'`

  return `
    SELECT schemaname, sequencename, last_value, COALESCE(is_called, false) AS is_called
    FROM pg_sequences
    ${schemaClause}
    ORDER BY schemaname, sequencename
  `
}

export function parseSequenceRows(rows: Array<{
  schemaname: string
  sequencename: string
  last_value: string | null
  is_called: boolean
}>): SequenceInfo[] {
  return rows
    .filter(r => r.last_value !== null)
    .map(r => ({
      schema: r.schemaname,
      name: r.sequencename,
      lastValue: parseInt(r.last_value!, 10),
      isCalled: r.is_called,
    }))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pnpm test`

Expected: all schema tests PASS.

- [ ] **Step 5: Commit**

```bash
git add ts/src/core/schema.ts ts/tests/unit/schema.test.ts
git commit -m "feat(ts): add schema discovery SQL builders and parsers"
```

---

### Task 9: Table chunker

**Files:**
- Create: `ts/src/core/chunker.ts`
- Create: `ts/tests/unit/chunker.test.ts`

- [ ] **Step 1: Write failing tests**

```typescript
// ts/tests/unit/chunker.test.ts
import { describe, it, expect } from 'vitest'
import {
  planChunks,
  buildCopyQuery,
  chunkFilePath,
} from '../../src/core/chunker.js'
import type { TableInfo, ChunkMeta } from '../../src/types/index.js'

const makeTable = (overrides: Partial<TableInfo> = {}): TableInfo => ({
  oid: 16385,
  schemaName: 'public',
  tableName: 'users',
  relkind: 'r',
  relpages: 1000,
  estimatedRows: 100000,
  actualBytes: 2_000_000_000,
  pkColumn: 'id',
  pkType: 'int8',
  columns: ['id', 'name', 'email'],
  generatedColumns: [],
  ...overrides,
})

describe('planChunks', () => {
  it('returns single chunk for small table', () => {
    const table = makeTable({ actualBytes: 100_000 })
    const chunks = planChunks(table, {
      splitThreshold: 1_073_741_824,
      maxChunks: 32,
      pgMajorVersion: 16,
      pkMin: null,
      pkMax: null,
    })
    expect(chunks).toHaveLength(1)
    expect(chunks[0]!.index).toBe(0)
  })

  it('splits by PK range for large table with integer PK', () => {
    const table = makeTable({ actualBytes: 4_000_000_000 })
    const chunks = planChunks(table, {
      splitThreshold: 1_073_741_824,
      maxChunks: 32,
      pgMajorVersion: 16,
      pkMin: 1,
      pkMax: 10_000_000,
    })
    expect(chunks.length).toBe(4)
    expect(chunks[0]!.rangeStart).toBe(1)
    expect(chunks[chunks.length - 1]!.rangeEnd).toBe(10_000_000)
    // Ranges should be contiguous
    for (let i = 1; i < chunks.length; i++) {
      expect(chunks[i]!.rangeStart).toBe(chunks[i - 1]!.rangeEnd! + 1)
    }
  })

  it('splits by ctid range for large table without PK on PG 14+', () => {
    const table = makeTable({
      actualBytes: 3_000_000_000,
      pkColumn: null,
      pkType: null,
      relpages: 365000,
    })
    const chunks = planChunks(table, {
      splitThreshold: 1_073_741_824,
      maxChunks: 32,
      pgMajorVersion: 16,
      pkMin: null,
      pkMax: null,
    })
    expect(chunks.length).toBe(3)
    expect(chunks[0]!.ctidStart).toBe(0)
    expect(chunks[chunks.length - 1]!.ctidEnd).toBeUndefined()  // open-ended
  })

  it('no splitting for table without PK on PG 13', () => {
    const table = makeTable({
      actualBytes: 3_000_000_000,
      pkColumn: null,
      pkType: null,
    })
    const chunks = planChunks(table, {
      splitThreshold: 1_073_741_824,
      maxChunks: 32,
      pgMajorVersion: 13,
      pkMin: null,
      pkMax: null,
    })
    expect(chunks).toHaveLength(1)
  })

  it('caps at maxChunks', () => {
    const table = makeTable({ actualBytes: 100_000_000_000 })
    const chunks = planChunks(table, {
      splitThreshold: 1_073_741_824,
      maxChunks: 8,
      pgMajorVersion: 16,
      pkMin: 1,
      pkMax: 100_000_000,
    })
    expect(chunks.length).toBeLessThanOrEqual(8)
  })
})

describe('buildCopyQuery', () => {
  const table = makeTable()

  it('builds plain COPY for unchunked table', () => {
    const chunk: ChunkMeta = { index: 0, file: 'data/public.users/chunk_0000.copy.lz4' }
    const sql = buildCopyQuery(table, chunk)
    expect(sql).toBe('COPY "public"."users" ("id", "name", "email") TO STDOUT')
  })

  it('builds COPY with PK range', () => {
    const chunk: ChunkMeta = {
      index: 0, file: '...', rangeStart: 1, rangeEnd: 1000000,
    }
    const sql = buildCopyQuery(table, chunk)
    expect(sql).toContain('SELECT "id", "name", "email"')
    expect(sql).toContain('WHERE "id" >= 1 AND "id" <= 1000000')
  })

  it('builds first chunk with NULL handling', () => {
    const chunk: ChunkMeta = {
      index: 0, file: '...', rangeStart: 1, rangeEnd: 1000000,
    }
    const sql = buildCopyQuery(table, chunk)
    expect(sql).toContain('"id" IS NULL OR')
  })

  it('builds COPY with ctid range', () => {
    const chunk: ChunkMeta = {
      index: 0, file: '...', ctidStart: 0, ctidEnd: 5000,
    }
    const sql = buildCopyQuery(table, chunk)
    expect(sql).toContain("ctid >= '(0,0)'::tid")
    expect(sql).toContain("ctid < '(5000,0)'::tid")
  })

  it('builds open-ended ctid for last chunk', () => {
    const chunk: ChunkMeta = {
      index: 2, file: '...', ctidStart: 10000,
    }
    const sql = buildCopyQuery(table, chunk)
    expect(sql).toContain("ctid >= '(10000,0)'::tid")
    expect(sql).not.toContain('ctid <')
  })

  it('excludes generated columns', () => {
    const t = makeTable({ generatedColumns: ['email'] })
    const chunk: ChunkMeta = { index: 0, file: '...' }
    const sql = buildCopyQuery(t, chunk)
    expect(sql).toContain('"id", "name"')
    expect(sql).not.toContain('"email"')
  })
})

describe('chunkFilePath', () => {
  it('generates correct path', () => {
    expect(chunkFilePath('public', 'users', 0)).toBe('data/public.users/chunk_0000.copy.lz4')
    expect(chunkFilePath('public', 'users', 12)).toBe('data/public.users/chunk_0012.copy.lz4')
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm test`

Expected: FAIL — module not found.

- [ ] **Step 3: Implement chunker**

```typescript
// ts/src/core/chunker.ts
import type { TableInfo, ChunkMeta, ChunkStrategy } from '../types/index.js'

export interface ChunkPlanOptions {
  splitThreshold: number     // bytes
  maxChunks: number
  pgMajorVersion: number
  pkMin: number | null       // from SELECT min(pk)
  pkMax: number | null       // from SELECT max(pk)
}

export function chunkFilePath(schema: string, table: string, index: number): string {
  const pad = index.toString().padStart(4, '0')
  return `data/${schema}.${table}/chunk_${pad}.copy.lz4`
}

export function planChunks(table: TableInfo, opts: ChunkPlanOptions): ChunkMeta[] {
  const { splitThreshold, maxChunks, pgMajorVersion, pkMin, pkMax } = opts

  // Small table — no splitting
  if (table.actualBytes < splitThreshold) {
    return [{ index: 0, file: chunkFilePath(table.schemaName, table.tableName, 0) }]
  }

  // PK range splitting
  if (table.pkColumn && pkMin !== null && pkMax !== null) {
    return planPkRangeChunks(table, pkMin, pkMax, splitThreshold, maxChunks)
  }

  // ctid range splitting — only PG 14+ (Tid Range Scan)
  if (pgMajorVersion >= 14 && table.relpages > 0) {
    return planCtidChunks(table, splitThreshold, maxChunks)
  }

  // Fallback — single chunk
  return [{ index: 0, file: chunkFilePath(table.schemaName, table.tableName, 0) }]
}

function planPkRangeChunks(
  table: TableInfo,
  pkMin: number,
  pkMax: number,
  splitThreshold: number,
  maxChunks: number,
): ChunkMeta[] {
  const numChunks = Math.min(
    Math.ceil(table.actualBytes / splitThreshold),
    maxChunks,
  )
  const range = pkMax - pkMin + 1
  const chunkSize = Math.ceil(range / numChunks)
  const chunks: ChunkMeta[] = []

  for (let i = 0; i < numChunks; i++) {
    const start = pkMin + i * chunkSize
    const end = Math.min(start + chunkSize - 1, pkMax)
    chunks.push({
      index: i,
      file: chunkFilePath(table.schemaName, table.tableName, i),
      rangeStart: start,
      rangeEnd: end,
    })
  }

  return chunks
}

function planCtidChunks(
  table: TableInfo,
  splitThreshold: number,
  maxChunks: number,
): ChunkMeta[] {
  const blockSize = 8192
  const pagesPerChunk = Math.ceil(splitThreshold / blockSize)
  const numChunks = Math.min(
    Math.ceil(table.relpages / pagesPerChunk),
    maxChunks,
  )
  const chunks: ChunkMeta[] = []

  for (let i = 0; i < numChunks; i++) {
    const start = i * pagesPerChunk
    const isLast = i === numChunks - 1
    chunks.push({
      index: i,
      file: chunkFilePath(table.schemaName, table.tableName, i),
      ctidStart: start,
      ...(isLast ? {} : { ctidEnd: start + pagesPerChunk }),
    })
  }

  return chunks
}

export function buildCopyQuery(table: TableInfo, chunk: ChunkMeta): string {
  const cols = table.columns
    .filter(c => !table.generatedColumns.includes(c))
    .map(c => `"${c}"`)
    .join(', ')
  const qualifiedTable = `"${table.schemaName}"."${table.tableName}"`

  // PK range chunk
  if (chunk.rangeStart !== undefined && chunk.rangeEnd !== undefined) {
    const pk = `"${table.pkColumn!}"`
    const nullClause = chunk.index === 0 ? `${pk} IS NULL OR ` : ''
    return `COPY (SELECT ${cols} FROM ${qualifiedTable} WHERE ${nullClause}${pk} >= ${chunk.rangeStart} AND ${pk} <= ${chunk.rangeEnd}) TO STDOUT`
  }

  // ctid range chunk
  if (chunk.ctidStart !== undefined) {
    const startClause = `ctid >= '(${chunk.ctidStart},0)'::tid`
    const endClause = chunk.ctidEnd !== undefined
      ? ` AND ctid < '(${chunk.ctidEnd},0)'::tid`
      : ''
    return `COPY (SELECT ${cols} FROM ${qualifiedTable} WHERE ${startClause}${endClause}) TO STDOUT`
  }

  // Single chunk — plain COPY
  return `COPY ${qualifiedTable} (${cols}) TO STDOUT`
}

export function chunkStrategy(table: TableInfo, opts: ChunkPlanOptions): ChunkStrategy {
  if (table.actualBytes < opts.splitThreshold) return 'none'
  if (table.pkColumn && opts.pkMin !== null && opts.pkMax !== null) return 'pk_range'
  if (opts.pgMajorVersion >= 14 && table.relpages > 0) return 'ctid_range'
  return 'none'
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pnpm test`

Expected: all chunker tests PASS.

- [ ] **Step 5: Commit**

```bash
git add ts/src/core/chunker.ts ts/tests/unit/chunker.test.ts
git commit -m "feat(ts): add table chunker with PK range and ctid splitting"
```

---

### Task 10: COPY stream pipeline

**Files:**
- Create: `ts/src/core/copy-stream.ts`
- Create: `ts/tests/unit/copy-stream.test.ts`

- [ ] **Step 1: Write failing tests**

The COPY pipeline depends on real pg connections for full testing (integration). Unit tests validate the pipeline builder and restore query builder.

```typescript
// ts/tests/unit/copy-stream.test.ts
import { describe, it, expect } from 'vitest'
import {
  buildRestoreCopyQuery,
  chunkDoneMarker,
  chunkRestoredMarker,
} from '../../src/core/copy-stream.js'

describe('buildRestoreCopyQuery', () => {
  it('builds COPY FROM query with columns', () => {
    const sql = buildRestoreCopyQuery('public', 'users', ['id', 'name', 'email'])
    expect(sql).toBe('COPY "public"."users" ("id", "name", "email") FROM STDIN')
  })

  it('quotes schema and table names', () => {
    const sql = buildRestoreCopyQuery('my schema', 'my-table', ['col'])
    expect(sql).toBe('COPY "my schema"."my-table" ("col") FROM STDIN')
  })
})

describe('marker helpers', () => {
  it('generates dump done marker path', () => {
    expect(chunkDoneMarker('/out/data/public.users/chunk_0000.copy.lz4'))
      .toBe('/out/data/public.users/chunk_0000.copy.lz4.done')
  })

  it('generates restore done marker path', () => {
    expect(chunkRestoredMarker('/out/data/public.users/chunk_0000.copy.lz4'))
      .toBe('/out/data/public.users/chunk_0000.copy.lz4.restored.done')
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm test`

Expected: FAIL — module not found.

- [ ] **Step 3: Implement copy-stream module**

```typescript
// ts/src/core/copy-stream.ts
import { pipeline } from 'node:stream/promises'
import { createWriteStream, createReadStream } from 'node:fs'
import { mkdir, writeFile, unlink } from 'node:fs/promises'
import { dirname } from 'node:path'
import { to as copyTo, from as copyFrom } from 'pg-copy-streams'
import lz4 from 'lz4'
import type pg from 'pg'

export function chunkDoneMarker(chunkPath: string): string {
  return `${chunkPath}.done`
}

export function chunkRestoredMarker(chunkPath: string): string {
  return `${chunkPath}.restored.done`
}

export function buildRestoreCopyQuery(schema: string, table: string, columns: string[]): string {
  const cols = columns.map(c => `"${c}"`).join(', ')
  return `COPY "${schema}"."${table}" (${cols}) FROM STDIN`
}

export interface DumpChunkResult {
  rowCount: number
  bytesWritten: number
}

export async function dumpChunk(
  client: pg.Client,
  copyQuery: string,
  outputPath: string,
  onData?: (bytes: number) => void,
): Promise<DumpChunkResult> {
  await mkdir(dirname(outputPath), { recursive: true })

  const copyStream = client.query(copyTo(copyQuery))
  const compressor = lz4.createEncoderStream({ blockMaxSize: 4 * 1024 * 1024 })
  const fileStream = createWriteStream(outputPath)

  let bytesWritten = 0
  compressor.on('data', (chunk: Buffer) => {
    bytesWritten += chunk.length
    onData?.(chunk.length)
  })

  await pipeline(copyStream, compressor, fileStream)

  // Write done marker
  await writeFile(chunkDoneMarker(outputPath), '', 'utf-8')

  return {
    rowCount: copyStream.rowCount,
    bytesWritten,
  }
}

export async function restoreChunk(
  client: pg.Client,
  schema: string,
  table: string,
  columns: string[],
  inputPath: string,
): Promise<void> {
  const copyQuery = buildRestoreCopyQuery(schema, table, columns)
  const copyStream = client.query(copyFrom(copyQuery))
  const decompressor = lz4.createDecoderStream()
  const fileStream = createReadStream(inputPath)

  await pipeline(fileStream, decompressor, copyStream)

  // Write restored marker
  await writeFile(chunkRestoredMarker(inputPath), '', 'utf-8')
}

export async function removePartialChunk(outputPath: string): Promise<void> {
  await unlink(outputPath).catch(() => {})
  await unlink(chunkDoneMarker(outputPath)).catch(() => {})
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pnpm test`

Expected: all copy-stream unit tests PASS.

- [ ] **Step 5: Commit**

```bash
git add ts/src/core/copy-stream.ts ts/tests/unit/copy-stream.test.ts
git commit -m "feat(ts): add COPY stream pipeline with lz4 compression"
```

---

## Phase 3 Complete

At this point you have:
- Schema discovery SQL builders and result parsers — tested
- Table chunker with PK range and ctid range splitting — tested
- COPY TO/FROM pipeline with lz4 streaming compression — tested (unit); full integration tested in Phase 5
- All pure logic is unit-testable without a database connection
