# Phase 2: Connection Manager, Manifest & CLI Args

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build connection string handling, snapshot management, manifest serialization, and CLI argument parsing.

**Architecture:** Connection module manages pg Client lifecycle with keepalive tuning. Manifest module serializes/deserializes manifest.json. Args module sets up commander with typed options.

**Tech Stack:** pg, commander, @commander-js/extra-typings, nano-spawn

**Depends on:** Phase 1 (types)

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `ts/src/core/connection.ts` | Connection string helpers, snapshot coordinator, worker factory |
| Create | `ts/tests/unit/connection.test.ts` | Tests for connection module |
| Create | `ts/src/core/manifest.ts` | Read/write manifest.json |
| Create | `ts/tests/unit/manifest.test.ts` | Tests for manifest module |
| Create | `ts/src/cli/args.ts` | CLI argument parsing with commander |
| Create | `ts/tests/unit/args.test.ts` | Tests for argument parsing |

---

### Task 5: Connection string helpers

**Files:**
- Create: `ts/src/core/connection.ts`
- Create: `ts/tests/unit/connection.test.ts`

- [ ] **Step 1: Write failing tests for pure string helpers**

```typescript
// ts/tests/unit/connection.test.ts
import { describe, it, expect } from 'vitest'
import {
  sanitizeConnectionString,
  extractDbName,
  cleanConnectionString,
  appendKeepaliveParams,
} from '../../src/core/connection.js'

describe('sanitizeConnectionString', () => {
  it('masks password', () => {
    expect(sanitizeConnectionString('postgresql://user:secret@host/db'))
      .toBe('postgresql://user:***@host/db')
  })

  it('handles no password', () => {
    expect(sanitizeConnectionString('postgresql://user@host/db'))
      .toBe('postgresql://user@host/db')
  })

  it('strips query params', () => {
    expect(sanitizeConnectionString('postgresql://user:pass@host/db?sslmode=require'))
      .toBe('postgresql://user:***@host/db')
  })
})

describe('extractDbName', () => {
  it('extracts database name', () => {
    expect(extractDbName('postgresql://user:pass@host/mydb')).toBe('mydb')
  })

  it('strips query params', () => {
    expect(extractDbName('postgresql://user:pass@host/mydb?sslmode=require')).toBe('mydb')
  })

  it('handles port', () => {
    expect(extractDbName('postgresql://user:pass@host:5432/mydb')).toBe('mydb')
  })
})

describe('cleanConnectionString', () => {
  it('strips GUI query params but keeps sslmode', () => {
    const cs = 'postgresql://u:p@h/db?statusColor=red&sslmode=require&env=staging'
    expect(cleanConnectionString(cs)).toBe('postgresql://u:p@h/db?sslmode=require')
  })

  it('strips all params when no sslmode', () => {
    const cs = 'postgresql://u:p@h/db?statusColor=red&env=staging'
    expect(cleanConnectionString(cs)).toBe('postgresql://u:p@h/db')
  })

  it('returns as-is when no query params', () => {
    expect(cleanConnectionString('postgresql://u:p@h/db')).toBe('postgresql://u:p@h/db')
  })
})

describe('appendKeepaliveParams', () => {
  it('appends keepalive params to clean URL', () => {
    const result = appendKeepaliveParams('postgresql://u:p@h/db')
    expect(result).toContain('keepalives=1')
    expect(result).toContain('keepalives_idle=10')
    expect(result).toContain('keepalives_interval=10')
    expect(result).toContain('keepalives_count=5')
    expect(result).toContain('connect_timeout=10')
  })

  it('appends to URL with existing params', () => {
    const result = appendKeepaliveParams('postgresql://u:p@h/db?sslmode=require')
    expect(result).toContain('sslmode=require')
    expect(result).toContain('keepalives=1')
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm test`

Expected: FAIL — module not found.

- [ ] **Step 3: Implement connection string helpers**

```typescript
// ts/src/core/connection.ts
import pg from 'pg'

const { Client } = pg

const KEEPALIVE_PARAMS = [
  'keepalives=1',
  'keepalives_idle=10',
  'keepalives_interval=10',
  'keepalives_count=5',
  'tcp_user_timeout=30000',
  'connect_timeout=10',
].join('&')

export function sanitizeConnectionString(cs: string): string {
  return cs.replace(/:\/\/([^:]+):[^@]+@/, '://$1:***@').replace(/\?.*/, '')
}

export function extractDbName(cs: string): string {
  const match = cs.match(/\/([^/?]+)(?:\?.*)?$/)
  return match?.[1] ?? ''
}

export function cleanConnectionString(cs: string): string {
  const [base, query] = cs.split('?')
  if (!query) return base!
  const sslmode = query.split('&').find(p => p.toLowerCase().startsWith('sslmode='))
  return sslmode ? `${base}?${sslmode}` : base!
}

export function appendKeepaliveParams(cs: string): string {
  const separator = cs.includes('?') ? '&' : '?'
  return `${cs}${separator}${KEEPALIVE_PARAMS}`
}

export interface SnapshotCoordinator {
  snapshotId: string
  client: InstanceType<typeof Client>
  close: () => Promise<void>
}

export async function createSnapshotCoordinator(connectionString: string): Promise<SnapshotCoordinator> {
  const client = new Client({ connectionString: appendKeepaliveParams(connectionString) })
  await client.connect()
  await client.query('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ')
  const { rows } = await client.query('SELECT pg_export_snapshot() AS snapshot_id')
  const snapshotId = rows[0].snapshot_id as string

  return {
    snapshotId,
    client,
    close: async () => {
      await client.query('COMMIT').catch(() => {})
      await client.end()
    },
  }
}

export async function createWorkerClient(
  connectionString: string,
  snapshotId: string | null,
): Promise<InstanceType<typeof Client>> {
  const client = new Client({ connectionString: appendKeepaliveParams(connectionString) })
  await client.connect()
  if (snapshotId) {
    await client.query('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ')
    await client.query(`SET TRANSACTION SNAPSHOT '${snapshotId}'`)
  }
  return client
}

export async function testConnection(connectionString: string): Promise<string> {
  const client = new Client({ connectionString: appendKeepaliveParams(connectionString) })
  await client.connect()
  try {
    const { rows } = await client.query('SELECT version() AS version')
    return rows[0].version as string
  } finally {
    await client.end()
  }
}

export async function isReadReplica(connectionString: string): Promise<boolean> {
  const client = new Client({ connectionString: appendKeepaliveParams(connectionString) })
  await client.connect()
  try {
    const { rows } = await client.query('SELECT pg_is_in_recovery() AS is_replica')
    return rows[0].is_replica as boolean
  } finally {
    await client.end()
  }
}

export async function releaseWorkerClient(client: InstanceType<typeof Client>): Promise<void> {
  await client.query('COMMIT').catch(() => {})
  await client.end()
}

export async function destroyClient(client: InstanceType<typeof Client>): Promise<void> {
  await client.end().catch(() => {})
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pnpm test`

Expected: all connection string helper tests PASS. (Snapshot/worker tests require a real DB — covered in integration tests.)

- [ ] **Step 5: Commit**

```bash
git add ts/src/core/connection.ts ts/tests/unit/connection.test.ts
git commit -m "feat(ts): add connection string helpers and snapshot coordinator"
```

---

### Task 6: Manifest module

**Files:**
- Create: `ts/src/core/manifest.ts`
- Create: `ts/tests/unit/manifest.test.ts`

- [ ] **Step 1: Write failing tests**

```typescript
// ts/tests/unit/manifest.test.ts
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { writeManifest, readManifest } from '../../src/core/manifest.js'
import type { DumpManifest } from '../../src/types/index.js'
import { mkdtempSync, rmSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'

const SAMPLE_MANIFEST: DumpManifest = {
  version: 1,
  tool: 'pg-resilient',
  createdAt: '2026-03-31T05:00:00.000Z',
  pgVersion: '16.2',
  database: 'testdb',
  snapshotId: '00000003-1B',
  compression: 'lz4',
  options: {
    schemaFilter: null,
    splitThresholdBytes: 1_073_741_824,
    jobs: 4,
  },
  tables: [
    {
      schema: 'public',
      name: 'users',
      oid: 16385,
      relkind: 'r',
      estimatedBytes: 1024,
      estimatedRows: 10,
      pkColumn: 'id',
      pkType: 'int8',
      chunkStrategy: 'none',
      columns: ['id', 'name'],
      generatedColumns: [],
      chunks: [{ index: 0, file: 'data/public.users/chunk_0000.copy.lz4' }],
    },
  ],
  sequences: [],
}

describe('manifest', () => {
  let tmpDir: string

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'pgr-test-'))
  })

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true })
  })

  it('writes and reads manifest roundtrip', async () => {
    await writeManifest(tmpDir, SAMPLE_MANIFEST)
    const read = await readManifest(tmpDir)
    expect(read).toEqual(SAMPLE_MANIFEST)
  })

  it('throws on missing manifest', async () => {
    await expect(readManifest(tmpDir)).rejects.toThrow()
  })

  it('throws on invalid version', async () => {
    const bad = { ...SAMPLE_MANIFEST, version: 99 }
    await writeManifest(tmpDir, bad as unknown as DumpManifest)
    await expect(readManifest(tmpDir)).rejects.toThrow('version')
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm test`

Expected: FAIL — module not found.

- [ ] **Step 3: Implement manifest module**

```typescript
// ts/src/core/manifest.ts
import { readFile, writeFile, mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import type { DumpManifest } from '../types/index.js'

const MANIFEST_FILENAME = 'manifest.json'
const CURRENT_VERSION = 1

export async function writeManifest(outputDir: string, manifest: DumpManifest): Promise<void> {
  await mkdir(outputDir, { recursive: true })
  const path = join(outputDir, MANIFEST_FILENAME)
  await writeFile(path, JSON.stringify(manifest, null, 2) + '\n', 'utf-8')
}

export async function readManifest(inputDir: string): Promise<DumpManifest> {
  const path = join(inputDir, MANIFEST_FILENAME)
  const raw = await readFile(path, 'utf-8')
  const parsed = JSON.parse(raw) as DumpManifest

  if (parsed.version !== CURRENT_VERSION) {
    throw new Error(
      `Unsupported manifest version ${parsed.version} (expected ${CURRENT_VERSION})`
    )
  }

  return parsed
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pnpm test`

Expected: all manifest tests PASS.

- [ ] **Step 5: Commit**

```bash
git add ts/src/core/manifest.ts ts/tests/unit/manifest.test.ts
git commit -m "feat(ts): add manifest read/write with version validation"
```

---

### Task 7: CLI argument parsing

**Files:**
- Create: `ts/src/cli/args.ts`
- Create: `ts/tests/unit/args.test.ts`

- [ ] **Step 1: Write failing tests**

```typescript
// ts/tests/unit/args.test.ts
import { describe, it, expect } from 'vitest'
import { parseDumpArgs, parseRestoreArgs, parseSize } from '../../src/cli/args.js'

describe('parseSize', () => {
  it('parses plain bytes', () => {
    expect(parseSize('1024')).toBe(1024)
  })

  it('parses KB', () => {
    expect(parseSize('10KB')).toBe(10 * 1024)
    expect(parseSize('10kb')).toBe(10 * 1024)
  })

  it('parses MB', () => {
    expect(parseSize('256MB')).toBe(256 * 1024 * 1024)
  })

  it('parses GB', () => {
    expect(parseSize('1GB')).toBe(1024 * 1024 * 1024)
  })

  it('throws on invalid input', () => {
    expect(() => parseSize('abc')).toThrow()
  })
})

describe('parseDumpArgs', () => {
  it('parses required args', () => {
    const opts = parseDumpArgs(['-d', 'postgresql://u:p@h/db', '--output', './out'])
    expect(opts.dbname).toBe('postgresql://u:p@h/db')
    expect(opts.output).toBe('./out')
  })

  it('applies defaults', () => {
    const opts = parseDumpArgs(['-d', 'pg://h/db', '--output', './out'])
    expect(opts.jobs).toBe(4)
    expect(opts.retries).toBe(5)
    expect(opts.retryDelay).toBe(5)
    expect(opts.dryRun).toBe(false)
    expect(opts.noSnapshot).toBe(false)
    expect(opts.splitThreshold).toBe(1024 * 1024 * 1024)
    expect(opts.maxChunksPerTable).toBe(32)
  })

  it('parses optional flags', () => {
    const opts = parseDumpArgs([
      '-d', 'pg://h/db', '--output', './out',
      '-n', 'public', '-j', '8', '--dry-run', '--no-snapshot',
      '--split-threshold', '512MB',
    ])
    expect(opts.schema).toBe('public')
    expect(opts.jobs).toBe(8)
    expect(opts.dryRun).toBe(true)
    expect(opts.noSnapshot).toBe(true)
    expect(opts.splitThreshold).toBe(512 * 1024 * 1024)
  })

  it('captures passthrough args after --', () => {
    const opts = parseDumpArgs([
      '-d', 'pg://h/db', '--output', './out',
      '--', '--no-comments', '--lock-wait-timeout=300',
    ])
    expect(opts.pgDumpArgs).toEqual(['--no-comments', '--lock-wait-timeout=300'])
  })
})

describe('parseRestoreArgs', () => {
  it('parses required args', () => {
    const opts = parseRestoreArgs(['-d', 'pg://h/db', '--input', './in'])
    expect(opts.dbname).toBe('pg://h/db')
    expect(opts.input).toBe('./in')
  })

  it('parses restore-specific flags', () => {
    const opts = parseRestoreArgs([
      '-d', 'pg://h/db', '--input', './in',
      '-c', '-a', '-t', 'users',
    ])
    expect(opts.clean).toBe(true)
    expect(opts.dataOnly).toBe(true)
    expect(opts.table).toBe('users')
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm test`

Expected: FAIL — module not found.

- [ ] **Step 3: Implement CLI args**

```typescript
// ts/src/cli/args.ts
import { Command } from 'commander'
import type { DumpOptions, RestoreOptions } from '../types/index.js'

export function parseSize(value: string): number {
  const match = value.match(/^(\d+(?:\.\d+)?)\s*(KB|MB|GB|TB)?$/i)
  if (!match) throw new Error(`Invalid size: ${value}`)
  const num = parseFloat(match[1]!)
  const unit = (match[2] ?? '').toUpperCase()
  const multipliers: Record<string, number> = {
    '': 1,
    KB: 1024,
    MB: 1024 ** 2,
    GB: 1024 ** 3,
    TB: 1024 ** 4,
  }
  return Math.floor(num * (multipliers[unit] ?? 1))
}

export function parseDumpArgs(argv: string[]): DumpOptions {
  const cmd = new Command()
    .requiredOption('-d, --dbname <cs>', 'PostgreSQL connection string')
    .requiredOption('--output <dir>', 'Output directory')
    .option('-n, --schema <name>', 'Dump only this schema')
    .option('-j, --jobs <n>', 'Parallel workers', '4')
    .option('--split-threshold <size>', 'Chunk tables larger than this', '1GB')
    .option('--max-chunks-per-table <n>', 'Max chunks per table', '32')
    .option('--retries <n>', 'Max retries per chunk', '5')
    .option('--retry-delay <s>', 'Base retry delay (seconds)', '5')
    .option('--no-snapshot', 'Skip snapshot (for read replicas)')
    .option('--dry-run', 'Preview without dumping')
    .allowUnknownOption(false)
    .passThroughOptions()
    .exitOverride()
    .configureOutput({ writeErr: () => {}, writeOut: () => {} })

  cmd.parse(argv, { from: 'user' })
  const opts = cmd.opts()
  const passthrough = cmd.args

  return {
    dbname: opts.dbname as string,
    output: opts.output as string,
    schema: opts.schema as string | undefined,
    jobs: parseInt(opts.jobs as string, 10),
    splitThreshold: parseSize(opts.splitThreshold as string),
    maxChunksPerTable: parseInt(opts.maxChunksPerTable as string, 10),
    retries: parseInt(opts.retries as string, 10),
    retryDelay: parseInt(opts.retryDelay as string, 10),
    noSnapshot: opts.snapshot === false,
    dryRun: opts.dryRun === true,
    pgDumpArgs: passthrough,
  }
}

export function parseRestoreArgs(argv: string[]): RestoreOptions {
  const cmd = new Command()
    .requiredOption('-d, --dbname <cs>', 'PostgreSQL connection string')
    .requiredOption('--input <dir>', 'Input directory (from dump)')
    .option('-n, --schema <name>', 'Restore only tables in this schema')
    .option('-t, --table <name>', 'Restore a single table')
    .option('-j, --jobs <n>', 'Parallel workers', '4')
    .option('-c, --clean', 'DROP + CREATE schema before restore')
    .option('-a, --data-only', 'Skip DDL, restore only table data')
    .option('--retries <n>', 'Max retries per chunk', '5')
    .option('--retry-delay <s>', 'Base retry delay (seconds)', '5')
    .option('--dry-run', 'Preview without restoring')
    .allowUnknownOption(false)
    .passThroughOptions()
    .exitOverride()
    .configureOutput({ writeErr: () => {}, writeOut: () => {} })

  cmd.parse(argv, { from: 'user' })
  const opts = cmd.opts()
  const passthrough = cmd.args

  return {
    dbname: opts.dbname as string,
    input: opts.input as string,
    schema: opts.schema as string | undefined,
    table: opts.table as string | undefined,
    jobs: parseInt(opts.jobs as string, 10),
    clean: opts.clean === true,
    dataOnly: opts.dataOnly === true,
    retries: parseInt(opts.retries as string, 10),
    retryDelay: parseInt(opts.retryDelay as string, 10),
    dryRun: opts.dryRun === true,
    pgRestoreArgs: passthrough,
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pnpm test`

Expected: all args tests PASS.

- [ ] **Step 5: Commit**

```bash
git add ts/src/cli/args.ts ts/tests/unit/args.test.ts
git commit -m "feat(ts): add CLI argument parsing for dump and restore commands"
```

---

## Phase 2 Complete

At this point you have:
- Connection string helpers (clean, sanitize, extract, keepalive) — tested
- Snapshot coordinator and worker client factory — implemented (integration-tested later)
- Manifest read/write with version validation — tested
- CLI argument parsing for both commands — tested
- All unit tests passing
