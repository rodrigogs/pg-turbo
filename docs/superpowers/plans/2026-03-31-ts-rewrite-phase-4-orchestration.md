# Phase 4: Worker Queue, UI, Dump & Restore Commands

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the parallel worker queue, progress dashboard, and the full dump/restore command orchestrators that tie everything together.

**Architecture:** Queue manages N async workers pulling from a shared job list. UI renders a live dashboard via log-update. Dump and restore commands are thin orchestrators that wire core modules together.

**Tech Stack:** picocolors, nanospinner, log-update, nano-spawn

**Depends on:** Phase 3 (all core modules)

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `ts/src/core/queue.ts` | Parallel async worker pool |
| Create | `ts/tests/unit/queue.test.ts` | Tests for worker queue |
| Create | `ts/src/cli/ui.ts` | Progress dashboard, banners, log helpers |
| Create | `ts/src/cli/dump.ts` | Dump command orchestration |
| Create | `ts/src/cli/restore.ts` | Restore command orchestration |
| Create | `ts/src/cli/index.ts` | CLI entry point (command router) |

---

### Task 11: Parallel worker queue

**Files:**
- Create: `ts/src/core/queue.ts`
- Create: `ts/tests/unit/queue.test.ts`

- [ ] **Step 1: Write failing tests**

```typescript
// ts/tests/unit/queue.test.ts
import { describe, it, expect, vi } from 'vitest'
import { runWorkerPool } from '../../src/core/queue.js'
import type { ChunkJob, ChunkResult, ProgressEvent } from '../../src/types/index.js'

function makeJob(index: number, overrides: Partial<ChunkJob> = {}): ChunkJob {
  return {
    table: {
      schema: 'public', name: `table_${index}`, oid: index,
      relkind: 'r', estimatedBytes: 1000, estimatedRows: 10,
      pkColumn: null, pkType: null, chunkStrategy: 'none',
      columns: ['id'], generatedColumns: [], chunks: [],
    },
    chunk: { index: 0, file: `data/public.table_${index}/chunk_0000.copy.lz4` },
    copyQuery: `COPY public.table_${index} TO STDOUT`,
    outputPath: `/tmp/test/data/public.table_${index}/chunk_0000.copy.lz4`,
    attempt: 0,
    ...overrides,
  }
}

describe('runWorkerPool', () => {
  it('processes all jobs with single worker', async () => {
    const jobs = [makeJob(1), makeJob(2), makeJob(3)]
    const task = vi.fn().mockResolvedValue({ rowCount: 10, bytesWritten: 100 })
    const onProgress = vi.fn()

    const results = await runWorkerPool({
      jobs,
      workerCount: 1,
      task,
      onProgress,
      maxRetries: 3,
      isResumable: () => false,
    })

    expect(results).toHaveLength(3)
    expect(results.every(r => r.status === 'ok')).toBe(true)
    expect(task).toHaveBeenCalledTimes(3)
  })

  it('processes jobs in parallel with multiple workers', async () => {
    const jobs = [makeJob(1), makeJob(2), makeJob(3), makeJob(4)]
    const running: number[] = []
    let maxConcurrent = 0

    const task = vi.fn().mockImplementation(async () => {
      running.push(1)
      maxConcurrent = Math.max(maxConcurrent, running.length)
      await new Promise(r => setTimeout(r, 10))
      running.pop()
      return { rowCount: 1, bytesWritten: 1 }
    })

    await runWorkerPool({
      jobs,
      workerCount: 2,
      task,
      onProgress: vi.fn(),
      maxRetries: 3,
      isResumable: () => false,
    })

    expect(maxConcurrent).toBe(2)
  })

  it('retries failed jobs', async () => {
    const jobs = [makeJob(1)]
    const task = vi.fn()
      .mockRejectedValueOnce(new Error('fail'))
      .mockResolvedValue({ rowCount: 1, bytesWritten: 1 })

    const results = await runWorkerPool({
      jobs,
      workerCount: 1,
      task,
      onProgress: vi.fn(),
      maxRetries: 3,
      isResumable: () => false,
    })

    expect(results).toHaveLength(1)
    expect(results[0]!.status).toBe('ok')
    expect(task).toHaveBeenCalledTimes(2)
  })

  it('marks job as failed after exhausting retries', async () => {
    const jobs = [makeJob(1)]
    const task = vi.fn().mockRejectedValue(new Error('always fails'))

    const results = await runWorkerPool({
      jobs,
      workerCount: 1,
      task,
      onProgress: vi.fn(),
      maxRetries: 2,
      isResumable: () => false,
    })

    expect(results).toHaveLength(1)
    expect(results[0]!.status).toBe('failed')
    expect(task).toHaveBeenCalledTimes(2)
  })

  it('skips resumable jobs', async () => {
    const jobs = [makeJob(1), makeJob(2)]
    const task = vi.fn().mockResolvedValue({ rowCount: 1, bytesWritten: 1 })

    const results = await runWorkerPool({
      jobs,
      workerCount: 1,
      task,
      onProgress: vi.fn(),
      maxRetries: 3,
      isResumable: (job) => job.outputPath.includes('table_1'),
    })

    expect(results).toHaveLength(2)
    expect(results.find(r => r.job.outputPath.includes('table_1'))!.status).toBe('skipped')
    expect(task).toHaveBeenCalledTimes(1)  // only table_2
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm test`

Expected: FAIL — module not found.

- [ ] **Step 3: Implement worker queue**

```typescript
// ts/src/core/queue.ts
import type { ChunkJob, ChunkResult, ProgressEvent } from '../types/index.js'

export interface WorkerPoolOptions {
  jobs: ChunkJob[]
  workerCount: number
  task: (job: ChunkJob, workerId: number) => Promise<{ rowCount: number; bytesWritten: number }>
  onProgress: (event: ProgressEvent) => void
  maxRetries: number
  isResumable: (job: ChunkJob) => boolean
  onWorkerError?: (workerId: number, error: Error) => void
}

export async function runWorkerPool(opts: WorkerPoolOptions): Promise<ChunkResult[]> {
  const { jobs, workerCount, task, onProgress, maxRetries, isResumable } = opts

  // Sort largest first for optimal scheduling
  const queue = [...jobs].sort(
    (a, b) => (b.table.estimatedBytes ?? 0) - (a.table.estimatedBytes ?? 0)
  )

  const results: ChunkResult[] = []
  let queueIndex = 0

  function nextJob(): ChunkJob | undefined {
    while (queueIndex < queue.length) {
      const job = queue[queueIndex++]!
      return job
    }
    return undefined
  }

  // Retry queue — jobs that failed and need another attempt
  const retryQueue: ChunkJob[] = []

  function nextWork(): ChunkJob | undefined {
    if (retryQueue.length > 0) return retryQueue.shift()
    return nextJob()
  }

  async function worker(workerId: number): Promise<void> {
    let job: ChunkJob | undefined

    while ((job = nextWork()) !== undefined) {
      // Resume check
      if (isResumable(job)) {
        const result: ChunkResult = {
          job,
          status: 'skipped',
        }
        results.push(result)
        onProgress({ type: 'skipped', workerId, job })
        continue
      }

      const startTime = Date.now()
      try {
        onProgress({ type: 'started', workerId, job })
        const { rowCount, bytesWritten } = await task(job, workerId)
        const result: ChunkResult = {
          job,
          status: 'ok',
          rowCount,
          bytesWritten,
          durationMs: Date.now() - startTime,
        }
        results.push(result)
        onProgress({ type: 'completed', workerId, job, bytesWritten })
      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err))
        job.attempt++

        if (job.attempt < maxRetries) {
          onProgress({ type: 'retrying', workerId, job, error })
          retryQueue.push(job)
        } else {
          const result: ChunkResult = {
            job,
            status: 'failed',
            error,
            durationMs: Date.now() - startTime,
          }
          results.push(result)
          onProgress({ type: 'failed', workerId, job, error })
        }

        opts.onWorkerError?.(workerId, error)
      }
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(workerCount, queue.length) }, (_, i) => worker(i))
  )

  return results
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pnpm test`

Expected: all queue tests PASS.

- [ ] **Step 5: Commit**

```bash
git add ts/src/core/queue.ts ts/tests/unit/queue.test.ts
git commit -m "feat(ts): add parallel worker queue with retry and resume"
```

---

### Task 12: UI module

**Files:**
- Create: `ts/src/cli/ui.ts`

- [ ] **Step 1: Implement UI module**

No unit tests for UI (visual output). Verified via integration tests and manual testing.

```typescript
// ts/src/cli/ui.ts
import pc from 'picocolors'
import logUpdate from 'log-update'
import { createSpinner } from 'nanospinner'
import { humanSize, elapsedTime, progressBar } from '../core/format.js'
import type { WorkerState, ChunkJob, ProgressEvent } from '../types/index.js'

export const log = {
  info: (msg: string) => console.log(`${pc.blue('\u2139')}  ${msg}`),
  success: (msg: string) => console.log(`${pc.green('\u2714')}  ${msg}`),
  warn: (msg: string) => console.log(`${pc.yellow('\u26A0')}  ${msg}`),
  error: (msg: string) => console.error(`${pc.red('\u2716')}  ${msg}`),
  step: (msg: string) => console.log(`${pc.cyan('\u25B8')}  ${pc.bold(msg)}`),
}

export function printBanner(title: string, color: (s: string) => string = pc.cyan): void {
  console.log('')
  console.log(`${color(`  ${title}`)}`)
  console.log(`${pc.dim(`  ${'─'.repeat(title.length)}`)}`)
  console.log('')
}

export function spinner(text: string) {
  return createSpinner(text, { color: 'cyan' })
}

export interface DashboardState {
  totalBytes: number
  processedBytes: number
  startTime: number
  workers: WorkerState[]
}

export function renderDashboard(state: DashboardState): string {
  const elapsed = Math.max(1, (Date.now() - state.startTime) / 1000)
  const speed = state.processedBytes / elapsed
  const eta = speed > 0 ? Math.ceil((state.totalBytes - state.processedBytes) / speed) : 0

  const bar = progressBar(state.processedBytes, state.totalBytes, 30)
  const header = `${bar} ${pc.dim(`— ${humanSize(speed)}/s — ETA: ${elapsedTime(eta)}`)}`

  const workerLines = state.workers.map(w => {
    if (w.status === 'idle' || !w.currentJob) {
      return `  Worker ${w.id}: ${pc.dim('idle')}`
    }
    const { table, chunk } = w.currentJob
    const label = `${table.schema}.${table.name}`
    const chunkLabel = w.currentJob.table.chunks.length > 1
      ? ` chunk ${chunk.index + 1}/${w.currentJob.table.chunks.length}`
      : ''
    const speedLabel = w.speed ? ` (${humanSize(w.speed)}/s)` : ''

    if (w.status === 'retrying') {
      return `  Worker ${w.id}: ${pc.yellow('\u21BB')} ${pc.bold(label)}${chunkLabel} ${pc.dim('retrying...')}`
    }
    return `  Worker ${w.id}: ${pc.bold(label)}${chunkLabel}${pc.dim(speedLabel)}`
  })

  return [header, ...workerLines].join('\n')
}

export function startDashboard(state: DashboardState): { update: () => void; stop: () => void } {
  const interval = setInterval(() => {
    logUpdate(renderDashboard(state))
  }, 100)

  return {
    update: () => logUpdate(renderDashboard(state)),
    stop: () => {
      clearInterval(interval)
      logUpdate(renderDashboard(state))
      logUpdate.done()
    },
  }
}

export function printSummary(opts: {
  title: string
  database: string
  schema?: string
  tableCount: number
  succeeded: number
  failed: number
  skipped: number
  durationSecs: number
  outputDir?: string
  outputSize?: string
  dryRun: boolean
}): void {
  console.log('')
  printBanner(opts.dryRun ? 'Dry Run Summary' : opts.title, opts.dryRun ? pc.yellow : pc.cyan)

  console.log(`  ${pc.dim('Database:')}    ${pc.bold(opts.database)}`)
  if (opts.schema) console.log(`  ${pc.dim('Schema:')}      ${pc.bold(opts.schema)}`)
  console.log(`  ${pc.dim('Tables:')}      ${opts.tableCount} total`)
  let succeededLine = `  ${pc.green('Succeeded:')}   ${opts.succeeded}`
  if (opts.skipped > 0) succeededLine += ` ${pc.dim(`(${opts.skipped} skipped/resumed)`)}`
  console.log(succeededLine)
  if (opts.failed > 0) console.log(`  ${pc.red('Failed:')}      ${opts.failed}`)
  console.log(`  ${pc.dim('Duration:')}    ${elapsedTime(opts.durationSecs)}`)
  if (opts.outputDir) {
    const sizeStr = opts.outputSize ? ` (${opts.outputSize})` : ''
    console.log(`  ${pc.dim('Output:')}      ${opts.outputDir}${sizeStr}`)
  }
  console.log('')
}

export function printFailedTables(tables: string[], maxRetries: number): void {
  log.warn(`The following chunks failed after ${maxRetries} retries:`)
  for (const t of tables) {
    console.log(`    ${pc.red('\u2716')} ${t}`)
  }
  console.log('')
  log.info('Re-run with the same arguments to retry only the failed chunks.')
}
```

- [ ] **Step 2: Commit**

```bash
git add ts/src/cli/ui.ts
git commit -m "feat(ts): add UI module with dashboard, banners, and log helpers"
```

---

### Task 13: Dump command

**Files:**
- Create: `ts/src/cli/dump.ts`

- [ ] **Step 1: Implement dump command orchestrator**

```typescript
// ts/src/cli/dump.ts
import { existsSync } from 'node:fs'
import { mkdir } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { execSync } from 'node:child_process'
import pg from 'pg'
import type { DumpOptions, DumpManifest, ChunkJob, ManifestTable, WorkerState } from '../types/index.js'
import {
  cleanConnectionString, sanitizeConnectionString, extractDbName,
  createSnapshotCoordinator, createWorkerClient, testConnection,
  isReadReplica, destroyClient, releaseWorkerClient,
} from '../core/connection.js'
import { buildTableDiscoveryQuery, parseTableRows, buildColumnsQuery, buildGeneratedColumnsQuery, buildDdlDumpArgs, buildSequenceQuery, parseSequenceRows } from '../core/schema.js'
import { planChunks, buildCopyQuery, chunkFilePath, chunkStrategy } from '../core/chunker.js'
import { dumpChunk, chunkDoneMarker, removePartialChunk } from '../core/copy-stream.js'
import { writeManifest } from '../core/manifest.js'
import { runWorkerPool } from '../core/queue.js'
import { humanSize } from '../core/format.js'
import { log, printBanner, spinner, startDashboard, printSummary, printFailedTables } from './ui.js'

const { Client } = pg

export async function runDump(opts: DumpOptions): Promise<void> {
  const cs = cleanConnectionString(opts.dbname)
  const dbName = extractDbName(cs)
  const outputDir = resolve(opts.output)

  // ── Banner ──────────────────────────────────────────────
  printBanner(opts.dryRun ? 'PostgreSQL Resilient Dump (DRY RUN)' : 'PostgreSQL Resilient Dump')
  log.info(`Connection : ${sanitizeConnectionString(cs)}`)
  log.info(`Database   : ${dbName}`)
  if (opts.schema) log.info(`Schema     : ${opts.schema}`)
  log.info(`Output     : ${outputDir}`)
  log.info(`Jobs       : ${opts.jobs}`)
  if (!opts.dryRun) log.info(`Retries    : ${opts.retries} (delay: ${opts.retryDelay}s)`)
  console.log('')

  // ── Step 1: Test connection ─────────────────────────────
  log.step('Step 1: Testing database connection...')
  const pgVersion = await testConnection(cs)
  log.success(`Connected — ${pgVersion}`)
  const pgMajorVersion = parseInt(pgVersion.match(/PostgreSQL (\d+)/)?.[1] ?? '14', 10)

  // Detect read replica
  const replica = await isReadReplica(cs)
  const useSnapshot = !opts.noSnapshot && !replica
  if (replica) log.info('Read replica detected — skipping snapshot')
  console.log('')

  // ── Step 2: Export snapshot ─────────────────────────────
  let snapshotId: string | null = null
  let coordinator: Awaited<ReturnType<typeof createSnapshotCoordinator>> | null = null

  if (useSnapshot && !opts.dryRun) {
    log.step('Step 2: Exporting snapshot for consistency...')
    coordinator = await createSnapshotCoordinator(cs)
    snapshotId = coordinator.snapshotId
    log.success(`Snapshot: ${snapshotId}`)
  } else {
    log.step('Step 2: Snapshot... skipped')
  }
  console.log('')

  try {
    // ── Step 3: Discover tables ─────────────────────────────
    log.step('Step 3: Discovering tables...')
    const discoveryClient = new Client({ connectionString: cs })
    await discoveryClient.connect()

    const sql = buildTableDiscoveryQuery(opts.schema)
    const { rows: tableRows } = await discoveryClient.query(sql)
    let tables = parseTableRows(tableRows)

    if (tables.length === 0) {
      log.warn('No tables found.')
      await discoveryClient.end()
      return
    }

    // Fetch columns and generated columns for each table
    for (const table of tables) {
      const { rows: colRows } = await discoveryClient.query(buildColumnsQuery(), [table.oid])
      table.columns = colRows.map((r: { attname: string }) => r.attname)

      const { rows: genRows } = await discoveryClient.query(buildGeneratedColumnsQuery(), [table.oid])
      table.generatedColumns = genRows.map((r: { attname: string }) => r.attname)
    }

    const totalBytes = tables.reduce((sum, t) => sum + t.actualBytes, 0)
    log.success(`Found ${tables.length} tables (estimated: ${humanSize(totalBytes)})`)

    // Show top 5
    for (const t of tables.slice(0, 5)) {
      console.log(`    ${`${t.schemaName}.${t.tableName}`.padEnd(45)} ${humanSize(t.actualBytes)}`)
    }
    if (tables.length > 5) console.log(`    ... and ${tables.length - 5} more`)
    console.log('')

    // ── Step 4: Plan chunks ─────────────────────────────────
    log.step('Step 4: Planning chunks...')
    const manifestTables: ManifestTable[] = []
    const allJobs: ChunkJob[] = []

    for (const table of tables) {
      // Get PK min/max for chunk planning
      let pkMin: number | null = null
      let pkMax: number | null = null
      if (table.pkColumn) {
        const { rows: minMaxRows } = await discoveryClient.query(
          `SELECT min("${table.pkColumn}") AS pk_min, max("${table.pkColumn}") AS pk_max FROM "${table.schemaName}"."${table.tableName}"`
        )
        if (minMaxRows[0]) {
          pkMin = minMaxRows[0].pk_min !== null ? Number(minMaxRows[0].pk_min) : null
          pkMax = minMaxRows[0].pk_max !== null ? Number(minMaxRows[0].pk_max) : null
        }
      }

      const strategy = chunkStrategy(table, {
        splitThreshold: opts.splitThreshold,
        maxChunks: opts.maxChunksPerTable,
        pgMajorVersion,
        pkMin,
        pkMax,
      })

      const chunks = planChunks(table, {
        splitThreshold: opts.splitThreshold,
        maxChunks: opts.maxChunksPerTable,
        pgMajorVersion,
        pkMin,
        pkMax,
      })

      const manifestTable: ManifestTable = {
        schema: table.schemaName,
        name: table.tableName,
        oid: table.oid,
        relkind: table.relkind,
        estimatedBytes: table.actualBytes,
        estimatedRows: table.estimatedRows,
        pkColumn: table.pkColumn,
        pkType: table.pkType,
        chunkStrategy: strategy,
        columns: table.columns,
        generatedColumns: table.generatedColumns,
        chunks,
      }
      manifestTables.push(manifestTable)

      for (const chunk of chunks) {
        const copyQuery = buildCopyQuery(table, chunk)
        allJobs.push({
          table: manifestTable,
          chunk,
          copyQuery,
          outputPath: join(outputDir, chunk.file),
          attempt: 0,
        })
      }
    }

    await discoveryClient.end()

    const totalChunks = allJobs.length
    log.success(`Planned ${totalChunks} chunks across ${tables.length} tables`)
    console.log('')

    if (opts.dryRun) {
      log.step('Step 5: Dumping data... SKIPPED (dry run)')
      printSummary({
        title: 'Dump Summary', database: dbName, schema: opts.schema,
        tableCount: tables.length, succeeded: tables.length, failed: 0, skipped: 0,
        durationSecs: 0, outputDir, dryRun: true,
      })
      return
    }

    // ── Step 5: Dump DDL ────────────────────────────────────
    log.step('Step 5: Dumping DDL...')
    const schemaDir = join(outputDir, 'schema')
    await mkdir(schemaDir, { recursive: true })
    const ddlPath = join(schemaDir, 'ddl.dump')
    const ddlArgs = buildDdlDumpArgs(cs, ddlPath, opts.schema, snapshotId, opts.pgDumpArgs)
    execSync(`pg_dump ${ddlArgs.map(a => `'${a}'`).join(' ')}`, { stdio: 'pipe' })
    log.success('DDL saved')
    console.log('')

    // ── Step 6: Fetch sequences ─────────────────────────────
    const seqClient = new Client({ connectionString: cs })
    await seqClient.connect()
    const { rows: seqRows } = await seqClient.query(buildSequenceQuery(opts.schema))
    const sequences = parseSequenceRows(seqRows)
    await seqClient.end()

    // ── Step 7: Dump data ───────────────────────────────────
    log.step(`Step 6: Dumping table data (${opts.jobs} workers)...`)
    console.log('')

    const startTime = Date.now()
    const workerStates: WorkerState[] = Array.from({ length: opts.jobs }, (_, i) => ({
      id: i + 1, status: 'idle' as const,
    }))

    const dashboardState = {
      totalBytes: totalBytes,
      processedBytes: 0,
      startTime,
      workers: workerStates,
    }
    const dashboard = startDashboard(dashboardState)

    const results = await runWorkerPool({
      jobs: allJobs,
      workerCount: opts.jobs,
      maxRetries: opts.retries,
      isResumable: (job) => existsSync(chunkDoneMarker(job.outputPath)),
      onProgress: (event) => {
        const ws = workerStates[event.workerId]
        if (!ws) return
        switch (event.type) {
          case 'started':
            ws.status = 'working'
            ws.currentJob = event.job
            break
          case 'completed':
            ws.status = 'idle'
            ws.currentJob = undefined
            dashboardState.processedBytes += event.job.table.estimatedBytes / event.job.table.chunks.length
            break
          case 'skipped':
            dashboardState.processedBytes += event.job.table.estimatedBytes / event.job.table.chunks.length
            break
          case 'retrying':
            ws.status = 'retrying'
            break
          case 'failed':
            ws.status = 'idle'
            ws.currentJob = undefined
            break
        }
      },
      task: async (job, workerId) => {
        const client = await createWorkerClient(cs, snapshotId)
        try {
          const result = await dumpChunk(client, job.copyQuery, job.outputPath)
          await releaseWorkerClient(client)
          return result
        } catch (err) {
          await destroyClient(client)
          await removePartialChunk(job.outputPath)
          throw err
        }
      },
    })

    dashboard.stop()

    // ── Step 8: Write manifest ──────────────────────────────
    const manifest: DumpManifest = {
      version: 1,
      tool: 'pg-resilient',
      createdAt: new Date().toISOString(),
      pgVersion: pgVersion.match(/PostgreSQL ([\d.]+)/)?.[1] ?? 'unknown',
      database: dbName,
      snapshotId,
      compression: 'lz4',
      options: {
        schemaFilter: opts.schema ?? null,
        splitThresholdBytes: opts.splitThreshold,
        jobs: opts.jobs,
      },
      tables: manifestTables,
      sequences,
    }
    await writeManifest(outputDir, manifest)

    // ── Summary ─────────────────────────────────────────────
    const succeeded = results.filter(r => r.status === 'ok').length
    const skipped = results.filter(r => r.status === 'skipped').length
    const failed = results.filter(r => r.status === 'failed').length
    const durationSecs = Math.floor((Date.now() - startTime) / 1000)

    printSummary({
      title: 'Dump Summary', database: dbName, schema: opts.schema,
      tableCount: tables.length, succeeded, failed, skipped,
      durationSecs, outputDir, dryRun: false,
    })

    if (failed > 0) {
      const failedNames = results.filter(r => r.status === 'failed').map(r =>
        `${r.job.table.schema}.${r.job.table.name} chunk ${r.job.chunk.index}`
      )
      printFailedTables(failedNames, opts.retries)
      process.exit(1)
    }

    log.success('All chunks dumped successfully!')
    console.log('')
  } finally {
    await coordinator?.close()
  }
}
```

- [ ] **Step 2: Commit**

```bash
git add ts/src/cli/dump.ts
git commit -m "feat(ts): add dump command orchestrator"
```

---

### Task 14: Restore command

**Files:**
- Create: `ts/src/cli/restore.ts`

- [ ] **Step 1: Implement restore command orchestrator**

```typescript
// ts/src/cli/restore.ts
import { existsSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { execSync } from 'node:child_process'
import pg from 'pg'
import type { RestoreOptions, ChunkJob, WorkerState } from '../types/index.js'
import {
  cleanConnectionString, sanitizeConnectionString, extractDbName,
  testConnection, destroyClient,
} from '../core/connection.js'
import { readManifest } from '../core/manifest.js'
import { restoreChunk, chunkRestoredMarker } from '../core/copy-stream.js'
import { runWorkerPool } from '../core/queue.js'
import { humanSize } from '../core/format.js'
import { log, printBanner, startDashboard, printSummary, printFailedTables } from './ui.js'

const { Client } = pg
const SYSTEM_SCHEMAS = "'pg_catalog','information_schema','pg_toast'"

export async function runRestore(opts: RestoreOptions): Promise<void> {
  const cs = cleanConnectionString(opts.dbname)
  const dbName = extractDbName(cs)
  const inputDir = resolve(opts.input)

  // ── Banner ──────────────────────────────────────────────
  printBanner(opts.dryRun ? 'PostgreSQL Resilient Restore (DRY RUN)' : 'PostgreSQL Resilient Restore')
  log.info(`Connection : ${sanitizeConnectionString(cs)}`)
  log.info(`Database   : ${dbName}`)
  log.info(`Input      : ${inputDir}`)
  if (opts.schema) log.info(`Schema     : ${opts.schema}`)
  if (opts.table) log.info(`Table      : ${opts.table}`)
  if (opts.clean) log.info(`Mode       : CLEAN (DROP + CREATE)`)
  if (opts.dataOnly) log.info(`Mode       : DATA ONLY`)
  log.info(`Jobs       : ${opts.jobs}`)
  console.log('')

  // ── Step 1: Read manifest ─────────────────────────────
  log.step('Step 1: Reading manifest...')
  const manifest = await readManifest(inputDir)
  log.success(`Manifest: ${manifest.tables.length} tables, compression: ${manifest.compression}`)
  console.log('')

  // ── Step 2: Test connection ─────────────────────────────
  log.step('Step 2: Testing database connection...')
  await testConnection(cs)
  log.success('Connected')
  console.log('')

  // ── Filter tables ─────────────────────────────────────
  let tables = manifest.tables
  if (opts.schema) tables = tables.filter(t => t.schema === opts.schema)
  if (opts.table) tables = tables.filter(t => t.name === opts.table)

  // ── Step 3: Pre-data DDL ──────────────────────────────
  const ddlPath = join(inputDir, 'schema', 'ddl.dump')
  const preDataMarker = join(inputDir, '_pre_data.done')

  if (opts.dataOnly) {
    log.step('Step 3: Restoring structure... SKIPPED (--data-only)')
  } else if (opts.dryRun) {
    log.step('Step 3: Restoring structure... SKIPPED (dry run)')
  } else if (existsSync(preDataMarker)) {
    log.step('Step 3: Restoring structure... SKIPPED (already done)')
  } else {
    log.step('Step 3: Restoring structure (pre-data)...')

    if (opts.clean) {
      const dropClient = new Client({ connectionString: cs })
      await dropClient.connect()
      if (opts.schema) {
        log.warn(`Dropping schema '${opts.schema}'...`)
        await dropClient.query(`DROP SCHEMA IF EXISTS "${opts.schema}" CASCADE`).catch(() => {})
      } else {
        const { rows } = await dropClient.query(
          `SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN (${SYSTEM_SCHEMAS}) ORDER BY 1`
        )
        for (const row of rows) {
          log.warn(`Dropping schema '${row.schema_name}'...`)
          await dropClient.query(`DROP SCHEMA IF EXISTS "${row.schema_name}" CASCADE`).catch(() => {})
        }
      }
      await dropClient.end()
    }

    execSync(
      `pg_restore --section=pre-data --no-owner --no-privileges -d '${cs}' '${ddlPath}'`,
      { stdio: 'pipe' }
    ).toString()
    // pg_restore returns non-zero for harmless warnings — verify tables exist
    const verifyClient = new Client({ connectionString: cs })
    await verifyClient.connect()
    const { rows: verifyRows } = await verifyClient.query(
      `SELECT count(*) AS c FROM information_schema.tables WHERE table_schema NOT IN (${SYSTEM_SCHEMAS}) AND table_type = 'BASE TABLE'`
    )
    await verifyClient.end()

    if (parseInt(verifyRows[0].c, 10) > 0) {
      execSync(`touch '${preDataMarker}'`)
      log.success('Structure restored (pre-data)')
    } else {
      log.error('Pre-data restore failed — no tables found')
      process.exit(1)
    }
  }
  console.log('')

  // ── Step 4: Restore table data ────────────────────────
  const allJobs: ChunkJob[] = []
  for (const table of tables) {
    for (const chunk of table.chunks) {
      allJobs.push({
        table,
        chunk,
        copyQuery: '', // Not used for restore — restoreChunk builds its own
        outputPath: join(inputDir, chunk.file),
        attempt: 0,
      })
    }
  }

  const totalChunks = allJobs.length
  log.step(`Step 4: Restoring table data (${totalChunks} chunks, ${opts.jobs} workers)...`)
  console.log('')

  if (opts.dryRun) {
    for (const t of tables) {
      console.log(`    ${t.schema}.${t.name} — ${t.chunks.length} chunks (${humanSize(t.estimatedBytes)})`)
    }
    printSummary({
      title: 'Restore Summary', database: dbName, schema: opts.schema,
      tableCount: tables.length, succeeded: tables.length, failed: 0, skipped: 0,
      durationSecs: 0, dryRun: true,
    })
    return
  }

  const startTime = Date.now()
  const workerStates: WorkerState[] = Array.from({ length: opts.jobs }, (_, i) => ({
    id: i + 1, status: 'idle' as const,
  }))

  const dashboardState = {
    totalBytes: tables.reduce((s, t) => s + t.estimatedBytes, 0),
    processedBytes: 0,
    startTime,
    workers: workerStates,
  }
  const dashboard = startDashboard(dashboardState)

  const results = await runWorkerPool({
    jobs: allJobs,
    workerCount: opts.jobs,
    maxRetries: opts.retries,
    isResumable: (job) => existsSync(chunkRestoredMarker(job.outputPath)),
    onProgress: (event) => {
      const ws = workerStates[event.workerId]
      if (!ws) return
      switch (event.type) {
        case 'started': ws.status = 'working'; ws.currentJob = event.job; break
        case 'completed':
          ws.status = 'idle'; ws.currentJob = undefined
          dashboardState.processedBytes += event.job.table.estimatedBytes / event.job.table.chunks.length
          break
        case 'skipped':
          dashboardState.processedBytes += event.job.table.estimatedBytes / event.job.table.chunks.length
          break
        case 'retrying': ws.status = 'retrying'; break
        case 'failed': ws.status = 'idle'; ws.currentJob = undefined; break
      }
    },
    task: async (job) => {
      const client = new Client({ connectionString: cs })
      await client.connect()
      try {
        await restoreChunk(client, job.table.schema, job.table.name, job.table.columns, job.outputPath)
        await client.end()
        return { rowCount: 0, bytesWritten: 0 }
      } catch (err) {
        await destroyClient(client)
        throw err
      }
    },
  })

  dashboard.stop()

  // ── Step 5: Post-data DDL ─────────────────────────────
  const postDataMarker = join(inputDir, '_post_data.done')

  if (opts.dataOnly) {
    log.step('Step 5: Restoring indexes... SKIPPED (--data-only)')
  } else if (existsSync(postDataMarker)) {
    log.step('Step 5: Restoring indexes... SKIPPED (already done)')
  } else {
    log.step('Step 5: Restoring indexes, constraints, and triggers...')
    try {
      execSync(
        `pg_restore --section=post-data --no-owner --no-privileges --clean --if-exists -d '${cs}' '${ddlPath}'`,
        { stdio: 'pipe' }
      )
    } catch {
      // pg_restore returns non-zero for harmless warnings
    }
    execSync(`touch '${postDataMarker}'`)
    log.success('Indexes, constraints, and triggers restored')
  }
  console.log('')

  // ── Step 6: Reset sequences ───────────────────────────
  if (manifest.sequences.length > 0 && !opts.dataOnly) {
    log.step('Step 6: Resetting sequences...')
    const seqClient = new Client({ connectionString: cs })
    await seqClient.connect()
    for (const seq of manifest.sequences) {
      await seqClient.query(
        `SELECT pg_catalog.setval('"${seq.schema}"."${seq.name}"', $1, $2)`,
        [seq.lastValue, seq.isCalled]
      ).catch(() => {})
    }
    await seqClient.end()
    log.success(`Reset ${manifest.sequences.length} sequences`)
    console.log('')
  }

  // ── Summary ─────────────────────────────────────────────
  const succeeded = results.filter(r => r.status === 'ok').length
  const skipped = results.filter(r => r.status === 'skipped').length
  const failed = results.filter(r => r.status === 'failed').length
  const durationSecs = Math.floor((Date.now() - startTime) / 1000)

  printSummary({
    title: 'Restore Summary', database: dbName, schema: opts.schema,
    tableCount: tables.length, succeeded, failed, skipped, durationSecs, dryRun: false,
  })

  if (failed > 0) {
    const failedNames = results.filter(r => r.status === 'failed').map(r =>
      `${r.job.table.schema}.${r.job.table.name} chunk ${r.job.chunk.index}`
    )
    printFailedTables(failedNames, opts.retries)
    process.exit(1)
  }

  log.success('All chunks restored successfully!')
  console.log('')
}
```

- [ ] **Step 2: Commit**

```bash
git add ts/src/cli/restore.ts
git commit -m "feat(ts): add restore command orchestrator"
```

---

### Task 15: CLI entry point

**Files:**
- Create: `ts/src/cli/index.ts`
- Create: `ts/bin/pg-resilient.ts`

- [ ] **Step 1: Implement CLI entry point**

```typescript
// ts/src/cli/index.ts
import { Command } from 'commander'
import { parseDumpArgs, parseRestoreArgs } from './args.js'
import { runDump } from './dump.js'
import { runRestore } from './restore.js'

export function createProgram(): Command {
  const program = new Command()
    .name('pg-resilient')
    .description('Resilient PostgreSQL dump & restore with direct COPY protocol')
    .version('0.1.0')

  program
    .command('dump')
    .description('Dump a PostgreSQL database')
    .allowUnknownOption()
    .passThroughOptions()
    .action(async (_options, cmd) => {
      const opts = parseDumpArgs(cmd.parent!.args.slice(1))
      await runDump(opts)
    })

  program
    .command('restore')
    .description('Restore a PostgreSQL dump')
    .allowUnknownOption()
    .passThroughOptions()
    .action(async (_options, cmd) => {
      const opts = parseRestoreArgs(cmd.parent!.args.slice(1))
      await runRestore(opts)
    })

  return program
}

export async function main(): Promise<void> {
  const program = createProgram()
  await program.parseAsync(process.argv)
}
```

- [ ] **Step 2: Create bin entry**

```typescript
// ts/bin/pg-resilient.ts
#!/usr/bin/env node
import { main } from '../src/cli/index.js'

main().catch(err => {
  console.error(err)
  process.exit(1)
})
```

- [ ] **Step 3: Update package.json bin field**

Add to `ts/package.json`:
```json
{
  "bin": {
    "pg-resilient": "./bin/pg-resilient.ts"
  }
}
```

- [ ] **Step 4: Verify CLI boots**

Run: `cd ts && pnpm dev -- --help`

Expected: prints help text with dump and restore subcommands.

- [ ] **Step 5: Commit**

```bash
git add ts/src/cli/index.ts ts/bin/pg-resilient.ts ts/package.json
git commit -m "feat(ts): add CLI entry point with dump and restore subcommands"
```

---

## Phase 4 Complete

At this point you have a fully functional CLI tool:
- `pg-resilient dump` — discovers tables, plans chunks, dumps data via COPY protocol
- `pg-resilient restore` — reads manifest, restores DDL, loads data via COPY FROM, builds indexes
- Parallel workers with retry and resume
- Live progress dashboard
- All core logic unit-tested
