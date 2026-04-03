import { execFileSync } from 'node:child_process'
import { existsSync } from 'node:fs'
import { mkdtemp, rm, stat, unlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import pg from 'pg'
import { extractArchive, isPgtArchive } from '../core/archive.js'
import { chunkEstimatedBytes } from '../core/chunker.js'
import {
  cleanConnectionString,
  createClient,
  destroyClient,
  extractDbName,
  sanitizeConnectionString,
  testConnection,
} from '../core/connection.js'
import {
  dropProgressTable,
  ensureProgressTable,
  fetchCompletedChunks,
  resetProgress,
  restoreChunk,
} from '../core/copy-stream.js'
import { humanSize } from '../core/format.js'
import { DDL_FILENAME, readManifest } from '../core/manifest.js'
import { runWorkerPool } from '../core/queue.js'
import { quoteIdent } from '../core/schema.js'
import type { ChunkJob, ManifestTable, RestoreOptions, WorkerState } from '../types/index.js'
import type { DashboardState } from './ui.js'
import {
  createProgressHandler,
  installSignalHandlers,
  log,
  printBanner,
  printFailedTables,
  printSummary,
  startDashboard,
} from './ui.js'

const { Client } = pg

export async function runRestore(opts: RestoreOptions): Promise<void> {
  const cs = cleanConnectionString(opts.dbname)
  const dbName = extractDbName(cs)
  const startTime = Date.now()

  let dashboard: ReturnType<typeof startDashboard> | null = null
  const signals = installSignalHandlers(() => dashboard)

  // ── Extract .pgt archive if needed ─────────────────────────────────────
  let inputDir = opts.input
  let tempDir: string | null = null

  if (isPgtArchive(opts.input)) {
    log.step('Extracting archive...')
    tempDir = await mkdtemp(join(tmpdir(), 'pgr-restore-'))
    await extractArchive(opts.input, tempDir)
    inputDir = tempDir
    log.success('Archive extracted')
    console.log('')
  }

  try {
    // ── Step 1: Read manifest ───────────────────────────────────────────────
    log.step('Reading manifest...')
    const manifest = await readManifest(inputDir)
    log.success(`Manifest loaded: ${manifest.tables.length} tables, ${manifest.sequences.length} sequences`)
    console.log('')

    // ── Banner ──────────────────────────────────────────────────────────────
    printBanner(opts.dryRun ? 'PostgreSQL Turbo Restore (DRY RUN)' : 'PostgreSQL Turbo Restore')
    log.info(`Connection : ${sanitizeConnectionString(cs)}`)
    log.info(`Database   : ${dbName}`)
    log.info(`Source DB  : ${manifest.database}`)
    if (opts.schema) log.info(`Schema     : ${opts.schema}`)
    if (opts.table) log.info(`Table      : ${opts.table}`)
    log.info(`Input      : ${opts.input}${tempDir ? ' (extracted)' : ''}`)
    if (!opts.dryRun) {
      log.info(`Workers    : ${opts.jobs}`)
      log.info(`Retries    : ${opts.retries} (delay: ${opts.retryDelay}s)`)
      if (opts.clean) log.info('Mode       : CLEAN (DROP + CREATE schema)')
      if (opts.dataOnly) log.info('Mode       : DATA ONLY (skip DDL)')
    }
    if (opts.pgRestoreArgs.length > 0) {
      log.info(`pg_restore args: ${opts.pgRestoreArgs.join(' ')}`)
    }
    console.log('')

    // ── Step 2: Test connection & setup progress tracking ────────────────────
    log.step('Testing connection...')
    const pgVersion = await testConnection(cs)
    log.success(`Connected — ${pgVersion.split(',')[0]}`)

    if (!opts.dryRun) {
      // Setup DB-based progress tracking (atomic with COPY transactions)
      const setupClient = await createClient(cs)
      try {
        await ensureProgressTable(setupClient)
      } finally {
        await setupClient.end()
      }
    }
    console.log('')

    // ── Filter tables ───────────────────────────────────────────────────────
    let tables: ManifestTable[] = manifest.tables
    if (opts.schema) {
      tables = tables.filter((t) => t.schema === opts.schema)
    }
    if (opts.table) {
      const tableFilter = opts.table
      tables = tables.filter((t) => {
        // Support both "table" and "schema.table" format
        if (tableFilter.includes('.')) {
          const [s, n] = tableFilter.split('.')
          return t.schema === s && t.name === n
        }
        return t.name === tableFilter
      })
    }
    log.info(`Restoring ${tables.length} tables (${humanSize(tables.reduce((s, t) => s + t.estimatedBytes, 0))})`)
    console.log('')

    const ddlPath = join(inputDir, DDL_FILENAME)
    // Scope resume markers by target database to avoid cross-target pollution
    const preDataMarker = join(inputDir, `_pre_data.${dbName}.done`)
    const postDataMarker = join(inputDir, `_post_data.${dbName}.done`)

    // ── Step 3: Clean (if requested) ────────────────────────────────────────
    if (opts.clean && !opts.dataOnly) {
      log.step('Cleaning...')
      if (opts.dryRun) {
        log.warn('Skipped (dry run)')
      } else if (opts.table) {
        // When filtering to a single table, only truncate that table — don't drop the entire schema
        const client = await createClient(cs)
        try {
          for (const t of tables) {
            if (t.relkind === 'm') continue
            const qt = `${quoteIdent(t.schema)}.${quoteIdent(t.name)}`
            log.info(`  TRUNCATE ${qt}`)
            await client.query(`TRUNCATE ${qt} CASCADE`).catch(() => {})
          }
          await resetProgress(client)
          log.success('Tables truncated, progress reset')
        } finally {
          await client.end()
        }
      } else {
        const client = await createClient(cs)
        try {
          const schemas = [...new Set(tables.map((t) => t.schema))]
          for (const schema of schemas) {
            log.info(`  DROP SCHEMA IF EXISTS ${quoteIdent(schema)} CASCADE`)
            await client.query(`DROP SCHEMA IF EXISTS ${quoteIdent(schema)} CASCADE`)
            log.info(`  CREATE SCHEMA ${quoteIdent(schema)}`)
            await client.query(`CREATE SCHEMA ${quoteIdent(schema)}`)
          }
          await resetProgress(client)
          log.success('Schemas cleaned, progress reset')
        } finally {
          await client.end()
        }
      }
      if (!opts.dryRun) {
        // Remove DDL markers so pre/post-data DDL re-runs after clean
        await unlink(preDataMarker).catch(() => {})
        await unlink(postDataMarker).catch(() => {})
      }
      console.log('')
    }

    // ── Step 4: Pre-data DDL ────────────────────────────────────────────────
    if (!opts.dataOnly) {
      log.step('Restoring pre-data DDL...')
      if (opts.dryRun) {
        log.warn('Skipped (dry run)')
      } else if (existsSync(preDataMarker)) {
        log.info('Already restored (resuming)')
      } else if (existsSync(ddlPath)) {
        try {
          const args = ['--section=pre-data', '--no-owner', '--no-privileges', '-d', cs, ...opts.pgRestoreArgs, ddlPath]
          execFileSync('pg_restore', args, { stdio: 'pipe' })
        } catch (err: unknown) {
          // pg_restore returns non-zero for warnings (e.g., "relation already exists")
          // This is expected behavior when resuming or restoring to non-empty DB
          const stderr = (err as { stderr?: Buffer })?.stderr?.toString() ?? ''
          if (stderr) log.warn(`pg_restore warnings: ${stderr.slice(0, 500)}`)
          else log.warn('pg_restore exited with warnings (this is often normal)')
        }
        await writeFile(preDataMarker, '', 'utf-8')
        log.success('Pre-data DDL restored')
      } else {
        log.warn('No DDL file found — skipping pre-data')
      }
      console.log('')
    }

    // ── Step 5: Restore table data via COPY ─────────────────────────────────
    let hadDataFailures = false
    let succeededCount = 0
    let failedCount = 0
    let skippedCount = 0
    log.step('Restoring table data...')

    // Fetch already-completed chunks from the target DB (atomic with COPY transactions)
    let completedChunks = new Set<string>()
    if (!opts.dryRun) {
      const progressClient = await createClient(cs)
      try {
        completedChunks = await fetchCompletedChunks(progressClient)
      } finally {
        await progressClient.end()
      }
      if (completedChunks.size > 0) log.info(`${completedChunks.size} chunks already restored (resuming)`)
    }

    // Build chunk jobs — skip materialized views (restored via REFRESH in post-data DDL)
    const allJobs: ChunkJob[] = []
    for (const table of tables) {
      if (table.relkind === 'm') {
        log.info(`Skipping materialized view ${table.schema}.${table.name} (refreshed via post-data DDL)`)
        continue
      }
      for (const chunk of table.chunks) {
        const inputPath = join(inputDir, chunk.file)
        allJobs.push({
          table,
          chunk,
          outputPath: inputPath,
          attempt: 0,
        })
      }
    }

    // Validate chunk files exist and collect their sizes for progress tracking
    const chunkFileSizes = new Map<string, number>()
    if (!opts.dryRun) {
      const missing: ChunkJob[] = []
      for (const j of allJobs) {
        if (!existsSync(j.outputPath)) {
          missing.push(j)
        } else {
          const { size } = await stat(j.outputPath)
          chunkFileSizes.set(j.outputPath, size)
        }
      }
      if (missing.length > 0) {
        for (const m of missing) log.error(`Missing chunk file: ${m.outputPath}`)
        throw new Error(`${missing.length} chunk file(s) missing from dump directory`)
      }
    }

    if (opts.dryRun) {
      log.warn('Skipped (dry run)')
      console.log('')
      log.info('Chunks that would be restored:')
      for (const job of allJobs) {
        const status = completedChunks.has(job.chunk.file) ? '(already restored)' : ''
        log.info(`  ${job.table.schema}.${job.table.name} chunk ${job.chunk.index} ${status}`)
      }
    } else if (allJobs.length > 0) {
      const totalBytes = tables.reduce((s, t) => s + t.estimatedBytes, 0)
      const workers: WorkerState[] = Array.from({ length: opts.jobs }, (_, i) => ({
        id: i,
        status: 'idle' as const,
        progressCurrent: 0,
        progressTotal: 0,
      }))
      const dashState: DashboardState = {
        totalBytes,
        processedBytes: 0,
        startTime: Date.now(),
        workers,
        completedChunks: 0,
        totalChunks: allJobs.length,
        failedChunks: 0,
        skippedChunks: 0,
        progressUnit: 'bytes',
        speedSamples: [],
      }
      dashboard = startDashboard(dashState)

      // Track worker clients for cleanup
      const workerClients = new Map<number, InstanceType<typeof Client>>()

      const results = await runWorkerPool({
        jobs: allJobs,
        workerCount: opts.jobs,
        maxRetries: opts.retries,
        retryDelayMs: opts.retryDelay * 1000,
        isResumable: (job) => completedChunks.has(job.chunk.file),
        onProgress: createProgressHandler(
          workers,
          dashState,
          dashboard,
          (event) => chunkEstimatedBytes(event.job),
          (job) => chunkFileSizes.get(job.outputPath) ?? 0,
        ),
        task: async (job, workerId) => {
          let client = workerClients.get(workerId)
          if (!client) {
            client = await createClient(cs)
            workerClients.set(workerId, client)
          }
          try {
            const columns = job.table.columns.filter((c) => !job.table.generatedColumns.includes(c))
            const w = workers[workerId]
            await restoreChunk(
              client,
              job.table.schema,
              job.table.name,
              columns,
              job.outputPath,
              job.chunk.file,
              manifest.compression,
              (bytes) => {
                if (w) w.progressCurrent = bytes
              },
            )
            return { rowCount: 0, bytesWritten: chunkEstimatedBytes(job) }
          } catch (err) {
            destroyClient(client)
            workerClients.delete(workerId)
            throw err
          }
        },
        onWorkerError: (workerId) => {
          workerClients.delete(workerId)
        },
      })

      dashboard?.stop()

      // Release remaining worker clients
      for (const client of workerClients.values()) {
        await client.end().catch(() => {})
      }
      workerClients.clear()

      succeededCount = results.filter((r) => r.status === 'ok').length
      const failed = results.filter((r) => r.status === 'failed')
      failedCount = failed.length
      skippedCount = results.filter((r) => r.status === 'skipped').length

      console.log('')

      if (failed.length > 0) {
        hadDataFailures = true
        printFailedTables(
          failed.map((r) => ({
            label: `${r.job.table.schema}.${r.job.table.name} chunk ${r.job.chunk.index}`,
            error: r.error?.message,
          })),
          opts.retries,
        )
      }
    } else {
      log.info('No table data to restore')
      console.log('')
    }

    // ── Post-data DDL (runs regardless of whether there were data chunks) ──
    if (!opts.dataOnly && !opts.dryRun) {
      log.step('Restoring post-data DDL (indexes, constraints)...')
      if (existsSync(postDataMarker)) {
        log.info('Already restored (resuming)')
      } else if (existsSync(ddlPath)) {
        try {
          const args = [
            '--section=post-data',
            '--no-owner',
            '--no-privileges',
            '--clean',
            '--if-exists',
            '-j',
            String(opts.jobs),
            '-d',
            cs,
            ...opts.pgRestoreArgs,
            ddlPath,
          ]
          execFileSync('pg_restore', args, { stdio: 'pipe' })
        } catch (err: unknown) {
          const stderr = (err as { stderr?: Buffer })?.stderr?.toString() ?? ''
          if (stderr) log.warn(`pg_restore warnings: ${stderr.slice(0, 500)}`)
          else log.warn('pg_restore exited with warnings (this is often normal)')
        }
        await writeFile(postDataMarker, '', 'utf-8')
        log.success('Post-data DDL restored')
      } else {
        log.warn('No DDL file found — skipping post-data')
      }
      console.log('')
    }

    // ── Refresh materialized views ──────────────────────────────────────────
    const matViews = tables.filter((t) => t.relkind === 'm')
    if (matViews.length > 0 && !opts.dataOnly && !opts.dryRun) {
      log.step('Refreshing materialized views...')
      const mvClient = await createClient(cs)
      try {
        for (const mv of matViews) {
          await mvClient.query(`REFRESH MATERIALIZED VIEW ${quoteIdent(mv.schema)}.${quoteIdent(mv.name)}`)
        }
        log.success(`${matViews.length} materialized view(s) refreshed`)
      } catch (err) {
        log.warn(`Failed to refresh materialized views: ${(err as Error).message}`)
      } finally {
        await mvClient.end().catch(() => {})
      }
      console.log('')
    }

    // ── Reset sequences (runs regardless of whether there were data chunks) ─
    if (manifest.sequences.length > 0 && !opts.dataOnly && !opts.dryRun) {
      log.step('Resetting sequences...')
      const seqClient = await createClient(cs)
      let seqOk = 0
      let seqFailed = 0
      try {
        for (const seq of manifest.sequences) {
          if (opts.schema && seq.schema !== opts.schema) continue
          try {
            await seqClient.query(`SELECT setval(($1 || '.' || $2)::regclass, $3, $4)`, [
              quoteIdent(seq.schema),
              quoteIdent(seq.name),
              seq.lastValue,
              seq.isCalled,
            ])
            seqOk++
          } catch (err) {
            seqFailed++
            log.warn(`  Failed to reset ${seq.schema}.${seq.name}: ${(err as Error).message}`)
          }
        }
        if (seqFailed > 0) {
          log.warn(`${seqOk} sequences reset, ${seqFailed} failed`)
        } else {
          log.success(`${seqOk} sequences reset`)
        }
      } finally {
        await seqClient.end()
      }
      console.log('')
    }

    // ── Final summary ──────────────────────────────────────────────────────
    const durationSecs = Math.round((Date.now() - startTime) / 1000)
    printSummary({
      title: 'Restore Summary',
      database: dbName,
      schema: opts.schema,
      tableCount: tables.length,
      succeeded: opts.dryRun ? allJobs.length : succeededCount,
      failed: failedCount,
      skipped: skippedCount,
      durationSecs,
      dryRun: opts.dryRun,
    })

    // Clean up progress tracking after fully successful restore
    if (!opts.dryRun && !hadDataFailures) {
      const cleanupClient = await createClient(cs)
      try {
        await dropProgressTable(cleanupClient)
      } catch {
        /* best-effort */
      } finally {
        await cleanupClient.end().catch(() => {})
      }
    }

    if (hadDataFailures) process.exit(1)
  } finally {
    if (tempDir) {
      await rm(tempDir, { recursive: true, force: true }).catch(() => {})
    }
    signals.cleanup()
    if (signals.wasInterrupted()) process.exit(130)
  }
}
