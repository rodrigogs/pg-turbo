// ts/src/cli/restore.ts
import { execFileSync } from 'node:child_process'
import { join } from 'node:path'
import { writeFile } from 'node:fs/promises'
import { existsSync } from 'node:fs'
import pg from 'pg'

import type {
  RestoreOptions, ChunkJob, ManifestTable,
  ProgressEvent, WorkerState,
} from '../types/index.js'
import {
  cleanConnectionString, sanitizeConnectionString, extractDbName,
  testConnection, destroyClient,
} from '../core/connection.js'
import { appendKeepaliveParams } from '../core/connection.js'
import { readManifest } from '../core/manifest.js'
import { restoreChunk, chunkRestoredMarker } from '../core/copy-stream.js'
import { runWorkerPool } from '../core/queue.js'
import { humanSize } from '../core/format.js'
import { quoteIdent } from '../core/schema.js'
import {
  log, printBanner, startDashboard, printSummary, printFailedTables,
} from './ui.js'
import type { DashboardState } from './ui.js'

const { Client } = pg

export async function runRestore(opts: RestoreOptions): Promise<void> {
  const cs = cleanConnectionString(opts.dbname)
  const dbName = extractDbName(cs)
  const startTime = Date.now()

  let dashboard: ReturnType<typeof startDashboard> | null = null
  let interrupted = false
  const cleanup = () => {
    interrupted = true
    dashboard?.stop()
    console.log('')
    log.warn('Interrupted — cleaning up...')
  }
  process.once('SIGINT', cleanup)
  process.once('SIGTERM', cleanup)

  // ── Step 1: Read manifest ───────────────────────────────────────────────
  log.step('Reading manifest...')
  const manifest = await readManifest(opts.input)
  log.success(`Manifest loaded: ${manifest.tables.length} tables, ${manifest.sequences.length} sequences`)
  console.log('')

  // ── Banner ──────────────────────────────────────────────────────────────
  printBanner(opts.dryRun ? 'PostgreSQL Resilient Restore (DRY RUN)' : 'PostgreSQL Resilient Restore')
  log.info(`Connection : ${sanitizeConnectionString(cs)}`)
  log.info(`Database   : ${dbName}`)
  log.info(`Source DB  : ${manifest.database}`)
  if (opts.schema) log.info(`Schema     : ${opts.schema}`)
  if (opts.table) log.info(`Table      : ${opts.table}`)
  log.info(`Input      : ${opts.input}`)
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

  // ── Step 2: Test connection ─────────────────────────────────────────────
  log.step('Testing connection...')
  const pgVersion = await testConnection(cs)
  log.success(`Connected — ${pgVersion.split(',')[0]}`)
  console.log('')

  // ── Filter tables ───────────────────────────────────────────────────────
  let tables: ManifestTable[] = manifest.tables
  if (opts.schema) {
    tables = tables.filter(t => t.schema === opts.schema)
  }
  if (opts.table) {
    tables = tables.filter(t => {
      // Support both "table" and "schema.table" format
      if (opts.table!.includes('.')) {
        const [s, n] = opts.table!.split('.')
        return t.schema === s && t.name === n
      }
      return t.name === opts.table
    })
  }
  log.info(`Restoring ${tables.length} tables (${humanSize(tables.reduce((s, t) => s + t.estimatedBytes, 0))})`)
  console.log('')

  const ddlPath = join(opts.input, '_schema_ddl.dump')
  // Scope resume markers by target database to avoid cross-target pollution
  const preDataMarker = join(opts.input, `_pre_data.${dbName}.done`)
  const postDataMarker = join(opts.input, `_post_data.${dbName}.done`)

  // ── Step 3: Clean (if requested) ────────────────────────────────────────
  if (opts.clean && !opts.dataOnly) {
    log.step('Cleaning...')
    if (opts.dryRun) {
      log.warn('Skipped (dry run)')
    } else if (opts.table) {
      // When filtering to a single table, only truncate that table — don't drop the entire schema
      const client = new Client({ connectionString: appendKeepaliveParams(cs) })
      await client.connect()
      try {
        for (const t of tables) {
          if (t.relkind === 'm') continue
          const qt = `${quoteIdent(t.schema)}.${quoteIdent(t.name)}`
          log.info(`  TRUNCATE ${qt}`)
          await client.query(`TRUNCATE ${qt} CASCADE`).catch(() => {})
        }
        log.success('Tables truncated')
      } finally {
        await client.end()
      }
    } else {
      const client = new Client({ connectionString: appendKeepaliveParams(cs) })
      await client.connect()
      try {
        const schemas = [...new Set(tables.map(t => t.schema))]
        for (const schema of schemas) {
          log.info(`  DROP SCHEMA IF EXISTS ${quoteIdent(schema)} CASCADE`)
          await client.query(`DROP SCHEMA IF EXISTS ${quoteIdent(schema)} CASCADE`)
          log.info(`  CREATE SCHEMA ${quoteIdent(schema)}`)
          await client.query(`CREATE SCHEMA ${quoteIdent(schema)}`)
        }
        log.success('Schemas cleaned')
      } finally {
        await client.end()
      }
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
        const args = [
          '--section=pre-data', '--no-owner', '--no-privileges',
          '-d', cs,
          ...opts.pgRestoreArgs,
          ddlPath,
        ]
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
  log.step('Restoring table data...')

  // Build chunk jobs — skip materialized views (restored via REFRESH in post-data DDL)
  const allJobs: ChunkJob[] = []
  for (const table of tables) {
    if (table.relkind === 'm') {
      log.info(`Skipping materialized view ${table.schema}.${table.name} (refreshed via post-data DDL)`)
      continue
    }
    for (const chunk of table.chunks) {
      const inputPath = join(opts.input, chunk.file)
      // copyQuery is not used for restore, but we keep it for the ChunkJob type
      allJobs.push({
        table,
        chunk,
        copyQuery: '',
        outputPath: inputPath,
        attempt: 0,
      })
    }
  }

  if (opts.dryRun) {
    log.warn('Skipped (dry run)')
    console.log('')
    log.info('Chunks that would be restored:')
    for (const job of allJobs) {
      const markerExists = existsSync(chunkRestoredMarker(job.outputPath, dbName))
      const status = markerExists ? '(already restored)' : ''
      log.info(`  ${job.table.schema}.${job.table.name} chunk ${job.chunk.index} ${status}`)
    }
  } else if (allJobs.length > 0) {
    const totalBytes = tables.reduce((s, t) => s + t.estimatedBytes, 0)
    const workers: WorkerState[] = Array.from({ length: opts.jobs }, (_, i) => ({
      id: i, status: 'idle' as const,
    }))
    const dashState: DashboardState = {
      totalBytes, processedBytes: 0, startTime: Date.now(), workers,
    }
    dashboard = startDashboard(dashState)

    // Track worker clients for cleanup
    const workerClients = new Map<number, InstanceType<typeof Client>>()

    const results = await runWorkerPool({
      jobs: allJobs,
      workerCount: opts.jobs,
      maxRetries: opts.retries,
      retryDelayMs: opts.retryDelay * 1000,
      isResumable: (job) => existsSync(chunkRestoredMarker(job.outputPath, dbName)),
      onProgress: (event: ProgressEvent) => {
        const w = workers[event.workerId]
        if (!w) return
        if (event.type === 'started') {
          w.status = 'working'
          w.currentJob = event.job
        } else if (event.type === 'completed') {
          w.status = 'idle'
          w.currentJob = undefined
          // Estimate bytes from table size / chunk count
          dashState.processedBytes += event.job.table.estimatedBytes / event.job.table.chunks.length
        } else if (event.type === 'retrying') {
          w.status = 'retrying'
        } else if (event.type === 'failed' || event.type === 'skipped') {
          w.status = 'idle'
          w.currentJob = undefined
          if (event.type === 'skipped') {
            dashState.processedBytes += event.job.table.estimatedBytes / event.job.table.chunks.length
          }
        }
        dashboard?.update()
      },
      task: async (job, workerId) => {
        let client = workerClients.get(workerId)
        if (!client) {
          client = new Client({ connectionString: appendKeepaliveParams(cs) })
          await client.connect()
          workerClients.set(workerId, client)
        }
        try {
          const columns = job.table.columns.filter(c => !job.table.generatedColumns.includes(c))
          await restoreChunk(client, job.table.schema, job.table.name, columns, job.outputPath, dbName)
          return { rowCount: 0, bytesWritten: job.table.estimatedBytes / job.table.chunks.length }
        } catch (err) {
          await destroyClient(client)
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

    const succeeded = results.filter(r => r.status === 'ok').length
    const failed = results.filter(r => r.status === 'failed')
    const skipped = results.filter(r => r.status === 'skipped').length

    console.log('')

    if (failed.length > 0) {
      hadDataFailures = true
      printFailedTables(
        failed.map(r => `${r.job.table.schema}.${r.job.table.name} chunk ${r.job.chunk.index}`),
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
          '--section=post-data', '--no-owner', '--no-privileges',
          '--clean', '--if-exists',
          '-d', cs,
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
  const matViews = tables.filter(t => t.relkind === 'm')
  if (matViews.length > 0 && !opts.dataOnly && !opts.dryRun) {
    log.step('Refreshing materialized views...')
    const mvClient = new Client({ connectionString: appendKeepaliveParams(cs) })
    await mvClient.connect()
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
    const seqClient = new Client({ connectionString: appendKeepaliveParams(cs) })
    await seqClient.connect()
    let seqOk = 0
    let seqFailed = 0
    try {
      for (const seq of manifest.sequences) {
        if (opts.schema && seq.schema !== opts.schema) continue
        try {
          await seqClient.query(
            `SELECT setval(($1 || '.' || $2)::regclass, $3, $4)`,
            [quoteIdent(seq.schema), quoteIdent(seq.name), seq.lastValue, seq.isCalled],
          )
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
  if (opts.dryRun) {
    console.log('')
    const durationSecs = Math.round((Date.now() - startTime) / 1000)
    printSummary({
      title: 'Restore Summary',
      database: dbName,
      schema: opts.schema,
      tableCount: tables.length,
      succeeded: allJobs.length,
      failed: 0,
      skipped: 0,
      durationSecs,
      dryRun: true,
    })
  }

  process.removeListener('SIGINT', cleanup)
  process.removeListener('SIGTERM', cleanup)
  if (interrupted) process.exit(130)
  if (hadDataFailures) process.exit(1)
}
