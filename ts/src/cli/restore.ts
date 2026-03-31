// ts/src/cli/restore.ts
import { execSync } from 'node:child_process'
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
import {
  log, printBanner, startDashboard, printSummary, printFailedTables,
} from './ui.js'
import type { DashboardState } from './ui.js'

const { Client } = pg

export async function runRestore(opts: RestoreOptions): Promise<void> {
  const cs = cleanConnectionString(opts.dbname)
  const dbName = extractDbName(cs)
  const startTime = Date.now()

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
  const preDataMarker = join(opts.input, '_pre_data.done')
  const postDataMarker = join(opts.input, '_post_data.done')

  // ── Step 3: Clean (if requested) ────────────────────────────────────────
  if (opts.clean && !opts.dataOnly) {
    log.step('Cleaning schemas...')
    if (opts.dryRun) {
      log.warn('Skipped (dry run)')
    } else {
      const client = new Client({ connectionString: appendKeepaliveParams(cs) })
      await client.connect()
      try {
        const schemas = [...new Set(tables.map(t => t.schema))]
        for (const schema of schemas) {
          log.info(`  DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
          await client.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
          log.info(`  CREATE SCHEMA "${schema}"`)
          await client.query(`CREATE SCHEMA "${schema}"`)
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
        execSync(`pg_restore ${args.map(a => `'${a}'`).join(' ')}`, { stdio: 'pipe' })
      } catch {
        // pg_restore returns non-zero for warnings (e.g., "relation already exists")
        // This is expected behavior when resuming or restoring to non-empty DB
        log.warn('pg_restore exited with warnings (this is often normal)')
      }
      await writeFile(preDataMarker, '', 'utf-8')
      log.success('Pre-data DDL restored')
    } else {
      log.warn('No DDL file found — skipping pre-data')
    }
    console.log('')
  }

  // ── Step 5: Restore table data via COPY ─────────────────────────────────
  log.step('Restoring table data...')

  // Build chunk jobs from manifest tables
  const allJobs: ChunkJob[] = []
  for (const table of tables) {
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
      const markerExists = existsSync(chunkRestoredMarker(job.outputPath))
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
    const dashboard = startDashboard(dashState)

    // Track worker clients for cleanup
    const workerClients = new Map<number, InstanceType<typeof Client>>()

    const results = await runWorkerPool({
      jobs: allJobs,
      workerCount: opts.jobs,
      maxRetries: opts.retries,
      isResumable: (job) => existsSync(chunkRestoredMarker(job.outputPath)),
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
        dashboard.update()
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
          await restoreChunk(client, job.table.schema, job.table.name, columns, job.outputPath)
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

    dashboard.stop()

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
      printFailedTables(
        failed.map(r => `${r.job.table.schema}.${r.job.table.name} chunk ${r.job.chunk.index}`),
        opts.retries,
      )
    }

    // ── Step 6: Post-data DDL ─────────────────────────────────────────────
    if (!opts.dataOnly) {
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
          execSync(`pg_restore ${args.map(a => `'${a}'`).join(' ')}`, { stdio: 'pipe' })
        } catch {
          log.warn('pg_restore exited with warnings (this is often normal)')
        }
        await writeFile(postDataMarker, '', 'utf-8')
        log.success('Post-data DDL restored')
      } else {
        log.warn('No DDL file found — skipping post-data')
      }
      console.log('')
    }

    // ── Step 7: Reset sequences ───────────────────────────────────────────
    if (manifest.sequences.length > 0 && !opts.dataOnly) {
      log.step('Resetting sequences...')
      const seqClient = new Client({ connectionString: appendKeepaliveParams(cs) })
      await seqClient.connect()
      try {
        for (const seq of manifest.sequences) {
          // Respect schema/table filters
          if (opts.schema && seq.schema !== opts.schema) continue
          await seqClient.query(
            `SELECT setval('"${seq.schema}"."${seq.name}"', $1, $2)`,
            [seq.lastValue, seq.isCalled],
          )
        }
        log.success(`${manifest.sequences.length} sequences reset`)
      } finally {
        await seqClient.end()
      }
      console.log('')
    }

    // ── Summary ───────────────────────────────────────────────────────────
    const durationSecs = Math.round((Date.now() - startTime) / 1000)
    printSummary({
      title: 'Restore Summary',
      database: dbName,
      schema: opts.schema,
      tableCount: tables.length,
      succeeded,
      failed: failed.length,
      skipped,
      durationSecs,
      dryRun: false,
    })

    if (failed.length > 0) {
      process.exit(1)
    }
  } else {
    log.info('No table data to restore')
    console.log('')
  }

  // Dry run summary
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
}
