// ts/src/cli/dump.ts
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

const execFileAsync = promisify(execFile)

import { existsSync } from 'node:fs'
import { mkdir } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import logUpdate from 'log-update'
import pg from 'pg'
import { createArchive } from '../core/archive.js'
import type { ChunkPlanOptions, RowSample } from '../core/chunker.js'
import { buildCopyQuery, chunkEstimatedBytes, chunkEstimatedRows, chunkStrategy, planChunks } from '../core/chunker.js'
import {
  cleanConnectionString,
  createClient,
  createSnapshotCoordinator,
  createWorkerClient,
  destroyClient,
  extractDbName,
  releaseWorkerClient,
  sanitizeConnectionString,
} from '../core/connection.js'
import { chunkDoneMarker, dumpChunk, removePartialChunk } from '../core/copy-stream.js'
import { humanSize, progressBar } from '../core/format.js'
import { writeManifest } from '../core/manifest.js'
import { runWorkerPool } from '../core/queue.js'
import {
  buildBatchColumnsQuery,
  buildDdlDumpArgs,
  buildSequenceQuery,
  buildTableDiscoveryQuery,
  buildVolumeSampleQuery,
  parseSequenceRows,
  parseTableRows,
  quoteIdent,
} from '../core/schema.js'
import type { ChunkJob, ChunkResult, DumpManifest, DumpOptions, ManifestTable, WorkerState } from '../types/index.js'
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

function parsePgMajorVersion(versionString: string): number {
  const match = versionString.match(/PostgreSQL (\d+)/)
  return match ? parseInt(match[1] ?? '14', 10) : 14
}

export async function runDump(opts: DumpOptions): Promise<void> {
  const cs = cleanConnectionString(opts.dbname)
  const dbName = extractDbName(cs)
  const startTime = Date.now()

  let dashboard: ReturnType<typeof startDashboard> | null = null
  const signals = installSignalHandlers(() => dashboard)

  // ── Banner ──────────────────────────────────────────────────────────────
  printBanner(opts.dryRun ? 'PostgreSQL Resilient Dump (DRY RUN)' : 'PostgreSQL Resilient Dump')
  log.info(`Connection : ${sanitizeConnectionString(cs)}`)
  log.info(`Database   : ${dbName}`)
  log.info(`Schema     : ${opts.schema ?? 'all user schemas'}`)
  log.info(`Output     : ${opts.output}`)
  if (!opts.dryRun) {
    log.info(`Workers    : ${opts.jobs}`)
    log.info(`Compression: ${opts.compression}`)
    log.info(`Retries    : ${opts.retries} (delay: ${opts.retryDelay}s)`)
  }
  if (opts.pgDumpArgs.length > 0) {
    log.info(`pg_dump args: ${opts.pgDumpArgs.join(' ')}`)
  }
  console.log('')

  // ── Step 1: Test connection ─────────────────────────────────────────────
  log.step('Step 1/7: Testing connection...')
  const discoveryClient = await createClient(cs)

  let pgVersion: string
  let replica: boolean
  try {
    const { rows } = await discoveryClient.query('SELECT version() AS version, pg_is_in_recovery() AS is_replica')
    pgVersion = rows[0].version as string
    replica = rows[0].is_replica as boolean
  } catch (err) {
    await discoveryClient.end().catch(() => {})
    throw err
  }
  const pgMajor = parsePgMajorVersion(pgVersion)
  log.success(`Connected — ${pgVersion.split(',')[0]}`)
  console.log('')

  // ── Step 2: Detect replica & export snapshot ────────────────────────────
  log.step('Step 2/7: Snapshot setup...')
  let snapshotId: string | null = null
  let snapshotCoordinator: Awaited<ReturnType<typeof createSnapshotCoordinator>> | null = null

  if (opts.dryRun) {
    log.info('Skipped (dry run)')
  } else if (replica) {
    log.warn('Read replica detected — skipping snapshot')
  } else if (opts.noSnapshot) {
    log.warn('Snapshot disabled via --no-snapshot')
  } else {
    snapshotCoordinator = await createSnapshotCoordinator(cs)
    snapshotId = snapshotCoordinator.snapshotId
    log.success(`Snapshot exported: ${snapshotId}`)
  }
  console.log('')

  // ── Step 3: Discover tables ─────────────────────────────────────────────
  log.step('Step 3/7: Discovering tables...')

  try {
    // Validate schema exists if filter specified
    if (opts.schema) {
      const { rows: schemaCheck } = await discoveryClient.query(
        `SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1`,
        [opts.schema],
      )
      if (schemaCheck.length === 0) {
        const { rows: availableSchemas } = await discoveryClient.query(
          `SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema' ORDER BY 1`,
        )
        log.error(`Schema '${opts.schema}' does not exist in database '${dbName}'.`)
        log.info('Available schemas:')
        for (const s of availableSchemas) {
          console.log(`    - ${(s as { nspname: string }).nspname}`)
        }
        throw new Error(`Schema '${opts.schema}' not found`)
      }
    }

    const tableQuery = buildTableDiscoveryQuery(opts.schema)
    const { rows: tableRows } = await discoveryClient.query(tableQuery.text, tableQuery.values)
    const tables = parseTableRows(tableRows as Parameters<typeof parseTableRows>[0])

    if (tables.length === 0) {
      log.warn('No tables found. Only DDL will be dumped.')
    } else {
      // Batch fetch all columns for all tables in one query
      const oids = tables.map((t) => t.oid)
      if (oids.length > 0) {
        const { rows: allCols } = await discoveryClient.query(buildBatchColumnsQuery(), [oids])
        // Group by oid
        for (const table of tables) {
          const tableCols = (allCols as Array<{ oid: number; attname: string; is_generated: boolean }>).filter(
            (r) => r.oid === table.oid,
          )
          table.columns = tableCols.filter((r) => !r.is_generated).map((r) => r.attname)
          table.generatedColumns = tableCols.filter((r) => r.is_generated).map((r) => r.attname)
        }
      }
      log.success(`Found ${tables.length} tables (${humanSize(tables.reduce((s, t) => s + t.actualBytes, 0))})`)

      // Show top 5 largest
      const top = tables.slice(0, 5)
      for (const t of top) {
        log.info(`  ${t.schemaName}.${t.tableName}  ${humanSize(t.actualBytes)}`)
      }
    }
    console.log('')

    // ── Step 4: Plan chunks ───────────────────────────────────────────────
    log.step('Step 4/7: Planning chunks...')

    const minMaxMap = new Map<number, { pkMin: number | null; pkMax: number | null }>()
    const volumeSampleMap = new Map<number, RowSample[]>()
    const pkTables = tables.filter((t) => t.pkColumn)
    for (let ti = 0; ti < pkTables.length; ti++) {
      const table = pkTables[ti]
      if (!table) continue
      const pk = table.pkColumn
      if (!pk) continue
      const needsSample = table.actualBytes >= opts.splitThreshold
      logUpdate(
        `  ${progressBar(ti, pkTables.length, 20)} ${table.schemaName}.${table.tableName}${needsSample ? ' (sampling)' : ''}`,
      )

      const { rows } = await discoveryClient.query(
        `SELECT min(${quoteIdent(pk)}) AS mn, max(${quoteIdent(pk)}) AS mx
         FROM ${quoteIdent(table.schemaName)}.${quoteIdent(table.tableName)}`,
      )
      const firstRow = rows[0]
      if (rows.length > 0 && firstRow?.mn !== null) {
        minMaxMap.set(table.oid, { pkMin: parseInt(String(firstRow.mn), 10), pkMax: parseInt(String(firstRow.mx), 10) })
      }

      if (needsSample) {
        const minMax = minMaxMap.get(table.oid)
        if (minMax?.pkMin != null && minMax.pkMax != null) {
          const sampleQuery = buildVolumeSampleQuery(table.schemaName, table.tableName, pk, minMax.pkMin, minMax.pkMax)
          const { rows: sampleRows } = await discoveryClient.query(sampleQuery)
          const allSamples: RowSample[] = []
          for (const r of sampleRows as Array<{ pk: string; bytes: string }>) {
            const pk = parseInt(r.pk, 10)
            // Deduplicate: PK gaps cause multiple probes to hit the same row
            if (allSamples.length === 0 || pk !== allSamples[allSamples.length - 1]?.pk) {
              allSamples.push({ pk, bytes: parseInt(r.bytes, 10) })
            }
          }
          if (allSamples.length > 0) volumeSampleMap.set(table.oid, allSamples)
        }
      }

      logUpdate(`  ${progressBar(ti + 1, pkTables.length, 20)}`)
    }
    logUpdate.done()

    const manifestTables: ManifestTable[] = []
    const allJobs: ChunkJob[] = []

    for (const table of tables) {
      const minMax = minMaxMap.get(table.oid)
      const pkMin = minMax?.pkMin ?? null
      const pkMax = minMax?.pkMax ?? null

      const planOpts: ChunkPlanOptions = {
        splitThreshold: opts.splitThreshold,
        maxChunks: opts.maxChunksPerTable,
        pgMajorVersion: pgMajor,
        pkMin,
        pkMax,
        compression: opts.compression,
        volumeSamples: volumeSampleMap.get(table.oid),
      }

      const chunks = planChunks(table, planOpts)
      const strategy = chunkStrategy(table, planOpts)

      const mt: ManifestTable = {
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
      manifestTables.push(mt)

      for (const chunk of chunks) {
        const copyQuery = buildCopyQuery(table, chunk)
        const outputPath = join(opts.output, chunk.file)
        allJobs.push({ table: mt, chunk, copyQuery, outputPath, attempt: 0 })
      }
    }

    log.success(
      `${allJobs.length} chunks across ${tables.length} tables (strategy mix: ${[...new Set(manifestTables.map((t) => t.chunkStrategy))].join(', ')})`,
    )
    console.log('')

    // Pre-create all output directories
    if (!opts.dryRun && allJobs.length > 0) {
      const dirs = [...new Set(allJobs.map((j) => dirname(j.outputPath)))]
      await Promise.all(dirs.map((d) => mkdir(d, { recursive: true })))
    }

    // ── Step 5: Dump DDL ──────────────────────────────────────────────────
    const ddlPath = join(opts.output, '_schema_ddl.dump')
    log.step('Step 5/7: Dumping DDL...')

    let ddlPromise: Promise<{ stdout: string; stderr: string }> | null = null
    if (opts.dryRun) {
      log.warn('Skipped (dry run)')
    } else {
      await mkdir(opts.output, { recursive: true })
      const ddlArgs = buildDdlDumpArgs(cs, ddlPath, opts.schema, snapshotId, opts.pgDumpArgs)
      // Start DDL dump in background — overlaps with data dump.
      // Don't log here — console.log during dashboard breaks logUpdate.
      ddlPromise = execFileAsync('pg_dump', ddlArgs)
    }
    console.log('')

    // ── Fetch sequences ───────────────────────────────────────────────────
    const seqQuery = buildSequenceQuery(opts.schema)
    const { rows: seqRows } = await discoveryClient.query(seqQuery.text, seqQuery.values)
    const sequences = parseSequenceRows(seqRows as Parameters<typeof parseSequenceRows>[0])
    if (sequences.length > 0) {
      log.info(`Found ${sequences.length} sequences`)
    }

    // ── Step 6: Dump table data ───────────────────────────────────────────
    log.step('Step 6/7: Dumping table data...')

    let succeeded = 0
    let failed: ChunkResult[] = []
    let skipped = 0

    if (opts.dryRun) {
      log.warn('Skipped (dry run)')
      console.log('')
      log.info('Chunks that would be dumped:')
      for (const job of allJobs) {
        log.info(`  ${job.table.schema}.${job.table.name} chunk ${job.chunk.index}`)
      }
    } else if (allJobs.length > 0) {
      const totalBytes = manifestTables.reduce((s, t) => s + t.estimatedBytes, 0)
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
        progressUnit: 'rows',
        speedSamples: [],
      }
      dashboard = startDashboard(dashState)

      // Track worker clients for cleanup.
      // activeSnapshotId degrades to null if the snapshot coordinator dies,
      // allowing workers to reconnect without snapshot consistency.
      const workerClients = new Map<number, InstanceType<typeof Client>>()
      let activeSnapshotId = snapshotId
      let snapshotLost = false

      const results = await runWorkerPool({
        jobs: allJobs,
        workerCount: opts.jobs,
        maxRetries: opts.retries,
        retryDelayMs: opts.retryDelay * 1000,
        isResumable: (job) => existsSync(chunkDoneMarker(job.outputPath)),
        onProgress: createProgressHandler(
          workers,
          dashState,
          dashboard,
          (event) => chunkEstimatedBytes(event.job),
          (job) => chunkEstimatedRows(job),
        ),
        task: async (job, workerId) => {
          let client = workerClients.get(workerId)
          if (!client) {
            try {
              client = await createWorkerClient(cs, activeSnapshotId)
            } catch {
              // Snapshot may be invalid after coordinator disconnect — fall back
              if (activeSnapshotId) {
                activeSnapshotId = null
                snapshotLost = true
              }
              client = await createWorkerClient(cs, null)
            }
            workerClients.set(workerId, client)
          }
          try {
            const w = workers[workerId]
            return await dumpChunk(client, job.copyQuery ?? '', job.outputPath, opts.compression, (rows) => {
              if (w) w.progressCurrent = rows
            })
          } catch (err) {
            // On error, destroy this worker's client so it gets recreated
            await destroyClient(client)
            workerClients.delete(workerId)
            await removePartialChunk(job.outputPath)
            throw err
          }
        },
        onWorkerError: (workerId) => {
          // Client already destroyed in task error handler
          workerClients.delete(workerId)
        },
      })

      if (snapshotLost) {
        log.warn('Snapshot was lost during dump — some chunks may reflect newer data')
      }

      dashboard?.stop()

      // Release remaining worker clients
      for (const client of workerClients.values()) {
        await releaseWorkerClient(client)
      }
      workerClients.clear()

      succeeded = results.filter((r) => r.status === 'ok').length
      failed = results.filter((r) => r.status === 'failed')
      skipped = results.filter((r) => r.status === 'skipped').length

      console.log('')
    } else {
      log.info('No table data to dump')
    }

    // Wait for DDL dump to finish (logging deferred until after dashboard stops)
    if (ddlPromise) {
      try {
        await ddlPromise
        log.success(`DDL saved -> ${ddlPath}`)
      } catch (err) {
        const stderr = (err as { stderr?: string })?.stderr ?? ''
        if (stderr) log.warn(`pg_dump warnings: ${stderr.slice(0, 500)}`)
        log.error(`DDL dump failed: ${(err as Error).message}`)
      }
    }

    // ── Write manifest & summary ────────────────────────────────────────
    if (!opts.dryRun) {
      const manifest: DumpManifest = {
        version: 1,
        tool: 'pg-resilient',
        createdAt: new Date().toISOString(),
        pgVersion,
        database: dbName,
        snapshotId,
        compression: opts.compression,
        options: {
          schemaFilter: opts.schema ?? null,
          splitThresholdBytes: opts.splitThreshold,
          jobs: opts.jobs,
        },
        tables: manifestTables,
        sequences,
      }
      await writeManifest(opts.output, manifest)
      log.success('Manifest written')
    }

    // ── Step 7: Package archive ──────────────────────────────────────────
    if (!opts.dryRun && !opts.noArchive && failed.length === 0) {
      const archivePath = `${opts.output}.pgr`
      log.step('Step 7/7: Packaging archive...')
      const archiveSize = await createArchive(opts.output, archivePath)
      log.success(`Archive created: ${archivePath} (${humanSize(archiveSize)})`)
      console.log('')
    }

    const durationSecs = Math.round((Date.now() - startTime) / 1000)
    printSummary({
      title: 'Dump Summary',
      database: dbName,
      schema: opts.schema,
      tableCount: tables.length,
      succeeded: opts.dryRun ? allJobs.length : succeeded,
      failed: failed.length,
      skipped,
      durationSecs,
      outputDir: opts.output,
      dryRun: opts.dryRun,
    })

    if (failed.length > 0) {
      printFailedTables(
        failed.map((r) => ({
          label: `${r.job.table.schema}.${r.job.table.name} chunk ${r.job.chunk.index}`,
          error: r.error?.message,
        })),
        opts.retries,
      )
      if (snapshotCoordinator) await snapshotCoordinator.close()
      process.exit(1)
    }
  } finally {
    await discoveryClient.end().catch(() => {})
    if (snapshotCoordinator) await snapshotCoordinator.close()
    signals.cleanup()
    if (signals.wasInterrupted()) process.exit(130)
  }
}
