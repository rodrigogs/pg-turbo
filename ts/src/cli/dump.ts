// ts/src/cli/dump.ts
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

const execFileAsync = promisify(execFile)
import { join, dirname } from 'node:path'
import { mkdir } from 'node:fs/promises'
import { existsSync } from 'node:fs'
import pg from 'pg'

import type {
  DumpOptions, DumpManifest, ManifestTable,
  ChunkJob, ChunkMeta, WorkerState,
} from '../types/index.js'
import {
  cleanConnectionString, sanitizeConnectionString, extractDbName,
  createClient, createSnapshotCoordinator, createWorkerClient,
  releaseWorkerClient, destroyClient,
} from '../core/connection.js'
import {
  buildTableDiscoveryQuery, parseTableRows,
  buildBatchColumnsQuery,
  buildDdlDumpArgs, buildSequenceQuery, parseSequenceRows,
  quoteIdent,
} from '../core/schema.js'
import { planChunks, buildCopyQuery, chunkStrategy } from '../core/chunker.js'
import type { ChunkPlanOptions } from '../core/chunker.js'
import { dumpChunk, chunkDoneMarker, removePartialChunk } from '../core/copy-stream.js'
import { runWorkerPool } from '../core/queue.js'
import { writeManifest } from '../core/manifest.js'
import { humanSize } from '../core/format.js'
import {
  log, printBanner, startDashboard, printSummary, printFailedTables,
  createProgressHandler, installSignalHandlers,
} from './ui.js'
import type { DashboardState } from './ui.js'

const { Client } = pg

function parsePgMajorVersion(versionString: string): number {
  const match = versionString.match(/PostgreSQL (\d+)/)
  return match ? parseInt(match[1]!, 10) : 14
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
    log.info(`Retries    : ${opts.retries} (delay: ${opts.retryDelay}s)`)
  }
  if (opts.pgDumpArgs.length > 0) {
    log.info(`pg_dump args: ${opts.pgDumpArgs.join(' ')}`)
  }
  console.log('')

  // ── Step 1: Test connection ─────────────────────────────────────────────
  log.step('Step 1/6: Testing connection...')
  const discoveryClient = await createClient(cs)

  let pgVersion: string
  let replica: boolean
  try {
    const [versionResult, replicaResult] = await Promise.all([
      discoveryClient.query('SELECT version() AS version'),
      discoveryClient.query('SELECT pg_is_in_recovery() AS is_replica'),
    ])
    pgVersion = versionResult.rows[0].version as string
    replica = replicaResult.rows[0].is_replica as boolean
  } catch (err) {
    await discoveryClient.end().catch(() => {})
    throw err
  }
  const pgMajor = parsePgMajorVersion(pgVersion)
  log.success(`Connected — ${pgVersion.split(',')[0]}`)
  console.log('')

  // ── Step 2: Detect replica & export snapshot ────────────────────────────
  log.step('Step 2/6: Snapshot setup...')
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
  log.step('Step 3/6: Discovering tables...')

  try {
    // Validate schema exists if filter specified
    if (opts.schema) {
      const { rows: schemaCheck } = await discoveryClient.query(
        `SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1`, [opts.schema]
      )
      if (schemaCheck.length === 0) {
        const { rows: availableSchemas } = await discoveryClient.query(
          `SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema' ORDER BY 1`
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
      const oids = tables.map(t => t.oid)
      if (oids.length > 0) {
        const { rows: allCols } = await discoveryClient.query(
          buildBatchColumnsQuery(), [oids]
        )
        // Group by oid
        for (const table of tables) {
          const tableCols = (allCols as Array<{ oid: number; attname: string; is_generated: boolean }>)
            .filter(r => r.oid === table.oid)
          table.columns = tableCols.filter(r => !r.is_generated).map(r => r.attname)
          table.generatedColumns = tableCols.filter(r => r.is_generated).map(r => r.attname)
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
    log.step('Step 4/6: Planning chunks...')

    // Pipeline all min/max PK queries concurrently on the discovery connection
    const minMaxResults = await Promise.all(
      tables.filter(t => t.pkColumn).map(async (table) => {
        const { rows } = await discoveryClient.query(
          `SELECT min(${quoteIdent(table.pkColumn!)}) AS mn, max(${quoteIdent(table.pkColumn!)}) AS mx
           FROM ${quoteIdent(table.schemaName)}.${quoteIdent(table.tableName)}`
        )
        return { oid: table.oid, rows }
      })
    )
    const minMaxMap = new Map<number, { pkMin: number | null; pkMax: number | null }>()
    for (const { oid, rows } of minMaxResults) {
      if (rows.length > 0 && rows[0]!.mn !== null) {
        minMaxMap.set(oid, { pkMin: parseInt(String(rows[0]!.mn), 10), pkMax: parseInt(String(rows[0]!.mx), 10) })
      }
    }

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
        pkMin, pkMax,
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

    log.success(`${allJobs.length} chunks across ${tables.length} tables (strategy mix: ${[...new Set(manifestTables.map(t => t.chunkStrategy))].join(', ')})`)
    console.log('')

    // Pre-create all output directories
    if (!opts.dryRun && allJobs.length > 0) {
      const dirs = [...new Set(allJobs.map(j => dirname(j.outputPath)))]
      await Promise.all(dirs.map(d => mkdir(d, { recursive: true })))
    }

    // ── Step 5: Dump DDL ──────────────────────────────────────────────────
    const ddlPath = join(opts.output, '_schema_ddl.dump')
    log.step('Step 5/6: Dumping DDL...')

    let ddlPromise: Promise<void> | null = null
    if (opts.dryRun) {
      log.warn('Skipped (dry run)')
    } else {
      await mkdir(opts.output, { recursive: true })
      const ddlArgs = buildDdlDumpArgs(cs, ddlPath, opts.schema, snapshotId, opts.pgDumpArgs)
      // Start DDL dump in background — overlaps with data dump
      ddlPromise = execFileAsync('pg_dump', ddlArgs)
        .then(() => { log.success(`DDL saved -> ${ddlPath}`) })
        .catch((err: unknown) => {
          const stderr = (err as { stderr?: string })?.stderr ?? ''
          if (stderr) log.warn(`pg_dump warnings: ${stderr.slice(0, 500)}`)
          throw err
        })
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
    log.step('Step 6/6: Dumping table data...')

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
        isResumable: (job) => existsSync(chunkDoneMarker(job.outputPath)),
        onProgress: createProgressHandler(workers, dashState, dashboard,
          (event) => event.bytesWritten ?? 0),
        task: async (job, workerId) => {
          let client = workerClients.get(workerId)
          if (!client) {
            client = await createWorkerClient(cs, snapshotId)
            workerClients.set(workerId, client)
          }
          try {
            return await dumpChunk(client, job.copyQuery!, job.outputPath)
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

      dashboard?.stop()

      // Release remaining worker clients
      for (const client of workerClients.values()) {
        await releaseWorkerClient(client)
      }
      workerClients.clear()

      const succeeded = results.filter(r => r.status === 'ok').length
      const failed = results.filter(r => r.status === 'failed')
      const skipped = results.filter(r => r.status === 'skipped').length

      console.log('')

      // Wait for DDL dump to finish
      if (ddlPromise) {
        try {
          await ddlPromise
        } catch (err) {
          log.error(`DDL dump failed: ${(err as Error).message}`)
          // Still write manifest and summary for data that was dumped
        }
      }

      // ── Write manifest ──────────────────────────────────────────────────
      const manifest: DumpManifest = {
        version: 1,
        tool: 'pg-resilient',
        createdAt: new Date().toISOString(),
        pgVersion,
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
      await writeManifest(opts.output, manifest)
      log.success('Manifest written')

      // ── Summary ─────────────────────────────────────────────────────────
      const durationSecs = Math.round((Date.now() - startTime) / 1000)
      printSummary({
        title: 'Dump Summary',
        database: dbName,
        schema: opts.schema,
        tableCount: tables.length,
        succeeded,
        failed: failed.length,
        skipped,
        durationSecs,
        outputDir: opts.output,
        dryRun: false,
      })

      if (failed.length > 0) {
        printFailedTables(
          failed.map(r => `${r.job.table.schema}.${r.job.table.name} chunk ${r.job.chunk.index}`),
          opts.retries,
        )
        // Close snapshot coordinator before exit
        if (snapshotCoordinator) await snapshotCoordinator.close()
        process.exit(1)
      }
    } else {
      log.info('No table data to dump')
    }

    // Dry run summary
    if (opts.dryRun) {
      console.log('')
      const durationSecs = Math.round((Date.now() - startTime) / 1000)
      printSummary({
        title: 'Dump Summary',
        database: dbName,
        schema: opts.schema,
        tableCount: tables.length,
        succeeded: allJobs.length,
        failed: 0,
        skipped: 0,
        durationSecs,
        outputDir: opts.output,
        dryRun: true,
      })
    }
  } finally {
    await discoveryClient.end().catch(() => {})
    if (snapshotCoordinator) await snapshotCoordinator.close()
    signals.cleanup()
    if (signals.wasInterrupted()) process.exit(130)
  }
}
