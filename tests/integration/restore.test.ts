import { execSync } from 'node:child_process'
import { mkdtempSync, readdirSync, rmSync, statSync, unlinkSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import pg from 'pg'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { runDump } from '../../src/cli/dump.js'
import { runRestore } from '../../src/cli/restore.js'
import type { DumpOptions, RestoreOptions } from '../../src/types/index.js'

const { Client } = pg
const COMPOSE = join(__dirname, 'docker-compose.yml')
const FIXTURES = join(__dirname, 'fixtures.sql')
const SOURCE = 'postgresql://test_admin@localhost:54399/pg_resilient_test'
const ADMIN = 'postgresql://test_admin@localhost:54399/postgres'
const RESTORE = 'postgresql://test_admin@localhost:54399/pg_resilient_restore'

function compose(cmd: string) {
  execSync(`docker-compose -f "${COMPOSE}" ${cmd}`, { stdio: 'pipe', timeout: 60_000 })
}

async function query(sql: string, connStr = SOURCE): Promise<string> {
  const client = new Client({ connectionString: connStr })
  await client.connect()
  const { rows } = await client.query(sql)
  await client.end()
  return rows[0] ? String(Object.values(rows[0])[0]) : ''
}

async function waitForPg(maxMs = 30_000) {
  const deadline = Date.now() + maxMs
  while (Date.now() < deadline) {
    try {
      await query('SELECT 1')
      return
    } catch {
      await new Promise((r) => setTimeout(r, 500))
    }
  }
  throw new Error('PG not ready')
}

async function recreateRestoreDb() {
  const client = new Client({ connectionString: ADMIN })
  await client.connect()
  await client
    .query(
      `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'pg_resilient_restore' AND pid <> pg_backend_pid()`,
    )
    .catch(() => {})
  await client.query('DROP DATABASE IF EXISTS pg_resilient_restore')
  await client.query('CREATE DATABASE pg_resilient_restore')
  await client.end()
}

/** Remove all restore resume markers from a dump directory */
function clearResumeMarkers(dir: string) {
  const removeMarkers = (d: string) => {
    for (const entry of readdirSync(d)) {
      const full = join(d, entry)
      if (statSync(full).isDirectory()) {
        removeMarkers(full)
      } else if (
        entry.endsWith('.restored.done') ||
        entry.includes('_pre_data.') ||
        entry.includes('_post_data.')
      ) {
        unlinkSync(full)
      }
    }
  }
  try {
    removeMarkers(dir)
  } catch {
    /* ignore */
  }
}

function defaultDumpOpts(output: string, overrides: Partial<DumpOptions> = {}): DumpOptions {
  return {
    dbname: SOURCE,
    output,
    jobs: 2,
    splitThreshold: 1_073_741_824,
    maxChunksPerTable: 64,
    retries: 2,
    retryDelay: 1,
    noSnapshot: true,
    noArchive: true,
    dryRun: false,
    compression: 'zstd',
    pgDumpArgs: [],
    ...overrides,
  }
}

function defaultRestoreOpts(
  input: string,
  overrides: Partial<RestoreOptions> = {},
): RestoreOptions {
  return {
    dbname: RESTORE,
    input,
    jobs: 2,
    clean: false,
    dataOnly: false,
    retries: 2,
    retryDelay: 1,
    dryRun: false,
    pgRestoreArgs: [],
    ...overrides,
  }
}

describe('restore integration', () => {
  let dumpDir: string
  let tmpDirs: string[] = []

  function freshTmpDir(): string {
    const dir = mkdtempSync(join(tmpdir(), 'pgr-restore-'))
    tmpDirs.push(dir)
    return dir
  }

  beforeAll(async () => {
    compose('up -d --wait')
    await waitForPg()
    // Load fixtures into source DB
    execSync(`psql "${SOURCE}" -f "${FIXTURES}"`, { stdio: 'pipe', timeout: 30_000 })

    // Dump the source DB once for all restore tests
    dumpDir = freshTmpDir()
    await runDump(defaultDumpOpts(dumpDir))
  }, 120_000)

  afterAll(async () => {
    compose('down -v')
    for (const dir of tmpDirs) {
      rmSync(dir, { recursive: true, force: true })
    }
    tmpDirs = []
  }, 30_000)

  beforeEach(async () => {
    await recreateRestoreDb()
    // Clear resume markers so each test gets a fresh restore
    if (dumpDir) clearResumeMarkers(dumpDir)
  })

  afterEach(async () => {
    // Drop restore DB between tests
    await recreateRestoreDb().catch(() => {})
  })

  it('restores all tables with correct row counts', async () => {
    await runRestore(defaultRestoreOpts(dumpDir))

    // Verify row counts match source
    const sourceUsers = await query('SELECT count(*) FROM public.users')
    const restoreUsers = await query('SELECT count(*) FROM public.users', RESTORE)
    expect(parseInt(restoreUsers, 10)).toBe(parseInt(sourceUsers, 10))

    const sourceLogs = await query('SELECT count(*) FROM public.logs')
    const restoreLogs = await query('SELECT count(*) FROM public.logs', RESTORE)
    expect(parseInt(restoreLogs, 10)).toBe(parseInt(sourceLogs, 10))

    const sourceConfig = await query('SELECT count(*) FROM public.config')
    const restoreConfig = await query('SELECT count(*) FROM public.config', RESTORE)
    expect(parseInt(restoreConfig, 10)).toBe(parseInt(sourceConfig, 10))

    const sourceProducts = await query('SELECT count(*) FROM public.products')
    const restoreProducts = await query('SELECT count(*) FROM public.products', RESTORE)
    expect(parseInt(restoreProducts, 10)).toBe(parseInt(sourceProducts, 10))

    const sourceEvents = await query('SELECT count(*) FROM analytics.events')
    const restoreEvents = await query('SELECT count(*) FROM analytics.events', RESTORE)
    expect(parseInt(restoreEvents, 10)).toBe(parseInt(sourceEvents, 10))
  })

  it('generated column (products.tax) computes correctly after restore', async () => {
    await runRestore(defaultRestoreOpts(dumpDir))

    const mismatchCount = await query(
      'SELECT count(*) FROM public.products WHERE tax <> (price * 0.1)::numeric(10,2)',
      RESTORE,
    )
    expect(parseInt(mismatchCount, 10)).toBe(0)

    const nullCount = await query('SELECT count(*) FROM public.products WHERE tax IS NULL', RESTORE)
    expect(parseInt(nullCount, 10)).toBe(0)
  })

  it('data-only restore works into pre-existing schema', async () => {
    // First do a full restore to set up DDL
    await runRestore(defaultRestoreOpts(dumpDir))

    // Truncate all tables in the restore DB
    await query('TRUNCATE public.users, public.logs, public.config, public.products CASCADE', RESTORE)
    await query('TRUNCATE analytics.events CASCADE', RESTORE)

    // Verify tables are empty
    const count = await query('SELECT count(*) FROM public.users', RESTORE)
    expect(parseInt(count, 10)).toBe(0)

    // Clear restored markers so data-only restore re-processes all chunks
    clearResumeMarkers(dumpDir)

    // Now do a data-only restore
    await runRestore(defaultRestoreOpts(dumpDir, { dataOnly: true }))

    // Verify data was restored
    const sourceUsers = await query('SELECT count(*) FROM public.users')
    const restoreUsers = await query('SELECT count(*) FROM public.users', RESTORE)
    expect(parseInt(restoreUsers, 10)).toBe(parseInt(sourceUsers, 10))
  })

  it('clean mode drops and recreates schema', async () => {
    // First restore
    await runRestore(defaultRestoreOpts(dumpDir))

    // Verify data exists
    const count1 = await query('SELECT count(*) FROM public.users', RESTORE)
    expect(parseInt(count1, 10)).toBeGreaterThan(0)

    // Clear markers for second restore
    clearResumeMarkers(dumpDir)

    // Restore with --clean
    await runRestore(defaultRestoreOpts(dumpDir, { clean: true }))

    // Verify data was restored again (schemas were dropped and recreated)
    const sourceUsers = await query('SELECT count(*) FROM public.users')
    const restoreUsers = await query('SELECT count(*) FROM public.users', RESTORE)
    expect(parseInt(restoreUsers, 10)).toBe(parseInt(sourceUsers, 10))
  })

  it('--table filter restores only a single table', async () => {
    // Full restore first to get DDL
    await runRestore(defaultRestoreOpts(dumpDir))

    // Truncate just the users table
    await query('TRUNCATE public.users CASCADE', RESTORE)
    const emptyCount = await query('SELECT count(*) FROM public.users', RESTORE)
    expect(parseInt(emptyCount, 10)).toBe(0)

    // Clear markers
    clearResumeMarkers(dumpDir)

    // Restore only the users table (data-only since DDL already exists)
    await runRestore(defaultRestoreOpts(dumpDir, { table: 'public.users', dataOnly: true }))

    // Verify users table was restored
    const sourceUsers = await query('SELECT count(*) FROM public.users')
    const restoreUsers = await query('SELECT count(*) FROM public.users', RESTORE)
    expect(parseInt(restoreUsers, 10)).toBe(parseInt(sourceUsers, 10))
  })

  it('resume: second restore with --clean succeeds after first restore', async () => {
    // First restore
    await runRestore(defaultRestoreOpts(dumpDir))

    const count1 = await query('SELECT count(*) FROM public.users', RESTORE)
    expect(parseInt(count1, 10)).toBeGreaterThan(0)

    // Clear markers for second restore
    clearResumeMarkers(dumpDir)

    // Run restore again with --clean -- should drop schemas, re-restore DDL and data
    await runRestore(defaultRestoreOpts(dumpDir, { clean: true }))

    // Data should match source
    const sourceUsers = await query('SELECT count(*) FROM public.users')
    const count2 = await query('SELECT count(*) FROM public.users', RESTORE)
    expect(parseInt(count2, 10)).toBe(parseInt(sourceUsers, 10))
  })
})
