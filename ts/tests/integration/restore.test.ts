import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest'
import { execSync } from 'node:child_process'
import { mkdtempSync, rmSync, unlinkSync, readdirSync, statSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { runDump } from '../../src/cli/dump.js'
import { runRestore } from '../../src/cli/restore.js'
import type { DumpOptions, RestoreOptions } from '../../src/types/index.js'

const COMPOSE_FILE = join(__dirname, 'docker-compose.yml')
const FIXTURES_FILE = join(__dirname, 'fixtures.sql')
const SOURCE_CONN = 'postgresql://test_admin@localhost:54399/pg_resilient_test'
const RESTORE_CONN = 'postgresql://test_admin@localhost:54399/pg_resilient_restore'
const ADMIN_CONN = 'postgresql://test_admin@localhost:54399/postgres'

function compose(cmd: string) {
  execSync(`docker-compose -f "${COMPOSE_FILE}" ${cmd}`, {
    stdio: 'pipe',
    timeout: 60_000,
  })
}

function psql(query: string, connStr: string = SOURCE_CONN): string {
  return execSync(`psql "${connStr}" -t -A -c "${query}"`, {
    encoding: 'utf-8',
    timeout: 30_000,
  }).trim()
}

function waitForPg(maxWaitSecs = 30) {
  const deadline = Date.now() + maxWaitSecs * 1000
  while (Date.now() < deadline) {
    try {
      psql('SELECT 1', ADMIN_CONN)
      return
    } catch {
      execSync('sleep 1')
    }
  }
  throw new Error('PostgreSQL did not become ready')
}

function createRestoreDb() {
  try {
    psql('DROP DATABASE IF EXISTS pg_resilient_restore', ADMIN_CONN)
  } catch { /* ignore */ }
  psql('CREATE DATABASE pg_resilient_restore', ADMIN_CONN)
}

function dropRestoreDb() {
  try {
    // Terminate connections to the restore DB before dropping
    psql(
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'pg_resilient_restore' AND pid <> pg_backend_pid()",
      ADMIN_CONN,
    )
  } catch { /* ignore */ }
  try {
    psql('DROP DATABASE IF EXISTS pg_resilient_restore', ADMIN_CONN)
  } catch { /* ignore */ }
}

/** Remove all .done / .restored.done marker files left by previous restore runs */
function clearResumeMarkers(dir: string) {
  const removeMarkers = (d: string) => {
    for (const entry of readdirSync(d)) {
      const full = join(d, entry)
      if (statSync(full).isDirectory()) {
        removeMarkers(full)
      } else if (entry.endsWith('.restored.done') || entry === '_pre_data.done' || entry === '_post_data.done') {
        unlinkSync(full)
      }
    }
  }
  try { removeMarkers(dir) } catch { /* ignore */ }
}

function defaultDumpOpts(output: string): DumpOptions {
  return {
    dbname: SOURCE_CONN,
    output,
    jobs: 2,
    splitThreshold: 1_073_741_824,
    maxChunksPerTable: 64,
    retries: 2,
    retryDelay: 1,
    noSnapshot: true,
    dryRun: false,
    pgDumpArgs: [],
  }
}

function defaultRestoreOpts(input: string, overrides: Partial<RestoreOptions> = {}): RestoreOptions {
  return {
    dbname: RESTORE_CONN,
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
    compose('up -d')
    waitForPg()
    // Load fixtures into source DB
    execSync(`psql "${SOURCE_CONN}" -f "${FIXTURES_FILE}"`, {
      stdio: 'pipe',
      timeout: 30_000,
    })
    // Dump the source DB once for all restore tests
    dumpDir = freshTmpDir()
    await runDump(defaultDumpOpts(dumpDir))
  }, 60_000)

  afterAll(() => {
    dropRestoreDb()
    compose('down -v')
    for (const dir of tmpDirs) {
      rmSync(dir, { recursive: true, force: true })
    }
    tmpDirs = []
  }, 30_000)

  beforeEach(() => {
    createRestoreDb()
    // Clear resume markers so each test gets a fresh restore
    if (dumpDir) clearResumeMarkers(dumpDir)
  })

  afterEach(() => {
    dropRestoreDb()
  })

  it('restores all tables with correct row counts', async () => {
    await runRestore(defaultRestoreOpts(dumpDir))

    // Verify row counts match source
    const sourceUsers = parseInt(psql('SELECT count(*) FROM public.users', SOURCE_CONN), 10)
    const restoreUsers = parseInt(psql('SELECT count(*) FROM public.users', RESTORE_CONN), 10)
    expect(restoreUsers).toBe(sourceUsers)

    const sourceLogs = parseInt(psql('SELECT count(*) FROM public.logs', SOURCE_CONN), 10)
    const restoreLogs = parseInt(psql('SELECT count(*) FROM public.logs', RESTORE_CONN), 10)
    expect(restoreLogs).toBe(sourceLogs)

    const sourceConfig = parseInt(psql('SELECT count(*) FROM public.config', SOURCE_CONN), 10)
    const restoreConfig = parseInt(psql('SELECT count(*) FROM public.config', RESTORE_CONN), 10)
    expect(restoreConfig).toBe(sourceConfig)

    const sourceProducts = parseInt(psql('SELECT count(*) FROM public.products', SOURCE_CONN), 10)
    const restoreProducts = parseInt(psql('SELECT count(*) FROM public.products', RESTORE_CONN), 10)
    expect(restoreProducts).toBe(sourceProducts)

    const sourceEvents = parseInt(psql('SELECT count(*) FROM analytics.events', SOURCE_CONN), 10)
    const restoreEvents = parseInt(psql('SELECT count(*) FROM analytics.events', RESTORE_CONN), 10)
    expect(restoreEvents).toBe(sourceEvents)
  })

  it('generated column (products.tax) computes correctly after restore', async () => {
    await runRestore(defaultRestoreOpts(dumpDir))

    // Verify the generated column recomputes correctly
    const mismatchCount = parseInt(
      psql(
        "SELECT count(*) FROM public.products WHERE tax <> (price * 0.1)::numeric(10,2)",
        RESTORE_CONN,
      ),
      10,
    )
    expect(mismatchCount).toBe(0)

    // Make sure tax values actually exist (not all NULL)
    const nullCount = parseInt(
      psql('SELECT count(*) FROM public.products WHERE tax IS NULL', RESTORE_CONN),
      10,
    )
    expect(nullCount).toBe(0)
  })

  it('data-only restore works into pre-existing schema', async () => {
    // First do a full restore to set up DDL
    await runRestore(defaultRestoreOpts(dumpDir))

    // Truncate all tables in the restore DB
    psql('TRUNCATE public.users, public.logs, public.config, public.products CASCADE', RESTORE_CONN)
    psql('TRUNCATE analytics.events CASCADE', RESTORE_CONN)

    // Verify tables are empty
    expect(parseInt(psql('SELECT count(*) FROM public.users', RESTORE_CONN), 10)).toBe(0)

    // Clear restored markers so data-only restore re-processes all chunks
    clearResumeMarkers(dumpDir)

    // Now do a data-only restore
    await runRestore(defaultRestoreOpts(dumpDir, { dataOnly: true }))

    // Verify data was restored
    const sourceUsers = parseInt(psql('SELECT count(*) FROM public.users', SOURCE_CONN), 10)
    const restoreUsers = parseInt(psql('SELECT count(*) FROM public.users', RESTORE_CONN), 10)
    expect(restoreUsers).toBe(sourceUsers)
  })
})
