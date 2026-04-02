import { mkdtempSync, readdirSync, rmSync, statSync, unlinkSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { runDump } from '../../src/cli/dump.js'
import { runRestore } from '../../src/cli/restore.js'
import type { RestoreOptions } from '../../src/types/index.js'
import { createDatabase, defaultDumpOpts, defaultRestoreOpts, loadFixtures, query } from './helpers.js'

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

describe('restore integration', () => {
  let container: StartedPostgreSqlContainer
  let sourceUri: string
  let adminUri: string
  let restoreUri: string
  let dumpDir: string
  let tmpDirs: string[] = []

  function freshTmpDir(): string {
    const dir = mkdtempSync(join(tmpdir(), 'pgr-restore-'))
    tmpDirs.push(dir)
    return dir
  }

  beforeAll(async () => {
    container = await new PostgreSqlContainer('postgres:16-alpine')
      .withDatabase('pg_resilient_test')
      .withUsername('test_admin')
      .withPassword('test_admin')
      .start()
    sourceUri = container.getConnectionUri()

    // Build admin URI (connects to 'postgres' db)
    const url = new URL(sourceUri)
    url.pathname = '/postgres'
    adminUri = url.toString()

    // Build restore URI
    const restoreUrl = new URL(sourceUri)
    restoreUrl.pathname = '/pg_resilient_restore'
    restoreUri = restoreUrl.toString()

    // Load fixtures into source DB
    await loadFixtures(sourceUri)

    // Dump the source DB once for all restore tests
    dumpDir = freshTmpDir()
    await runDump(defaultDumpOpts(sourceUri, dumpDir))
  }, 120_000)

  afterAll(async () => {
    await container?.stop()
    for (const dir of tmpDirs) {
      rmSync(dir, { recursive: true, force: true })
    }
    tmpDirs = []
  }, 30_000)

  beforeEach(async () => {
    await createDatabase(adminUri, 'pg_resilient_restore')
    // Clear resume markers so each test gets a fresh restore
    if (dumpDir) clearResumeMarkers(dumpDir)
  })

  afterEach(async () => {
    // Drop restore DB between tests
    await createDatabase(adminUri, 'pg_resilient_restore').catch(() => {})
  })

  it('restores all tables with correct row counts', async () => {
    await runRestore(defaultRestoreOpts(restoreUri, dumpDir))

    // Verify row counts match source
    const sourceUsers = await query(sourceUri, 'SELECT count(*) FROM public.users')
    const restoreUsers = await query(restoreUri, 'SELECT count(*) FROM public.users')
    expect(parseInt(restoreUsers, 10)).toBe(parseInt(sourceUsers, 10))

    const sourceLogs = await query(sourceUri, 'SELECT count(*) FROM public.logs')
    const restoreLogs = await query(restoreUri, 'SELECT count(*) FROM public.logs')
    expect(parseInt(restoreLogs, 10)).toBe(parseInt(sourceLogs, 10))

    const sourceConfig = await query(sourceUri, 'SELECT count(*) FROM public.config')
    const restoreConfig = await query(restoreUri, 'SELECT count(*) FROM public.config')
    expect(parseInt(restoreConfig, 10)).toBe(parseInt(sourceConfig, 10))

    const sourceProducts = await query(sourceUri, 'SELECT count(*) FROM public.products')
    const restoreProducts = await query(restoreUri, 'SELECT count(*) FROM public.products')
    expect(parseInt(restoreProducts, 10)).toBe(parseInt(sourceProducts, 10))

    const sourceEvents = await query(sourceUri, 'SELECT count(*) FROM analytics.events')
    const restoreEvents = await query(restoreUri, 'SELECT count(*) FROM analytics.events')
    expect(parseInt(restoreEvents, 10)).toBe(parseInt(sourceEvents, 10))
  })

  it('generated column (products.tax) computes correctly after restore', async () => {
    await runRestore(defaultRestoreOpts(restoreUri, dumpDir))

    const mismatchCount = await query(
      restoreUri,
      'SELECT count(*) FROM public.products WHERE tax <> (price * 0.1)::numeric(10,2)',
    )
    expect(parseInt(mismatchCount, 10)).toBe(0)

    const nullCount = await query(restoreUri, 'SELECT count(*) FROM public.products WHERE tax IS NULL')
    expect(parseInt(nullCount, 10)).toBe(0)
  })

  it('data-only restore works into pre-existing schema', async () => {
    // First do a full restore to set up DDL
    await runRestore(defaultRestoreOpts(restoreUri, dumpDir))

    // Truncate all tables in the restore DB
    await query(restoreUri, 'TRUNCATE public.users, public.logs, public.config, public.products CASCADE')
    await query(restoreUri, 'TRUNCATE analytics.events CASCADE')

    // Verify tables are empty
    const count = await query(restoreUri, 'SELECT count(*) FROM public.users')
    expect(parseInt(count, 10)).toBe(0)

    // Clear restored markers so data-only restore re-processes all chunks
    clearResumeMarkers(dumpDir)

    // Now do a data-only restore
    await runRestore(defaultRestoreOpts(restoreUri, dumpDir, { dataOnly: true }))

    // Verify data was restored
    const sourceUsers = await query(sourceUri, 'SELECT count(*) FROM public.users')
    const restoreUsers = await query(restoreUri, 'SELECT count(*) FROM public.users')
    expect(parseInt(restoreUsers, 10)).toBe(parseInt(sourceUsers, 10))
  })

  it('clean mode drops and recreates schema', async () => {
    // First restore
    await runRestore(defaultRestoreOpts(restoreUri, dumpDir))

    // Verify data exists
    const count1 = await query(restoreUri, 'SELECT count(*) FROM public.users')
    expect(parseInt(count1, 10)).toBeGreaterThan(0)

    // Clear markers for second restore
    clearResumeMarkers(dumpDir)

    // Restore with --clean
    await runRestore(defaultRestoreOpts(restoreUri, dumpDir, { clean: true }))

    // Verify data was restored again (schemas were dropped and recreated)
    const sourceUsers = await query(sourceUri, 'SELECT count(*) FROM public.users')
    const restoreUsers = await query(restoreUri, 'SELECT count(*) FROM public.users')
    expect(parseInt(restoreUsers, 10)).toBe(parseInt(sourceUsers, 10))
  })

  it('--table filter restores only a single table', async () => {
    // Full restore first to get DDL
    await runRestore(defaultRestoreOpts(restoreUri, dumpDir))

    // Truncate just the users table
    await query(restoreUri, 'TRUNCATE public.users CASCADE')
    const emptyCount = await query(restoreUri, 'SELECT count(*) FROM public.users')
    expect(parseInt(emptyCount, 10)).toBe(0)

    // Clear markers
    clearResumeMarkers(dumpDir)

    // Restore only the users table (data-only since DDL already exists)
    await runRestore(
      defaultRestoreOpts(restoreUri, dumpDir, { table: 'public.users', dataOnly: true }),
    )

    // Verify users table was restored
    const sourceUsers = await query(sourceUri, 'SELECT count(*) FROM public.users')
    const restoreUsers = await query(restoreUri, 'SELECT count(*) FROM public.users')
    expect(parseInt(restoreUsers, 10)).toBe(parseInt(sourceUsers, 10))
  })

  it('resume: second restore with --clean succeeds after first restore', async () => {
    // First restore
    await runRestore(defaultRestoreOpts(restoreUri, dumpDir))

    const count1 = await query(restoreUri, 'SELECT count(*) FROM public.users')
    expect(parseInt(count1, 10)).toBeGreaterThan(0)

    // Clear markers for second restore
    clearResumeMarkers(dumpDir)

    // Run restore again with --clean — should drop schemas, re-restore DDL and data
    await runRestore(defaultRestoreOpts(restoreUri, dumpDir, { clean: true }))

    // Data should match source
    const sourceUsers = await query(sourceUri, 'SELECT count(*) FROM public.users')
    const count2 = await query(restoreUri, 'SELECT count(*) FROM public.users')
    expect(parseInt(count2, 10)).toBe(parseInt(sourceUsers, 10))
  })
})
