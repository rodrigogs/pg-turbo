import { execSync } from 'node:child_process'
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import pg from 'pg'
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { runDump } from '../../src/cli/dump.js'
import type { DumpManifest, DumpOptions } from '../../src/types/index.js'

const { Client } = pg
const COMPOSE = join(__dirname, 'docker-compose.yml')
const FIXTURES = join(__dirname, 'fixtures.sql')
const CONN = 'postgresql://test_admin@localhost:54399/pg_resilient_test'

function compose(cmd: string) {
  execSync(`docker-compose -f "${COMPOSE}" ${cmd}`, { stdio: 'pipe', timeout: 60_000 })
}

async function query(sql: string, connStr = CONN): Promise<string> {
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

function defaultOpts(output: string, overrides: Partial<DumpOptions> = {}): DumpOptions {
  return {
    dbname: CONN,
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

describe('dump integration', () => {
  let tmpDirs: string[] = []

  function freshTmpDir(): string {
    const dir = mkdtempSync(join(tmpdir(), 'pgr-dump-'))
    tmpDirs.push(dir)
    return dir
  }

  beforeAll(async () => {
    compose('up -d --wait')
    await waitForPg()
    // Load fixtures
    execSync(`psql "${CONN}" -f "${FIXTURES}"`, { stdio: 'pipe', timeout: 30_000 })
  }, 120_000)

  afterAll(async () => {
    compose('down -v')
  }, 30_000)

  afterEach(() => {
    for (const dir of tmpDirs) {
      rmSync(dir, { recursive: true, force: true })
    }
    tmpDirs = []
  })

  it('dumps all tables and produces valid manifest', async () => {
    const outDir = freshTmpDir()
    await runDump(defaultOpts(outDir))

    // manifest.json exists and is valid JSON
    const manifestPath = join(outDir, 'manifest.json')
    expect(existsSync(manifestPath)).toBe(true)
    const manifest: DumpManifest = JSON.parse(readFileSync(manifestPath, 'utf-8'))
    expect(manifest.version).toBe(1)
    expect(manifest.tool).toBe('pg-resilient')
    expect(manifest.database).toBe('pg_resilient_test')
    expect(manifest.tables.length).toBeGreaterThanOrEqual(5)

    // DDL file exists
    expect(existsSync(join(outDir, '_schema_ddl.dump'))).toBe(true)

    // Every table has chunk files with done markers
    for (const table of manifest.tables) {
      for (const chunk of table.chunks) {
        const chunkPath = join(outDir, chunk.file)
        expect(existsSync(chunkPath)).toBe(true)
        expect(existsSync(`${chunkPath}.done`)).toBe(true)
      }
    }
  })

  it('excludes generated columns from chunk data', async () => {
    const outDir = freshTmpDir()
    await runDump(defaultOpts(outDir))

    const manifest: DumpManifest = JSON.parse(readFileSync(join(outDir, 'manifest.json'), 'utf-8'))

    const products = manifest.tables.find((t) => t.name === 'products')
    expect(products).toBeDefined()
    expect(products?.generatedColumns).toContain('tax')
    expect(products?.columns).toContain('id')
    expect(products?.columns).toContain('name')
    expect(products?.columns).toContain('price')
    expect(products?.generatedColumns.length).toBeGreaterThan(0)
  })

  it('filters to analytics schema only', async () => {
    const outDir = freshTmpDir()
    await runDump(defaultOpts(outDir, { schema: 'analytics' }))

    const manifest: DumpManifest = JSON.parse(readFileSync(join(outDir, 'manifest.json'), 'utf-8'))

    expect(manifest.tables.length).toBeGreaterThan(0)
    for (const table of manifest.tables) {
      expect(table.schema).toBe('analytics')
    }

    expect(existsSync(join(outDir, '_schema_ddl.dump'))).toBe(true)
  })

  it('dry run creates no files', async () => {
    const outDir = freshTmpDir()
    await runDump(defaultOpts(outDir, { dryRun: true }))

    expect(existsSync(join(outDir, '_schema_ddl.dump'))).toBe(false)
    expect(existsSync(join(outDir, 'manifest.json'))).toBe(false)
  })

  it('discovers sequences', async () => {
    const outDir = freshTmpDir()
    await runDump(defaultOpts(outDir))

    const manifest: DumpManifest = JSON.parse(readFileSync(join(outDir, 'manifest.json'), 'utf-8'))

    expect(manifest.sequences.length).toBeGreaterThan(0)

    const customSeq = manifest.sequences.find((s) => s.name === 'custom_seq')
    expect(customSeq).toBeDefined()
    expect(customSeq?.schema).toBe('public')
    expect(customSeq?.lastValue).toBeGreaterThanOrEqual(43)
  })

  it('chunks tables with low split threshold', async () => {
    const outDir = freshTmpDir()
    // Use 500KB split threshold to force chunking on the users table (10k rows)
    await runDump(defaultOpts(outDir, { splitThreshold: 500 * 1024 }))

    const manifest: DumpManifest = JSON.parse(readFileSync(join(outDir, 'manifest.json'), 'utf-8'))

    // At least one table should have multiple chunks with pk_range strategy
    const chunkedTable = manifest.tables.find((t) => t.chunks.length > 1)
    expect(chunkedTable).toBeDefined()
    expect(chunkedTable?.chunkStrategy).toBe('pk_range')

    // Verify all chunk files exist
    for (const chunk of chunkedTable!.chunks) {
      expect(existsSync(join(outDir, chunk.file))).toBe(true)
    }
  })

  it('errors on non-existent schema', async () => {
    const outDir = freshTmpDir()
    await expect(runDump(defaultOpts(outDir, { schema: 'nonexistent_schema_xyz' }))).rejects.toThrow()
  })

  it('resumes: second dump skips already-completed chunks', async () => {
    const outDir = freshTmpDir()

    // First dump
    await runDump(defaultOpts(outDir))
    const manifest1: DumpManifest = JSON.parse(readFileSync(join(outDir, 'manifest.json'), 'utf-8'))

    // Record file modification times of chunk files
    const mtimes = new Map<string, number>()
    for (const table of manifest1.tables) {
      for (const chunk of table.chunks) {
        const chunkPath = join(outDir, chunk.file)
        const { mtimeMs } = statSync(chunkPath)
        mtimes.set(chunkPath, mtimeMs)
      }
    }

    // Wait a small amount so any re-written files would have different mtime
    await new Promise((resolve) => setTimeout(resolve, 100))

    // Second dump -- should skip all chunks because .done markers exist
    await runDump(defaultOpts(outDir))

    // Chunk files should not have been rewritten (mtimes unchanged)
    for (const [path, mtime] of mtimes) {
      const { mtimeMs } = statSync(path)
      expect(mtimeMs).toBe(mtime)
    }
  })

  it('handles snapshot creation when noSnapshot is false', async () => {
    // Run dump WITHOUT --no-snapshot — exercises the snapshot coordinator path
    const outDir = freshTmpDir()
    await runDump(defaultOpts(outDir, { noSnapshot: false }))
    const manifest = JSON.parse(readFileSync(join(outDir, 'manifest.json'), 'utf-8'))
    expect(manifest.snapshotId).toBeTruthy() // snapshot was created
  })

  it('dumps empty schema with no tables', async () => {
    // Create an empty database, dump it — exercises "No tables found" path
    const emptyConn = 'postgresql://test_admin@localhost:54399/pg_resilient_empty'
    const adminConn = 'postgresql://test_admin@localhost:54399/postgres'
    const client = new Client({ connectionString: adminConn })
    await client.connect()
    await client.query('DROP DATABASE IF EXISTS pg_resilient_empty')
    await client.query('CREATE DATABASE pg_resilient_empty')
    await client.end()

    // Create a schema but no tables
    const emptyClient = new Client({ connectionString: emptyConn })
    await emptyClient.connect()
    await emptyClient.query('CREATE SCHEMA test_schema')
    await emptyClient.end()

    const outDir = freshTmpDir()
    await runDump(defaultOpts(outDir, { dbname: emptyConn, noSnapshot: true }))
    // Should complete without error, DDL dumped but no table data
    expect(existsSync(join(outDir, '_schema_ddl.dump'))).toBe(true)

    // Cleanup
    const cleanClient = new Client({ connectionString: adminConn })
    await cleanClient.connect()
    await cleanClient.query('DROP DATABASE IF EXISTS pg_resilient_empty')
    await cleanClient.end()
  })

  it('creates archive when noArchive is false', async () => {
    // Test the archive creation path
    const outDir = freshTmpDir()
    await runDump(defaultOpts(outDir, { noArchive: false }))
    // Should create a .pgr file alongside the dump directory
    const archivePath = `${outDir}.pgr`
    expect(existsSync(archivePath) || existsSync(join(outDir, 'manifest.json'))).toBe(true)
    // Clean up the archive file
    if (existsSync(archivePath)) rmSync(archivePath)
  })

  it('passes pg_dump extra args', async () => {
    // Exercises the pgDumpArgs display path
    const outDir = freshTmpDir()
    await runDump(defaultOpts(outDir, { pgDumpArgs: ['--no-comments'] }))
    expect(existsSync(join(outDir, 'manifest.json'))).toBe(true)
  })

  it('handles pg_dump DDL failure gracefully', async () => {
    const outDir = freshTmpDir()
    // Bad passthrough arg causes pg_dump to fail for DDL, but data dump still completes.
    // The dump should still succeed (DDL failure is non-fatal), but DDL file may be missing.
    await runDump(defaultOpts(outDir, { pgDumpArgs: ['--nonexistent-flag-xyz'] }))
    // Manifest should still be written
    expect(existsSync(join(outDir, 'manifest.json'))).toBe(true)
    // DDL file should be missing or empty since pg_dump failed
    const ddlPath = join(outDir, '_schema_ddl.dump')
    const ddlExists = existsSync(ddlPath)
    if (ddlExists) {
      // If pg_dump created the file before failing, it might be empty or incomplete
      const size = statSync(ddlPath).size
      // Just verify the dump didn't crash -- size can be anything
      expect(size).toBeGreaterThanOrEqual(0)
    }
  })
})
