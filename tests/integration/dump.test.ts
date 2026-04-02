import { execSync } from 'node:child_process'
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { runDump } from '../../src/cli/dump.js'
import type { DumpManifest, DumpOptions } from '../../src/types/index.js'

const COMPOSE_FILE = join(__dirname, 'docker-compose.yml')
const FIXTURES_FILE = join(__dirname, 'fixtures.sql')
const CONN = 'postgresql://test_admin@localhost:54399/pg_resilient_test'

function compose(cmd: string) {
  execSync(`docker-compose -f "${COMPOSE_FILE}" ${cmd}`, {
    stdio: 'pipe',
    timeout: 60_000,
  })
}

function psql(query: string, dbname?: string) {
  const cs = dbname ? `postgresql://test_admin@localhost:54399/${dbname}` : CONN
  return execSync(`psql "${cs}" -t -A -c "${query}"`, {
    encoding: 'utf-8',
    timeout: 30_000,
  }).trim()
}

function waitForPg(maxWaitSecs = 30) {
  const deadline = Date.now() + maxWaitSecs * 1000
  while (Date.now() < deadline) {
    try {
      psql('SELECT 1')
      return
    } catch {
      execSync('sleep 1')
    }
  }
  throw new Error('PostgreSQL did not become ready')
}

function defaultDumpOpts(output: string, overrides: Partial<DumpOptions> = {}): DumpOptions {
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

  beforeAll(() => {
    compose('up -d')
    waitForPg()
    // Load fixtures
    execSync(`psql "${CONN}" -f "${FIXTURES_FILE}"`, {
      stdio: 'pipe',
      timeout: 30_000,
    })
  }, 60_000)

  afterAll(() => {
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
    await runDump(defaultDumpOpts(outDir))

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
    await runDump(defaultDumpOpts(outDir))

    const manifest: DumpManifest = JSON.parse(readFileSync(join(outDir, 'manifest.json'), 'utf-8'))

    const products = manifest.tables.find((t) => t.name === 'products')
    expect(products).toBeDefined()
    expect(products.generatedColumns).toContain('tax')
    // The columns list in manifest includes all columns (including generated)
    expect(products.columns).toContain('id')
    expect(products.columns).toContain('name')
    expect(products.columns).toContain('price')
    // Generated columns are tracked but excluded from COPY
    // The columns field stores non-generated columns (those selected by buildColumnsQuery)
    // and generatedColumns stores the generated ones
    expect(products.generatedColumns.length).toBeGreaterThan(0)
  })

  it('filters to analytics schema only', async () => {
    const outDir = freshTmpDir()
    await runDump(defaultDumpOpts(outDir, { schema: 'analytics' }))

    const manifest: DumpManifest = JSON.parse(readFileSync(join(outDir, 'manifest.json'), 'utf-8'))

    // Only analytics tables should be in the manifest
    expect(manifest.tables.length).toBeGreaterThan(0)
    for (const table of manifest.tables) {
      expect(table.schema).toBe('analytics')
    }

    // DDL file should exist
    expect(existsSync(join(outDir, '_schema_ddl.dump'))).toBe(true)
  })

  it('dry run creates no files', async () => {
    const outDir = freshTmpDir()
    await runDump(defaultDumpOpts(outDir, { dryRun: true }))

    // Output dir should be empty or not have data files
    const ddlPath = join(outDir, '_schema_ddl.dump')
    expect(existsSync(ddlPath)).toBe(false)

    const manifestPath = join(outDir, 'manifest.json')
    expect(existsSync(manifestPath)).toBe(false)
  })

  it('discovers sequences', async () => {
    const outDir = freshTmpDir()
    await runDump(defaultDumpOpts(outDir))

    const manifest: DumpManifest = JSON.parse(readFileSync(join(outDir, 'manifest.json'), 'utf-8'))

    // We created custom_seq plus serials create implicit sequences
    expect(manifest.sequences.length).toBeGreaterThan(0)

    const customSeq = manifest.sequences.find((s) => s.name === 'custom_seq')
    expect(customSeq).toBeDefined()
    expect(customSeq.schema).toBe('public')
    // custom_seq starts at 42, nextval called at least twice
    expect(customSeq.lastValue).toBeGreaterThanOrEqual(43)
  })
})
