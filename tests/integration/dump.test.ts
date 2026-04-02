import { existsSync, mkdtempSync, readFileSync, rmSync, statSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql'
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { runDump } from '../../src/cli/dump.js'
import type { DumpManifest } from '../../src/types/index.js'
import { defaultDumpOpts, loadFixtures } from './helpers.js'

describe('dump integration', () => {
  let container: StartedPostgreSqlContainer
  let connUri: string
  let tmpDirs: string[] = []

  function freshTmpDir(): string {
    const dir = mkdtempSync(join(tmpdir(), 'pgr-dump-'))
    tmpDirs.push(dir)
    return dir
  }

  beforeAll(async () => {
    container = await new PostgreSqlContainer('postgres:16-alpine')
      .withDatabase('pg_resilient_test')
      .withUsername('test_admin')
      .withPassword('test_admin')
      .start()
    connUri = container.getConnectionUri()
    await loadFixtures(connUri)
  }, 120_000)

  afterAll(async () => {
    await container?.stop()
  }, 30_000)

  afterEach(() => {
    for (const dir of tmpDirs) {
      rmSync(dir, { recursive: true, force: true })
    }
    tmpDirs = []
  })

  it('dumps all tables and produces valid manifest', async () => {
    const outDir = freshTmpDir()
    await runDump(defaultDumpOpts(connUri, outDir))

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
    await runDump(defaultDumpOpts(connUri, outDir))

    const manifest: DumpManifest = JSON.parse(readFileSync(join(outDir, 'manifest.json'), 'utf-8'))

    const products = manifest.tables.find((t) => t.name === 'products')
    expect(products).toBeDefined()
    expect(products!.generatedColumns).toContain('tax')
    expect(products!.columns).toContain('id')
    expect(products!.columns).toContain('name')
    expect(products!.columns).toContain('price')
    expect(products!.generatedColumns.length).toBeGreaterThan(0)
  })

  it('filters to analytics schema only', async () => {
    const outDir = freshTmpDir()
    await runDump(defaultDumpOpts(connUri, outDir, { schema: 'analytics' }))

    const manifest: DumpManifest = JSON.parse(readFileSync(join(outDir, 'manifest.json'), 'utf-8'))

    expect(manifest.tables.length).toBeGreaterThan(0)
    for (const table of manifest.tables) {
      expect(table.schema).toBe('analytics')
    }

    expect(existsSync(join(outDir, '_schema_ddl.dump'))).toBe(true)
  })

  it('dry run creates no files', async () => {
    const outDir = freshTmpDir()
    await runDump(defaultDumpOpts(connUri, outDir, { dryRun: true }))

    expect(existsSync(join(outDir, '_schema_ddl.dump'))).toBe(false)
    expect(existsSync(join(outDir, 'manifest.json'))).toBe(false)
  })

  it('discovers sequences', async () => {
    const outDir = freshTmpDir()
    await runDump(defaultDumpOpts(connUri, outDir))

    const manifest: DumpManifest = JSON.parse(readFileSync(join(outDir, 'manifest.json'), 'utf-8'))

    expect(manifest.sequences.length).toBeGreaterThan(0)

    const customSeq = manifest.sequences.find((s) => s.name === 'custom_seq')
    expect(customSeq).toBeDefined()
    expect(customSeq!.schema).toBe('public')
    expect(customSeq!.lastValue).toBeGreaterThanOrEqual(43)
  })

  it('chunks tables with low split threshold', async () => {
    const outDir = freshTmpDir()
    // Use 500KB split threshold to force chunking on the users table (10k rows)
    await runDump(defaultDumpOpts(connUri, outDir, { splitThreshold: 512_000 }))

    const manifest: DumpManifest = JSON.parse(readFileSync(join(outDir, 'manifest.json'), 'utf-8'))

    // At least one table should have multiple chunks with pk_range strategy
    const chunkedTable = manifest.tables.find((t) => t.chunks.length > 1)
    expect(chunkedTable).toBeDefined()
    expect(chunkedTable!.chunkStrategy).toBe('pk_range')

    // Verify all chunk files exist
    for (const chunk of chunkedTable!.chunks) {
      expect(existsSync(join(outDir, chunk.file))).toBe(true)
    }
  })

  it('errors on non-existent schema', async () => {
    const outDir = freshTmpDir()
    await expect(
      runDump(defaultDumpOpts(connUri, outDir, { schema: 'nonexistent_schema_xyz' })),
    ).rejects.toThrow(/not found/)
  })

  it('resumes: second dump skips already-completed chunks', async () => {
    const outDir = freshTmpDir()

    // First dump
    await runDump(defaultDumpOpts(connUri, outDir))
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

    // Second dump — should skip all chunks because .done markers exist
    await runDump(defaultDumpOpts(connUri, outDir))

    // Chunk files should not have been rewritten (mtimes unchanged)
    for (const [path, mtime] of mtimes) {
      const { mtimeMs } = statSync(path)
      expect(mtimeMs).toBe(mtime)
    }
  })
})
