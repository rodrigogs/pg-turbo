import { existsSync } from 'node:fs'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { createArchive, extractArchive, isPgtArchive } from '../../src/core/archive.js'

describe('isPgtArchive', () => {
  it('returns true for .pgt extension', () => {
    expect(isPgtArchive('/path/to/dump.pgt')).toBe(true)
    expect(isPgtArchive('dump.pgt')).toBe(true)
  })

  it('returns false for directories and other extensions', () => {
    expect(isPgtArchive('/path/to/dump')).toBe(false)
    expect(isPgtArchive('/path/to/dump.tar')).toBe(false)
    expect(isPgtArchive('/path/to/dump.pgt.bak')).toBe(false)
  })
})

describe('archive round-trip', () => {
  let sourceDir: string
  let extractDir: string
  let archivePath: string

  beforeEach(async () => {
    sourceDir = await mkdtemp(join(tmpdir(), 'pgr-test-src-'))
    extractDir = await mkdtemp(join(tmpdir(), 'pgr-test-ext-'))
    archivePath = join(sourceDir, '..', 'test-dump.pgt')
  })

  afterEach(async () => {
    await rm(sourceDir, { recursive: true, force: true }).catch(() => {})
    await rm(extractDir, { recursive: true, force: true }).catch(() => {})
    await rm(archivePath, { force: true }).catch(() => {})
  })

  it('creates and extracts archive preserving file contents', async () => {
    // Create mock dump structure
    const manifest = JSON.stringify({ version: 1, tables: [] })
    await writeFile(join(sourceDir, 'manifest.json'), manifest)
    await writeFile(join(sourceDir, '_schema_ddl.dump'), 'DDL content here')
    await mkdir(join(sourceDir, 'data', 'public.test_table'), { recursive: true })
    await writeFile(join(sourceDir, 'data', 'public.test_table', 'chunk_0000.copy.zst'), 'chunk-data')

    // Create archive
    const size = await createArchive(sourceDir, archivePath)
    expect(size).toBeGreaterThan(0)
    expect(existsSync(archivePath)).toBe(true)

    // Extract archive
    await extractArchive(archivePath, extractDir)

    // Verify all files present with correct content
    expect(await readFile(join(extractDir, 'manifest.json'), 'utf-8')).toBe(manifest)
    expect(await readFile(join(extractDir, '_schema_ddl.dump'), 'utf-8')).toBe('DDL content here')
    expect(await readFile(join(extractDir, 'data', 'public.test_table', 'chunk_0000.copy.zst'), 'utf-8')).toBe(
      'chunk-data',
    )
  })

  it('handles empty directories', async () => {
    await writeFile(join(sourceDir, 'manifest.json'), '{}')

    const size = await createArchive(sourceDir, archivePath)
    expect(size).toBeGreaterThan(0)

    await extractArchive(archivePath, extractDir)
    expect(await readFile(join(extractDir, 'manifest.json'), 'utf-8')).toBe('{}')
  })
})
