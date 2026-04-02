import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { readManifest, writeManifest } from '../../src/core/manifest.js'
import type { DumpManifest } from '../../src/types/index.js'

const SAMPLE_MANIFEST: DumpManifest = {
  version: 1,
  tool: 'pg-turbo',
  createdAt: '2026-03-31T05:00:00.000Z',
  pgVersion: '16.2',
  database: 'testdb',
  snapshotId: '00000003-1B',
  compression: 'lz4',
  options: { schemaFilter: null, splitThresholdBytes: 1_073_741_824, jobs: 4 },
  tables: [
    {
      schema: 'public',
      name: 'users',
      oid: 16385,
      relkind: 'r',
      estimatedBytes: 1024,
      estimatedRows: 10,
      pkColumn: 'id',
      pkType: 'int8',
      chunkStrategy: 'none',
      columns: ['id', 'name'],
      generatedColumns: [],
      chunks: [{ index: 0, file: 'data/public.users/chunk_0000.copy.lz4' }],
    },
  ],
  sequences: [],
}

describe('manifest', () => {
  let tmpDir: string
  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'pgr-test-'))
  })
  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true })
  })

  it('writes and reads manifest roundtrip', async () => {
    await writeManifest(tmpDir, SAMPLE_MANIFEST)
    const read = await readManifest(tmpDir)
    expect(read).toEqual(SAMPLE_MANIFEST)
  })
  it('throws on missing manifest', async () => {
    await expect(readManifest(tmpDir)).rejects.toThrow()
  })
  it('throws on invalid version', async () => {
    const bad = { ...SAMPLE_MANIFEST, version: 99 }
    await writeManifest(tmpDir, bad as unknown as DumpManifest)
    await expect(readManifest(tmpDir)).rejects.toThrow('version')
  })

  it('coerces string numeric fields to numbers', async () => {
    // Simulate pg bigint -> JS string scenario
    const manifestWithStrings = {
      ...SAMPLE_MANIFEST,
      tables: [
        {
          ...SAMPLE_MANIFEST.tables[0],
          estimatedBytes: '2048' as unknown as number,
          estimatedRows: '500' as unknown as number,
          chunks: [
            {
              index: 0,
              file: 'data/public.users/chunk_0000.copy.lz4',
              estimatedBytes: '1024' as unknown as number,
              estimatedRows: '250' as unknown as number,
            },
          ],
        },
      ],
    }
    await writeManifest(tmpDir, manifestWithStrings as unknown as DumpManifest)
    const read = await readManifest(tmpDir)
    expect(read.tables[0].estimatedBytes).toBe(2048)
    expect(read.tables[0].estimatedRows).toBe(500)
    expect(read.tables[0].chunks[0].estimatedBytes).toBe(1024)
    expect(read.tables[0].chunks[0].estimatedRows).toBe(250)
  })

  it('coerces non-numeric string fields to 0', async () => {
    const manifestWithBadValues = {
      ...SAMPLE_MANIFEST,
      tables: [
        {
          ...SAMPLE_MANIFEST.tables[0],
          estimatedBytes: 'not_a_number' as unknown as number,
          estimatedRows: '' as unknown as number,
        },
      ],
    }
    await writeManifest(tmpDir, manifestWithBadValues as unknown as DumpManifest)
    const read = await readManifest(tmpDir)
    expect(read.tables[0].estimatedBytes).toBe(0)
    expect(read.tables[0].estimatedRows).toBe(0)
  })

  it('handles chunks without estimatedRows/estimatedBytes fields', async () => {
    const manifestNoEstimates = {
      ...SAMPLE_MANIFEST,
      tables: [
        {
          ...SAMPLE_MANIFEST.tables[0],
          chunks: [{ index: 0, file: 'data/public.users/chunk_0000.copy.lz4' }],
        },
      ],
    }
    await writeManifest(tmpDir, manifestNoEstimates as unknown as DumpManifest)
    const read = await readManifest(tmpDir)
    // When estimatedRows/estimatedBytes are undefined on chunks, they should remain undefined
    expect(read.tables[0].chunks[0].estimatedRows).toBeUndefined()
    expect(read.tables[0].chunks[0].estimatedBytes).toBeUndefined()
  })

  it('handles chunks with explicit null estimatedRows/estimatedBytes', async () => {
    // When JSON serialized, null is preserved. The `!= null` check is false for null,
    // so null values pass through unchanged (not coerced to 0).
    const manifestWithNulls = {
      ...SAMPLE_MANIFEST,
      tables: [
        {
          ...SAMPLE_MANIFEST.tables[0],
          chunks: [
            {
              index: 0,
              file: 'data/public.users/chunk_0000.copy.lz4',
              estimatedRows: null as unknown as number | undefined,
              estimatedBytes: null as unknown as number | undefined,
            },
          ],
        },
      ],
    }
    await writeManifest(tmpDir, manifestWithNulls as unknown as DumpManifest)
    const read = await readManifest(tmpDir)
    // null != null is false, so the coercion code does NOT run for null values.
    // They remain null after JSON round-trip.
    expect(read.tables[0].chunks[0].estimatedRows).toBeNull()
    expect(read.tables[0].chunks[0].estimatedBytes).toBeNull()
  })

  it('rejects manifest with path traversal in chunk file', async () => {
    const manifestWithTraversal = {
      ...SAMPLE_MANIFEST,
      tables: [
        {
          ...SAMPLE_MANIFEST.tables[0],
          chunks: [{ index: 0, file: '../../../etc/passwd' }],
        },
      ],
    }
    await writeManifest(tmpDir, manifestWithTraversal as unknown as DumpManifest)
    await expect(readManifest(tmpDir)).rejects.toThrow('Path traversal')
  })
})
