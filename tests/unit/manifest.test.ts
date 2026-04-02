// ts/tests/unit/manifest.test.ts

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { readManifest, writeManifest } from '../../src/core/manifest.js'
import type { DumpManifest } from '../../src/types/index.js'

const SAMPLE_MANIFEST: DumpManifest = {
  version: 1,
  tool: 'pg-resilient',
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
})
