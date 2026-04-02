// ts/tests/unit/chunker.test.ts
import { describe, expect, it } from 'vitest'
import type { RowSample } from '../../src/core/chunker.js'
import {
  buildCopyQuery,
  chunkEstimatedBytes,
  chunkEstimatedRows,
  chunkFilePath,
  chunkStrategy,
  planChunks,
} from '../../src/core/chunker.js'
import type { ChunkMeta, TableInfo } from '../../src/types/index.js'

const makeTable = (overrides: Partial<TableInfo> = {}): TableInfo => ({
  oid: 16385,
  schemaName: 'public',
  tableName: 'users',
  relkind: 'r',
  relpages: 1000,
  estimatedRows: 100000,
  actualBytes: 2_000_000_000,
  pkColumn: 'id',
  pkType: 'int8',
  columns: ['id', 'name', 'email'],
  generatedColumns: [],
  ...overrides,
})

describe('planChunks', () => {
  it('returns single chunk for small table with estimates', () => {
    const table = makeTable({ actualBytes: 100_000 })
    const chunks = planChunks(table, {
      splitThreshold: 1_073_741_824,
      maxChunks: 32,
      pgMajorVersion: 16,
      pkMin: null,
      pkMax: null,
    })
    expect(chunks).toHaveLength(1)
    expect(chunks[0].estimatedBytes).toBe(100_000)
    expect(chunks[0].estimatedRows).toBe(100000)
  })
  it('splits by PK range for large table with per-chunk estimates', () => {
    const table = makeTable({ actualBytes: 4_000_000_000 })
    const chunks = planChunks(table, {
      splitThreshold: 1_073_741_824,
      maxChunks: 32,
      pgMajorVersion: 16,
      pkMin: 1,
      pkMax: 10_000_000,
    })
    expect(chunks.length).toBe(4)
    expect(chunks[0].rangeStart).toBe(1)
    expect(chunks[chunks.length - 1].rangeEnd).toBe(10_000_000)
    for (let i = 1; i < chunks.length; i++) {
      expect(chunks[i].rangeStart).toBe(chunks[i - 1].rangeEnd + 1)
    }
    // Per-chunk estimates should sum to table total
    const totalBytes = chunks.reduce((s, c) => s + (c.estimatedBytes ?? 0), 0)
    const totalRows = chunks.reduce((s, c) => s + (c.estimatedRows ?? 0), 0)
    expect(totalBytes).toBe(4_000_000_000)
    expect(totalRows).toBe(100000)
  })
  it('splits by ctid for large table without PK on PG 14+ with estimates', () => {
    const table = makeTable({ actualBytes: 3_000_000_000, pkColumn: null, pkType: null, relpages: 365000 })
    const chunks = planChunks(table, {
      splitThreshold: 1_073_741_824,
      maxChunks: 32,
      pgMajorVersion: 16,
      pkMin: null,
      pkMax: null,
    })
    expect(chunks.length).toBe(3)
    expect(chunks[0].ctidStart).toBe(0)
    expect(chunks[chunks.length - 1].ctidEnd).toBeUndefined()
    // Per-chunk estimates should sum to table total
    const totalBytes = chunks.reduce((s, c) => s + (c.estimatedBytes ?? 0), 0)
    const totalRows = chunks.reduce((s, c) => s + (c.estimatedRows ?? 0), 0)
    expect(totalBytes).toBe(3_000_000_000)
    expect(totalRows).toBe(100000)
  })
  it('no splitting for table without PK on PG 13', () => {
    const table = makeTable({ actualBytes: 3_000_000_000, pkColumn: null, pkType: null })
    const chunks = planChunks(table, {
      splitThreshold: 1_073_741_824,
      maxChunks: 32,
      pgMajorVersion: 13,
      pkMin: null,
      pkMax: null,
    })
    expect(chunks).toHaveLength(1)
  })
  it('caps at maxChunks', () => {
    const table = makeTable({ actualBytes: 100_000_000_000 })
    const chunks = planChunks(table, {
      splitThreshold: 1_073_741_824,
      maxChunks: 8,
      pgMajorVersion: 16,
      pkMin: 1,
      pkMax: 100_000_000,
    })
    expect(chunks.length).toBeLessThanOrEqual(8)
  })
})

describe('buildCopyQuery', () => {
  const table = makeTable()
  it('builds plain COPY for unchunked table', () => {
    const chunk: ChunkMeta = { index: 0, file: 'data/public.users/chunk_0000.copy.lz4' }
    expect(buildCopyQuery(table, chunk)).toBe('COPY "public"."users" ("id", "name", "email") TO STDOUT')
  })
  it('builds COPY with PK range', () => {
    const chunk: ChunkMeta = { index: 0, file: '...', rangeStart: 1, rangeEnd: 1000000 }
    const sql = buildCopyQuery(table, chunk)
    expect(sql).toContain('SELECT "id", "name", "email"')
    expect(sql).toContain('>= 1')
    expect(sql).toContain('<= 1000000')
  })
  it('builds first chunk with NULL handling', () => {
    const chunk: ChunkMeta = { index: 0, file: '...', rangeStart: 1, rangeEnd: 1000000 }
    expect(buildCopyQuery(table, chunk)).toContain('"id" IS NULL OR')
  })
  it('builds COPY with ctid range', () => {
    const chunk: ChunkMeta = { index: 0, file: '...', ctidStart: 0, ctidEnd: 5000 }
    const sql = buildCopyQuery(table, chunk)
    expect(sql).toContain("ctid >= '(0,0)'::tid")
    expect(sql).toContain("ctid < '(5000,0)'::tid")
  })
  it('builds open-ended ctid for last chunk', () => {
    const chunk: ChunkMeta = { index: 2, file: '...', ctidStart: 10000 }
    const sql = buildCopyQuery(table, chunk)
    expect(sql).toContain("ctid >= '(10000,0)'::tid")
    expect(sql).not.toContain('ctid <')
  })
  it('excludes generated columns', () => {
    const t = makeTable({ generatedColumns: ['email'] })
    const chunk: ChunkMeta = { index: 0, file: '...' }
    const sql = buildCopyQuery(t, chunk)
    expect(sql).toContain('"id", "name"')
    expect(sql).not.toContain('"email"')
  })

  it('uses SELECT form for materialized views', () => {
    const matView = makeTable({ relkind: 'm' })
    const chunk: ChunkMeta = { index: 0, file: '...' }
    const sql = buildCopyQuery(matView, chunk)
    expect(sql).toBe('COPY (SELECT "id", "name", "email" FROM "public"."users") TO STDOUT')
    expect(sql).not.toContain('COPY "public"')
  })
})

describe('chunkFilePath', () => {
  it('generates correct path with default compression (zstd)', () => {
    expect(chunkFilePath('public', 'users', 0)).toBe('data/public.users/chunk_0000.copy.zst')
    expect(chunkFilePath('public', 'users', 12)).toBe('data/public.users/chunk_0012.copy.zst')
  })

  it('generates correct path with lz4 compression', () => {
    expect(chunkFilePath('public', 'users', 0, 'lz4')).toBe('data/public.users/chunk_0000.copy.lz4')
  })

  it('sanitizes filesystem-unsafe characters', () => {
    expect(chunkFilePath('public', 'table"with"quotes', 0)).toBe('data/public.table_with_quotes/chunk_0000.copy.zst')
    expect(chunkFilePath('my/schema', 'my:table', 0)).toBe('data/my_schema.my_table/chunk_0000.copy.zst')
  })
})

describe('chunkEstimatedBytes / chunkEstimatedRows', () => {
  it('returns chunk estimates when available', () => {
    const table = {
      ...makeTable(),
      schema: 'public',
      name: 'users',
      estimatedBytes: 1000,
      estimatedRows: 100,
      chunkStrategy: 'none' as const,
      chunks: [{ index: 0, file: '...', estimatedBytes: 500, estimatedRows: 50 }],
    }
    const job = { table, chunk: table.chunks[0], outputPath: '/tmp/test', attempt: 0 }
    expect(chunkEstimatedBytes(job)).toBe(500)
    expect(chunkEstimatedRows(job)).toBe(50)
  })
  it('falls back to uniform distribution for old manifests', () => {
    const table = {
      ...makeTable(),
      schema: 'public',
      name: 'users',
      estimatedBytes: 1000,
      estimatedRows: 100,
      chunkStrategy: 'none' as const,
      chunks: [
        { index: 0, file: '...' },
        { index: 1, file: '...' },
      ],
    }
    const job = { table, chunk: table.chunks[0], outputPath: '/tmp/test', attempt: 0 }
    expect(chunkEstimatedBytes(job)).toBe(500)
    expect(chunkEstimatedRows(job)).toBe(50)
  })
})

describe('planChunks with volumeSamples', () => {
  it('uses volume-balanced boundaries with per-chunk estimates', () => {
    const table = makeTable({ actualBytes: 4_000_000_000, estimatedRows: 100_000 })
    // Simulate skewed data: first half of PKs has 10% of bytes, second half has 90%
    const samples: RowSample[] = []
    for (let pk = 1; pk <= 100; pk++) {
      samples.push({ pk: pk * 100_000, bytes: pk <= 50 ? 10 : 90 })
    }
    const chunks = planChunks(table, {
      splitThreshold: 1_073_741_824,
      maxChunks: 32,
      pgMajorVersion: 16,
      pkMin: 1,
      pkMax: 10_000_000,
      volumeSamples: samples,
    })
    expect(chunks.length).toBe(4)
    expect(chunks[0].rangeStart).toBe(1)
    expect(chunks[chunks.length - 1].rangeEnd).toBe(10_000_000)
    // With skewed data, first chunk should cover a wider PK range (lean rows)
    const firstChunkRange = chunks[0].rangeEnd - chunks[0].rangeStart
    const lastChunkRange = chunks[chunks.length - 1].rangeEnd - chunks[chunks.length - 1].rangeStart
    expect(firstChunkRange).toBeGreaterThan(lastChunkRange)
    // Per-chunk estimates should sum to table total
    const totalBytes = chunks.reduce((s, c) => s + (c.estimatedBytes ?? 0), 0)
    const totalRows = chunks.reduce((s, c) => s + (c.estimatedRows ?? 0), 0)
    expect(totalBytes).toBe(4_000_000_000)
    expect(totalRows).toBe(100_000)
    // Skewed data: first chunk (lean rows) should have MORE rows but FEWER bytes
    // than last chunk (dense rows)
    expect(chunks[0].estimatedRows).toBeGreaterThan(chunks[chunks.length - 1].estimatedRows)
  })

  it('falls back to PK range when samples empty', () => {
    const table = makeTable({ actualBytes: 4_000_000_000 })
    const chunks = planChunks(table, {
      splitThreshold: 1_073_741_824,
      maxChunks: 32,
      pgMajorVersion: 16,
      pkMin: 1,
      pkMax: 10_000_000,
      volumeSamples: [],
    })
    // Falls back to even PK range split
    expect(chunks.length).toBe(4)
    const range = chunks[0].rangeEnd - chunks[0].rangeStart
    const range2 = chunks[1].rangeEnd - chunks[1].rangeStart
    expect(range).toBe(range2)
  })

  it('produces single chunk when numChunks is 1', () => {
    const table = makeTable({ actualBytes: 1_500_000_000 })
    const samples: RowSample[] = [{ pk: 500, bytes: 100 }]
    const chunks = planChunks(table, {
      splitThreshold: 1_073_741_824,
      maxChunks: 32,
      pgMajorVersion: 16,
      pkMin: 1,
      pkMax: 10_000_000,
      volumeSamples: samples,
    })
    expect(chunks.length).toBe(2)
    expect(chunks[0].rangeStart).toBe(1)
    expect(chunks[chunks.length - 1].rangeEnd).toBe(10_000_000)
  })

  it('handles case where accounted estimates exceed table totals in remainder chunk', () => {
    // Create samples where rounding causes accounted values to overshoot table estimates.
    // With extreme skew, rounding each chunk's scaled estimates can sum > table total.
    const table = makeTable({ actualBytes: 2_200_000_000, estimatedRows: 1000 })
    // All samples have same bytes so each chunk gets roughly equal volume,
    // but with 3 chunks the remainder might go negative without Math.max(0, ...)
    const samples: RowSample[] = []
    for (let pk = 1; pk <= 100; pk++) {
      samples.push({ pk: pk * 100_000, bytes: 100 })
    }
    const chunks = planChunks(table, {
      splitThreshold: 1_000_000_000,
      maxChunks: 32,
      pgMajorVersion: 16,
      pkMin: 1,
      pkMax: 10_000_000,
      volumeSamples: samples,
    })
    expect(chunks.length).toBeGreaterThanOrEqual(2)
    // Last chunk estimates should never be negative
    const lastChunk = chunks[chunks.length - 1]
    expect(lastChunk.estimatedBytes).toBeGreaterThanOrEqual(0)
    expect(lastChunk.estimatedRows).toBeGreaterThanOrEqual(0)
  })

  it('returns single chunk when volume-balanced numChunks <= 1', () => {
    // actualBytes just slightly above splitThreshold so ceil gives 1
    const table = makeTable({ actualBytes: 1_073_741_824, estimatedRows: 5000 })
    const samples: RowSample[] = [
      { pk: 100, bytes: 50 },
      { pk: 500, bytes: 60 },
      { pk: 900, bytes: 70 },
    ]
    const chunks = planChunks(table, {
      splitThreshold: 1_073_741_824,
      maxChunks: 32,
      pgMajorVersion: 16,
      pkMin: 1,
      pkMax: 10_000,
      volumeSamples: samples,
    })
    expect(chunks).toHaveLength(1)
    expect(chunks[0].rangeStart).toBe(1)
    expect(chunks[0].rangeEnd).toBe(10_000)
    expect(chunks[0].estimatedBytes).toBe(1_073_741_824)
    expect(chunks[0].estimatedRows).toBe(5000)
  })
})

describe('buildCopyQuery for materialized views', () => {
  it('uses SELECT form for chunked matview with ctid', () => {
    const matView = makeTable({ relkind: 'm', pkColumn: null, pkType: null })
    const chunk: ChunkMeta = { index: 0, file: '...', ctidStart: 0, ctidEnd: 5000 }
    const sql = buildCopyQuery(matView, chunk)
    expect(sql).toContain("ctid >= '(0,0)'::tid")
    expect(sql).toContain("ctid < '(5000,0)'::tid")
  })

  it('uses SELECT form for chunked matview with PK range', () => {
    const matView = makeTable({ relkind: 'm' })
    const chunk: ChunkMeta = { index: 1, file: '...', rangeStart: 1001, rangeEnd: 2000 }
    const sql = buildCopyQuery(matView, chunk)
    expect(sql).toContain('>= 1001')
    expect(sql).toContain('<= 2000')
  })
})

describe('chunkStrategy', () => {
  it('returns none for small table', () => {
    const table = makeTable({ actualBytes: 100_000 })
    expect(chunkStrategy(table, { splitThreshold: 1_073_741_824, maxChunks: 32, pgMajorVersion: 16, pkMin: 1, pkMax: 1000 })).toBe('none')
  })

  it('returns pk_range when PK and bounds available', () => {
    const table = makeTable({ actualBytes: 2_000_000_000 })
    expect(chunkStrategy(table, { splitThreshold: 1_073_741_824, maxChunks: 32, pgMajorVersion: 16, pkMin: 1, pkMax: 1000 })).toBe('pk_range')
  })

  it('returns ctid_range for PG 14+ without PK', () => {
    const table = makeTable({ actualBytes: 2_000_000_000, pkColumn: null, pkType: null, relpages: 10000 })
    expect(chunkStrategy(table, { splitThreshold: 1_073_741_824, maxChunks: 32, pgMajorVersion: 14, pkMin: null, pkMax: null })).toBe('ctid_range')
  })

  it('returns none for PG 13 without PK', () => {
    const table = makeTable({ actualBytes: 2_000_000_000, pkColumn: null, pkType: null, relpages: 10000 })
    expect(chunkStrategy(table, { splitThreshold: 1_073_741_824, maxChunks: 32, pgMajorVersion: 13, pkMin: null, pkMax: null })).toBe('none')
  })
})
