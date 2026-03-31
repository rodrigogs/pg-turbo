// ts/tests/unit/chunker.test.ts
import { describe, it, expect } from 'vitest'
import { planChunks, buildCopyQuery, chunkFilePath, chunkStrategy } from '../../src/core/chunker.js'
import type { TableInfo, ChunkMeta } from '../../src/types/index.js'

const makeTable = (overrides: Partial<TableInfo> = {}): TableInfo => ({
  oid: 16385, schemaName: 'public', tableName: 'users', relkind: 'r',
  relpages: 1000, estimatedRows: 100000, actualBytes: 2_000_000_000,
  pkColumn: 'id', pkType: 'int8', columns: ['id', 'name', 'email'], generatedColumns: [],
  ...overrides,
})

describe('planChunks', () => {
  it('returns single chunk for small table', () => {
    const table = makeTable({ actualBytes: 100_000 })
    const chunks = planChunks(table, { splitThreshold: 1_073_741_824, maxChunks: 32, pgMajorVersion: 16, pkMin: null, pkMax: null })
    expect(chunks).toHaveLength(1)
  })
  it('splits by PK range for large table with integer PK', () => {
    const table = makeTable({ actualBytes: 4_000_000_000 })
    const chunks = planChunks(table, { splitThreshold: 1_073_741_824, maxChunks: 32, pgMajorVersion: 16, pkMin: 1, pkMax: 10_000_000 })
    expect(chunks.length).toBe(4)
    expect(chunks[0]!.rangeStart).toBe(1)
    expect(chunks[chunks.length - 1]!.rangeEnd).toBe(10_000_000)
    for (let i = 1; i < chunks.length; i++) {
      expect(chunks[i]!.rangeStart).toBe(chunks[i - 1]!.rangeEnd! + 1)
    }
  })
  it('splits by ctid for large table without PK on PG 14+', () => {
    const table = makeTable({ actualBytes: 3_000_000_000, pkColumn: null, pkType: null, relpages: 365000 })
    const chunks = planChunks(table, { splitThreshold: 1_073_741_824, maxChunks: 32, pgMajorVersion: 16, pkMin: null, pkMax: null })
    expect(chunks.length).toBe(3)
    expect(chunks[0]!.ctidStart).toBe(0)
    expect(chunks[chunks.length - 1]!.ctidEnd).toBeUndefined()
  })
  it('no splitting for table without PK on PG 13', () => {
    const table = makeTable({ actualBytes: 3_000_000_000, pkColumn: null, pkType: null })
    const chunks = planChunks(table, { splitThreshold: 1_073_741_824, maxChunks: 32, pgMajorVersion: 13, pkMin: null, pkMax: null })
    expect(chunks).toHaveLength(1)
  })
  it('caps at maxChunks', () => {
    const table = makeTable({ actualBytes: 100_000_000_000 })
    const chunks = planChunks(table, { splitThreshold: 1_073_741_824, maxChunks: 8, pgMajorVersion: 16, pkMin: 1, pkMax: 100_000_000 })
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
  it('generates correct path', () => {
    expect(chunkFilePath('public', 'users', 0)).toBe('data/public.users/chunk_0000.copy.lz4')
    expect(chunkFilePath('public', 'users', 12)).toBe('data/public.users/chunk_0012.copy.lz4')
  })

  it('sanitizes filesystem-unsafe characters', () => {
    expect(chunkFilePath('public', 'table"with"quotes', 0)).toBe('data/public.table_with_quotes/chunk_0000.copy.lz4')
    expect(chunkFilePath('my/schema', 'my:table', 0)).toBe('data/my_schema.my_table/chunk_0000.copy.lz4')
  })
})
