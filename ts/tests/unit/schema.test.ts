// ts/tests/unit/schema.test.ts
import { describe, it, expect } from 'vitest'
import { buildTableDiscoveryQuery, buildBatchColumnsQuery, parseTableRows, buildDdlDumpArgs, buildSequenceQuery, quoteIdent } from '../../src/core/schema.js'

describe('quoteIdent', () => {
  it('quotes a simple identifier', () => {
    expect(quoteIdent('users')).toBe('"users"')
  })
  it('escapes embedded double quotes', () => {
    expect(quoteIdent('my"table')).toBe('"my""table"')
  })
  it('handles empty string', () => {
    expect(quoteIdent('')).toBe('""')
  })
})

describe('buildTableDiscoveryQuery', () => {
  it('builds query without schema filter', () => {
    const { text, values } = buildTableDiscoveryQuery(undefined)
    expect(text).toContain('pg_catalog.pg_class')
    expect(text).toContain("n.nspname NOT LIKE 'pg_%'")
    expect(text).not.toContain('$1')
    expect(values).toEqual([])
  })
  it('builds query with schema filter using parameterized query', () => {
    const { text, values } = buildTableDiscoveryQuery('public')
    expect(text).toContain('n.nspname = $1')
    expect(text).not.toContain("'public'")
    expect(values).toEqual(['public'])
  })
})

describe('buildBatchColumnsQuery', () => {
  it('returns query with array parameter', () => {
    const sql = buildBatchColumnsQuery()
    expect(sql).toContain('pg_attribute')
    expect(sql).toContain('$1::oid[]')
    expect(sql).toContain('is_generated')
  })
})

describe('parseTableRows', () => {
  it('parses rows into TableInfo array', () => {
    const rows = [{ oid: 16385, schema_name: 'public', table_name: 'users', relkind: 'r', relpages: 1000, estimated_rows: 50000, actual_bytes: '8388608', pk_column: 'id', pk_type: 'int8' }]
    const tables = parseTableRows(rows)
    expect(tables).toHaveLength(1)
    expect(tables[0]).toEqual({
      oid: 16385, schemaName: 'public', tableName: 'users', relkind: 'r',
      relpages: 1000, estimatedRows: 50000, actualBytes: 8388608,
      pkColumn: 'id', pkType: 'int8', columns: [], generatedColumns: [],
    })
  })
  it('handles null PK', () => {
    const rows = [{ oid: 16390, schema_name: 'public', table_name: 'config', relkind: 'r', relpages: 1, estimated_rows: 5, actual_bytes: '8192', pk_column: null, pk_type: null }]
    const tables = parseTableRows(rows)
    expect(tables[0]!.pkColumn).toBeNull()
  })
})

describe('buildDdlDumpArgs', () => {
  it('builds args for full database', () => {
    const args = buildDdlDumpArgs('pg://h/db', '/out/ddl.dump', undefined, null, [])
    expect(args).toContain('--schema-only')
    expect(args).toContain('--format=custom')
    expect(args).toContain('--no-owner')
  })
  it('adds schema filter', () => {
    const args = buildDdlDumpArgs('pg://h/db', '/out/ddl.dump', 'public', null, [])
    expect(args).toContain('--schema=public')
  })
  it('adds snapshot', () => {
    const args = buildDdlDumpArgs('pg://h/db', '/out/ddl.dump', undefined, 'snap-123', [])
    expect(args).toContain('--snapshot=snap-123')
  })
})

describe('buildSequenceQuery', () => {
  it('builds query to fetch sequence values without filter', () => {
    const { text, values } = buildSequenceQuery(undefined)
    expect(text).toContain('pg_sequences')
    expect(text).toContain('last_value')
    expect(values).toEqual([])
  })
  it('builds parameterized query with schema filter', () => {
    const { text, values } = buildSequenceQuery('public')
    expect(text).toContain('schemaname = $1')
    expect(text).not.toContain("'public'")
    expect(values).toEqual(['public'])
  })
})
