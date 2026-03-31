// ts/tests/unit/schema.test.ts
import { describe, it, expect } from 'vitest'
import { buildTableDiscoveryQuery, buildGeneratedColumnsQuery, parseTableRows, buildDdlDumpArgs, buildSequenceQuery } from '../../src/core/schema.js'

describe('buildTableDiscoveryQuery', () => {
  it('builds query without schema filter', () => {
    const sql = buildTableDiscoveryQuery(undefined)
    expect(sql).toContain('pg_catalog.pg_class')
    expect(sql).toContain("n.nspname NOT LIKE 'pg_%'")
    expect(sql).not.toContain('n.nspname =')
  })
  it('builds query with schema filter', () => {
    const sql = buildTableDiscoveryQuery('public')
    expect(sql).toContain("n.nspname = 'public'")
  })
})

describe('buildGeneratedColumnsQuery', () => {
  it('returns query with oid parameter', () => {
    const sql = buildGeneratedColumnsQuery()
    expect(sql).toContain('pg_attribute')
    expect(sql).toContain('attgenerated')
    expect(sql).toContain('$1')
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
  it('builds query to fetch sequence values', () => {
    const sql = buildSequenceQuery(undefined)
    expect(sql).toContain('pg_sequences')
    expect(sql).toContain('last_value')
  })
})
