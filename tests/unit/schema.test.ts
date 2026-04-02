// ts/tests/unit/schema.test.ts
import { describe, expect, it } from 'vitest'
import {
  buildBatchColumnsQuery,
  buildDdlDumpArgs,
  buildSequenceQuery,
  buildTableDiscoveryQuery,
  buildVolumeSampleQuery,
  parseSequenceRows,
  parseTableRows,
  quoteIdent,
} from '../../src/core/schema.js'

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
    const rows = [
      {
        oid: 16385,
        schema_name: 'public',
        table_name: 'users',
        relkind: 'r',
        relpages: 1000,
        estimated_rows: 50000,
        actual_bytes: '8388608',
        pk_column: 'id',
        pk_type: 'int8',
      },
    ]
    const tables = parseTableRows(rows)
    expect(tables).toHaveLength(1)
    expect(tables[0]).toEqual({
      oid: 16385,
      schemaName: 'public',
      tableName: 'users',
      relkind: 'r',
      relpages: 1000,
      estimatedRows: 50000,
      actualBytes: 8388608,
      pkColumn: 'id',
      pkType: 'int8',
      columns: [],
      generatedColumns: [],
    })
  })
  it('handles null PK', () => {
    const rows = [
      {
        oid: 16390,
        schema_name: 'public',
        table_name: 'config',
        relkind: 'r',
        relpages: 1,
        estimated_rows: 5,
        actual_bytes: '8192',
        pk_column: null,
        pk_type: null,
      },
    ]
    const tables = parseTableRows(rows)
    expect(tables[0]?.pkColumn).toBeNull()
  })
  it('coerces NaN estimatedRows to 0', () => {
    const rows = [
      {
        oid: 16390,
        schema_name: 'public',
        table_name: 'config',
        relkind: 'r',
        relpages: 1,
        estimated_rows: NaN,
        actual_bytes: '8192',
        pk_column: null,
        pk_type: null,
      },
    ]
    const tables = parseTableRows(rows)
    expect(tables[0].estimatedRows).toBe(0)
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
  it('appends extra args', () => {
    const args = buildDdlDumpArgs('pg://h/db', '/out/ddl.dump', undefined, null, ['--no-comments', '--verbose'])
    expect(args).toContain('--no-comments')
    expect(args).toContain('--verbose')
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

describe('buildVolumeSampleQuery', () => {
  it('builds LATERAL generate_series query with correct step', () => {
    const sql = buildVolumeSampleQuery('public', 'users', 'id', 1, 10_000_000)
    expect(sql).toContain('generate_series(')
    expect(sql).toContain('octet_length(t::text)')
    expect(sql).toContain('JOIN LATERAL')
    expect(sql).toContain('LIMIT 1')
    // step = floor((10M - 1) / 10000) = 999
    expect(sql).toContain('999::bigint')
  })
  it('uses minimum step of 1 for small ranges', () => {
    const sql = buildVolumeSampleQuery('public', 'tiny', 'id', 1, 100)
    // step = max(1, floor(99 / 10000)) = 1
    expect(sql).toContain(', 1::bigint)')
  })
  it('quotes identifiers', () => {
    const sql = buildVolumeSampleQuery('my schema', 'my"table', 'pk col', 1, 100_000)
    expect(sql).toContain('"my schema"."my""table"')
    expect(sql).toContain('"pk col"')
  })
})

describe('parseSequenceRows', () => {
  it('parses sequence rows with valid last_value', () => {
    const rows = [{ schemaname: 'public', sequencename: 'users_id_seq', last_value: '100', is_called: true }]
    const result = parseSequenceRows(rows)
    expect(result).toEqual([{ schema: 'public', name: 'users_id_seq', lastValue: 100, isCalled: true }])
  })

  it('filters out null last_value', () => {
    const rows = [{ schemaname: 'public', sequencename: 'unused_seq', last_value: null, is_called: false }]
    expect(parseSequenceRows(rows)).toEqual([])
  })

  it('parses sequence with zero last_value', () => {
    const rows = [{ schemaname: 'public', sequencename: 'zero_seq', last_value: '0', is_called: false }]
    const result = parseSequenceRows(rows)
    expect(result).toEqual([{ schema: 'public', name: 'zero_seq', lastValue: 0, isCalled: false }])
  })

  it('handles mixed rows with null and non-null last_value', () => {
    const rows = [
      { schemaname: 'public', sequencename: 'active_seq', last_value: '42', is_called: true },
      { schemaname: 'public', sequencename: 'unused_seq', last_value: null, is_called: false },
      { schemaname: 'app', sequencename: 'orders_seq', last_value: '9999', is_called: true },
    ]
    const result = parseSequenceRows(rows)
    expect(result).toHaveLength(2)
    expect(result[0]).toEqual({ schema: 'public', name: 'active_seq', lastValue: 42, isCalled: true })
    expect(result[1]).toEqual({ schema: 'app', name: 'orders_seq', lastValue: 9999, isCalled: true })
  })
})
