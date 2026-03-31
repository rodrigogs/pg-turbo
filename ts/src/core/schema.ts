import type { TableInfo, SequenceInfo } from '../types/index.js'

export function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}

export function buildTableDiscoveryQuery(schemaFilter: string | undefined): { text: string; values: unknown[] } {
  if (schemaFilter) {
    return {
      text: `
    SELECT c.oid, n.nspname AS schema_name, c.relname AS table_name, c.relkind,
      c.relpages, c.reltuples::bigint AS estimated_rows,
      pg_table_size(c.oid)::text AS actual_bytes,
      pkeys.attname AS pk_column, pkeys.typname AS pk_type
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    LEFT JOIN LATERAL (
      SELECT a.attname, t.typname
      FROM pg_index x
      JOIN pg_class i ON i.oid = x.indexrelid
      JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = x.indkey[0]
      JOIN pg_type t ON t.oid = a.atttypid
      WHERE x.indrelid = c.oid AND (x.indisprimary OR x.indisunique)
        AND array_length(x.indkey::integer[], 1) = 1
        AND a.atttypid IN ('smallint'::regtype, 'int'::regtype, 'bigint'::regtype)
      ORDER BY NOT x.indisprimary, NOT x.indisunique LIMIT 1
    ) AS pkeys ON true
    WHERE c.relkind IN ('r', 'm') AND c.relpersistence IN ('p', 'u') AND n.nspname = $1
    ORDER BY pg_table_size(c.oid) DESC NULLS LAST`,
      values: [schemaFilter],
    }
  }
  return {
    text: `
    SELECT c.oid, n.nspname AS schema_name, c.relname AS table_name, c.relkind,
      c.relpages, c.reltuples::bigint AS estimated_rows,
      pg_table_size(c.oid)::text AS actual_bytes,
      pkeys.attname AS pk_column, pkeys.typname AS pk_type
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    LEFT JOIN LATERAL (
      SELECT a.attname, t.typname
      FROM pg_index x
      JOIN pg_class i ON i.oid = x.indexrelid
      JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = x.indkey[0]
      JOIN pg_type t ON t.oid = a.atttypid
      WHERE x.indrelid = c.oid AND (x.indisprimary OR x.indisunique)
        AND array_length(x.indkey::integer[], 1) = 1
        AND a.atttypid IN ('smallint'::regtype, 'int'::regtype, 'bigint'::regtype)
      ORDER BY NOT x.indisprimary, NOT x.indisunique LIMIT 1
    ) AS pkeys ON true
    WHERE c.relkind IN ('r', 'm') AND c.relpersistence IN ('p', 'u') AND n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema'
    ORDER BY pg_table_size(c.oid) DESC NULLS LAST`,
    values: [],
  }
}

export function buildGeneratedColumnsQuery(): string {
  return `SELECT attname FROM pg_attribute WHERE attrelid = $1 AND attgenerated <> '' AND NOT attisdropped AND attnum > 0`
}

export function buildColumnsQuery(): string {
  return `SELECT attname FROM pg_attribute WHERE attrelid = $1 AND NOT attisdropped AND attnum > 0 AND attgenerated = '' ORDER BY attnum`
}

export function buildBatchColumnsQuery(): string {
  return `
    SELECT attrelid::int AS oid, attname, (attgenerated <> '') AS is_generated
    FROM pg_attribute
    WHERE attrelid = ANY($1::oid[])
      AND NOT attisdropped AND attnum > 0
    ORDER BY attrelid, attnum
  `
}

interface TableRow {
  oid: number; schema_name: string; table_name: string; relkind: string
  relpages: number; estimated_rows: number; actual_bytes: string
  pk_column: string | null; pk_type: string | null
}

export function parseTableRows(rows: TableRow[]): TableInfo[] {
  return rows.map(r => ({
    oid: r.oid, schemaName: r.schema_name, tableName: r.table_name,
    relkind: r.relkind as 'r' | 'm', relpages: r.relpages,
    estimatedRows: r.estimated_rows, actualBytes: parseInt(r.actual_bytes, 10),
    pkColumn: r.pk_column, pkType: r.pk_type as TableInfo['pkType'],
    columns: [], generatedColumns: [],
  }))
}

export function buildDdlDumpArgs(cs: string, outputPath: string, schemaFilter: string | undefined, snapshotId: string | null, extraArgs: string[]): string[] {
  const args = [cs, '--schema-only', '--format=custom', '--no-owner', '--no-privileges', '--verbose', '-f', outputPath]
  if (schemaFilter) args.push(`--schema=${schemaFilter}`)
  if (snapshotId) args.push(`--snapshot=${snapshotId}`)
  args.push(...extraArgs)
  return args
}

export function buildSequenceQuery(schemaFilter: string | undefined): { text: string; values: unknown[] } {
  if (schemaFilter) {
    return {
      text: `SELECT schemaname, sequencename, last_value, (last_value IS NOT NULL) AS is_called FROM pg_sequences WHERE schemaname = $1 ORDER BY schemaname, sequencename`,
      values: [schemaFilter],
    }
  }
  return {
    text: `SELECT schemaname, sequencename, last_value, (last_value IS NOT NULL) AS is_called FROM pg_sequences WHERE schemaname NOT LIKE 'pg_%' AND schemaname <> 'information_schema' ORDER BY schemaname, sequencename`,
    values: [],
  }
}

export function parseSequenceRows(rows: Array<{ schemaname: string; sequencename: string; last_value: string | null; is_called: boolean }>): SequenceInfo[] {
  return rows.filter(r => r.last_value !== null).map(r => ({
    schema: r.schemaname, name: r.sequencename,
    lastValue: parseInt(r.last_value!, 10), isCalled: r.is_called,
  }))
}
