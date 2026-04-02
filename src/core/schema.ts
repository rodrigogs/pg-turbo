import type { SequenceInfo, TableInfo } from '../types/index.js'

export function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}

export function buildTableDiscoveryQuery(schemaFilter: string | undefined): { text: string; values: unknown[] } {
  const schemaClause = schemaFilter
    ? 'n.nspname = $1'
    : `n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema'`
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
    WHERE c.relkind IN ('r', 'm') AND c.relpersistence IN ('p', 'u') AND ${schemaClause}
    ORDER BY pg_table_size(c.oid) DESC NULLS LAST`,
    values: schemaFilter ? [schemaFilter] : [],
  }
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
  oid: number
  schema_name: string
  table_name: string
  relkind: string
  relpages: number
  estimated_rows: number
  actual_bytes: string
  pk_column: string | null
  pk_type: string | null
}

export function parseTableRows(rows: TableRow[]): TableInfo[] {
  return rows.map((r) => ({
    oid: r.oid,
    schemaName: r.schema_name,
    tableName: r.table_name,
    relkind: r.relkind as 'r' | 'm',
    relpages: r.relpages,
    estimatedRows: parseInt(String(r.estimated_rows), 10) || 0,
    actualBytes: parseInt(r.actual_bytes, 10),
    pkColumn: r.pk_column,
    pkType: r.pk_type as TableInfo['pkType'],
    columns: [],
    generatedColumns: [],
  }))
}

/**
 * Sample rows at evenly-spaced PK points to measure byte distribution.
 * Uses a LATERAL join on generate_series so that:
 * - Exactly `targetRows` probes are made (no dependency on stale reltuples)
 * - Each probe uses the PK index (no full table scan)
 * - Results are already in PK order (no sort needed)
 * - octet_length(t::text) only runs on matched rows (not the whole table)
 *
 * octet_length(t::text) detoasts all values and measures their text representation,
 * closely approximating COPY TEXT output size. pg_column_size(t.*) only measures
 * on-disk tuple size (TOAST pointers instead of actual data).
 */
export function buildVolumeSampleQuery(
  schema: string,
  table: string,
  pkColumn: string,
  pkMin: number,
  pkMax: number,
  targetRows: number = 10_000,
): string {
  if (!Number.isFinite(pkMin) || !Number.isFinite(pkMax) || !Number.isFinite(targetRows)) {
    throw new Error('PK range and targetRows must be finite numbers')
  }
  const step = Math.max(1, Math.floor((pkMax - pkMin) / targetRows))
  const qt = `${quoteIdent(schema)}.${quoteIdent(table)}`
  const pk = quoteIdent(pkColumn)
  return `SELECT t.${pk} AS pk, octet_length(t::text) AS bytes
    FROM generate_series(${pkMin}::bigint, ${pkMax}::bigint, ${step}::bigint) AS s(probe)
    JOIN LATERAL (
      SELECT * FROM ${qt} WHERE ${pk} >= s.probe ORDER BY ${pk} LIMIT 1
    ) t ON true`
}

export function buildDdlDumpArgs(
  cs: string,
  outputPath: string,
  schemaFilter: string | undefined,
  snapshotId: string | null,
  extraArgs: string[],
): string[] {
  const args = [cs, '--schema-only', '--format=custom', '--no-owner', '--no-privileges', '--verbose', '-f', outputPath]
  if (schemaFilter) args.push(`--schema=${schemaFilter}`)
  if (snapshotId) args.push(`--snapshot=${snapshotId}`)
  args.push(...extraArgs)
  return args
}

export function buildSequenceQuery(schemaFilter: string | undefined): { text: string; values: unknown[] } {
  const schemaClause = schemaFilter
    ? 'schemaname = $1'
    : `schemaname NOT LIKE 'pg_%' AND schemaname <> 'information_schema'`
  return {
    text: `SELECT schemaname, sequencename, last_value, (last_value IS NOT NULL) AS is_called FROM pg_sequences WHERE ${schemaClause} ORDER BY schemaname, sequencename`,
    values: schemaFilter ? [schemaFilter] : [],
  }
}

export function parseSequenceRows(
  rows: Array<{ schemaname: string; sequencename: string; last_value: string | null; is_called: boolean }>,
): SequenceInfo[] {
  return rows
    .filter((r) => r.last_value !== null)
    .map((r) => ({
      schema: r.schemaname,
      name: r.sequencename,
      lastValue: parseInt(r.last_value ?? '0', 10),
      isCalled: r.is_called,
    }))
}
