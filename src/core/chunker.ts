import type { ChunkJob, ChunkMeta, ChunkStrategy, TableInfo } from '../types/index.js'
import { quoteIdent } from './schema.js'

export interface RowSample {
  pk: number
  bytes: number
}

export interface ChunkPlanOptions {
  splitThreshold: number
  maxChunks: number
  pgMajorVersion: number
  pkMin: number | null
  pkMax: number | null
  /** Sampled rows sorted by PK with per-row byte sizes. Enables volume-balanced chunking. */
  volumeSamples?: RowSample[]
}

/** Per-chunk estimated bytes, falling back to uniform distribution for old manifests. */
export function chunkEstimatedBytes(job: ChunkJob): number {
  return job.chunk.estimatedBytes ?? Math.round(job.table.estimatedBytes / job.table.chunks.length)
}

/** Per-chunk estimated rows, falling back to uniform distribution for old manifests. */
export function chunkEstimatedRows(job: ChunkJob): number {
  return job.chunk.estimatedRows ?? Math.round(job.table.estimatedRows / job.table.chunks.length)
}

/** Sanitize a name for safe use in filesystem paths */
function safeName(name: string): string {
  // biome-ignore lint/suspicious/noControlCharactersInRegex: intentional control char stripping for path sanitization
  return name.replace(/[/\\<>:"|?*\x00-\x1f]/g, '_')
}

export function chunkFilePath(schema: string, table: string, index: number): string {
  return `data/${safeName(schema)}.${safeName(table)}/chunk_${index.toString().padStart(4, '0')}.copy.lz4`
}

export function planChunks(table: TableInfo, opts: ChunkPlanOptions): ChunkMeta[] {
  const singleChunk: ChunkMeta = {
    index: 0,
    file: chunkFilePath(table.schemaName, table.tableName, 0),
    estimatedBytes: table.actualBytes,
    estimatedRows: table.estimatedRows,
  }
  if (table.actualBytes < opts.splitThreshold) return [singleChunk]
  if (table.pkColumn && opts.pkMin !== null && opts.pkMax !== null) {
    if (opts.volumeSamples && opts.volumeSamples.length > 0) {
      return planVolumeBalancedChunks(
        table,
        opts.pkMin,
        opts.pkMax,
        opts.volumeSamples,
        opts.splitThreshold,
        opts.maxChunks,
      )
    }
    return planPkRangeChunks(table, opts.pkMin, opts.pkMax, opts.splitThreshold, opts.maxChunks)
  }
  if (opts.pgMajorVersion >= 14 && table.relpages > 0) return planCtidChunks(table, opts.splitThreshold, opts.maxChunks)
  return [singleChunk]
}

function planPkRangeChunks(
  table: TableInfo,
  pkMin: number,
  pkMax: number,
  splitThreshold: number,
  maxChunks: number,
): ChunkMeta[] {
  const numChunks = Math.min(Math.ceil(table.actualBytes / splitThreshold), maxChunks)
  const range = pkMax - pkMin + 1
  const chunkSize = Math.ceil(range / numChunks)
  const bytesPerChunk = Math.round(table.actualBytes / numChunks)
  const rowsPerChunk = Math.round(table.estimatedRows / numChunks)
  const chunks: ChunkMeta[] = []
  for (let i = 0; i < numChunks; i++) {
    const start = pkMin + i * chunkSize
    const end = Math.min(start + chunkSize - 1, pkMax)
    const isLast = i === numChunks - 1
    chunks.push({
      index: i,
      file: chunkFilePath(table.schemaName, table.tableName, i),
      rangeStart: start,
      rangeEnd: end,
      estimatedBytes: isLast ? Math.max(0, table.actualBytes - bytesPerChunk * (numChunks - 1)) : bytesPerChunk,
      estimatedRows: isLast ? Math.max(0, table.estimatedRows - rowsPerChunk * (numChunks - 1)) : rowsPerChunk,
    })
  }
  return chunks
}

/** Split a PK-indexed table into chunks with roughly equal data volume using sampled row sizes. */
function planVolumeBalancedChunks(
  table: TableInfo,
  pkMin: number,
  pkMax: number,
  samples: RowSample[],
  splitThreshold: number,
  maxChunks: number,
): ChunkMeta[] {
  const numChunks = Math.min(Math.ceil(table.actualBytes / splitThreshold), maxChunks)
  if (numChunks <= 1)
    return [
      {
        index: 0,
        file: chunkFilePath(table.schemaName, table.tableName, 0),
        rangeStart: pkMin,
        rangeEnd: pkMax,
        estimatedBytes: table.actualBytes,
        estimatedRows: table.estimatedRows,
      },
    ]

  // Compute cumulative size from sorted samples
  const totalSampleBytes = samples.reduce((s, r) => s + r.bytes, 0)
  const targetPerChunk = totalSampleBytes / numChunks

  // Scale factors: convert sample-space stats → table-level estimates
  const rowScale = table.estimatedRows / samples.length
  const byteScale = table.actualBytes / totalSampleBytes

  const chunks: ChunkMeta[] = []
  let cumBytes = 0
  let chunkStart = pkMin
  let chunkSampleBytes = 0
  let chunkSampleCount = 0

  for (const sample of samples) {
    cumBytes += sample.bytes
    chunkSampleBytes += sample.bytes
    chunkSampleCount++
    const targetBoundary = (chunks.length + 1) * targetPerChunk
    if (cumBytes >= targetBoundary && chunks.length < numChunks - 1) {
      chunks.push({
        index: chunks.length,
        file: chunkFilePath(table.schemaName, table.tableName, chunks.length),
        rangeStart: chunkStart,
        rangeEnd: sample.pk,
        estimatedBytes: Math.round(chunkSampleBytes * byteScale),
        estimatedRows: Math.round(chunkSampleCount * rowScale),
      })
      chunkStart = sample.pk + 1
      chunkSampleBytes = 0
      chunkSampleCount = 0
    }
  }

  // Final chunk gets the remainder so totals always add up to table estimates
  const accountedBytes = chunks.reduce((s, c) => s + (c.estimatedBytes ?? 0), 0)
  const accountedRows = chunks.reduce((s, c) => s + (c.estimatedRows ?? 0), 0)
  chunks.push({
    index: chunks.length,
    file: chunkFilePath(table.schemaName, table.tableName, chunks.length),
    rangeStart: chunkStart,
    rangeEnd: pkMax,
    estimatedBytes: Math.max(0, table.actualBytes - accountedBytes),
    estimatedRows: Math.max(0, table.estimatedRows - accountedRows),
  })

  return chunks
}

function planCtidChunks(table: TableInfo, splitThreshold: number, maxChunks: number): ChunkMeta[] {
  const blockSize = 8192
  const pagesPerChunk = Math.ceil(splitThreshold / blockSize)
  const numChunks = Math.min(Math.ceil(table.relpages / pagesPerChunk), maxChunks)
  const chunks: ChunkMeta[] = []
  let accountedBytes = 0
  let accountedRows = 0
  for (let i = 0; i < numChunks; i++) {
    const start = i * pagesPerChunk
    const isLast = i === numChunks - 1
    const pagesInChunk = isLast ? table.relpages - start : pagesPerChunk
    const frac = pagesInChunk / table.relpages
    const chunkBytes = isLast ? Math.max(0, table.actualBytes - accountedBytes) : Math.round(table.actualBytes * frac)
    const chunkRows = isLast ? Math.max(0, table.estimatedRows - accountedRows) : Math.round(table.estimatedRows * frac)
    accountedBytes += chunkBytes
    accountedRows += chunkRows
    chunks.push({
      index: i,
      file: chunkFilePath(table.schemaName, table.tableName, i),
      ctidStart: start,
      ...(isLast ? {} : { ctidEnd: start + pagesPerChunk }),
      estimatedBytes: chunkBytes,
      estimatedRows: chunkRows,
    })
  }
  return chunks
}

export function buildCopyQuery(table: TableInfo, chunk: ChunkMeta): string {
  const cols = table.columns
    .filter((c) => !table.generatedColumns.includes(c))
    .map((c) => quoteIdent(c))
    .join(', ')
  const qualifiedTable = `${quoteIdent(table.schemaName)}.${quoteIdent(table.tableName)}`
  if (chunk.rangeStart !== undefined && chunk.rangeEnd !== undefined && table.pkColumn) {
    const pk = quoteIdent(table.pkColumn)
    const nullClause = chunk.index === 0 ? `${pk} IS NULL OR ` : ''
    return `COPY (SELECT ${cols} FROM ${qualifiedTable} WHERE ${nullClause}${pk} >= ${chunk.rangeStart} AND ${pk} <= ${chunk.rangeEnd}) TO STDOUT`
  }
  if (chunk.ctidStart !== undefined) {
    const startClause = `ctid >= '(${chunk.ctidStart},0)'::tid`
    const endClause = chunk.ctidEnd !== undefined ? ` AND ctid < '(${chunk.ctidEnd},0)'::tid` : ''
    return `COPY (SELECT ${cols} FROM ${qualifiedTable} WHERE ${startClause}${endClause}) TO STDOUT`
  }
  // Materialized views require COPY (SELECT ...) form — COPY matview TO STDOUT is not supported
  if (table.relkind === 'm') {
    return `COPY (SELECT ${cols} FROM ${qualifiedTable}) TO STDOUT`
  }
  return `COPY ${qualifiedTable} (${cols}) TO STDOUT`
}

export function chunkStrategy(table: TableInfo, opts: ChunkPlanOptions): ChunkStrategy {
  if (table.actualBytes < opts.splitThreshold) return 'none'
  if (table.pkColumn && opts.pkMin !== null && opts.pkMax !== null) return 'pk_range'
  if (opts.pgMajorVersion >= 14 && table.relpages > 0) return 'ctid_range'
  return 'none'
}
