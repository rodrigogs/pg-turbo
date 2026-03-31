import type { TableInfo, ChunkMeta, ChunkStrategy } from '../types/index.js'

export interface ChunkPlanOptions {
  splitThreshold: number; maxChunks: number; pgMajorVersion: number
  pkMin: number | null; pkMax: number | null
}

export function chunkFilePath(schema: string, table: string, index: number): string {
  return `data/${schema}.${table}/chunk_${index.toString().padStart(4, '0')}.copy.lz4`
}

export function planChunks(table: TableInfo, opts: ChunkPlanOptions): ChunkMeta[] {
  if (table.actualBytes < opts.splitThreshold) return [{ index: 0, file: chunkFilePath(table.schemaName, table.tableName, 0) }]
  if (table.pkColumn && opts.pkMin !== null && opts.pkMax !== null) return planPkRangeChunks(table, opts.pkMin, opts.pkMax, opts.splitThreshold, opts.maxChunks)
  if (opts.pgMajorVersion >= 14 && table.relpages > 0) return planCtidChunks(table, opts.splitThreshold, opts.maxChunks)
  return [{ index: 0, file: chunkFilePath(table.schemaName, table.tableName, 0) }]
}

function planPkRangeChunks(table: TableInfo, pkMin: number, pkMax: number, splitThreshold: number, maxChunks: number): ChunkMeta[] {
  const numChunks = Math.min(Math.ceil(table.actualBytes / splitThreshold), maxChunks)
  const range = pkMax - pkMin + 1
  const chunkSize = Math.ceil(range / numChunks)
  const chunks: ChunkMeta[] = []
  for (let i = 0; i < numChunks; i++) {
    const start = pkMin + i * chunkSize
    const end = Math.min(start + chunkSize - 1, pkMax)
    chunks.push({ index: i, file: chunkFilePath(table.schemaName, table.tableName, i), rangeStart: start, rangeEnd: end })
  }
  return chunks
}

function planCtidChunks(table: TableInfo, splitThreshold: number, maxChunks: number): ChunkMeta[] {
  const blockSize = 8192
  const pagesPerChunk = Math.ceil(splitThreshold / blockSize)
  const numChunks = Math.min(Math.ceil(table.relpages / pagesPerChunk), maxChunks)
  const chunks: ChunkMeta[] = []
  for (let i = 0; i < numChunks; i++) {
    const start = i * pagesPerChunk
    const isLast = i === numChunks - 1
    chunks.push({ index: i, file: chunkFilePath(table.schemaName, table.tableName, i), ctidStart: start, ...(isLast ? {} : { ctidEnd: start + pagesPerChunk }) })
  }
  return chunks
}

export function buildCopyQuery(table: TableInfo, chunk: ChunkMeta): string {
  const cols = table.columns.filter(c => !table.generatedColumns.includes(c)).map(c => `"${c}"`).join(', ')
  const qualifiedTable = `"${table.schemaName}"."${table.tableName}"`
  if (chunk.rangeStart !== undefined && chunk.rangeEnd !== undefined) {
    const pk = `"${table.pkColumn!}"`
    const nullClause = chunk.index === 0 ? `${pk} IS NULL OR ` : ''
    return `COPY (SELECT ${cols} FROM ${qualifiedTable} WHERE ${nullClause}${pk} >= ${chunk.rangeStart} AND ${pk} <= ${chunk.rangeEnd}) TO STDOUT`
  }
  if (chunk.ctidStart !== undefined) {
    const startClause = `ctid >= '(${chunk.ctidStart},0)'::tid`
    const endClause = chunk.ctidEnd !== undefined ? ` AND ctid < '(${chunk.ctidEnd},0)'::tid` : ''
    return `COPY (SELECT ${cols} FROM ${qualifiedTable} WHERE ${startClause}${endClause}) TO STDOUT`
  }
  return `COPY ${qualifiedTable} (${cols}) TO STDOUT`
}

export function chunkStrategy(table: TableInfo, opts: ChunkPlanOptions): ChunkStrategy {
  if (table.actualBytes < opts.splitThreshold) return 'none'
  if (table.pkColumn && opts.pkMin !== null && opts.pkMax !== null) return 'pk_range'
  if (opts.pgMajorVersion >= 14 && table.relpages > 0) return 'ctid_range'
  return 'none'
}
