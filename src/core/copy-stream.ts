// ts/src/core/copy-stream.ts

import { createReadStream, createWriteStream } from 'node:fs'
import { stat, unlink, writeFile } from 'node:fs/promises'
import { Transform, type TransformCallback } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import lz4 from 'lz4'
import type pg from 'pg'
import { from as copyFrom, to as copyTo } from 'pg-copy-streams'
import { CompressStream, DecompressStream } from 'zstd-napi'
import type { Compression } from '../types/index.js'
import { quoteIdent } from './schema.js'

export function createCompressor(compression: Compression): Transform {
  if (compression === 'lz4') return lz4.createEncoderStream({ blockMaxSize: 4 * 1024 * 1024 })
  return new CompressStream()
}

export function createDecompressor(compression: Compression): Transform {
  if (compression === 'lz4') return lz4.createDecoderStream()
  return new DecompressStream()
}

/** Passthrough transform that reports cumulative bytes to a callback. */
export function createByteCounter(onBytes: (totalBytes: number) => void): Transform {
  let total = 0
  return new Transform({
    transform(chunk: Buffer, _encoding: string, cb: TransformCallback) {
      total += chunk.length
      onBytes(total)
      cb(null, chunk)
    },
  })
}

/** Passthrough transform that counts rows (\n bytes) in COPY TEXT output. */
export function createRowCounter(onRows: (totalRows: number) => void): Transform {
  let total = 0
  return new Transform({
    transform(chunk: Buffer, _encoding: string, cb: TransformCallback) {
      for (let i = 0; i < chunk.length; i++) {
        if (chunk[i] === 0x0a) total++
      }
      onRows(total)
      cb(null, chunk)
    },
  })
}

export function chunkDoneMarker(chunkPath: string): string {
  return `${chunkPath}.done`
}

export function buildRestoreCopyQuery(schema: string, table: string, columns: string[]): string {
  const cols = columns.map((c) => quoteIdent(c)).join(', ')
  return `COPY ${quoteIdent(schema)}.${quoteIdent(table)} (${cols}) FROM STDIN`
}

export interface DumpChunkResult {
  rowCount: number
  bytesWritten: number
}

export async function dumpChunk(
  client: pg.Client,
  copyQuery: string,
  outputPath: string,
  compression: Compression,
  onProgress?: (rowsProcessed: number) => void,
): Promise<DumpChunkResult> {
  const copyStream = client.query(copyTo(copyQuery, { highWaterMark: 256 * 1024 }))
  const compressor = createCompressor(compression)
  const fileStream = createWriteStream(outputPath, { highWaterMark: 256 * 1024 })
  if (onProgress) {
    const counter = createRowCounter(onProgress)
    await pipeline(copyStream, counter, compressor, fileStream)
  } else {
    await pipeline(copyStream, compressor, fileStream)
  }
  const { size } = await stat(outputPath)
  await writeFile(chunkDoneMarker(outputPath), '', 'utf-8')
  return { rowCount: copyStream.rowCount, bytesWritten: size }
}

// ── Restore progress tracking ────────────────────────────────────────────────
// Completion markers live in the target DB so they are atomic with the COPY data.
// COMMIT persists both the rows AND the progress record, or neither.

const PROGRESS_SCHEMA = '_pg_resilient'
const PROGRESS_TABLE = `${PROGRESS_SCHEMA}._progress`

export async function ensureProgressTable(client: pg.Client): Promise<void> {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${PROGRESS_SCHEMA}`)
  await client.query(`CREATE TABLE IF NOT EXISTS ${PROGRESS_TABLE} (
    chunk_key TEXT PRIMARY KEY,
    completed_at TIMESTAMPTZ DEFAULT now()
  )`)
}

export async function fetchCompletedChunks(client: pg.Client): Promise<Set<string>> {
  const { rows } = await client.query(`SELECT chunk_key FROM ${PROGRESS_TABLE}`)
  return new Set(rows.map((r: { chunk_key: string }) => r.chunk_key))
}

export async function resetProgress(client: pg.Client): Promise<void> {
  await client.query(`TRUNCATE ${PROGRESS_TABLE}`)
}

export async function dropProgressTable(client: pg.Client): Promise<void> {
  await client.query(`DROP SCHEMA IF EXISTS ${PROGRESS_SCHEMA} CASCADE`)
}

export async function restoreChunk(
  client: pg.Client,
  schema: string,
  table: string,
  columns: string[],
  inputPath: string,
  chunkKey: string,
  compression: Compression,
  onProgress?: (bytesRead: number) => void,
): Promise<void> {
  await client.query('SET synchronous_commit = off')
  await client.query('BEGIN')
  try {
    const copyStream = client.query(copyFrom(buildRestoreCopyQuery(schema, table, columns)))
    const decompressor = createDecompressor(compression)
    const fileStream = createReadStream(inputPath, { highWaterMark: 256 * 1024 })
    if (onProgress) {
      const counter = createByteCounter(onProgress)
      await pipeline(fileStream, counter, decompressor, copyStream)
    } else {
      await pipeline(fileStream, decompressor, copyStream)
    }
    // Atomic: data + completion marker in the same transaction.
    // Either both persist (COMMIT) or neither does (ROLLBACK / crash).
    await client.query(`INSERT INTO ${PROGRESS_TABLE} (chunk_key) VALUES ($1) ON CONFLICT DO NOTHING`, [chunkKey])
    await client.query('COMMIT')
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {})
    throw err
  }
}

export async function removePartialChunk(outputPath: string): Promise<void> {
  await unlink(outputPath).catch(() => {})
  await unlink(chunkDoneMarker(outputPath)).catch(() => {})
}
