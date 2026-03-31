// ts/src/core/copy-stream.ts
import { pipeline } from 'node:stream/promises'
import { createWriteStream, createReadStream } from 'node:fs'
import { mkdir, writeFile, unlink } from 'node:fs/promises'
import { dirname } from 'node:path'
import { to as copyTo, from as copyFrom } from 'pg-copy-streams'
import lz4 from 'lz4'
import type pg from 'pg'

export function chunkDoneMarker(chunkPath: string): string { return `${chunkPath}.done` }
export function chunkRestoredMarker(chunkPath: string): string { return `${chunkPath}.restored.done` }

export function buildRestoreCopyQuery(schema: string, table: string, columns: string[]): string {
  const cols = columns.map(c => `"${c}"`).join(', ')
  return `COPY "${schema}"."${table}" (${cols}) FROM STDIN`
}

export interface DumpChunkResult { rowCount: number; bytesWritten: number }

export async function dumpChunk(
  client: pg.Client, copyQuery: string, outputPath: string,
  onData?: (bytes: number) => void,
): Promise<DumpChunkResult> {
  await mkdir(dirname(outputPath), { recursive: true })
  const copyStream = client.query(copyTo(copyQuery))
  const compressor = lz4.createEncoderStream({ blockMaxSize: 4 * 1024 * 1024 })
  const fileStream = createWriteStream(outputPath)
  let bytesWritten = 0
  compressor.on('data', (chunk: Buffer) => { bytesWritten += chunk.length; onData?.(chunk.length) })
  await pipeline(copyStream, compressor, fileStream)
  await writeFile(chunkDoneMarker(outputPath), '', 'utf-8')
  return { rowCount: copyStream.rowCount, bytesWritten }
}

export async function restoreChunk(
  client: pg.Client, schema: string, table: string, columns: string[], inputPath: string,
): Promise<void> {
  const copyStream = client.query(copyFrom(buildRestoreCopyQuery(schema, table, columns)))
  const decompressor = lz4.createDecoderStream()
  const fileStream = createReadStream(inputPath)
  await pipeline(fileStream, decompressor, copyStream)
  await writeFile(chunkRestoredMarker(inputPath), '', 'utf-8')
}

export async function removePartialChunk(outputPath: string): Promise<void> {
  await unlink(outputPath).catch(() => {})
  await unlink(chunkDoneMarker(outputPath)).catch(() => {})
}
