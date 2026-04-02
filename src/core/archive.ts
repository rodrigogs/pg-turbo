import { createReadStream, createWriteStream } from 'node:fs'
import { stat } from 'node:fs/promises'
import { pipeline } from 'node:stream/promises'
import * as tar from 'tar'
import { CompressStream, DecompressStream } from 'zstd-napi'

export function isPgtArchive(path: string): boolean {
  return path.endsWith('.pgt')
}

/**
 * Package a dump directory into a single .pgt archive (tar + zstd).
 * Uses low zstd compression level since chunk data is already compressed.
 */
export async function createArchive(sourceDir: string, outputPath: string): Promise<number> {
  const tarStream = tar.create({ gzip: false, cwd: sourceDir, portable: true }, ['.'])
  const compressor = new CompressStream({ compressionLevel: 1 })
  const fileStream = createWriteStream(outputPath)
  await pipeline(tarStream, compressor, fileStream)
  const { size } = await stat(outputPath)
  return size
}

/** Extract a .pgt archive into a target directory. */
export async function extractArchive(archivePath: string, targetDir: string): Promise<void> {
  const fileStream = createReadStream(archivePath)
  const decompressor = new DecompressStream()
  const extractor = tar.extract({ cwd: targetDir })
  await pipeline(fileStream, decompressor, extractor)
}
