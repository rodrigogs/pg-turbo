// ts/src/core/manifest.ts
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import type { DumpManifest } from '../types/index.js'

const MANIFEST_FILENAME = 'manifest.json'
const CURRENT_VERSION = 1

export async function writeManifest(outputDir: string, manifest: DumpManifest): Promise<void> {
  await mkdir(outputDir, { recursive: true })
  await writeFile(join(outputDir, MANIFEST_FILENAME), `${JSON.stringify(manifest, null, 2)}\n`, 'utf-8')
}

function assertPathWithinDir(dir: string, filePath: string): void {
  const resolved = resolve(dir, filePath)
  if (!resolved.startsWith(`${resolve(dir)}/`)) {
    throw new Error(`Path traversal detected in manifest: ${filePath}`)
  }
}

export async function readManifest(inputDir: string): Promise<DumpManifest> {
  const raw = await readFile(join(inputDir, MANIFEST_FILENAME), 'utf-8')
  const parsed = JSON.parse(raw) as DumpManifest
  if (parsed.version !== CURRENT_VERSION) {
    throw new Error(`Unsupported manifest version ${parsed.version} (expected ${CURRENT_VERSION})`)
  }
  for (const table of parsed.tables) {
    // Coerce numeric fields that may be stored as strings (pg bigint → JS string)
    table.estimatedRows = Number(table.estimatedRows) || 0
    table.estimatedBytes = Number(table.estimatedBytes) || 0
    for (const chunk of table.chunks) {
      assertPathWithinDir(inputDir, chunk.file)
      if (chunk.estimatedRows != null) chunk.estimatedRows = Number(chunk.estimatedRows) || 0
      if (chunk.estimatedBytes != null) chunk.estimatedBytes = Number(chunk.estimatedBytes) || 0
    }
  }
  return parsed
}
