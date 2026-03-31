// ts/src/core/manifest.ts
import { readFile, writeFile, mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import type { DumpManifest } from '../types/index.js'

const MANIFEST_FILENAME = 'manifest.json'
const CURRENT_VERSION = 1

export async function writeManifest(outputDir: string, manifest: DumpManifest): Promise<void> {
  await mkdir(outputDir, { recursive: true })
  await writeFile(join(outputDir, MANIFEST_FILENAME), JSON.stringify(manifest, null, 2) + '\n', 'utf-8')
}

export async function readManifest(inputDir: string): Promise<DumpManifest> {
  const raw = await readFile(join(inputDir, MANIFEST_FILENAME), 'utf-8')
  const parsed = JSON.parse(raw) as DumpManifest
  if (parsed.version !== CURRENT_VERSION) {
    throw new Error(`Unsupported manifest version ${parsed.version} (expected ${CURRENT_VERSION})`)
  }
  return parsed
}
