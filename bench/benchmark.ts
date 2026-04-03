#!/usr/bin/env tsx
// Benchmark: pg-turbo vs raw pg_dump

import { execFileSync, execSync } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { runDump } from '../src/cli/dump.js'

const CS = 'postgresql://test_admin@localhost:54399/benchmark_test'

async function timeIt(_label: string, fn: () => Promise<void> | void): Promise<number> {
  const start = performance.now()
  await fn()
  const ms = performance.now() - start
  return ms
}

function dirSize(dir: string): number {
  const output = execSync(`du -sb ${dir} 2>/dev/null || du -sk ${dir} | awk '{print $1*1024}'`).toString().trim()
  return parseInt(output.split('\t')[0] ?? '0', 10)
}

async function main() {
  console.log('=== pg-turbo vs pg_dump benchmark ===\n')

  // Show DB size
  const dbSize = execSync(`psql "${CS}" -tAc "SELECT pg_database_size(current_database())"`).toString().trim()
  console.log(`Database size: ${(parseInt(dbSize, 10) / 1024 / 1024).toFixed(1)} MB\n`)

  // Show table sizes
  execSync(
    `psql "${CS}" -c "SELECT schemaname || '.' || relname AS table, pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) AS size FROM pg_stat_user_tables ORDER BY pg_total_relation_size(schemaname || '.' || relname) DESC"`,
  )
    .toString()
    .split('\n')
    .forEach((l) => {
      console.log(l)
    })

  const results: Array<{ label: string; timeMs: number; sizeBytes: number }> = []

  // Benchmark 1: pg_dump custom format (single process)
  {
    const dir = mkdtempSync(join(tmpdir(), 'bench-pgdump-'))
    const outFile = join(dir, 'dump.custom')
    const ms = await timeIt('pg_dump -Fc', () => {
      execFileSync('pg_dump', [CS, '--format=custom', '--no-owner', '--no-privileges', '-f', outFile], {
        stdio: 'pipe',
      })
    })
    const size = dirSize(dir)
    results.push({ label: 'pg_dump -Fc (single)', timeMs: ms, sizeBytes: size })
    rmSync(dir, { recursive: true, force: true })
  }

  // Benchmark 2: pg_dump directory format with -j 4
  {
    const dir = mkdtempSync(join(tmpdir(), 'bench-pgdump-parallel-'))
    const outDir = join(dir, 'out')
    const ms = await timeIt('pg_dump -Fd -j4', () => {
      execFileSync('pg_dump', [CS, '--format=directory', '--jobs=4', '--no-owner', '--no-privileges', '-f', outDir], {
        stdio: 'pipe',
      })
    })
    const size = dirSize(dir)
    results.push({ label: 'pg_dump -Fd -j4', timeMs: ms, sizeBytes: size })
    rmSync(dir, { recursive: true, force: true })
  }

  // Benchmark 3: pg-turbo with 1 worker
  {
    const dir = mkdtempSync(join(tmpdir(), 'bench-resilient-1-'))
    const ms = await timeIt('pg-turbo -j1', async () => {
      await runDump({
        dbname: CS,
        output: dir,
        jobs: 1,
        splitThreshold: 1024 * 1024 * 1024,
        maxChunksPerTable: 32,
        retries: 1,
        retryDelay: 1,
        noSnapshot: false,
        noArchive: true,
        dryRun: false,
        compression: 'zstd',
        pgDumpArgs: [],
      })
    })
    const size = dirSize(dir)
    results.push({ label: 'pg-turbo -j1', timeMs: ms, sizeBytes: size })
    rmSync(dir, { recursive: true, force: true })
  }

  // Benchmark 4: pg-turbo with 4 workers
  {
    const dir = mkdtempSync(join(tmpdir(), 'bench-resilient-4-'))
    const ms = await timeIt('pg-turbo -j4', async () => {
      await runDump({
        dbname: CS,
        output: dir,
        jobs: 4,
        splitThreshold: 1024 * 1024 * 1024,
        maxChunksPerTable: 32,
        retries: 1,
        retryDelay: 1,
        noSnapshot: false,
        noArchive: true,
        dryRun: false,
        compression: 'zstd',
        pgDumpArgs: [],
      })
    })
    const size = dirSize(dir)
    results.push({ label: 'pg-turbo -j4', timeMs: ms, sizeBytes: size })
    rmSync(dir, { recursive: true, force: true })
  }

  // Print results table
  console.log('\n\n=== RESULTS ===\n')
  console.log('| Method                    |    Time |   Output Size |  Speed |')
  console.log('|---------------------------|---------|---------------|--------|')
  for (const r of results) {
    const secs = (r.timeMs / 1000).toFixed(2)
    const sizeMB = (r.sizeBytes / 1024 / 1024).toFixed(1)
    const speedMBs = (r.sizeBytes / 1024 / 1024 / (r.timeMs / 1000)).toFixed(1)
    console.log(
      `| ${r.label.padEnd(25)} | ${secs.padStart(7)}s | ${sizeMB.padStart(8)} MB | ${speedMBs.padStart(6)} MB/s |`,
    )
  }

  const baseline = results[0]?.timeMs ?? 1
  console.log('\n=== RELATIVE PERFORMANCE ===\n')
  for (const r of results) {
    const ratio = (baseline / r.timeMs).toFixed(2)
    console.log(
      `${r.label.padEnd(25)} ${ratio}x ${r.timeMs < baseline ? 'FASTER' : r.timeMs > baseline ? 'slower' : '(baseline)'}`,
    )
  }
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
