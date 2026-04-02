import { Command } from 'commander'
import type { Compression, DumpOptions, RestoreOptions } from '../types/index.js'

export function parseSize(value: string): number {
  const match = value.match(/^(\d+(?:\.\d+)?)\s*(KB|MB|GB|TB)?$/i)
  if (!match) throw new Error(`Invalid size: ${value}`)
  const num = parseFloat(match[1] ?? '0')
  const unit = (match[2] ?? '').toUpperCase()
  const multipliers: Record<string, number> = { '': 1, KB: 1024, MB: 1024 ** 2, GB: 1024 ** 3, TB: 1024 ** 4 }
  return Math.floor(num * (multipliers[unit] ?? 1))
}

export function parseDumpArgs(argv: string[]): DumpOptions {
  // Split args at '--' separator
  const separatorIdx = argv.indexOf('--')
  const ourArgs = separatorIdx === -1 ? argv : argv.slice(0, separatorIdx)
  const passthroughArgs = separatorIdx === -1 ? [] : argv.slice(separatorIdx + 1)

  const cmd = new Command()
    .requiredOption('-d, --dbname <cs>', 'PostgreSQL connection string')
    .requiredOption('--output <dir>', 'Output directory')
    .option('-n, --schema <name>', 'Dump only this schema')
    .option('-j, --jobs <n>', 'Parallel workers', '4')
    .option('--split-threshold <size>', 'Chunk tables larger than this', '1GB')
    .option('--max-chunks-per-table <n>', 'Max chunks per table', '32')
    .option('--retries <n>', 'Max retries per chunk', '5')
    .option('--retry-delay <s>', 'Base retry delay (seconds)', '5')
    .option('--compression <type>', 'Compression algorithm (zstd, lz4)', 'zstd')
    .option('--no-snapshot', 'Skip snapshot')
    .option('--no-archive', 'Skip .pgt archive packaging')
    .option('--dry-run', 'Preview without dumping')
    .allowUnknownOption(false)
  cmd.parse(ourArgs, { from: 'user' })
  const opts = cmd.opts()
  return {
    dbname: opts.dbname as string,
    output: opts.output as string,
    schema: opts.schema as string | undefined,
    jobs: parseInt(opts.jobs as string, 10),
    splitThreshold: parseSize(opts.splitThreshold as string),
    maxChunksPerTable: parseInt(opts.maxChunksPerTable as string, 10),
    retries: parseInt(opts.retries as string, 10),
    retryDelay: parseInt(opts.retryDelay as string, 10),
    noSnapshot: opts.snapshot === false,
    noArchive: opts.archive === false,
    dryRun: opts.dryRun === true,
    compression: (opts.compression as Compression) === 'lz4' ? 'lz4' : 'zstd',
    pgDumpArgs: passthroughArgs,
  }
}

export function parseRestoreArgs(argv: string[]): RestoreOptions {
  // Split args at '--' separator
  const separatorIdx = argv.indexOf('--')
  const ourArgs = separatorIdx === -1 ? argv : argv.slice(0, separatorIdx)
  const passthroughArgs = separatorIdx === -1 ? [] : argv.slice(separatorIdx + 1)

  const cmd = new Command()
    .requiredOption('-d, --dbname <cs>', 'PostgreSQL connection string')
    .requiredOption('--input <dir|file.pgt>', 'Input directory or .pgt archive')
    .option('-n, --schema <name>', 'Filter schema')
    .option('-t, --table <name>', 'Restore single table')
    .option('-j, --jobs <n>', 'Parallel workers', '4')
    .option('-c, --clean', 'DROP + CREATE schema')
    .option('-a, --data-only', 'Skip DDL')
    .option('--retries <n>', 'Max retries', '5')
    .option('--retry-delay <s>', 'Retry delay', '5')
    .option('--dry-run', 'Preview')
    .allowUnknownOption(false)
  cmd.parse(ourArgs, { from: 'user' })
  const opts = cmd.opts()
  return {
    dbname: opts.dbname as string,
    input: opts.input as string,
    schema: opts.schema as string | undefined,
    table: opts.table as string | undefined,
    jobs: parseInt(opts.jobs as string, 10),
    clean: opts.clean === true,
    dataOnly: opts.dataOnly === true,
    retries: parseInt(opts.retries as string, 10),
    retryDelay: parseInt(opts.retryDelay as string, 10),
    dryRun: opts.dryRun === true,
    pgRestoreArgs: passthroughArgs,
  }
}
