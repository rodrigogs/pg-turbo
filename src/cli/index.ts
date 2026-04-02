import { parseDumpArgs, parseRestoreArgs } from './args.js'
import { runDump } from './dump.js'
import { runRestore } from './restore.js'

function printHelp(): void {
  console.log(`pg-resilient — Resilient PostgreSQL dump & restore with direct COPY protocol

Usage:
  pg-resilient dump  -d <connection_string> --output <dir> [options]
  pg-resilient restore -d <connection_string> --input <dir> [options]

Commands:
  dump      Dump a PostgreSQL database
  restore   Restore a PostgreSQL dump

Run "pg-resilient dump --help" or "pg-resilient restore --help" for command options.`)
}

export async function main(): Promise<void> {
  // Simple subcommand routing — pass all args after the subcommand to our parsers
  const args = process.argv.slice(2)
  const subcommand = args[0]
  const subArgs = args.slice(1)

  switch (subcommand) {
    case 'dump':
      await runDump(parseDumpArgs(subArgs))
      break
    case 'restore':
      await runRestore(parseRestoreArgs(subArgs))
      break
    default:
      printHelp()
      break
  }
}
