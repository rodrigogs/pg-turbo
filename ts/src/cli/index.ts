// ts/src/cli/index.ts
import { Command } from 'commander'
import { parseDumpArgs, parseRestoreArgs } from './args.js'
import { runDump } from './dump.js'
import { runRestore } from './restore.js'

export function createProgram(): Command {
  const program = new Command()
    .name('pg-resilient')
    .description('Resilient PostgreSQL dump & restore with direct COPY protocol')
    .version('0.1.0')
    .enablePositionalOptions()

  program
    .command('dump')
    .description('Dump a PostgreSQL database')
    .allowUnknownOption()
    .passThroughOptions()
    .action(async (_options: unknown, cmd: Command) => {
      const opts = parseDumpArgs(cmd.parent!.args.slice(1))
      await runDump(opts)
    })

  program
    .command('restore')
    .description('Restore a PostgreSQL dump')
    .allowUnknownOption()
    .passThroughOptions()
    .action(async (_options: unknown, cmd: Command) => {
      const opts = parseRestoreArgs(cmd.parent!.args.slice(1))
      await runRestore(opts)
    })

  return program
}

export async function main(): Promise<void> {
  const program = createProgram()
  await program.parseAsync(process.argv)
}
