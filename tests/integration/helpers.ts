import pg from 'pg'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { dirname } from 'node:path'
import type { DumpOptions, RestoreOptions } from '../../src/types/index.js'

const { Client } = pg
const __dirname = dirname(fileURLToPath(import.meta.url))

export async function loadFixtures(connUri: string): Promise<void> {
  const client = new Client({ connectionString: connUri })
  await client.connect()
  const sql = readFileSync(join(__dirname, 'fixtures.sql'), 'utf-8')
  await client.query(sql)
  await client.end()
}

export async function query(connUri: string, sql: string): Promise<string> {
  const client = new Client({ connectionString: connUri })
  await client.connect()
  const { rows } = await client.query(sql)
  await client.end()
  return rows[0] ? (Object.values(rows[0])[0] as string) : ''
}

export async function createDatabase(adminUri: string, dbName: string): Promise<void> {
  const client = new Client({ connectionString: adminUri })
  await client.connect()
  // Terminate existing connections first
  await client
    .query(
      `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`,
      [dbName],
    )
    .catch(() => {})
  await client.query(`DROP DATABASE IF EXISTS "${dbName}"`)
  await client.query(`CREATE DATABASE "${dbName}"`)
  await client.end()
}

export function defaultDumpOpts(
  connUri: string,
  output: string,
  overrides: Partial<DumpOptions> = {},
): DumpOptions {
  return {
    dbname: connUri,
    output,
    jobs: 2,
    splitThreshold: 1_073_741_824,
    maxChunksPerTable: 64,
    retries: 2,
    retryDelay: 1,
    noSnapshot: true,
    noArchive: true,
    dryRun: false,
    compression: 'zstd',
    pgDumpArgs: [],
    ...overrides,
  }
}

export function defaultRestoreOpts(
  connUri: string,
  input: string,
  overrides: Partial<RestoreOptions> = {},
): RestoreOptions {
  return {
    dbname: connUri,
    input,
    jobs: 2,
    clean: false,
    dataOnly: false,
    retries: 2,
    retryDelay: 1,
    dryRun: false,
    pgRestoreArgs: [],
    ...overrides,
  }
}
