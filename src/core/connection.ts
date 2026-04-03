import pg from 'pg'
import { isTransientError } from './errors.js'

const { Client } = pg

const KEEPALIVE_PARAMS = [
  'keepalives=1',
  'keepalives_idle=10',
  'keepalives_interval=10',
  'keepalives_count=5',
  'tcp_user_timeout=30000',
  'connect_timeout=10',
].join('&')

export function sanitizeConnectionString(cs: string): string {
  return cs.replace(/:\/\/([^:]+):[^@]+@/, '://$1:***@').replace(/\?.*/, '')
}

export function extractDbName(cs: string): string {
  const match = cs.match(/\/([^/?]+)(?:\?.*)?$/)
  return match?.[1] ?? ''
}

export function cleanConnectionString(cs: string): string {
  const [base = '', query] = cs.split('?')
  if (!query) return base
  const sslmode = query.split('&').find((p) => p.toLowerCase().startsWith('sslmode='))
  return sslmode ? `${base}?${sslmode}` : base
}

export function appendKeepaliveParams(cs: string): string {
  const [, existingQuery] = cs.split('?')
  const existing = new Set(
    (existingQuery ?? '')
      .split('&')
      .filter(Boolean)
      .map((p) => p.split('=')[0] ?? ''),
  )
  const newParams = KEEPALIVE_PARAMS.split('&').filter((p) => !existing.has(p.split('=')[0] ?? ''))
  if (newParams.length === 0) return cs
  const sep = existingQuery ? '&' : '?'
  return `${cs}${sep}${newParams.join('&')}`
}

/** Create a Client with TCP keepalive enabled and a no-op error handler so socket
 *  errors (ETIMEDOUT, ECONNRESET) don't crash the process.
 *  NOTE: The connection string keepalive params (keepalives=1, keepalives_idle, etc.)
 *  are libpq parameters that node-postgres IGNORES. We must set keepAlive via the
 *  Client constructor options for them to actually take effect on the TCP socket. */
function newClient(connectionString: string): InstanceType<typeof Client> {
  const client = new Client({
    connectionString: appendKeepaliveParams(connectionString),
    keepAlive: true,
    keepAliveInitialDelayMillis: 10_000, // Start probing after 10s idle
    connectionTimeoutMillis: 10_000, // Fail connect() after 10s (connect_timeout in CS is ignored by pg)
  })
  client.on('error', () => {})
  return client
}

/** Max attempts per connectWithRetry call. Kept small so the QUEUE retry loop
 *  (which tracks visible retry count in the dashboard) drives the outer retry. */
const CONNECT_MAX_ATTEMPTS = 3
const CONNECT_BASE_DELAY_MS = 2_000

/** Try to connect with a few fast retries. Throws on failure so the caller
 *  (queue worker) can re-queue and show retry progress in the dashboard.
 *  Permanent errors (auth failure, bad config) throw immediately. */
async function connectWithRetry(connectionString: string): Promise<InstanceType<typeof Client>> {
  let lastError: unknown
  for (let attempt = 0; attempt < CONNECT_MAX_ATTEMPTS; attempt++) {
    const client = newClient(connectionString)
    try {
      await client.connect()
      return client
    } catch (err) {
      lastError = err
      destroyClient(client)
      // Permanent errors: throw immediately (no point retrying auth failures)
      if (!isTransientError(err)) throw err
      if (attempt < CONNECT_MAX_ATTEMPTS - 1) {
        const delay = Math.min(CONNECT_BASE_DELAY_MS * 2 ** attempt, 10_000) + Math.random() * 1_000
        await new Promise((r) => setTimeout(r, delay))
      }
    }
  }
  throw lastError
}

export async function createClient(connectionString: string): Promise<InstanceType<typeof Client>> {
  return connectWithRetry(connectionString)
}

export async function createSnapshotCoordinator(connectionString: string) {
  const client = await connectWithRetry(connectionString)
  await client.query('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ')
  const { rows } = await client.query('SELECT pg_export_snapshot() AS snapshot_id')
  const snapshotId = rows[0].snapshot_id as string
  if (!/^[\dA-F]+-[\dA-F]+(-\d+)?$/i.test(snapshotId)) {
    await client.query('ROLLBACK').catch(() => {})
    await client.end()
    throw new Error(`Invalid snapshot ID format: ${snapshotId}`)
  }
  return {
    snapshotId,
    client,
    close: async () => {
      await client.query('COMMIT').catch(() => {})
      await client.end()
    },
  }
}

export async function createWorkerClient(connectionString: string, snapshotId: string | null) {
  const client = await connectWithRetry(connectionString)
  if (snapshotId) {
    if (!/^[\dA-F]+-[\dA-F]+(-\d+)?$/i.test(snapshotId)) {
      await client.end().catch(() => {})
      throw new Error(`Invalid snapshot ID format: ${snapshotId}`)
    }
    try {
      await client.query('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ')
      await client.query(`SET TRANSACTION SNAPSHOT '${snapshotId}'`)
    } catch (err) {
      await client.end().catch(() => {})
      throw err
    }
  }
  return client
}

export async function testConnection(connectionString: string): Promise<string> {
  const client = await connectWithRetry(connectionString)
  try {
    const { rows } = await client.query('SELECT version() AS version')
    return rows[0].version as string
  } finally {
    await client.end()
  }
}

export async function isReadReplica(connectionString: string): Promise<boolean> {
  const client = await connectWithRetry(connectionString)
  try {
    const { rows } = await client.query('SELECT pg_is_in_recovery() AS is_replica')
    return rows[0].is_replica as boolean
  } finally {
    await client.end()
  }
}

export async function releaseWorkerClient(client: InstanceType<typeof Client>): Promise<void> {
  await client.query('COMMIT').catch(() => {})
  const timeout = new Promise<void>((resolve) => setTimeout(resolve, 2_000))
  await Promise.race([client.end().catch(() => {}), timeout])
}

export function destroyClient(client: InstanceType<typeof Client>): void {
  // Forcibly destroy the underlying socket without waiting. client.end() can hang
  // if the 'end' event already fired (dead connection), blocking the worker forever.
  try {
    ;(client as any).connection?.stream?.destroy()
  } catch {
    // ignore — stream may already be destroyed
  }
  client.end().catch(() => {})
}
