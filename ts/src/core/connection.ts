import pg from 'pg'
const { Client } = pg

const KEEPALIVE_PARAMS = [
  'keepalives=1', 'keepalives_idle=10', 'keepalives_interval=10',
  'keepalives_count=5', 'tcp_user_timeout=30000', 'connect_timeout=10',
].join('&')

export function sanitizeConnectionString(cs: string): string {
  return cs.replace(/:\/\/([^:]+):[^@]+@/, '://$1:***@').replace(/\?.*/, '')
}

export function extractDbName(cs: string): string {
  const match = cs.match(/\/([^/?]+)(?:\?.*)?$/)
  return match?.[1] ?? ''
}

export function cleanConnectionString(cs: string): string {
  const [base, query] = cs.split('?')
  if (!query) return base!
  const sslmode = query.split('&').find(p => p.toLowerCase().startsWith('sslmode='))
  return sslmode ? `${base}?${sslmode}` : base!
}

export function appendKeepaliveParams(cs: string): string {
  const separator = cs.includes('?') ? '&' : '?'
  return `${cs}${separator}${KEEPALIVE_PARAMS}`
}

export async function createSnapshotCoordinator(connectionString: string) {
  const client = new Client({ connectionString: appendKeepaliveParams(connectionString) })
  await client.connect()
  await client.query('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ')
  const { rows } = await client.query('SELECT pg_export_snapshot() AS snapshot_id')
  const snapshotId = rows[0].snapshot_id as string
  return {
    snapshotId, client,
    close: async () => { await client.query('COMMIT').catch(() => {}); await client.end() },
  }
}

export async function createWorkerClient(connectionString: string, snapshotId: string | null) {
  const client = new Client({ connectionString: appendKeepaliveParams(connectionString) })
  await client.connect()
  if (snapshotId) {
    await client.query('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ')
    await client.query(`SET TRANSACTION SNAPSHOT '${snapshotId}'`)
  }
  return client
}

export async function testConnection(connectionString: string): Promise<string> {
  const client = new Client({ connectionString: appendKeepaliveParams(connectionString) })
  await client.connect()
  try {
    const { rows } = await client.query('SELECT version() AS version')
    return rows[0].version as string
  } finally { await client.end() }
}

export async function isReadReplica(connectionString: string): Promise<boolean> {
  const client = new Client({ connectionString: appendKeepaliveParams(connectionString) })
  await client.connect()
  try {
    const { rows } = await client.query('SELECT pg_is_in_recovery() AS is_replica')
    return rows[0].is_replica as boolean
  } finally { await client.end() }
}

export async function releaseWorkerClient(client: InstanceType<typeof Client>): Promise<void> {
  await client.query('COMMIT').catch(() => {})
  await client.end()
}

export async function destroyClient(client: InstanceType<typeof Client>): Promise<void> {
  await client.end().catch(() => {})
}
