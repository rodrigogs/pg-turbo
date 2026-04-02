import * as net from 'node:net'
import { mkdtempSync, rmSync, existsSync, readFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { execSync } from 'node:child_process'
import pg from 'pg'
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { runDump } from '../../src/cli/dump.js'
import { isNetworkError } from '../../src/core/errors.js'
import type { DumpManifest } from '../../src/types/index.js'

const { Client } = pg
const COMPOSE = join(__dirname, 'docker-compose.yml')
const FIXTURES = join(__dirname, 'fixtures.sql')
const PG_PORT = 54399
const PG_HOST = '127.0.0.1'

/**
 * TCP proxy that forwards connections between clients and PostgreSQL.
 * Can kill all connections and stop/restart to simulate VPN drops.
 * Tracks bytes transferred and supports an onData callback so tests
 * can trigger a kill mid-stream based on actual data flow.
 */
class TcpProxy {
  server: net.Server | null = null
  private sockets: net.Socket[] = []
  port = 0
  /** Total bytes forwarded from server to client (across all connections) */
  totalServerToClientBytes = 0
  /** Callback invoked after each server->client chunk; can be used to trigger kill */
  onData: ((bytesTotal: number) => void) | null = null

  async start(listenPort?: number): Promise<number> {
    return new Promise((resolve, reject) => {
      this.server = net.createServer((clientSocket) => {
        const serverSocket = net.createConnection({ host: PG_HOST, port: PG_PORT })
        this.sockets.push(clientSocket, serverSocket)

        // client -> server: unthrottled (queries are small)
        clientSocket.pipe(serverSocket)

        // server -> client: track bytes and notify callback
        serverSocket.on('data', (chunk: Buffer) => {
          this.totalServerToClientBytes += chunk.length
          clientSocket.write(chunk)
          this.onData?.(this.totalServerToClientBytes)
        })

        const cleanup = () => {
          clientSocket.unpipe?.(serverSocket)
          serverSocket.unpipe?.(clientSocket)
          clientSocket.destroy()
          serverSocket.destroy()
          this.sockets = this.sockets.filter(s => s !== clientSocket && s !== serverSocket)
        }
        clientSocket.on('error', cleanup)
        serverSocket.on('error', cleanup)
        clientSocket.on('close', cleanup)
      })

      this.server.on('error', reject)
      this.server.listen(listenPort ?? 0, PG_HOST, () => {
        this.port = (this.server!.address() as net.AddressInfo).port
        resolve(this.port)
      })
    })
  }

  /** Kill ALL active connections -- simulates abrupt TCP reset */
  killAll(): void {
    for (const s of this.sockets) {
      s.destroy()
    }
    this.sockets = []
  }

  /** Stop accepting new connections AND kill existing ones */
  async shutdown(): Promise<void> {
    this.killAll()
    const srv = this.server
    this.server = null
    return new Promise((resolve) => {
      if (srv?.listening) {
        srv.close(() => resolve())
      } else {
        resolve()
      }
    })
  }

  /** Restart the proxy on the same port (shutdown first if needed) */
  async restart(): Promise<void> {
    if (this.server?.listening) {
      await this.shutdown()
    }
    await this.start(this.port)
  }

  async stop(): Promise<void> {
    await this.shutdown()
  }
}

async function waitForPg(maxMs = 30_000) {
  const deadline = Date.now() + maxMs
  while (Date.now() < deadline) {
    try {
      const c = new Client({ connectionString: `postgresql://test_admin@${PG_HOST}:${PG_PORT}/pg_turbo_test` })
      await c.connect()
      await c.end()
      return
    } catch {
      await new Promise(r => setTimeout(r, 500))
    }
  }
  throw new Error('PG not ready')
}

describe('connection recovery via TCP proxy', () => {
  let proxy: TcpProxy
  const tmpDirs: string[] = []

  function freshDir(): string {
    const d = mkdtempSync(join(tmpdir(), 'pgr-reconnect-'))
    tmpDirs.push(d)
    return d
  }

  beforeAll(async () => {
    await waitForPg()
    // Load fixtures if needed
    try {
      execSync(`psql "postgresql://test_admin@${PG_HOST}:${PG_PORT}/pg_turbo_test" -c "SELECT count(*) FROM public.users"`, { stdio: 'pipe' })
    } catch {
      execSync(`psql "postgresql://test_admin@${PG_HOST}:${PG_PORT}/pg_turbo_test" -f "${FIXTURES}"`, { stdio: 'pipe', timeout: 30_000 })
    }
    // Create a large table for the reconnect test
    const c = new Client({ connectionString: `postgresql://test_admin@${PG_HOST}:${PG_PORT}/pg_turbo_test` })
    await c.connect()
    await c.query(`
      CREATE TABLE IF NOT EXISTS public.big_data (
        id SERIAL PRIMARY KEY,
        payload TEXT NOT NULL
      )
    `)
    const { rows } = await c.query('SELECT count(*)::int AS cnt FROM public.big_data')
    if (rows[0].cnt < 500_000) {
      console.log('[TEST] Inserting 500k rows into big_data...')
      await c.query(`
        INSERT INTO public.big_data (payload)
        SELECT repeat('x', 200) || i::text
        FROM generate_series(1, 500000) AS i
      `)
      console.log('[TEST] big_data ready')
    }
    await c.end()

    proxy = new TcpProxy()
    await proxy.start()
    console.log(`[TEST] TCP proxy started on port ${proxy.port}`)
  }, 120_000)

  afterAll(async () => {
    await proxy?.stop()
    for (const d of tmpDirs) rmSync(d, { recursive: true, force: true })
  }, 30_000)

  it('isNetworkError recognises the error from a killed COPY stream', async () => {
    const connStr = `postgresql://test_admin@${PG_HOST}:${proxy.port}/pg_turbo_test`
    const client = new Client({ connectionString: connStr })
    client.on('error', () => {})

    await client.connect()
    const { to: pgCopyTo } = await import('pg-copy-streams')
    const { PassThrough } = await import('node:stream')
    const { pipeline } = await import('node:stream/promises')

    const copyStream = client.query(pgCopyTo('COPY (SELECT generate_series(1, 10000000)) TO STDOUT'))
    const sink = new PassThrough()
    sink.resume()

    setTimeout(() => proxy.killAll(), 200)

    let caughtError: Error | undefined
    try {
      await pipeline(copyStream, sink)
    } catch (err) {
      caughtError = err as Error
    }

    expect(caughtError).toBeDefined()
    console.log('=== ACTUAL ERROR FROM KILLED COPY ===')
    console.log('message:', caughtError!.message)
    console.log('code:', (caughtError as any).code)
    console.log('isNetworkError:', isNetworkError(caughtError))
    console.log('=====================================')

    expect(isNetworkError(caughtError)).toBe(true)

    await client.end().catch(() => {})
    await proxy.restart()
  }, 30_000)

  it('dump recovers after proxy kill and restart', async () => {
    const connStr = `postgresql://test_admin@${PG_HOST}:${proxy.port}/pg_turbo_test`
    const outDir = freshDir()

    // Use the onData callback to kill proxy after significant data flow,
    // ensuring the kill happens during active COPY streaming (not during
    // the discovery/DDL phases). The big_data table is ~120MB, so 50MB
    // means we're well into the COPY stream.
    let killed = false
    proxy.totalServerToClientBytes = 0
    proxy.onData = (totalBytes) => {
      if (!killed && totalBytes > 50_000_000) {
        killed = true
        console.log(`[TEST] Killing proxy after ${(totalBytes / 1e6).toFixed(1)}MB transferred`)
        proxy.shutdown()
        // Restart proxy after 3 seconds (simulating VPN recovery)
        setTimeout(async () => {
          console.log('[TEST] Restarting proxy...')
          proxy.onData = null
          await proxy.start(proxy.port)
          console.log('[TEST] Proxy restarted')
        }, 3_000)
      }
    }

    try {
      await runDump({
        dbname: connStr,
        output: outDir,
        jobs: 2,
        splitThreshold: 1_073_741_824,
        maxChunksPerTable: 64,
        retries: 5,
        retryDelay: 1,
        noSnapshot: true,
        noArchive: true,
        dryRun: false,
        compression: 'zstd',
        pgDumpArgs: [],
      })

      const manifestPath = join(outDir, 'manifest.json')
      expect(existsSync(manifestPath)).toBe(true)
      const manifest: DumpManifest = JSON.parse(readFileSync(manifestPath, 'utf-8'))
      expect(manifest.tables.length).toBeGreaterThan(0)

      // Verify all chunks completed
      for (const table of manifest.tables) {
        for (const chunk of table.chunks) {
          expect(existsSync(join(outDir, chunk.file))).toBe(true)
          expect(existsSync(join(outDir, `${chunk.file}.done`))).toBe(true)
        }
      }

      // The kill must have actually fired for this test to be meaningful
      expect(killed).toBe(true)
      console.log('[TEST] Dump completed successfully after connection recovery!')
    } finally {
      proxy.onData = null
    }
  }, 120_000)
})
