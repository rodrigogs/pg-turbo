import * as net from 'node:net'
import { PassThrough } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import pg from 'pg'
import { to as copyTo } from 'pg-copy-streams'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { isNetworkError } from '../../src/core/errors.js'

const { Client } = pg

// Simple TCP proxy that forwards between client and PostgreSQL
class TcpProxy {
  private server: net.Server | null = null
  private connections: net.Socket[] = []
  public port = 0

  constructor(
    private targetHost: string,
    private targetPort: number,
  ) {}

  async start(): Promise<number> {
    return new Promise((resolve) => {
      this.server = net.createServer((clientSocket) => {
        const serverSocket = net.createConnection(this.targetPort, this.targetHost)
        this.connections.push(clientSocket, serverSocket)

        clientSocket.pipe(serverSocket)
        serverSocket.pipe(clientSocket)

        clientSocket.on('error', () => serverSocket.destroy())
        serverSocket.on('error', () => clientSocket.destroy())
        clientSocket.on('close', () => serverSocket.destroy())
        serverSocket.on('close', () => clientSocket.destroy())
      })
      this.server.listen(0, '127.0.0.1', () => {
        this.port = (this.server?.address() as net.AddressInfo).port
        resolve(this.port)
      })
    })
  }

  // Kill all active connections to simulate VPN drop
  dropConnections() {
    for (const sock of this.connections) {
      sock.destroy()
    }
    this.connections = []
  }

  // Stop accepting new connections (simulate VPN down)
  pause() {
    this.server?.close()
    this.server = null
  }

  // Restart accepting connections (simulate VPN back)
  async resume(): Promise<void> {
    return new Promise((resolve) => {
      this.server = net.createServer((clientSocket) => {
        const serverSocket = net.createConnection(this.targetPort, this.targetHost)
        this.connections.push(clientSocket, serverSocket)
        clientSocket.pipe(serverSocket)
        serverSocket.pipe(clientSocket)
        clientSocket.on('error', () => serverSocket.destroy())
        serverSocket.on('error', () => clientSocket.destroy())
        clientSocket.on('close', () => serverSocket.destroy())
        serverSocket.on('close', () => clientSocket.destroy())
      })
      this.server.listen(this.port, '127.0.0.1', () => resolve())
    })
  }

  async stop() {
    this.dropConnections()
    return new Promise<void>((resolve) => {
      if (this.server) {
        this.server.close(() => resolve())
      } else {
        resolve()
      }
    })
  }
}

const PG_PORT = 54399
const PG_CONN = `postgresql://test_admin@localhost:${PG_PORT}/pg_turbo_test`

describe('connection recovery with TCP proxy', () => {
  let proxy: TcpProxy
  let pgAvailable = false

  beforeAll(async () => {
    try {
      const client = new Client({ connectionString: PG_CONN })
      await client.connect()
      await client.end()
      pgAvailable = true
    } catch {
      pgAvailable = false
    }
    if (pgAvailable) {
      proxy = new TcpProxy('127.0.0.1', PG_PORT)
      await proxy.start()
    }
  }, 10_000)

  afterAll(async () => {
    if (proxy) await proxy.stop()
  })

  it('isNetworkError detects connection drop errors from pg COPY stream', async () => {
    if (!pgAvailable) return

    const connStr = `postgresql://test_admin@127.0.0.1:${proxy.port}/pg_turbo_test`
    const client = new Client({ connectionString: connStr })
    client.on('error', () => {}) // suppress unhandled

    await client.connect()

    // Start a COPY stream that generates enough data to not finish before we kill the proxy
    const copyStream = client.query(copyTo('COPY (SELECT generate_series(1, 10000000)) TO STDOUT'))
    const sink = new PassThrough()
    // Drain the sink to keep backpressure from stalling the stream
    sink.resume()

    // Kill the proxy connection mid-stream
    setTimeout(() => proxy.dropConnections(), 50)

    try {
      await pipeline(copyStream, sink)
      expect.fail('Should have thrown')
    } catch (err) {
      console.log('Error type:', (err as any).constructor.name)
      console.log('Error message:', (err as Error).message)
      console.log('Error code:', (err as any).code)
      console.log('isNetworkError:', isNetworkError(err))

      // The error from a killed connection during COPY must be classified as a network error
      expect(isNetworkError(err)).toBe(true)
    }

    await client.end().catch(() => {})
    // Restart proxy for next test
    await proxy.stop()
    proxy = new TcpProxy('127.0.0.1', PG_PORT)
    await proxy.start()
  }, 30_000)

  it('isNetworkError detects errors through compressor pipeline (real dumpChunk path)', async () => {
    if (!pgAvailable) return

    const { createCompressor } = await import('../../src/core/copy-stream.js')
    const { createWriteStream } = await import('node:fs')
    const { mkdtemp } = await import('node:fs/promises')
    const { tmpdir } = await import('node:os')
    const { join } = await import('node:path')

    const connStr = `postgresql://test_admin@127.0.0.1:${proxy.port}/pg_turbo_test`
    const client = new Client({ connectionString: connStr })
    client.on('error', () => {})
    await client.connect()

    const tmpDir = await mkdtemp(join(tmpdir(), 'pg-turbo-test-'))
    const outPath = join(tmpDir, 'test.copy.zst')

    const copyStream = client.query(copyTo('COPY (SELECT generate_series(1, 10000000)) TO STDOUT'))
    const compressor = createCompressor('zstd')
    const fileStream = createWriteStream(outPath)

    setTimeout(() => proxy.dropConnections(), 50)

    try {
      await pipeline(copyStream, compressor, fileStream)
      expect.fail('Should have thrown')
    } catch (err) {
      console.log('Pipeline+compressor error type:', (err as any).constructor.name)
      console.log('Pipeline+compressor error message:', (err as Error).message)
      console.log('Pipeline+compressor error code:', (err as any).code)
      console.log('isNetworkError:', isNetworkError(err))

      // Even through the compressor pipeline, must be detected as network error
      expect(isNetworkError(err)).toBe(true)
    }

    await client.end().catch(() => {})
    await proxy.stop()
    proxy = new TcpProxy('127.0.0.1', PG_PORT)
    await proxy.start()
  }, 30_000)

  it('isNetworkError detects ERR_STREAM_PREMATURE_CLOSE (pipeline wrapper error)', () => {
    // Node's stream.pipeline can wrap errors as ERR_STREAM_PREMATURE_CLOSE
    // when a stream is destroyed without an error before it finishes
    const err = new Error('Premature close')
    ;(err as any).code = 'ERR_STREAM_PREMATURE_CLOSE'
    expect(isNetworkError(err)).toBe(true)
  })

  it('isNetworkError detects ERR_STREAM_DESTROYED', () => {
    const err = new Error('Cannot call write after a stream was destroyed')
    ;(err as any).code = 'ERR_STREAM_DESTROYED'
    expect(isNetworkError(err)).toBe(true)
  })

  it('createClient reconnects after proxy drop and resume', async () => {
    if (!pgAvailable) return

    const { createClient } = await import('../../src/core/connection.js')
    const connStr = `postgresql://test_admin@127.0.0.1:${proxy.port}/pg_turbo_test`

    // First connection works
    const client1 = await createClient(connStr)
    const { rows } = await client1.query('SELECT 1 AS ok')
    expect(rows[0].ok).toBe(1)
    await client1.end()

    // Kill proxy and stop accepting connections
    proxy.dropConnections()
    proxy.pause()

    // Try to connect — should retry in background
    const connectPromise = createClient(connStr)

    // After 2 seconds, bring proxy back
    setTimeout(async () => {
      await proxy.resume()
    }, 2000)

    // Should eventually connect (within ~15s)
    const client2 = await connectPromise
    const { rows: rows2 } = await client2.query('SELECT 1 AS ok')
    expect(rows2[0].ok).toBe(1)
    await client2.end()
  }, 30_000)
})
