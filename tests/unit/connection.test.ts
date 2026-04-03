import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const mockQuery = vi.fn()
const mockConnect = vi.fn()
const mockEnd = vi.fn()
const mockOn = vi.fn()

vi.mock('pg', () => {
  return {
    default: {
      Client: class MockClient {
        query = mockQuery
        connect = mockConnect
        end = mockEnd
        on = mockOn
      },
    },
  }
})

import {
  appendKeepaliveParams,
  cleanConnectionString,
  createClient,
  createSnapshotCoordinator,
  createWorkerClient,
  destroyClient,
  extractDbName,
  isReadReplica,
  releaseWorkerClient,
  sanitizeConnectionString,
  testConnection,
} from '../../src/core/connection.js'

describe('sanitizeConnectionString', () => {
  it('masks password', () => {
    expect(sanitizeConnectionString('postgresql://user:secret@host/db')).toBe('postgresql://user:***@host/db')
  })
  it('handles no password', () => {
    expect(sanitizeConnectionString('postgresql://user@host/db')).toBe('postgresql://user@host/db')
  })
  it('strips query params', () => {
    expect(sanitizeConnectionString('postgresql://user:pass@host/db?sslmode=require')).toBe(
      'postgresql://user:***@host/db',
    )
  })
})

describe('extractDbName', () => {
  it('extracts database name', () => {
    expect(extractDbName('postgresql://user:pass@host/mydb')).toBe('mydb')
  })
  it('strips query params', () => {
    expect(extractDbName('postgresql://user:pass@host/mydb?sslmode=require')).toBe('mydb')
  })
  it('handles port', () => {
    expect(extractDbName('postgresql://user:pass@host:5432/mydb')).toBe('mydb')
  })
  it('returns empty string when no database name found', () => {
    // The regex /\/([^/?]+)(?:\?.*)?$/ matches the last path segment after a /
    // A URL without a trailing path has no match
    expect(extractDbName('')).toBe('')
  })
})

describe('cleanConnectionString', () => {
  it('strips GUI params but keeps sslmode', () => {
    expect(cleanConnectionString('postgresql://u:p@h/db?statusColor=red&sslmode=require&env=staging')).toBe(
      'postgresql://u:p@h/db?sslmode=require',
    )
  })
  it('strips all params when no sslmode', () => {
    expect(cleanConnectionString('postgresql://u:p@h/db?statusColor=red&env=staging')).toBe('postgresql://u:p@h/db')
  })
  it('returns as-is when no query params', () => {
    expect(cleanConnectionString('postgresql://u:p@h/db')).toBe('postgresql://u:p@h/db')
  })
})

describe('appendKeepaliveParams', () => {
  it('appends keepalive params to clean URL', () => {
    const result = appendKeepaliveParams('postgresql://u:p@h/db')
    expect(result).toContain('keepalives=1')
    expect(result).toContain('keepalives_idle=10')
    expect(result).toContain('connect_timeout=10')
  })
  it('appends to URL with existing params', () => {
    const result = appendKeepaliveParams('postgresql://u:p@h/db?sslmode=require')
    expect(result).toContain('sslmode=require')
    expect(result).toContain('keepalives=1')
  })
  it('does not override existing keepalive params', () => {
    const result = appendKeepaliveParams('postgresql://u:p@h/db?keepalives_idle=60&connect_timeout=30')
    expect(result).toContain('keepalives_idle=60') // preserved
    expect(result).toContain('connect_timeout=30') // preserved
    expect(result).not.toMatch(/keepalives_idle=10/) // not overridden
    expect(result).not.toMatch(/connect_timeout=10/) // not overridden
    expect(result).toContain('keepalives=1') // added (wasn't present)
  })
  it('returns URL unchanged when all keepalive params already exist', () => {
    const allParams =
      'keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=5&tcp_user_timeout=30000&connect_timeout=10'
    const url = `postgresql://u:p@h/db?${allParams}`
    const result = appendKeepaliveParams(url)
    expect(result).toBe(url)
  })
})

describe('createClient', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockConnect.mockResolvedValue(undefined)
  })

  it('connects and returns a client', async () => {
    const client = await createClient('postgresql://u:p@h/db')
    expect(mockConnect).toHaveBeenCalledTimes(1)
    expect(client).toBeDefined()
    expect(client.query).toBe(mockQuery)
  })
})

describe('createSnapshotCoordinator', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockConnect.mockResolvedValue(undefined)
  })

  it('begins transaction and exports snapshot', async () => {
    mockQuery
      .mockResolvedValueOnce(undefined) // BEGIN
      .mockResolvedValueOnce({ rows: [{ snapshot_id: '00000003-1B' }] }) // pg_export_snapshot
    const coord = await createSnapshotCoordinator('postgresql://u:p@h/db')
    expect(coord.snapshotId).toBe('00000003-1B')
    expect(mockQuery).toHaveBeenCalledWith('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ')
  })

  it('throws on invalid snapshot ID format', async () => {
    mockQuery
      .mockResolvedValueOnce(undefined) // BEGIN
      .mockResolvedValueOnce({ rows: [{ snapshot_id: 'invalid-format!' }] })
      .mockResolvedValueOnce(undefined) // ROLLBACK
    mockEnd.mockResolvedValue(undefined)
    await expect(createSnapshotCoordinator('postgresql://u:p@h/db')).rejects.toThrow('Invalid snapshot ID format')
  })

  it('accepts 3-part snapshot ID format', async () => {
    mockQuery
      .mockResolvedValueOnce(undefined) // BEGIN
      .mockResolvedValueOnce({ rows: [{ snapshot_id: '00000003-1B-5' }] })
    const coord = await createSnapshotCoordinator('postgresql://u:p@h/db')
    expect(coord.snapshotId).toBe('00000003-1B-5')
  })

  it('close commits and ends client', async () => {
    mockQuery
      .mockResolvedValueOnce(undefined) // BEGIN
      .mockResolvedValueOnce({ rows: [{ snapshot_id: 'AABB1122-33' }] })
    const coord = await createSnapshotCoordinator('postgresql://u:p@h/db')
    mockQuery.mockResolvedValue(undefined)
    mockEnd.mockResolvedValue(undefined)
    await coord.close()
    expect(mockQuery).toHaveBeenCalledWith('COMMIT')
    expect(mockEnd).toHaveBeenCalled()
  })
})

describe('createWorkerClient', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockConnect.mockResolvedValue(undefined)
  })

  it('sets snapshot when provided', async () => {
    mockQuery.mockResolvedValue(undefined)
    await createWorkerClient('postgresql://u:p@h/db', '00000003-1B')
    expect(mockQuery).toHaveBeenCalledWith('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ')
    expect(mockQuery).toHaveBeenCalledWith("SET TRANSACTION SNAPSHOT '00000003-1B'")
  })

  it('skips snapshot when null', async () => {
    await createWorkerClient('postgresql://u:p@h/db', null)
    expect(mockQuery).not.toHaveBeenCalled()
  })

  it('cleans up client when SET TRANSACTION fails', async () => {
    mockQuery
      .mockResolvedValueOnce(undefined) // BEGIN
      .mockRejectedValueOnce(new Error('snapshot expired'))
    mockEnd.mockResolvedValue(undefined)
    await expect(createWorkerClient('postgresql://u:p@h/db', 'ABC-123')).rejects.toThrow('snapshot expired')
    expect(mockEnd).toHaveBeenCalled()
  })

  it('rejects invalid snapshot ID format', async () => {
    mockEnd.mockResolvedValue(undefined)
    await expect(createWorkerClient('postgresql://u:p@h/db', 'invalid; DROP TABLE')).rejects.toThrow(
      'Invalid snapshot ID format',
    )
    expect(mockEnd).toHaveBeenCalled()
    // Should never have issued any queries
    expect(mockQuery).not.toHaveBeenCalled()
  })
})

describe('testConnection', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockConnect.mockResolvedValue(undefined)
    mockEnd.mockResolvedValue(undefined)
  })

  it('returns version string', async () => {
    mockQuery.mockResolvedValueOnce({ rows: [{ version: 'PostgreSQL 16.2' }] })
    const version = await testConnection('postgresql://u:p@h/db')
    expect(version).toBe('PostgreSQL 16.2')
    expect(mockEnd).toHaveBeenCalled()
  })
})

describe('isReadReplica', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockConnect.mockResolvedValue(undefined)
    mockEnd.mockResolvedValue(undefined)
  })

  it('returns true for replica', async () => {
    mockQuery.mockResolvedValueOnce({ rows: [{ is_replica: true }] })
    expect(await isReadReplica('postgresql://u:p@h/db')).toBe(true)
  })

  it('returns false for primary', async () => {
    mockQuery.mockResolvedValueOnce({ rows: [{ is_replica: false }] })
    expect(await isReadReplica('postgresql://u:p@h/db')).toBe(false)
  })
})

describe('releaseWorkerClient', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('commits and ends', async () => {
    mockQuery.mockResolvedValue(undefined)
    mockEnd.mockResolvedValue(undefined)
    const client = { query: mockQuery, end: mockEnd } as any
    await releaseWorkerClient(client)
    expect(mockQuery).toHaveBeenCalledWith('COMMIT')
    expect(mockEnd).toHaveBeenCalled()
  })
})

describe('destroyClient', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('destroys socket and calls end without throwing', () => {
    mockEnd.mockRejectedValue(new Error('already closed'))
    const mockDestroy = vi.fn()
    const client = {
      end: mockEnd,
      connection: { stream: { destroy: mockDestroy } },
    } as any
    expect(() => destroyClient(client)).not.toThrow()
    expect(mockDestroy).toHaveBeenCalled()
  })

  it('handles missing connection/stream gracefully', () => {
    const client = { end: mockEnd, connection: null } as any
    expect(() => destroyClient(client)).not.toThrow()
  })
})

describe('connectWithRetry (via createClient)', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('retries on failure and succeeds', async () => {
    mockConnect
      .mockRejectedValueOnce(new Error('ECONNREFUSED'))
      .mockRejectedValueOnce(new Error('ECONNREFUSED'))
      .mockResolvedValueOnce(undefined)
    mockEnd.mockResolvedValue(undefined)
    const promise = createClient('postgresql://u:p@h/db')
    // Advance past the retry delays (2s base * 2^attempt + jitter)
    await vi.advanceTimersByTimeAsync(5_000)
    await vi.advanceTimersByTimeAsync(10_000)
    const client = await promise
    expect(client).toBeDefined()
    // 2 failed + 1 success = 3 connect calls
    expect(mockConnect).toHaveBeenCalledTimes(3)
  })

  it('throws after exhausting all connect retries for non-network errors', async () => {
    // Use real timers with zero delays by mocking setTimeout to fire immediately
    vi.useRealTimers()
    // Mock Math.random to return 0 (no jitter) for deterministic behavior
    const randomSpy = vi.spyOn(Math, 'random').mockReturnValue(0)
    // Mock setTimeout to fire callbacks immediately
    const origSetTimeout = globalThis.setTimeout
    const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout').mockImplementation((fn: any) => {
      return origSetTimeout(fn, 0)
    })

    mockConnect.mockRejectedValue(new Error('authentication failed'))
    mockEnd.mockResolvedValue(undefined)

    await expect(createClient('postgresql://u:p@h/db')).rejects.toThrow('authentication failed')
    // CONNECT_RETRIES=5, attempts 0..5 = 6 total connect calls
    expect(mockConnect).toHaveBeenCalledTimes(6)

    setTimeoutSpy.mockRestore()
    randomSpy.mockRestore()
    vi.useFakeTimers()
  })
})

describe('connectWithRetry resilience', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('retries indefinitely on network errors until success', async () => {
    vi.useFakeTimers()

    // Simulate 10 network failures then success
    const networkErr = Object.assign(new Error('ECONNREFUSED'), { code: 'ECONNREFUSED' })
    for (let i = 0; i < 10; i++) {
      mockConnect.mockRejectedValueOnce(networkErr)
    }
    mockConnect.mockResolvedValueOnce(undefined)
    mockEnd.mockResolvedValue(undefined)

    const promise = createClient('postgresql://u:p@h/db')

    // Advance through all 10 retries
    for (let i = 0; i < 10; i++) {
      await vi.advanceTimersByTimeAsync(60_000)
    }

    const client = await promise
    expect(client).toBeDefined()
    expect(mockConnect).toHaveBeenCalledTimes(11) // 10 failures + 1 success

    vi.useRealTimers()
  })

  it('gives up on non-network errors after retry limit', async () => {
    vi.useRealTimers()
    const randomSpy = vi.spyOn(Math, 'random').mockReturnValue(0)
    const origSetTimeout = globalThis.setTimeout
    const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout').mockImplementation((fn: any) => {
      return origSetTimeout(fn, 0)
    })

    mockConnect.mockRejectedValue(new Error('authentication failed'))
    mockEnd.mockResolvedValue(undefined)

    await expect(createClient('postgresql://u:p@h/db')).rejects.toThrow('authentication failed')
    // CONNECT_RETRIES=5, attempts 0..5 = 6 total connect calls
    expect(mockConnect).toHaveBeenCalledTimes(6)

    setTimeoutSpy.mockRestore()
    randomSpy.mockRestore()
    vi.useFakeTimers()
  })
})
