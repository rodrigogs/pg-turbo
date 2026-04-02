// ts/tests/unit/copy-stream.test.ts
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { PassThrough, Readable, Transform } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// Mock pg-copy-streams before importing copy-stream
vi.mock('pg-copy-streams', () => ({
  to: vi.fn((query: string) => query),
  from: vi.fn((query: string) => query),
}))

import {
  buildRestoreCopyQuery,
  chunkDoneMarker,
  createByteCounter,
  createCompressor,
  createDecompressor,
  createRowCounter,
  dumpChunk,
  ensureProgressTable,
  fetchCompletedChunks,
  removePartialChunk,
  resetProgress,
  dropProgressTable,
  restoreChunk,
} from '../../src/core/copy-stream.js'

describe('buildRestoreCopyQuery', () => {
  it('builds COPY FROM query with columns', () => {
    expect(buildRestoreCopyQuery('public', 'users', ['id', 'name', 'email'])).toBe(
      'COPY "public"."users" ("id", "name", "email") FROM STDIN',
    )
  })
  it('quotes schema and table names', () => {
    expect(buildRestoreCopyQuery('my schema', 'my-table', ['col'])).toBe(
      'COPY "my schema"."my-table" ("col") FROM STDIN',
    )
  })
})

describe('marker helpers', () => {
  it('generates dump done marker path', () => {
    expect(chunkDoneMarker('/out/data/public.users/chunk_0000.copy.lz4')).toBe(
      '/out/data/public.users/chunk_0000.copy.lz4.done',
    )
  })
})

describe('createCompressor', () => {
  it('returns a Transform stream for zstd', () => {
    const compressor = createCompressor('zstd')
    expect(compressor).toBeInstanceOf(Transform)
  })

  it('returns a Transform stream for lz4', () => {
    const compressor = createCompressor('lz4')
    expect(compressor).toBeInstanceOf(Transform)
  })

  it('compresses and decompresses data with zstd roundtrip', async () => {
    const input = new PassThrough()
    const compressor = createCompressor('zstd')
    const decompressor = createDecompressor('zstd')
    const output = new PassThrough()
    const chunks: Buffer[] = []
    output.on('data', (chunk: Buffer) => chunks.push(chunk))
    const p = pipeline(input, compressor, decompressor, output)
    input.end(Buffer.from('hello world from zstd'))
    await p
    const result = Buffer.concat(chunks).toString()
    expect(result).toBe('hello world from zstd')
  })

  it('compresses and decompresses data with lz4 roundtrip', async () => {
    const input = new PassThrough()
    const compressor = createCompressor('lz4')
    const decompressor = createDecompressor('lz4')
    const output = new PassThrough()
    const chunks: Buffer[] = []
    output.on('data', (chunk: Buffer) => chunks.push(chunk))
    const p = pipeline(input, compressor, decompressor, output)
    input.end(Buffer.from('hello world from lz4'))
    await p
    const result = Buffer.concat(chunks).toString()
    expect(result).toBe('hello world from lz4')
  })
})

describe('createDecompressor', () => {
  it('returns a Transform stream for zstd', () => {
    const decompressor = createDecompressor('zstd')
    expect(decompressor).toBeInstanceOf(Transform)
  })

  it('returns a Transform stream for lz4', () => {
    const decompressor = createDecompressor('lz4')
    expect(decompressor).toBeInstanceOf(Transform)
  })
})

describe('createByteCounter', () => {
  it('counts bytes passing through', async () => {
    let reported = 0
    const counter = createByteCounter((total) => {
      reported = total
    })
    const input = new PassThrough()
    const output = new PassThrough()
    const p = pipeline(input, counter, output)
    input.end(Buffer.from('hello world'))
    await p
    expect(reported).toBe(11)
  })

  it('accumulates bytes across multiple chunks', async () => {
    const reports: number[] = []
    const counter = createByteCounter((total) => {
      reports.push(total)
    })
    const input = new PassThrough()
    const output = new PassThrough()
    const p = pipeline(input, counter, output)
    input.write(Buffer.from('hello'))
    input.end(Buffer.from(' world'))
    await p
    expect(reports[reports.length - 1]).toBe(11)
  })
})

describe('createRowCounter', () => {
  it('counts newlines as rows', async () => {
    let reported = 0
    const counter = createRowCounter((total) => {
      reported = total
    })
    const input = new PassThrough()
    const output = new PassThrough()
    const p = pipeline(input, counter, output)
    input.end(Buffer.from('row1\nrow2\nrow3\n'))
    await p
    expect(reported).toBe(3)
  })

  it('handles data without trailing newline', async () => {
    let reported = 0
    const counter = createRowCounter((total) => {
      reported = total
    })
    const input = new PassThrough()
    const output = new PassThrough()
    const p = pipeline(input, counter, output)
    input.end(Buffer.from('row1\nrow2'))
    await p
    expect(reported).toBe(1)
  })
})

describe('dumpChunk', () => {
  let tmpDir: string
  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'pgr-dump-'))
  })
  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true })
  })

  it('writes compressed data and creates done marker', async () => {
    const outputPath = join(tmpDir, 'chunk_0000.copy.zst')
    const copyStream = new PassThrough()
    Object.defineProperty(copyStream, 'rowCount', { value: 3, writable: false })

    const mockClient = {
      query: vi.fn().mockReturnValue(copyStream),
    }

    // Push data then end the stream asynchronously
    setTimeout(() => {
      copyStream.push(Buffer.from('row1\nrow2\nrow3\n'))
      copyStream.push(null)
    }, 10)

    const result = await dumpChunk(mockClient as any, 'COPY test TO STDOUT', outputPath, 'zstd')
    expect(result.rowCount).toBe(3)
    expect(result.bytesWritten).toBeGreaterThan(0)
    // Verify done marker was created
    expect(() => readFileSync(`${outputPath}.done`)).not.toThrow()
  })

  it('calls onProgress callback when provided', async () => {
    const outputPath = join(tmpDir, 'chunk_progress.copy.zst')
    const copyStream = new PassThrough()
    Object.defineProperty(copyStream, 'rowCount', { value: 2, writable: false })

    const mockClient = {
      query: vi.fn().mockReturnValue(copyStream),
    }

    const progressCalls: number[] = []
    setTimeout(() => {
      copyStream.push(Buffer.from('row1\nrow2\n'))
      copyStream.push(null)
    }, 10)

    await dumpChunk(mockClient as any, 'COPY test TO STDOUT', outputPath, 'zstd', (rows) => {
      progressCalls.push(rows)
    })
    expect(progressCalls.length).toBeGreaterThan(0)
    expect(progressCalls[progressCalls.length - 1]).toBe(2)
  })
})

describe('restoreChunk', () => {
  let tmpDir: string
  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'pgr-restore-'))
  })
  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true })
  })

  it('restores data with BEGIN/COMMIT transaction', async () => {
    // Create a compressed chunk file first
    const chunkPath = join(tmpDir, 'chunk_0000.copy.zst')
    const compressor = createCompressor('zstd')
    const { createWriteStream } = await import('node:fs')
    const ws = createWriteStream(chunkPath)
    await pipeline(Readable.from(Buffer.from('1\ttest\n')), compressor, ws)

    const queryCalls: string[] = []
    const copyStream = new PassThrough()

    const mockClient = {
      query: vi.fn().mockImplementation((q: string) => {
        if (typeof q === 'string') queryCalls.push(q)
        if (typeof q === 'string' && q.startsWith('COPY')) return copyStream
        return { rows: [] }
      }),
    }

    // Consume the copy stream data so pipeline completes
    copyStream.on('data', () => {})

    await restoreChunk(mockClient as any, 'public', 'users', ['id', 'name'], chunkPath, 'public.users.0', 'zstd')

    expect(queryCalls).toContain('SET synchronous_commit = off')
    expect(queryCalls).toContain('BEGIN')
    expect(queryCalls).toContain('COMMIT')
    expect(queryCalls).not.toContain('ROLLBACK')
  })

  it('calls onProgress callback when provided', async () => {
    const chunkPath = join(tmpDir, 'chunk_progress.copy.zst')
    const compressor = createCompressor('zstd')
    const { createWriteStream } = await import('node:fs')
    const ws = createWriteStream(chunkPath)
    await pipeline(Readable.from(Buffer.from('data\n')), compressor, ws)

    const copyStream = new PassThrough()
    const mockClient = {
      query: vi.fn().mockImplementation((q: string) => {
        if (typeof q === 'string' && q.startsWith('COPY')) return copyStream
        return { rows: [] }
      }),
    }
    copyStream.on('data', () => {})

    const progressCalls: number[] = []
    await restoreChunk(
      mockClient as any, 'public', 'users', ['id'], chunkPath, 'public.users.0', 'zstd',
      (bytes) => { progressCalls.push(bytes) },
    )
    expect(progressCalls.length).toBeGreaterThan(0)
  })

  it('rolls back on pipeline error', async () => {
    // Create a file that is not valid compressed data to cause decompressor error
    const chunkPath = join(tmpDir, 'bad_chunk.copy.zst')
    writeFileSync(chunkPath, 'not valid zstd data')

    const queryCalls: string[] = []
    const copyStream = new PassThrough()

    const mockClient = {
      query: vi.fn().mockImplementation((q: string) => {
        if (typeof q === 'string') queryCalls.push(q)
        if (typeof q === 'string' && q.startsWith('COPY')) return copyStream
        return { rows: [] }
      }),
    }

    await expect(
      restoreChunk(mockClient as any, 'public', 'users', ['id'], chunkPath, 'public.users.0', 'zstd'),
    ).rejects.toThrow()
    expect(queryCalls).toContain('ROLLBACK')
  })
})

describe('ensureProgressTable', () => {
  it('creates schema and table', async () => {
    const mockClient = { query: vi.fn().mockResolvedValue({ rows: [] }) }
    await ensureProgressTable(mockClient as any)
    expect(mockClient.query).toHaveBeenCalledTimes(2)
    expect(mockClient.query.mock.calls[0][0]).toContain('CREATE SCHEMA IF NOT EXISTS')
    expect(mockClient.query.mock.calls[1][0]).toContain('CREATE TABLE IF NOT EXISTS')
  })
})

describe('fetchCompletedChunks', () => {
  it('returns a set of chunk keys', async () => {
    const mockClient = {
      query: vi.fn().mockResolvedValue({
        rows: [{ chunk_key: 'public.users.0' }, { chunk_key: 'public.users.1' }],
      }),
    }
    const result = await fetchCompletedChunks(mockClient as any)
    expect(result).toBeInstanceOf(Set)
    expect(result.size).toBe(2)
    expect(result.has('public.users.0')).toBe(true)
  })
})

describe('resetProgress', () => {
  it('truncates the progress table', async () => {
    const mockClient = { query: vi.fn().mockResolvedValue({ rows: [] }) }
    await resetProgress(mockClient as any)
    expect(mockClient.query.mock.calls[0][0]).toContain('TRUNCATE')
  })
})

describe('dropProgressTable', () => {
  it('drops the progress schema', async () => {
    const mockClient = { query: vi.fn().mockResolvedValue({ rows: [] }) }
    await dropProgressTable(mockClient as any)
    expect(mockClient.query.mock.calls[0][0]).toContain('DROP SCHEMA IF EXISTS')
  })
})

describe('removePartialChunk', () => {
  let tmpDir: string
  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'pgr-remove-'))
  })
  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true })
  })

  it('removes chunk and done marker files', async () => {
    const chunkPath = join(tmpDir, 'chunk.copy.zst')
    const donePath = `${chunkPath}.done`
    writeFileSync(chunkPath, 'data')
    writeFileSync(donePath, '')
    await removePartialChunk(chunkPath)
    expect(() => readFileSync(chunkPath)).toThrow()
    expect(() => readFileSync(donePath)).toThrow()
  })

  it('does not throw when files do not exist', async () => {
    await expect(removePartialChunk(join(tmpDir, 'nonexistent.copy.zst'))).resolves.toBeUndefined()
  })
})
