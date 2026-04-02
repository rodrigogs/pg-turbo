// ts/tests/unit/copy-stream.test.ts
import { PassThrough, Transform } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { describe, expect, it } from 'vitest'
import {
  buildRestoreCopyQuery,
  chunkDoneMarker,
  createByteCounter,
  createCompressor,
  createDecompressor,
  createRowCounter,
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
