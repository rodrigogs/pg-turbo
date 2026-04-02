// ts/tests/unit/args.test.ts
import { describe, expect, it } from 'vitest'
import { parseDumpArgs, parseRestoreArgs, parseSize } from '../../src/cli/args.js'

describe('parseSize', () => {
  it('parses plain bytes', () => {
    expect(parseSize('1024')).toBe(1024)
  })
  it('parses KB', () => {
    expect(parseSize('10KB')).toBe(10 * 1024)
  })
  it('parses MB', () => {
    expect(parseSize('256MB')).toBe(256 * 1024 * 1024)
  })
  it('parses GB', () => {
    expect(parseSize('1GB')).toBe(1024 * 1024 * 1024)
  })
  it('parses TB', () => {
    expect(parseSize('2TB')).toBe(2 * 1024 ** 4)
  })
  it('parses plain number without unit as bytes', () => {
    expect(parseSize('500')).toBe(500)
  })
  it('parses fractional sizes', () => {
    expect(parseSize('1.5GB')).toBe(Math.floor(1.5 * 1024 ** 3))
  })
  it('throws on invalid input', () => {
    expect(() => parseSize('abc')).toThrow()
  })
  it('parses case-insensitive units', () => {
    expect(parseSize('10kb')).toBe(10 * 1024)
    expect(parseSize('10Kb')).toBe(10 * 1024)
    expect(parseSize('1gb')).toBe(1024 ** 3)
  })
  it('parses size with space before unit', () => {
    expect(parseSize('10 MB')).toBe(10 * 1024 * 1024)
    expect(parseSize('1 GB')).toBe(1024 ** 3)
  })
  it('throws on negative number', () => {
    expect(() => parseSize('-5MB')).toThrow()
  })
  it('throws on empty string', () => {
    expect(() => parseSize('')).toThrow()
  })
})

describe('parseDumpArgs', () => {
  it('parses required args', () => {
    const opts = parseDumpArgs(['-d', 'postgresql://u:p@h/db', '--output', './out'])
    expect(opts.dbname).toBe('postgresql://u:p@h/db')
    expect(opts.output).toBe('./out')
  })
  it('applies defaults', () => {
    const opts = parseDumpArgs(['-d', 'pg://h/db', '--output', './out'])
    expect(opts.jobs).toBe(4)
    expect(opts.retries).toBe(5)
    expect(opts.dryRun).toBe(false)
    expect(opts.noSnapshot).toBe(false)
    expect(opts.noArchive).toBe(false)
    expect(opts.splitThreshold).toBe(1024 * 1024 * 1024)
  })
  it('parses optional flags', () => {
    const opts = parseDumpArgs([
      '-d',
      'pg://h/db',
      '--output',
      './out',
      '-n',
      'public',
      '-j',
      '8',
      '--dry-run',
      '--no-snapshot',
      '--split-threshold',
      '512MB',
    ])
    expect(opts.schema).toBe('public')
    expect(opts.jobs).toBe(8)
    expect(opts.dryRun).toBe(true)
    expect(opts.noSnapshot).toBe(true)
    expect(opts.splitThreshold).toBe(512 * 1024 * 1024)
  })
  it('captures passthrough args after --', () => {
    const opts = parseDumpArgs([
      '-d',
      'pg://h/db',
      '--output',
      './out',
      '--',
      '--no-comments',
      '--lock-wait-timeout=300',
    ])
    expect(opts.pgDumpArgs).toEqual(['--no-comments', '--lock-wait-timeout=300'])
  })
})

describe('parseRestoreArgs', () => {
  it('parses required args', () => {
    const opts = parseRestoreArgs(['-d', 'pg://h/db', '--input', './in'])
    expect(opts.dbname).toBe('pg://h/db')
    expect(opts.input).toBe('./in')
  })
  it('parses restore-specific flags', () => {
    const opts = parseRestoreArgs(['-d', 'pg://h/db', '--input', './in', '-c', '-a', '-t', 'users'])
    expect(opts.clean).toBe(true)
    expect(opts.dataOnly).toBe(true)
    expect(opts.table).toBe('users')
  })
  it('applies defaults', () => {
    const opts = parseRestoreArgs(['-d', 'pg://h/db', '--input', './in'])
    expect(opts.jobs).toBe(4)
    expect(opts.retries).toBe(5)
    expect(opts.retryDelay).toBe(5)
    expect(opts.dryRun).toBe(false)
    expect(opts.clean).toBe(false)
    expect(opts.dataOnly).toBe(false)
  })
  it('captures passthrough args after --', () => {
    const opts = parseRestoreArgs(['-d', 'pg://h/db', '--input', './in', '--', '--no-comments', '--single-transaction'])
    expect(opts.pgRestoreArgs).toEqual(['--no-comments', '--single-transaction'])
  })
  it('returns empty passthrough when no separator', () => {
    const opts = parseRestoreArgs(['-d', 'pg://h/db', '--input', './in'])
    expect(opts.pgRestoreArgs).toEqual([])
  })
})

describe('parseDumpArgs compression', () => {
  it('defaults to zstd compression', () => {
    const opts = parseDumpArgs(['-d', 'pg://h/db', '--output', './out'])
    expect(opts.compression).toBe('zstd')
  })
  it('accepts lz4 compression', () => {
    const opts = parseDumpArgs(['-d', 'pg://h/db', '--output', './out', '--compression', 'lz4'])
    expect(opts.compression).toBe('lz4')
  })
  it('falls back to zstd for unknown compression value', () => {
    const opts = parseDumpArgs(['-d', 'pg://h/db', '--output', './out', '--compression', 'gzip'])
    expect(opts.compression).toBe('zstd')
  })
  it('parses --no-archive flag', () => {
    const opts = parseDumpArgs(['-d', 'pg://h/db', '--output', './out', '--no-archive'])
    expect(opts.noArchive).toBe(true)
  })
})
