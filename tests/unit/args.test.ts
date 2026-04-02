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
  it('throws on invalid input', () => {
    expect(() => parseSize('abc')).toThrow()
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
})
