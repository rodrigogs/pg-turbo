import { describe, it, expect } from 'vitest'
import { humanSize, elapsedTime, progressBar } from '../../src/core/format.js'

describe('humanSize', () => {
  it('formats bytes', () => {
    expect(humanSize(0)).toBe('0 B')
    expect(humanSize(500)).toBe('500 B')
    expect(humanSize(1023)).toBe('1023 B')
  })
  it('formats kilobytes', () => {
    expect(humanSize(1024)).toBe('1.0 KB')
    expect(humanSize(1536)).toBe('1.5 KB')
  })
  it('formats megabytes', () => {
    expect(humanSize(1048576)).toBe('1.0 MB')
    expect(humanSize(2621440)).toBe('2.5 MB')
  })
  it('formats gigabytes', () => {
    expect(humanSize(1073741824)).toBe('1.00 GB')
    expect(humanSize(3758096384)).toBe('3.50 GB')
  })
})

describe('elapsedTime', () => {
  it('formats seconds', () => {
    expect(elapsedTime(0)).toBe('0s')
    expect(elapsedTime(59)).toBe('59s')
  })
  it('formats minutes', () => {
    expect(elapsedTime(60)).toBe('1m 0s')
    expect(elapsedTime(61)).toBe('1m 1s')
    expect(elapsedTime(330)).toBe('5m 30s')
  })
  it('formats hours', () => {
    expect(elapsedTime(3600)).toBe('1h 0m 0s')
    expect(elapsedTime(3661)).toBe('1h 1m 1s')
    expect(elapsedTime(9045)).toBe('2h 30m 45s')
  })
})

describe('progressBar', () => {
  it('shows percentage', () => {
    expect(progressBar(0, 10, 20)).toContain('0%')
    expect(progressBar(5, 10, 20)).toContain('50%')
    expect(progressBar(10, 10, 20)).toContain('100%')
  })
  it('shows byte counts for large totals', () => {
    expect(progressBar(1048576, 1048576, 20)).toContain('1.0 MB')
  })
  it('shows item counts for small totals', () => {
    expect(progressBar(3, 7, 20)).toContain('3/7')
  })
  it('handles current > total without crashing', () => {
    expect(() => progressBar(20, 10, 20)).not.toThrow()
    expect(progressBar(20, 10, 20)).toContain('100%')
  })
  it('handles total = 0', () => {
    expect(() => progressBar(0, 0, 20)).not.toThrow()
    expect(progressBar(0, 0, 20)).toContain('0%')
  })
  it('handles negative current', () => {
    expect(() => progressBar(-5, 100, 20)).not.toThrow()
  })
})
