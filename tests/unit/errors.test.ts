import { describe, expect, it } from 'vitest'
import { isNetworkError } from '../../src/core/errors.js'

describe('isNetworkError', () => {
  it('detects ECONNREFUSED', () => {
    const err = new Error('connect ECONNREFUSED')
    ;(err as any).code = 'ECONNREFUSED'
    expect(isNetworkError(err)).toBe(true)
  })

  it('detects ECONNRESET', () => {
    const err = new Error('read ECONNRESET')
    ;(err as any).code = 'ECONNRESET'
    expect(isNetworkError(err)).toBe(true)
  })

  it('detects EPIPE', () => {
    const err = new Error('write EPIPE')
    ;(err as any).code = 'EPIPE'
    expect(isNetworkError(err)).toBe(true)
  })

  it('detects ETIMEDOUT', () => {
    const err = new Error('connect ETIMEDOUT')
    ;(err as any).code = 'ETIMEDOUT'
    expect(isNetworkError(err)).toBe(true)
  })

  it('detects ENOTFOUND', () => {
    const err = new Error('getaddrinfo ENOTFOUND')
    ;(err as any).code = 'ENOTFOUND'
    expect(isNetworkError(err)).toBe(true)
  })

  it('detects ENETUNREACH', () => {
    const err = new Error('connect ENETUNREACH')
    ;(err as any).code = 'ENETUNREACH'
    expect(isNetworkError(err)).toBe(true)
  })

  it('detects EHOSTUNREACH', () => {
    const err = new Error('connect EHOSTUNREACH')
    ;(err as any).code = 'EHOSTUNREACH'
    expect(isNetworkError(err)).toBe(true)
  })

  it('detects PostgreSQL connection error codes (08xxx)', () => {
    const err = new Error('connection lost')
    ;(err as any).code = '08006'
    expect(isNetworkError(err)).toBe(true)
  })

  it('detects "Connection terminated unexpectedly"', () => {
    const err = new Error('Connection terminated unexpectedly')
    expect(isNetworkError(err)).toBe(true)
  })

  it('detects "Connection terminated"', () => {
    const err = new Error('Connection terminated')
    expect(isNetworkError(err)).toBe(true)
  })

  it('does NOT flag constraint violations', () => {
    const err = new Error('unique constraint violation')
    ;(err as any).code = '23505'
    expect(isNetworkError(err)).toBe(false)
  })

  it('does NOT flag syntax errors', () => {
    const err = new Error('syntax error')
    ;(err as any).code = '42601'
    expect(isNetworkError(err)).toBe(false)
  })

  it('does NOT flag generic errors', () => {
    expect(isNetworkError(new Error('something failed'))).toBe(false)
  })

  it('does NOT flag COPY data errors', () => {
    const err = new Error('COPY failed')
    expect(isNetworkError(err)).toBe(false)
  })
})
