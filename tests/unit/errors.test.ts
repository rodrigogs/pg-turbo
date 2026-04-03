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

  it('detects ERR_STREAM_PREMATURE_CLOSE', () => {
    const err = new Error('Premature close')
    ;(err as any).code = 'ERR_STREAM_PREMATURE_CLOSE'
    expect(isNetworkError(err)).toBe(true)
  })

  it('detects ERR_STREAM_DESTROYED', () => {
    const err = new Error('Cannot call write after a stream was destroyed')
    ;(err as any).code = 'ERR_STREAM_DESTROYED'
    expect(isNetworkError(err)).toBe(true)
  })

  it('detects PostgreSQL connection error codes (08xxx)', () => {
    const err = new Error('connection lost')
    ;(err as any).code = '08006'
    expect(isNetworkError(err)).toBe(true)
  })

  it('detects PostgreSQL admin shutdown (57P01)', () => {
    const err = new Error('terminating connection due to administrator command')
    ;(err as any).code = '57P01'
    expect(isNetworkError(err)).toBe(true)
  })

  it('detects PostgreSQL cannot connect now (57P03)', () => {
    const err = new Error('the database system is shutting down')
    ;(err as any).code = '57P03'
    expect(isNetworkError(err)).toBe(true)
  })

  it('detects PostgreSQL crash recovery (57P02)', () => {
    const err = new Error('terminating connection due to crash of another server process')
    ;(err as any).code = '57P02'
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

  it('detects "Connection idle timeout"', () => {
    const err = new Error('Connection idle timeout — no data received for 30s')
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

  it('detects network error in cause chain', () => {
    const cause = new Error('Connection terminated unexpectedly')
    const wrapper = new Error('pipeline failed', { cause })
    expect(isNetworkError(wrapper)).toBe(true)
  })

  it('detects network error code in cause chain', () => {
    const cause = new Error('read ECONNRESET')
    ;(cause as any).code = 'ECONNRESET'
    const wrapper = new Error('stream error', { cause })
    expect(isNetworkError(wrapper)).toBe(true)
  })

  it('does NOT flag wrapper with non-network cause', () => {
    const cause = new Error('syntax error')
    ;(cause as any).code = '42601'
    const wrapper = new Error('pipeline failed', { cause })
    expect(isNetworkError(wrapper)).toBe(false)
  })

  it('returns false for non-Error values', () => {
    expect(isNetworkError('string error')).toBe(false)
    expect(isNetworkError(null)).toBe(false)
    expect(isNetworkError(undefined)).toBe(false)
    expect(isNetworkError(42)).toBe(false)
  })
})
