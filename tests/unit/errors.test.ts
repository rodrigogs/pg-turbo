import { describe, expect, it } from 'vitest'
import {
  ErrorCategory,
  classifyError,
  isNetworkError,
  isTransientError,
} from '../../src/core/errors.js'

function makeError(message: string, code?: string): Error {
  const err = new Error(message)
  if (code) (err as any).code = code
  return err
}

describe('classifyError', () => {
  describe('NETWORK category', () => {
    it.each([
      ['ECONNREFUSED', 'connect ECONNREFUSED'],
      ['ECONNRESET', 'read ECONNRESET'],
      ['EPIPE', 'write EPIPE'],
      ['ETIMEDOUT', 'connect ETIMEDOUT'],
      ['ENOTFOUND', 'getaddrinfo ENOTFOUND'],
      ['ENETUNREACH', 'connect ENETUNREACH'],
      ['EHOSTUNREACH', 'connect EHOSTUNREACH'],
      ['ECONNABORTED', 'connection aborted'],
      ['EAI_AGAIN', 'getaddrinfo EAI_AGAIN'],
    ])('classifies %s as NETWORK', (code, message) => {
      expect(classifyError(makeError(message, code))).toBe(ErrorCategory.NETWORK)
    })
  })

  describe('SERVER category', () => {
    it.each([
      ['08006', 'connection failure'],
      ['08003', 'connection does not exist'],
      ['57P01', 'terminating connection due to administrator command'],
      ['57P02', 'terminating connection due to crash of another server process'],
      ['57P03', 'the database system is shutting down'],
    ])('classifies PG %s as SERVER', (code, message) => {
      expect(classifyError(makeError(message, code))).toBe(ErrorCategory.SERVER)
    })
  })

  describe('STREAM category', () => {
    it.each([
      ['ERR_STREAM_PREMATURE_CLOSE', 'Premature close'],
      ['ERR_STREAM_DESTROYED', 'Cannot call write after a stream was destroyed'],
    ])('classifies %s as STREAM', (code, message) => {
      expect(classifyError(makeError(message, code))).toBe(ErrorCategory.STREAM)
    })

    it.each([
      'Connection terminated unexpectedly',
      'Connection terminated',
      'connection lost',
      'server closed the connection unexpectedly',
      'Connection idle timeout — no data received for 15s',
      'timeout expired',
    ])('classifies message "%s" as STREAM', (message) => {
      expect(classifyError(makeError(message))).toBe(ErrorCategory.STREAM)
    })
  })

  describe('UNKNOWN category (non-transient)', () => {
    it.each([
      ['23505', 'unique constraint violation'],
      ['42601', 'syntax error'],
      [undefined, 'something failed'],
      [undefined, 'COPY failed'],
    ])('classifies code=%s message="%s" as UNKNOWN', (code, message) => {
      expect(classifyError(makeError(message, code))).toBe(ErrorCategory.UNKNOWN)
    })

    it('classifies non-Error values as UNKNOWN', () => {
      expect(classifyError('string')).toBe(ErrorCategory.UNKNOWN)
      expect(classifyError(null)).toBe(ErrorCategory.UNKNOWN)
      expect(classifyError(undefined)).toBe(ErrorCategory.UNKNOWN)
      expect(classifyError(42)).toBe(ErrorCategory.UNKNOWN)
    })
  })

  describe('cause chain traversal', () => {
    it('detects network error in cause', () => {
      const cause = makeError('read ECONNRESET', 'ECONNRESET')
      const wrapper = new Error('pipeline failed')
      ;(wrapper as any).cause = cause
      expect(classifyError(wrapper)).toBe(ErrorCategory.NETWORK)
    })

    it('detects server error in cause', () => {
      const cause = makeError('admin shutdown', '57P01')
      const wrapper = new Error('pipeline failed')
      ;(wrapper as any).cause = cause
      expect(classifyError(wrapper)).toBe(ErrorCategory.SERVER)
    })

    it('returns UNKNOWN when cause is also unknown', () => {
      const cause = new Error('data problem')
      const wrapper = new Error('pipeline failed')
      ;(wrapper as any).cause = cause
      expect(classifyError(wrapper)).toBe(ErrorCategory.UNKNOWN)
    })
  })
})

describe('isTransientError', () => {
  it('returns true for NETWORK, SERVER, STREAM', () => {
    expect(isTransientError(makeError('x', 'ECONNRESET'))).toBe(true)
    expect(isTransientError(makeError('x', '57P01'))).toBe(true)
    expect(isTransientError(makeError('Connection terminated'))).toBe(true)
  })

  it('returns false for UNKNOWN/DATA', () => {
    expect(isTransientError(makeError('bad data', '23505'))).toBe(false)
    expect(isTransientError(makeError('unknown'))).toBe(false)
    expect(isTransientError(null)).toBe(false)
  })
})

describe('isNetworkError (backward compat)', () => {
  it('delegates to isTransientError', () => {
    expect(isNetworkError(makeError('x', 'ECONNRESET'))).toBe(true)
    expect(isNetworkError(makeError('bad'))).toBe(false)
  })
})
