import { describe, expect, it, vi } from 'vitest'
import { calculateDelay, retryWithBackoff } from '../../src/core/retry.js'

describe('calculateDelay', () => {
  it('returns base delay on first attempt', () => {
    const delay = calculateDelay(0, 5, 60)
    expect(delay).toBeGreaterThanOrEqual(5_000)
    expect(delay).toBeLessThan(6_000)
  })
  it('doubles delay on each attempt', () => {
    const delay = calculateDelay(2, 5, 60)
    expect(delay).toBeGreaterThanOrEqual(20_000)
    expect(delay).toBeLessThan(21_000)
  })
  it('caps at maxDelay', () => {
    const delay = calculateDelay(10, 5, 60)
    expect(delay).toBeGreaterThanOrEqual(60_000)
    expect(delay).toBeLessThan(61_000)
  })
})

describe('retryWithBackoff', () => {
  it('returns result on first success', async () => {
    const fn = vi.fn().mockResolvedValue('ok')
    const result = await retryWithBackoff(fn, { maxRetries: 3, baseDelay: 0, maxDelay: 0 })
    expect(result).toBe('ok')
    expect(fn).toHaveBeenCalledTimes(1)
  })
  it('retries on failure and succeeds', async () => {
    const fn = vi
      .fn()
      .mockRejectedValueOnce(new Error('fail 1'))
      .mockRejectedValueOnce(new Error('fail 2'))
      .mockResolvedValue('ok')
    const result = await retryWithBackoff(fn, { maxRetries: 5, baseDelay: 0, maxDelay: 0 })
    expect(result).toBe('ok')
    expect(fn).toHaveBeenCalledTimes(3)
  })
  it('throws after exhausting retries', async () => {
    const fn = vi.fn().mockRejectedValue(new Error('always fails'))
    await expect(retryWithBackoff(fn, { maxRetries: 3, baseDelay: 0, maxDelay: 0 })).rejects.toThrow('always fails')
    expect(fn).toHaveBeenCalledTimes(3)
  })
  it('throws when maxRetries is 0', async () => {
    const fn = vi.fn().mockResolvedValue('ok')
    await expect(retryWithBackoff(fn, { maxRetries: 0, baseDelay: 0, maxDelay: 0 })).rejects.toThrow(
      'maxRetries must be at least 1',
    )
    expect(fn).not.toHaveBeenCalled()
  })
  it('calls onRetry callback', async () => {
    const onRetry = vi.fn()
    const fn = vi.fn().mockRejectedValueOnce(new Error('fail')).mockResolvedValue('ok')
    await retryWithBackoff(fn, { maxRetries: 3, baseDelay: 0, maxDelay: 0, onRetry })
    expect(onRetry).toHaveBeenCalledTimes(1)
    expect(onRetry).toHaveBeenCalledWith(1, expect.any(Error))
  })
})
