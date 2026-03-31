export interface RetryOptions {
  maxRetries: number
  baseDelay: number
  maxDelay: number
  onRetry?: (attempt: number, error: Error) => void
}

export function calculateDelay(attempt: number, baseDelaySec: number, maxDelaySec: number): number {
  const delayMs = Math.min(baseDelaySec * Math.pow(2, attempt), maxDelaySec) * 1_000
  const jitter = Math.random() * 1_000
  return delayMs + jitter
}

export async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  opts: RetryOptions,
): Promise<T> {
  let lastError: Error | undefined
  for (let attempt = 0; attempt < opts.maxRetries; attempt++) {
    try {
      return await fn()
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err))
      if (attempt + 1 < opts.maxRetries) {
        opts.onRetry?.(attempt + 1, lastError)
        const delay = calculateDelay(attempt, opts.baseDelay, opts.maxDelay)
        if (delay > 0) {
          await new Promise(resolve => setTimeout(resolve, delay))
        }
      }
    }
  }
  throw lastError!
}
