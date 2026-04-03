import type { ChunkJob, ChunkResult, ProgressEvent } from '../types/index.js'
import { chunkEstimatedBytes } from './chunker.js'
import { isTransientError } from './errors.js'
import { calculateDelay } from './retry.js'

export interface WorkerPoolOptions {
  jobs: ChunkJob[]
  workerCount: number
  task: (job: ChunkJob, workerId: number) => Promise<{ rowCount: number; bytesWritten: number }>
  onProgress: (event: ProgressEvent) => void
  maxRetries: number
  retryDelayMs?: number
  isResumable: (job: ChunkJob) => boolean
  onWorkerError?: (workerId: number, error: Error) => void
}

export async function runWorkerPool(opts: WorkerPoolOptions): Promise<ChunkResult[]> {
  if (opts.workerCount <= 0) {
    throw new Error('workerCount must be at least 1')
  }
  // Sort largest chunks first (not just by table) so big work items start early and don't become stragglers
  const queue = [...opts.jobs].sort((a, b) => chunkEstimatedBytes(b) - chunkEstimatedBytes(a))
  const results: ChunkResult[] = []
  let queueIndex = 0
  const retryQueue: ChunkJob[] = []

  function nextWork(): ChunkJob | undefined {
    if (retryQueue.length > 0) return retryQueue.shift()
    if (queueIndex < queue.length) return queue[queueIndex++]
    return undefined
  }

  async function worker(workerId: number): Promise<void> {
    let job: ChunkJob | undefined = nextWork()
    while (job !== undefined) {
      if (opts.isResumable(job)) {
        results.push({ job, status: 'skipped' })
        opts.onProgress({ type: 'skipped', workerId, job })
        job = nextWork()
        continue
      }
      // Apply backoff delay for both data retries and network retries
      const retryCount = (job.networkRetries ?? 0) + job.attempt
      if (retryCount > 0 && opts.retryDelayMs) {
        const baseDelaySec = opts.retryDelayMs / 1000
        const delay = calculateDelay(Math.min(retryCount - 1, 5), baseDelaySec, 60)
        await new Promise((r) => setTimeout(r, delay))
      }
      const startTime = Date.now()
      try {
        opts.onProgress({ type: 'started', workerId, job })
        const { rowCount, bytesWritten } = await opts.task(job, workerId)
        // Success — reset network retries
        job.networkRetries = 0
        results.push({ job, status: 'ok', rowCount, bytesWritten, durationMs: Date.now() - startTime })
        opts.onProgress({ type: 'completed', workerId, job, bytesWritten })
      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err))

        if (isTransientError(error)) {
          // Network errors: don't count against retry limit, but track for backoff
          job.networkRetries = (job.networkRetries ?? 0) + 1
          opts.onProgress({ type: 'retrying', workerId, job, error })
          retryQueue.push(job)
        } else {
          // Data errors: count against retry limit
          job.attempt++
          if (job.attempt < opts.maxRetries) {
            opts.onProgress({ type: 'retrying', workerId, job, error })
            retryQueue.push(job)
          } else {
            results.push({ job, status: 'failed', error, durationMs: Date.now() - startTime })
            opts.onProgress({ type: 'failed', workerId, job, error })
          }
        }
        opts.onWorkerError?.(workerId, error)
      }
      job = nextWork()
    }
  }

  await Promise.all(Array.from({ length: Math.min(opts.workerCount, queue.length) }, (_, i) => worker(i)))
  return results
}
