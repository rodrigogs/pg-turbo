import type { ChunkJob, ChunkResult, ProgressEvent } from '../types/index.js'

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
  const queue = [...opts.jobs].sort((a, b) => (b.table.estimatedBytes ?? 0) - (a.table.estimatedBytes ?? 0))
  const results: ChunkResult[] = []
  let queueIndex = 0
  const retryQueue: ChunkJob[] = []

  function nextWork(): ChunkJob | undefined {
    if (retryQueue.length > 0) return retryQueue.shift()
    if (queueIndex < queue.length) return queue[queueIndex++]
    return undefined
  }

  async function worker(workerId: number): Promise<void> {
    let job: ChunkJob | undefined
    while ((job = nextWork()) !== undefined) {
      if (opts.isResumable(job)) {
        results.push({ job, status: 'skipped' })
        opts.onProgress({ type: 'skipped', workerId, job })
        continue
      }
      if (job.attempt > 0 && opts.retryDelayMs) {
        const delay = Math.min(opts.retryDelayMs * Math.pow(2, job.attempt - 1), 60_000)
        await new Promise(r => setTimeout(r, delay))
      }
      const startTime = Date.now()
      try {
        opts.onProgress({ type: 'started', workerId, job })
        const { rowCount, bytesWritten } = await opts.task(job, workerId)
        results.push({ job, status: 'ok', rowCount, bytesWritten, durationMs: Date.now() - startTime })
        opts.onProgress({ type: 'completed', workerId, job, bytesWritten })
      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err))
        job.attempt++
        if (job.attempt < opts.maxRetries) {
          opts.onProgress({ type: 'retrying', workerId, job, error })
          retryQueue.push(job)
        } else {
          results.push({ job, status: 'failed', error, durationMs: Date.now() - startTime })
          opts.onProgress({ type: 'failed', workerId, job, error })
        }
        opts.onWorkerError?.(workerId, error)
      }
    }
  }

  await Promise.all(Array.from({ length: Math.min(opts.workerCount, queue.length) }, (_, i) => worker(i)))
  return results
}
