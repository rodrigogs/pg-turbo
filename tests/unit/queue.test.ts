import { describe, expect, it, vi } from 'vitest'
import { runWorkerPool } from '../../src/core/queue.js'
import type { ChunkJob, ManifestTable } from '../../src/types/index.js'

function makeJob(index: number): ChunkJob {
  const chunk = {
    index: 0,
    file: `data/public.table_${index}/chunk_0000.copy.lz4`,
    estimatedBytes: 1000,
    estimatedRows: 10,
  }
  const table: ManifestTable = {
    schema: 'public',
    name: `table_${index}`,
    oid: index,
    relkind: 'r',
    estimatedBytes: 1000,
    estimatedRows: 10,
    pkColumn: null,
    pkType: null,
    chunkStrategy: 'none',
    columns: ['id'],
    generatedColumns: [],
    chunks: [chunk],
  }
  return {
    table,
    chunk,
    copyQuery: `COPY public.table_${index} TO STDOUT`,
    outputPath: `/tmp/test/data/public.table_${index}/chunk_0000.copy.lz4`,
    attempt: 0,
  }
}

describe('runWorkerPool', () => {
  it('processes all jobs with single worker', async () => {
    const jobs = [makeJob(1), makeJob(2), makeJob(3)]
    const task = vi.fn().mockResolvedValue({ rowCount: 10, bytesWritten: 100 })
    const results = await runWorkerPool({
      jobs,
      workerCount: 1,
      task,
      onProgress: vi.fn(),
      maxRetries: 3,
      isResumable: () => false,
    })
    expect(results).toHaveLength(3)
    expect(results.every((r) => r.status === 'ok')).toBe(true)
    expect(task).toHaveBeenCalledTimes(3)
  })

  it('processes jobs in parallel', async () => {
    const jobs = [makeJob(1), makeJob(2), makeJob(3), makeJob(4)]
    const running: number[] = []
    let maxConcurrent = 0
    const task = vi.fn().mockImplementation(async () => {
      running.push(1)
      maxConcurrent = Math.max(maxConcurrent, running.length)
      await new Promise((r) => setTimeout(r, 10))
      running.pop()
      return { rowCount: 1, bytesWritten: 1 }
    })
    await runWorkerPool({ jobs, workerCount: 2, task, onProgress: vi.fn(), maxRetries: 3, isResumable: () => false })
    expect(maxConcurrent).toBe(2)
  })

  it('fails immediately on non-transient errors (no retry)', async () => {
    const jobs = [makeJob(1)]
    const task = vi.fn().mockRejectedValue(new Error('FK violation'))
    const results = await runWorkerPool({
      jobs,
      workerCount: 1,
      task,
      onProgress: vi.fn(),
      maxRetries: 5,
      isResumable: () => false,
    })
    expect(results).toHaveLength(1)
    expect(results[0].status).toBe('failed')
    expect(task).toHaveBeenCalledTimes(1) // no retry
  })

  it('throws when workerCount is 0', async () => {
    await expect(
      runWorkerPool({
        jobs: [makeJob(1)],
        workerCount: 0,
        task: vi.fn(),
        onProgress: vi.fn(),
        maxRetries: 3,
        isResumable: () => false,
      }),
    ).rejects.toThrow('workerCount must be at least 1')
  })
  it('skips resumable jobs', async () => {
    const jobs = [makeJob(1), makeJob(2)]
    const task = vi.fn().mockResolvedValue({ rowCount: 1, bytesWritten: 1 })
    const results = await runWorkerPool({
      jobs,
      workerCount: 1,
      task,
      onProgress: vi.fn(),
      maxRetries: 3,
      isResumable: (job) => job.outputPath.includes('table_1'),
    })
    expect(results).toHaveLength(2)
    expect(results.find((r) => r.job.outputPath.includes('table_1'))?.status).toBe('skipped')
    expect(task).toHaveBeenCalledTimes(1)
  })

  it('applies retry delay on transient errors', async () => {
    const start = Date.now()
    const networkErr = Object.assign(new Error('ECONNRESET'), { code: 'ECONNRESET' })
    const task = vi.fn().mockRejectedValueOnce(networkErr).mockResolvedValue({ rowCount: 1, bytesWritten: 1 })
    const results = await runWorkerPool({
      jobs: [makeJob(1)],
      workerCount: 1,
      task,
      onProgress: vi.fn(),
      maxRetries: 3,
      isResumable: () => false,
      retryDelayMs: 50,
    })
    expect(results).toHaveLength(1)
    expect(results[0].status).toBe('ok')
    expect(task).toHaveBeenCalledTimes(2)
    // Should have waited at least ~50ms for the retry delay
    expect(Date.now() - start).toBeGreaterThanOrEqual(40)
  })

  it('wraps non-Error thrown values', async () => {
    const task = vi.fn().mockRejectedValue('string error')
    const results = await runWorkerPool({
      jobs: [makeJob(1)],
      workerCount: 1,
      task,
      onProgress: vi.fn(),
      maxRetries: 1,
      isResumable: () => false,
    })
    expect(results[0].status).toBe('failed')
    expect(results[0].error).toBeInstanceOf(Error)
    expect(results[0].error?.message).toBe('string error')
  })

  it('calls onWorkerError callback on failure', async () => {
    const onWorkerError = vi.fn()
    const task = vi.fn().mockRejectedValue(new Error('boom'))
    await runWorkerPool({
      jobs: [makeJob(1)],
      workerCount: 1,
      task,
      onProgress: vi.fn(),
      maxRetries: 1,
      isResumable: () => false,
      onWorkerError,
    })
    expect(onWorkerError).toHaveBeenCalledWith(0, expect.any(Error))
  })

  it('does not count network errors against job retry limit', async () => {
    const jobs = [makeJob(1)]
    let callCount = 0

    const task = vi.fn().mockImplementation(async () => {
      callCount++
      if (callCount <= 5) {
        // First 5 calls throw network error
        throw Object.assign(new Error('ECONNRESET'), { code: 'ECONNRESET' })
      }
      return { rowCount: 1, bytesWritten: 1 }
    })

    const results = await runWorkerPool({
      jobs,
      workerCount: 1,
      task,
      onProgress: vi.fn(),
      maxRetries: 3, // Only 3 retries for data errors
      isResumable: () => false,
      retryDelayMs: 0,
    })

    // Should succeed — network errors don't count against the retry limit
    expect(results).toHaveLength(1)
    expect(results[0]?.status).toBe('ok')
    expect(callCount).toBe(6)
  })

  it('fails immediately on data errors (no retry)', async () => {
    const jobs = [makeJob(1)]
    const task = vi.fn().mockRejectedValue(new Error('unique constraint violation'))

    const results = await runWorkerPool({
      jobs,
      workerCount: 1,
      task,
      onProgress: vi.fn(),
      maxRetries: 10,
      isResumable: () => false,
      retryDelayMs: 0,
    })

    expect(results[0]?.status).toBe('failed')
    expect(task).toHaveBeenCalledTimes(1) // no retry for data errors
  })
})
