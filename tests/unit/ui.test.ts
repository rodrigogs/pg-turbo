import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'
import type { ChunkJob, ManifestTable, ProgressEvent, WorkerState } from '../../src/types/index.js'
import { createProgressHandler, installSignalHandlers, renderDashboard } from '../../src/cli/ui.js'
import type { DashboardState } from '../../src/cli/ui.js'

function makeWorker(id: number): WorkerState {
  return {
    id,
    status: 'idle',
    progressCurrent: 0,
    progressTotal: 0,
  }
}

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
    outputPath: `/tmp/test/data/public.table_${index}/chunk_0000.copy.lz4`,
    attempt: 0,
  }
}

function makeDashboardState(overrides: Partial<DashboardState> = {}): DashboardState {
  return {
    totalBytes: 10000,
    processedBytes: 5000,
    startTime: Date.now() - 10_000,
    workers: [makeWorker(0), makeWorker(1)],
    completedChunks: 5,
    totalChunks: 10,
    failedChunks: 0,
    skippedChunks: 0,
    progressUnit: 'bytes',
    speedSamples: [],
    ...overrides,
  }
}

describe('renderDashboard', () => {
  it('renders a dashboard string with progress info', () => {
    const state = makeDashboardState()
    const output = renderDashboard(state)
    expect(typeof output).toBe('string')
    expect(output).toContain('5/10 chunks')
    expect(output).toContain('W0')
    expect(output).toContain('W1')
  })

  it('shows failed chunks count', () => {
    const state = makeDashboardState({ failedChunks: 2 })
    const output = renderDashboard(state)
    expect(output).toContain('2 failed')
  })

  it('shows skipped/resumed chunks count', () => {
    const state = makeDashboardState({ skippedChunks: 3 })
    const output = renderDashboard(state)
    expect(output).toContain('3 resumed')
  })

  it('shows worker status when working', () => {
    const workers = [makeWorker(0)]
    const job = makeJob(1)
    workers[0].status = 'working'
    workers[0].currentJob = job
    workers[0].progressCurrent = 500
    workers[0].progressTotal = 1000
    const state = makeDashboardState({ workers })
    const output = renderDashboard(state)
    expect(output).toContain('public.table_1')
  })

  it('shows retrying status', () => {
    const workers = [makeWorker(0)]
    const job = makeJob(1)
    job.attempt = 2
    workers[0].status = 'retrying'
    workers[0].currentJob = job
    const state = makeDashboardState({ workers })
    const output = renderDashboard(state)
    expect(output).toContain('retry')
  })

  it('shows done when all bytes processed', () => {
    const state = makeDashboardState({ processedBytes: 10000, totalBytes: 10000 })
    const output = renderDashboard(state)
    expect(output).toContain('done')
  })
})

describe('createProgressHandler', () => {
  it('handles started event', () => {
    const workers = [makeWorker(0)]
    const dashState = makeDashboardState({ workers })
    const dashboard = { update: vi.fn() }
    const handler = createProgressHandler(
      workers,
      dashState,
      dashboard,
      () => 1000,
      () => 1000,
    )
    const job = makeJob(1)
    handler({ type: 'started', workerId: 0, job })
    expect(workers[0].status).toBe('working')
    expect(workers[0].currentJob).toBe(job)
    expect(dashboard.update).toHaveBeenCalled()
  })

  it('handles completed event', () => {
    const workers = [makeWorker(0)]
    const dashState = makeDashboardState({ workers, processedBytes: 0, completedChunks: 0 })
    const handler = createProgressHandler(
      workers,
      dashState,
      null,
      () => 500,
      () => 1000,
    )
    handler({ type: 'completed', workerId: 0, job: makeJob(1), bytesWritten: 500 })
    expect(workers[0].status).toBe('idle')
    expect(dashState.processedBytes).toBe(500)
    expect(dashState.completedChunks).toBe(1)
  })

  it('handles retrying event', () => {
    const workers = [makeWorker(0)]
    workers[0].status = 'working'
    workers[0].progressCurrent = 300
    const dashState = makeDashboardState({ workers })
    const handler = createProgressHandler(
      workers,
      dashState,
      null,
      () => 0,
      () => 1000,
    )
    handler({ type: 'retrying', workerId: 0, job: makeJob(1), error: new Error('timeout') })
    expect(workers[0].status).toBe('retrying')
    expect(workers[0].progressCurrent).toBe(0)
  })

  it('handles failed event', () => {
    const workers = [makeWorker(0)]
    const dashState = makeDashboardState({ workers, failedChunks: 0 })
    const handler = createProgressHandler(
      workers,
      dashState,
      null,
      () => 0,
      () => 1000,
    )
    handler({ type: 'failed', workerId: 0, job: makeJob(1), error: new Error('fatal') })
    expect(workers[0].status).toBe('idle')
    expect(dashState.failedChunks).toBe(1)
  })

  it('handles skipped event', () => {
    const workers = [makeWorker(0)]
    const dashState = makeDashboardState({ workers, processedBytes: 0, skippedChunks: 0 })
    const handler = createProgressHandler(
      workers,
      dashState,
      null,
      () => 800,
      () => 1000,
    )
    handler({ type: 'skipped', workerId: 0, job: makeJob(1) })
    expect(workers[0].status).toBe('idle')
    expect(dashState.processedBytes).toBe(800)
    expect(dashState.skippedChunks).toBe(1)
  })

  it('ignores events for unknown worker IDs', () => {
    const workers = [makeWorker(0)]
    const dashState = makeDashboardState({ workers })
    const handler = createProgressHandler(
      workers,
      dashState,
      null,
      () => 0,
      () => 0,
    )
    // Should not throw for non-existent worker
    handler({ type: 'started', workerId: 99, job: makeJob(1) })
  })
})

describe('renderDashboard edge cases', () => {
  it('handles zero totalBytes', () => {
    const state = makeDashboardState({ totalBytes: 0, processedBytes: 0 })
    const output = renderDashboard(state)
    expect(typeof output).toBe('string')
  })

  it('renders with rows progressUnit', () => {
    const workers = [makeWorker(0)]
    workers[0].status = 'working'
    workers[0].currentJob = makeJob(1)
    workers[0].progressCurrent = 500
    workers[0].progressTotal = 1000
    const state = makeDashboardState({ workers, progressUnit: 'rows' })
    const output = renderDashboard(state)
    expect(output).toContain('rows')
  })

  it('renders worker speed when speedSnapshot exists and delta >= 1s', () => {
    const workers = [makeWorker(0)]
    workers[0].status = 'working'
    workers[0].currentJob = makeJob(1)
    workers[0].progressCurrent = 500
    workers[0].progressTotal = 1000
    workers[0].speedSnapshot = { time: Date.now() - 2000, current: 100 }
    const state = makeDashboardState({ workers })
    const output = renderDashboard(state)
    expect(typeof output).toBe('string')
  })

  it('renders worker speed when speedSnapshot delta >= 5s (resets snapshot)', () => {
    const workers = [makeWorker(0)]
    workers[0].status = 'working'
    workers[0].currentJob = makeJob(1)
    workers[0].progressCurrent = 800
    workers[0].progressTotal = 1000
    workers[0].speedSnapshot = { time: Date.now() - 6000, current: 100 }
    const state = makeDashboardState({ workers })
    renderDashboard(state)
    // After rendering, speedSnapshot should be reset to recent values
    expect(workers[0].speedSnapshot!.current).toBe(800)
  })
})

describe('installSignalHandlers', () => {
  it('returns cleanup and wasInterrupted functions', () => {
    const { cleanup, wasInterrupted } = installSignalHandlers(() => null)
    expect(typeof cleanup).toBe('function')
    expect(typeof wasInterrupted).toBe('function')
    expect(wasInterrupted()).toBe(false)
    cleanup()
  })
})
