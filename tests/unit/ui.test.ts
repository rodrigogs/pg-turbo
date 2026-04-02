import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'
import type { ChunkJob, ManifestTable, ProgressEvent, WorkerState } from '../../src/types/index.js'
import {
  createProgressHandler,
  installSignalHandlers,
  log,
  printBanner,
  printFailedTables,
  printSummary,
  renderDashboard,
  startDashboard,
} from '../../src/cli/ui.js'
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
  it('trims old speed samples from the ring buffer', () => {
    const now = Date.now()
    const state = makeDashboardState({
      speedSamples: [
        { time: now - 20_000, bytes: 100 },
        { time: now - 15_000, bytes: 200 },
        { time: now - 5_000, bytes: 400 },
      ],
    })
    renderDashboard(state)
    // Old samples (>10s) should have been trimmed
    expect(state.speedSamples.length).toBeLessThanOrEqual(3) // at most the recent one + new one
    // The newest surviving sample should be recent
    expect(state.speedSamples[0].time).toBeGreaterThan(now - 11_000)
  })

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

  it('sets interrupted flag and calls dashboard stop on SIGINT', () => {
    const stopFn = vi.fn()
    const mockDashboard = { update: vi.fn(), stop: stopFn }
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {})
    const { cleanup, wasInterrupted } = installSignalHandlers(() => mockDashboard)

    // Simulate SIGINT
    process.emit('SIGINT', 'SIGINT')

    expect(wasInterrupted()).toBe(true)
    expect(stopFn).toHaveBeenCalled()
    cleanup()
    logSpy.mockRestore()
  })

  it('handles SIGINT when dashboard is null', () => {
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {})
    const { cleanup, wasInterrupted } = installSignalHandlers(() => null)
    process.emit('SIGINT', 'SIGINT')
    expect(wasInterrupted()).toBe(true)
    cleanup()
    logSpy.mockRestore()
  })
})

describe('log helpers', () => {
  it('log.info writes to console.log', () => {
    const spy = vi.spyOn(console, 'log').mockImplementation(() => {})
    log.info('test info')
    expect(spy).toHaveBeenCalledTimes(1)
    expect(spy.mock.calls[0][0]).toContain('test info')
    spy.mockRestore()
  })

  it('log.success writes to console.log', () => {
    const spy = vi.spyOn(console, 'log').mockImplementation(() => {})
    log.success('test success')
    expect(spy).toHaveBeenCalledTimes(1)
    expect(spy.mock.calls[0][0]).toContain('test success')
    spy.mockRestore()
  })

  it('log.warn writes to console.log', () => {
    const spy = vi.spyOn(console, 'log').mockImplementation(() => {})
    log.warn('test warn')
    expect(spy).toHaveBeenCalledTimes(1)
    expect(spy.mock.calls[0][0]).toContain('test warn')
    spy.mockRestore()
  })

  it('log.error writes to console.error', () => {
    const spy = vi.spyOn(console, 'error').mockImplementation(() => {})
    log.error('test error')
    expect(spy).toHaveBeenCalledTimes(1)
    expect(spy.mock.calls[0][0]).toContain('test error')
    spy.mockRestore()
  })

  it('log.step writes to console.log', () => {
    const spy = vi.spyOn(console, 'log').mockImplementation(() => {})
    log.step('test step')
    expect(spy).toHaveBeenCalledTimes(1)
    expect(spy.mock.calls[0][0]).toContain('test step')
    spy.mockRestore()
  })
})

describe('printBanner', () => {
  it('prints a formatted banner', () => {
    const spy = vi.spyOn(console, 'log').mockImplementation(() => {})
    printBanner('Test Banner')
    expect(spy).toHaveBeenCalledTimes(4) // empty, title, separator, empty
    expect(spy.mock.calls[1][0]).toContain('Test Banner')
    spy.mockRestore()
  })
})

describe('printSummary', () => {
  beforeEach(() => {
    vi.spyOn(console, 'log').mockImplementation(() => {})
  })
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('prints basic summary', () => {
    const spy = console.log as ReturnType<typeof vi.fn>
    printSummary({
      title: 'Dump Complete',
      database: 'testdb',
      tableCount: 10,
      succeeded: 8,
      failed: 0,
      skipped: 0,
      durationSecs: 120,
      dryRun: false,
    })
    const allOutput = spy.mock.calls.map((c: any[]) => c[0]).join('\n')
    expect(allOutput).toContain('testdb')
    expect(allOutput).toContain('10 total')
  })

  it('prints dry run summary with different banner', () => {
    const spy = console.log as ReturnType<typeof vi.fn>
    printSummary({
      title: 'Dump Complete',
      database: 'testdb',
      tableCount: 5,
      succeeded: 5,
      failed: 0,
      skipped: 0,
      durationSecs: 30,
      dryRun: true,
    })
    const allOutput = spy.mock.calls.map((c: any[]) => c[0]).join('\n')
    expect(allOutput).toContain('Dry Run Summary')
  })

  it('prints schema filter when provided', () => {
    const spy = console.log as ReturnType<typeof vi.fn>
    printSummary({
      title: 'Dump',
      database: 'db',
      schema: 'public',
      tableCount: 5,
      succeeded: 5,
      failed: 0,
      skipped: 0,
      durationSecs: 10,
      dryRun: false,
    })
    const allOutput = spy.mock.calls.map((c: any[]) => c[0]).join('\n')
    expect(allOutput).toContain('public')
  })

  it('prints skipped count when > 0', () => {
    const spy = console.log as ReturnType<typeof vi.fn>
    printSummary({
      title: 'Restore',
      database: 'db',
      tableCount: 10,
      succeeded: 7,
      failed: 0,
      skipped: 3,
      durationSecs: 60,
      dryRun: false,
    })
    const allOutput = spy.mock.calls.map((c: any[]) => c[0]).join('\n')
    expect(allOutput).toContain('3 skipped')
  })

  it('prints failed count when > 0', () => {
    const spy = console.log as ReturnType<typeof vi.fn>
    printSummary({
      title: 'Restore',
      database: 'db',
      tableCount: 10,
      succeeded: 8,
      failed: 2,
      skipped: 0,
      durationSecs: 60,
      dryRun: false,
    })
    const allOutput = spy.mock.calls.map((c: any[]) => c[0]).join('\n')
    expect(allOutput).toContain('2')
  })

  it('prints output dir and size when provided', () => {
    const spy = console.log as ReturnType<typeof vi.fn>
    printSummary({
      title: 'Dump',
      database: 'db',
      tableCount: 5,
      succeeded: 5,
      failed: 0,
      skipped: 0,
      durationSecs: 60,
      outputDir: '/tmp/dump',
      outputSize: '1.5 GB',
      dryRun: false,
    })
    const allOutput = spy.mock.calls.map((c: any[]) => c[0]).join('\n')
    expect(allOutput).toContain('/tmp/dump')
    expect(allOutput).toContain('1.5 GB')
  })

  it('prints output dir without size', () => {
    const spy = console.log as ReturnType<typeof vi.fn>
    printSummary({
      title: 'Dump',
      database: 'db',
      tableCount: 5,
      succeeded: 5,
      failed: 0,
      skipped: 0,
      durationSecs: 60,
      outputDir: '/tmp/dump',
      dryRun: false,
    })
    const allOutput = spy.mock.calls.map((c: any[]) => c[0]).join('\n')
    expect(allOutput).toContain('/tmp/dump')
  })
})

describe('printFailedTables', () => {
  it('prints failed table labels and errors', () => {
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {})
    const warnSpy = vi.spyOn(console, 'log').mockImplementation(() => {})
    printFailedTables(
      [
        { label: 'public.users chunk 0', error: 'connection timeout' },
        { label: 'public.orders chunk 1' },
      ],
      5,
    )
    const allOutput = logSpy.mock.calls.map((c: any[]) => c[0]).join('\n')
    expect(allOutput).toContain('public.users chunk 0')
    expect(allOutput).toContain('connection timeout')
    expect(allOutput).toContain('public.orders chunk 1')
    logSpy.mockRestore()
    warnSpy.mockRestore()
  })
})

describe('startDashboard', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  it('starts an interval that renders the dashboard and stops cleanly', () => {
    const state = makeDashboardState()
    const { update, stop } = startDashboard(state)
    expect(typeof update).toBe('function')
    expect(typeof stop).toBe('function')
    // Advance time to trigger the interval
    vi.advanceTimersByTime(250)
    // Should not throw
    update()
    stop()
  })
})

describe('renderDashboard worker speed with rows unit', () => {
  it('formats speed with rows/s suffix', () => {
    const workers = [makeWorker(0)]
    workers[0].status = 'working'
    workers[0].currentJob = makeJob(1)
    workers[0].progressCurrent = 5000
    workers[0].progressTotal = 10000
    workers[0].speedSnapshot = { time: Date.now() - 2000, current: 1000 }
    const state = makeDashboardState({ workers, progressUnit: 'rows' })
    const output = renderDashboard(state)
    expect(output).toContain('rows/s')
  })
})

describe('formatProgress rows unit branches', () => {
  it('formats rows >= 1M as M', () => {
    const workers = [makeWorker(0)]
    workers[0].status = 'working'
    workers[0].currentJob = makeJob(1)
    workers[0].progressCurrent = 2_500_000
    workers[0].progressTotal = 5_000_000
    const state = makeDashboardState({ workers, progressUnit: 'rows' })
    const output = renderDashboard(state)
    // Should contain M suffix for million-scale rows
    expect(output).toContain('M')
    expect(output).toContain('rows')
  })

  it('formats rows >= 1K as K', () => {
    const workers = [makeWorker(0)]
    workers[0].status = 'working'
    workers[0].currentJob = makeJob(1)
    workers[0].progressCurrent = 5_000
    workers[0].progressTotal = 10_000
    const state = makeDashboardState({ workers, progressUnit: 'rows' })
    const output = renderDashboard(state)
    expect(output).toContain('K')
    expect(output).toContain('rows')
  })

  it('formats small row counts as plain numbers', () => {
    const workers = [makeWorker(0)]
    workers[0].status = 'working'
    workers[0].currentJob = makeJob(1)
    workers[0].progressCurrent = 5
    workers[0].progressTotal = 10
    const state = makeDashboardState({ workers, progressUnit: 'rows' })
    const output = renderDashboard(state)
    expect(output).toContain('rows')
  })

  it('formats rows >= 10 as rounded integers', () => {
    const workers = [makeWorker(0)]
    workers[0].status = 'working'
    workers[0].currentJob = makeJob(1)
    workers[0].progressCurrent = 50
    workers[0].progressTotal = 100
    const state = makeDashboardState({ workers, progressUnit: 'rows' })
    const output = renderDashboard(state)
    expect(output).toContain('rows')
  })
})

describe('renderDashboard with worker lastSpeed set', () => {
  it('uses lastSpeed when speedSnapshot delta < 1s', () => {
    const workers = [makeWorker(0)]
    workers[0].status = 'working'
    workers[0].currentJob = makeJob(1)
    workers[0].progressCurrent = 500
    workers[0].progressTotal = 1000
    workers[0].speedSnapshot = { time: Date.now() - 500, current: 400 } // < 1s
    workers[0].lastSpeed = 250
    const state = makeDashboardState({ workers })
    const output = renderDashboard(state)
    // Should still render without errors and use lastSpeed
    expect(typeof output).toBe('string')
  })
})

describe('renderDashboard multi-chunk table', () => {
  it('shows chunk label for multi-chunk tables', () => {
    const chunk1 = { index: 0, file: 'data/public.big/chunk_0000.copy.lz4', estimatedBytes: 500, estimatedRows: 5 }
    const chunk2 = { index: 1, file: 'data/public.big/chunk_0001.copy.lz4', estimatedBytes: 500, estimatedRows: 5 }
    const table: ManifestTable = {
      schema: 'public',
      name: 'big',
      oid: 1,
      relkind: 'r',
      estimatedBytes: 1000,
      estimatedRows: 10,
      pkColumn: 'id',
      pkType: 'int8',
      chunkStrategy: 'pk_range',
      columns: ['id'],
      generatedColumns: [],
      chunks: [chunk1, chunk2],
    }
    const job: ChunkJob = { table, chunk: chunk1, outputPath: '/tmp/test', attempt: 0 }
    const workers = [makeWorker(0)]
    workers[0].status = 'working'
    workers[0].currentJob = job
    workers[0].progressCurrent = 200
    workers[0].progressTotal = 500
    const state = makeDashboardState({ workers })
    const output = renderDashboard(state)
    expect(output).toContain('1/2')
  })
})
