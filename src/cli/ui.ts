import logUpdate from 'log-update'
import pc from 'picocolors'
import { chunkEstimatedBytes } from '../core/chunker.js'
import { computeBar, elapsedTime, humanSize, progressBar } from '../core/format.js'
import type { ProgressEvent, WorkerState } from '../types/index.js'

export const log = {
  info: (msg: string) => console.log(`${pc.blue('\u2139')}  ${msg}`),
  success: (msg: string) => console.log(`${pc.green('\u2714')}  ${msg}`),
  warn: (msg: string) => console.log(`${pc.yellow('\u26A0')}  ${msg}`),
  error: (msg: string) => console.error(`${pc.red('\u2716')}  ${msg}`),
  step: (msg: string) => console.log(`${pc.cyan('\u25B8')}  ${pc.bold(msg)}`),
}

export function printBanner(title: string, color: (s: string) => string = pc.cyan): void {
  console.log('')
  console.log(`${color(`  ${title}`)}`)
  console.log(`${pc.dim(`  ${'─'.repeat(title.length)}`)}`)
  console.log('')
}

export interface DashboardState {
  totalBytes: number
  processedBytes: number
  startTime: number
  workers: WorkerState[]
  completedChunks: number
  totalChunks: number
  failedChunks: number
  skippedChunks: number
  progressUnit: 'bytes' | 'rows'
  /** Ring buffer for rolling speed calculation (last ~10s) */
  speedSamples: Array<{ time: number; bytes: number }>
}

const SPINNER = ['\u280B', '\u2819', '\u2839', '\u2838', '\u283C', '\u2834', '\u2826', '\u2827', '\u2807', '\u280F']

function formatProgress(value: number, unit: 'bytes' | 'rows'): string {
  if (unit === 'bytes') return humanSize(value)
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`
  return value >= 10 ? String(Math.round(value)) : value.toFixed(1)
}

function miniProgressBar(current: number, total: number, width: number, unit: 'bytes' | 'rows'): string {
  if (total <= 0) return pc.dim(`[${'·'.repeat(width)}]`)
  const { pct, filled, empty } = computeBar(current, total, width)
  const label = `${formatProgress(current, unit)}/${formatProgress(total, unit)}${unit === 'rows' ? ' rows' : ''}`
  return `[${pc.green(filled)}${pc.dim(empty)}] ${pc.dim(`${pct.toString().padStart(3)}% ${label}`)}`
}

export function renderDashboard(state: DashboardState): string {
  const elapsed = Math.max(1, (Date.now() - state.startTime) / 1000)

  // Include in-flight progress from active workers for smooth real-time updates.
  // Use each worker's completion ratio (progressCurrent/progressTotal) scaled to the
  // chunk's share of totalBytes, so units stay consistent with processedBytes.
  const inFlightEstimate = state.workers.reduce((sum, w) => {
    if (w.status !== 'working' || !w.currentJob || w.progressTotal <= 0) return sum
    const ratio = Math.min(w.progressCurrent / w.progressTotal, 1)
    const chunkShare = chunkEstimatedBytes(w.currentJob)
    return sum + ratio * chunkShare
  }, 0)
  const effectiveBytes = Math.min(state.processedBytes + inFlightEstimate, state.totalBytes)

  // Rolling speed: use a 10s window for responsive, accurate speed/ETA
  const now = Date.now()
  state.speedSamples.push({ time: now, bytes: effectiveBytes })
  const cutoff = now - 10_000
  while (state.speedSamples.length > 1 && (state.speedSamples[0]?.time ?? 0) < cutoff) {
    state.speedSamples.shift()
  }
  const oldest = state.speedSamples[0] ?? { time: now, bytes: 0 }
  const windowSecs = Math.max(1, (now - oldest.time) / 1000)
  const speed = (effectiveBytes - oldest.bytes) / windowSecs
  const eta = speed > 0 ? Math.ceil((state.totalBytes - effectiveBytes) / speed) : 0
  const spinIdx = Math.floor(Date.now() / 80) % SPINNER.length
  const spinner = pc.cyan(SPINNER[spinIdx] ?? '')

  // Progress bar line
  const bar = progressBar(effectiveBytes, state.totalBytes, 30)
  const allDone = effectiveBytes >= state.totalBytes

  // Chunk counter
  const done = state.completedChunks + state.skippedChunks
  let chunkStatus = `${done}/${state.totalChunks} chunks`
  if (state.failedChunks > 0) chunkStatus += pc.red(` (${state.failedChunks} failed)`)
  if (state.skippedChunks > 0) chunkStatus += pc.dim(` (${state.skippedChunks} resumed)`)

  // Speed + timing
  const speedStr = speed > 0 ? `${humanSize(speed)}/s` : 'calculating...'
  const etaStr = allDone ? 'done' : speed > 0 ? elapsedTime(eta) : '--:--'
  const timingLine = `  ${pc.dim('Elapsed:')} ${elapsedTime(Math.round(elapsed))}  ${pc.dim('ETA:')} ${etaStr}  ${pc.dim('Speed:')} ${speedStr}`

  const header = `${spinner} ${bar}  ${pc.dim(chunkStatus)}`

  // Worker lines
  const workerLines = state.workers.map((w) => {
    const prefix = `    ${pc.dim(`W${w.id}`)}`
    if (w.status === 'idle' || !w.currentJob) return `${prefix} ${pc.dim('\u2500 idle')}`
    const { table, chunk } = w.currentJob
    const label = `${table.schema}.${table.name}`
    const chunkLabel = table.chunks.length > 1 ? pc.dim(` [${chunk.index + 1}/${table.chunks.length}]`) : ''
    if (w.status === 'retrying') {
      const attempt = w.currentJob.attempt
      return `${prefix} ${pc.yellow('\u21BB')} ${pc.bold(label)}${chunkLabel} ${pc.yellow(`retry ${attempt}`)}`
    }
    const miniBar = miniProgressBar(w.progressCurrent, w.progressTotal, 12, state.progressUnit)
    // Rolling speed: compute from a ~5s sliding anchor instead of lifetime average
    let workerSpeed = w.lastSpeed ?? 0
    if (w.speedSnapshot) {
      const dt = (now - w.speedSnapshot.time) / 1000
      if (dt >= 1) {
        workerSpeed = (w.progressCurrent - w.speedSnapshot.current) / dt
        w.lastSpeed = workerSpeed
      }
      if (dt >= 5) {
        w.speedSnapshot = { time: now, current: w.progressCurrent }
      }
    } else {
      w.speedSnapshot = { time: now, current: w.progressCurrent }
    }
    const speedSuffix = state.progressUnit === 'rows' ? ' rows/s' : '/s'
    const speedLabel =
      workerSpeed > 0 ? pc.dim(` ${formatProgress(workerSpeed, state.progressUnit)}${speedSuffix}`) : ''
    return `${prefix} ${pc.green('\u25B6')} ${pc.bold(label)}${chunkLabel} ${miniBar}${speedLabel}`
  })

  return [header, timingLine, ...workerLines].join('\n')
}

export function startDashboard(state: DashboardState): { update: () => void; stop: () => void } {
  const interval = setInterval(() => {
    logUpdate(renderDashboard(state))
  }, 100)
  return {
    update: () => logUpdate(renderDashboard(state)),
    stop: () => {
      clearInterval(interval)
      logUpdate(renderDashboard(state))
      logUpdate.done()
    },
  }
}

export function printSummary(opts: {
  title: string
  database: string
  schema?: string
  tableCount: number
  succeeded: number
  failed: number
  skipped: number
  durationSecs: number
  outputDir?: string
  outputSize?: string
  dryRun: boolean
}): void {
  console.log('')
  printBanner(opts.dryRun ? 'Dry Run Summary' : opts.title, opts.dryRun ? pc.yellow : pc.cyan)
  console.log(`  ${pc.dim('Database:')}    ${pc.bold(opts.database)}`)
  if (opts.schema) console.log(`  ${pc.dim('Schema:')}      ${pc.bold(opts.schema)}`)
  console.log(`  ${pc.dim('Tables:')}      ${opts.tableCount} total`)
  let line = `  ${pc.green('Succeeded:')}   ${opts.succeeded}`
  if (opts.skipped > 0) line += ` ${pc.dim(`(${opts.skipped} skipped/resumed)`)}`
  console.log(line)
  if (opts.failed > 0) console.log(`  ${pc.red('Failed:')}      ${opts.failed}`)
  console.log(`  ${pc.dim('Duration:')}    ${elapsedTime(opts.durationSecs)}`)
  if (opts.outputDir)
    console.log(`  ${pc.dim('Output:')}      ${opts.outputDir}${opts.outputSize ? ` (${opts.outputSize})` : ''}`)
  console.log('')
}

export function printFailedTables(failures: Array<{ label: string; error?: string }>, maxRetries: number): void {
  log.warn(`The following chunks failed after ${maxRetries} retries:`)
  for (const f of failures) {
    console.log(`    ${pc.red('\u2716')} ${f.label}`)
    if (f.error) console.log(`      ${pc.dim(f.error)}`)
  }
  console.log('')
  log.info('Re-run with the same arguments to retry only the failed chunks.')
}

export function createProgressHandler(
  workers: WorkerState[],
  dashState: DashboardState,
  dashboard: { update: () => void } | null,
  bytesForCompleted: (event: ProgressEvent) => number,
  chunkProgressTotal: (job: ProgressEvent['job']) => number,
): (event: ProgressEvent) => void {
  function resetWorker(w: WorkerState): void {
    w.status = 'idle'
    w.currentJob = undefined
    w.startedAt = undefined
    w.progressCurrent = 0
    w.progressTotal = 0
    w.speedSnapshot = undefined
    w.lastSpeed = undefined
  }

  return (event: ProgressEvent) => {
    const w = workers[event.workerId]
    if (!w) return
    switch (event.type) {
      case 'started':
        w.status = 'working'
        w.currentJob = event.job
        w.startedAt = Date.now()
        w.progressCurrent = 0
        w.progressTotal = chunkProgressTotal(event.job)
        break
      case 'completed':
        resetWorker(w)
        dashState.processedBytes += bytesForCompleted(event)
        dashState.completedChunks++
        break
      case 'retrying':
        w.status = 'retrying'
        w.progressCurrent = 0
        break
      case 'failed':
        resetWorker(w)
        dashState.failedChunks++
        break
      case 'skipped':
        resetWorker(w)
        dashState.processedBytes += bytesForCompleted(event)
        dashState.skippedChunks++
        break
    }
    dashboard?.update()
  }
}

export function installSignalHandlers(getDashboard: () => ReturnType<typeof startDashboard> | null): {
  cleanup: () => void
  wasInterrupted: () => boolean
} {
  let interrupted = false
  const handler = () => {
    interrupted = true
    getDashboard()?.stop()
    console.log('')
    log.warn('Interrupted — cleaning up...')
  }
  process.once('SIGINT', handler)
  process.once('SIGTERM', handler)
  return {
    cleanup: () => {
      process.removeListener('SIGINT', handler)
      process.removeListener('SIGTERM', handler)
    },
    wasInterrupted: () => interrupted,
  }
}
