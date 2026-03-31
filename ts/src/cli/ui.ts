import pc from 'picocolors'
import logUpdate from 'log-update'
import { createSpinner } from 'nanospinner'
import { humanSize, elapsedTime, progressBar } from '../core/format.js'
import type { WorkerState } from '../types/index.js'

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

export function spinner(text: string) { return createSpinner(text, { color: 'cyan' }) }

export interface DashboardState {
  totalBytes: number; processedBytes: number; startTime: number; workers: WorkerState[]
}

export function renderDashboard(state: DashboardState): string {
  const elapsed = Math.max(1, (Date.now() - state.startTime) / 1000)
  const speed = state.processedBytes / elapsed
  const eta = speed > 0 ? Math.ceil((state.totalBytes - state.processedBytes) / speed) : 0
  const bar = progressBar(state.processedBytes, state.totalBytes, 30)
  const header = `${bar} ${pc.dim(`— ${humanSize(speed)}/s — ETA: ${elapsedTime(eta)}`)}`
  const workerLines = state.workers.map(w => {
    if (w.status === 'idle' || !w.currentJob) return `  Worker ${w.id}: ${pc.dim('idle')}`
    const { table, chunk } = w.currentJob
    const label = `${table.schema}.${table.name}`
    const chunkLabel = w.currentJob.table.chunks.length > 1 ? ` chunk ${chunk.index + 1}/${w.currentJob.table.chunks.length}` : ''
    const speedLabel = w.speed ? ` (${humanSize(w.speed)}/s)` : ''
    if (w.status === 'retrying') return `  Worker ${w.id}: ${pc.yellow('\u21BB')} ${pc.bold(label)}${chunkLabel} ${pc.dim('retrying...')}`
    return `  Worker ${w.id}: ${pc.bold(label)}${chunkLabel}${pc.dim(speedLabel)}`
  })
  return [header, ...workerLines].join('\n')
}

export function startDashboard(state: DashboardState): { update: () => void; stop: () => void } {
  const interval = setInterval(() => { logUpdate(renderDashboard(state)) }, 100)
  return {
    update: () => logUpdate(renderDashboard(state)),
    stop: () => { clearInterval(interval); logUpdate(renderDashboard(state)); logUpdate.done() },
  }
}

export function printSummary(opts: {
  title: string; database: string; schema?: string; tableCount: number
  succeeded: number; failed: number; skipped: number; durationSecs: number
  outputDir?: string; outputSize?: string; dryRun: boolean
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
  if (opts.outputDir) console.log(`  ${pc.dim('Output:')}      ${opts.outputDir}${opts.outputSize ? ` (${opts.outputSize})` : ''}`)
  console.log('')
}

export function printFailedTables(tables: string[], maxRetries: number): void {
  log.warn(`The following chunks failed after ${maxRetries} retries:`)
  for (const t of tables) console.log(`    ${pc.red('\u2716')} ${t}`)
  console.log('')
  log.info('Re-run with the same arguments to retry only the failed chunks.')
}
