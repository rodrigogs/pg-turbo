export function humanSize(bytes: number): string {
  if (bytes >= 1_073_741_824) return `${(bytes / 1_073_741_824).toFixed(2)} GB`
  if (bytes >= 1_048_576) return `${(bytes / 1_048_576).toFixed(1)} MB`
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${bytes} B`
}

export function elapsedTime(secs: number): string {
  if (secs >= 3600) {
    const h = Math.floor(secs / 3600)
    const m = Math.floor((secs % 3600) / 60)
    const s = secs % 60
    return `${h}h ${m}m ${s}s`
  }
  if (secs >= 60) {
    const m = Math.floor(secs / 60)
    const s = secs % 60
    return `${m}m ${s}s`
  }
  return `${secs}s`
}

export function progressBar(current: number, total: number, width: number = 30): string {
  const pct = total > 0 ? Math.min(Math.floor((current * 100) / total), 100) : 0
  const filled = total > 0 ? Math.max(0, Math.min(Math.floor((current * width) / total), width)) : 0
  const empty = width - filled
  const bar = '\u2588'.repeat(filled) + '\u2591'.repeat(empty)
  const status = total > 1024
    ? `(${humanSize(current)} / ${humanSize(total)})`
    : `(${current}/${total})`
  return `[${bar}] ${pct.toString().padStart(3)}% ${status}`
}
