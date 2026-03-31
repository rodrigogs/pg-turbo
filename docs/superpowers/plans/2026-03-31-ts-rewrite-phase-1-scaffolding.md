# Phase 1: Project Scaffolding, Types & Pure Utilities

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Set up the TypeScript project with working toolchain, shared types, and pure utility modules (no database dependencies).

**Architecture:** Create `ts/` subdirectory with pnpm, TypeScript strict mode, vitest, and ESM. Build foundation modules that have zero dependencies and are trivially testable.

**Tech Stack:** TypeScript 5.x, pnpm, vitest, picocolors

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `ts/package.json` | Package manifest, scripts, ESM config |
| Create | `ts/tsconfig.json` | TypeScript strict config |
| Create | `ts/vitest.config.ts` | Unit test config |
| Create | `ts/src/types/index.ts` | All shared type definitions |
| Create | `ts/src/core/format.ts` | `humanSize`, `elapsedTime`, `progressBar` |
| Create | `ts/tests/unit/format.test.ts` | Tests for format utilities |
| Create | `ts/src/core/retry.ts` | `retryWithBackoff`, `calculateDelay` |
| Create | `ts/tests/unit/retry.test.ts` | Tests for retry logic |

---

### Task 1: Initialize project

**Files:**
- Create: `ts/package.json`
- Create: `ts/tsconfig.json`
- Create: `ts/vitest.config.ts`
- Create: `ts/.gitignore`

- [ ] **Step 1: Create package.json**

```json
{
  "name": "pg-resilient",
  "version": "0.1.0",
  "description": "Resilient PostgreSQL dump & restore with direct COPY protocol",
  "type": "module",
  "engines": { "node": ">=20" },
  "scripts": {
    "typecheck": "tsc --noEmit",
    "test": "vitest run",
    "test:watch": "vitest",
    "test:integration": "vitest run --config vitest.integration.config.ts",
    "dev": "tsx src/cli/index.ts"
  }
}
```

- [ ] **Step 2: Create tsconfig.json**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "noImplicitOverride": true,
    "verbatimModuleSyntax": true,
    "isolatedModules": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "outDir": "dist",
    "rootDir": "src",
    "noEmit": true
  },
  "include": ["src"]
}
```

- [ ] **Step 3: Create vitest.config.ts**

```typescript
import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['tests/unit/**/*.test.ts'],
    coverage: {
      provider: 'v8',
      include: ['src/**/*.ts'],
      exclude: ['src/types/**'],
    },
  },
})
```

- [ ] **Step 4: Create .gitignore**

```
node_modules/
dist/
*.tsbuildinfo
coverage/
```

- [ ] **Step 5: Install dependencies**

Run from `ts/` directory:
```bash
pnpm init  # if package.json needs regeneration
pnpm add commander @commander-js/extra-typings pg pg-copy-streams lz4 picocolors nanospinner log-update nano-spawn
pnpm add -D typescript vitest tsx @types/pg @types/pg-copy-streams @types/lz4
```

- [ ] **Step 6: Create directory structure**

```bash
mkdir -p src/cli src/core src/types tests/unit tests/integration bin
```

- [ ] **Step 7: Verify toolchain works**

Run: `pnpm typecheck && pnpm test`

Expected: tsc exits 0 (no source files yet is OK), vitest exits 0 with "No test files found".

- [ ] **Step 8: Commit**

```bash
git add ts/
git commit -m "feat(ts): initialize TypeScript project with pnpm, vitest, strict config"
```

---

### Task 2: Shared types

**Files:**
- Create: `ts/src/types/index.ts`

- [ ] **Step 1: Write all shared type definitions**

```typescript
// ts/src/types/index.ts

/** Table metadata from pg_catalog discovery */
export interface TableInfo {
  oid: number
  schemaName: string
  tableName: string
  relkind: 'r' | 'm'               // regular table or materialized view
  relpages: number
  estimatedRows: number
  actualBytes: number
  pkColumn: string | null
  pkType: 'int2' | 'int4' | 'int8' | null
  columns: string[]
  generatedColumns: string[]
}

/** How a table is split for parallel dump */
export type ChunkStrategy = 'pk_range' | 'ctid_range' | 'none'

/** A single chunk within a table dump */
export interface ChunkMeta {
  index: number
  file: string
  rangeStart?: number               // PK range start (for pk_range strategy)
  rangeEnd?: number                 // PK range end
  ctidStart?: number                // Page start (for ctid_range strategy)
  ctidEnd?: number                  // Page end (open-ended for last chunk)
}

/** Table entry in the manifest */
export interface ManifestTable {
  schema: string
  name: string
  oid: number
  relkind: 'r' | 'm'
  estimatedBytes: number
  estimatedRows: number
  pkColumn: string | null
  pkType: string | null
  chunkStrategy: ChunkStrategy
  columns: string[]
  generatedColumns: string[]
  chunks: ChunkMeta[]
}

/** Sequence state for restore */
export interface SequenceInfo {
  schema: string
  name: string
  lastValue: number
  isCalled: boolean
}

/** The dump manifest.json structure */
export interface DumpManifest {
  version: 1
  tool: 'pg-resilient'
  createdAt: string                 // ISO 8601
  pgVersion: string
  database: string
  snapshotId: string | null
  compression: 'lz4'
  options: {
    schemaFilter: string | null
    splitThresholdBytes: number
    jobs: number
  }
  tables: ManifestTable[]
  sequences: SequenceInfo[]
}

/** A job in the worker queue */
export interface ChunkJob {
  table: ManifestTable
  chunk: ChunkMeta
  copyQuery: string
  outputPath: string
  attempt: number
}

/** Result of processing a single chunk */
export interface ChunkResult {
  job: ChunkJob
  status: 'ok' | 'skipped' | 'failed'
  rowCount?: number
  bytesWritten?: number
  error?: Error
  durationMs?: number
}

/** Progress event emitted by workers */
export interface ProgressEvent {
  type: 'started' | 'completed' | 'skipped' | 'failed' | 'retrying'
  workerId: number
  job: ChunkJob
  bytesWritten?: number
  error?: Error
}

/** Worker state for dashboard display */
export interface WorkerState {
  id: number
  status: 'idle' | 'working' | 'retrying'
  currentJob?: ChunkJob
  speed?: number                    // bytes per second
}

/** Dump command options (parsed from CLI) */
export interface DumpOptions {
  dbname: string
  output: string
  schema?: string
  jobs: number
  splitThreshold: number            // bytes
  maxChunksPerTable: number
  retries: number
  retryDelay: number                // seconds
  noSnapshot: boolean
  dryRun: boolean
  pgDumpArgs: string[]
}

/** Restore command options (parsed from CLI) */
export interface RestoreOptions {
  dbname: string
  input: string
  schema?: string
  table?: string
  jobs: number
  clean: boolean
  dataOnly: boolean
  retries: number
  retryDelay: number                // seconds
  dryRun: boolean
  pgRestoreArgs: string[]
}
```

- [ ] **Step 2: Verify types compile**

Run: `pnpm typecheck`

Expected: exits 0 with no errors.

- [ ] **Step 3: Commit**

```bash
git add ts/src/types/
git commit -m "feat(ts): add shared type definitions for manifest, chunks, options"
```

---

### Task 3: Format utilities

**Files:**
- Create: `ts/src/core/format.ts`
- Create: `ts/tests/unit/format.test.ts`

- [ ] **Step 1: Write failing tests**

```typescript
// ts/tests/unit/format.test.ts
import { describe, it, expect } from 'vitest'
import { humanSize, elapsedTime, progressBar } from '../../src/core/format.js'

describe('humanSize', () => {
  it('formats bytes', () => {
    expect(humanSize(0)).toBe('0 B')
    expect(humanSize(500)).toBe('500 B')
    expect(humanSize(1023)).toBe('1023 B')
  })

  it('formats kilobytes', () => {
    expect(humanSize(1024)).toBe('1.0 KB')
    expect(humanSize(1536)).toBe('1.5 KB')
  })

  it('formats megabytes', () => {
    expect(humanSize(1048576)).toBe('1.0 MB')
    expect(humanSize(2621440)).toBe('2.5 MB')
  })

  it('formats gigabytes', () => {
    expect(humanSize(1073741824)).toBe('1.00 GB')
    expect(humanSize(3758096384)).toBe('3.50 GB')
  })
})

describe('elapsedTime', () => {
  it('formats seconds', () => {
    expect(elapsedTime(0)).toBe('0s')
    expect(elapsedTime(59)).toBe('59s')
  })

  it('formats minutes', () => {
    expect(elapsedTime(60)).toBe('1m 0s')
    expect(elapsedTime(61)).toBe('1m 1s')
    expect(elapsedTime(330)).toBe('5m 30s')
  })

  it('formats hours', () => {
    expect(elapsedTime(3600)).toBe('1h 0m 0s')
    expect(elapsedTime(3661)).toBe('1h 1m 1s')
    expect(elapsedTime(9045)).toBe('2h 30m 45s')
  })
})

describe('progressBar', () => {
  it('shows percentage', () => {
    expect(progressBar(0, 10, 20)).toContain('0%')
    expect(progressBar(5, 10, 20)).toContain('50%')
    expect(progressBar(10, 10, 20)).toContain('100%')
  })

  it('shows byte counts for large totals', () => {
    expect(progressBar(1048576, 1048576, 20)).toContain('1.0 MB')
  })

  it('shows item counts for small totals', () => {
    expect(progressBar(3, 7, 20)).toContain('3/7')
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm test`

Expected: FAIL — modules not found.

- [ ] **Step 3: Implement format utilities**

```typescript
// ts/src/core/format.ts

export function humanSize(bytes: number): string {
  if (bytes >= 1_073_741_824) {
    return `${(bytes / 1_073_741_824).toFixed(2)} GB`
  }
  if (bytes >= 1_048_576) {
    return `${(bytes / 1_048_576).toFixed(1)} MB`
  }
  if (bytes >= 1024) {
    return `${(bytes / 1024).toFixed(1)} KB`
  }
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
  const pct = total > 0 ? Math.floor((current * 100) / total) : 0
  const filled = total > 0 ? Math.floor((current * width) / total) : 0
  const empty = width - filled

  const bar = '\u2588'.repeat(filled) + '\u2591'.repeat(empty)

  const status = total > 1024
    ? `(${humanSize(current)} / ${humanSize(total)})`
    : `(${current}/${total})`

  return `[${bar}] ${pct.toString().padStart(3)}% ${status}`
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pnpm test`

Expected: all format tests PASS.

- [ ] **Step 5: Commit**

```bash
git add ts/src/core/format.ts ts/tests/unit/format.test.ts
git commit -m "feat(ts): add format utilities (humanSize, elapsedTime, progressBar)"
```

---

### Task 4: Retry module

**Files:**
- Create: `ts/src/core/retry.ts`
- Create: `ts/tests/unit/retry.test.ts`

- [ ] **Step 1: Write failing tests**

```typescript
// ts/tests/unit/retry.test.ts
import { describe, it, expect, vi } from 'vitest'
import { calculateDelay, retryWithBackoff } from '../../src/core/retry.js'

describe('calculateDelay', () => {
  it('returns base delay on first attempt', () => {
    const delay = calculateDelay(0, 5, 60)
    // base * 2^0 = 5, plus jitter [0, 1)
    expect(delay).toBeGreaterThanOrEqual(5_000)
    expect(delay).toBeLessThan(6_000)
  })

  it('doubles delay on each attempt', () => {
    const delay = calculateDelay(2, 5, 60)
    // base * 2^2 = 20, plus jitter [0, 1)
    expect(delay).toBeGreaterThanOrEqual(20_000)
    expect(delay).toBeLessThan(21_000)
  })

  it('caps at maxDelay', () => {
    const delay = calculateDelay(10, 5, 60)
    // base * 2^10 = 5120, capped to 60 + jitter
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
    const fn = vi.fn()
      .mockRejectedValueOnce(new Error('fail 1'))
      .mockRejectedValueOnce(new Error('fail 2'))
      .mockResolvedValue('ok')

    const result = await retryWithBackoff(fn, { maxRetries: 5, baseDelay: 0, maxDelay: 0 })
    expect(result).toBe('ok')
    expect(fn).toHaveBeenCalledTimes(3)
  })

  it('throws after exhausting retries', async () => {
    const fn = vi.fn().mockRejectedValue(new Error('always fails'))
    await expect(
      retryWithBackoff(fn, { maxRetries: 3, baseDelay: 0, maxDelay: 0 })
    ).rejects.toThrow('always fails')
    expect(fn).toHaveBeenCalledTimes(3)
  })

  it('calls onRetry callback', async () => {
    const onRetry = vi.fn()
    const fn = vi.fn()
      .mockRejectedValueOnce(new Error('fail'))
      .mockResolvedValue('ok')

    await retryWithBackoff(fn, { maxRetries: 3, baseDelay: 0, maxDelay: 0, onRetry })
    expect(onRetry).toHaveBeenCalledTimes(1)
    expect(onRetry).toHaveBeenCalledWith(1, expect.any(Error))
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm test`

Expected: FAIL — modules not found.

- [ ] **Step 3: Implement retry module**

```typescript
// ts/src/core/retry.ts

export interface RetryOptions {
  maxRetries: number
  baseDelay: number      // seconds
  maxDelay: number       // seconds
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pnpm test`

Expected: all retry tests PASS.

- [ ] **Step 5: Commit**

```bash
git add ts/src/core/retry.ts ts/tests/unit/retry.test.ts
git commit -m "feat(ts): add retry with exponential backoff and jitter"
```

---

## Phase 1 Complete

At this point you have:
- Working TypeScript project with strict config
- All shared types defined
- `humanSize`, `elapsedTime`, `progressBar` — tested
- `retryWithBackoff`, `calculateDelay` — tested
- All tests passing, types checking
