/** Table metadata from pg_catalog discovery */
export interface TableInfo {
  oid: number
  schemaName: string
  tableName: string
  relkind: 'r' | 'm'
  relpages: number
  estimatedRows: number
  actualBytes: number
  pkColumn: string | null
  pkType: 'int2' | 'int4' | 'int8' | null
  columns: string[]
  generatedColumns: string[]
}

export type ChunkStrategy = 'pk_range' | 'ctid_range' | 'none'

export interface ChunkMeta {
  index: number
  file: string
  rangeStart?: number
  rangeEnd?: number
  ctidStart?: number
  ctidEnd?: number
  /** Estimated uncompressed bytes this chunk will produce (from sampling or proportional). */
  estimatedBytes?: number
  /** Estimated row count for this chunk (from sampling or proportional). */
  estimatedRows?: number
}

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

export interface SequenceInfo {
  schema: string
  name: string
  lastValue: number
  isCalled: boolean
}

export interface DumpManifest {
  version: 1
  tool: 'pg-turbo'
  createdAt: string
  pgVersion: string
  database: string
  snapshotId: string | null
  compression: Compression
  options: {
    schemaFilter: string | null
    splitThresholdBytes: number
    jobs: number
  }
  tables: ManifestTable[]
  sequences: SequenceInfo[]
}

export interface ChunkJob {
  table: ManifestTable
  chunk: ChunkMeta
  copyQuery?: string
  outputPath: string
  attempt: number
  /** Number of consecutive network errors (not counted against retry limit but used for backoff). */
  networkRetries?: number
}

export interface ChunkResult {
  job: ChunkJob
  status: 'ok' | 'skipped' | 'failed'
  rowCount?: number
  bytesWritten?: number
  error?: Error
  durationMs?: number
}

export interface ProgressEvent {
  type: 'started' | 'completed' | 'skipped' | 'failed' | 'retrying'
  workerId: number
  job: ChunkJob
  bytesWritten?: number
  error?: Error
}

export interface WorkerState {
  id: number
  status: 'idle' | 'working' | 'retrying'
  currentJob?: ChunkJob
  startedAt?: number
  progressCurrent: number
  progressTotal: number
  /** Rolling speed: anchor point for delta calculation */
  speedSnapshot?: { time: number; current: number }
  /** Last computed speed (shown while waiting for next meaningful delta) */
  lastSpeed?: number
}

export type Compression = 'zstd' | 'lz4'

export interface DumpOptions {
  dbname: string
  output: string
  schema?: string
  jobs: number
  splitThreshold: number
  maxChunksPerTable: number
  retries: number
  retryDelay: number
  noSnapshot: boolean
  dryRun: boolean
  compression: Compression
  noArchive: boolean
  pgDumpArgs: string[]
}

export interface RestoreOptions {
  dbname: string
  input: string
  schema?: string
  table?: string
  jobs: number
  clean: boolean
  dataOnly: boolean
  retries: number
  retryDelay: number
  dryRun: boolean
  pgRestoreArgs: string[]
}
