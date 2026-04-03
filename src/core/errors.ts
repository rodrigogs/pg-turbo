/**
 * Structured error classification for pg-turbo connection resilience.
 *
 * Errors are classified into categories that determine retry behavior:
 * - NETWORK:  Transient — retry indefinitely with backoff (VPN drop, DNS failure, timeout)
 * - SERVER:   Transient — retry indefinitely (PG shutdown, crash recovery)
 * - STREAM:   Transient — connection died mid-COPY (idle timeout, premature close)
 * - DATA:     Permanent — do not retry (constraint violation, syntax error)
 * - UNKNOWN:  Unclassified — treat as permanent (safe default)
 */

// ── Error Categories ────────────────────────────────────────────────────────

export enum ErrorCategory {
  /** TCP/OS-level network failure — retryable */
  NETWORK = 'NETWORK',
  /** PostgreSQL server going down or restarting — retryable */
  SERVER = 'SERVER',
  /** Stream/pipeline failure from dead connection — retryable */
  STREAM = 'STREAM',
  /** Data or logic error — permanent, do not retry */
  DATA = 'DATA',
  /** Unclassified — treat as permanent */
  UNKNOWN = 'UNKNOWN',
}

// ── Error Code Enums ────────────────────────────────────────────────────────

/** Node.js system error codes from TCP/DNS operations. */
export enum NetworkErrorCode {
  ECONNREFUSED = 'ECONNREFUSED',
  ECONNRESET = 'ECONNRESET',
  EPIPE = 'EPIPE',
  ETIMEDOUT = 'ETIMEDOUT',
  ENOTFOUND = 'ENOTFOUND',
  ENETUNREACH = 'ENETUNREACH',
  EHOSTUNREACH = 'EHOSTUNREACH',
  ECONNABORTED = 'ECONNABORTED',
  EAI_AGAIN = 'EAI_AGAIN',
}

/** PostgreSQL SQLSTATE classes that indicate server-level transient failures. */
export enum PgTransientClass {
  /** 08xxx — Connection Exception */
  CONNECTION_EXCEPTION = '08',
  /** 57xxx — Operator Intervention (admin shutdown, crash recovery, cannot connect) */
  OPERATOR_INTERVENTION = '57',
}

/** Node.js stream errors that indicate a broken pipeline (dead connection). */
export enum StreamErrorCode {
  ERR_STREAM_PREMATURE_CLOSE = 'ERR_STREAM_PREMATURE_CLOSE',
  ERR_STREAM_DESTROYED = 'ERR_STREAM_DESTROYED',
}

/** pg driver error messages that indicate connection death (no error code available). */
enum PgDriverMessage {
  CONNECTION_TERMINATED = 'Connection terminated',
  CONNECTION_LOST = 'connection lost',
  SERVER_CLOSED = 'server closed the connection unexpectedly',
}

/** pg driver internal error messages (no error code available). */
enum PgInternalMessage {
  /** Thrown by pg when connectionTimeoutMillis fires */
  TIMEOUT_EXPIRED = 'timeout expired',
}

/** pg-turbo internal error messages. */
enum InternalMessage {
  IDLE_TIMEOUT = 'Connection idle timeout',
  CONNECT_TIMEOUT = 'pg-turbo connect timeout',
}

// ── Lookup Sets (built from enums for O(1) matching) ────────────────────────

const NETWORK_CODES = new Set<string>(Object.values(NetworkErrorCode))
const STREAM_CODES = new Set<string>(Object.values(StreamErrorCode))
const PG_TRANSIENT_PREFIXES = Object.values(PgTransientClass)
const TRANSIENT_MESSAGE_PATTERNS = [
  ...Object.values(PgDriverMessage),
  ...Object.values(PgInternalMessage),
  ...Object.values(InternalMessage),
]

// ── Classification ──────────────────────────────────────────────────────────

function classifyShallow(err: Error): ErrorCategory {
  const code = (err as Error & { code?: string }).code

  if (code) {
    if (NETWORK_CODES.has(code)) return ErrorCategory.NETWORK
    if (STREAM_CODES.has(code)) return ErrorCategory.STREAM
    for (const prefix of PG_TRANSIENT_PREFIXES) {
      if (code.startsWith(prefix)) return ErrorCategory.SERVER
    }
  }

  const msg = err.message
  for (const pattern of TRANSIENT_MESSAGE_PATTERNS) {
    if (msg.includes(pattern)) return ErrorCategory.STREAM
  }

  return ErrorCategory.UNKNOWN
}

/** Classify an error into a retry category, traversing the cause chain. */
export function classifyError(err: unknown): ErrorCategory {
  if (!(err instanceof Error)) return ErrorCategory.UNKNOWN

  const cat = classifyShallow(err)
  if (cat !== ErrorCategory.UNKNOWN) return cat

  // stream.pipeline wraps the original error in .cause
  const cause = (err as Error & { cause?: unknown }).cause
  if (cause instanceof Error) return classifyShallow(cause)

  return ErrorCategory.UNKNOWN
}

/** Returns true if the error is transient (network, server, or stream failure). */
export function isTransientError(err: unknown): boolean {
  const cat = classifyError(err)
  return cat === ErrorCategory.NETWORK || cat === ErrorCategory.SERVER || cat === ErrorCategory.STREAM
}

/**
 * @deprecated Use `isTransientError` instead. Kept for backward compatibility.
 */
export function isNetworkError(err: unknown): boolean {
  return isTransientError(err)
}
