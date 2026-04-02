/** Node.js system error codes that indicate a transient network/connection issue. */
const NETWORK_ERROR_CODES = new Set([
  'ECONNREFUSED',  // Server refused connection (down or restarting)
  'ECONNRESET',    // Connection reset by peer (network interruption)
  'EPIPE',         // Broken pipe (connection closed unexpectedly)
  'ETIMEDOUT',     // Operation timed out
  'ENOTFOUND',     // DNS resolution failed
  'ENETUNREACH',   // Network unreachable (VPN down)
  'EHOSTUNREACH',  // Host unreachable
  'ECONNABORTED',  // Connection aborted
  'EAI_AGAIN',     // DNS lookup timeout (transient)
])

const CONNECTION_TERMINATED_PATTERNS = [
  'Connection terminated',
  'connection lost',
  'server closed the connection unexpectedly',
]

export function isNetworkError(err: unknown): boolean {
  if (!(err instanceof Error)) return false
  const code = (err as Error & { code?: string }).code

  // Node.js system error codes
  if (code && NETWORK_ERROR_CODES.has(code)) return true

  // PostgreSQL connection error class (08xxx)
  if (code && /^08/.test(code)) return true

  // pg driver connection terminated messages
  const msg = err.message
  for (const pattern of CONNECTION_TERMINATED_PATTERNS) {
    if (msg.includes(pattern)) return true
  }

  return false
}
