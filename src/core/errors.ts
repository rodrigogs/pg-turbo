const NETWORK_ERROR_CODES = new Set([
  'ECONNREFUSED',
  'ECONNRESET',
  'EPIPE',
  'ETIMEDOUT',
  'ENOTFOUND',
  'ENETUNREACH',
  'EHOSTUNREACH',
  'ECONNABORTED',
  'EAI_AGAIN',
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
