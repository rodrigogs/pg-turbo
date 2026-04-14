#!/usr/bin/env node
import { main } from '../src/cli/index.js'

main().catch((err: unknown) => {
  let msg: string
  if (err instanceof AggregateError) {
    // Node.js dual-stack (IPv4+IPv6) connection failures produce AggregateError with empty message
    msg = err.errors.map((e: Error) => e.message).join('; ') || err.message || 'Unknown error'
  } else if (err instanceof Error) {
    msg = err.message || err.constructor.name
  } else {
    msg = String(err)
  }
  console.error(`\n\x1b[31m\u2716\x1b[0m  ${msg}\n`)
  process.exit(1)
})
