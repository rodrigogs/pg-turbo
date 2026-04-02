#!/usr/bin/env node
import { main } from '../src/cli/index.js'

main().catch((err: unknown) => {
  const msg = err instanceof Error ? err.message : String(err)
  console.error(`\n\x1b[31m\u2716\x1b[0m  ${msg}\n`)
  process.exit(1)
})
