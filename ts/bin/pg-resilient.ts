#!/usr/bin/env node
import { main } from '../src/cli/index.js'

main().catch((err: unknown) => {
  console.error(err)
  process.exit(1)
})
