import { execSync } from 'node:child_process'
import { describe, expect, it } from 'vitest'

describe('CLI routing', () => {
  it('shows help with no arguments', () => {
    const output = execSync('npx tsx bin/pg-resilient.ts', { encoding: 'utf-8', timeout: 15_000 })
    expect(output).toContain('pg-resilient')
    expect(output).toContain('dump')
    expect(output).toContain('restore')
  })

  it('shows help for unknown subcommand', () => {
    const output = execSync('npx tsx bin/pg-resilient.ts unknown', { encoding: 'utf-8', timeout: 15_000 })
    expect(output).toContain('pg-resilient')
  })

  it('shows dump help', () => {
    const output = execSync('npx tsx bin/pg-resilient.ts dump --help', { encoding: 'utf-8', timeout: 15_000 })
    expect(output).toContain('--dbname')
    expect(output).toContain('--output')
  })

  it('shows restore help', () => {
    const output = execSync('npx tsx bin/pg-resilient.ts restore --help', { encoding: 'utf-8', timeout: 15_000 })
    expect(output).toContain('--dbname')
    expect(output).toContain('--input')
  })
})
