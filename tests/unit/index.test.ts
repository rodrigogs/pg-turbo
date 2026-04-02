import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

// Mock the dump and restore modules so main() doesn't actually run them
vi.mock('../../src/cli/dump.js', () => ({
  runDump: vi.fn().mockResolvedValue(undefined),
}))
vi.mock('../../src/cli/restore.js', () => ({
  runRestore: vi.fn().mockResolvedValue(undefined),
}))
vi.mock('../../src/cli/args.js', () => ({
  parseDumpArgs: vi.fn().mockReturnValue({}),
  parseRestoreArgs: vi.fn().mockReturnValue({}),
}))

import { main } from '../../src/cli/index.js'

describe('CLI index routing', () => {
  const origArgv = process.argv
  let logSpy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    logSpy = vi.spyOn(console, 'log').mockImplementation(() => {})
  })

  afterEach(() => {
    logSpy.mockRestore()
    process.argv = origArgv
  })

  it('prints help when no subcommand given', async () => {
    process.argv = ['node', 'pg-resilient']
    await main()
    const output = logSpy.mock.calls.map((c) => c[0]).join('\n')
    expect(output).toContain('pg-resilient')
    expect(output).toContain('dump')
    expect(output).toContain('restore')
  })

  it('prints help for unknown subcommand', async () => {
    process.argv = ['node', 'pg-resilient', 'garbage']
    await main()
    const output = logSpy.mock.calls.map((c) => c[0]).join('\n')
    expect(output).toContain('pg-resilient')
  })

  it('routes dump subcommand to runDump', async () => {
    const { runDump } = await import('../../src/cli/dump.js')
    process.argv = ['node', 'pg-resilient', 'dump', '-d', 'postgresql://localhost/test']
    await main()
    expect(runDump).toHaveBeenCalled()
  })

  it('routes restore subcommand to runRestore', async () => {
    const { runRestore } = await import('../../src/cli/restore.js')
    process.argv = ['node', 'pg-resilient', 'restore', '-d', 'postgresql://localhost/test']
    await main()
    expect(runRestore).toHaveBeenCalled()
  })
})
