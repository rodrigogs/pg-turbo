import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['tests/integration/**/*.test.ts'],
    testTimeout: 120_000,
    hookTimeout: 60_000,
    pool: 'forks',
    maxWorkers: 1,
    minWorkers: 1,
    coverage: {
      provider: 'v8',
      include: ['src/**/*.ts'],
      exclude: ['src/types/**'],
    },
  },
})
