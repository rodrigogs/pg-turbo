# Phase 5: Integration Tests & Build

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Docker-based integration tests that verify the full dump/restore cycle against a real PostgreSQL database, and configure the build pipeline.

**Architecture:** Docker Compose spins up PG 16, fixtures create test tables with various characteristics (PK, no PK, generated columns, large table for chunking). Tests run full dump → verify manifest → restore → verify data.

**Tech Stack:** vitest, Docker, PostgreSQL 16

**Depends on:** Phase 4 (all modules)

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `ts/tests/integration/docker-compose.yml` | PG 16 for integration tests |
| Create | `ts/tests/integration/fixtures.sql` | Test schema and data |
| Create | `ts/tests/integration/dump.test.ts` | End-to-end dump tests |
| Create | `ts/tests/integration/restore.test.ts` | End-to-end restore tests |
| Create | `ts/vitest.integration.config.ts` | Integration test config |
| Create | `ts/Makefile` | Build, lint, test targets |

---

### Task 16: Integration test infrastructure

**Files:**
- Create: `ts/tests/integration/docker-compose.yml`
- Create: `ts/tests/integration/fixtures.sql`
- Create: `ts/vitest.integration.config.ts`

- [ ] **Step 1: Create Docker Compose**

```yaml
# ts/tests/integration/docker-compose.yml
services:
  db:
    image: postgres:16-alpine
    ports:
      - "54399:5432"
    environment:
      POSTGRES_DB: pg_resilient_test
      POSTGRES_USER: test_admin
      POSTGRES_HOST_AUTH_METHOD: trust
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U test_admin"]
      interval: 5s
      timeout: 5s
      retries: 5
    tmpfs:
      - /var/lib/postgresql/data
```

- [ ] **Step 2: Create test fixtures**

```sql
-- ts/tests/integration/fixtures.sql

-- Schema with various table types for testing

-- 1. Regular table with integer PK (will be chunked if large enough)
CREATE TABLE public.users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

INSERT INTO public.users (name, email)
SELECT
    'user_' || i,
    'user_' || i || '@example.com'
FROM generate_series(1, 10000) AS i;

-- 2. Table without PK (tests ctid fallback / no-chunk path)
CREATE TABLE public.logs (
    ts TIMESTAMPTZ DEFAULT now(),
    level TEXT,
    message TEXT
);

INSERT INTO public.logs (level, message)
SELECT
    CASE WHEN i % 3 = 0 THEN 'ERROR' WHEN i % 2 = 0 THEN 'WARN' ELSE 'INFO' END,
    'Log message number ' || i
FROM generate_series(1, 5000) AS i;

-- 3. Small config table (no chunking needed)
CREATE TABLE public.config (
    key TEXT PRIMARY KEY,
    value TEXT
);

INSERT INTO public.config (key, value) VALUES
    ('version', '1.0'),
    ('feature_flag', 'true'),
    ('max_retries', '5');

-- 4. Table with generated column
CREATE TABLE public.products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    tax NUMERIC(10, 2) GENERATED ALWAYS AS (price * 0.1) STORED
);

INSERT INTO public.products (name, price)
SELECT 'product_' || i, (random() * 100)::numeric(10, 2)
FROM generate_series(1, 1000) AS i;

-- 5. Table in a separate schema
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE analytics.events (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);

INSERT INTO analytics.events (event_type, payload)
SELECT
    CASE WHEN i % 2 = 0 THEN 'click' ELSE 'view' END,
    jsonb_build_object('item_id', i, 'source', 'test')
FROM generate_series(1, 3000) AS i;

-- 6. Sequence for testing sequence reset
CREATE SEQUENCE public.custom_seq START 42;
SELECT nextval('public.custom_seq');
SELECT nextval('public.custom_seq');
```

- [ ] **Step 3: Create integration vitest config**

```typescript
// ts/vitest.integration.config.ts
import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['tests/integration/**/*.test.ts'],
    testTimeout: 120_000,
    hookTimeout: 60_000,
    pool: 'forks',
    poolOptions: {
      forks: { singleFork: true },
    },
  },
})
```

- [ ] **Step 4: Commit**

```bash
git add ts/tests/integration/docker-compose.yml ts/tests/integration/fixtures.sql ts/vitest.integration.config.ts
git commit -m "feat(ts): add integration test infrastructure (Docker + fixtures)"
```

---

### Task 17: Dump integration test

**Files:**
- Create: `ts/tests/integration/dump.test.ts`

- [ ] **Step 1: Write dump integration test**

```typescript
// ts/tests/integration/dump.test.ts
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { execSync } from 'node:child_process'
import { existsSync, rmSync, readdirSync } from 'node:fs'
import { join } from 'node:path'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { readManifest } from '../../src/core/manifest.js'
import { runDump } from '../../src/cli/dump.js'

const TEST_CS = 'postgresql://test_admin@localhost:54399/pg_resilient_test'
const DOCKER_DIR = join(import.meta.dirname, '.')

let tmpDir: string

beforeAll(async () => {
  // Start Docker
  execSync('docker compose up -d --wait', { cwd: DOCKER_DIR, stdio: 'pipe' })
  // Wait for PG
  for (let i = 0; i < 30; i++) {
    try {
      execSync(`psql "${TEST_CS}" -c "SELECT 1"`, { stdio: 'pipe' })
      break
    } catch {
      await new Promise(r => setTimeout(r, 1000))
    }
  }
  // Load fixtures
  execSync(`psql "${TEST_CS}" -f "${join(DOCKER_DIR, 'fixtures.sql')}"`, { stdio: 'pipe' })
}, 60_000)

afterAll(() => {
  execSync('docker compose down', { cwd: DOCKER_DIR, stdio: 'pipe' })
})

afterEach(() => {
  if (tmpDir && existsSync(tmpDir)) {
    rmSync(tmpDir, { recursive: true, force: true })
  }
})

describe('dump', () => {
  it('dumps all tables with manifest', async () => {
    tmpDir = mkdtempSync(join(tmpdir(), 'pgr-dump-'))

    await runDump({
      dbname: TEST_CS,
      output: tmpDir,
      jobs: 2,
      splitThreshold: 1024 * 1024 * 1024,
      maxChunksPerTable: 32,
      retries: 3,
      retryDelay: 1,
      noSnapshot: false,
      dryRun: false,
      pgDumpArgs: [],
    })

    // Verify manifest exists and is valid
    const manifest = await readManifest(tmpDir)
    expect(manifest.version).toBe(1)
    expect(manifest.tool).toBe('pg-resilient')
    expect(manifest.compression).toBe('lz4')
    expect(manifest.tables.length).toBeGreaterThanOrEqual(5)

    // Verify DDL dump exists
    expect(existsSync(join(tmpDir, 'schema', 'ddl.dump'))).toBe(true)

    // Verify each table has chunk files
    for (const table of manifest.tables) {
      for (const chunk of table.chunks) {
        const chunkPath = join(tmpDir, chunk.file)
        expect(existsSync(chunkPath), `Missing chunk: ${chunk.file}`).toBe(true)
        expect(existsSync(`${chunkPath}.done`), `Missing done marker: ${chunk.file}`).toBe(true)
      }
    }

    // Verify generated columns are excluded
    const productsTable = manifest.tables.find(t => t.name === 'products')
    expect(productsTable).toBeDefined()
    expect(productsTable!.columns).not.toContain('tax')
    expect(productsTable!.generatedColumns).toContain('tax')
  })

  it('dumps with schema filter', async () => {
    tmpDir = mkdtempSync(join(tmpdir(), 'pgr-dump-'))

    await runDump({
      dbname: TEST_CS,
      output: tmpDir,
      schema: 'analytics',
      jobs: 1,
      splitThreshold: 1024 * 1024 * 1024,
      maxChunksPerTable: 32,
      retries: 3,
      retryDelay: 1,
      noSnapshot: false,
      dryRun: false,
      pgDumpArgs: [],
    })

    const manifest = await readManifest(tmpDir)
    expect(manifest.tables.every(t => t.schema === 'analytics')).toBe(true)
  })

  it('dry run creates no files', async () => {
    tmpDir = mkdtempSync(join(tmpdir(), 'pgr-dump-'))

    await runDump({
      dbname: TEST_CS,
      output: tmpDir,
      jobs: 1,
      splitThreshold: 1024 * 1024 * 1024,
      maxChunksPerTable: 32,
      retries: 3,
      retryDelay: 1,
      noSnapshot: false,
      dryRun: true,
      pgDumpArgs: [],
    })

    expect(existsSync(join(tmpDir, 'manifest.json'))).toBe(false)
  })
})
```

- [ ] **Step 2: Run integration test**

Run: `cd ts && pnpm test:integration`

Expected: all dump integration tests PASS.

- [ ] **Step 3: Commit**

```bash
git add ts/tests/integration/dump.test.ts
git commit -m "test(ts): add dump integration tests"
```

---

### Task 18: Restore integration test

**Files:**
- Create: `ts/tests/integration/restore.test.ts`

- [ ] **Step 1: Write restore integration test**

```typescript
// ts/tests/integration/restore.test.ts
import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest'
import { execSync } from 'node:child_process'
import { existsSync, rmSync, mkdtempSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import pg from 'pg'
import { runDump } from '../../src/cli/dump.js'
import { runRestore } from '../../src/cli/restore.js'

const { Client } = pg
const SOURCE_CS = 'postgresql://test_admin@localhost:54399/pg_resilient_test'
const RESTORE_DB = 'pg_resilient_restore'
const RESTORE_CS = `postgresql://test_admin@localhost:54399/${RESTORE_DB}`
const DOCKER_DIR = join(import.meta.dirname, '.')

let tmpDir: string

beforeAll(async () => {
  // Ensure Docker is running and fixtures loaded (dump test should have done this)
  execSync('docker compose up -d --wait', { cwd: DOCKER_DIR, stdio: 'pipe' })
  for (let i = 0; i < 30; i++) {
    try {
      execSync(`psql "${SOURCE_CS}" -c "SELECT 1"`, { stdio: 'pipe' })
      break
    } catch {
      await new Promise(r => setTimeout(r, 1000))
    }
  }
  // Ensure fixtures loaded
  try {
    execSync(`psql "${SOURCE_CS}" -c "SELECT count(*) FROM public.users"`, { stdio: 'pipe' })
  } catch {
    execSync(`psql "${SOURCE_CS}" -f "${join(DOCKER_DIR, 'fixtures.sql')}"`, { stdio: 'pipe' })
  }
}, 60_000)

afterAll(() => {
  execSync('docker compose down', { cwd: DOCKER_DIR, stdio: 'pipe' })
})

beforeEach(() => {
  // Create fresh restore database
  execSync(`psql "postgresql://test_admin@localhost:54399/postgres" -c "DROP DATABASE IF EXISTS ${RESTORE_DB}"`, { stdio: 'pipe' })
  execSync(`psql "postgresql://test_admin@localhost:54399/postgres" -c "CREATE DATABASE ${RESTORE_DB}"`, { stdio: 'pipe' })
})

afterEach(() => {
  if (tmpDir && existsSync(tmpDir)) {
    rmSync(tmpDir, { recursive: true, force: true })
  }
  execSync(`psql "postgresql://test_admin@localhost:54399/postgres" -c "DROP DATABASE IF EXISTS ${RESTORE_DB}"`, { stdio: 'pipe' }).toString()
})

describe('restore', () => {
  it('full dump → restore cycle preserves data', async () => {
    tmpDir = mkdtempSync(join(tmpdir(), 'pgr-cycle-'))

    // Dump
    await runDump({
      dbname: SOURCE_CS,
      output: tmpDir,
      jobs: 2,
      splitThreshold: 1024 * 1024 * 1024,
      maxChunksPerTable: 32,
      retries: 3,
      retryDelay: 1,
      noSnapshot: false,
      dryRun: false,
      pgDumpArgs: [],
    })

    // Restore
    await runRestore({
      dbname: RESTORE_CS,
      input: tmpDir,
      jobs: 2,
      clean: false,
      dataOnly: false,
      retries: 3,
      retryDelay: 1,
      dryRun: false,
      pgRestoreArgs: [],
    })

    // Verify data
    const client = new Client({ connectionString: RESTORE_CS })
    await client.connect()

    const { rows: userCount } = await client.query('SELECT count(*)::int AS c FROM public.users')
    expect(userCount[0].c).toBe(10000)

    const { rows: logCount } = await client.query('SELECT count(*)::int AS c FROM public.logs')
    expect(logCount[0].c).toBe(5000)

    const { rows: configCount } = await client.query('SELECT count(*)::int AS c FROM public.config')
    expect(configCount[0].c).toBe(3)

    const { rows: productCount } = await client.query('SELECT count(*)::int AS c FROM public.products')
    expect(productCount[0].c).toBe(1000)

    const { rows: eventCount } = await client.query('SELECT count(*)::int AS c FROM analytics.events')
    expect(eventCount[0].c).toBe(3000)

    // Verify generated column is correct
    const { rows: taxRows } = await client.query('SELECT price, tax FROM public.products WHERE id = 1')
    expect(Number(taxRows[0].tax)).toBeCloseTo(Number(taxRows[0].price) * 0.1, 2)

    await client.end()
  })

  it('data-only restore skips DDL', async () => {
    tmpDir = mkdtempSync(join(tmpdir(), 'pgr-dataonly-'))

    // Dump source
    await runDump({
      dbname: SOURCE_CS,
      output: tmpDir,
      jobs: 1,
      splitThreshold: 1024 * 1024 * 1024,
      maxChunksPerTable: 32,
      retries: 3,
      retryDelay: 1,
      noSnapshot: false,
      dryRun: false,
      pgDumpArgs: [],
    })

    // Create schema manually in restore DB
    execSync(`pg_restore --section=pre-data --no-owner -d '${RESTORE_CS}' '${join(tmpDir, 'schema', 'ddl.dump')}'`, { stdio: 'pipe' }).toString()

    // Data-only restore
    await runRestore({
      dbname: RESTORE_CS,
      input: tmpDir,
      jobs: 1,
      clean: false,
      dataOnly: true,
      retries: 3,
      retryDelay: 1,
      dryRun: false,
      pgRestoreArgs: [],
    })

    const client = new Client({ connectionString: RESTORE_CS })
    await client.connect()
    const { rows } = await client.query('SELECT count(*)::int AS c FROM public.users')
    expect(rows[0].c).toBe(10000)
    await client.end()
  })
})
```

- [ ] **Step 2: Run integration tests**

Run: `cd ts && pnpm test:integration`

Expected: all restore integration tests PASS.

- [ ] **Step 3: Commit**

```bash
git add ts/tests/integration/restore.test.ts
git commit -m "test(ts): add restore integration tests with data verification"
```

---

### Task 19: Makefile and build

**Files:**
- Create: `ts/Makefile`

- [ ] **Step 1: Create Makefile**

```makefile
# ts/Makefile
.PHONY: typecheck test test-integration test-all dev build clean help

## typecheck: Run TypeScript type checking
typecheck:
	pnpm exec tsc --noEmit

## test: Run unit tests
test:
	pnpm exec vitest run

## test-watch: Run unit tests in watch mode
test-watch:
	pnpm exec vitest

## test-integration: Run integration tests (requires Docker)
test-integration:
	pnpm exec vitest run --config vitest.integration.config.ts

## test-all: Run unit + integration tests
test-all: test test-integration

## dev: Run CLI in development mode (pass args after --)
dev:
	pnpm exec tsx bin/pg-resilient.ts

## build: Bundle for distribution
build:
	pnpm exec tsdown src/cli/index.ts --format esm --target node20 --clean

## clean: Remove build artifacts
clean:
	rm -rf dist coverage

## help: Show available targets
help:
	@echo "Available targets:"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /'

.DEFAULT_GOAL := help
```

- [ ] **Step 2: Verify all targets work**

Run: `cd ts && make typecheck && make test`

Expected: both pass.

- [ ] **Step 3: Commit**

```bash
git add ts/Makefile
git commit -m "feat(ts): add Makefile with build, test, and dev targets"
```

---

## Phase 5 Complete

The TypeScript rewrite is now fully functional:
- Full dump/restore cycle verified against real PostgreSQL
- Unit tests for all core modules
- Integration tests for dump and restore
- Build pipeline configured
- CLI ready for use via `pnpm dev -- dump -d ... --output ...`
