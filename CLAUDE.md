# CLAUDE.md

## Project Overview

**pg-resilient** -- Resilient PostgreSQL dump & restore CLI tool for large databases.

Designed for scenarios with flaky connections (RDS over VPN, remote servers) where standard `pg_dump`/`pg_restore` fail on large schemas. Uses the PostgreSQL COPY protocol directly with chunked streaming, compression, and per-chunk retry -- NOT a wrapper around pg_dump for data transfer.

## Architecture

```
pg_resilient/
├── bin/
│   └── pg-resilient.ts          # CLI entrypoint (shebang)
├── src/
│   ├── cli/
│   │   ├── index.ts             # Subcommand routing (dump / restore / help)
│   │   ├── args.ts              # Argument parsing (commander) + size parsing
│   │   ├── dump.ts              # Dump orchestrator (7-step workflow)
│   │   ├── restore.ts           # Restore orchestrator
│   │   └── ui.ts                # Terminal UI (live dashboard, banners, progress handler, signal handling)
│   ├── core/
│   │   ├── archive.ts           # .pgr archive creation/extraction (tar + zstd)
│   │   ├── chunker.ts           # Table chunking strategies (PK range, ctid range, volume-balanced)
│   │   ├── connection.ts        # Connection string helpers, snapshot coordination, keepalive tuning
│   │   ├── copy-stream.ts       # COPY TO/FROM streaming with compression (zstd/lz4)
│   │   ├── format.ts            # Human-readable formatting (sizes, durations, progress bars)
│   │   ├── manifest.ts          # Dump manifest (JSON) read/write with path traversal protection
│   │   ├── queue.ts             # Parallel worker pool with retry + jitter
│   │   ├── retry.ts             # Exponential backoff with jitter (calculateDelay)
│   │   └── schema.ts            # pg_catalog introspection (tables, columns, sequences, quoteIdent)
│   └── types/
│       └── index.ts             # Shared type definitions
├── tests/
│   ├── unit/                    # Unit tests (vitest) -- 10 test files
│   └── integration/             # Integration tests (require Docker + PostgreSQL)
│       ├── docker-compose.yml
│       ├── fixtures.sql
│       ├── dump.test.ts
│       └── restore.test.ts
├── bench/
│   └── benchmark.ts             # Performance benchmarks (pg-resilient vs pg_dump)
├── package.json
├── tsconfig.json
├── vitest.config.ts
├── vitest.integration.config.ts
├── biome.json                   # Linter + formatter config
├── .github/workflows/ci.yml    # CI: typecheck + unit tests
├── .gitignore
├── LICENSE
├── README.md
└── CLAUDE.md                    # This file
```

### How It Works

**Dump flow:**
1. Test connection, detect PG version and read replica status
2. Export snapshot for consistency (REPEATABLE READ + `pg_export_snapshot()`)
3. Discover tables via `pg_catalog` (single batch query for columns)
4. Plan chunks -- split large tables by PK ranges (volume-balanced) or ctid ranges
5. Dump DDL via `pg_dump --schema-only` (runs async, overlaps with data dump)
6. Parallel COPY TO STDOUT workers -- each chunk streams through compressor to file
7. Write manifest.json, optionally package as `.pgr` archive

**Restore flow:**
1. Read manifest (or extract `.pgr` archive first)
2. Test connection, create progress tracking table (`_pg_resilient._progress`)
3. Clean schemas if `--clean` (DROP + CREATE or TRUNCATE for `--table`)
4. Pre-data DDL via `pg_restore --section=pre-data`
5. Parallel COPY FROM STDIN workers -- streams file through decompressor into PG (skips materialized views)
6. Post-data DDL via `pg_restore --section=post-data -j N` (parallel index creation)
7. Refresh materialized views, reset sequences, drop progress table

**Key design decisions:**
- **Direct COPY protocol** -- bypasses pg_dump for data, uses `pg-copy-streams` for maximum throughput
- **Sub-table chunking** -- splits large tables across workers (pg_dump can't do this)
- **Volume-balanced chunks** -- samples row sizes to create equal-byte chunks, not equal-row
- **Per-chunk retry** -- failed 250MB chunk retries, not entire 500GB table
- **Resume support** -- `.done` markers for dump, `_pg_resilient._progress` table for restore
- **Snapshot coordination** -- all workers see identical data via shared snapshot
- **Streaming compression** -- zstd (default, `.zst`) or lz4 (`.lz4`), never buffers full table in memory
- **Connection retry** -- initial connection retries 5 times with exponential backoff (2s base, 30s cap)
- **Materialized view handling** -- dumped via `COPY (SELECT ...)` form, skipped during restore data phase, refreshed explicitly after post-data DDL

### Output Structure

```
<output_dir>/
├── manifest.json                      # Metadata: tables, chunks, sequences, options
├── _schema_ddl.dump                   # pg_dump custom format (DDL only)
└── data/
    ├── <schema>.<table>/
    │   ├── chunk_0000.copy.zst        # COPY text format, zstd compressed
    │   ├── chunk_0000.copy.zst.done   # Resume marker
    │   ├── chunk_0001.copy.zst
    │   └── ...
    └── ...
```

## Tech Stack

| Tool | Purpose |
|------|---------|
| TypeScript 6.x | Primary language (ESM, strict mode) |
| Node.js >= 20 | Runtime |
| tsx | TypeScript execution (dev & bin) |
| vitest | Test runner + coverage |
| biome | Linter + formatter |
| pg | PostgreSQL client (node-postgres, pure JS -- NOT pg-native) |
| pg-copy-streams | COPY protocol streaming |
| zstd-napi | Zstandard compression (default) |
| lz4 | LZ4 compression (alternative, faster decompression) |
| tar | Archive packaging (.pgr format) |
| commander | CLI argument parsing |
| picocolors | Terminal colors (zero deps) |
| log-update | Live terminal UI updates |

## Commands

```bash
# Install dependencies
npm install

# Typecheck
npm run typecheck

# Run unit tests
npm test

# Run unit tests in watch mode
npm run test:watch

# Run integration tests (requires Docker)
npm run test:integration

# Run benchmarks (requires Docker)
npm run bench

# Lint
npm run lint

# Lint + autofix
npm run lint:fix

# Format
npm run format

# Run CLI in dev mode
npm run dev -- dump -d "postgresql://user:pass@host/db" --output "./dump_dir"
npm run dev -- restore -d "postgresql://user:pass@host/db" --input "./dump_dir"
```

## CLI Usage

```bash
# Dump
pg-resilient dump -d <connection_string> --output <dir> [options]

# Restore
pg-resilient restore -d <connection_string> --input <dir|file.pgr> [options]
```

### Dump Options

| Option | Description | Default |
|--------|-------------|---------|
| `-d, --dbname` | PostgreSQL connection string (required) | -- |
| `--output` | Output directory (required) | -- |
| `-n, --schema` | Filter to specific schema | all user schemas |
| `-j, --jobs` | Parallel workers | 4 |
| `--split-threshold` | Chunk tables larger than this (e.g. "512MB", "1GB") | 1GB |
| `--max-chunks-per-table` | Cap chunks per table | 32 |
| `--retries` | Max retries per chunk | 5 |
| `--retry-delay` | Base retry delay in seconds | 5 |
| `--compression` | zstd or lz4 | zstd |
| `--no-snapshot` | Skip synchronized snapshot (for read replicas) | -- |
| `--no-archive` | Skip .pgr archive packaging | -- |
| `--dry-run` | Preview without writing | -- |

### Restore Options

| Option | Description | Default |
|--------|-------------|---------|
| `-d, --dbname` | PostgreSQL connection string (required) | -- |
| `--input` | Input directory or .pgr file (required) | -- |
| `-n, --schema` | Restore only tables in this schema | all |
| `-t, --table` | Restore a single table | all |
| `-j, --jobs` | Parallel workers | 4 |
| `-c, --clean` | DROP + CREATE schema before restore | -- |
| `-a, --data-only` | Skip DDL, restore only data | -- |
| `--retries` | Max retries per chunk | 5 |
| `--retry-delay` | Base retry delay in seconds | 5 |
| `--dry-run` | Preview without writing | -- |

Passthrough args after `--` are forwarded to pg_dump (dump) or pg_restore (restore) for DDL operations only.

## External Dependencies

System binaries required:
- `pg_dump` -- for DDL extraction only (`--schema-only`)
- `pg_restore` -- for DDL restoration only (`--section=pre-data`, `--section=post-data`)

Data transfer uses direct COPY protocol via `pg-copy-streams`. No external binaries needed for data.

## Development Rules

### Module Organization

- `src/cli/` -- CLI concerns (arg parsing, orchestration, terminal UI)
- `src/core/` -- Reusable core logic (connection, streaming, chunking, schema introspection)
- `src/types/` -- Shared type definitions

Shared patterns live in `src/cli/ui.ts` (progress handler, signal handlers) and `src/core/` modules. Do not duplicate logic between dump and restore.

### Code Style

- **Language**: TypeScript with strict mode, ESM modules
- **Linter/formatter**: biome (configured in `biome.json`)
- **Variables**: camelCase for locals, UPPER_SNAKE_CASE for constants
- **Functions**: camelCase
- **Types**: PascalCase for interfaces and type aliases
- **Indentation**: 2 spaces
- **Semicolons**: as needed (omitted when possible)

### Testing

Run unit tests before committing:
```bash
npm test
```

Integration tests require Docker with PostgreSQL:
```bash
npm run test:integration
```

### Security

- All SQL identifiers go through `quoteIdent()` (doubles embedded `"` chars)
- Schema filters use parameterized queries (`$1`)
- External processes use `execFileSync`/`execFile` (array args, no shell)
- Manifest chunk paths validated against path traversal
- Snapshot IDs validated by regex before interpolation
- Connection keepalive params respect user's existing values
