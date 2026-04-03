# pg-turbo

[![CI](https://github.com/rodrigogs/pg-turbo/actions/workflows/ci.yml/badge.svg)](https://github.com/rodrigogs/pg-turbo/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/rodrigogs/pg-turbo/branch/main/graph/badge.svg)](https://codecov.io/gh/rodrigogs/pg-turbo)
[![npm version](https://img.shields.io/npm/v/pg-turbo.svg)](https://www.npmjs.com/package/pg-turbo)
[![npm downloads](https://img.shields.io/npm/dm/pg-turbo.svg)](https://www.npmjs.com/package/pg-turbo)
[![Node.js](https://img.shields.io/node/v/pg-turbo.svg)](https://www.npmjs.com/package/pg-turbo)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Resilient PostgreSQL dump & restore CLI for large databases over flaky connections.

Uses the PostgreSQL **COPY protocol directly** with chunked streaming, parallel workers, and per-chunk retry. Designed for RDS over VPN, remote servers, and other scenarios where standard `pg_dump`/`pg_restore` fail on large schemas.

## Why pg-turbo?

| Problem with pg_dump | pg-turbo solution |
|---------------------|----------------------|
| One large table fails = restart entire dump | Per-chunk retry -- only the failed 250MB chunk retries |
| Can't parallelize within a single table | Sub-table chunking splits large tables across workers |
| No resume after interruption | `.done` markers let you pick up where you left off |
| Connection drops after hours of dumping | TCP keepalive tuning detects drops in ~60s, auto-retry |
| Slow on remote databases | Direct COPY protocol, no pg_dump process overhead per table |

## Quick Start

```bash
# Install globally
npm install -g pg-turbo

# Dump a database
pg-turbo dump \
    -d "postgresql://user:pass@host:5432/mydb" \
    --output ./mydb_dump \
    -j 4

# Restore to another database
pg-turbo restore \
    -d "postgresql://user:pass@host:5432/target_db" \
    --input ./mydb_dump \
    -j 4
```

## Requirements

- **Node.js >= 20**
- **pg_dump** and **pg_restore** (for DDL only -- data uses direct COPY)

## Features

- **Chunked COPY streaming** -- large tables split by PK range or ctid range for parallel transfer
- **Volume-balanced chunks** -- samples row sizes to create equal-byte chunks (handles skewed data)
- **Per-chunk retry** with exponential backoff + jitter
- **Resume support** -- skip completed chunks on re-run
- **Snapshot consistency** -- all workers see identical data via `pg_export_snapshot()`
- **Streaming compression** -- zstd (default) or lz4, never buffers full table in memory
- **Live progress dashboard** -- per-worker status, overall speed, ETA
- **Connection resilience** -- TCP keepalive tuning, auto-reconnect on failure
- **Archive packaging** -- optional `.pgt` single-file format (tar + zstd)
- **Connection string cleaning** -- strips GUI params (statusColor, env, etc.) from tools like TablePlus

## Usage

### Dump

```bash
# Dump entire database with 4 parallel workers
pg-turbo dump \
    -d "postgresql://user:pass@host:5432/mydb" \
    --output ./mydb_dump \
    -j 4

# Dump specific schema, force chunking at 256MB
pg-turbo dump \
    -d "postgresql://user:pass@host:5432/mydb" \
    --output ./mydb_dump \
    -n public \
    -j 4 \
    --split-threshold 256MB

# Dump from read replica (skip snapshot)
pg-turbo dump \
    -d "postgresql://readonly:pass@replica:5432/mydb" \
    --output ./mydb_dump \
    -j 8 \
    --no-snapshot

# Preview what would be dumped
pg-turbo dump \
    -d "postgresql://user:pass@host:5432/mydb" \
    --output ./mydb_dump \
    --dry-run
```

### Restore

```bash
# Restore to target database
pg-turbo restore \
    -d "postgresql://user:pass@host:5432/target_db" \
    --input ./mydb_dump \
    -j 4

# Clean restore (drop + recreate schemas first)
pg-turbo restore \
    -d "postgresql://user:pass@host:5432/target_db" \
    --input ./mydb_dump \
    -j 4 -c

# Restore only data (schema already exists)
pg-turbo restore \
    -d "postgresql://user:pass@host:5432/target_db" \
    --input ./mydb_dump \
    -j 4 -a

# Restore a single table
pg-turbo restore \
    -d "postgresql://user:pass@host:5432/target_db" \
    --input ./mydb_dump \
    -t users

# Restore from .pgt archive
pg-turbo restore \
    -d "postgresql://user:pass@host:5432/target_db" \
    --input ./mydb_dump.pgt \
    -j 4
```

## Options

### dump

| Option | Description | Default |
|--------|-------------|---------|
| `-d, --dbname` | PostgreSQL connection string | required |
| `--output` | Output directory | required |
| `-n, --schema` | Dump only this schema | all user schemas |
| `-j, --jobs` | Parallel workers | 4 |
| `--split-threshold` | Chunk tables larger than this | 1GB |
| `--max-chunks-per-table` | Max chunks per table | 32 |
| `--retries` | Max retries per chunk | 5 |
| `--retry-delay` | Base retry delay (seconds) | 5 |
| `--compression` | zstd or lz4 | zstd |
| `--no-snapshot` | Skip snapshot (for read replicas) | |
| `--no-archive` | Skip .pgt archive packaging | |
| `--dry-run` | Preview without dumping | |

### restore

| Option | Description | Default |
|--------|-------------|---------|
| `-d, --dbname` | PostgreSQL connection string | required |
| `--input` | Input directory or .pgt file | required |
| `-n, --schema` | Restore only this schema | all |
| `-t, --table` | Restore a single table | all |
| `-j, --jobs` | Parallel workers | 4 |
| `-c, --clean` | Drop + recreate schemas first | |
| `-a, --data-only` | Skip DDL, restore only data | |
| `--retries` | Max retries per chunk | 5 |
| `--retry-delay` | Base retry delay (seconds) | 5 |
| `--dry-run` | Preview without restoring | |

Passthrough: args after `--` are forwarded to pg_dump/pg_restore for DDL operations.

## How It Works

### Architecture

```
Coordinator Connection
  BEGIN REPEATABLE READ
  pg_export_snapshot() --> snapshot_id
  (holds transaction open for consistency)
       |
       +-- Worker 1: SET TRANSACTION SNAPSHOT
       |   COPY (SELECT * FROM big_table WHERE id BETWEEN 0 AND 25000) TO STDOUT
       |     --> zstd compress --> chunk_0000.copy.zst
       |
       +-- Worker 2: SET TRANSACTION SNAPSHOT
       |   COPY (SELECT * FROM big_table WHERE id BETWEEN 25001 AND 50000) TO STDOUT
       |     --> zstd compress --> chunk_0001.copy.zst
       |
       +-- Worker 3: SET TRANSACTION SNAPSHOT
       |   COPY small_table TO STDOUT
       |     --> zstd compress --> chunk_0000.copy.zst
       |
       +-- Worker 4: ...
```

### Chunking Strategies

| Strategy | When | How |
|----------|------|-----|
| **pk_range** | Table > threshold, has integer PK | `WHERE id BETWEEN start AND end` |
| **ctid_range** | Table > threshold, no integer PK, PG 14+ | `WHERE ctid >= '(page,0)'::tid` |
| **none** | Table < threshold | Single `COPY table TO STDOUT` |

Large tables are split into volume-balanced chunks by sampling row sizes at evenly-spaced PK points. This handles skewed data distributions where some rows are much larger than others.

### Resume

**Dump**: Each completed chunk writes a `.done` marker file. Re-running the dump skips chunks that already have markers.

**Restore**: Completed chunks are tracked in a `_pg_turbo._progress` table in the target database, committed atomically with the data. Survives crashes.

### Output Format

```
dump_dir/
  manifest.json                    # Metadata: tables, chunks, sequences, options
  _schema_ddl.dump                 # pg_dump custom format (DDL only)
  data/
    public.users/
      chunk_0000.copy.zst          # COPY text format, zstd compressed
      chunk_0000.copy.zst.done     # Resume marker
      chunk_0001.copy.zst
      chunk_0001.copy.zst.done
    public.orders/
      chunk_0000.copy.zst
      chunk_0000.copy.zst.done
```

## Benchmarks

On a 144MB test database (500K rows, local Docker):

| Method | Time | vs pg_dump |
|--------|------|-----------|
| pg_dump -Fc (single) | 2.1s | baseline |
| pg_dump -Fd -j4 | 1.6s | 1.3x |
| **pg-turbo -j1** | **1.0s** | **2.1x** |
| **pg-turbo -j4** | **0.8s** | **2.6x** |

The speed advantage comes from direct COPY protocol (no per-table pg_dump process spawning) and overlapping DDL dump with data dump. On remote databases over VPN, the advantage is larger due to connection reuse and chunked retry.

## Development

```bash
npm install          # Install dependencies
npm run typecheck    # TypeScript strict mode check
npm test             # Unit tests (vitest)
npm run test:integration  # Integration tests (requires Docker)
npm run bench        # Performance benchmarks
npm run lint         # Lint (biome)
npm run format       # Format (biome)
```

### Project Structure

```
src/
  cli/          # CLI concerns (args, orchestration, terminal UI)
  core/         # Reusable logic (connection, streaming, chunking, schema)
  types/        # Shared TypeScript type definitions
tests/
  unit/         # Vitest unit tests
  integration/  # Docker-based end-to-end tests
bench/          # Performance benchmarks
```

## License

[MIT](LICENSE)
