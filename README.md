# pg_utils

[![ShellCheck](https://github.com/rodrigo.gomes/pg_resilient/actions/workflows/ci.yml/badge.svg)](https://github.com/rodrigo.gomes/pg_resilient/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Resilient PostgreSQL dump & restore CLI tools for large databases.

Designed for scenarios with flaky connections (RDS over VPN, remote servers) where standard `pg_dump`/`pg_restore` fail on large schemas.

## Features

- **Table-by-table** operations for granular retry — a single table failure doesn't abort the entire dump/restore
- **Resume support** — skip already-completed tables via `.done` marker files
- **Connection resilience** — configurable retries with backoff for each table
- **Progress reporting** — colored output with progress bars and size formatting
- **Connection string cleaning** — strips GUI params (statusColor, env, etc.), keeps only sslmode
- **Passthrough arguments** — unrecognized flags pass directly to pg_dump/pg_restore

## Requirements

- **Bash** (4.0+)
- **PostgreSQL client tools** — `pg_dump`, `pg_restore`, `psql`
- **bc** — for size calculations

## Installation

```bash
git clone https://github.com/rodrigo.gomes/pg_resilient.git
cd pg_resilient
chmod +x dump.sh restore.sh
```

## Usage

### Dump

Export a database table-by-table with retry and resume:

```bash
# Dump entire database
./dump.sh \
    -d "postgresql://user:pass@host:5432/mydb" \
    --output "./mydb_dump"

# Dump a specific schema
./dump.sh \
    -d "postgresql://user:pass@host:5432/mydb" \
    -n "public" \
    --output "./public_dump"

# Preview without dumping
./dump.sh \
    -d "postgresql://user:pass@host:5432/mydb" \
    --output "./mydb_dump" \
    --dry-run
```

### Restore

Restore a dump created by `dump.sh`:

```bash
# Restore entire dump
./restore.sh \
    -d "postgresql://user:pass@host:5432/mydb" \
    --input "./mydb_dump"

# Restore only data (schema already exists)
./restore.sh \
    -d "postgresql://user:pass@host:5432/mydb" \
    --input "./mydb_dump" \
    -a

# Clean restore (DROP + CREATE schema first)
./restore.sh \
    -d "postgresql://user:pass@host:5432/mydb" \
    --input "./mydb_dump" \
    -c

# Restore a single table
./restore.sh \
    -d "postgresql://user:pass@host:5432/mydb" \
    --input "./mydb_dump" \
    -t "users"
```

## Options

### dump.sh

| Option | Description | Default |
|--------|-------------|---------|
| `-d`, `--dbname` | PostgreSQL connection string (required) | — |
| `--output` | Output directory (required) | — |
| `-n`, `--schema` | Dump only this schema | all user schemas |
| `--retries` | Max retries per table on failure | 5 |
| `--retry-delay` | Seconds between retries | 10 |
| `--dry-run` | Preview what would be dumped | — |
| `--help` | Show help message | — |

### restore.sh

| Option | Description | Default |
|--------|-------------|---------|
| `-d`, `--dbname` | PostgreSQL connection string (required) | — |
| `--input` | Input directory from dump.sh (required) | — |
| `-n`, `--schema` | Restore only tables matching this schema | — |
| `-t`, `--table` | Restore a single specific table | — |
| `-c`, `--clean` | DROP and re-CREATE schema before restoring | — |
| `-a`, `--data-only` | Skip DDL, restore only table data | — |
| `--retries` | Max retries per table on failure | 5 |
| `--retry-delay` | Seconds between retries | 10 |
| `--dry-run` | Preview what would be restored | — |
| `--help` | Show help message | — |

## Passthrough Arguments

Any unrecognized flags are passed directly to `pg_dump` (for dump.sh) or `pg_restore` (for restore.sh). Use `--` to separate pg_utils flags from pg_dump/pg_restore flags.

```bash
# Pass lock timeout and disable comments
./dump.sh \
    -d "postgresql://user:pass@host:5432/mydb" \
    --output "./mydb_dump" \
    --lock-wait-timeout=300 --no-comments

# Override default compression (default: -Z 6)
./dump.sh \
    -d "postgresql://user:pass@host:5432/mydb" \
    --output "./mydb_dump" \
    -- -Z 9

# Pass disable-triggers to pg_restore
./restore.sh \
    -d "postgresql://user:pass@host:5432/mydb" \
    --input "./mydb_dump" \
    --disable-triggers
```

### Blocked flags

Some pg_dump/pg_restore flags conflict with the table-by-table operation model and are rejected:

| Flag | Script | Reason |
|------|--------|--------|
| `-f`, `--file` | dump, restore | Output/input file paths are controlled internally |
| `-F`, `--format` | dump | Must be custom format for table-by-table dumps |
| `-1`, `--single-transaction` | restore | Breaks per-table retry |
| `--exit-on-error` | restore | Breaks resilience (failures are handled per-table) |

## Output Structure

`dump.sh` creates the following directory structure, which `restore.sh` reads:

```
<output_dir>/
├── _schema_ddl.sql           # Schema structure (CREATE TABLE, indexes, etc.)
├── _dump.log                 # Verbose pg_dump log
└── tables/
    ├── <table_name>.dump      # Custom-format table data
    ├── <table_name>.dump.done # Resume marker (empty file)
    └── ...
```

## Architecture

```
pg_utils/
├── dump.sh              # Table-by-table dump with retry & resume
├── restore.sh           # Restore from dump with retry & resume
└── lib/                 # Shared modules (sourced by both scripts)
    ├── common.sh        # Module loader + require_commands
    ├── colors.sh        # ANSI color codes & constants
    ├── log.sh           # Logging functions (info, success, warn, error)
    ├── args.sh          # Blocked flag validation for passthrough
    ├── format.sh        # Size/time formatting, progress bars
    ├── connection.sh    # Connection string parsing & validation
    ├── ui.sh            # Banner and summary display
    └── retry.sh         # Retry wrapper with backoff
```

## Testing

```bash
# Run all tests
make test

# Or run individually
bash tests/test_format.sh
bash tests/test_connection.sh
bash tests/test_retry.sh
bash tests/test_ui.sh
bash tests/test_args.sh

# Lint with shellcheck
make lint

# Run everything
make check
```

## License

[MIT](LICENSE)
