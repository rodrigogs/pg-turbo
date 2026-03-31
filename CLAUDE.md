# CLAUDE.md

## Project Overview

**pg_utils** — Resilient PostgreSQL dump & restore CLI tools for large databases.

Designed for scenarios with flaky connections (RDS over VPN, remote servers) where standard `pg_dump`/`pg_restore` fail on large schemas.

## Architecture

```
pg_utils/
├── dump.sh                    # Export: table-by-table dump with retry & resume
├── restore.sh                 # Import: restore from dump with retry & resume
├── lib/                       # Shared modules (sourced by both scripts)
│   ├── common.sh              # Loader + require_commands + source guard
│   ├── colors.sh              # ANSI color codes & SYSTEM_SCHEMAS constant
│   ├── log.sh                 # log_info, log_success, log_warn, log_error, log_step
│   ├── args.sh                # check_blocked_flag (passthrough arg validation)
│   ├── format.sh              # human_size, elapsed_time, progress_bar, file_size
│   ├── connection.sh          # sanitize_cs, extract_db_name, clean_connection_string, replace_db_in_cs, test_connection
│   ├── ui.sh                  # print_banner, print_failed_tables
│   ├── retry.sh               # run_with_retry
│   └── queue.sh               # Shared parallel queue (queue_init, queue_start_worker, etc.)
├── tests/                     # Unit tests
│   ├── test_helper.sh         # Shared test harness (assert_eq, assert_contains, etc.)
│   ├── test_format.sh         # Tests for human_size, elapsed_time, progress_bar, file_size
│   ├── test_connection.sh     # Tests for connection string helpers
│   ├── test_retry.sh          # Tests for run_with_retry
│   ├── test_ui.sh             # Tests for print_banner, print_failed_tables
│   ├── test_args.sh           # Tests for check_blocked_flag + blocked flag integration
│   └── integration/           # Integration tests (require Docker)
│       ├── run.sh             # Orchestrator: Docker up → fixtures → tests → down
│       ├── fixtures.sql       # Schema + data setup (test_alpha, test_beta)
│       ├── test_dump.sh       # Tests for dump.sh against real PG
│       └── test_restore.sh    # Tests for restore.sh against real PG
├── docker-compose.yml         # PG 16 Alpine for integration tests (port 54399)
├── .github/workflows/ci.yml   # CI pipeline (shellcheck + shfmt + tests)
├── Makefile                   # Build targets: lint, fmt, test, integration-test, test-all, check
├── .editorconfig              # Editor settings (indent, charset, newlines)
├── .shellcheckrc              # ShellCheck configuration
├── .gitignore                 # Git ignore rules
├── LICENSE                    # MIT License
├── README.md                  # Project documentation
└── CLAUDE.md                  # This file
```

Both scripts share the same design philosophy:
- **Table-by-table** operations for granular retry
- **Resume support** via `.done` marker files
- **Connection resilience** with configurable retries + backoff
- **Progress reporting** with colored output and progress bars
- **Connection string cleaning** (strips GUI params like statusColor, env, etc.)

## Tech Stack

| Tool | Purpose |
|------|------------|
| Bash | Shell scripting (set -euo pipefail) |
| pg_dump | PostgreSQL native export |
| pg_restore | PostgreSQL native import |
| psql | Connection testing, schema queries |
| bc | Size calculations |

## Commands

```bash
# Run all tests
make test

# Run integration tests (requires Docker)
make integration-test

# Run all tests (unit + integration)
make test-all

# Lint (shellcheck)
make lint

# Format check (shfmt)
make fmt-check

# Lint + tests
make check

# Dump (see --help for all options)
./dump.sh -d "postgresql://user:pass@host/db" --output "./dump_dir"

# Restore (see --help for all options)
./restore.sh -d "postgresql://user:pass@host/db" --input "./dump_dir"
```

## Development Rules

### Shared Library (`lib/`)

All common code lives in `lib/`. Both scripts load it via:
```bash
source "$(dirname "$0")/lib/common.sh"
```

Module dependency order (maintained by `common.sh`):
1. `colors.sh` — no dependencies
2. `log.sh` — depends on `colors.sh`
3. `args.sh` — depends on `log.sh`
4. `format.sh` — depends on `colors.sh`
5. `connection.sh` — depends on `log.sh` (for `test_connection`)
6. `ui.sh` — depends on `colors.sh`, `log.sh`
7. `retry.sh` — depends on `log.sh`
8. `queue.sh` — no dependencies (standalone parallel queue infrastructure)
9. `common.sh` — sources all modules + provides `require_commands()`

> [!CAUTION]
> When adding new shared functions, add them to the appropriate `lib/` module (or create a new one). **DO NOT** duplicate code between `dump.sh` and `restore.sh`.

### Shared Patterns

Both scripts MUST follow these patterns consistently:

1. **Connection string handling** (via `lib/connection.sh`)
   - `clean_connection_string()` — strip GUI query params, keep only sslmode
   - `sanitize_cs()` — mask password for display
   - `extract_db_name()` — parse DB name from URI
   - `replace_db_in_cs()` — override database name

2. **Argument interface**
   - `-d`/`--dbname` (required) — PostgreSQL connection string
   - `--output` (required for dump) / `--input` (required for restore)
   - `-n`/`--schema` (optional) — filter to specific schema
   - `-j`/`--jobs` (optional) — parallel workers (dump and restore)
   - `--dry-run` — preview without side effects
   - `--help` — usage info (no `-h` shortcut — conflicts with pg_dump's `--host`)
   - Unrecognized flags pass through to pg_dump/pg_restore
   - `--` separator for explicit passthrough

3. **Output structure** (dump creates, restore reads)
   ```
   <output_dir>/
   ├── _schema_ddl.dump          # Custom-format DDL (supports --section for split restore)
   ├── _dump.log                 # Verbose pg_dump/pg_restore log
   └── tables/
       ├── <table_name>.dump      # Custom-format table data
       ├── <table_name>.dump.done # Resume marker (empty file)
       └── ...
   ```

4. **Logging** — consistent colored output (via `lib/log.sh`):
   - `log_info` (ℹ blue), `log_success` (✔ green), `log_warn` (⚠ yellow), `log_error` (✖ red), `log_step` (▸ cyan)

5. **Error handling**
   - `set -euo pipefail`
   - `run_with_retry()` wrapper for all pg_dump/pg_restore calls
   - Individual table failures don't abort the whole process
   - Exit code 1 if any table failed

6. **DRY code** (via `lib/` modules)
   - `SYSTEM_SCHEMAS` constant (no hardcoding `'pg_catalog','information_schema','pg_toast'`)
   - `print_banner()` for header/summary boxes
   - `table_label()` for qualified vs unqualified table names
   - `human_size()` and `elapsed_time()` for formatting

### Code Style

- **Language**: Bash with `set -euo pipefail`
- **Variables**: UPPER_SNAKE_CASE for globals, lower_snake_case for locals
- **Functions**: snake_case, declared before use
- **Comments**: Section headers with `# ── Section ─────` or `# ═══ Step ═══`
- **Indentation**: 2 spaces
- **Quoting**: Always quote variables: `"$VAR"`, `"${ARRAY[@]}"`

### Testing

Run unit tests before committing:
```bash
make test
# or individually:
bash tests/test_format.sh && bash tests/test_connection.sh && bash tests/test_retry.sh && bash tests/test_args.sh
```

Test against local Docker PostgreSQL before remote:
```bash
# Local Docker PostgreSQL (trust auth, no password)
-d "postgresql://postgres@localhost:5432/postgres"

# Create test schema quickly
psql "postgresql://postgres@localhost:5432/postgres" -c "
  CREATE SCHEMA IF NOT EXISTS test_dump;
  CREATE TABLE IF NOT EXISTS test_dump.t1 (id serial, data text);
  INSERT INTO test_dump.t1 (data) SELECT md5(random()::text) FROM generate_series(1,100);
"
```

## dump.sh ✅

Dumps a database table-by-table. See `--help` for usage.

Key features:
- Table-by-table custom-format dumps with per-table retry
- Compression cascade: lz4 → zstd → gzip (fastest available)
- DDL dumped in custom format (`_schema_ddl.dump`) for section-based restore
- Resume support (skips tables with `.done` markers)
- `-n`/`--schema` filter for specific schema
- `-j`/`--jobs` for parallel table dumps
- `--dry-run` for preview
- DB/schema existence validation with helpful error messages
- Passthrough of unrecognized flags to pg_dump (blocked: `-f`, `-F`)

## restore.sh ✅

Restores a dump created by `dump.sh`. See `--help` for usage.

Key features:
- **Section-based DDL**: pre-data (tables, types) → data → post-data (indexes, constraints)
  - Data loads into bare tables without index overhead
  - Indexes built in a single fast pass after all data is loaded
- Reads dump directory auto-discovering `_schema_ddl.dump` and `tables/*.dump`
- `-j`/`--jobs` for parallel table restores with dashboard
- Per-table retry with configurable retries + backoff
- Resume support via `.restored.done`, `_pre_data.done`, `_post_data.done` markers
- `-c`/`--clean` flag — DROP + CREATE schema before restore
- `-a`/`--data-only` flag — skip DDL, restore only table data
- `-t`/`--table` flag — restore a single specific table
- `--dry-run` for preview
- Passthrough of unrecognized flags to pg_restore (blocked: `-f`, `-1`, `--exit-on-error`)
