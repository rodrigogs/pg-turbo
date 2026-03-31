# Performance Optimizations for pg_resilient

## Problem

Dump and restore operations are slower than they need to be:

1. **Compression**: gzip fallback is 20x slower than lz4
2. **Restore is sequential**: no parallel table restore
3. **Indexes during data load**: DDL creates indexes before data is loaded, causing expensive incremental index updates on every row

## Changes

### 1. Compression Cascade (dump.sh)

Try compression algorithms in speed order: lz4 > zstd > gzip.

| Algorithm | Compress | Decompress | Ratio |
|-----------|----------|------------|-------|
| lz4       | ~780 MB/s | ~4,200 MB/s | ~2.1x |
| zstd:3    | ~400 MB/s | ~1,400 MB/s | ~2.9x |
| gzip -6   | ~35 MB/s  | ~400 MB/s   | ~3.2x |

Detection: `pg_dump --help | grep -q "lz4"`. lz4 requires PG 16+ compiled with `--with-lz4`.

### 2. Custom Format DDL (dump.sh)

Change DDL dump from plain SQL to custom format:

- Before: `pg_dump --schema-only -f _schema_ddl.sql`
- After: `pg_dump --schema-only --format=custom -f _schema_ddl.dump`

This enables `pg_restore --section=pre-data|post-data` for split restore.

### 3. Section-Based Restore (restore.sh)

Split DDL restore into two phases around data loading:

```
Step 2/5: pre-data  -> CREATE TABLE, types, sequences, functions
                       (NO indexes, NO constraints, NO triggers)
Step 4/5: data      -> Load all table rows (fast - no index overhead)
Step 5/5: post-data -> CREATE INDEX, ADD CONSTRAINT, CREATE TRIGGER
                       (bulk index build - much faster than incremental)
```

Why this is fast: PostgreSQL builds indexes in a single pass over the data (sort + write), which is O(n log n). Incremental updates during INSERT are O(n * log n) per row -- dramatically slower for large tables.

`--disable-triggers` is unnecessary since FK constraints don't exist during data load (they're in post-data).

#### Resume markers

- `_pre_data.done` -- pre-data completed, skip on resume to avoid dropping tables with data
- `tables/*.dump.restored.done` -- per-table data restore markers (existing)
- `_post_data.done` -- post-data completed, skip index recreation on resume

#### Clean mode

Query the database for user schemas instead of grepping the DDL file (which is now binary):

```bash
psql -tAc "SELECT schema_name FROM information_schema.schemata
           WHERE schema_name NOT IN (${SYSTEM_SCHEMAS})"
```

#### Data-only mode

When `--data-only` is used, skip both pre-data and post-data (schema already exists with indexes). Parallel data restore still applies.

### 4. Parallel Restore (restore.sh)

Add `-j`/`--jobs N` flag. Default: 1 (sequential, current behavior).

When jobs > 1, use queue-based worker system:

1. Populate `.queue/pending/` with one job file per table
2. Start N worker subshells
3. Each worker: claim job (atomic mv) -> restore with retry -> move to done/failed
4. Main process: dashboard loop showing progress bar + per-worker status
5. After completion: count results, cleanup queue

#### TRUNCATE safety in parallel mode

During normal restore (section-based), FK constraints don't exist yet. TRUNCATE without CASCADE is safe.

For `--data-only` mode with parallel (constraints exist), retry TRUNCATE without CASCADE first, fall back to `DELETE FROM` if it fails due to FK references.

### 5. Shared Queue Module (lib/queue.sh)

Extract queue infrastructure from dump.sh into a shared module. Both dump.sh and restore.sh use the same queue primitives with different task callbacks.

Functions:

- `queue_init` -- create .queue/{pending,processing,done,failed}
- `queue_add_job <label> <content>` -- write job file
- `queue_start_worker <id> <callback_fn>` -- worker claim/process loop
- `queue_done_bytes` -- sum first field (size) from done jobs
- `queue_count <subdir>` -- count files in a queue subdirectory
- `queue_collect_failed` -- list failed job labels

The callback function receives `<job_content> <worker_id>` and returns 0 (success) or 1 (failure). Each script parses the pipe-delimited job content in its own callback.

### 6. Dashboard for Parallel Restore

Same pattern as dump.sh: hide cursor, show progress bar + per-worker status, restore cursor on exit/interrupt.

Signal handling: `kill -TERM 0` to kill entire process group (workers + any child processes).

## Output Structure

```
output_dir/
  _schema_ddl.dump          # Custom format DDL (supports --section)
  _dump.log                 # Verbose log
  tables/
    <schema.table>.dump      # Custom format per-table data
    <schema.table>.dump.done # Dump resume marker
```

Restore markers (written into the input directory):

```
input_dir/
  _pre_data.done              # Pre-data section applied
  _post_data.done             # Post-data section applied (indexes, constraints)
  tables/
    <schema.table>.dump.restored.done  # Per-table data restore marker
```

## Files Changed

| File | Change |
|------|--------|
| dump.sh | Compression cascade, DDL format change |
| restore.sh | Section-based DDL, parallel restore, -j flag, step 5 |
| lib/queue.sh | New shared queue module |
| lib/common.sh | Source queue.sh |
| dump.sh | Refactor to use lib/queue.sh |
| tests/integration/test_dump.sh | Update DDL file check (.dump) |
| tests/integration/test_restore.sh | Add parallel restore test, section-based verify |
| CLAUDE.md | Update architecture, output structure, new flags |

## Not Changing

- dump.sh parallel worker logic (already works, refactoring to use queue.sh callbacks)
- Sequential restore path (jobs=1 uses the same queue with 1 worker)
- Pipe-based pg_dump|pg_restore (no retry possible, defeats the tool's purpose)
- Switching to directory format for dump (custom format + our queue gives same benefits plus resilience)
