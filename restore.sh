#!/usr/bin/env bash
# ============================================================================
# restore.sh — Resilient PostgreSQL restore with progress & retry
# ============================================================================
#
# Restores a dump created by dump.sh, table-by-table, so that
# if the connection drops, only the failed table is retried.
#
# Performance: DDL is split into pre-data (structure) and post-data (indexes).
# Data loads into bare tables without index overhead, then indexes are built
# in a single fast pass.
#
# Usage:
#   # Restore entire dump
#   ./restore.sh -d "postgresql://user:pass@host/db" --input "./dump"
#
#   # Restore only data (skip DDL)
#   ./restore.sh -d "postgresql://..." --input "./dump" -a
#
#   # Clean restore (DROP + CREATE schema first)
#   ./restore.sh -d "postgresql://..." --input "./dump" -c
#
#   # Parallel restore (4 workers)
#   ./restore.sh -d "postgresql://..." --input "./dump" -j 4
#
# Requirements: pg_restore, psql (from postgresql-client or libpq)
# ============================================================================

set -euo pipefail

# ── Load shared library ────────────────────────────────────────────────────
source "$(dirname "$0")/lib/common.sh"

# ── Defaults ────────────────────────────────────────────────────────────────
MAX_RETRIES=5
RETRY_DELAY=10
CONNECTION_STRING=""
INPUT_DIR=""
SCHEMA=""
DRY_RUN=false
CLEAN=false
DATA_ONLY=false
SINGLE_TABLE=""
JOBS=1
PG_EXTRA_ARGS=()
RESTORE_BLOCKED_FLAGS=(-f --file -1 --single-transaction --exit-on-error)

# ── Usage ───────────────────────────────────────────────────────────────────

usage() {
  cat <<EOF
${BOLD}restore.sh${NC} — Resilient PostgreSQL database restore

${BOLD}USAGE:${NC}
  ./restore.sh -d <connection_string> --input <dir> [options] [-- pg_restore_args...]

${BOLD}REQUIRED:${NC}
  -d, --dbname      PostgreSQL connection string (postgresql://...)
  --input           Input directory (created by dump.sh)

${BOLD}OPTIONAL:${NC}
  -n, --schema      Restore only tables matching this schema
  -t, --table       Restore a single specific table
  -c, --clean       DROP and re-CREATE schema before restoring
  -a, --data-only   Skip DDL restore, only restore table data
  -j, --jobs        Number of parallel restore workers    (default: 1)
  --retries         Max retries per table on failure      (default: ${MAX_RETRIES})
  --retry-delay     Seconds between retries               (default: ${RETRY_DELAY})
  --dry-run         Preview what would be restored (no changes made)
  --help            Show this help message

${BOLD}PERFORMANCE:${NC}
  DDL is restored in two phases: structure first (tables, types),
  then indexes and constraints after all data is loaded. This avoids
  expensive incremental index updates during data load.

  Use -j N for parallel table restores. Combine with --disable-triggers
  (via passthrough) for maximum speed if you have superuser access.

${BOLD}PASSTHROUGH:${NC}
  Any unrecognized flags are passed directly to pg_restore.
  Use -- to separate pg_utils flags from pg_restore flags.

  ${BOLD}Blocked flags${NC} (conflict with table-by-table operation):
    -f, --file              We control input file paths
    -1, --single-transaction  Breaks per-table retry
    --exit-on-error           Breaks resilience (we handle failures per-table)

${BOLD}EXAMPLES:${NC}
  # Restore entire dump
  ./restore.sh \\
      -d "postgresql://user:pass@host:5432/mydb" \\
      --input "./dump"

  # Parallel restore (4 workers)
  ./restore.sh \\
      -d "postgresql://user:pass@host:5432/mydb" \\
      --input "./dump" \\
      -j 4

  # Restore only data (schema already exists)
  ./restore.sh \\
      -d "postgresql://user:pass@host:5432/mydb" \\
      --input "./dump" \\
      -a

  # Clean restore (drop + recreate schema)
  ./restore.sh \\
      -d "postgresql://user:pass@host:5432/mydb" \\
      --input "./dump" \\
      -c

  # Restore single table
  ./restore.sh \\
      -d "postgresql://user:pass@host:5432/mydb" \\
      --input "./dump" \\
      -t "users"

  # Pass extra pg_restore flags
  ./restore.sh \\
      -d "postgresql://user:pass@host:5432/mydb" \\
      --input "./dump" \\
      -- --disable-triggers --no-comments
EOF
  exit 0
}

# ── Parse arguments ─────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
  case "$1" in
    -d | --dbname)
      CONNECTION_STRING="$2"
      shift 2
      ;;
    -n | --schema)
      SCHEMA="$2"
      shift 2
      ;;
    --input)
      INPUT_DIR="$2"
      shift 2
      ;;
    -t | --table)
      SINGLE_TABLE="$2"
      shift 2
      ;;
    -c | --clean)
      CLEAN=true
      shift
      ;;
    -a | --data-only)
      DATA_ONLY=true
      shift
      ;;
    -j | --jobs)
      JOBS="$2"
      shift 2
      ;;
    --retries)
      MAX_RETRIES="$2"
      shift 2
      ;;
    --retry-delay)
      RETRY_DELAY="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --help) usage ;;
    --)
      shift
      PG_EXTRA_ARGS+=("$@")
      break
      ;;
    *)
      flag_name="${1%%=*}"
      if check_blocked_flag "${flag_name}" "${RESTORE_BLOCKED_FLAGS[@]}"; then
        log_error "Flag '$1' conflicts with table-by-table operation."
        log_info "restore.sh controls input paths (-f), uses per-table transactions, and handles errors per-table."
        exit 1
      fi
      PG_EXTRA_ARGS+=("$1")
      if [[ "$1" != *"="* && $# -ge 2 && "$2" != -* ]]; then
        PG_EXTRA_ARGS+=("$2")
        shift
      fi
      shift
      ;;
  esac
done

# ── Validate arguments ──────────────────────────────────────────────────────

if [[ -z "${CONNECTION_STRING}" ]]; then
  log_error "Missing -d <connection_string>"
  exit 1
fi
if [[ -z "${INPUT_DIR}" ]]; then
  log_error "Missing --input <directory>"
  exit 1
fi

if [[ ! -d "${INPUT_DIR}" ]]; then
  log_error "Input directory does not exist: ${INPUT_DIR}"
  exit 1
fi

# ── Build clean connection string ───────────────────────────────────────────

PG_CS=$(clean_connection_string "${CONNECTION_STRING}")
DB_NAME=$(extract_db_name "${PG_CS}")

# ── Check dependencies ──────────────────────────────────────────────────────

require_commands pg_restore psql bc

# ── Validate input directory ────────────────────────────────────────────────

SCHEMA_DDL_FILE="${INPUT_DIR}/_schema_ddl.dump"
TABLES_DIR="${INPUT_DIR}/tables"
RESTORE_LOG="${INPUT_DIR}/_restore.log"
PRE_DATA_MARKER="${INPUT_DIR}/_pre_data.done"
POST_DATA_MARKER="${INPUT_DIR}/_post_data.done"

if [[ "${DATA_ONLY}" != true ]] && [[ ! -f "${SCHEMA_DDL_FILE}" ]]; then
  log_error "DDL file not found: ${SCHEMA_DDL_FILE}"
  log_info "Use --data-only to skip DDL restore, or verify the input directory."
  exit 1
fi

if [[ ! -d "${TABLES_DIR}" ]]; then
  log_warn "No tables/ directory found in ${INPUT_DIR}. Only DDL will be restored."
fi

# ── Discover dump files ─────────────────────────────────────────────────────

declare -a DUMP_FILES=() DUMP_NAMES=() DUMP_SIZES=()

if [[ -d "${TABLES_DIR}" ]]; then
  for f in "${TABLES_DIR}"/*.dump; do
    [[ ! -f "${f}" ]] && continue
    fname=$(basename "${f}" .dump)

    # Filter by --schema if specified
    if [[ -n "${SCHEMA}" ]]; then
      if [[ "${fname}" == *.* ]]; then
        file_schema="${fname%%.*}"
        [[ "${file_schema}" != "${SCHEMA}" ]] && continue
      fi
    fi

    # Filter by --table if specified
    if [[ -n "${SINGLE_TABLE}" ]]; then
      table_part="${fname##*.}"                                    # Get part after last dot (table name)
      [[ "${fname}" == "${table_part}" ]] && table_part="${fname}" # No dot = plain name
      [[ "${table_part}" != "${SINGLE_TABLE}" ]] && continue
    fi

    fsize=$(file_size "${f}")
    DUMP_FILES+=("${f}")
    DUMP_NAMES+=("${fname}")
    DUMP_SIZES+=("${fsize}")
  done
fi

TABLE_COUNT=${#DUMP_FILES[@]}
TOTAL_STEPS=5

# ── Banner ──────────────────────────────────────────────────────────────────

echo ""
if [[ "${DRY_RUN}" == true ]]; then
  print_banner "PostgreSQL Resilient Database Restore (DRY RUN)" "${YELLOW}"
else
  print_banner "PostgreSQL Resilient Database Restore" "${CYAN}"
fi
echo ""
log_info "Connection : $(sanitize_cs "${PG_CS}")"
log_info "Database   : ${BOLD}${DB_NAME}${NC}"
log_info "Input      : ${BOLD}${INPUT_DIR}${NC}"
log_info "Tables     : ${BOLD}${TABLE_COUNT}${NC} dump files found"
[[ -n "${SCHEMA}" ]] && log_info "Schema     : ${BOLD}${SCHEMA}${NC} (filter)"
[[ -n "${SINGLE_TABLE}" ]] && log_info "Table      : ${BOLD}${SINGLE_TABLE}${NC} (single)"
[[ "${CLEAN}" == true ]] && log_info "Mode       : ${YELLOW}CLEAN${NC} (DROP + CREATE schema)"
[[ "${DATA_ONLY}" == true ]] && log_info "Mode       : ${DIM}DATA ONLY${NC} (skip DDL)"
if [[ "${DRY_RUN}" == true ]]; then
  log_info "Mode       : ${YELLOW}DRY RUN${NC} (no changes will be made)"
else
  log_info "Retries    : ${MAX_RETRIES} (delay: ${RETRY_DELAY}s)"
  if ((JOBS > 1)); then
    log_info "Jobs       : ${JOBS} (parallel)"
  fi
fi
if [[ ${#PG_EXTRA_ARGS[@]} -gt 0 ]]; then
  log_info "pg_restore args: ${PG_EXTRA_ARGS[*]}"
fi
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# Step 1/5: Test connection
# ══════════════════════════════════════════════════════════════════════════════

log_step "Step 1/${TOTAL_STEPS}: Testing database connection..."
test_connection
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# Step 2/5: Restore pre-data (tables, types, sequences — NO indexes)
# ══════════════════════════════════════════════════════════════════════════════

RESTORE_START=$(date +%s)

if [[ "${DATA_ONLY}" == true ]]; then
  log_step "Step 2/${TOTAL_STEPS}: Restoring structure... ${DIM}SKIPPED (--data-only)${NC}"
elif [[ "${DRY_RUN}" == true ]]; then
  log_step "Step 2/${TOTAL_STEPS}: Restoring structure... ${YELLOW}SKIPPED (dry run)${NC}"
  if [[ -f "${SCHEMA_DDL_FILE}" ]]; then
    ddl_size=$(file_size "${SCHEMA_DDL_FILE}")
    log_info "Would restore: $(basename "${SCHEMA_DDL_FILE}") ($(human_size "${ddl_size}"))"
    [[ "${CLEAN}" == true ]] && log_info "Would DROP + CREATE schema first"
  fi
elif [[ -f "${PRE_DATA_MARKER}" ]]; then
  log_step "Step 2/${TOTAL_STEPS}: Restoring structure... ${DIM}SKIPPED (already done)${NC}"
else
  log_step "Step 2/${TOTAL_STEPS}: Restoring structure (tables, types, sequences)..."

  # Clean mode: drop existing user schemas
  if [[ "${CLEAN}" == true ]]; then
    if [[ -n "${SCHEMA}" ]]; then
      log_warn "Dropping schema '${SCHEMA}'..."
      psql "${PG_CS}" -c "DROP SCHEMA IF EXISTS \"${SCHEMA}\" CASCADE;" 2>>"${RESTORE_LOG}" || true
    else
      # Query database for existing user schemas (more reliable than parsing binary DDL)
      schemas_to_drop=$(psql "${PG_CS}" -tAc "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN (${SYSTEM_SCHEMAS}) ORDER BY 1" 2>/dev/null || true)
      if [[ -n "${schemas_to_drop}" ]]; then
        while read -r s; do
          [[ -z "${s}" ]] && continue
          log_warn "Dropping schema '${s}'..."
          psql "${PG_CS}" -c "DROP SCHEMA IF EXISTS \"${s}\" CASCADE;" 2>>"${RESTORE_LOG}" || true
        done <<<"${schemas_to_drop}"
      fi
    fi
  fi

  # Restore pre-data section: CREATE TABLE, types, sequences, functions (NO indexes).
  # pg_restore returns non-zero even for harmless warnings (ownership, comments),
  # so we verify success by checking that tables were actually created.
  pg_restore \
    --section=pre-data \
    --no-owner --no-privileges \
    -d "${PG_CS}" \
    "${SCHEMA_DDL_FILE}" 2>>"${RESTORE_LOG}" || true

  # Verify that at least some tables were created
  table_check=$(psql "${PG_CS}" -tAc \
    "SELECT count(*) FROM information_schema.tables
     WHERE table_schema NOT IN (${SYSTEM_SCHEMAS}) AND table_type = 'BASE TABLE'" 2>/dev/null || echo "0")
  if [[ "${table_check}" -gt 0 ]]; then
    touch "${PRE_DATA_MARKER}"
    log_success "Structure restored (tables, types, sequences)"
  else
    log_error "Failed to restore structure. No tables found after pre-data restore."
    log_info "Check the log: ${RESTORE_LOG}"
    exit 1
  fi
fi
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# Step 3/5: Show table inventory
# ══════════════════════════════════════════════════════════════════════════════

log_step "Step 3/${TOTAL_STEPS}: Discovering tables to restore..."

if ((TABLE_COUNT == 0)); then
  if [[ "${DATA_ONLY}" == true ]]; then
    log_warn "No table dump files found in ${TABLES_DIR}"
    exit 1
  else
    log_info "No table dump files found. Only DDL was restored."
    log_success "Done!"
    exit 0
  fi
fi

# Calculate total size
TOTAL_DUMP_SIZE=0
for s in "${DUMP_SIZES[@]}"; do
  TOTAL_DUMP_SIZE=$((TOTAL_DUMP_SIZE + s))
done

log_success "Found ${BOLD}${TABLE_COUNT}${NC} tables to restore (total dump size: $(human_size "${TOTAL_DUMP_SIZE}"))"
echo ""

# Show inventory
log_info "Tables to restore:"
for i in $(seq 0 $((TABLE_COUNT > 10 ? 9 : TABLE_COUNT - 1))); do
  printf "    %s%-45s%s %s\n" "${DIM}" "${DUMP_NAMES[${i}]}" "${NC}" "$(human_size "${DUMP_SIZES[${i}]}")"
done
((TABLE_COUNT > 10)) && printf "    %s... and %d more%s\n" "${DIM}" "$((TABLE_COUNT - 10))" "${NC}"
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# Step 4/5: Restore table data
# ══════════════════════════════════════════════════════════════════════════════

SUCCEEDED=0
FAILED=0
SKIPPED=0
declare -a FAILED_TABLES=()

# ── Restore task for parallel mode ─────────────────────────────────────────

# shellcheck disable=SC2329
restore_table_task() {
  local job_content=$1
  local worker_id=$2
  IFS='|' read -r dump_size dump_file dump_name qualified_table <<<"${job_content}"

  local done_marker="${dump_file}.restored.done"
  local status_file=".queue/worker_${worker_id}.status"

  # Resume check
  if [[ -f "${done_marker}" ]]; then
    return 0
  fi

  echo " ${BOLD}${dump_name}${NC} ${DIM}($(human_size "${dump_size}"))${NC}" >"${status_file}"

  local attempt=0
  while ((attempt < MAX_RETRIES)); do
    attempt=$((attempt + 1))
    local table_start
    table_start=$(date +%s)

    # TRUNCATE on retry to avoid duplicate key errors
    if ((attempt > 1)); then
      # No CASCADE in section-based mode (no FK constraints yet).
      # For data-only mode, fall back to DELETE if TRUNCATE fails.
      psql "${PG_CS}" -c "TRUNCATE TABLE ${qualified_table};" &>/dev/null \
        || psql "${PG_CS}" -c "DELETE FROM ${qualified_table};" &>/dev/null \
        || true
      echo " ${YELLOW}↻${NC} ${BOLD}${dump_name}${NC} ${DIM}(retry ${attempt}/${MAX_RETRIES})${NC}" >"${status_file}"
    fi

    local restore_stderr
    restore_stderr=$(mktemp /tmp/restore_stderr.XXXXXX)
    pg_restore \
      --data-only \
      --no-owner \
      --no-privileges \
      --verbose \
      ${PG_EXTRA_ARGS[@]+"${PG_EXTRA_ARGS[@]}"} \
      -d "${PG_CS}" \
      "${dump_file}" 2>"${restore_stderr}" || true

    cat "${restore_stderr}" >>"${RESTORE_LOG}"

    local has_data_error=false
    if grep -qiE 'COPY failed|constraint|violates' "${restore_stderr}" 2>/dev/null; then
      has_data_error=true
    fi
    rm -f "${restore_stderr}"

    if [[ "${has_data_error}" == false ]]; then
      local table_elapsed=$(($(date +%s) - table_start))
      touch "${done_marker}"
      echo " ${GREEN}✔${NC} ${BOLD}${dump_name}${NC} ${DIM}($(elapsed_time "${table_elapsed}"))${NC}" >"${status_file}"
      return 0
    fi

    if ((attempt < MAX_RETRIES)); then
      sleep "${RETRY_DELAY}"
    fi
  done

  echo " ${RED}✖${NC} ${BOLD}${dump_name}${NC} ${DIM}(FAILED)${NC}" >"${status_file}"
  return 1
}

# ── Signal handling ────────────────────────────────────────────────────────
_RESTORE_STDERR=""
_CURSOR_HIDDEN=false

# shellcheck disable=SC2329
cleanup_restore() {
  trap '' SIGINT SIGTERM
  [[ "${_CURSOR_HIDDEN}" == true ]] && printf "\033[?25h"
  if ((JOBS > 1)); then
    kill -TERM 0 2>/dev/null || true
    wait 2>/dev/null || true
    queue_cleanup
  fi
  [[ -n "${_RESTORE_STDERR}" && -f "${_RESTORE_STDERR}" ]] && rm -f "${_RESTORE_STDERR}"
  exit 1
}
trap cleanup_restore SIGINT SIGTERM

if [[ "${DRY_RUN}" == true ]]; then
  log_step "Step 4/${TOTAL_STEPS}: Restoring table data... ${YELLOW}SKIPPED (dry run)${NC}"
  echo ""
  for i in $(seq 0 $((TABLE_COUNT - 1))); do
    progress_bar "$((i + 1))" "${TABLE_COUNT}"
    printf "  %s%s%s %s(%s)%s %s→ would restore%s\n" "${DIM}" "${DUMP_NAMES[${i}]}" "${NC}" "${DIM}" "$(human_size "${DUMP_SIZES[${i}]}")" "${NC}" "${YELLOW}" "${NC}"
  done
  SUCCEEDED=${TABLE_COUNT}

elif ((JOBS > 1)); then
  # ── Parallel restore ─────────────────────────────────────────────────────
  log_step "Step 4/${TOTAL_STEPS}: Restoring table data (Jobs: ${JOBS})..."
  echo ""

  queue_init

  # Populate queue
  for i in $(seq 0 $((TABLE_COUNT - 1))); do
    dump_file="${DUMP_FILES[${i}]}"
    dump_name="${DUMP_NAMES[${i}]}"
    dump_size="${DUMP_SIZES[${i}]}"

    # Derive qualified table name
    if [[ "${dump_name}" == *.* ]]; then
      qualified_table="\"${dump_name%%.*}\".\"${dump_name#*.}\""
    else
      qualified_table="\"${dump_name}\""
    fi

    done_marker="${dump_file}.restored.done"

    # Resume: mark as pre-done
    if [[ -f "${done_marker}" ]]; then
      SKIPPED=$((SKIPPED + 1))
      queue_add_job "${dump_name}" "${dump_size}|${dump_file}|${dump_name}|${qualified_table}"
      mv ".queue/pending/${dump_name}" ".queue/done/${dump_name}"
      continue
    fi

    queue_add_job "${dump_name}" "${dump_size}|${dump_file}|${dump_name}|${qualified_table}"
  done

  # Launch workers
  declare -a WORKER_PIDS=()
  for j in $(seq 1 "${JOBS}"); do
    queue_start_worker "${j}" restore_table_task &
    WORKER_PIDS+=($!)
  done

  # Dashboard
  _CURSOR_HIDDEN=true
  printf "\033[?25l"

  while true; do
    active_workers=0
    for i in $(seq 1 "${JOBS}"); do
      pid="${WORKER_PIDS[$((i - 1))]}"
      if kill -0 "${pid}" 2>/dev/null && [[ -f ".queue/worker_${i}.status" ]]; then
        active_workers=$((active_workers + 1))
      fi
    done

    if ((active_workers == 0)); then
      break
    fi

    done_count=$(queue_count "done")
    progress_bar "$((done_count))" "${TABLE_COUNT}"
    printf " %s(%d/%d tables)%s\n" "${DIM}" "${done_count}" "${TABLE_COUNT}" "${NC}"

    for j in $(seq 1 "${JOBS}"); do
      status="Idle"
      # Suppress TOCTOU race: file may vanish between -f check and read
      status=$(cat ".queue/worker_${j}.status" 2>/dev/null) || true
      [[ -z "${status}" ]] && status="Idle"
      printf "\033[2K  Worker %d:%s\n" "${j}" "${status}"
    done

    printf "\033[%dA" $((JOBS + 1))
    sleep 0.2
  done

  wait "${WORKER_PIDS[@]}" 2>/dev/null || true

  # Move cursor past dashboard
  printf "\033[%dB" $((JOBS + 1))
  printf "\033[?25h"
  _CURSOR_HIDDEN=false
  trap - SIGINT SIGTERM

  # Count results
  queue_collect_failed FAILED_TABLES
  FAILED=${#FAILED_TABLES[@]}
  SUCCEEDED=$(queue_count "done")
  queue_cleanup

else
  # ── Sequential restore (jobs=1) ──────────────────────────────────────────
  log_step "Step 4/${TOTAL_STEPS}: Restoring table data..."
  echo ""

  for i in $(seq 0 $((TABLE_COUNT - 1))); do
    dump_file="${DUMP_FILES[${i}]}"
    dump_name="${DUMP_NAMES[${i}]}"
    dump_size="${DUMP_SIZES[${i}]}"
    table_num=$((i + 1))
    done_marker="${dump_file}.restored.done"

    # Resume support: skip already-restored tables
    if [[ -f "${done_marker}" ]]; then
      SKIPPED=$((SKIPPED + 1))
      SUCCEEDED=$((SUCCEEDED + 1))
      progress_bar "${table_num}" "${TABLE_COUNT}"
      printf "  %s%s (skipped — already restored)%s\n" "${DIM}" "${dump_name}" "${NC}"
      continue
    fi

    # Derive qualified table name for TRUNCATE
    if [[ "${dump_name}" == *.* ]]; then
      qualified_table="\"${dump_name%%.*}\".\"${dump_name#*.}\""
    else
      qualified_table="\"${dump_name}\""
    fi

    progress_bar "${table_num}" "${TABLE_COUNT}"
    printf "  %s%s%s %s(%s)%s" "${BOLD}" "${dump_name}" "${NC}" "${DIM}" "$(human_size "${dump_size}")" "${NC}"

    attempt=0
    table_ok=false

    while ((attempt < MAX_RETRIES)); do
      attempt=$((attempt + 1))
      table_start=$(date +%s)

      # TRUNCATE on retry to avoid duplicate key errors
      if ((attempt > 1)); then
        psql "${PG_CS}" -c "TRUNCATE TABLE ${qualified_table};" &>/dev/null \
          || psql "${PG_CS}" -c "DELETE FROM ${qualified_table};" &>/dev/null \
          || true
      fi

      # Capture stderr separately to distinguish warnings from real errors
      restore_stderr=$(mktemp /tmp/restore_stderr.XXXXXX)
      _RESTORE_STDERR="${restore_stderr}"
      pg_restore \
        --data-only \
        --no-owner \
        --no-privileges \
        --verbose \
        ${PG_EXTRA_ARGS[@]+"${PG_EXTRA_ARGS[@]}"} \
        -d "${PG_CS}" \
        "${dump_file}" 2>"${restore_stderr}" || true

      cat "${restore_stderr}" >>"${RESTORE_LOG}"

      has_data_error=false
      if grep -qiE 'COPY failed|constraint|violates' "${restore_stderr}" 2>/dev/null; then
        has_data_error=true
      fi
      rm -f "${restore_stderr}"
      _RESTORE_STDERR=""

      if [[ "${has_data_error}" == false ]]; then
        table_elapsed=$(($(date +%s) - table_start))
        touch "${done_marker}"

        printf " %s✔%s %s%s%s\n" "${GREEN}" "${NC}" "${DIM}" "$(elapsed_time "${table_elapsed}")" "${NC}"
        SUCCEEDED=$((SUCCEEDED + 1))
        table_ok=true
        break
      else
        if ((attempt < MAX_RETRIES)); then
          printf "\n"
          log_warn "  Table '${dump_name}' failed (attempt ${attempt}/${MAX_RETRIES}). Retrying in ${RETRY_DELAY}s..."
          sleep "${RETRY_DELAY}"
          progress_bar "${table_num}" "${TABLE_COUNT}"
          printf "  %s↻%s %s%s%s %s(retry %d/%d)%s" "${YELLOW}" "${NC}" "${BOLD}" "${dump_name}" "${NC}" "${DIM}" "$((attempt + 1))" "${MAX_RETRIES}" "${NC}"
        fi
      fi
    done

    if [[ "${table_ok}" != "true" ]]; then
      printf " %s✖ FAILED%s\n" "${RED}" "${NC}"
      FAILED=$((FAILED + 1))
      FAILED_TABLES+=("${dump_name}")
    fi
  done
fi

trap - SIGINT SIGTERM
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# Step 5/5: Restore post-data (indexes, constraints, triggers)
# ══════════════════════════════════════════════════════════════════════════════

if [[ "${DATA_ONLY}" == true ]]; then
  log_step "Step 5/${TOTAL_STEPS}: Restoring indexes... ${DIM}SKIPPED (--data-only)${NC}"
elif [[ "${DRY_RUN}" == true ]]; then
  log_step "Step 5/${TOTAL_STEPS}: Restoring indexes... ${YELLOW}SKIPPED (dry run)${NC}"
elif [[ -f "${POST_DATA_MARKER}" ]]; then
  log_step "Step 5/${TOTAL_STEPS}: Restoring indexes... ${DIM}SKIPPED (already done)${NC}"
else
  log_step "Step 5/${TOTAL_STEPS}: Restoring indexes, constraints, and triggers..."
  # pg_restore returns non-zero for harmless warnings, so use || true
  pg_restore \
    --section=post-data \
    --no-owner --no-privileges \
    --clean --if-exists \
    -d "${PG_CS}" \
    "${SCHEMA_DDL_FILE}" 2>>"${RESTORE_LOG}" || true
  touch "${POST_DATA_MARKER}"
  log_success "Indexes, constraints, and triggers restored"
fi
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════════════════

TOTAL_ELAPSED=$(($(date +%s) - RESTORE_START))

echo ""
if [[ "${DRY_RUN}" == true ]]; then
  print_banner "Dry Run Summary" "${YELLOW}"
else
  print_banner "Restore Summary" "${CYAN}"
fi
echo ""

printf "  %sDatabase:%s    %s%s%s\n" "${DIM}" "${NC}" "${BOLD}" "${DB_NAME}" "${NC}"
printf "  %sInput:%s       %s\n" "${DIM}" "${NC}" "${INPUT_DIR}"
printf "  %sTables:%s      %d total\n" "${DIM}" "${NC}" "${TABLE_COUNT}"
printf "  %sSucceeded:%s   %d" "${GREEN}" "${NC}" "${SUCCEEDED}"
((SKIPPED > 0)) && printf " %s(%d skipped/resumed)%s" "${DIM}" "${SKIPPED}" "${NC}"
echo ""
((FAILED > 0)) && printf "  %sFailed:%s      %d\n" "${RED}" "${NC}" "${FAILED}"
printf "  %sDuration:%s    %s\n" "${DIM}" "${NC}" "$(elapsed_time "${TOTAL_ELAPSED}")"
echo ""

# ── Final status ────────────────────────────────────────────────────────────

if ((FAILED > 0)); then
  print_failed_tables "${FAILED_TABLES[@]}"
  log_info "Successfully restored tables will be automatically skipped."
  echo ""
  exit 1
fi

if [[ "${DRY_RUN}" == true ]]; then
  log_success "Dry run complete! No changes were made."
  echo ""
  log_info "To perform the actual restore, re-run without --dry-run"
else
  log_success "All tables restored successfully!"
fi
echo ""

exit 0
