#!/usr/bin/env bash
# ============================================================================
# dump.sh — Resilient PostgreSQL database dump with progress & retry
# ============================================================================
#
# Dumps a PostgreSQL database (or a specific schema) table-by-table so that
# if the connection drops, only the failed table is retried — not the entire dump.
#
# Usage:
#   # Dump entire database
#   ./dump.sh -d "postgresql://user:pass@host/db" --output "./dump"
#
#   # Filter to a specific schema
#   ./dump.sh -d "postgresql://user:pass@host/db" -n public --output "./dump"
#
# Requirements: pg_dump, psql (from postgresql-client or libpq)
# ============================================================================

set -euo pipefail

# ── Load shared library ────────────────────────────────────────────────────
source "$(dirname "$0")/lib/common.sh"

# ── Defaults ────────────────────────────────────────────────────────────────
MAX_RETRIES=5
RETRY_DELAY=10
CONNECTION_STRING=""
SCHEMA=""
OUTPUT_DIR=""
DRY_RUN=false
JOBS=1
PG_EXTRA_ARGS=()
DUMP_BLOCKED_FLAGS=(-f --file -F --format)

# ── Table name helpers ──────────────────────────────────────────────────────

# Qualified display name: "schema.table" when multi-schema, "table" when single
table_label() {
  local idx=$1
  if [[ -z "${SCHEMA}" ]]; then
    echo "${TABLE_SCHEMAS[${idx}]}.${TABLE_NAMES[${idx}]}"
  else
    echo "${TABLE_NAMES[${idx}]}"
  fi
}

# ── Parallel Workers ────────────────────────────────────────────────────────

# shellcheck disable=SC2329
dump_table_task() {
  local tschema=$1
  local table=$2
  local tsize=$3
  local label=$4
  local worker_id=$5

  local table_output="${TABLES_DIR}/${label}.dump"
  local status_file=".queue/worker_${worker_id}.status"

  # Resume check
  if [[ -f "${table_output}.done" ]]; then
    return 0 # Already done
  fi

  local attempt=0

  while ((attempt < MAX_RETRIES)); do
    attempt=$((attempt + 1))

    # Update status for dashboard
    if [[ "${DRY_RUN}" == false ]]; then
      # Monitor the current worker shell.
      # On Bash 4+, use BASHPID. On Bash 3 (macOS), use sh -c 'echo $PPID'
      local mon_pid=""
      if [[ -n "${BASHPID}" ]]; then
        mon_pid=${BASHPID}
      else
        mon_pid=$(sh -c 'echo $PPID')
      fi

      monitor_progress "${mon_pid}" "${table_output}" "${tsize}" "${status_file}" &
      local monitor_pid=$!
    fi

    local log_file="${TABLES_DIR}/${label}.log"

    if pg_dump "${PG_CS}" \
      --schema="${tschema}" \
      --table="\"${tschema}\".\"${table}\"" \
      --data-only \
      --format=custom \
      --no-owner \
      --no-privileges \
      --verbose \
      "${PG_EXTRA_ARGS[@]}" \
      -f "${table_output}" >"${log_file}" 2>&1; then

      # Force kill monitor to avoid deadlock if it ignores SIGTERM
      # Squelch job termination noise by waiting
      if [[ -n "${monitor_pid}" ]]; then
        kill -9 "${monitor_pid}" 2>/dev/null || true
        wait "${monitor_pid}" 2>/dev/null || true
      fi
      touch "${table_output}.done"
      return 0
    else
      if [[ -n "${monitor_pid}" ]]; then
        kill -9 "${monitor_pid}" 2>/dev/null || true
        wait "${monitor_pid}" 2>/dev/null || true
      fi
      if ((attempt < MAX_RETRIES)); then
        # We can't use log_warn/sleep easily in parallel without blocking worker
        # Just sleep and retry
        sleep "${RETRY_DELAY}"
      fi
    fi
  done

  return 1 # Failed after retries
}

# Queue callback: parses job content and delegates to dump_table_task
# shellcheck disable=SC2329
dump_task_callback() {
  local job_content=$1
  local worker_id=$2
  IFS='|' read -r tsize tschema table label <<<"${job_content}"
  dump_table_task "${tschema}" "${table}" "${tsize}" "${label}" "${worker_id}"
}

usage() {
  cat <<EOF
${BOLD}dump.sh${NC} — Resilient PostgreSQL database dump

${BOLD}USAGE:${NC}
  ./dump.sh -d <connection_string> --output <dir> [options] [-- pg_dump_args...]

${BOLD}REQUIRED:${NC}
  -d, --dbname    PostgreSQL connection string (postgresql://...)
  --output        Output directory for the dump files

${BOLD}OPTIONAL:${NC}
  -n, --schema    Dump only this schema (default: all user schemas)
  --retries       Max retries per table on failure  (default: ${MAX_RETRIES})
  --retry-delay   Seconds between retries           (default: ${RETRY_DELAY})
  -j, --jobs      Number of parallel jobs           (default: 1)
  --dry-run       Preview what would be dumped (no actual dump)
  --help          Show this help message

${BOLD}PASSTHROUGH:${NC}
  Any unrecognized flags are passed directly to pg_dump.
  Use -- to separate pg_utils flags from pg_dump flags.

  ${BOLD}Blocked flags${NC} (conflict with table-by-table operation):
    -f, --file      We control output file paths
    -F, --format    Must be custom format for table-by-table

${BOLD}EXAMPLES:${NC}
  # Dump entire database
  ./dump.sh \\
      -d "postgresql://user:pass@host:5432/mydb" \\
      --output "./mydb_dump"

  # Dump a specific schema
  ./dump.sh \\
      -d "postgresql://user:pass@host:5432/mydb" \\
      -n "public" \\
      --output "./public_dump"

  # Pass extra pg_dump flags (lock timeout, no comments)
  ./dump.sh \\
      -d "postgresql://user:pass@host:5432/mydb" \\
      --output "./mydb_dump" \\
      --lock-wait-timeout=300 --no-comments

  # Override default compression level
  ./dump.sh \\
      -d "postgresql://user:pass@host:5432/mydb" \\
      --output "./mydb_dump" \\
      -- -Z 9
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
    --output)
      OUTPUT_DIR="$2"
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
    -j | --jobs)
      JOBS="$2"
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
      if check_blocked_flag "${flag_name}" "${DUMP_BLOCKED_FLAGS[@]}"; then
        log_error "Flag '$1' conflicts with table-by-table operation."
        log_info "dump.sh controls output paths (-f) and format (-F) internally."
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

# ── Validate required args ──────────────────────────────────────────────────

if [[ -z "${CONNECTION_STRING}" ]]; then
  log_error "Missing -d <connection_string>"
  exit 1
fi
if [[ -z "${OUTPUT_DIR}" ]]; then
  log_error "Missing --output <directory>"
  exit 1
fi

# ── Build clean connection string ───────────────────────────────────────────

PG_CS=$(clean_connection_string "${CONNECTION_STRING}")
DB_NAME=$(extract_db_name "${PG_CS}")

# ── Check dependencies ──────────────────────────────────────────────────────

require_commands pg_dump psql bc

# ── Computed labels ─────────────────────────────────────────────────────────

SCHEMA_DISPLAY="all user schemas"
DUMP_LABEL="database '${DB_NAME}'"
if [[ -n "${SCHEMA}" ]]; then
  SCHEMA_DISPLAY="${SCHEMA}"
  DUMP_LABEL="schema '${SCHEMA}' in database '${DB_NAME}'"
fi

# ── Banner ──────────────────────────────────────────────────────────────────

echo ""
if [[ "${DRY_RUN}" == true ]]; then
  print_banner "PostgreSQL Resilient Database Dump (DRY RUN)" "${YELLOW}"
else
  print_banner "PostgreSQL Resilient Database Dump" "${CYAN}"
fi
echo ""
log_info "Connection : $(sanitize_cs "${PG_CS}")"
log_info "Database   : ${BOLD}${DB_NAME}${NC}"
if [[ -n "${SCHEMA}" ]]; then
  log_info "Schema     : ${BOLD}${SCHEMA}${NC}"
else
  log_info "Schema     : ${DIM}all user schemas${NC}"
fi
log_info "Output     : ${BOLD}${OUTPUT_DIR}${NC}"
if [[ "${DRY_RUN}" == true ]]; then
  log_info "Mode       : ${YELLOW}DRY RUN${NC} (no files will be written)"
else
  log_info "Retries    : ${MAX_RETRIES} (delay: ${RETRY_DELAY}s)"
  if ((JOBS > 1)); then
    log_info "Jobs       : ${JOBS} (parallel)"
  fi
fi
if [[ ${#PG_EXTRA_ARGS[@]} -gt 0 ]]; then
  log_info "pg_dump args: ${PG_EXTRA_ARGS[*]}"
fi
echo ""

# ── Create output directory ─────────────────────────────────────────────────

if [[ "${DRY_RUN}" != true ]]; then
  mkdir -p "${OUTPUT_DIR}"
fi
DUMP_LOG="${OUTPUT_DIR}/_dump.log"

# ══════════════════════════════════════════════════════════════════════════════
# Step 1/5: Test connection
# ══════════════════════════════════════════════════════════════════════════════

log_step "Step 1/5: Testing database connection..."
test_connection
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# Step 2/5: Verify schema / discover schemas
# ══════════════════════════════════════════════════════════════════════════════

# ── Compression Check ─────────────────────────────────────────────────────
# Try fastest algorithm available: lz4 > zstd > gzip
compress_arg="-Z 6"
compress_label="${YELLOW}gzip (level 6)${NC} (lz4/zstd not available)"
if pg_dump --help | grep -q "lz4"; then
  compress_arg="-Z lz4"
  compress_label="${GREEN}lz4${NC}"
elif pg_dump --help | grep -q "zstd"; then
  compress_arg="-Z zstd:3"
  compress_label="${GREEN}zstd:3${NC}"
fi
log_info "Compression: ${compress_label}"
PG_EXTRA_ARGS+=("${compress_arg}")

if [[ -n "${SCHEMA}" ]]; then
  log_step "Step 2/5: Verifying schema '${SCHEMA}' exists..."
  SCHEMA_EXISTS=$(psql "${PG_CS}" -tAc "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '${SCHEMA}'")
  if [[ "${SCHEMA_EXISTS}" -eq 0 ]]; then
    log_error "Schema '${SCHEMA}' does not exist in database '${DB_NAME}'."
    log_info "Available schemas:"
    psql "${PG_CS}" -tAc "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN (${SYSTEM_SCHEMAS}) ORDER BY 1" | while read -r s; do
      echo "    - ${s}"
    done
    exit 1
  fi
  log_success "Schema '${SCHEMA}' found"
  SCHEMA_FILTER="AND t.table_schema = '${SCHEMA}'"
else
  log_step "Step 2/5: Discovering user schemas..."
  USER_SCHEMAS=$(psql "${PG_CS}" -tAc "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN (${SYSTEM_SCHEMAS}) ORDER BY 1")
  if [[ -z "${USER_SCHEMAS}" ]]; then
    log_error "No user schemas found in database '${DB_NAME}'."
    exit 1
  fi
  log_success "Found user schemas:"
  echo "${USER_SCHEMAS}" | while read -r s; do echo "    - ${s}"; done
  SCHEMA_FILTER="AND t.table_schema NOT IN (${SYSTEM_SCHEMAS})"
fi
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# Step 3/5: Dump schema structure (DDL)
# ══════════════════════════════════════════════════════════════════════════════

SCHEMA_DDL_FILE="${OUTPUT_DIR}/_schema_ddl.dump"

DDL_ARGS=("${PG_CS}" --schema-only --format=custom --no-owner --no-privileges --verbose)
if [[ -n "${SCHEMA}" ]]; then
  DDL_ARGS+=(--schema="${SCHEMA}")
else
  DDL_ARGS+=(--exclude-schema='pg_catalog' --exclude-schema='information_schema' --exclude-schema='pg_toast')
fi
DDL_ARGS+=("${PG_EXTRA_ARGS[@]}")

if [[ "${DRY_RUN}" == true ]]; then
  log_step "Step 3/5: Dumping DDL (${SCHEMA_DISPLAY})... ${YELLOW}SKIPPED (dry run)${NC}"
else
  log_step "Step 3/5: Dumping DDL (${SCHEMA_DISPLAY})..."
  if ! run_with_retry "DDL dump" pg_dump "${DDL_ARGS[@]}" -f "${SCHEMA_DDL_FILE}" 2>>"${DUMP_LOG}"; then
    log_error "Failed to dump DDL after ${MAX_RETRIES} attempts."
    exit 1
  fi
  log_success "DDL saved → $(basename "${SCHEMA_DDL_FILE}")"
fi
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# Step 4/5: Discover tables and estimate sizes
# ══════════════════════════════════════════════════════════════════════════════

log_step "Step 4/5: Discovering tables (${SCHEMA_DISPLAY})..."

TABLES_INFO=$(psql "${PG_CS}" -tAF'|' -c "
  SELECT
    t.table_schema,
    t.table_name,
    COALESCE(pg_total_relation_size('\"' || t.table_schema || '\".\"' || t.table_name || '\"'), 0)
  FROM information_schema.tables t
  WHERE t.table_type = 'BASE TABLE'
    ${SCHEMA_FILTER}
  ORDER BY 3 DESC
")

if [[ -z "${TABLES_INFO}" ]]; then
  log_warn "No tables found in ${DUMP_LABEL}. Only DDL was dumped."
  log_success "Done! Output → ${OUTPUT_DIR}"
  exit 0
fi

declare -a TABLE_SCHEMAS=() TABLE_NAMES=() TABLE_SIZES=()
TOTAL_SIZE=0

while IFS='|' read -r tschema tname tsize; do
  [[ -z "${tname}" ]] && continue
  TABLE_SCHEMAS+=("${tschema}")
  TABLE_NAMES+=("${tname}")
  TABLE_SIZES+=("${tsize}")
  TOTAL_SIZE=$((TOTAL_SIZE + tsize))
done <<<"${TABLES_INFO}"

TABLE_COUNT=${#TABLE_NAMES[@]}
log_success "Found ${BOLD}${TABLE_COUNT}${NC} tables (estimated total: $(human_size "${TOTAL_SIZE}"))"
echo ""

# Show top 5 largest tables
if ((TABLE_COUNT > 0)); then
  log_info "Largest tables:"
  for i in $(seq 0 $((TABLE_COUNT > 5 ? 4 : TABLE_COUNT - 1))); do
    printf "    %s%-45s%s %s\n" "${DIM}" "$(table_label "${i}")" "${NC}" "$(human_size "${TABLE_SIZES[${i}]}")"
  done
  echo ""
fi

# ══════════════════════════════════════════════════════════════════════════════
# Step 5/5: Dump table data
# ══════════════════════════════════════════════════════════════════════════════

TABLES_DIR="${OUTPUT_DIR}/tables"
SUCCEEDED=0
FAILED=0
SKIPPED=0
PROCESSED_BYTES=0
DUMP_START=$(date +%s)

declare -a FAILED_TABLES=()

if [[ "${DRY_RUN}" == true ]]; then
  log_step "Step 5/5: Dumping table data... ${YELLOW}SKIPPED (dry run)${NC}"
  echo ""
  log_info "Tables that would be dumped:"
  echo ""
  for i in $(seq 0 $((TABLE_COUNT - 1))); do
    progress_bar "${PROCESSED_BYTES}" "${TOTAL_SIZE}"
    tsize="${TABLE_SIZES[${i}]}"
    PROCESSED_BYTES=$((PROCESSED_BYTES + tsize))
    printf "  %s%s%s %s(%s)%s %s→ would dump%s\n" "${DIM}" "$(table_label "${i}")" "${NC}" "${DIM}" "$(human_size "${TABLE_SIZES[${i}]}")" "${NC}" "${YELLOW}" "${NC}"
  done
  SUCCEEDED=${TABLE_COUNT}
else
  log_step "Step 5/5: Dumping table data (Jobs: ${JOBS})..."
  echo ""
  mkdir -p "${TABLES_DIR}"

  # ── Initialize Queue ──────────────────────────────────────────────────────
  queue_init

  # Populate queue
  for i in $(seq 0 $((TABLE_COUNT - 1))); do
    tschema="${TABLE_SCHEMAS[${i}]}"
    table="${TABLE_NAMES[${i}]}"
    tsize="${TABLE_SIZES[${i}]}"
    label=$(table_label "${i}")

    # Check if already done (Resume)
    if [[ -f "${TABLES_DIR}/${label}.dump.done" ]]; then
      SKIPPED=$((SKIPPED + 1))
      SUCCEEDED=$((SUCCEEDED + 1))
      PROCESSED_BYTES=$((PROCESSED_BYTES + tsize))
      # Create dummy done file for stats
      queue_add_job "${label}" "${tsize}|${tschema}|${table}|${label}"
      mv ".queue/pending/${label}" ".queue/done/${label}"
      continue
    fi

    queue_add_job "${label}" "${tsize}|${tschema}|${table}|${label}"
  done

  # ── Snapshot Synchronization ──────────────────────────────────────────────

  SNAPSHOT_ID=""
  SNAPSHOT_PID=""

  if [[ "${DRY_RUN}" == false && "${JOBS}" -gt 1 ]]; then
    # Start a background psql session to export snapshot
    # We use a fifo or just a coprocess? Coprocess is cleaner in bash 4+ but we need compat.
    # Let's use a background process reading from a pipe.

    # Create a pipe for communication if needed, or just parse output
    # Simpler: run psql, print snapshot, sleep

    log_info "Synchronizing snapshot for parallel workers..."

    # Use file-based approach (portable bash 3/4/5, no pipe buffering issues)
    SNAPSHOT_FILE=".queue/snapshot.id"

    # Start background psql
    # We use a subshell to ensure the file is written and then we sleep
    # We filter out BEGIN/COMMIT or other noise, looking for the snapshot ID format (hex chars)
    # Actually, psql -At should only print the result of queries.
    # But "BEGIN" is printed if not -q? No, -t should hide headers.
    # Wait, BEGIN is printed by "BEGIN TRANSACTION".
    # Use -q to suppress non-query output? Or just grep?
    (
      psql "${PG_CS}" -Atq <<EOF >"${SNAPSHOT_FILE}"
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SET idle_in_transaction_session_timeout = 0;
-- Pad output to 8KB to force buffer flush
SELECT rpad(pg_export_snapshot(), 8192, ' ');
SELECT pg_sleep(10000);
EOF
    ) &
    SNAPSHOT_PID=$!

    # Wait for snapshot ID (timeout 30s)
    wait_time=0
    while true; do
      if [[ -s "${SNAPSHOT_FILE}" ]]; then
        # Check if file content is valid (not empty, not just whitespace)
        # Read the first token
        read -r potential_id <"${SNAPSHOT_FILE}"
        if [[ -n "${potential_id}" && "${potential_id}" != "BEGIN" ]]; then
          break
        fi
      fi

      sleep 0.1
      wait_time=$((wait_time + 1))
      if ((wait_time > 300)); then # 30 seconds
        log_error "Timeout waiting for snapshot export."
        kill "${SNAPSHOT_PID}" 2>/dev/null
        exit 1
      fi
      if ! kill -0 "${SNAPSHOT_PID}" 2>/dev/null; then
        log_error "Snapshot process died unexpectedly."
        cat "${SNAPSHOT_FILE}" 2>/dev/null
        exit 1
      fi
    done

    # Read ID
    read -r SNAPSHOT_ID <"${SNAPSHOT_FILE}"
    # Trim trailing spaces from padding
    SNAPSHOT_ID="${SNAPSHOT_ID%% *}"

    if [[ -n "${SNAPSHOT_ID}" ]]; then
      log_info "Snapshot ID  : ${BOLD}${SNAPSHOT_ID}${NC}"
      PG_EXTRA_ARGS+=("--snapshot=${SNAPSHOT_ID}")
    else
      log_error "Failed to export snapshot (empty ID)."
      kill "${SNAPSHOT_PID}" 2>/dev/null
      exit 1
    fi
  fi

  # ── Launch Workers ────────────────────────────────────────────────────────
  declare -a WORKER_PIDS=()
  for j in $(seq 1 "${JOBS}"); do
    queue_start_worker "${j}" dump_task_callback &
    WORKER_PIDS+=($!)
  done

  # ── Dashboard Loop ────────────────────────────────────────────────────────

  # Set trap BEFORE hiding cursor to guarantee restoration on Ctrl+C.
  # Use kill 0 to send SIGTERM to the entire process group — this kills
  # workers AND their monitor_progress grandchildren in one shot.
  _CURSOR_HIDDEN=false
  # shellcheck disable=SC2329
  cleanup_workers() {
    trap '' SIGINT SIGTERM # Prevent re-entry from our own kill
    [[ "${_CURSOR_HIDDEN}" == true ]] && printf "\033[?25h"
    kill -TERM 0 2>/dev/null || true # Kill entire process group
    wait 2>/dev/null || true         # Reap zombies
    queue_cleanup
    exit 1
  }
  trap cleanup_workers SIGINT SIGTERM

  printf "\033[?25l"
  _CURSOR_HIDDEN=true

  # Dashboard Stats
  prev_bytes=0
  prev_time=$(date +%s)
  smoothed_speed=0

  while true; do
    # Check if workers are still alive
    # kill -0 returns true for zombies, so we also check status file existence.
    active_workers=0
    for i in $(seq 1 "${JOBS}"); do
      pid="${WORKER_PIDS[$((i - 1))]}"
      # If PID is alive (or zombie) AND status file exists, worker is still running.
      if kill -0 "${pid}" 2>/dev/null && [[ -f ".queue/worker_${i}.status" ]]; then
        active_workers=$((active_workers + 1))
      fi
    done

    # If no workers alive, break
    if ((active_workers == 0)); then
      break
    fi

    # Calculate Progress from .queue/done
    PROCESSED_BYTES=$(queue_done_bytes)

    # Calculate Total Speed
    current_time=$(date +%s)
    delta_time=$((current_time - prev_time))

    if ((delta_time >= 1)); then
      delta_bytes=$((PROCESSED_BYTES - prev_bytes))
      if ((delta_bytes < 0)); then delta_bytes=0; fi

      current_speed=$((delta_bytes / delta_time))

      # EMA Smoothing (alpha=0.3)
      if ((prev_bytes == 0)); then
        smoothed_speed=${current_speed}
      else
        smoothed_speed=$(echo "${current_speed} * 0.3 + ${smoothed_speed} * 0.7" | bc)
      fi

      prev_bytes=${PROCESSED_BYTES}
      prev_time=${current_time}
    fi

    # Print Dashboard
    # 1. Overall Progress
    progress_bar "${PROCESSED_BYTES}" "${TOTAL_SIZE}"
    printf " %s(Total: %s/s)%s\n" "${DIM}" "$(human_size "${smoothed_speed%.*}")" "${NC}"

    # 2. Worker Status
    for j in $(seq 1 "${JOBS}"); do
      # Suppress TOCTOU race: file may vanish between -f check and read
      status=$(cat ".queue/worker_${j}.status" 2>/dev/null) || true
      [[ -z "${status}" ]] && status="Idle"
      printf "\033[2K  Worker %d: %s\n" "${j}" "${status}"
    done

    # Move cursor up (JOBS + 1 lines)
    printf "\033[%dA" $((JOBS + 1))

    sleep 0.2
  done

  # Terminate snapshot holder early (important: before wait!)
  [[ -n "${SNAPSHOT_PID}" ]] && kill "${SNAPSHOT_PID}" 2>/dev/null

  # Workers finished. Wait to be sure.
  wait "${WORKER_PIDS[@]}" 2>/dev/null

  # Final calculation
  PROCESSED_BYTES=$(queue_done_bytes)

  # Restore cursor and move down past dashboard
  printf "\033[%dB" $((JOBS + 1))
  printf "\033[?25h"
  _CURSOR_HIDDEN=false
  trap - SIGINT SIGTERM # Remove cleanup trap after normal completion

  # Count results from queue
  # Count results from queue
  queue_collect_failed FAILED_TABLES
  FAILED=${#FAILED_TABLES[@]}
  SUCCEEDED=$(queue_count "done")

  queue_cleanup
fi

echo ""

# ══════════════════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════════════════

TOTAL_ELAPSED=$(($(date +%s) - DUMP_START))

OUTPUT_SIZE="N/A"
if [[ "${DRY_RUN}" != true ]] && command -v du &>/dev/null; then
  OUTPUT_SIZE=$(du -sh "${OUTPUT_DIR}" 2>/dev/null | cut -f1 || echo "N/A")
fi

echo ""
if [[ "${DRY_RUN}" == true ]]; then
  print_banner "Dry Run Summary" "${YELLOW}"
else
  print_banner "Dump Summary" "${CYAN}"
fi
echo ""

printf "  %sDatabase:%s    %s%s%s\n" "${DIM}" "${NC}" "${BOLD}" "${DB_NAME}" "${NC}"
[[ -n "${SCHEMA}" ]] && printf "  %sSchema:%s      %s%s%s\n" "${DIM}" "${NC}" "${BOLD}" "${SCHEMA}" "${NC}"
printf "  %sTables:%s      %d total\n" "${DIM}" "${NC}" "${TABLE_COUNT}"
printf "  %sSucceeded:%s   %d" "${GREEN}" "${NC}" "${SUCCEEDED}"
((SKIPPED > 0)) && printf " %s(%d skipped/resumed)%s" "${DIM}" "${SKIPPED}" "${NC}"
echo ""
((FAILED > 0)) && printf "  %sFailed:%s      %d\n" "${RED}" "${NC}" "${FAILED}"
printf "  %sDuration:%s    %s\n" "${DIM}" "${NC}" "$(elapsed_time "${TOTAL_ELAPSED}")"

if [[ "${DRY_RUN}" == true ]]; then
  printf "  %sOutput:%s      %s %s(not created — dry run)%s\n" "${DIM}" "${NC}" "${OUTPUT_DIR}" "${YELLOW}" "${NC}"
else
  printf "  %sOutput:%s      %s (%s)\n" "${DIM}" "${NC}" "${OUTPUT_DIR}" "${OUTPUT_SIZE}"
fi
echo ""

# ── Final status ────────────────────────────────────────────────────────────

if ((FAILED > 0)); then
  print_failed_tables "${FAILED_TABLES[@]}"
  log_info "Successfully dumped tables will be automatically skipped."
  echo ""
  exit 1
fi

if [[ "${DRY_RUN}" == true ]]; then
  log_success "Dry run complete! No files were written."
  echo ""
  log_info "To perform the actual dump, re-run without --dry-run"
else
  log_success "All tables dumped successfully!"
  echo ""
  log_info "To restore, use:"
  echo ""
  echo "  ./restore.sh -d \$TARGET_CS --input \"${OUTPUT_DIR}\""
fi
echo ""

exit 0
