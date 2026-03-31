#!/usr/bin/env bash
# ── Tests for lib/args.sh ──────────────────────────────────────────────────

# shellcheck source=./test_helper.sh
source "$(dirname "$0")/test_helper.sh"
source "${SCRIPT_DIR}/lib/common.sh"

begin_tests "lib/args.sh"

# ── check_blocked_flag ─────────────────────────────────────────────────────

BLOCKLIST=(-f --file -F --format)

# Blocked flags → return 0
assert_exit_code "blocks -f" "0" \
  check_blocked_flag "-f" "${BLOCKLIST[@]}"

assert_exit_code "blocks --file" "0" \
  check_blocked_flag "--file" "${BLOCKLIST[@]}"

assert_exit_code "blocks -F" "0" \
  check_blocked_flag "-F" "${BLOCKLIST[@]}"

assert_exit_code "blocks --format" "0" \
  check_blocked_flag "--format" "${BLOCKLIST[@]}"

assert_exit_code "blocks --file=/tmp/x (=value form)" "0" \
  check_blocked_flag "--file=/tmp/x" "${BLOCKLIST[@]}"

assert_exit_code "blocks --format=plain (=value form)" "0" \
  check_blocked_flag "--format=plain" "${BLOCKLIST[@]}"

# Allowed flags → return 1
assert_exit_code "allows --verbose" "1" \
  check_blocked_flag "--verbose" "${BLOCKLIST[@]}"

assert_exit_code "allows -Z" "1" \
  check_blocked_flag "-Z" "${BLOCKLIST[@]}"

assert_exit_code "allows --no-comments" "1" \
  check_blocked_flag "--no-comments" "${BLOCKLIST[@]}"

assert_exit_code "allows --lock-wait-timeout=300" "1" \
  check_blocked_flag "--lock-wait-timeout=300" "${BLOCKLIST[@]}"

# ── Restore blocklist ─────────────────────────────────────────────────────

RESTORE_BLOCKLIST=(-f --file -1 --single-transaction --exit-on-error)

assert_exit_code "restore: blocks -1" "0" \
  check_blocked_flag "-1" "${RESTORE_BLOCKLIST[@]}"

assert_exit_code "restore: blocks --single-transaction" "0" \
  check_blocked_flag "--single-transaction" "${RESTORE_BLOCKLIST[@]}"

assert_exit_code "restore: blocks --exit-on-error" "0" \
  check_blocked_flag "--exit-on-error" "${RESTORE_BLOCKLIST[@]}"

assert_exit_code "restore: allows --disable-triggers" "1" \
  check_blocked_flag "--disable-triggers" "${RESTORE_BLOCKLIST[@]}"

# ── Integration: dump.sh rejects blocked flags ─────────────────────────────

DUMP="${SCRIPT_DIR}/dump.sh"

assert_exit_code "dump.sh rejects --format=plain" "1" \
  bash "${DUMP}" -d "postgresql://x@localhost/db" --output /tmp/x --format=plain

assert_exit_code "dump.sh rejects -F plain" "1" \
  bash "${DUMP}" -d "postgresql://x@localhost/db" --output /tmp/x -F plain

assert_exit_code "dump.sh rejects --file=/tmp/out" "1" \
  bash "${DUMP}" -d "postgresql://x@localhost/db" --output /tmp/x --file=/tmp/out

assert_exit_code "dump.sh rejects -f /tmp/out" "1" \
  bash "${DUMP}" -d "postgresql://x@localhost/db" --output /tmp/x -f /tmp/out

# ── Integration: restore.sh rejects blocked flags ──────────────────────────

RESTORE="${SCRIPT_DIR}/restore.sh"

# restore.sh needs a valid --input directory to pass initial validation,
# but blocked flags are checked during arg parsing (before dir validation)
assert_exit_code "restore.sh rejects --single-transaction" "1" \
  bash "${RESTORE}" -d "postgresql://x@localhost/db" --input /tmp/x --single-transaction

assert_exit_code "restore.sh rejects -1" "1" \
  bash "${RESTORE}" -d "postgresql://x@localhost/db" --input /tmp/x -1

assert_exit_code "restore.sh rejects --exit-on-error" "1" \
  bash "${RESTORE}" -d "postgresql://x@localhost/db" --input /tmp/x --exit-on-error

assert_exit_code "restore.sh rejects --file=/tmp/out" "1" \
  bash "${RESTORE}" -d "postgresql://x@localhost/db" --input /tmp/x --file=/tmp/out

# ── Missing Required Args ────────────────────────────────────────────────

assert_exit_code "dump: missing --output" "1" \
  bash "${DUMP}" -d "postgresql://x@localhost/db"

assert_exit_code "dump: missing --db" "1" \
  bash "${DUMP}" --output /tmp/x

assert_exit_code "restore: missing --input" "1" \
  bash "${RESTORE}" -d "postgresql://x@localhost/db"

assert_exit_code "restore: missing --db" "1" \
  bash "${RESTORE}" --input /tmp/x

# ── Allowed Short Flags  ─────────────────────────────────────────────────────

assert_exit_code "allows -n (schema)" "1" \
  check_blocked_flag "-n" "${BLOCKLIST[@]}"

assert_exit_code "restore: allows -c (clean)" "1" \
  check_blocked_flag "-c" "${RESTORE_BLOCKLIST[@]}"

assert_exit_code "restore: allows -a (data-only)" "1" \
  check_blocked_flag "-a" "${RESTORE_BLOCKLIST[@]}"

assert_exit_code "restore: allows -t (table)" "1" \
  check_blocked_flag "-t" "${RESTORE_BLOCKLIST[@]}"

assert_exit_code "restore: allows -j (jobs)" "1" \
  check_blocked_flag "-j" "${RESTORE_BLOCKLIST[@]}"

finish_tests
