#!/usr/bin/env bash
# ── Unit tests for lib/format.sh ────────────────────────────────────────────
# shellcheck source=./test_helper.sh
source "$(dirname "$0")/test_helper.sh"
source "${SCRIPT_DIR}/lib/colors.sh"
source "${SCRIPT_DIR}/lib/format.sh"

begin_tests "lib/format.sh"

# ── human_size ──────────────────────────────────────────────────────────────

echo "  human_size:"
assert_eq "0 bytes" "0 B" "$(human_size 0)"
assert_eq "500 bytes" "500 B" "$(human_size 500)"
assert_eq "1023 bytes" "1023 B" "$(human_size 1023)"
assert_eq "1 KB" "1.0 KB" "$(human_size 1024)"
assert_eq "1.5 KB" "1.5 KB" "$(human_size 1536)"
assert_eq "1 MB" "1.0 MB" "$(human_size 1048576)"
assert_eq "2.5 MB" "2.5 MB" "$(human_size 2621440)"
assert_eq "1 GB" "1.00 GB" "$(human_size 1073741824)"
assert_eq "3.50 GB" "3.50 GB" "$(human_size 3758096384)"
echo ""

# ── elapsed_time ────────────────────────────────────────────────────────────

echo "  elapsed_time:"
assert_eq "0 seconds" "0s" "$(elapsed_time 0)"
assert_eq "59 seconds" "59s" "$(elapsed_time 59)"
assert_eq "1 minute" "1m 0s" "$(elapsed_time 60)"
assert_eq "1m 1s" "1m 1s" "$(elapsed_time 61)"
assert_eq "5m 30s" "5m 30s" "$(elapsed_time 330)"
assert_eq "1 hour" "1h 0m 0s" "$(elapsed_time 3600)"
assert_eq "1h 1m 1s" "1h 1m 1s" "$(elapsed_time 3661)"
assert_eq "2h 30m 45s" "2h 30m 45s" "$(elapsed_time 9045)"
echo ""

# ── progress_bar ────────────────────────────────────────────────────────────

echo "  progress_bar:"
assert_contains "shows 0%" "0%" "$(progress_bar 0 10)"
assert_contains "shows 50%" "50%" "$(progress_bar 5 10)"
assert_contains "shows 100%" "100%" "$(progress_bar 10 10)"
assert_contains "shows count" "(3/7)" "$(progress_bar 3 7)"
assert_contains "shows bytes" "(1.0 MB / 1.0 MB)" "$(progress_bar 1048576 1048576)"
assert_contains "shows bytes pct" " 50%" "$(progress_bar 524288 1048576)"
echo ""

# ── file_size ───────────────────────────────────────────────────────────────

echo "  file_size:"
tmpfile="./tests/tmp_test_file"
printf "hello" >"${tmpfile}"
assert_eq "real file (5 bytes)" "5" "$(file_size "${tmpfile}")"
rm -f "${tmpfile}"
assert_eq "missing file" "0" "$(file_size "/nonexistent/file")"
echo ""

# ── Summary ─────────────────────────────────────────────────────────────────

finish_tests
