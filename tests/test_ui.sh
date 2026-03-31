#!/usr/bin/env bash
# ── Unit tests for lib/ui.sh ───────────────────────────────────────────────
# shellcheck source=./test_helper.sh
source "$(dirname "$0")/test_helper.sh"
source "${SCRIPT_DIR}/lib/colors.sh"
source "${SCRIPT_DIR}/lib/log.sh"
source "${SCRIPT_DIR}/lib/ui.sh"

begin_tests "lib/ui.sh"

# ── print_banner ────────────────────────────────────────────────────────────

echo "  print_banner:"
banner_output=$(print_banner "Test Banner" "${GREEN}")
assert_contains "contains title" "Test Banner" "${banner_output}"
assert_contains "has underline" "───────────" "${banner_output}"

empty_banner=$(print_banner "" "${BLUE}")
assert_contains "empty title works" "" "${empty_banner}"
echo ""

# ── print_failed_tables ────────────────────────────────────────────────────

echo "  print_failed_tables:"
export MAX_RETRIES=3
failed_output=$(print_failed_tables "users" "orders" 2>&1)
assert_contains "shows retry count" "3 retries" "${failed_output}"
assert_contains "lists first table" "users" "${failed_output}"
assert_contains "lists second table" "orders" "${failed_output}"
assert_contains "shows re-run hint" "Re-run" "${failed_output}"
echo ""

# ── monitor_progress ────────────────────────────────────────────────────────

# Mock human_size and elapsed_time for deterministic output
human_size() { echo "$1"; }
elapsed_time() { echo "$1s"; }
file_size() { echo "${SIZE_MOCK}"; }

# Mock kill so it returns true first N times then false to stop loop
KILL_CALLS=0
kill() {
  if [[ "$1" == "-0" ]]; then
    ((KILL_CALLS++))
    if ((KILL_CALLS <= 3)); then return 0; else return 1; fi
  fi
  return 0
}

echo "  monitor_progress:"
SIZE_MOCK=100
touch foo
# Run monitor_progress with PID=123 (mocked), File=foo, Total=1000
# Output capture is tricky due to backspaces, so we pipe to cat -v
output=$(UNIT_TEST=1 monitor_progress 123 "foo" 1000 | cat -v)
rm foo

# Iteration 1: size=100, prev=0, delta=100, speed=100/s, total=1000, rem=900, eta=9s
# Iteration 2: size=100... (since mocked size constant) -> delta=0 -> speed=0/s -> stall
# Iteration 3: ...

# Check for expected substrings in the raw output (ignoring backspaces/ANSI)
# "current: 100"
if [[ "${output}" == *"current: 100"* ]]; then
  echo "    ${GREEN}✔${NC} shows current size"
else
  echo "    ${RED}✖${NC} missing current size in output: ${output}"
  exit 1
fi

# "100/s" (speed)
if [[ "${output}" == *"100/s"* ]]; then
  echo "    ${GREEN}✔${NC} shows speed"
else
  echo "    ${RED}✖${NC} missing speed in output: ${output}"
  exit 1
fi

# "ETA: 9s" ((1000-100)/100 = 9)
if [[ "${output}" == *"ETA: 9s"* ]]; then
  echo "    ${GREEN}✔${NC} shows ETA"
else
  echo "    ${RED}✖${NC} missing ETA in output: ${output}"
  exit 1
fi
echo ""

# ── Summary ─────────────────────────────────────────────────────────────────

finish_tests
