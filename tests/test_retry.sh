#!/usr/bin/env bash
# ── Unit tests for lib/retry.sh ─────────────────────────────────────────────
# shellcheck source=./test_helper.sh
source "$(dirname "$0")/test_helper.sh"
source "${SCRIPT_DIR}/lib/colors.sh"
source "${SCRIPT_DIR}/lib/log.sh"
source "${SCRIPT_DIR}/lib/retry.sh"

begin_tests "lib/retry.sh"

# ── run_with_retry ──────────────────────────────────────────────────────────

echo "  run_with_retry:"

# Test: succeeds on first try
MAX_RETRIES=3
RETRY_DELAY=0
run_with_retry "true command" true >/dev/null 2>&1
assert_eq "succeeds on first try (exit 0)" "0" "$?"

# Test: fails after all retries
MAX_RETRIES=2
RETRY_DELAY=0
if run_with_retry "false command" false >/dev/null 2>&1; then
  assert_eq "fails after retries (exit 1)" "1" "0"
else
  assert_eq "fails after retries (exit 1)" "1" "1"
fi

# Test: succeeds with a command that uses arguments
MAX_RETRIES=3
RETRY_DELAY=0
result=$(run_with_retry "echo test" echo "hello world" 2>/dev/null)
assert_eq "passes arguments through" "hello world" "${result}"

# Test: counter-based success (succeed on Nth attempt)
ATTEMPT_FILE=$(mktemp /tmp/test_retry.XXXXXX)
echo "0" >"${ATTEMPT_FILE}"

succeed_on_third() {
  local count
  count=$(<"${ATTEMPT_FILE}")
  count=$((count + 1))
  echo "${count}" >"${ATTEMPT_FILE}"
  ((count >= 3))
}

MAX_RETRIES=5
RETRY_DELAY=0
run_with_retry "succeed on 3rd" succeed_on_third >/dev/null 2>&1
attempts=$(<"${ATTEMPT_FILE}")
assert_eq "succeeds on 3rd attempt" "3" "${attempts}"
rm -f "${ATTEMPT_FILE}"

# Test: retries exactly MAX_RETRIES times on failure
ATTEMPT_FILE=$(mktemp /tmp/test_retry.XXXXXX)
echo "0" >"${ATTEMPT_FILE}"

count_attempts() {
  local count
  count=$(<"${ATTEMPT_FILE}")
  count=$((count + 1))
  echo "${count}" >"${ATTEMPT_FILE}"
  return 1
}

export MAX_RETRIES=3
export RETRY_DELAY=0
run_with_retry "count attempts" count_attempts >/dev/null 2>&1 || true
attempts=$(<"${ATTEMPT_FILE}")
assert_eq "runs exactly MAX_RETRIES attempts" "3" "${attempts}"
rm -f "${ATTEMPT_FILE}"
echo ""

# ── Summary ─────────────────────────────────────────────────────────────────

finish_tests
