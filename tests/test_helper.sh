#!/usr/bin/env bash
# ── Shared test harness ────────────────────────────────────────────────────
# Source this from every test file. Provides assertions and summary reporting.
#
# Usage:
#   source "$(dirname "$0")/test_helper.sh"
#   begin_tests "lib/format.sh"
#   assert_eq "test name" "expected" "actual"
#   finish_tests

[[ -n "${_TEST_HELPER_LOADED:-}" ]] && return 0
_TEST_HELPER_LOADED=1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export SCRIPT_DIR
_PASS=0
_FAIL=0

begin_tests() {
  echo "Testing $1"
  echo ""
}

assert_eq() {
  local test_name="$1" expected="$2" actual="$3"
  if [[ "${actual}" == "${expected}" ]]; then
    printf "  ✔ %s\n" "${test_name}"
    _PASS=$((_PASS + 1))
  else
    printf "  ✖ %s\n    expected: '%s'\n    actual:   '%s'\n" "${test_name}" "${expected}" "${actual}"
    _FAIL=$((_FAIL + 1))
  fi
}

assert_contains() {
  local test_name="$1" needle="$2" haystack="$3"
  if [[ "${haystack}" == *"${needle}"* ]]; then
    printf "  ✔ %s\n" "${test_name}"
    _PASS=$((_PASS + 1))
  else
    printf "  ✖ %s\n    expected to contain: '%s'\n    actual:               '%s'\n" "${test_name}" "${needle}" "${haystack}"
    _FAIL=$((_FAIL + 1))
  fi
}

assert_exit_code() {
  local test_name="$1" expected="$2"
  shift 2
  local actual=0
  "$@" >/dev/null 2>&1 || actual=$?
  if [[ "${actual}" == "${expected}" ]]; then
    printf "  ✔ %s\n" "${test_name}"
    _PASS=$((_PASS + 1))
  else
    printf "  ✖ %s\n    expected exit code: %s\n    actual exit code:   %s\n" "${test_name}" "${expected}" "${actual}"
    _FAIL=$((_FAIL + 1))
  fi
}

finish_tests() {
  echo ""
  echo "Results: ${_PASS} passed, ${_FAIL} failed"
  if ((_FAIL > 0)); then
    exit 1
  fi
}
