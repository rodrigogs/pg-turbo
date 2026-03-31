#!/usr/bin/env bash
# ── Argument helpers ───────────────────────────────────────────────────────
# Depends on: log.sh

# check_blocked_flag FLAG BLOCKLIST...
# Returns 0 (true) if FLAG matches any entry in BLOCKLIST.
# Handles --flag=value form by stripping everything after '='.
check_blocked_flag() {
  local flag="${1%%=*}"
  shift
  local blocked
  for blocked in "$@"; do
    [[ "${flag}" == "${blocked}" ]] && return 0
  done
  return 1
}
