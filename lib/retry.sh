#!/usr/bin/env bash
# ── Retry wrapper ───────────────────────────────────────────────────────────
# Depends on: log.sh
# Expects globals: MAX_RETRIES, RETRY_DELAY

# Usage: run_with_retry <label> <command...>
# Returns 0 on success, 1 after exhausting retries
run_with_retry() {
  local label="$1"
  shift
  local attempt=0
  while ((attempt < MAX_RETRIES)); do
    attempt=$((attempt + 1))
    if "$@"; then
      return 0
    else
      if ((attempt < MAX_RETRIES)); then
        log_warn "${label} failed (attempt ${attempt}/${MAX_RETRIES}), retrying in ${RETRY_DELAY}s..."
        sleep "${RETRY_DELAY}"
      fi
    fi
  done
  return 1
}
