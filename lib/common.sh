#!/usr/bin/env bash
# ── Common library loader ──────────────────────────────────────────────────
# Source this file to load all pg_utils shared modules.
# Usage: source "$(dirname "$0")/lib/common.sh"

[[ -n "${_PG_UTILS_LOADED:-}" ]] && return 0
_PG_UTILS_LOADED=1

_PG_UTILS_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=./colors.sh
source "${_PG_UTILS_LIB_DIR}/colors.sh"
# shellcheck source=./log.sh
source "${_PG_UTILS_LIB_DIR}/log.sh"
# shellcheck source=./args.sh
source "${_PG_UTILS_LIB_DIR}/args.sh"
# shellcheck source=./format.sh
source "${_PG_UTILS_LIB_DIR}/format.sh"
# shellcheck source=./connection.sh
source "${_PG_UTILS_LIB_DIR}/connection.sh"
# shellcheck source=./ui.sh
source "${_PG_UTILS_LIB_DIR}/ui.sh"
# shellcheck source=./retry.sh
source "${_PG_UTILS_LIB_DIR}/retry.sh"
# shellcheck source=./queue.sh
source "${_PG_UTILS_LIB_DIR}/queue.sh"

# ── Dependency checker ─────────────────────────────────────────────────────

require_commands() {
  for cmd in "$@"; do
    if ! command -v "${cmd}" &>/dev/null; then
      log_error "Required command not found: ${cmd}"
      exit 1
    fi
  done
}
