#!/usr/bin/env bash
# ── Connection string helpers ───────────────────────────────────────────────
# No dependencies (pure string manipulation)

# Hide password for safe display
sanitize_cs() {
  echo "$1" | sed -E 's|://([^:]+):[^@]+@|://\1:***@|' | sed -E 's|\?.*||'
}

# Extract database name from postgresql://.../db_name?...
extract_db_name() {
  echo "$1" | sed -E 's|^postgresql://[^/]*/([^?]*).*|\1|'
}

# Strip GUI query params, keeping only sslmode if present
clean_connection_string() {
  local cs="$1"
  local base="${cs%%\?*}"
  if [[ "${cs}" == *"?"* ]]; then
    local query="${cs#*\?}"
    local sslmode
    sslmode=$(echo "${query}" | tr '&' '\n' | grep -i '^sslmode=' | head -1 || true)
    if [[ -n "${sslmode}" ]]; then
      echo "${base}?${sslmode}"
      return
    fi
  fi
  echo "${base}"
}

# Replace database name in connection string
replace_db_in_cs() {
  echo "$1" | sed -E "s|/([^/?]+)(\\?.*)?$|/${2}\\2|"
}

# Test database connection, show available databases on failure
# Globals: PG_CS, DB_NAME
test_connection() {
  if ! psql "${PG_CS}" -c "SELECT 1" &>/dev/null; then
    log_error "Cannot connect to database '${DB_NAME}'. Check your connection string."
    exit 1
  fi
  log_success "Connection OK"
}
