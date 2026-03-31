#!/usr/bin/env bash
# ── Formatting helpers ──────────────────────────────────────────────────────
# Depends on: colors.sh

human_size() {
  local bytes=$1
  if ((bytes >= 1073741824)); then
    LC_NUMERIC=C printf "%.2f GB" "$(echo "scale=2; ${bytes} / 1073741824" | bc)"
  elif ((bytes >= 1048576)); then
    LC_NUMERIC=C printf "%.1f MB" "$(echo "scale=1; ${bytes} / 1048576" | bc)"
  elif ((bytes >= 1024)); then
    LC_NUMERIC=C printf "%.1f KB" "$(echo "scale=1; ${bytes} / 1024" | bc)"
  else
    printf "%d B" "${bytes}"
  fi
}

elapsed_time() {
  local secs=$1
  if ((secs >= 3600)); then
    printf "%dh %dm %ds" $((secs / 3600)) $(((secs % 3600) / 60)) $((secs % 60))
  elif ((secs >= 60)); then
    printf "%dm %ds" $((secs / 60)) $((secs % 60))
  else
    printf "%ds" "${secs}"
  fi
}

progress_bar() {
  local current=$1 total=$2 width=30
  local pct=$((current * 100 / total))
  local filled=$((current * width / total))
  local empty=$((width - filled))
  # Use human_size if total is roughly > 1KB
  local status_str
  if ((total > 1024)); then
    status_str="($(human_size "${current}") / $(human_size "${total}"))"
  else
    status_str="(${current}/${total})"
  fi

  printf "\r  %s[%s%s" "${DIM}" "${NC}" "${GREEN}"
  printf '█%.0s' $(seq 1 "${filled}" 2>/dev/null) || true
  printf "%s" "${DIM}"
  printf '░%.0s' $(seq 1 "${empty}" 2>/dev/null) || true
  printf "%s]%s %s%3d%%%s %s" "${NC}${DIM}" "${NC}" "${BOLD}" "${pct}" "${NC}" "${status_str}"
}

# Cross-platform file size in bytes (macOS + Linux)
file_size() { stat -f%z "$1" 2>/dev/null || stat --printf='%s' "$1" 2>/dev/null || echo "0"; }
