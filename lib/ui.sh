#!/usr/bin/env bash
# ── UI helpers ──────────────────────────────────────────────────────────────
# Depends on: colors.sh

print_banner() {
  local title=$1
  local color=${2:-${CYAN}}

  echo ""
  printf "%s  %s%s\n" "${color}" "${title}" "${NC}"
  printf "%s  %s%s\n" "${DIM}" "$(printf '%*s' "${#title}" '' | tr ' ' '─')" "${NC}"
  echo ""
}

# Print failed tables list and re-run hint
# Usage: print_failed_tables FAILED_TABLES[@]
# Globals: MAX_RETRIES
print_failed_tables() {
  local tables=("$@")
  log_warn "The following tables failed after ${MAX_RETRIES} retries:"
  for ft in "${tables[@]}"; do printf "    %s✖%s %s\n" "${RED}" "${NC}" "${ft}"; done
  echo ""
  log_info "Re-run with the same arguments to retry only the failed tables."
}
# Monitor file size in background and update current line
# Usage: monitor_progress PID FILE_PATH
# Depends on: format.sh
monitor_progress() {
  local pid=$1
  local file=$2
  local total_size=${3:-0}
  local status_file=${4:-}
  local delay=1
  local spin='-\|/'
  local i=0
  local prev_size=0
  local smoothed_speed=0

  # Wait for file to exist
  while kill -0 "${pid}" 2>/dev/null && [[ ! -f "${file}" ]]; do sleep 0.1; done

  # Loop while process is running
  while kill -0 "${pid}" 2>/dev/null; do
    # If using a status file, and it was removed by the worker, stop monitoring.
    if [[ -n "${status_file}" && ! -f "${status_file}" ]]; then
      break
    fi

    local size=0
    if [[ -f "${file}" ]]; then
      size=$(file_size "${file}")
    fi

    local delta=$((size - prev_size))
    # If delta is negative (file rewritten/truncated), assume 0 for speed
    if ((delta < 0)); then delta=0; fi

    # Calculate speed (bytes per second, since delay is 1s)
    # Use Exponential Moving Average (EMA) for smoothing
    # smoothed_speed = (current_speed * alpha) + (prev_smoothed * (1 - alpha))
    local current_speed=${delta}
    local alpha="0.3"

    # If first iteration (prev_size=0), set smoothed_speed to current
    if ((prev_size == 0)); then
      smoothed_speed=${current_speed}
    else
      # bash doesn't do float, use bc
      smoothed_speed=$(echo "${current_speed} * ${alpha} + ${smoothed_speed} * (1 - ${alpha})" | bc)
    fi

    local speed_str
    speed_str=$(human_size "${smoothed_speed%.*}") # Truncate decimals for display
    local readable_size
    readable_size=$(human_size "${size}")

    # Calculate ETA if total_size > 0
    local eta_str=""
    if ((total_size > 0)); then
      local remaining=$((total_size - size))
      if ((remaining < 0)); then remaining=0; fi

      # Use smoothed_speed for ETA to be stable
      local calc_speed=${smoothed_speed%.*}
      if ((calc_speed > 0)); then
        local seconds_left=$((remaining / calc_speed))
        eta_str=", ETA: $(elapsed_time "${seconds_left}")"
      elif ((size > 0)); then
        # Stalled
        eta_str=", ETA: --"
      fi
    fi

    local spinner=${spin:i++%4:1}

    # Status string
    local status=" ${DIM}(current: ${readable_size}, ${speed_str}/s${eta_str}) ${spinner}${NC}   "

    if [[ -n "${status_file}" ]]; then
      # Write to file (for parallel dashboard)
      echo "${status}" >"${status_file}"
    else
      # Print status and move cursor back with \b
      # Extra spaces at end to overwrite previous longer values
      printf "%s" "${status}"

      # Calculate backspaces required (strip ANSI codes)
      local plain_status
      plain_status=$(printf "%s" "${status}" | sed 's/\x1b\[[0-9;]*m//g')
      local backspaces=${#plain_status}

      # Move cursor back
      for ((j = 0; j < backspaces; j++)); do printf "\b"; done
    fi

    prev_size=${size}
    sleep "${delay}"
  done

  # Clear the status line one last time when done
  if [[ -n "${status_file}" ]]; then
    # Clear file
    echo "" >"${status_file}"
  else
    # Create a blank string of same length as typical status to overwrite it
    local status
    status=" ${DIM}(current: $(human_size 0), 0 B/s, ETA: 0s) -${NC}   "
    local plain_status
    plain_status=$(printf "%s" "${status}" | sed 's/\x1b\[[0-9;]*m//g')
    local backspaces=${#plain_status}

    printf "%*s" "${backspaces}" ""                          # Print spaces to overwrite
    for ((j = 0; j < backspaces; j++)); do printf "\b"; done # Move back
  fi
}
