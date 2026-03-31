#!/usr/bin/env bash
# ── Shared queue infrastructure for parallel job processing ───────────────
# No dependencies (standalone module).
#
# Job file format: pipe-delimited fields, first field is byte size.
# Example: "32768|public|users|public.users"
#
# Uses atomic file moves (mv) for job claiming — no two workers
# can process the same job, even under race conditions.

# Initialize queue directories
queue_init() {
  rm -rf .queue
  mkdir -p .queue/{pending,processing,done,failed}
}

# Add a job to the pending queue
# Usage: queue_add_job <label> <content>
queue_add_job() {
  local label=$1
  local content=$2
  echo "${content}" >".queue/pending/${label}"
}

# Sum bytes from completed jobs (first pipe-delimited field)
queue_done_bytes() {
  local bytes=0
  if find .queue/done -maxdepth 1 -type f 2>/dev/null | grep -q .; then
    bytes=$(awk -F'|' '{sum+=$1} END {print sum}' .queue/done/*)
  fi
  echo "${bytes:-0}"
}

# Count files in a queue subdirectory
# Usage: queue_count done
queue_count() {
  local subdir=$1
  find ".queue/${subdir}" -maxdepth 1 -type f 2>/dev/null | wc -l | tr -d ' '
}

# Collect failed job labels into a nameref array
# Usage: queue_collect_failed MY_ARRAY
queue_collect_failed() {
  local -n _arr=$1
  _arr=()
  shopt -s nullglob
  for f in .queue/failed/*; do
    local name
    name=$(basename "${f}")
    # Strip worker ID prefix: "1_label" → "label"
    name="${name#*_}"
    _arr+=("${name}")
  done
  shopt -u nullglob
}

# Worker loop: claim and process jobs until queue is empty.
# Usage: queue_start_worker <worker_id> <task_callback>
#
# The callback receives: <job_content> <worker_id>
# It must return 0 on success, non-zero on failure.
# Workers write status to .queue/worker_<id>.status for dashboard display.
queue_start_worker() {
  set +e # Don't exit on task failures
  local id=$1
  local task_fn=$2
  local queue_dir=".queue"
  local status_file="${queue_dir}/worker_${id}.status"

  echo "Idle" >"${status_file}"

  while true; do
    shopt -s nullglob
    local pending_files=("${queue_dir}"/pending/*)
    shopt -u nullglob

    if [[ ${#pending_files[@]} -eq 0 ]]; then
      break
    fi

    local claimed=false
    local job_file=""

    for f in "${pending_files[@]}"; do
      local filename
      filename=$(basename "${f}")
      if mv "${f}" "${queue_dir}/processing/${id}_${filename}" 2>/dev/null; then
        job_file="${queue_dir}/processing/${id}_${filename}"
        claimed=true
        break
      fi
    done

    if [[ "${claimed}" == false ]]; then
      sleep 0.1
      continue
    fi

    # Read job content and call task callback
    local job_content
    job_content=$(<"${job_file}")

    if "${task_fn}" "${job_content}" "${id}"; then
      mv "${job_file}" "${queue_dir}/done/$(basename "${job_file}")"
    else
      mv "${job_file}" "${queue_dir}/failed/$(basename "${job_file}")"
    fi

    echo "Idle" >"${status_file}"
  done

  rm -f "${status_file}"
}

# Clean up queue directory
queue_cleanup() {
  rm -rf .queue
}
