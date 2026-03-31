#!/usr/bin/env bash
# ── Colors & visual constants ───────────────────────────────────────────────

export RED=$'\033[0;31m'
export GREEN=$'\033[0;32m'
export YELLOW=$'\033[1;33m'
export BLUE=$'\033[0;34m'
export CYAN=$'\033[0;36m'
export BOLD=$'\033[1m'
export DIM=$'\033[2m'
export NC=$'\033[0m'

# Schemas to always exclude (system schemas)
export SYSTEM_SCHEMAS="'pg_catalog','information_schema','pg_toast'"
