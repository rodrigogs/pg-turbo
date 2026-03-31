#!/usr/bin/env bash
# ── Logging helpers ─────────────────────────────────────────────────────────
# Depends on: colors.sh

log_info() { printf "%sℹ%s  %s\n" "${BLUE}" "${NC}" "$*"; }
log_success() { printf "%s✔%s  %s\n" "${GREEN}" "${NC}" "$*"; }
log_warn() { printf "%s⚠%s  %s\n" "${YELLOW}" "${NC}" "$*"; }
log_error() { printf "%s✖%s  %s\n" "${RED}" "${NC}" "$*" >&2; }
log_step() { printf "%s▸%s  %s%s%s\n" "${CYAN}" "${NC}" "${BOLD}" "$*" "${NC}"; }
