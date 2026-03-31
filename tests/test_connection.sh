#!/usr/bin/env bash
# ── Unit tests for lib/connection.sh ────────────────────────────────────────
# shellcheck source=./test_helper.sh
source "$(dirname "$0")/test_helper.sh"
source "${SCRIPT_DIR}/lib/connection.sh"

begin_tests "lib/connection.sh"

# ── sanitize_cs ─────────────────────────────────────────────────────────────

echo "  sanitize_cs:"
assert_eq "mask password" \
  "postgresql://user:***@host:5432/db" \
  "$(sanitize_cs "postgresql://user:secret@host:5432/db")"

assert_eq "mask password + strip query" \
  "postgresql://user:***@host/db" \
  "$(sanitize_cs "postgresql://user:p4ss@host/db?sslmode=require&statusColor=red")"

assert_eq "no password (no @)" \
  "postgresql://localhost/db" \
  "$(sanitize_cs "postgresql://localhost/db")"

assert_eq "password with special chars" \
  "postgresql://user:***@host/db" \
  "$(sanitize_cs "postgresql://user:p%40ss%23word@host/db")"
echo ""

# ── extract_db_name ─────────────────────────────────────────────────────────

echo "  extract_db_name:"
assert_eq "simple" \
  "mydb" \
  "$(extract_db_name "postgresql://user:pass@host:5432/mydb")"

assert_eq "with query params" \
  "mydb" \
  "$(extract_db_name "postgresql://user:pass@host/mydb?sslmode=require")"

assert_eq "no port" \
  "testdb" \
  "$(extract_db_name "postgresql://user@host/testdb")"

assert_eq "with credentials" \
  "mydb" \
  "$(extract_db_name "postgresql://user:pass@host/mydb")"
echo ""

# ── clean_connection_string ─────────────────────────────────────────────────

echo "  clean_connection_string:"
assert_eq "no params" \
  "postgresql://host/db" \
  "$(clean_connection_string "postgresql://host/db")"

assert_eq "keep sslmode" \
  "postgresql://host/db?sslmode=require" \
  "$(clean_connection_string "postgresql://host/db?sslmode=require&statusColor=red&env=prod")"

assert_eq "strip all non-ssl" \
  "postgresql://host/db" \
  "$(clean_connection_string "postgresql://host/db?statusColor=red&env=prod")"

assert_eq "sslmode only" \
  "postgresql://host/db?sslmode=verify-full" \
  "$(clean_connection_string "postgresql://host/db?sslmode=verify-full")"

assert_eq "sslmode not first param" \
  "postgresql://host/db?sslmode=require" \
  "$(clean_connection_string "postgresql://host/db?env=prod&sslmode=require&statusColor=red")"

assert_eq "multiple params, no sslmode" \
  "postgresql://user:pass@host:5432/db" \
  "$(clean_connection_string "postgresql://user:pass@host:5432/db?statusColor=red&env=staging&name=myconn")"
echo ""

# ── replace_db_in_cs ────────────────────────────────────────────────────────

echo "  replace_db_in_cs:"
assert_eq "simple replacement" \
  "postgresql://host:5432/new_db" \
  "$(replace_db_in_cs "postgresql://host:5432/old_db" "new_db")"

assert_eq "with query params" \
  "postgresql://host/new_db?sslmode=require" \
  "$(replace_db_in_cs "postgresql://host/old_db?sslmode=require" "new_db")"

assert_eq "with credentials" \
  "postgresql://user:pass@host:5432/new_db" \
  "$(replace_db_in_cs "postgresql://user:pass@host:5432/old_db" "new_db")"
echo ""

# ── Summary ─────────────────────────────────────────────────────────────────

finish_tests
