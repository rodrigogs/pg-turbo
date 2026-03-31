#!/usr/bin/env bash
set -e

# Setup
TMP_DIR="/tmp/dump_test"
mkdir -p "${TMP_DIR}"
rm -rf "${TMP_DIR:?}"/*

echo "Testing dump..."

# Test 1: Full Dump
echo "  [1/6] Full dump..."
./dump.sh --output "${TMP_DIR}/full" -d "${PG_CS}"
if [ ! -f "${TMP_DIR}/full/_schema_ddl.dump" ]; then
  echo "FAIL: Schema DDL missing (_schema_ddl.dump)"
  exit 1
fi

# Test 2: Schema Filter (-n test_alpha)
echo "  [2/6] Schema filter..."
./dump.sh --output "${TMP_DIR}/alpha" -d "${PG_CS}" -n test_alpha >/dev/null
# Verify DDL is custom format (binary, starts with PGDMP magic)
if ! pg_restore --list "${TMP_DIR}/alpha/_schema_ddl.dump" >/dev/null 2>&1; then
  echo "FAIL: DDL file is not valid custom format"
  exit 1
fi

# Test 3: Dry Run
echo "  [3/6] Dry run..."
./dump.sh --output "${TMP_DIR}/dry" -d "${PG_CS}" --dry-run >/dev/null
if [ -d "${TMP_DIR}/dry" ]; then
  echo "FAIL: Dry run created directory"
  exit 1
fi

# Test 4: Passthrough Args (--no-comments)
echo "  [4/6] Passthrough args..."
./dump.sh --output "${TMP_DIR}/pass" -d "${PG_CS}" -- --no-comments >/dev/null
# Verify DDL dump exists and is valid
if [ ! -f "${TMP_DIR}/pass/_schema_ddl.dump" ]; then
  echo "FAIL: Passthrough dump missing DDL"
  exit 1
fi

# Test 5: Resume
echo "  [5/6] Resume..."
mkdir -p "${TMP_DIR}/resume"
mkdir -p "${TMP_DIR}/resume/tables"
touch "${TMP_DIR}/resume/tables/test_alpha.clients.dump.done"
touch "${TMP_DIR}/resume/tables/test_alpha.orders.dump.done"
touch "${TMP_DIR}/resume/tables/test_beta.products.dump.done"
OUTPUT=$(./dump.sh --output "${TMP_DIR}/resume" -d "${PG_CS}")
if [[ "${OUTPUT}" != *"(3 skipped/resumed)"* ]]; then
  echo "FAIL: Resume did not detect completion. Output was:"
  echo "${OUTPUT}"
  exit 1
fi

# Test 6: Parallel Dump
echo "  [6/6] Parallel dump..."
mkdir -p "${TMP_DIR}/parallel"
OUTPUT=$(./dump.sh --output "${TMP_DIR}/parallel" -d "${PG_CS}" -j 2)

if [ ! -f "${TMP_DIR}/parallel/_schema_ddl.dump" ]; then
  echo "FAIL: Parallel dump schema DDL missing"
  exit 1
fi

if ! ls "${TMP_DIR}/parallel/tables/"*.dump >/dev/null 2>&1; then
  echo "FAIL: Parallel dump produced no table files"
  echo "${OUTPUT}"
  exit 1
fi

echo "Dump tests passed."
rm -rf "${TMP_DIR}"
