#!/usr/bin/env bash
set -e

# Setup
TMP_DIR="/tmp/restore_test"
mkdir -p "${TMP_DIR}"
rm -rf "${TMP_DIR:?}"/*

# Create a fresh dump to restore from
./dump.sh --output "${TMP_DIR}/source" -d "${PG_CS}" >/dev/null

echo "Testing restore..."

# Test 1: Full Roundtrip (clean) — section-based: pre-data → data → post-data
echo "  [1/5] Full restore (clean)..."
./restore.sh --input "${TMP_DIR}/source" -d "${PG_CS}" --clean
# Verify count
COUNT=$(psql "${PG_CS}" -tAc "SELECT count(*) FROM test_alpha.clients")
if [ "${COUNT}" -ne 3 ]; then
  echo "FAIL: Expected 3 clients, got ${COUNT}"
  exit 1
fi
# Verify indexes were created (post-data step)
INDEX_COUNT=$(psql "${PG_CS}" -tAc "SELECT count(*) FROM pg_indexes WHERE schemaname IN ('test_alpha','test_beta')")
if [ "${INDEX_COUNT}" -lt 1 ]; then
  echo "FAIL: Expected indexes to be created by post-data step, got ${INDEX_COUNT}"
  exit 1
fi
# Clear markers for next test
rm -f "${TMP_DIR}/source/tables/"*.restored.done
rm -f "${TMP_DIR}/source/_pre_data.done" "${TMP_DIR}/source/_post_data.done"

# Test 2: Data Only (-a) — skips pre-data and post-data
echo "  [2/5] Data only..."
psql "${PG_CS}" -c "TRUNCATE test_alpha.orders, test_alpha.clients, test_beta.products CASCADE;" >/dev/null
./restore.sh --input "${TMP_DIR}/source" -d "${PG_CS}" -a
COUNT=$(psql "${PG_CS}" -tAc "SELECT count(*) FROM test_alpha.clients")
if [ "${COUNT}" -ne 3 ]; then
  echo "FAIL: Data-only restore failed (count=${COUNT})"
  exit 1
fi
rm -f "${TMP_DIR}/source/tables/"*.restored.done

# Test 3: Schema filter (Data Only)
echo "  [3/5] Schema filter (Data Only)..."
psql "${PG_CS}" -c "TRUNCATE test_alpha.clients, test_alpha.orders, test_beta.products CASCADE;" >/dev/null
./restore.sh --input "${TMP_DIR}/source" -d "${PG_CS}" -n test_beta -a
COUNT=$(psql "${PG_CS}" -tAc "SELECT count(*) FROM test_beta.products")
if [ "${COUNT}" -ne 2 ]; then
  echo "FAIL: Schema filter restore failed for beta (count=${COUNT})"
  exit 1
fi
COUNT_A=$(psql "${PG_CS}" -tAc "SELECT count(*) FROM test_alpha.clients")
if [ "${COUNT_A}" -ne 0 ]; then
  echo "FAIL: Schema filter restored alpha (count=${COUNT_A})"
  exit 1
fi
rm -f "${TMP_DIR}/source/tables/"*.restored.done

# Test 4: Resume
echo "  [4/5] Resume..."
psql "${PG_CS}" -c "INSERT INTO test_alpha.clients (id, username) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');" >/dev/null
touch "${TMP_DIR}/source/tables/test_alpha.clients.dump.restored.done"
OUTPUT=$(./restore.sh --input "${TMP_DIR}/source" -d "${PG_CS}" -a)
if [[ "${OUTPUT}" != *"test_alpha.clients (skipped — already restored)"* ]]; then
  echo "FAIL: Resume did not detect completion. Output snippet:"
  echo "${OUTPUT}" | grep skipped || echo "(no skipped message found)"
  exit 1
fi
rm "${TMP_DIR}/source/tables/test_alpha.clients.dump.restored.done"
rm -f "${TMP_DIR}/source/tables/"*.restored.done

# Test 5: Parallel restore (-j 2)
echo "  [5/5] Parallel restore..."
rm -f "${TMP_DIR}/source/_pre_data.done" "${TMP_DIR}/source/_post_data.done"
./restore.sh --input "${TMP_DIR}/source" -d "${PG_CS}" --clean -j 2
COUNT=$(psql "${PG_CS}" -tAc "SELECT count(*) FROM test_alpha.clients")
if [ "${COUNT}" -ne 3 ]; then
  echo "FAIL: Parallel restore failed (clients count=${COUNT})"
  exit 1
fi
COUNT_B=$(psql "${PG_CS}" -tAc "SELECT count(*) FROM test_beta.products")
if [ "${COUNT_B}" -ne 2 ]; then
  echo "FAIL: Parallel restore failed (products count=${COUNT_B})"
  exit 1
fi

echo "Restore tests passed."
rm -rf "${TMP_DIR}"
