#!/usr/bin/env bash
set -e

# Setup colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}Starting Integration Tests (via Docker exec)...${NC}"

# Add libpq to PATH (homebrew)
export PATH="/opt/homebrew/opt/libpq/bin:${PATH}"

# Check Docker and tools
if ! command -v docker c &>/dev/null; then
  echo -e "${RED}docker not found. Skipping integration tests.${NC}"
  exit 0
fi
if ! command -v psql &>/dev/null; then
  echo -e "${RED}psql not found (checked /opt/homebrew/opt/libpq/bin). Skipping integration tests.${NC}"
  exit 0
fi

# Trap cleanup
cleanup() {
  echo -e "${GREEN}Cleaning up Docker resources...${NC}"
  docker-compose down -v
}
trap cleanup EXIT

# Start Docker
echo -e "${GREEN}Starting Postgres container...${NC}"
docker-compose up -d --wait

# Connection string
PG_CS="postgresql://test_admin@localhost:54399/pg_utils_test"
export PG_CS

# Apply fixtures
echo -e "${GREEN}Applying fixtures...${NC}"
psql "${PG_CS}" -f tests/integration/fixtures.sql >/dev/null

# Run tests
TEST_FILES=(
  "tests/integration/test_dump.sh"
  "tests/integration/test_restore.sh"
)

errors=0
for test_file in "${TEST_FILES[@]}"; do
  if [ -f "${test_file}" ]; then
    echo -e "${GREEN}Running ${test_file}...${NC}"
    if bash "${test_file}"; then
      echo -e "${GREEN}PASS: ${test_file}${NC}"
    else
      echo -e "${RED}FAIL: ${test_file}${NC}"
      errors=$((errors + 1))
    fi
  else
    echo -e "${RED}Test file not found: ${test_file}${NC}"
    errors=$((errors + 1))
  fi
done

if [ "${errors}" -eq 0 ]; then
  echo -e "${GREEN}All integration tests passed.${NC}"
else
  echo -e "${RED}${errors} integration tests failed.${NC}"
  exit 1
fi
