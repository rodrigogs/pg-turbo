.PHONY: lint fmt fmt-check test integration-test test-all check help

SHELL_FILES := $(shell find . -name '*.sh' -not -path './test_dump_input/*')

## lint: Run shellcheck on all .sh files
lint:
	shellcheck $(SHELL_FILES)

## fmt: Format all .sh files with shfmt
fmt:
	shfmt -w -i 2 -ci -bn $(SHELL_FILES)

## fmt-check: Check formatting without modifying files
fmt-check:
	shfmt -d -i 2 -ci -bn $(SHELL_FILES)

## test: Run all unit tests
test:
	@bash tests/test_format.sh
	@bash tests/test_connection.sh
	@bash tests/test_retry.sh
	@bash tests/test_ui.sh
	@bash tests/test_args.sh

## integration-test: Run integration tests (requires Docker)
integration-test:
	@bash tests/integration/run.sh

## test-all: Run unit + integration tests
test-all: test integration-test

## check: Run lint + tests
check: lint test

## help: Show available targets
help:
	@echo "Available targets:"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /'

.DEFAULT_GOAL := help
