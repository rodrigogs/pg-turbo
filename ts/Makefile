.PHONY: typecheck test test-watch test-integration test-all dev clean help

## typecheck: Run TypeScript type checking
typecheck:
	npx tsc --noEmit

## test: Run unit tests
test:
	npx vitest run

## test-watch: Run unit tests in watch mode
test-watch:
	npx vitest

## test-integration: Run integration tests (requires Docker)
test-integration:
	npx vitest run --config vitest.integration.config.ts

## test-all: Run unit + integration tests
test-all: test test-integration

## dev: Run CLI in development mode
dev:
	npx tsx bin/pg-resilient.ts

## clean: Remove build artifacts
clean:
	rm -rf dist coverage

## help: Show available targets
help:
	@echo "Available targets:"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /'

.DEFAULT_GOAL := help
