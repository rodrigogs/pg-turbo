# pg_resilient TypeScript Rewrite — Implementation Plans

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite pg_resilient from Bash to TypeScript with direct COPY protocol, sub-table parallelism, and chunk-level resilience.

**Spec:** `docs/superpowers/specs/2026-03-31-typescript-rewrite-design.md`

## Phases

Execute in order. Each phase produces working, testable software.

| Phase | File | What it builds | Depends on |
|-------|------|----------------|------------|
| 1 | [phase-1-scaffolding.md](./2026-03-31-ts-rewrite-phase-1-scaffolding.md) | Project setup, types, format utils, retry | Nothing |
| 2 | [phase-2-connection.md](./2026-03-31-ts-rewrite-phase-2-connection.md) | Connection manager, manifest, CLI args | Phase 1 |
| 3 | [phase-3-pipeline.md](./2026-03-31-ts-rewrite-phase-3-pipeline.md) | Schema discovery, chunker, COPY stream | Phase 2 |
| 4 | [phase-4-orchestration.md](./2026-03-31-ts-rewrite-phase-4-orchestration.md) | Worker queue, UI, dump command, restore command | Phase 3 |
| 5 | [phase-5-integration.md](./2026-03-31-ts-rewrite-phase-5-integration.md) | Integration tests, build config, bin entry | Phase 4 |
