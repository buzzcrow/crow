<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R134: diskdb — Liveness and RPC Module Boundaries

## Problem

### Current behavior + impact

The DiskDB behavioral conformance work separated several feature boundaries,
but `liveness/keepalive.rs` still combines group-0 observation,
reconciliation, heartbeat publication, and loading coordination, while
`service/diskdb_rpc_service.rs` still combines request validation, mutation,
query, and admin handlers. At 1,230 and 1,951 lines respectively, these files
make feature ownership and focused testing harder to follow. This is a
maintainability issue; the completed behavioral fixes do not depend on the
remaining split.

### Design pointers

- `doc/design/diskdb/design-crowdb-diskdb.md` §3, §8, §11, and §17–§19.
- `doc/design/diskdb/design-crowdb-diskdb-zone-management.md` §4–§10.

### Use scenarios

- **Sync maintenance** — a developer changes group-0 observation without
  touching heartbeat publication or runtime reconciliation behavior.
- **Mutation maintenance** — a developer changes allocate, commit, or free
  validation through one shared mutation boundary and runs focused tests.
- **Query and admin maintenance** — capacity, scanner, and status handlers
  evolve without expanding the mutation module.

## Solution

Extract cohesive internal modules while preserving the public service and
background-task interfaces and all existing lock-free hot paths.

1. **Liveness split** — `app/crowdb-diskdb/src/liveness/`: separate stable
   observation, reconciliation, heartbeat, and load-coordination units from
   the `KeepAlive` scheduler.
2. **RPC split** — `app/crowdb-diskdb/src/service/`: separate common request
   gates, mutations, queries, and admin/scanner handlers behind the current
   `DiskdbRpcService` interface.
3. **Feature tests** — keep public behavior in the existing feature-grouped
   server and `crowdb-diskdb-client` suites; add narrow unit tests only for
   newly extracted pure boundaries.

```text
KeepAlive scheduler ──► observation ──► reconciliation ──► loading
        └─────────────────────────────► heartbeat

DiskdbRpcService ──► common gate ──► mutations
                  ├───────────────► queries
                  └───────────────► admin/scanner
```

### Edge cases at a glance

- Partial group-0 view → retain last-known-good state and degraded behavior.
- Ownership or bind changes during load → retain generation fencing.
- Allocate, commit, and free → retain the same lifecycle and ownership gate.
- Extracted modules → add no blocking lock to a hot path.

## Dependencies

- Builds on the liveness, mutation-gate, and feature-test behavior completed
  by R130.
- Must preserve R102 ownership/binding boundaries and R132 compaction fencing.
- No later requirement depends on this refactor.

## Acceptance

**Liveness boundaries**:

- Feed complete, partial, stale, and changed owner/bind observations through
  the extracted units → assert the current last-known-good, degraded, and
  generation-fenced outcomes are unchanged. Integration test.
- Complete or fail an asynchronous zone load after its generation changes →
  assert only the current owner/bind generation can publish. Integration test.

**RPC boundaries**:

- Send allocate, commit, and free requests in Up, degraded, non-owner, and
  missing-bind states → assert all mutations retain the shared gate and typed
  errors. Integration test.
- Exercise capacity, scanner, and status RPCs through `DiskdbClient` → assert
  their wire responses remain unchanged after extraction. E2E test.

**Concurrency and quality**:

- Run concurrent allocation while sync, reporting, and queries execute →
  assert no duplicate allocation and exact accounting without adding a
  blocking hot-path lock. E2E test.
- Run `pixi run -- cargo fmt --all -- --check`,
  `pixi run -- cargo clippy --all-targets -- -D warnings`,
  `pixi run test-diskdb`, and `pixi run test-diskdb-client`.
