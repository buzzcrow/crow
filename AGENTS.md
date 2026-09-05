<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB

Distributed storage platform: Paxos consensus, per-key slots, WAL durability,
crowdb-tree engine, and a disk-block allocator. Rust workspace with C++ storage
and transport exposed through FFI.

## Project rules

- Run every build, test, lint, and executable through `pixi run`.
- `unsafe_code = deny` by default. Keep unsafe confined to existing scoped FFI
  or low-level modules; raise any new exception before adding it.
- Keep hot paths lock-free. Before adding a lock, stop and ask the user with
  the contention, ordering, and complexity trade-off.
- Add Rust tests under each crate's `tests/`; crates with `test-util` enable it
  for their own tests through self dev-dependencies.
- Preserve existing work. Never hard-reset, revert, or restore files from an
  older commit without explicit approval. Use stash or a temporary branch.
- Ordinary interactive work is committed only when asked, as one coherent
  commit. Requirement work follows `/implement-requirement`.
- Commit messages are single-line subjects without bodies, trailers, doc
  references, or requirement numbers. Code comments also omit those references.
- Before committing, run the relevant gate through pixi:
  Rust fmt and clippy, changed C++ format/tree-lint, and affected tests. Fix
  ordinary failures up to three times; report confirmed pre-existing failures.
  Requirement work uses `/implement-requirement` retry and blocking rules.
- Playwright uses an installed system browser. Do not install one locally.
- Use 60-second shell timeouts by default. Start hang-prone commands in the
  background and poll. Show complete output; do not hide errors with filters.
- Markdown is primarily read raw. Prefer bullets; use tables for real
  comparisons only. `doc/doc_index.md` always uses tables.

## Skill dispatch

- Code: `/coding`; visible UI or Playwright: also `/console-ui-e2e`.
- Test failure: `/debug-test`.
- Documentation: `/doc`, then its matched document-type guide.
- Backlog requirement: read its index and detail, then follow
  `/implement-requirement` end to end.
- Pre-push review: `/review`.
- Design question: use `doc/doc_index.md` to select one design section.
- Operations and user behavior: `doc/user-manual/user-guide.md`.

## Benchmarks

Regression sentinels live under `tools/bench-*-regression.sh`. The KV bench
spawns `crowdb-kv-server` as a subprocess, so build both binaries first:
`pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server`.

- KV: use `crowdb-cli bench kv read|write|scan`; the matching
  `tools/bench-kv-*-regression.sh` configures deployment and storage mode.
- RPC: `crowdb-cli bench rpc`; sentinel `tools/bench-rpc-regression.sh`.
- Diskdb / chunkdb: sentinels `tools/bench-diskdb-regression.sh`,
  `tools/bench-chunkdb-regression.sh`.
