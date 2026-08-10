<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan — diskdb Protocol + Core Types + Config Validation

Task plan for R70. Tracks implementation progress. Deleted after merge.

Design: `doc/working/design-diskdb-protocol-core-types.md`

## Tasks

- [ ] 1. Rewrite `lib/protocol/src/proto/diskdb.proto` (full proto from design)
- [ ] 2. Convert `app/crow-diskdb/src/types.rs` → `types/` module
  - [ ] 2a. `types/mod.rs` (declarations + re-exports)
  - [ ] 2b. `types/ids.rs` (NodeId, DiskGroupId, DiskUuid, Segment, ClaimSnapshot)
  - [ ] 2c. `types/status.rs` (Status + effective_status + allows_*)
  - [ ] 2d. `types/zone_state.rs` (ZoneState, ZoneAllocationState)
  - [ ] 2e. `types/disk_state.rs` (DiskState, DiskType)
  - [ ] 2f. `types/journal.rs` (BusyRecord, FreeRecord, ZoneRecord, key helpers, CRC)
  - [ ] 2g. `types/disk.rs` (DiskMeta)
  - [ ] 2h. `types/disk_group.rs` (DiskGroupMeta)
  - [ ] 2i. `types/node.rs` (NodeMeta)
  - [ ] 2j. `types/instance.rs` (InstanceMeta)
- [ ] 3. Convert `app/crow-diskdb/src/config.rs` → `config/` module
  - [ ] 3a. `config/mod.rs` (structs + re-exports)
  - [ ] 3b. `config/validation.rs` (validate())
- [ ] 4. Create `app/crow-diskdb/src/zone/` module + `bitmap.rs`
- [ ] 5. Update `app/crow-diskdb/src/lib.rs` (module declarations + re-exports)
- [ ] 6. Update `app/crow-diskdb/src/main.rs` (CLI flag renames, field names)
- [ ] 7. Update `app/crow-diskdb/Cargo.toml` (add crc32fast, bincode, uuid)
- [ ] 8. Write `app/crow-diskdb/tests/types.rs` (integration tests)
- [ ] 9. Run `pixi run cargo fmt --all -- --check` + fix
- [ ] 10. Run `pixi run cargo clippy --all-targets -- -D warnings` + fix
- [ ] 11. Run `pixi run clean-env && pixi run cargo test -p crow-diskdb` + fix
- [ ] 12. Commit implementation + design + plan docs
- [ ] 13. Merge design into `doc/design/diskdb/design-crow-diskdb.md`
- [ ] 14. Cleanup: delete R70 doc, backlog entry, plan + design working docs
- [ ] 15. Commit cleanup
- [ ] 16. Local CI: fmt, clippy, all test commands

## File List

- `lib/protocol/src/proto/diskdb.proto` — full rewrite
- `app/crow-diskdb/src/lib.rs` — update modules + re-exports
- `app/crow-diskdb/src/types.rs` → delete (replaced by `types/` dir)
- `app/crow-diskdb/src/types/` — new: `mod.rs`, `ids.rs`, `status.rs`,
  `zone_state.rs`, `disk_state.rs`, `journal.rs`, `disk.rs`,
  `disk_group.rs`, `node.rs`, `instance.rs`
- `app/crow-diskdb/src/config.rs` → delete (replaced by `config/` dir)
- `app/crow-diskdb/src/config/` — new: `mod.rs`, `validation.rs`
- `app/crow-diskdb/src/zone/` — new: `mod.rs`, `bitmap.rs`
- `app/crow-diskdb/src/main.rs` — CLI flag + field renames
- `app/crow-diskdb/Cargo.toml` — add deps
- `app/crow-diskdb/tests/types.rs` — new integration test

## Dependency Ordering

proto (1) → types (2) → config (3) → zone/bitmap (4) → lib.rs (5) →
main.rs (6) → Cargo.toml (7) → tests (8) → fmt/clippy/test (9-11).

## Test Checklist

- [ ] Segment serde round-trip (JSON + bincode)
- [ ] DiskUuid Display + to_key_component format
- [ ] Status ordering + effective_status + allows_allocate/allows_free
- [ ] ZoneAllocationState from_u8 (valid + unknown→Error)
- [ ] BusyRecord/FreeRecord bincode round-trip + size ≤ 32 bytes
- [ ] ZoneRecord bincode round-trip + CRC compute/verify + tamper detection
- [ ] Journal key format strings (busy, free, snapshot, prefixes)
- [ ] Sysdata key format strings (node, disk_group, disk, owner, bind, instance)
- [ ] Config validate accepts default; rejects bad block size, zone, granularity, addr
- [ ] Bitmap: range_set/range_clear, double-set, double-clear, snapshot/restore, cross-word, count_set
