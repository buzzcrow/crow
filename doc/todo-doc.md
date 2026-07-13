# CrowKV Doc Reorganization — Status & Remaining Tasks

## Completed

### Phase 1: Code comment cleanup
- Removed all `doc/todo-sm.md` references (deleted file) across crowkv, crowkv-server, crowkv-console.
- Fixed `design-crowkv-async.md` → `design-crowkv-async-kvengine.md` in crowtree_engine.rs.
- Per review workflow, removed all `doc/` path references from code comments.

### Phase 2: Sub-design merge & index regroup
- Merged `design-paxos-error.md` into `design-rpc.md` §8 (Paxos Error Model).
- Merged `design-crowkv-async-kvengine.md` into `design-crowtree-engine.md` §4 (Rust-Side KVEngine Async Trait Shape).
- Deleted both merged files from `design/`.
- Updated all cross-references in code comments and docs to point to merged parent docs.
- Regrouped `doc_index.md` sub-design listing into thematic groups: Consensus, Storage, Operations/UI.

### Phase 3: requirement.md simplification
- Merged `design-parallel-slots.md` into `design-slot.md` (combined parallel pipelining protocol + concurrent slot list data structure; ~470 lines from ~1040 combined).
- Moved §15.4.5 CLI Command Hierarchy → `design-console.md` §12.
- Moved §15.4.6 Web UI Requirements → `design-ui.md` §13.
- Trimmed §15.2 crowkv-server (CLI table, HTTP API endpoints, topology workflow, non-goals, error handling, logging) to a 3-line pointer to `design-kv-server.md`.
- Fixed stale cross-references in: `design.md`, `design-state-machine.md`, `design-console.md`, `design-ui.md`, `design-slot.md`, `doc_index.md`.
- Updated `requirement.md` TOC (removed §15.2.x and §7.3.1 sub-entries).
- Updated `doc_index.md` line counts and descriptions.
- `requirement.md` reduced from ~900 to ~600 lines; now a pure "what to build" spec with pointers to design docs.

### Phase 3b: Merge design-async-io.md into design.md
- Merged `design-async-io.md` (~197 lines) into `design.md` §12.1 (Async Disk I/O Substrate).
- Updated references in `design.md` (§8.3, intro, §12 rule 2), `design-crowtree-storage.md`, `doc_index.md`, `todo-doc.md`.
- Deleted `design/design-async-io.md`.
- No code references to update (none existed).

### Phase 3c: Merge design-crowtree-test.md and move test.md
- Merged `design-crowtree-test.md` into `test.md` as "crowtree C++ Test Layers" section.
- Moved `test.md` → `design/design-test.md`, updated all relative paths and cross-references.
- Mentioned `design-test.md` in `design.md` intro.

### Task 1: Review design.md (master) — done
- Fixed §8.2: opportunistic gap repair (not background task).
- Fixed §5.1: fail-fast Busy backpressure (not bounded queue).
- Merged async I/O substrate §12.1.

### Task 2: Review consensus sub-designs — done
- Fixed `design-rpc.md`: PeerStream→LearnerStream, proto field numbers, SnapshotService, field freeze ranges.
- Fixed `design-leader-election.md`: 10 code-accuracy fixes.
- Fixed `design-state-machine.md`: removed non-existent ordered file engine.
- Fixed `design-slot.md` (merged): backpressure, safe-slot, gap repair, tunables.

### Task 3: Review storage sub-designs — done
- Fixed `design-wal.md`: segment rotation (only size-based implemented), watchdog (caps coalesce, not independent timer), GC disk-pressure (not implemented), retention window (not implemented), fail-out procedure (not implemented), tunable table updated to match `WalConfig` struct.
- Fixed 11 stale `design-crowtree-snapshot-gc.md` code comment references → `design-crowtree-storage.md` with correct section numbers (§6/§7/§9/§10).

### Task 4: Review console/UI sub-designs — done
- Fixed `design-kv-server.md`: file structure (management.rs→mgmt_api.rs, state.rs→store_registry.rs, +startup.rs, +lib.rs), port parser (inclusive not half-open), AppState→KvStoreRegistry with full fields, startup sequence (management API first, then stores), shutdown sequence (graceful_shutdown cascade), missing endpoints (step-down, join, openapi.json).
- Fixed `design-console.md`: SshCreds enum→flat NodeEntry fields, config path (log/→~/.crowkv/), process lifecycle (SSH vs local-fork, no scp/config template), server/start→server/restart, deploy returns 409 not idempotent, CLI hierarchy restart, CLI output (JSON not comfy-table), ProcState includes Unknown.
- Fixed `design-ui.md`: deploy/start/stop→deploy/restart/stop in functional surface.

### Task 5: Review design-test.md — done
- Fixed stale `peer_stream.rs` → `learner_stream.rs` in test binary map.
- Added `file_restore` module to WAL Subsystem Layer.
- Added `snapshot_join` and `membership_epoch_fence` to Group Layer coverage.
- Added snapshot join e2e to Deployment Layer coverage.

### Task 6: Final consistency pass — done
- Fixed stale `PeerStream` / `peer_stream.rs` in `plan-test.md`.
- Fixed stale `peer_stream_window_frames` → `learner_stream_window_frames` in `design-rpc.md`.
- Verified no stale references to deleted docs (`design-crowtree-snapshot-gc.md`, `design-parallel-slots.md`, `design-async-io.md`, `design-crowtree-test.md`) in any active doc or code file.
- Verified all `design-*.md` cross-references point to existing files.

## Remaining Tasks

### Medium priority

- [x] 1. **Review and update `design.md` (master)** — done. See Completed §Task 1.

- [x] 2. **Review consensus-related sub-designs against code** — done. See Completed §Task 2.

- [x] 3. **Review storage-related sub-designs against code** — done. See Completed §Task 3.

- [x] 4. **Review console/UI sub-designs against code** — done. See Completed §Task 4.

- [x] 5. **Review and update `design-test.md`** — done. See Completed §Task 5.

### Low priority

- [x] 6. **Final consistency pass** — done. See Completed §Task 6.
