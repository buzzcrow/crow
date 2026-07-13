# CrowKV - State Machine (crowtree) Gap Analysis & Plan

Scope: is `crowtree` actually used by `crowkv` as a `KVEngine`, and is it
actually persisting btree pages to files? Verified against code (not just
design docs) on 2026-07-10.

## 0. tl;dr — the premise needs correction

**`crowtree` is already wired into `crowkv` end-to-end, and file persistence
of btree pages is already implemented and tested.** It is not a stub. See
§1 for the evidence. The real gaps are narrower and listed in §2: mostly
error-path plumbing, observability, and one operational default — not "make
crowtree work," but "make the already-working crowtree path
production-hardened and the default."

---

## 1. What's already wired (verified in code)

| Piece | Evidence |
| --- | --- |
| `CrowtreeEngine` implements `KVEngine` over the C FFI | `crowkv/src/kv/crowtree_engine.rs` |
| CLI selects engine per-store: `--kv-engine {memory,crowtree}`, default `memory` | `crowkv-server/src/cli.rs:54` |
| CLI selects durable backend: `--kv-backend {file,block}` | `crowkv-server/src/cli.rs:62-65`, `crowkv-server/src/store_registry.rs::parse_crowtree_backend` |
| Fresh/recovered group opens a real on-disk file at `{data_root}/store{id}/group{id}.ctdb` | `crowkv-server/src/startup.rs::open_crowtree_engine` / `store_crowtree_path` |
| Restart replays WAL into the *same* file and skips the already-durable prefix via `resume_from_slot`/`seed_resume_frontier` | `crowkv/src/cluster/local_replica.rs::restore_from_replay_with_engine` (destaled in `doc/design/design-state-machine.md §2.1`, `doc/design/design-wal.md §6.2`) |
| End-to-end restart-persistence test, parameterized over `File`/`Block` backend | `crowkv-server/tests/startup_test.rs::crowtree_engine_persists_across_restart` |
| Periodic durable snapshot + GC watermark push + GC sweep, gated on group-wide `safe_slot`/`snapshot_slot` | `crowkv/src/cluster/group_maintenance.rs::run_pass` (calls `engine.persist_snapshot()` / `set_gc_watermark()` / `collect_garbage()`) |
| New-member bootstrap over the wire via engine snapshot, not just WAL replay | `crowkv/src/rpc/snapshot_service.rs` (serves `KVEngine::snapshot_export`) + `crowkv/src/cluster/group.rs` (`snapshot_import` on join) + `crowkv-server/src/mgmt_api.rs` add-replica flow |
| Real file-backed durability at the C++ layer: commit-anchor + segment-image persistence, two-generation crash safety, CRC-validated pages | `crowtree/src/persist.cpp`, `crowtree/include/crowtree/page_store.h::FilePageStore`/`BlockPageStore` |
| Crash/recovery test matrix actually implemented (not just designed) | `crowtree/tests/integration/crash_recovery_test.cpp`, `persist_test.cpp`, `snapshot_export_test.cpp`, `stress_test.cpp`, `parity_test.cpp` (~24 integration test files, ~24 unit test files) |
| Cross-engine parity oracle (`InMemKV` vs `CrowtreeEngine`) | `crowtree/tests/integration/parity_test.cpp`; design in `doc/design/design-crowtree-test.md §6` |

The `BlockPageStore` (`O_DIRECT` raw block device — SSD/SCM/mem media
drivers) also exists per `doc/design/design-crowtree-storage.md §2`; RDMA is
explicitly deferred (no remote-block medium driver implemented), but that is
a distinct, documented, and *already-flagged* limitation, not something
newly discovered here.

---

## 2. Real gaps found (verified, with evidence)

### G1. I/O failures during `apply` are silently swallowed
`CrowtreeEngine::apply` (`crowkv/src/kv/crowtree_engine.rs:64-92`) calls
`Crowtree::apply_batch` and discards its `Status` (`let _ = ...`). A durable
write failure (`ENOSPC`/`EIO`) during a Paxos-committed apply is invisible
to the Paxos/WAL layer — the entry is still treated as durably applied. The
only signal is `Crowtree::io_failed()`, an out-of-band latched flag
(`crowtree/include/crowtree/crowtree.h:511`, intended per its own doc
comment for "a caller [to] poll this after reads ... and fail the node out
of the group").

**Nothing in `crowkv` ever calls `io_failed()`/`clear_io_error()`.** Grep
across `crowkv/src` and `crowkv-server/src` finds zero call sites outside
the one doc comment that names the gap
(`crowkv/src/kv/crowtree_engine.rs:87`). The C++ side implements and tests
this flag thoroughly (`crowtree/tests/integration/crash_recovery_test.cpp`);
the Rust side never reads it.

### G2. `KVEngine` trait has no health/fault-check surface
Because of G1, wiring `io_failed()` into `crowkv` isn't a one-line fix:
`KVEngine` (`crowkv/src/kv/kv_engine.rs`) has no `is_healthy`/`io_failed`
method at all — only `CrowtreeEngine::handle() -> Arc<Crowtree>` gives
engine-specific escape-hatch access (already used for
`flush`/`snapshot`/GC). Any caller that wants to check for a media fault
today has to downcast or special-case `KvEngineKind::Crowtree`.

### G3. `CrowtreeEngine::clear()` panics
`crowkv/src/kv/crowtree_engine.rs:153-161` is `unimplemented!()` — crowtree
has no native reset/wipe primitive. Currently unreachable (no production
caller), but the snapshot-import path documented in
`doc/design/design-state-machine.md` (reset-before-import for a *rejoining*
member with stale state) would need this the moment it's wired up for a
non-fresh crowtree-backed replica. `group.rs`'s current `snapshot_import`
call site imports into an already-fresh engine (a brand-new group), so it
never hits this — but a "re-sync a diverged/corrupted replica in place"
recovery path would.

### G4. `scan` bypasses an async path that already exists one layer down
`crowkv/src/kv/crowtree_engine.rs:106-115` (`scan`) calls
`self.inner.handle().scan(...)` — `Crowtree::scan` (`crowtree/ffi/src/
lib.rs:540`), the fully synchronous path. But the C API already has
`ct_scan_async` (`crowtree/include/crowtree/c_api.h:168`,
`crowtree/ffi/src/lib.rs:148`), and `crowtree_ffi::AsyncCrowtree` already has
a genuinely-reactor-backed `async fn scan` built on it
(`crowtree/ffi/src/lib.rs:1031-1035`). What's missing is only a `try_scan`
mirroring `try_get`'s `Ready`/`Pending` split (`AsyncCrowtree::try_get`,
`lib.rs:1011-1021`, returning `GetOutcome`) — no such `try_scan`/
`ScanOutcome` exists yet, so `CrowtreeEngine::scan` has no zero-alloc-fast-
path way to consume the reactor and falls back to the old synchronous call
instead. `apply` has the same shape (always synchronous — `apply_batch` is
MemTable-insert-only today, no page I/O on that path, so this is lower
priority than `scan`).

A `scan` over keys not resident in the buffer pool runs synchronous FFI I/O
directly on a Tokio worker thread today. This is a liveness risk (stalls
the executor under real disk-backed load, not just a missed optimization)
once a tree's working set exceeds the buffer pool.

### G5. Default `--kv-engine` is `memory`, not `crowtree`
`crowkv-server/src/cli.rs:54`. Every `crowkv-server` instance started
without `--kv-engine crowtree` explicitly runs in-memory-only — no
durability across process restart at all, regardless of how mature the
crowtree path is. This is a pure operational/defaults question, not a code
gap, but it means "crowtree is used" is currently true only when an
operator opts in.

### G6. Zero crowtree-specific observability
`crowkv-console` has no reference to crowtree, buffer pool, or page store
at all (grepped `crowkv-console/`: no hits). `Crowtree` already tracks
useful diagnostics internally (`snapshot_pages_written_`,
`snapshot_segments_written_`, buffer-pool occupancy — see
`crowtree/include/crowtree/crowtree.h:970-972` and neighboring counters) but
none of it is exposed through the FFI/`KVEngine` surface, `crowkv-server`'s
metrics, or the console. Operators running `--kv-engine crowtree` today
have no visibility into buffer-pool hit rate, page-store size, GC
effectiveness, or `io_failed` state. `doc/todo_code.md` already tracks a
generic "add metrics module" item; this is the crowtree-specific instance
of it.

### G7. Fallible `apply` is a real trait-shape change, not a patch
Closing G1 properly means `KVEngine::apply` should be able to return an
error (today: `KVFuture<()>`, no `Result`). This ripples through
`PxLearner::apply_entry`/`Learner::learn` and their callers
(`PxLocalReplica::learn_chosen`/`apply_committed_up_to`) — the same kind of
signature-shape decision `doc/design/design-crowkv-async-kvengine.md`
already went through once for the async conversion. Do not patch around
this with a panic or a silent log line; it needs the same deliberate
trait-shape review.

---

## 3. Implementation plan

Ordered by dependency, not just priority — G1/G7 gate G2's usefulness.

### Step 1 — Fallible apply (closes G1, G7)
1. Change `KVEngine::apply` to return `KVFuture<Result<(), EngineError>>`
   (or a dedicated `ApplyResult`), matching the existing `EngineError`
   pattern already used elsewhere in `kv/`.
2. `InMemKV::apply`: always `Ok(())` (no I/O to fail).
3. `CrowtreeEngine::apply`: map `Crowtree::apply_batch`'s `Status` into the
   new `Result` instead of discarding it.
4. `PxLearner::apply_entry` propagates the error. Decide the contract for a
   failed apply at the Paxos layer: at minimum, log at `ERROR` with
   `group_id`/`slot`/`node_id` (per `design.md`'s structured-logging
   convention) and mark the replica unhealthy (feeds Step 2). Do **not**
   treat it as "not applied" for consensus purposes — the value is still
   chosen; this is a local-node durability failure, handled like G2/G3
   below (fail the node out), not a re-proposal.
5. Update the ~4 direct `KVEngine::apply` call sites in tests
   (`crowkv/tests/kv/*`, `crowkv/tests/wal/replay_tests.rs`) for the new
   return type.

### Step 2 — Health/fault surface on `KVEngine` (closes G2)
1. Add `fn is_healthy(&self) -> bool` to `KVEngine` (default `true`).
2. `CrowtreeEngine::is_healthy` returns `!self.inner.handle().io_failed()`.
3. Wire a check into `group_maintenance.rs::run_pass` (same place that
   already calls `persist_snapshot`/`set_gc_watermark` periodically): on
   `!is_healthy()`, log `ERROR` and trigger the existing "fail the node out
   of the group" path already specified for WAL disk failure in
   `doc/design/design-wal.md §8.1` (step-out RPC to the leader) — reuse that
   mechanism rather than inventing a new one.
4. Surface `is_healthy` through `crowkv-server`'s management API (a field on
   the existing store/group status response) as the observability
   foundation for G6.

### Step 3 — Implement `clear()` for real (closes G3)
1. Cheapest correct option: `Crowtree::open` a **fresh** tree at the same
   path (truncate + reinitialize), i.e. `clear()` = close + delete the
   backing file(s) + reopen empty. Needs a `Crowtree`-level "wipe" C API
   entry point (`ct_clear` or equivalent) — check whether truncating
   `PageStore` in place (drop mapping table, reset root to one empty leaf,
   reset commit anchor) is simpler than a full file delete/recreate; either
   is acceptable, prefer whichever needs less new C++ surface.
2. Add a crash-safety test: `clear()` mid-flush doesn't leave a corrupt file
   (or, if implemented as delete+reopen, verify no torn state is possible
   because the old file is fully gone before the new one is created).
3. Remove the `unimplemented!()` panic and its `todo_code.md`-style note
   once landed.

### Step 4 — Async `scan` (closes G4)
`ct_scan_async` and `AsyncCrowtree::scan` already exist and are reactor-backed
(`crowtree/ffi/src/lib.rs:1031-1035`) — this step is wiring, not new async
C API work:
1. Add `AsyncCrowtree::try_scan(prefix, limit) -> ScanOutcome` mirroring
   `try_get`'s `Ready`/`Pending` split (`lib.rs:1011-1021`): poll
   `ct_scan_async`'s future once inline; return `Ready` if it resolves
   immediately (the common resident-tree case), else box the remainder as
   `Pending` exactly like `GetOutcome`.
2. `CrowtreeEngine::scan` switches from `self.inner.handle().scan(...)`
   (`crowtree_engine.rs:106-115`) to `self.inner.try_scan(...)`, matching
   `get`'s existing `KVFuture::Ready`/`KVFuture::Pending` construction.
3. `apply` can stay synchronous for now — it's MemTable-insert-only on the
   fast path (no page I/O), unlike `scan`'s "walk possibly-evicted leaves"
   pattern. Re-evaluate only if a future crowtree change makes `apply`
   itself capable of blocking on I/O beyond the already-async flush path.
4. Regression test mirroring the existing
   `get_constructs_pending_for_genuine_demand_load_miss`
   (`crowkv/tests/kv/crowtree_engine_test.rs`) for `scan`.

### Step 5 — Flip the default, deliberately (closes G5)
1. Do **not** flip `--kv-engine`'s default silently. Gate on Steps 1-3
   landing first (a durability path that swallows errors and panics on
   `clear()` should not become the default).
2. Once Steps 1-3 land, re-run the full crash/recovery + parity suite
   (§1's existing test matrix) as a release gate, then flip
   `crowkv-server/src/cli.rs:54`'s default to `crowtree` and update
   `doc/design/design-kv-server.md` accordingly.
3. Keep `memory` available and documented as the explicit low-durability/
   test/dev choice, not remove it.

### Step 6 — Observability (closes G6)
1. Expose `Crowtree`'s existing internal counters
   (`snapshot_pages_written_`, `snapshot_segments_written_`, buffer-pool
   occupancy/hit-rate) through a new FFI diagnostics call
   (`ct_stats`-style, batched into one struct to avoid many small FFI
   calls) and a corresponding `CrowtreeEngine::stats()` method (engine-
   specific, alongside `handle()`, not on the generic `KVEngine` trait —
   `InMemKV` has no equivalent internals worth exposing the same way).
2. Feed those into whatever metrics module lands for
   `doc/todo_code.md`'s existing generic "add metrics module" item, rather
   than building a crowtree-only metrics path in parallel.
3. `crowkv-console`: add a per-group panel showing engine kind, `is_healthy`
   (Step 2), and the new stats when `kv_engine == Crowtree`. Coordinate with
   `doc/design/design-console.md`'s existing panel structure rather than
   bolting on a new page.

---

## 4. Explicitly not gaps (checked, found fine)

- **New-member bootstrap for a crowtree-backed group** — works today via
  `SnapshotService` + `snapshot_import`, not just WAL replay from slot 0.
  `mgmt_api.rs` already documents the same-engine-kind constraint correctly.
- **Crash safety of the file format itself** — two-generation commit-anchor
  fallback, torn-write/CRC handling, and the FI fault-injection matrix are
  real, implemented, and tested (`crowtree/tests/integration/
  crash_recovery_test.cpp` and neighbors). This is the most mature part of
  the stack, not a gap.
- **WAL/engine restart coordination** — already destaled and verified
  correct in this session; see `doc/design/design-state-machine.md §2.1` and
  `doc/design/design-wal.md §6.2`.
