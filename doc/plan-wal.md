# CrowKV — Plan: WAL Persistence & Crash Recovery (remaining work)

> Consolidated remaining-work plan. Supersedes the old `plan-wal.md` (W1–W14
> milestone plan, all DONE) and `plan-wal-refract.md` (pipeline refactor, first
> slice DONE). Both originals were deleted. (Filename note: the request said
> `plan-wsl.md`; interpreted as a typo for **WAL** = write-ahead log. Rename if
> you actually meant something else.)

Depends on: [`design/design-wal.md`](design/design-wal.md),
[`design/design-async-io.md`](design/design-async-io.md),
[`design/design-console.md`](design/design-console.md) (node workspace layout).

---

## 0. What is already DONE (do not redo)

- **W1–W14** (record codec, segment, index, fsync worker, acceptor ack-contract
  hook, multi-disk `WalEngine`, replay engine producing `ReplayResult`, GC).
  Replay is validated at the `ReplayResult` level only (see gaps below).
- **Pipeline refactor first slice:** `Segment`→`WalSegment`, `WalConfig` moved to
  `common/config.rs`, `DiskState`→`WalPipeline`, `pipeline_backend.rs` backend
  model (`File`/`MemBlock`/`Block`).
- **Block-device alignment (device level):** `BlockDevice` now has two write
  implementations selected by `WalBlockAlignment` — unaligned (RAM/SCM/PMEM,
  `BlockDevice::new`) and 4 KiB-aligned read-modify-write (SSD/NVMe,
  `BlockDevice::ssd_4k`), with write-amplification stats
  (`physical_bytes_written`, `rmw_count`). Planning logic lives on
  `WalBlockAlignment::plan_write`. Tests in
  `crowkv/tests/wal/block_backend_tests.rs`.

## 1. Known gaps (root of the remaining work)

- `PxLocalReplica::set_wal` is now wired for live groups created through
  `crowkv-server` bootstrap and management API paths; replay + restore are also
  wired there. Remaining validation work is crash/restart coverage (A3/A5), not
  basic attachment.
- WAL data now has a managed-node home under
  `runtime-data/N-<node_id>/data/wal/`, while direct bare runs still default to
  relative `wal/`.
- **Aligned-device full replay (✅ RESOLVED, see B1/B2/B3 DONE):** a sealed
  segment on a 4 KiB device is padded to the block boundary. `SegmentReader` is
  now padding-tolerant — `next_record` stops cleanly on a `FOOTER_MAGIC` or zero
  `frame_len` marker, and `read_footer` scans the file tail to locate the footer
  past trailing zero padding. No format/version bump was needed.

ai-todo: the 4K is configable. During test, we could use a file to simulate the block device require aligned. We need UT to cover this case. 
We can test mem backend for performance, test with file and file simulate block device for functional.

---

## 2. Target outcome

A killed `crowkv-server` process, when redeployed/restarted against the same
`runtime-data/N-<node_id>/data/` workspace, **replays its WAL and restores its
consensus + KV state with no data loss**, then rejoins/re-elects normally.

ai-todo: kill is a case, normal shutdown and reload is also a common case.

---

## A. Crash recovery subsystem (chosen: full)

Do these in order; each step lands with its tests.

**Current status**

- **DONE:** A1 WAL data placement + path plumbing
- **DONE:** A2 restore-from-replay core path
- **DONE:** A4 live startup wiring (`create_group_with_wal`, replay on startup,
  WAL attached to live groups, next-slot and next-segment-id resume)
- **NEXT:** A3 single-node crash/restart no-data-loss test over the real file
  backend and shared workspace

### A1 — WAL data placement + log/data path audit ✅ DONE
- Add a WAL root concept tied to the node workspace:
  `runtime-data/N-<node_id>/data/wal/` (per-group subdir `group<N>/seg-*.log`,
  matching `replay_group`'s expectation). File naming under that dir per the
  refactor note: `…/data/wal/group<N>/seg-NNNNNNN.log`.
- Thread a configurable WAL root from CLI/config down to `WalConfig.wal_disks`
  (default still relative for bare runs; web-managed nodes get the workspace
  path).
- **Audit all log/data path usage** and align to the workspace hierarchy where
  required:
  - `crowkv-server/src/main.rs` logging root `"log"` (relative cwd; under
    web-managed nodes resolves to `runtime-data/N-<id>/log/` — confirm/keep).
  - `crowkv-console/web/src/state.rs::prepare_node_workspace` (creates
    `bin/`,`log/`,`data/`) — WAL should live under `data/`.
  - `crowkv-console/shared/src/lifecycle.rs` stdout/stderr log placement.
  - Document the final hierarchy in `design-wal.md` / `design-console.md`.
- Tests: workspace creates `data/wal/`; `WalConfig` resolves to the workspace
  path for a managed node.

### A2 — Restore-from-replay (rebuild a live replica) ✅ DONE
- Add a restore path (e.g. `PxLocalReplica::restore_from_replay(id, role,
  &ReplayResult)` or a free fn) that, from a `ReplayResult`, rebuilds:
  - `PxAcceptor` promised/accepted per slot (highest-ballot wins — already the
    replay dedup rule),
  - `PxLearner` chosen entries + dedup cache,
  - `current_term` and `voted_for` seeded into `ElectionPersistentState`.
- Use existing acceptor/learner APIs; do **not** change Paxos semantics.
- Tests: write a known accept/vote/dedup sequence → `ReplayResult` → restore →
  assert `promised_at`/`accepted_at`/term/`voted_for`/dedup match.

### A3 — Single-node crash/restart no-data-loss (real fsync) ← NEXT
- Integration test under `crowkv/tests/cluster/` using a **tempfile** dir +
  fallback `File` backend: attach WAL to a replica, accept entries (durable via
  ack contract), drop everything (crash), `replay_group` + A2 restore into a
  fresh replica, assert all accepted values + term + `voted_for` survive.

### A4 — Attach WAL to the live store/group + startup replay ✅ DONE
- Wire `WalEngine::create` + `replay_group` + A2 restore into
  `PxKvStore`/`PxGroup` construction (and `crowkv-server` startup), gated so
  no-WAL mode still works for existing in-memory tests.
- Tests: a store created over a populated WAL dir comes up with prior state.

### A5 — G2: multi-node kill/restart/re-elect/no-data-loss
- End-to-end test (`crowkv/tests/cluster/g2_crash_restart_no_data_loss_test.rs`)
  reusing the testkit harness with per-node tempfile WAL dirs: commit writes,
  `kill` the leader, restart it (startup replay via A4), let the cluster
  re-elect, verify committed data is intact and readable.
- This is the `plan.md` §3 **G2** freeze gate.

## B. Aligned block backend — full replay integration

**Current status:** B1, B2, B3 all ✅ DONE (aligned 4 KiB device survives the full append→rotate→seal→replay cycle; tests in `crowkv/tests/wal/segment_tests.rs` and `manager_tests.rs`).

### B1 — Segment padding tolerance ✅ DONE
- Chose the simpler padding-tolerant `SegmentReader` over a header version
  bump: `next_record` treats `FOOTER_MAGIC`/zero `frame_len` as clean end,
  `read_footer` scans the tail past zero padding. Documented in `segment.rs`.
- Record logical content length so a reader can find records/footer past device
  padding: write logical length into the segment header at `seal()` (bump
  `SEG_VERSION`, keep v1 readable) **or** make `SegmentReader` padding-tolerant
  (treat a zero `frame_len` as clean end-of-records). Pick the simpler correct
  option; document in `segment.rs`.

### B2 — Aligned `WalEngine` append + replay test ✅ DONE
- `WalEngine` integration test over `BlockDevice::ssd_4k()`: append across
  rotation, seal, `replay_group`, assert every record recovered and
  `rmw_count`/`physical_bytes_written` reflect the aligned path.

### B3 — Plumb alignment through config ✅ DONE
- Add `WalConfig` alignment selection (default `Unaligned`); `WalEngine` builds
  the per-pipeline `WalPipelineBackend` from it; remove the `#[allow(dead_code)]`
  on `WalPipeline.backend` by actually consulting it.

---

## 3. Conventions & boundaries

- Follow `/coding`: integration tests under `tests/`, structured `tracing`
  fields (`store_id`/`group_id`/`slot`/`ballot`), no inline `#[cfg(test)] mod`.
- SimDisk/`BlockDevice` unit tests: `#[tokio::test(flavor = "current_thread",
  start_paused = true)]`. Crash/restart tests: real `File` backend over
  `tempfile`.
- **No consensus-semantics changes** — restore only rebuilds existing state.
- **No `WALRecord` format reorder** — versioned/append-only (frozen at W2).

## 4. Exit criteria

- Managed-node WAL lives under `runtime-data/N-<node_id>/data/wal/` and all log
  paths follow the workspace hierarchy.
- Startup replay restores acceptor/learner/term/`voted_for`/dedup (A2/A4).
- G2 green: kill leader → restart → re-elect → no data loss (A5).
- Aligned 4 KiB device survives the full append→seal→replay cycle (B1/B2).

Delete this file once all of §4 is green.
