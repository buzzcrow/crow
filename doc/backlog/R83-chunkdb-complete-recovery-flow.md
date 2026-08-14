<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R83: chunkdb — Complete Recovery Flow (Real Data Recovery + Speed Control)

**Problem**:

- **Current behavior + impact** — diskdb's recovery story is
  disk-layer only and stops at the hand-off boundary. diskdb has zone
  bitmap reconstruction (zone-management §6, strategies 1/2/3) and the
  per-disk failure recovery scan (R76, `RecoveryScanTask` in
  `app/crow-diskdb/src/recovery/disk_recovery.rs`). The R76 scan does
  the right *discovery* work — it iterates a `Bad` disk's zones, lists
  live `BusyBlockValue`s, and collects each impacted block's
  `owner_chunk` (`ChunkId`, 192-bit, §3.4) — but the *repair* step is a
  placeholder: `RecoveryAction` has exactly one variant `LogOnly`, and
  `recover_zone_blocks` logs the impacted blocks + owner chunks and
  does no data repair or relocation (disk_recovery.rs:378). The
  doc-comment states the intent: "Future versions add
  `Relocate`/`RebuildFromEc` when the `diskio` service exists"
  (disk_recovery.rs:37). There is **no chunkdb yet** — chunks (logical
  blocks composed of mirror or EC strips, each strip a set of diskdb
  `Segment`s) are only a reserved proto surface
  (`chunkdb_type.proto`, `chunkdb_service.proto`). So when a disk goes
  `Bad`, its impacted blocks are identified and handed to a
  "future recovery/relocation path" (§8) that does not exist: the owner
  is notified nowhere, no surviving replica/parity is read, no rebuilt
  data is written to new segments, and the chunk's strips are never
  updated. Full data recovery is missing. Recovery speed is also
  uncontrolled — once real rebuild I/O ships, an unthrottled rebuild
  would starve foreground allocate/free/read traffic; the throttle
  belongs at the chunkdb layer (the component that owns the
  chunk→strip→segment mapping and drives rebuild I/O), not at diskdb
  (which has no data I/O envelope, §2).
- **Design pointers** —
  [`doc/design/diskdb/design-crow-diskdb.md`](../design/diskdb/design-crow-diskdb.md)
  §2 (Non-Goals — no data I/O; "a future diskio-like component does
  data I/O"), §3.4 (records are the source of truth;
  `BusyBlockValue` carries `owner_chunk` — the recovery hand-off
  handle), §8 (disk failure detection + bad-disk handling —
  "relocation/rebuild is a follow-up requirement"; the impacted list
  is "handed to a future recovery/relocation path: the data-IO layer
  rebuilds from EC/mirror, or the owner is notified to re-allocate
  elsewhere"),
  [`design-crow-diskdb-zone-management.md`](../design/diskdb/design-crow-diskdb-zone-management.md)
  §6 (crash recovery strategies 1/2/3 — diskdb-layer bitmap
  reconstruction only). Proto surfaces:
  `chunkdb_type.proto` (`Chunk`, `ChunkStrip`, `MirrorStrip`,
  `EcStrip`, `StripType`, `ECState`, `ChunkState`),
  `chunkdb_service.proto` (`ChunkdbService` — `AllocateChunk` /
  `AppendChunk` / `SealChunk` / `DeleteChunk` / `UpdateChunkStrip` /
  `ListChunks`), `diskio_service.proto` (`DiskWrite` / `DiskRead`).
  aioss analog: aioss disk-failure recovery rebuilds lost blocks from
  mirror/EC at the chunk layer (the chunk manager drives rebuild I/O
  and paces it); CROW's chunkdb is the analogous owner.
- **Use scenarios** —
  - **Mirror replica rebuild after disk failure** — a disk goes `Bad`;
    diskdb's R76 scan lists the impacted busy blocks + their
    `owner_chunk`. chunkdb (the owner) reads each impacted chunk's
    strip layout, finds the surviving mirror replica(s) on healthy
    disks, reads the surviving data via `diskio` `DiskRead`, allocates
    a new segment on a healthy disk via diskdb `AllocateBlocks`, writes
    the rebuilt replica via `diskio` `DiskWrite`, and updates the
    chunk's strip (`UpdateChunkStrip`) to reference the new `Segment`.
    The old segment on the `Bad` disk is freed (`FreeBlocks`) once the
    disk is replaced/removed. The chunk is whole again.
  - **EC data-block reconstruction** — an EC strip (`data_num` + `code_num`)
    loses one data block when its disk goes `Bad`. chunkdb reads the
    surviving data + parity blocks via `diskio`, reconstructs the lost
    data block, writes it to a newly-allocated segment, and updates
    the strip. Parity is recomputed if a parity block was lost.
  - **Recovery speed control during foreground load** — a cluster is
    serving foreground writes while a disk is `Bad`. The operator sets
    a recovery throttle (bandwidth / IOps / concurrent rebuilds).
    chunkdb paces the rebuild reads + writes so foreground
    allocate/free/read latency is unaffected; recovery progress +
    throttle state are visible to the operator.
  - **Restart mid-recovery** — chunkdb crashes or restarts while a
    rebuild is in progress. On restart it reads persisted per-chunk /
    per-strip recovery progress from KV and resumes the rebuild from
    the last completed strip, without re-rebuilding completed strips
    or losing the in-flight one.
  - **Unrecoverable data loss** — a mirror strip loses *all* replicas,
    or an EC strip loses more than `code_num` blocks. chunkdb detects
    the loss is unrecoverable, reports it (metric + log), and does not
    attempt a rebuild that would silently corrupt data.

**Solution**:

- **No clear solution yet — deferred to design.** chunkdb does not
  exist yet — the chunkdb server component (`ChunkdbService`
  implementation) is itself unbuilt, and the recovery flow's shape
  depends on chunkdb's architecture (chunk→strip→segment storage,
  ownership model, KV schema) plus the `diskio` service (also unbuilt).
  The high-level shape is known from R76's placeholder + the reserved
  proto surfaces, but the detailed design (rebuild orchestration,
  hand-off mechanism, throttle mechanism, progress schema) needs a
  design draft once chunkdb's core is defined.

- **One-line summary**: chunkdb owns real data recovery — rebuild lost
  mirror replicas / EC data+parity from surviving strips via `diskio`,
  pace rebuild I/O at the chunkdb layer so foreground traffic is not
  starved, and persist per-chunk/per-strip progress for restart-resume.

- **Numbered work items**:
  1. **chunkdb server component (prerequisite)** —
     `app/crow-chunkdb` (new crate, not yet existing) implementing
     `ChunkdbService` (`AllocateChunk` / `AppendChunk` / `SealChunk` /
     `DeleteChunk` / `UpdateChunkStrip` / `ListChunks`). This is a
     prerequisite for R83, not part of it — it should be filed as its
     own backlog item. R83 assumes chunkdb exists and owns the
     chunk→strip→segment mapping persisted to CROW KV.
  2. **Recovery orchestration** (`app/crow-chunkdb/src/recovery/`) —
     consume diskdb's impacted-blocks + `owner_chunk` hand-off (R76
     `RecoveryScanTask` output); for each impacted chunk, read its
     strip layout, classify each impacted strip (`MirrorStrip` vs
     `EcStrip`), and build a rebuild plan (which surviving
     replicas/blocks to read, how to reconstruct, where to write the
     new segment). Replaces R76's `RecoveryAction::LogOnly` with
     `Relocate` / `RebuildFromEc` variants.
  3. **`diskio` integration** — read surviving blocks via
     `DiskioService::DiskRead` and write rebuilt blocks via
     `DiskioService::DiskWrite` to newly-allocated segments (diskdb
     `AllocateBlocks` on healthy disks, with `exclude_disks` anti-
     affinity against the `Bad` disk). Prerequisite: the `diskio`
     server component (also unbuilt; should be its own backlog item).
  4. **Strip update + old-segment free** — after a rebuilt block is
     durably written to its new segment, `UpdateChunkStrip` the chunk's
     strip to reference the new `Segment`, then `FreeBlocks` the old
     segment on the `Bad` disk (after the disk is replaced/removed, or
     once the strip no longer references it).
  5. **Recovery speed control** (`app/crow-chunkdb/src/recovery/throttle.rs`)
     — pace rebuild I/O at the chunkdb layer: configurable
     `recovery.max_bandwidth_bytes`, `recovery.max_iops`,
     `recovery.max_concurrent_rebuilds` (live-reloadable). The
     orchestrator gates each rebuild read/write through the throttle so
     foreground allocate/free/read traffic is not starved. Emit
     recovery-progress + throttle metrics (`recovery.rebuilt_blocks`,
     `recovery.pending_chunks`, `recovery.throttle_utilization`).
  6. **Recovery progress persistence** — per-chunk / per-strip
     recovery progress written to CROW KV (schema follows R76's
     `RecoveryScanProgressValue` precedent, keyed by `ChunkId` /
     strip index). On chunkdb restart, resume from the last completed
     strip; no double-rebuild of completed strips.

- **Flow diagram**:

```
  disk goes Bad
       │
       ▼
  diskdb RecoveryScanTask (R76)
       │  lists impacted BusyBlockValue + owner_chunk per zone
       ▼
  chunkdb recovery orchestrator (item 2)
       │  for each impacted chunk:
       │    read strip layout → classify Mirror / EC
       │    build rebuild plan (surviving blocks + reconstruct fn)
       ▼
  ┌──────────────────────────────────────────┐
  │ throttle gate (item 5)                   │
  │  max_bandwidth / max_iops / max_concurrent │
  └──────────────┬───────────────────────────┘
                 ▼
  diskio DiskRead (surviving blocks) ──► reconstruct (mirror copy / EC)
                 │
                 ▼
  diskdb AllocateBlocks (healthy disk, exclude Bad) ──► diskio DiskWrite (new segment)
                 │
                 ▼
  UpdateChunkStrip (new Segment) ──► FreeBlocks (old segment on Bad disk)
                 │
                 ▼
  persist per-chunk/strip progress (item 6) ──► next strip / next chunk
```

- **Edge cases at a glance**:
  - Mirror strip loses all replicas → unrecoverable; report data loss,
    do not rebuild.
  - EC strip loses more than `code_num` blocks → unrecoverable; report
    data loss, do not rebuild.
  - Rebuild target disk goes `Bad` mid-rebuild → re-pick a healthy
    target on the next cycle; the in-flight write to the now-`Bad`
    target is abandoned.
  - chunkdb restart mid-rebuild → resume from persisted per-strip
    progress; no double-rebuild, no lost in-flight strip (the strip is
    re-attempted).
  - Recovery throttle set very low → recovery takes long; operator
    sees progress via metrics; no foreground impact.
  - `owner_chunk` missing on a busy block (legacy / corrupt record) →
    skip the block, report it (cannot rebuild without the owner).
  - EC parity not yet computed (`EC_STATE_NO_PARITY`) on a strip that
    loses a data block → cannot reconstruct from parity; rebuild from
    mirror fallback or report unrecoverable (design decision).
  - Disk moved (R81 Part 2) with segments still on the original paxos
    group → recovery must read the moved disk's records via the
    per-disk bind, not the disk-group's default bind.

**Dependencies**:

- **chunkdb server component (prerequisite, unlanded)** — R83 assumes
  the chunkdb server exists (`ChunkdbService` impl, chunk→strip→segment
  KV schema). This must be filed as its own backlog item and landed
  first; without it there is no chunk owner to drive recovery.
- **`diskio` service (prerequisite, unlanded)** — real data recovery
  needs `DiskRead` (surviving blocks) + `DiskWrite` (rebuilt blocks).
  The `diskio` server is a future component (§2, `diskio_service.proto`
  reserved). Must be filed as its own backlog item. Fallback without
  diskio: R76's `LogOnly` placeholder stays (current state).
- **R76** — `RecoveryScanTask`, impacted-blocks + `owner_chunk`
  collection, `RecoveryAction` placeholder (`LogOnly` → `Relocate` /
  `RebuildFromEc`), `RecoveryScanProgressValue` schema as the progress
  precedent.
- **R72** — diskdb `AllocateBlocks` / `FreeBlocks`, `Segment` record
  model, `BusyBlockValue` / `owner_chunk`.
- **R81 Part 2** — disk move with per-disk bind; recovery must account
  for moved disks whose records live on the original paxos group
  (per-disk bind routing, not the disk-group's default bind).
- Nothing depends on R83 yet.

**Acceptance**:

- **Mirror replica rebuild**: a disk goes `Bad`; a mirror strip with 2
  replicas (one on the `Bad` disk, one on a healthy disk) → chunkdb
  reads the surviving replica via `diskio`, allocates a new segment on
  a healthy disk (excluding the `Bad` disk), writes the rebuilt
  replica, `UpdateChunkStrip`s to the new `Segment`, and frees the old
  segment → the chunk reads back correctly from the new replica. E2E
  test (pending chunkdb + diskio).
- **EC data-block reconstruction**: an EC strip (`data_num=4`,
  `code_num=2`) loses 1 data block → chunkdb reconstructs the lost
  block from surviving data + parity, writes it to a new segment, and
  updates the strip → the chunk reads back correctly. E2E test.
- **EC parity-block reconstruction**: an EC strip loses 1 parity block
  → chunkdb recomputes parity from the surviving data blocks, writes
  it to a new segment, updates the strip → `EC_STATE_PARITY` restored.
  E2E test.
- **Unrecoverable — mirror all replicas lost**: a mirror strip with 2
  replicas both on `Bad` disks → chunkdb reports data loss (metric +
  log), does not attempt a rebuild, no silent corruption. E2E test.
- **Unrecoverable — EC loses more than `code_num`**: an EC strip
  (`code_num=2`) loses 3 blocks → chunkdb reports data loss, does not
  rebuild. E2E test.
- **Recovery speed control**: `recovery.max_iops` set to N → rebuild
  read/write rate stays ≤ N; foreground allocate/free/read latency
  stays within the pre-recovery baseline (no starvation). E2E test.
- **Restart resume**: chunkdb restarts mid-rebuild → on restart it
  resumes from the persisted per-strip progress; completed strips are
  not re-rebuilt, the in-flight strip is re-attempted and completes.
  E2E test.
- **`owner_chunk` missing**: a busy block with no `owner_chunk` →
  chunkdb skips it and reports it (metric); no crash, no rebuild
  attempt. E2E test.
- **Moved-disk recovery**: a disk moved via R81 Part 2 (records on the
  original paxos group via per-disk bind) goes `Bad` → chunkdb reads
  the impacted blocks via the per-disk bind and rebuilds correctly.
  E2E test.
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- `pixi run test-chunkdb` (relevant integration tests pass, pending
  chunkdb crate) + `pixi run test-diskdb` (R76 hand-off integration).

**Open Questions**:

- **chunkdb + diskio as separate backlog items?** R83 depends on both
  the chunkdb server and the diskio server existing. These are
  prerequisites that should be filed as their own backlog items
  (chunkdb core, diskio core) before R83 can be designed in detail.
  Cannot be resolved autonomously — it is a scope/sequencing decision
  that needs a human to confirm the prerequisite items should be
  filed and in what order.
- **Recovery hand-off mechanism** — how does diskdb's
  `RecoveryScanTask` hand the impacted-blocks + `owner_chunk` list to
  chunkdb? Options: (a) push — diskdb notifies chunkdb (the owner) via
  an owner-notification mechanism (does not exist yet); (b) pull —
  chunkdb subscribes to disk `Bad` transitions (watch/notify, R78) and
  pulls the impacted list from diskdb. Trade-off: (a) couples diskdb
  to chunkdb; (b) keeps diskdb unaware of chunkdb but adds latency.
  Design decision.
- **Recovery speed control granularity** — per-chunk, per-disk, or
  per-instance throttle? Per-instance is simplest (one global budget);
  per-disk allows prioritizing hot disks; per-chunk is the finest.
  Recommendation: per-instance for v1, revisit if per-disk priority is
  needed. Design decision.
- **EC strip with no parity yet (`EC_STATE_NO_PARITY`) loses a data
  block** — cannot reconstruct from parity. Rebuild from a mirror
  fallback (if the chunk has one), or report unrecoverable? Design
  decision.
