<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R84: chunkdb — Post-Disk-Move Chunk Placement Scanner

**Problem**:

- **Current behavior + impact** — R81 Part 2 adds disk move with a
  stable `DiskId` (UUID): a physical disk is relocated from
  `(old_rack, old_node, old_dg)` to `(new_rack, new_node, new_dg)`,
  its zone/busy/free records are copied from the old disk-group's bind
  to the new disk-group's bind during Maintenance (no concurrent
  writes), and the disk becomes available at the new placement without
  a full recovery scan. The move is placement-only — the disk's block
  data is intact on the physical medium, and the records are keyed
  only by `DiskId` (globally unique, §3.9), so copying them between
  paxos groups is a literal key-value copy. What is missing is any
  verification that chunk placement is still consistent after a move.
  Chunks reference the blocks they own via `Segment { disk_id,
  zone_index, unit_offset, unit_count }` (one `Segment` per allocated
  range, embedded in `MirrorStrip` / `EcStrip` inside each
  `ChunkStrip`, `chunkdb_type.proto`). After a disk move, every chunk
  that has a `Segment` on the moved disk must still be able to reach
  that segment: the `disk_id` must resolve to a disk that is `Up` at
  its new placement. There is no scanner that walks
  chunk→strip→segment placement after a move and verifies
  reachability + health. A move that leaves a chunk's segments on a
  disk that is now unreachable (e.g. the move partially failed, the
  disk went `Missing` at the new placement) would silently break chunk
  reads with no detection until a client hits the broken segment. This
  is the chunk-placement analog of diskdb's background scanner (§10) —
  but diskdb's scanner checks disk-layer record/bitmap drift, not
  chunk→segment placement integrity, which is chunkdb's domain.
- **Design pointers** —
  [`doc/backlog/R81-sysdata-id-reuse-safety-and-disk-move.md`](R81-sysdata-id-reuse-safety-and-disk-move.md)
  Part 2 (disk move, record copy during Maintenance, records keyed
  by `DiskId`, no full scan),
  [`doc/design/diskdb/design-crow-diskdb.md`](../design/diskdb/design-crow-diskdb.md)
  §3.9 (unit-based sizes; disk-id key routing — record keys carry
  `DiskId`, no `node_id`/`disk_group_id`), §8 (disk status management
  — effective status = `max(node, group, disk)`; `Missing` detection
  by absence from a group-0 sync response), §10 (Background Scanner —
  `ScannerTask` / `BgRunner` / `BackgroundTask` pattern, KV-persisted
  progress, resume after restart, admin RPCs `TriggerScan` /
  `GetScanStatus`). Proto surfaces: `chunkdb_type.proto` (`Chunk`,
  `ChunkStrip`, `MirrorStrip`, `EcStrip`, `Segment` reference),
  `chunkdb_service.proto` (`ListChunks` — paginated chunk scan).
  CROW's disk-move model (R81) is new, so the post-move chunk
  placement scanner is new work shaped on diskdb's `ScannerTask`
  precedent.
- **Use scenarios** —
  - **Post-move placement verification** — an operator moves a disk
    from disk-group A to disk-group B (R81 Part 2). The move completes
    and the disk is `Up` at the new placement. A scanner walks every
    chunk that has a `Segment` on the moved `DiskId`, resolves each
    segment's disk to its current placement (group-0 `DiskValue`),
    and verifies the disk is `Up`. All segments reachable → the move
    is placement-clean;
    the scanner reports 0 inconsistencies.
  - **Detect partially-failed move** — a disk move left the disk
    `Missing` at the new placement (the new disk-group's instance has
    not picked it up via keepalive reconcile). The scanner finds
    chunks whose segments reference the moved `DiskId`, sees the disk
    is not `Up` at the new placement, and reports the unreachable
    segments + their owning chunks so the operator can fix the move
    or trigger recovery (R83).
  - **Detect orphaned segment after disk removal (not move)** — a
    segment references a `DiskId` that no longer exists in group 0
    (the disk was removed, not moved). The scanner reports the
    orphaned segment + owning chunk as a data-loss risk; the chunk
    needs recovery (R83) if it has surviving replicas/parity.
  - **Periodic placement integrity sweep** — independent of any
    specific move, a periodic sweep verifies that every chunk's
    segments resolve to a live, `Up` disk, catching placement drift
    from any cause (move, removal, status change).

**Solution**:

- **No clear solution yet — deferred to design.** chunkdb does not
  exist yet (see R83 prerequisites), so the scanner's host (chunkdb-side
  vs diskdb-side), the chunk iteration mechanism, and the trigger
  model need a design draft once chunkdb's core is defined. The
  high-level shape is known from diskdb's `ScannerTask` precedent (§10)
  + the disk-move model (R81).

- **One-line summary**: a placement-integrity scanner that, after a
  disk move (and periodically), walks chunk→strip→segment placement,
  resolves each segment's `DiskId` to its current group-0 placement,
  and reports any segment that is unreachable or orphaned.

- **Numbered work items**:
  1. **Placement scanner task** (`app/crow-chunkdb/src/scanner/placement.rs`,
     new — or `app/crow-diskdb/src/scanner/` if the design picks
     diskdb as the host) — a background task following the
     `BgRunner` + `BackgroundTask` pattern (R75 / §10 `ScannerTask`).
     Runs on a configurable interval (`scanner.placement_interval_secs`,
     default 600) and is triggerable on-demand after a disk move
     (admin RPC `TriggerPlacementScan`).
  2. **Chunk→strip→segment walk** — iterate chunks via
     `ChunkdbService::ListChunks` (paginated, `ChunkId` order); for
     each chunk, iterate its `ChunkStrip`s and the `Segment`s inside
     each `MirrorStrip` / `EcStrip`. Collect the set of `DiskId`s
     referenced by live (non-deleted) chunks.
  3. **Segment reachability check** — for each referenced `DiskId`,
     resolve its current placement from group 0 (`HardwareClient`
     `DiskValue` lookup). Verify: the disk exists in group 0 (not
     orphaned); the disk's effective status is `Up` (not
     `Missing`/`Bad`/`Offline`). A segment whose disk is
     `Bad`/`Missing` is reported + handed to recovery (R83).
  4. **Placement integrity report + metrics** — emit a
     `PlacementScanSummary` (counts: `reachable_segments`,
     `unreachable_segments`, `orphaned_segments`) + gauges
     (`scanner.placement.unreachable`,
     `scanner.placement.orphaned`). Admin RPCs `TriggerPlacementScan`
     / `GetPlacementScanStatus` mirror diskdb's scanner admin surface.
  5. **Move trigger** — the scanner is triggered on disk-move
     completion via watch/notify (R78, `/hw/disk/` prefix) when a
     `DiskValue` moves path, with periodic sweep as a safety net
     (same pattern as diskdb's watch/notify + polling safety net, §8).
  6. **Progress persistence + resume** — KV-persisted scan progress
     (last scanned `ChunkId`, follows R76's
     `RecoveryScanProgressValue` / R80's `RebalancePlanValue`
     precedent); on restart the scanner resumes from the last
     checkpoint, no full re-walk.

- **Flow diagram**:

```
  disk move completes (R81 Part 2)  ──or──  periodic interval
       │
       ▼
  PlacementScannerTask (item 1)
       │
       ▼
  ListChunks (paginated by ChunkId)  ──► for each chunk:
       │   iterate ChunkStrips → MirrorStrip / EcStrip → Segments
       │   collect referenced DiskId set
       ▼
  for each referenced DiskId (item 3):
       ├─ group-0 DiskValue lookup (HardwareClient)
       │     ├─ disk missing in group 0 → orphaned segment (report)
       │     ├─ effective status != Up  → unreachable (report + hand to R83)
       │     └─ status Up → segment OK
       ▼
  PlacementScanSummary + gauges (item 4)
       │
       ▼
  persist progress (item 6) ──► next ChunkId page / done
```

- **Edge cases at a glance**:
  - Segment on a moved disk, disk `Up` at new placement → reachable;
    scanner reports 0 issues for that segment.
  - Segment on a moved disk, disk `Missing` at new placement (move
    partially failed) → unreachable; report + hand to recovery (R83).
  - Segment on a disk removed from group 0 (not moved) → orphaned;
    report data-loss risk + hand to recovery (R83).
  - Segment on a `Bad` disk → unreachable; hand to R76/R83 recovery
    (the placement scanner does not rebuild, it detects + routes).
  - chunkdb restart mid-scan → resume from persisted
    `last_scanned_chunk_id`; no full re-walk.
  - Chunk deleted mid-scan (`ChunkState::Deleted`) → skip its segments
    (freed); no false orphan.
  - No chunks reference the moved disk → scanner reports 0 segments
    scanned for that disk; no overhead beyond the chunk walk.
  - Disk moved multiple times → only the final placement matters; the
    scanner resolves the current `DiskValue` each run.

**Dependencies**:

- **R81 Part 2** — disk move with record copy during Maintenance;
  the scanner verifies placement integrity after a move. Without R81
  there is no disk move to scan.
- **chunkdb server component (prerequisite, unlanded)** — the scanner
  walks chunks via `ListChunks`; chunkdb must exist (same prerequisite
  as R83). Must be filed as its own backlog item.
- **R75** — `BgRunner` + `BackgroundTask` pattern; the placement
  scanner follows `ScannerTask`'s structure (interval trigger,
  on-demand `TriggerScan`, `ScanState`, in-progress flag).
- **R78** — watch/notify on `/hw/disk/` for the move trigger (optional;
  polling fallback if watch/notify is not landed).
- **R83** — recovery flow; the placement scanner detects unreachable
  / orphaned segments and hands them to R83 for rebuild. R83 must
  exist for the hand-off to do real work; without R83 the scanner
  reports only (same placeholder shape as R76's `LogOnly`).
- **R72** — `Segment` record model; the scanner reads `Segment`s from
  chunk strips.
- Nothing depends on R84 yet.

**Acceptance**:

- **Post-move reachability**: move a disk via R81 Part 2 to a new
  disk-group, disk `Up` at the new placement → the scanner walks all
  chunks with segments on the moved `DiskId`, verifies each segment
  is reachable, reports 0 unreachable / 0 orphaned. E2E test (pending
  chunkdb).
- **Partially-failed move detection**: move a disk but leave it
  `Missing` at the new placement → the scanner reports the chunks +
  segments referencing the moved `DiskId` as unreachable. E2E test.
- **Orphaned segment after removal**: a chunk's segment references a
  `DiskId` removed from group 0 (not moved) → the scanner reports the
  segment as orphaned with its owning chunk. E2E test.
- **`Bad`-disk segment routing**: a chunk's segment references a disk
  that is `Bad` → the scanner reports the segment as unreachable and
  hands it to recovery (R83); the scanner does not rebuild. E2E test
  (pending R83).
- **Deleted chunk skipped**: a chunk transitions to
  `ChunkState::Deleted` mid-scan → its segments are not reported as
  orphaned (they are freed). E2E test.
- **Restart resume**: chunkdb restarts mid-scan → on restart the
  scanner resumes from the persisted `last_scanned_chunk_id`; no full
  re-walk, no missed chunks. E2E test.
- **On-demand trigger**: `TriggerPlacementScan` after a move → the
  scan runs immediately (not waiting for the interval) and returns the
  current summary + `scan_in_progress` flag. E2E test.
- **No chunks reference the moved disk**: a disk is moved but no chunk
  has a segment on it → the scanner completes with 0 segments scanned
  for that disk; no false positives. E2E test.
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- `pixi run test-chunkdb` (relevant integration tests pass, pending
  chunkdb crate).

**Open Questions**:

- **Scanner host: chunkdb-side or diskdb-side?** The scanner walks
  chunk→strip→segment placement (chunkdb's data model) but checks disk
  reachability (diskdb's / group-0's domain). Options:
  (a) chunkdb-side — chunkdb owns the chunk→segment mapping, so it is
  the natural host; it calls `HardwareClient` to resolve disk
  placement. (b) diskdb-side — diskdb has the scanner infrastructure
  (`ScannerTask`) and the disk records, but it does not know which
  chunks reference which disks (that is chunkdb's domain), so it would
  need a reverse lookup chunkdb does not expose. Recommendation:
  chunkdb-side (a) — the chunk→segment walk is the core of the scan
  and only chunkdb has it. Design decision.
- **Trigger model: watch/notify vs periodic-only?** Watch/notify
  (R78) on `/hw/disk/` gives immediate post-move triggering; periodic-
  only is simpler but adds detection latency. Recommendation:
  watch/notify trigger + periodic safety net (same as diskdb's model,
  §8). Depends on R78 being landed. Design decision.
- **Should the placement scanner be bundled with diskdb's existing
  background scanner (§10) or kept separate?** Bundling reuses one
  `ScannerTask` loop + admin surface; keeping it separate respects
  the chunkdb/diskdb component boundary. Recommendation: separate
  (chunkdb-owned), mirroring diskdb's scanner structure. Design
  decision.
