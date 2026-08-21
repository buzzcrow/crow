<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R80: diskdb — Space Rebalance Across Disks + Disk-Groups

**Problem**: R72's allocator spreads new allocations round-robin over
`DdbDiskGroup::allocating_disks` via a pure even-split cursor
(`pos_v_disk_ctx`, `allocate_block` in disk_group.rs:116). It is
**load-unaware**: every allocatable disk gets an equal share of *new*
allocations regardless of how full it already is. Existing allocations
are never moved. This produces persistent space imbalance whenever the
allocatable set changes membership or load is uneven:

- **New disk added** — the operator adds a disk in group 0
  (`HardwareClient.add_disk`); on the next sync tick diskdb runs the
  disk-add init flow (§8), creates empty zones, and
  `rebuild_allocating_disks` (disk_group.rs:74) re-includes it. The new
  disk enters `allocating_disks` at 0% used while its peers may be
  near-full. Round-robin gives it an equal *new* share, but the old
  disks keep their existing load. With low churn the new disk stays
  under-utilized indefinitely — capacity is stranded on the empty disk
  while peers run hot.
- **Recovered disk (`HwStatus::Bad → Up`, R76)** — a disk that was
  physically replaced or whose data was relocated by the owner comes
  back `Up` empty. Same shape as a new disk: round-robin cannot drain
  load from the still-full peers onto the empty recovered disk. (In v1
  R76 placeholder recovery = `LogOnly`, data is intact and the disk
  comes back with its old load — but once real relocation ships, or on
  physical disk replacement, the recovered disk is empty and the
  imbalance is real.)
- **Disk-group level** — different disk-groups (on different nodes)
  carry different fill levels: a new node's disk-group is empty while
  old nodes' disk-groups are full. The `AllocateBlocks` request carries
  `disk_group_id` and the **caller** picks the disk-group (§3.2 — "the
  caller (or a future placement service) picks the disk-group"). diskdb
  has no cluster-wide view and cannot move allocations across diskdb
  instances (a disk-group is owned by exactly one diskdb instance, §3.3).
  Today the caller has no per-disk-group load signal from diskdb beyond
  the keepalive-piggybacked `DiskGroupUsageKey` summary (§9), so it
  cannot route around a hot disk-group.

**Current behavior + impact**: there is no imbalance metric, no
load-aware allocation, and no rebalance planner. Operators cannot see
that a disk-group is imbalanced; the allocator actively *preserves*
imbalance by giving a near-full disk the same new-allocation share as
an empty one. A new/recovered disk's capacity is unreachable without
operator-driven workload churn, and a hot disk-group keeps receiving
allocations because the caller has no hint to route elsewhere. Root
cause: the allocator's selection policy is load-blind, and diskdb's
no-data-I/O envelope (§2 — "diskdb allocates blocks; it does not
read/write block contents") forbids diskdb from moving block data
itself, so active relocation must be delegated to the owner / a future
`diskio` service that does not exist yet (same constraint as R76's
explicit skip of real data recovery).

**Design pointers**: §2 (Non-Goals — no data I/O; a future diskio-like
component does data I/O), §3.2 (disk-group → paxos group binding;
`AllocateBlocks` carries `disk_group_id`; the caller picks the
disk-group — disk-group-level rebalance is a caller concern), §3.3
(exclusive ownership — one diskdb instance owns a disk-group; no
cross-instance move), §3.4 (records are the source of truth;
`BusyBlockValue` carries `owner_chunk` — the relocation notification
handle), §8 (disk-add init flow; `Bad → Up` recovery path; effective
status = `max(node, group, disk)`), §9 (Space Metrics — per-disk /
per-disk-group / per-zone usage; keepalive-piggybacked
`DiskGroupUsageKey` summary; gauges are derived snapshots), §10
(Background Scanner — `ScannerTask` / `BgRunner` background-task
pattern, KV-persisted progress, resume after restart). CROW's
caller-routed disk-group model (§3.2) makes cross-disk-group rebalance
a caller concern, which is new; the per-disk load-aware allocation and
the rebalance-planner-with-owner-hand-off are new work shaped on R76's
placeholder-recovery precedent.

**Use scenarios**:
- **New disk added to a hot disk-group** — a disk-group has 3 disks at
  ~90% used; the operator adds a 4th disk. After disk-add init, the
  new disk is 0% used. diskdb's load-aware allocator skews new
  allocations to the new disk so it catches up: new writes go
  preferentially to the empty disk until its `used_pct` approaches the
  group average. No data is moved; the imbalance shrinks via new
  allocations + natural frees on the hot disks. Operators see the
  imbalance gauge fall over time.
- **Recovered disk comes back empty (`Bad → Up` after physical
  replacement)** — a failed disk is physically replaced and the
  operator marks it `Up` (R76 `Bad → Up` path). The disk re-enters
  `allocating_disks` empty. Load-aware allocation fills it
  preferentially, same as the new-disk scenario. The rebalance planner
  flags the persistent imbalance (peers still near-full) and emits a
  relocation plan listing source blocks on the hot disks + the empty
  disk as target — in v1 the plan is logged only (placeholder, no data
  move); in the future the owner relocates the listed blocks.
- **Steady-state, all disks evenly loaded** — all disks in a disk-group
  hover at the same `used_pct`. Load-aware allocation degenerates to
  round-robin (weights equal); no rebalance plan is emitted (imbalance
  below threshold). No overhead vs. today.
- **Persistent imbalance with no churn** — a disk-group stays
  imbalanced because the workload is read-heavy with no frees.
  Load-aware allocation cannot help (no new allocations to skew). The
  rebalance planner detects the persistent imbalance, emits a
  relocation plan, and hands the source-block + `owner_chunk` list to
  the future relocation path. In v1 nothing is moved (placeholder); the
  plan + owner list is the deliverable. Operators see the imbalance
  gauge and the plan count.
- **Disk-group-level imbalance** — two disk-groups, A at 95% and B at
  10%. diskdb reports both groups' `used_pct` via the keepalive
  piggyback + a new `GetRebalanceHint` RPC. The caller / placement
  service routes new `AllocateBlocks` to B. diskdb does not move
  allocations between A and B (cross-instance, §3.3); the routing
  decision is the caller's. diskdb's contribution is the load signal +
  hint, not the move.

**Solution**: A layered rebalance that respects diskdb's no-data-I/O
envelope. v1 ships the diskdb-internal pieces that need no data
movement (imbalance metrics + load-aware allocation skewing) plus a
rebalance planner that produces relocation plans and hands them to the
owner / future `diskio` service via `owner_chunk` (placeholder
relocation in v1, real relocation deferred — same shape as R76's
placeholder recovery). Disk-group-level rebalance is a caller concern
(§3.2); diskdb contributes the load signal + a hint RPC, not
cross-instance orchestration.

One-line summary: detect per-disk / per-disk-group imbalance, skew new
allocations toward under-loaded disks (passive convergence, no data
move), and emit relocation plans with owner hand-off for persistent
imbalance (placeholder relocation in v1; real move deferred to a future
`diskio` service).

1. **Imbalance metrics** — `app/crow-diskdb/src/metrics/` (extends
   `DiskdbMetrics` from R74) + the keepalive reporting loop:
   - Per-disk-group gauges computed from
     `DdbDiskGroup::aggregate_usage()` (disk_group.rs:229):
     `disk_group.imbalance.used_pct_spread` (max `used_pct` − min
     `used_pct` across the group's disks), `disk_group.imbalance.
     used_pct_max`, `disk_group.imbalance.used_pct_min`. Updated on
     the reporting interval (gauges are derived snapshots, §9), not on
     the hot path.
   - Per-disk `disk.used_pct` gauge already exists (§9); the spread is
     derived from the per-disk values the reporting loop already
     collects.
   - Per-instance gauge `rebalance.plan_count` (active relocation plans
     emitted by the planner, work item 3) and `rebalance.planned_blocks`
     (total source blocks across active plans).
   - These give operators the visibility that is entirely missing today.

2. **Load-aware allocation skewing** —
   `app/crow-diskdb/src/model/disk_group.rs` `allocate_block` /
   `allocate_blocks` (disk_group.rs:116 / :150):
   - Replace the pure round-robin cursor (`pos_v_disk_ctx` fetch_add %
     ctx_len) with a **load-weighted** selection over
     `allocating_disks`. Weight = a function of free space (default
     `free_bytes`, configurable via `allocator.load_aware_weight`:
     `free_bytes` | `inverse_used_pct`). Under-loaded disks get a
     larger share of new allocations; the empty/new/recovered disk
     absorbs allocations until its `used_pct` approaches the group
     average.
   - **Degenerate-to-round-robin guarantee** — when all disks' weights
     are equal (steady state), selection is identical to today's
     round-robin (no behavior change, no overhead beyond the weight
     read). The cursor is retained as the tie-breaker among
     equal-weight disks.
   - Configurable: `allocator.load_aware` (default true), on false the
     allocator falls back to pure round-robin (today's behavior).
   - `exclude_disks` anti-affinity and the second-pass full scan in
     `allocate_blocks` are preserved; only the *selection order* within
     a pass becomes load-weighted.
   - This is the disk-level rebalance that needs no data I/O — it
     converges imbalance over time via new allocations + churn.

3. **Rebalance planner (background task + KV-persisted plan,
   placeholder relocation)** — `app/crow-diskdb/src/rebalance/
   planner.rs` (new), following the `BgRunner` + `BackgroundTask`
   pattern (R75 `ScannerTask`, R76 `RecoveryScanTask`):
   - `RebalancePlannerTask` — a per-disk-group background task that
     runs on a configurable interval (`rebalance.plan_interval_secs`,
     default 300). Computes per-disk `used_pct` from
     `aggregate_usage()`, and when the spread exceeds
     `rebalance.imbalance_threshold` (default 20 `used_pct` points),
     selects source busy blocks from the over-loaded disks and target
     under-loaded disks within the same disk-group.
   - **Source selection** — iterate the over-loaded disks' zones via
     `DdbKvClient::read_zone_records` (R72), list live
     `BusyBlockValue`s (each carries `owner_chunk`, §3.4), pick a batch
     sized to close the imbalance (`rebalance.plan_batch_blocks`,
     default 1024). Source disks must be `HwStatus::Up`
     (`DdbDisk::allocatable()`); `Bad`/`Missing` disks are skipped
     (their blocks are the recovery scan's domain, R76).
   - **Target selection** — pick under-loaded `Up` disks in the same
     disk-group as relocation targets. Cross-disk-group targets are
     **not** chosen by the planner (cross-instance move is the caller's
     job, §3.2/§3.3); the planner is intra-disk-group only.
   - **Placeholder relocation** — call `relocate_blocks(plan) ->
     RelocateAction` — a placeholder that logs the plan (source blocks
     + `owner_chunk` + target disk) but does **not** move data (no
     `diskio` component, §2). v1's `RelocateAction` is `LogOnly`;
     future versions add `NotifyOwner` / `ExecuteViaDiskio` when the
     `diskio` service + an owner-notification mechanism exist. This
     mirrors R76's `recover_zone_blocks` placeholder.
   - **Persist plan to KV** — write a `RebalancePlanValue` to the bound
     data group at a per-disk-group key (`RebalancePlanKey {
     disk_group_id }`). The value carries: `status` (`InProgress` /
     `Stopped` / `Complete`), `source_blocks` (batch of
     `{disk_id, zone_index, unit_offset, unit_count, owner_chunk}`),
     `target_disk_id`, `planned_count`, `relocated_count` (0 in v1),
     `started_at_ms`, `updated_at_ms`. Survives restart — on restart
     the planner reads the persisted plan and resumes (or marks it
     `Stopped` if the imbalance has resolved).
   - **Gauge emission** — update `rebalance.plan_count` and
     `rebalance.planned_blocks` (work item 1) after each plan cycle.
   - **Completion** — when the imbalance falls below threshold (or the
     source batch is exhausted), mark the plan `Complete`. The task
     does not loop a single plan; the next cycle re-evaluates and may
     emit a new plan.

4. **Rebalance plan KV schema** —
   `lib/crow-protocol/src/proto/diskdb_type.proto` +
   `lib/crow-protocol/src/key/`:
   - New key type `RebalancePlanKey { disk_group_id }` (BinaryKey, on
     the bound data group alongside zone records).
   - New value type `RebalancePlanValue { status, source_blocks,
     target_disk_id, planned_count, relocated_count, started_at_ms,
     updated_at_ms }` — bincode-serialized (same as other diskdb
     data-group values). `status`: `InProgress` / `Stopped` /
     `Complete`. `source_blocks` is a batch of
     `RebalanceBlock { disk_id, zone_index, unit_offset, unit_count,
     owner_chunk }`.
   - Read/written by the planner (work item 3). In v1 `relocated_count`
     stays 0 (placeholder); the schema is forward-compatible with real
     relocation.

5. **Disk-group-level imbalance hint API** —
   `app/crow-diskdb` gRPC service (`DiskdbService`, §4 Protocol):
   - Add `GetRebalanceHint` RPC — returns per-owned-disk-group
     `used_pct` + `allocatable_disk_count` + an `imbalance_spread`
     flag, derived from `aggregate_usage()`. The caller / placement
     service uses this to route new `AllocateBlocks` to less-loaded
     disk-groups. diskdb does **not** orchestrate cross-instance moves
     (a disk-group is owned by one diskdb instance, §3.3; the caller
     picks `disk_group_id`, §3.2).
   - This is the disk-group-level contribution: report + hint, not
     move. The actual disk-group rebalance is the caller's routing
     decision.

6. **Skip real data relocation** — explicit non-goal (same as R76 work
   item 4):
   - v1 does **not** move block data between disks or disk-groups. There
     is no `diskio` service (§2) and no owner-notification mechanism.
     The planner's `relocate_blocks` is a placeholder that logs the
     plan but **does not act on it**; `relocated_count` stays 0.
   - Load-aware allocation (work item 2) is the only v1 mechanism that
     actually changes where space is consumed — and it does so by
     skewing *new* allocations, never by moving existing data.
   - Automatic data migration / owner notification is a future
     requirement (needs the `diskio` service + an owner-notification
     path that does not exist yet). The `RebalancePlanValue` +
     `owner_chunk` schema is the forward-compatible hand-off.

```
  sync tick / reporting interval
       │
       ├─ aggregate_usage() per owned disk-group
       │      │
       │      ├─ per-disk used_pct → imbalance gauges (work item 1)
       │      │     disk_group.imbalance.used_pct_spread / _max / _min
       │      │
       │      └─ keepalive piggyback DiskGroupUsageKey (existing, §9)
       │
       ├─ allocate_block / allocate_blocks (hot path)
       │      │
       │      └─ LOAD-WEIGHTED selection over allocating_disks (item 2)
       │            ├─ weight = free_bytes (default)
       │            ├─ under-loaded disk gets larger share
       │            └─ equal weights → round-robin (today's behavior)
       │
       └─ RebalancePlannerTask (per-disk-group, BgRunner, interval)
              │
              ├─ spread > threshold? ─ no → no plan, next cycle
              │
              ├─ yes → select source busy blocks (Up disks, owner_chunk)
              │        + target under-loaded Up disk (SAME disk-group)
              │
              ├─ relocate_blocks(plan) → PLACEHOLDER (log only, no move)
              │
              ├─ persist RebalancePlanValue to KV (resume after restart)
              │
              └─ update rebalance.plan_count / planned_blocks gauges

  disk-group level (caller's job, §3.2):
       caller reads GetRebalanceHint (item 5) + keepalive summary
       → routes AllocateBlocks to less-loaded disk-group
       (diskdb does NOT move across instances, §3.3)
```

**Edge cases at a glance**:
- New disk added to a full disk-group → load-aware skew sends new
  allocations to it; imbalance shrinks via churn. If no churn, planner
  flags persistent imbalance and emits a (placeholder) plan — no move
  in v1.
- Recovered disk (`Bad → Up`) comes back empty → same as new disk:
  load-aware fills it preferentially; planner emits a plan if peers
  stay hot.
- All disks equally loaded → load-aware degenerates to round-robin
  (weights equal); planner spread below threshold → no plan. No
  overhead vs. today.
- Single-disk disk-group → no peer to offload to; load-aware is
  round-robin over one disk (no-op); planner no-ops (spread = 0).
- Disk-group-level imbalance (A full, B empty) → diskdb reports both
  via hint + keepalive; caller routes to B. diskdb does not move A→B
  (cross-instance, §3.3).
- Planner threshold not met → no plan emitted; existing plan (if any)
  marked `Complete` on the next cycle that sees spread below threshold.
- Restart mid-plan → planner reads persisted `RebalancePlanValue`
  (`InProgress`), resumes; if the imbalance has resolved while down,
  marks `Stopped`.
- Over-loaded source disk goes `Bad` mid-plan → planner skips `Bad`
  disks on source selection (`allocatable()` = false); in-flight plan
  for that disk abandoned, `relocated_count` stays 0 (no blocks were
  moved in v1).
- Under-loaded target disk goes `Bad` mid-plan → planner re-picks a
  target on the next cycle; the persisted plan is rewritten with the
  new `target_disk_id`.
- `allocator.load_aware = false` → pure round-robin (today's behavior);
  planner still runs and reports imbalance, but no passive convergence
  — operators see the imbalance gauge rise.
- All disks in a disk-group go `Bad` → `allocating_disks` empty,
  allocate returns `NoSpace` (existing behavior); planner skips (no
  `Up` source or target).

**Dependencies**: R71 (`KeepAlive`, sync loop, keepalive piggyback),
R72 (`DdbDisk`, `DdbDiskGroup`, `allocate_block` / `allocate_blocks`,
`DdbKvClient::read_zone_records`, `BusyBlockValue` / `owner_chunk`
record model), R74 (`DiskdbMetrics`, `aggregate_usage`, per-disk
`used_pct` gauge, reporting loop), R75 (`BgRunner`, `ScannerTask` /
`BackgroundTask` pattern — planner follows the same background-task
structure), R76 (`RecoveryScanTask` pattern, `RecoveryScanProgressValue`
schema as the model for `RebalancePlanValue`, placeholder-recovery
precedent — `recover_zone_blocks` LogOnly shapes `relocate_blocks`, and
the `Bad → Up` recovery path that produces the empty-recovered-disk
imbalance). No dependency on R77 (console — the imbalance gauges + hint
RPC are the console's data source, but the console work is separate),
R78 (notify/watch — the planner polls on its own interval), or R79 (free
batch). No dependency on a future `diskio` service — real data
relocation is explicitly skipped (placeholder planner only). Nothing
depends on R80 yet.

**Acceptance**:
- **Imbalance metrics**:
  - A disk-group with 3 disks at `used_pct` 10 / 50 / 90 → reporting
    loop emits `disk_group.imbalance.used_pct_spread = 80`,
    `used_pct_max = 90`, `used_pct_min = 10`. Integration test
    (allocate to skew, read gauges).
  - A disk-group with all disks at `used_pct` 50 → `used_pct_spread =
    0`, `_max = _min = 50`. Integration test.
  - `rebalance.plan_count` and `rebalance.planned_blocks` gauges
    reflect the planner's active plans (0 when no plan, N when N plans
    active). Integration test.
- **Load-aware allocation skewing**:
  - `allocate_block` with `allocating_disks` = [disk A 90% used, disk B
    0% used], `allocator.load_aware = true`, 100 sequential single-
    block allocates → B receives a majority share (e.g. ≥ 80 of 100
    with `free_bytes` weighting); A receives the remainder. Integration
    test.
  - Same setup, `allocator.load_aware = false` → A and B each receive
    ~50 (round-robin, today's behavior). Integration test.
  - `allocating_disks` all at equal `used_pct` → load-aware selection
    matches round-robin (each disk gets ~1/N, within tie-break cursor
    order). Integration test.
  - `exclude_disks` containing B → all allocates go to A (anti-
    affinity preserved under load-aware). Integration test.
  - `allocate_blocks` (multi-block) with skewed disks → first-pass
    round-robin + second-pass full scan both respect load weighting;
    `count` blocks claimed, spread across disks by weight. Integration
    test.
  - Single-disk disk-group → all allocates go to the one disk (no
    crash, no `NoSpace` when it has space). Integration test.
- **Rebalance planner**:
  - Disk-group with spread > `rebalance.imbalance_threshold` (default
    20) → `RebalancePlannerTask` emits a `RebalancePlanValue` with
    `status = InProgress`, `source_blocks` drawn from the over-loaded
    disk's live `BusyBlockValue`s (each carrying `owner_chunk`),
    `target_disk_id` = an under-loaded `Up` disk, `relocated_count =
    0` (placeholder). Integration test.
  - `relocate_blocks` placeholder is called with the plan and returns
    `RelocateAction::LogOnly` — no `BusyBlockKey` deletes, no
    `FreeBlockValue` writes, no data move. Unit test.
  - Disk-group with spread < threshold → no plan emitted; an existing
    `InProgress` plan is marked `Complete` on the next cycle.
    Integration test.
  - Over-loaded source disk transitions `Bad` mid-plan → planner skips
    it on the next source-selection cycle (`allocatable()` = false);
    no source blocks drawn from `Bad` disks. Integration test.
  - Under-loaded target disk transitions `Bad` mid-plan → planner
    re-picks a target; persisted plan rewritten with new
    `target_disk_id`. Integration test.
  - All disks `Bad` → planner no-ops (no `Up` source or target); no
    plan emitted. Integration test.
  - Single-disk disk-group → planner no-ops (spread = 0). Unit test.
- **Plan persistence + resume**:
  - Restart diskdb while a plan is `InProgress` → planner reads
    persisted `RebalancePlanValue`, resumes (re-evaluates imbalance;
    marks `Stopped` if resolved, else continues). Integration test.
  - `RebalancePlanKey` + `RebalancePlanValue` round-trip through KV
    (write + read back), including `source_blocks` batch with
    `owner_chunk`. Unit test.
- **Disk-group-level hint API**:
  - `GetRebalanceHint` returns per-owned-disk-group `used_pct` +
    `allocatable_disk_count` + `imbalance_spread` flag, matching
    `aggregate_usage()`. Integration test.
  - Two disk-groups A (95%) and B (10%) → hint reports both; caller
    can route `AllocateBlocks` to B (diskdb does not move A→B).
    Integration test (verify hint content; cross-instance move is out
    of scope — assert no move occurs).
- **Skip real data relocation**:
  - On a planner cycle that emits a plan, no `BusyBlockKey` deletes or
    `FreeBlockValue` writes happen on the source disks (placeholder =
    `LogOnly`); `relocated_count` stays 0. Integration test.
  - `allocator.load_aware = true` changes only *new* allocation
    placement — existing `BusyBlockKey`s are never moved or deleted by
    the allocator. Integration test.
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- `pixi run test-diskdb` (relevant integration tests pass).

**Open Questions**:
- **Load-weight function** — `free_bytes` (absolute free space) vs.
  `inverse_used_pct` (relative). `free_bytes` favors large disks even
  when they are relatively full; `inverse_used_pct` favors relatively
  empty disks regardless of size. The two converge differently on
  mixed-capacity disk-groups. Default `free_bytes` is simpler and
  matches the "fill the empty disk" intuition, but a mixed-capacity
  cluster may want `inverse_used_pct`. Decision deferred to the design
  draft — needs a small capacity-mix scenario analysis. Cannot be
  resolved automatically because it depends on the target deployment's
  disk-size homogeneity, which is not fixed.
- **Planner source-block selection policy** — oldest-first (free the
  longest-lived blocks first, likely cold) vs. random vs.
  owner-chunk-grouped (batch all blocks of one owner together so the
  owner can relocate one chunk in one shot). Owner-chunk-grouped
  minimizes owner-side relocation work but couples diskdb to the
  owner's chunk granularity. Decision deferred to the design draft —
  needs the owner-notification interface to be sketched first, which
  does not exist yet (R76 has the same open dependency on a future
  `diskio` / owner-notify path). Cannot be resolved automatically
  because it depends on an interface that has not been designed.
