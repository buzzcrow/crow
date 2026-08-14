<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R86: chunkdb — Topology Management

**Problem**:

- **Current behavior + impact** — chunkdb placement decisions (R87) need
  cluster topology (site → rack → node → disk-group) to enforce
  rack/node-aware fault isolation. There is no topology cache in the
  chunkdb server yet (R85 lands only the skeleton). Without a topology
  cache, every placement decision would have to query group-0
  synchronously — too slow for allocation latency targets — and there
  would be no mechanism to react to disk failures or maintenance mode
  changes in real time. Placement would either be stale (wrong
  fault domains) or serial (unacceptable latency). The watch/notify
  extension (R82 coalescing, design
  `design-crow-kv-watch-notify.md` §5 diskdb notify handler) already
  provides the real-time update primitive; chunkdb must subscribe to
  it for disk-group and node status keys.
- **Design pointers** —
  [`doc/design/chunkdb/design-crow-chunkdb.md`](../design/chunkdb/design-crow-chunkdb.md)
  §3.4 (topology cache with group-0 integration), §3.4a (watch/notify
  for real-time updates), §6 (topology management — `TopologyCache`,
  `TopologySnapshot`, `TopologyRefresh` hybrid approach),
  [`design-crow-kv-group0.md`](../design/kv/design-crow-kv-group0.md)
  §2.8 (hardware admin via `kv-client` — `HardwareClient` is the
  sysdata API surface), §3.1 (key layout — `/hw/rack/...`,
  `/hw/node/...`, `/hw/dg/...`),
  [`design-crow-kv-watch-notify.md`](../design/kv/design-crow-kv-watch-notify.md)
  §4 (`WatchNotifyClient`), §5 (diskdb notify handler + polling safety
  net — chunkdb follows the same pattern). aioss analog: aioss chunkdb
  refreshes topology from metadb every 10s; CROW adds watch/notify for
  sub-second responsiveness (design §3.4a — new work beyond aioss).
- **Use scenarios** —
  - **Cold-start topology load** — chunkdb server starts; the
    `TopologyRefresh` task fetches the full site/rack/node/disk-group
    hierarchy from group-0 via `HardwareClient` and populates
    `TopologyCache`. Placement decisions (R87) immediately have a
    consistent snapshot to work against.
  - **Periodic refresh catches a missed notification** — a watch/notify
    message is lost (network blip); the 30s periodic refresh fetches
    the full topology and corrects the cache. Placement never stays
    stale for more than one refresh interval.
  - **Real-time disk-group status change** — a disk-group goes
    `Bad` (disk failure detected by diskdb); group-0 watch fires a
    notify on the disk-group status key; chunkdb's notify handler
    updates `TopologyCache` immediately, so the next placement
    (R87) excludes that disk-group without waiting for the periodic
    refresh.
  - **Node enters maintenance mode** — an operator sets a node's
    `HwStatus` to `Maintenance` via `HardwareClient`; the watch fires;
    chunkdb updates the cache so placement skips that node's
    disk-groups.
  - **Placement reads a consistent snapshot** — an `AllocateChunk`
    call (R89) fetches a `TopologySnapshot` (point-in-time immutable
    clone) from `TopologyCache` and uses it for the entire placement
    decision; concurrent cache updates do not affect the in-flight
    placement.
  - **chunkdb restart rebuilds cache** — chunkdb crashes and restarts;
    on startup the `TopologyRefresh` task repopulates `TopologyCache`
    from group-0 (stateless — no local persistence, design §3.6).

**Solution**:

**One-line summary**: add a `TopologyCache` (point-in-time snapshot for
placement) with a hybrid update path — periodic full refresh from
group-0 via `HardwareClient` plus watch/notify for immediate status
changes — following the diskdb notify handler pattern.

1. **TopologyCache + TopologySnapshot** —
   `app/crow-chunkdb/src/topology/mod.rs` (new module):
   - `TopologyCache`: `Arc<RwLock<TopologySnapshot>>` holding the
     current cluster hierarchy (sites, racks, nodes, disk-groups with
     `HwStatus` + capacity info). Design §6.
   - `TopologySnapshot`: immutable point-in-time clone; placement (R87)
     calls `cache.snapshot()` to get a consistent view for one
     allocation. Lock scoping: acquire in `{}` block, drop before
     `.await` (design §12).
   - Accessors: `healthy_disk_groups()`, `disk_group_leader(dg_id)`,
     `racks_for_nodes(node_ids)`, `nodes_in_rack(rack_id)` — used by
     R87 placement selector.

2. **Periodic refresh task** —
   `app/crow-chunkdb/src/topology/refresh.rs` (new):
   - `tokio::spawn` background loop; interval configurable (default
     30s, design §13 `topology_refresh_interval`). Fetches full
     hierarchy via `HardwareClient` (rack/node/disk-group scans per
     group-0 §3.1 key layout) and replaces the cache snapshot.
   - Fallback for missed notifications (design §3.4a); never skips a
     refresh even if watch/notify is healthy (consistency verification).

3. **Watch/notify integration** —
   `app/crow-chunkdb/src/topology/notify.rs` (new):
   - Register a `WatchNotifyClient` subscription for group-0 keys
     matching `/hw/node/*` and `/hw/dg/*` (status + capacity keys).
   - On notify: parse the key, fetch the updated record via
     `HardwareClient`, update the affected entry in `TopologyCache`
     immediately (fine-grained update, not full refresh).
   - Follow the diskdb notify handler pattern (design
     `design-crow-kv-watch-notify.md` §5.4 — notify handler task) +
     polling safety net (§5.5 — periodic refresh covers missed
     notifies).

**Flow diagram**:

```
  group-0 (HardwareClient + WatchNotifyClient)
       │
       ├── periodic refresh (30s) ──► full hierarchy ──► TopologyCache (replace)
       │
       └── watch/notify on /hw/node/* , /hw/dg/*
                    │
                    ▼
              notify handler (item 3)
                    │  parse key → fetch one record → update entry
                    ▼
              TopologyCache (fine-grained update)
                    │
                    ▼
   placement (R87) calls cache.snapshot() ──► TopologySnapshot (immutable)
```

- **Edge cases at a glance**:
  - Watch/notify connection drops → periodic refresh (30s) is the
    safety net; cache stays eventually consistent. Reconnect logic
    lives in `WatchNotifyClient` (design §4).
  - Notify arrives for a disk-group that no longer exists (deleted
    between notify and fetch) → `HardwareClient` fetch returns
    not-found; cache entry is removed; no crash.
  - Periodic refresh returns empty topology (group-0 temporarily
    unreachable) → keep the previous snapshot (do not replace with
    empty); log a warning; placement continues against stale data
    (better than no data).
  - Concurrent notify + periodic refresh → `RwLock` serializes writes;
    last writer wins; snapshot readers always see a consistent point-
    in-time view.
  - chunkdb restart → cache is empty until the first refresh completes;
    placement requests before the first refresh return a
    `TopologyNotReady` error (or block briefly — design decision, see
    Open Questions).

**Dependencies**:

- **R85** (foundation) — chunkdb server crate + `crow-common` must
  exist; R86 adds the `topology/` module to the server.
- **R71** (group-0 sysdata) — `HardwareClient` in `crow-kv-client` is
  the topology fetch API; must be landed.
- **R82** (watch/notify coalescing) — the coalescer reduces notify
  amplification; R86 works without R82 (correctness) but benefits from
  R82 (load). Fallback without R82: uncoalesced notifies (one per
  changed key) — still correct, just more wakeups.
- **`WatchNotifyClient`** in `crow-kv-client` (design
  `design-crow-kv-watch-notify.md` §4) — must be landed for the
  real-time update path.
- **R87** (placement) depends on R86 — the selector reads
  `TopologySnapshot`.
- **R99** (dynamic range binding) — R99's `RangeBindingClient`
  follows the same hybrid cache pattern (periodic refresh +
  watch/notify) as R86's `TopologyCache`; the two caches may share
  infrastructure. R86 is not blocked by R99 (topology cache works
  without sharding).

**Acceptance**:

**Cold-start + periodic refresh**:
- chunkdb server starts with an empty `TopologyCache`; after the first
  refresh (≤ refresh interval), `cache.snapshot()` returns a
  `TopologySnapshot` with all racks/nodes/disk-groups from group-0 →
  verify against a 3-rack, 5-node, 8-disk-group test cluster.
  Integration test.
- Periodic refresh replaces the cache every interval; a topology
  change (new disk-group added via `HardwareClient`) is visible in the
  snapshot within one refresh cycle → add disk-group, wait ≤ 30s,
  `snapshot()` includes it. Integration test.

**Watch/notify real-time updates**:
- A disk-group's `HwStatus` changes from `Ok` to `Bad` via
  `HardwareClient`; the watch fires; within 1s `cache.snapshot()`
  reflects the `Bad` status (no 30s wait) → verify the disk-group is
  excluded from `healthy_disk_groups()`. Integration test.
- A node enters `Maintenance`; within 1s `cache.snapshot()` marks the
  node `Maintenance` and its disk-groups are excluded from
  `healthy_disk_groups()`. Integration test.

**Missed-notification recovery**:
- Watch/notify connection is dropped; a disk-group goes `Bad` during
  the outage; the next periodic refresh (≤ 30s) corrects the cache →
  `snapshot()` reflects `Bad`. Integration test.

**Snapshot consistency**:
- An `AllocateChunk` placement call holds a `TopologySnapshot` for the
  full duration; a concurrent watch/notify updates the cache; the
  in-flight placement sees the original snapshot, not the updated one
  (no torn read) → verify by injecting a status change mid-placement
  and checking the placement used the pre-change topology.
  Integration test.

**Edge cases**:
- Notify for a deleted disk-group → `HardwareClient` fetch returns
  not-found; cache entry removed; no panic. Unit test.
- Periodic refresh fails (group-0 unreachable) → previous snapshot
  retained; warning logged; `snapshot()` still returns the stale-but-
  valid topology. Unit test.
- Concurrent notify + refresh → `RwLock` serializes; no torn snapshot
  visible to readers. Unit test.

**Lint + test commands**:
- `pixi run cargo fmt --all -- --check` passes.
- `pixi run cargo clippy --all-targets -- -D warnings` passes.
- `pixi run test-chunkdb` (topology unit + integration tests pass).

**Open Questions**:

- **Placement before first refresh — block or error?** On chunkdb
  restart, the cache is empty until the first periodic refresh
  completes. Options: (a) block placement requests until the first
  refresh (simple, adds startup latency); (b) return a
  `TopologyNotReady` error immediately (caller retries); (c) do a
  synchronous refresh on startup before serving requests. Trade-off:
  (a) and (c) add startup latency but simplify the caller; (b) pushes
  retry logic to the caller. Recommendation: (c) — synchronous initial
  refresh on startup, then switch to periodic. Design decision.
