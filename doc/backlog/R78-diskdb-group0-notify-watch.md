<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R78: diskdb — Group-0 Notify/Watch (Replace Polling Sync)

**Problem**:

- **Current behavior + impact** — R71 implements the diskdb group-0
  sync loop with fixed-interval polling (default 10 s, read from
  `DdbConfig.sync.sync_interval_secs`). Every diskdb instance prefix-
  scans group 0 on each tick to detect ownership changes, new disks,
  and status updates. This works for v1 but has two drawbacks:
  - **Latency** — a status change (disk bad, disk-group reassigned)
    takes up to one sync interval (10 s) to be observed. For failure
    detection and ownership transfer, faster propagation is
    desirable.
  - **Wasted reads** — every poll is a prefix scan of group 0 even
    when nothing changed. With many diskdb instances, this is
    redundant load on group 0.
  - **Root cause** — deferred placeholder, not a bug: R71 shipped
    polling as the v1 change-detection mechanism; push-based notify
    was always a follow-up.
- **Design pointers** —
  [`doc/design/diskdb/design-crow-diskdb.md`](../design/diskdb/design-crow-diskdb.md)
  §10 (Follow-up — group-0 notify/watch) states v1 ships with
  fixed-interval polling and lists watch/notify as a follow-up where
  group 0 pushes notifications to diskdb and polling stays as a
  safety net with an increased interval. The implementation design is
  in
  [`doc/working/design-crow-kv-watch-notify.md`](../working/design-crow-kv-watch-notify.md)
  (design draft). No direct aioss analog — new work; the reference
  model is etcd watch (watcher fed from the mvcc backend apply
  stream, not the Raft proposal path).
- **Use scenarios** —
  - **Disk failure propagation** — an operator marks a disk bad (or
    a health probe flips it) in group 0; the owning diskdb observes
    the new status and stops allocating on it within ~1 s, not 10 s.
  - **Ownership transfer** — a disk-group's owner is reassigned in
    group 0; the old owner drops the disk-group and the new owner
    picks it up within ~1 s, not 10 s.
  - **New disk discovery** — a disk is added to a node in group 0;
    the owning diskdb initializes the disk (zone baselines) within
    ~1 s, not 10 s.
  - **Client endpoint cache proactive refresh (R74 use case)** — a
    diskdb instance moves (endpoint change in its service-registry
    record); a client holding the old endpoint cache entry refreshes
    it before its next `allocate_blocks`/`free_blocks` call, instead
    of failing one attempt and retrying.
  - **Missed-notify fallback** — a notify is dropped (watcher channel
    full) or the WatchNotify stream is disconnected; the diskdb still
    converges to the group-0 state on the next safety-net poll (60 s).

**Solution**:

Add a watch/notify mechanism so a crow-kv leader pushes
hw-status-change and ownership-change notifications to subscribers
over a long-lived gRPC bidi stream, replacing fixed-interval polling
as the primary change-detection mechanism. diskdb subscribes to the
group-0 prefixes it cares about and triggers an immediate sync on
notify; polling stays as a safety net at a raised interval.

*One-line summary*: client-pulled `WatchNotify` bidi stream + leader-
side apply-path trigger, with polling as a safety net.

The detailed design (proto, registry, coalescer, trigger path
selection, lifecycle) is in the design draft
[`doc/working/design-crow-kv-watch-notify.md`](../working/design-crow-kv-watch-notify.md);
this section states *what* is built and *where*, not *how*.

1. **crow-kv watch/notify extension** (`lib/crow-kv`) — add a
   `WatchNotify` gRPC bidi stream to `KvService` and a per-group
   `WatchRegistry` so a client can subscribe to a `(group_id, prefix)`
   and receive `WatchNotify` frames (changed keys, not values) when a
   watched prefix is written. The notify fires on the leader's apply
   path after a slot is chosen (the etcd model — watcher fed from the
   apply stream, not the proposal path), gated by an empty-registry
   fast path so a group with no watchers pays one predicted-not-taken
   branch per apply. The trigger-path selection rationale (apply-path
   hook vs two-code-copies vs WAL tailer) is in the design draft §2.4;
   the chosen option is the apply-path hook. This is the main crow-kv
   design work and the subject of a future sub-design doc
   (`design-crow-kv-watch-notify.md`, folded from the working draft on
   merge).
2. **WatchNotify client** (`lib/crow-kv-client`) — a reusable
   `WatchNotifyClient` that opens the bidi stream to the group-0
   leader (discovered via the topology cache), sends subscribe frames,
   and delivers notify frames over a channel. Handles leader-change by
   reconnecting to the new leader and re-subscribing, keeping the
   subscriber's channel open across the reconnect (mirrors the
   `LearnerStream` reconnect pattern).
3. **diskdb notify handler** (`app/crow-diskdb/src/liveness/notify.rs`,
   new) — a `NotifyHandler` task that subscribes to the group-0
   prefixes diskdb cares about (`/hw/dg_owner/`, `/hw/dg_bind/`,
   `/hw/disk/`) and wakes the keepalive sync loop on each notify. The
   notify is a **trigger, not a transport** — diskdb re-reads the
   actual values from group 0 via the normal sync path
   (`observe_ownership`, `observe_disks`), keeping the notify payload
   small and avoiding a duplicated read path.
4. **KeepAlive hybrid wake** (`app/crow-diskdb/src/liveness/keepalive.rs`
   + `app/crow-diskdb/src/bg_task.rs`) — extend the `Trigger` enum
   with a `TimerOrEvent` variant (timer + external `Notify` signal) so
   the keepalive loop wakes on either the safety-net timer tick or an
   external notify. KeepAlive gains a `sync_trigger` field + a
   `trigger_now()` method called by the notify handler.
5. **Polling as a safety net** — keep the fixed-interval sync loop
   (R71) as a safety net, but raise the effective interval (e.g. 60 s
   instead of 10 s) when notify is on, since notifies handle the
   common case. The polling loop catches any missed notifies
   (reliability fallback). When notify is off (v1 default), behavior
   is unchanged — fixed-interval polling at `sync_interval_secs`.
6. **Configuration** (`app/crow-diskdb/src/ddb_config.rs`) — add a
   `NotifyConfig { notify_enabled }` section. `notify_enabled`
   (default false) toggles polling-only vs notify+polling.
7. **Client endpoint cache proactive refresh (R74 use case)**
   (`lib/crow-diskdb-client/src/client.rs`) — `DiskdbClient`'s
   `endpoint_cache` (R74, now implemented: `refresh_endpoints` reads
   `read_all_diskdb_instances`) refreshes **on demand** today
   (startup, cache miss, error-retry). R78 extends this to proactive
   refresh: the client subscribes to the `/srv/diskdb/` prefix via
   `WatchNotifyClient`; on a notify (instance register/deregister/
   move), it refreshes the affected cache entries so the next
   `allocate_blocks`/`free_blocks`/`query_*` routes to the new
   endpoint without a failed attempt + retry. The v1 on-demand
   refresh remains as the safety net; proactive refresh is an
   optimization, not a correctness requirement.

**Note on endpoint registration** — the original design-doc §10
framing ("group 0 pushes notifications to registered diskdb
endpoints; each diskdb registers its endpoint on sync") evolved in
the design draft to a **client-pulled bidi stream**: diskdb opens the
`WatchNotify` stream to the group-0 leader and subscribes; the leader
pushes notifies over that stream. No separate notify-endpoint
registration is needed for notify delivery. The `grpc_endpoint` arg
of `heartbeat_diskdb` (R71/R74) is the diskdb gRPC service address
for service-registry discovery (so clients can route
`allocate_blocks`), and is **already populated** from
`config.server.listen_addr` (`app/crow-diskdb/src/main.rs`,
`.with_grpc_endpoint(...)`), passed on every sync tick
(`keepalive.rs` `heartbeat_diskdb` call). Design-doc §10 will be
updated to the bidi-stream model when the design draft is folded in.

```
                     group-0 leader (crow-kv)
                     ┌──────────────────────────────┐
                     │ PxGroup                       │
                     │  WatchRegistry (per-prefix)   │
                     │  apply-path trigger ──────────┼──┐
                     └─────────────┬────────────────┘  │ (chosen slot
                                   │ WatchNotify       │  touches watched
                       ┌───────────┴────────────┐      │  prefix)
                       │   bidi gRPC stream     │      │
            subscribe  │  (client-pulled)       │      │
        ┌──────────────┴────────┐  ┌────────────┴───────┴──────┐
        │ diskdb NotifyHandler  │  │ crow-kv KvService         │
        │  /hw/dg_owner/        │  │  watch_notify handler     │
        │  /hw/dg_bind/         │  │  (subscribe/leader-check) │
        │  /hw/disk/            │  └───────────────────────────┘
        └──────────┬────────────┘
                   │ notify_one()
        ┌──────────┴────────────┐
        │ KeepAlive             │   safety-net timer (60 s)
        │  Trigger::TimerOrEvent│ ────────────────────────────┐
        │  tick() → re-read     │                             │ fallback
        └───────────────────────┘                             │
                                                              │
        ┌──────────────────────┐  subscribe /srv/diskdb/      │
        │ DiskdbClient (R74)   │ ◄────────────────────────────┘
        │  endpoint_cache      │   proactive refresh on notify
        │  + WatchNotifyClient │   (on-demand refresh = safety net)
        └──────────────────────┘
```

**Edge cases at a glance**:

- **Watcher channel full** → `try_send` drops the notify silently
  (write path never blocks on a slow watcher); safety-net polling
  catches the missed change.
- **Leader step-down mid-stream** → old leader clears its
  `WatchRegistry` + flushes the coalescer (final notifies emitted);
  dropping the watcher senders closes client streams; clients
  reconnect to the new leader and re-subscribe; safety-net covers
  the gap.
- **Client subscribed to a group this node doesn't lead** → server
  returns `WatchNotifyError { not_leader_hint }` per-subscribe
  without closing the stream (client may subscribe to other groups
  on the same stream); client reconnects to the leader for that
  group.
- **Foreign-value retry on the write path** → trigger fires for the
  foreign value (a real write to real keys) and again for the
  retried client value; both notifies are correct.
- **NoOp / repair slots** → `Batch::decode` on empty payload yields
  no ops; no notify fires.
- **Notify disabled (v1 default)** → no notify handler, no
  `WatchNotifyClient`, keepalive uses the existing `TimerFn` trigger
  at `sync_interval_secs` (10 s); zero overhead when disabled.
- **diskdb down when a notify fires** → no stream is open, so no
  notify is delivered; on restart diskdb opens the stream and the
  next change is notified; any change missed while down is caught by
  the safety-net poll on restart.
- **Client misses a notify (partition / was down)** → v1 on-demand
  refresh (cache-miss + error-retry) remains as the safety net; the
  cache is eventually consistent either way.

**Dependencies**:

- **R71** (landed) — `KeepAlive` sync loop
  (`app/crow-diskdb/src/liveness/keepalive.rs`), fixed-interval
  polling, `heartbeat_diskdb` (endpoint slot wired and **populated**
  from `config.server.listen_addr`), `BackgroundTask` / `BgRunner` /
  `Trigger` framework (`app/crow-diskdb/src/bg_task.rs`).
- **R74** (landed) — `DiskdbClient` + `endpoint_cache` +
  `refresh_endpoints` + `read_all_diskdb_instances`
  (`lib/crow-diskdb-client/src/client.rs`); the service-registry
  `/srv/diskdb/` prefix that item 7 watches. Item 7 is no longer
  blocked by R74.
- **Depends on this** — none yet. Future groups opt into watch/notify
  via the same `WatchNotify` stream with no design change (the
  registry is per-group; the empty-registry fast path is the only
  gate).

**Acceptance**:

**WatchRegistry + coalescer (crow-kv)**:
- `WatchRegistry::subscribe` for prefix `/hw/disk/`, `emit` a changed
  key `/hw/disk/1/2/3/abcd` → watcher channel receives a `WatchNotify`
  with the key; a key outside the prefix (`/hw/rack/1`) does not
  notify. Unit test.
- Two watchers on the same prefix, `emit` one key → both receive the
  notify. Unit test.
- `subscribe` then `unsubscribe` by `watcher_id`, `emit` → no notify
  received (channel stays empty). Unit test.
- `subscribe` 3 prefixes via one stream, `remove_all` by the recorded
  ids, `emit` to all 3 → no notifies. Unit test.
- `subscribe` with a capacity-1 channel, fill it, `emit` 3 keys → no
  panic (`try_send` drops silently); watcher misses notifies. Unit
  test.
- `is_empty` true before subscribe, false after, true after
  `unsubscribe` + `clear`. Unit test.
- `subscribe` 3 watchers, `clear()` → `is_empty` true and all
  channels closed (sender dropped). Unit test.

**WatchNotifyClient (crow-kv-client)**:
- `WatchNotifyClient::subscribe` to `(group_0, /hw/disk/)` against a
  3-node cluster; `Put` a matching key on the leader → the
  subscription's `notify_rx` yields a `WatchNotify` with the key.
  Integration test.
- Force a leader change while subscribed → the client reconnects to
  the new leader, re-subscribes, and the subscriber's `notify_rx`
  stays open; a subsequent `Put` is notified within 1 s. Integration
  test.
- Drop the `WatchSubscription` → server removes the watcher; a
  subsequent `Put` to the prefix produces no notify on the dropped
  subscription. Integration test.

**WatchNotify server handler (crow-kv)**:
- Open a stream to a non-leader node, `WatchSubscribe` for a group it
  doesn't lead → `WatchNotifyError { not_leader_hint }` received;
  stream stays open (can subscribe to another group). Unit test.
- Open a stream to the leader, subscribe to a prefix, `Put` a matching
  key → `WatchNotify` frame received with the key. Integration test.
- Open a stream, subscribe, drop the stream, `Put` a matching key →
  no notify (registry cleaned up on stream end). Integration test.
- Subscribe on the leader, force step-down → client stream closes
  (sender dropped). Integration test.

**Trigger::TimerOrEvent (diskdb)**:
- `TimerOrEvent` with a 10 ms interval, no notify → task wakes within
  50 ms. Unit test.
- `TimerOrEvent` with a 60 s interval, `notify_one()` → task wakes
  immediately (< 50 ms). Unit test.
- Both timer and notify fire simultaneously → `select!` does not
  panic. Unit test.

**diskdb notify-driven sync (E2E)**:
- Start a 3-node kv-cluster (group 0) + one diskdb with
  `notify_enabled=true`; wait for initial sync; add a disk via
  `HardwareClient::add_disk` → diskdb container sees the new disk
  within 1 s (notify woke keepalive), not 60 s. E2E test.
- Reassign a disk-group's owner in group 0 → owning diskdb drops the
  disk-group within 1 s (notify-driven), not 60 s. E2E test.
- Subscribe with a capacity-1 channel, fill it (drop the reader),
  write to group 0 → diskdb still syncs within 60 s (safety-net
  timer). E2E test.
- Subscribe on the group-0 leader, force a leader change → diskdb
  `WatchNotifyClient` reconnects to the new leader and subsequent
  writes are notified within 1 s; safety-net covers the reconnect
  gap. E2E test.
- `notify_enabled=false` (v1 default), add a disk → diskdb sees it
  only on the next timer tick (10 s default), proving zero overhead
  when disabled. E2E test.

**Client endpoint cache proactive refresh (R74 use case, item 7)**:
- `DiskdbClient` subscribed to `/srv/diskdb/`, change a diskdb
  instance's `grpc_endpoint` in group 0 → client's `endpoint_cache`
  entry for the affected `disk_group_id` refreshes without a failed
  `allocate_blocks` attempt; next call routes to the new endpoint.
  Integration test.
- Client misses a notify (stream disconnected) → next
  `allocate_blocks` to a moved instance fails once, triggers
  `refresh_endpoints` (on-demand safety net), retries successfully.
  Integration test.

**Test commands**: `pixi run test-kv-core`,
`pixi run test-kv-client`, `pixi run test-diskdb`,
`pixi run test-diskdb-client`, `pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.

**Non-goals**:

- Not a sysdata-only mechanism by design — the watch/notify is
  general-purpose (etcd-style range watch on any key range). v1
  *deploys* it only for group-0 sysdata (node/disk/disk-group
  metadata, ownership map, binding map, service registry
  `/srv/diskdb/` for client endpoint cache); other groups opt in via
  future requirements with no design change. The gating mechanism is
  an empty-registry fast path on the apply path (one predicted-not-
  taken branch when no watchers), not a per-group perf flag.
- Not a replacement for the sync read path — diskdb still reads the
  actual values from group 0; the notify is only a trigger.
- Not in v1 — v1 ships with fixed-interval polling (R71) and
  on-demand client cache refresh (R74). This requirement is a
  follow-up.
