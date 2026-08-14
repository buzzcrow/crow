<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R76 + R78 — Unresolved Questions & Gaps

## R76 (Disk Failure Detection + Recovery Scan)

### Resolved
- Bad → Up operator override added to state machine.
- `RecoveryScanProgressValue` schema + persistence via `DdbKvClient`.
- `RecoveryScanTask` per-disk background scan with progress persistence
  and `disk.bad.impacted_blocks` gauge updates.
- `KeepAlive` per-disk miss tracking, Missing → Bad on consecutive
  misses, scan spawn on Bad, scan stop on Up, scan resume on startup.

### Open → Resolved / Deferred (review 2026-08-14)

1. **Recovery scan block iterator** — **Resolved in code.**
   `RecoveryScanTask::scan_zone` (`app/crow-diskdb/src/recovery/
   disk_recovery.rs`) calls `DdbKvClient::read_zone_records`
   (`app/crow-diskdb/src/ddb_kv_client.rs`), which does real prefix
   scans of `BusyBlockKey` per zone (`BusyBlockKey::prefix_for_zone`)
   and returns live `BusyBlockValue`s. The earlier "placeholder
   iterator that yields an empty list" note was stale — the scan
   iterates real busy blocks zone by zone, exactly as the design doc
   specifies. The list-by-zone approach is correct; a list-by-disk
   optimization (fewer list ops for sparse disks) is a possible future
   follow-up, not a correctness gap.

2. **Scan throttle / rate limit** — **Deferred (design decision).**
   diskdb does not perform the recovery job itself (no `diskio`
   service); the scan only enumerates impacted blocks. The current
   loop lists one zone's busy blocks, processes them, then moves to
   the next zone — naturally bounded by one list op per zone, not an
   unthrottled hot loop. An explicit blocks/sec rate limit is deferred
   to the future recovery process that will actually consume disk I/O.
   No code change now; the decision is recorded here. (The "rate limit"
   was never in the formal design doc, only in this gap note.)

3. **Operator override API** — **Resolved (API exists in kv-client).**
   `HardwareClient` in `crow-kv-client` already exposes
   `set_disk_status(rack_id, node_id, dg_id, disk_id, status)` and
   `set_disk_group_status(rack_id, node_id, dg_id, status)`
   (`lib/crow-kv-client/src/hardware.rs`). The operator changes disk /
   disk-group status directly on the sys group (group 0); diskdb
   observes the change via the keepalive sync loop (and, when enabled,
   the R78 watch/notify stream) and runs the unified recovery path on
   the next `→ Up` transition. The CLI / HTTP admin surface that wraps
   these client calls is R77's scope (`R77-diskdb-console-cli.md`
   already references `set_disk_status` / `set_disk_group_status`).

4. **Recovery scan progress persistence key collision** — **Resolved
   for R76; broader question split to R81.**
   `RecoveryScanProgressKey` is keyed by `DiskId`, which is a 128-bit
   **globally unique** identifier (`common_type.proto`: "128-bit disk
   identifier ... Globally unique"; `design-crow-kv-group0.md` §2.5;
   `design-crow-protocol-key.md` §3.4). A disk removed and re-added
   gets a **new** `DiskId`, so the old progress record is never
   incorrectly resumed — at worst it is orphaned (a minor leak, not a
   correctness collision). No epoch/generation is needed in this key.
   The broader question — adding an epoch/generation to the reusable
   **integer** IDs (`RackId`, `NodeId`, `DiskGroupId`, `store_id`,
   `group_id`, `replica_id`) so a removed-and-readded entity with the
   same integer ID can be distinguished — is a large cross-cutting
   change (proto + key encoding + all sysdata records + all
   consumers). It is out of R76 scope and tracked as **R81**
   (`R81-sysdata-epoch-for-integer-ids.md`). Note: paxos groups already
   carry a `membership_epoch` fence (`design-crow-kv-reconfiguration.md`
   §6) for consensus safety, but that is per-group reconfiguration
   fencing, not identity-reuse disambiguation. A related placement
   concern — a disk **moved** between nodes/disk-groups keeps its same
   `DiskId` (UUID) but changes its bind; an epoch on `DiskValue` is
   needed to track that placement change (stale bind/ownership +
   orphaned recovery-scan progress on the old bind). That is also
   tracked in **R81**.

## R78 (Watch/Notify Extension)

### Resolved
- Proto schema: `WatchSubscribe`, `WatchUnsubscribe`, `WatchNotify`,
  `WatchNotifyError`, `WatchNotifyRequest` (oneof), `WatchNotifyResponse`
  (oneof), `WatchNotify` bidi RPC on `KvService`.
- `WatchRegistry` (per-group, DashMap-backed, atomic `has_watchers`
  fast path).
- `PxLearner::apply_entry` apply-path hook: `registry.emit` after
  each engine apply, gated by `has_watchers()` for zero overhead when
  no watchers.
- `PxGroup` holds `watch_registry` Arc; wired into the learner in
  `PxGroup::new`.
- Leader step-down cleanup: `step_down` + the two direct
  `become_follower` calls in `group_propose.rs` clear the registry
  (drops watcher tx senders, closing client streams for clean
  reconnect).
- `watch_notify` server handler in `kv_service.rs`: bidi stream,
  per-stream watcher-id tracking, leader check with
  `not_leader_hint` redirect, stream-end cleanup via `remove_all`.
- `WatchNotifyClient` in `crow-kv-client`: `subscribe` returns a
  `WatchSubscription` with a `notify_rx`; spawned reader task
  auto-reconnects on leader change with exponential backoff (capped
  at 2 s); updates topology cache on `not_leader_hint`.
- `Trigger::TimerOrEvent` variant in `bg_task.rs` for keepalive:
  wakes on either the timer (safety-net poll) or the notify.
- `NotifyHandler` in `app/crow-diskdb/src/liveness/notify.rs`:
  subscribes to group-0 prefixes (`/hw/dg_owner/`, `/hw/dg_bind/`,
  `/hw/disk/`), merges notify streams, wakes keepalive on each frame.
- `NotifyConfig` in `ddb_config.rs`: `notify_enabled` (default
  false). Wired into `main.rs`: spawns `NotifyHandler` + passes sync
  trigger to keepalive when enabled.
- `StopHandle::notified()` helper for long-lived tasks that don't
  follow the trigger→cycle model.

### Open
1. **Resume from slot / replay**: the design doc mentions an optional
   `from_slot` for replaying changes since a given slot before live
   tailing. Not implemented — the proto `WatchSubscribe` has no
   `from_slot` field (dropped in favor of the simpler key-list
   model). Clients rely on the safety-net poller to catch missed
   changes during reconnect gaps.
2. **WatchNotify carries keys, not values**: per the design doc, the
   notify frame carries only the changed keys (deduplicated), not
   the values. The client re-reads via the normal `Get` path. This
   is implemented as designed, but means a notify storm followed by
   a re-read storm can amplify read load. Consider a future "fat
   notify" mode that includes the latest value.
3. **Per-prefix watcher fan-out**: `WatchRegistry::emit` iterates all
   registered prefixes for each changed key (`O(watchers × keys)` per
   apply). For large watcher counts this could be a hot-path
   bottleneck. A trie-based prefix index would reduce this to
   `O(key_len × keys)`. Not urgent at current scale.
4. **No tests for the watch/notify flow**: the implementation is
   compile-checked and lint-clean but has no integration tests
   exercising the subscribe → write → notify → client-receive loop.
   Needs a testkit-based test that spins up a group, subscribes,
   writes, and asserts the notify arrives.
5. **diskdb prefix list is hardcoded**: `NotifyHandler` subscribes to
   three hardcoded group-0 prefixes. If the sysdata schema adds new
   prefixes, the handler must be updated manually. Consider deriving
   the prefix list from the schema.
6. **Client endpoint cache proactive refresh (item 7 of the plan)**:
   the `WatchNotifyClient` updates the topology cache on
   `not_leader_hint` (reactive), but there is no proactive refresh
   on stream reconnect. The reader task calls `topology.refresh()`
   only when the cached leader is `None`. A proactive refresh on
   every reconnect would catch leader changes that happened while
   the client was disconnected but before the stream error surfaced.
7. **Backpressure on slow watchers**: `WatchRegistry::emit` uses
   `try_send` (non-blocking). If a watcher's channel is full, the
   notify is silently dropped. The client will catch the missed
   change on the next safety-net poll, but there's no metric for
   dropped notifies. Consider a `watch_notify_dropped` counter.
