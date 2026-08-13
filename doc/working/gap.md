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

### Open
1. **Recovery scan block iterator**: the scan task iterates over
   "impacted blocks" for a Bad disk. The exact source of the block
   list (WAL replay vs. crow-tree zone index vs. a separate manifest)
   was not specified in the design doc. Current implementation uses a
   placeholder iterator that yields an empty list — needs the real
   source once the storage layer exposes it.
2. **Scan throttle / rate limit**: the design doc mentions a
   configurable scan rate limit (blocks/sec) to avoid saturating disk
   I/O on the recovery path. Not yet implemented; the scan runs
   unthrottled.
3. **Operator override API**: the Bad → Up transition is allowed in
   the state machine, but there is no admin RPC / CLI command to
   actually trigger it. Needs an admin endpoint (e.g.
   `POST /admin/disk/{id}/mark-up`).
4. **Recovery scan progress persistence key collision**: the
   `RecoveryScanProgressKey` uses `(disk_id)` as the key. If a disk
   is removed and re-added with the same id, the old progress record
   would be incorrectly resumed. Consider including a generation /
   epoch in the key.

## R78 (Watch/Notify Extension)

### Resolved
- Proto schema: `WatchSubscribe`, `WatchUnsubscribe`, `WatchNotify`,
  `WatchNotifyError`, `WatchNotifyRequest` (oneof), `WatchNotifyResponse`
  (oneof), `WatchNotify` bidi RPC on `KvService`.
- `WatchRegistry` (per-group, DashMap-backed, atomic `has_watchers`
  fast path) + `WatchCoalescer` (debounce window, default 100 ms, 0 =
  immediate emit).
- `PxLearner::apply_entry` apply-path hook: `record_chosen` after
  each engine apply, gated by `has_watchers()` for zero overhead when
  no watchers.
- `PxGroup` holds `watch_registry` + `watch_coalescer` Arcs; wired
  into the learner in `PxGroup::new`.
- Leader step-down cleanup: `step_down` + the two direct
  `become_follower` calls in `group_propose.rs` flush the coalescer
  (emit pending notifies) then clear the registry (drops watcher tx
  senders, closing client streams for clean reconnect).
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
- `NotifyConfig` in `ddb_config.rs`: `notify_enabled` (default false)
  + `notify_debounce_ms` (default 100). Wired into `main.rs`:
  spawns `NotifyHandler` + passes sync trigger to keepalive when
  enabled.
- `StopHandle::notified()` helper for long-lived tasks that don't
  follow the trigger→cycle model.

### Open
1. **Coalescer timer task**: `WatchCoalescer::record_chosen` with
   `debounce_ms > 0` buffers keys into `pending[prefix]` but the
   timer task that flushes pending sets after the debounce window is
   not fully wired (it needs a weak ref to the registry + coalescer
   to emit on flush). The default config uses `debounce_ms = 100`,
   so this path is exercised in production — currently the buffered
   keys are never emitted until the next non-debounced call. **Fix
   priority: high.** Either wire the timer task or default
   `debounce_ms = 0` (immediate emit) until the timer is implemented.
2. **Resume from slot / replay**: the design doc mentions an optional
   `from_slot` for replaying changes since a given slot before live
   tailing. Not implemented — the proto `WatchSubscribe` has no
   `from_slot` field (dropped in favor of the simpler key-list
   model). Clients rely on the safety-net poller to catch missed
   changes during reconnect gaps.
3. **WatchNotify carries keys, not values**: per the design doc, the
   notify frame carries only the changed keys (deduplicated,
   coalesced), not the values. The client re-reads via the normal
   `Get` path. This is implemented as designed, but means a notify
   storm followed by a re-read storm can amplify read load. Consider
   a future "fat notify" mode that includes the latest value.
4. **Per-prefix watcher fan-out**: `WatchRegistry::emit` iterates all
   registered prefixes for each changed key (`O(watchers × keys)` per
   apply). For large watcher counts this could be a hot-path
   bottleneck. A trie-based prefix index would reduce this to
   `O(key_len × keys)`. Not urgent at current scale.
5. **No tests for the watch/notify flow**: the implementation is
   compile-checked and lint-clean but has no integration tests
   exercising the subscribe → write → notify → client-receive loop.
   Needs a testkit-based test that spins up a group, subscribes,
   writes, and asserts the notify arrives.
6. **diskdb prefix list is hardcoded**: `NotifyHandler` subscribes to
   three hardcoded group-0 prefixes. If the sysdata schema adds new
   prefixes, the handler must be updated manually. Consider deriving
   the prefix list from the schema.
7. **Client endpoint cache proactive refresh (item 7 of the plan)**:
   the `WatchNotifyClient` updates the topology cache on
   `not_leader_hint` (reactive), but there is no proactive refresh
   on stream reconnect. The reader task calls `topology.refresh()`
   only when the cached leader is `None`. A proactive refresh on
   every reconnect would catch leader changes that happened while
   the client was disconnected but before the stream error surfaced.
8. **`watch_notify_debounce_ms` live reload**: the
   `WatchCoalescer::set_debounce_ms` method exists but is never
   called on config reload. The group's `set_from_config` would need
   to propagate the new value to the coalescer.
9. **Backpressure on slow watchers**: `WatchRegistry::emit` uses
   `try_send` (non-blocking). If a watcher's channel is full, the
   notify is silently dropped. The client will catch the missed
   change on the next safety-net poll, but there's no metric for
   dropped notifies. Consider a `watch_notify_dropped` counter.
