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
- **Resume from slot / replay — decided not to implement.** The
  design doc mentioned an optional `from_slot` for replaying changes
  since a given slot before live tailing. Dropped: the proto
  `WatchSubscribe` has no `from_slot` field (key-list model only).
  Rationale: in practice the consumer runs two channels in parallel
  — the notify stream for low-latency push, and the keepalive timer
  (`sync.sync_interval_secs`) as a safety-net poller that re-reads
  the watched prefixes every cycle. The poller already converges to
  the latest value regardless of any notify gap (reconnect, dropped
  frame, missed emit), so a slot-based replay would only add WAL /
  engine history-scan complexity on the server and slot-bookkeeping
  on the client for no correctness gain. The dual-channel flow is
  the final design, not a placeholder.
- **diskdb prefix list centralized.** The three group-0 watch
  prefixes (`/hw/dg_owner/`, `/hw/dg_bind/`, `/hw/disk/`) are now
  defined as a single constant `DISKDB_WATCH_PREFIXES` in
  `crow-protocol` (`key/diskdb.rs`), alongside the key types whose
  `TextKey::prefix_all()` they correspond to. `NotifyHandler::run`
  references this constant instead of a local `&[&[u8]]` literal.
  Adding a new keepalive-relevant group-0 key kind now requires
  updating only the constant in `crow-protocol`, not the diskdb
  handler.
- **Client endpoint cache proactive refresh on reconnect.** The
  `WatchNotifyClient` reader loop now calls `topology.refresh()`
  unconditionally at the top of every (re)connect iteration, before
  looking up the leader endpoint — instead of only refreshing when
  the cached leader was `None`. This avoids connecting to a stale
  leader cached before a leader change that happened during the
  disconnect gap: previously the client would connect to the old
  leader, get bounced back via `not_leader_hint`, then reconnect
  (or, if the old leader was down, get stuck in backoff retries
  against a dead endpoint). The reactive `not_leader_hint` path is
  kept as a fallback for leader changes that happen between the
  refresh and the stream open.
- **Fat notify (keys + values).** The `WatchNotify` proto now
  carries a `repeated bytes values` field (field 5) parallel to
  `keys`; `values[i]` is the latest value for `keys[i]` (empty
  bytes for a Delete tombstone). `WatchRegistry::emit` extracts the
  value from each `BatchOp` (`Op::Put(v)` → value bytes,
  `Op::Delete` → empty) and includes it in the notify frame. The
  client can now act on a notify without a re-read, eliminating the
  notify-storm → re-read-storm amplification. The `keys`-only
  contract is preserved as a subset (clients that only need
  invalidation ignore `values`).
- **Trie-based prefix index.** `WatchRegistry` is now backed by a
  byte-level prefix trie (`PrefixTrie` in `watch_registry.rs`)
  instead of a `DashMap<Vec<u8>, Vec<(u64, Watcher)>>`. `emit`
  walks each changed key through the trie once (`O(key_len)` per
  key), collecting watchers at every node whose prefix matches —
  reducing the per-apply cost from `O(watchers × keys)` to
  `O(key_len × keys)`. `subscribe`/`unsubscribe`/`remove_all` use
  a `parking_lot::RwLock` write lock (rare; watcher setup/teardown),
  while `emit` takes a read lock. The atomic `has_watchers` fast
  path is unchanged. No mature crate provided the exact "find all
  registered prefixes that are a prefix of this key" API, so a
  ~50-line hand-rolled trie was the natural choice.
- **Backpressure metric + critical log.** `WatchRegistry` now
  tracks a `dropped_notifies: AtomicU64` counter. When
  `try_send` fails with `Full`, the counter is incremented and a
  `tracing::error!` is emitted ("critical: watch notify dropped --
  watcher channel full, client will catch up via safety-net
  poller"). The counter is exposed via `WatchRegistry::dropped_notifies()`
  for future metrics wiring. The client still converges via the
  safety-net poller, but the drop is now observable instead of
  silent.
- **Watch registry re-wire on group rebuild.** The learner's
  `watch_registry` field changed from `OnceLock` to
  `RwLock<Option<...>>` so it can be re-wired when a group is
  rebuilt via `inherit_local_state_from` (the learner is
  `Arc::clone`-shared across rebuilds, but the new group has a new
  `WatchRegistry`). Without this, subscribes went to the new
  registry while `emit` fired on the old (orphaned) registry —
  notifies never reached the client. `inherit_local_state_from`
  now calls `set_watch_registry` after constructing the inherited
  replica.
- **End-to-end watch/notify tests.** Five integration tests in
  `tests/group_test/watch_notify_test.rs` exercise the full
  subscribe → write → notify → client-receive loop against a live
  gRPC bidi stream (no mocks):
  - `watch_notify_put_receives_key_and_value` — put a key under
    the watched prefix, assert the notify arrives with the correct
    key and value.
  - `watch_notify_delete_receives_key_with_empty_value` — delete
    a key under the watched prefix, assert the notify arrives with
    the key and an empty value (tombstone).
  - `watch_notify_non_matching_key_no_notify` — write a key
    outside the watched prefix, assert no notify arrives within a
    short window.
  - `watch_notify_batch_write_multiple_keys` — batch-write two
    keys under the watched prefix, assert both appear in the
    notify with their correct values.
  - `watch_notify_follower_redirects_to_leader` — subscribe from
    a follower, assert the follower returns an error frame with a
    non-empty `not_leader_hint`.

### Open
(none — all R78 open items resolved)
