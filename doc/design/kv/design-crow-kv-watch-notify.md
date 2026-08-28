<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Design: crow-kv Watch/Notify Extension

Depends on: [`design-crow-kv-rpc.md`](design-crow-kv-rpc.md) §3, §5, §6; [`design-crow-kv.md`](design-crow-kv.md) §3; [`design-crow-kv-state-machine.md`](design-crow-kv-state-machine.md) §2
Satisfies: [`design-crow-diskdb.md`](../diskdb/design-crow-diskdb.md) §8 (Follow-up — group-0 notify/watch), §15

This document covers the crow-kv watch/notify extension, the diskdb
server-side notify handler, and the client endpoint cache proactive
refresh that together replace fixed-interval polling as the primary
change-detection mechanism for group-0 sysdata. The notify is a
**client-pulled `WatchNotify` bidi stream**: diskdb opens the stream to
the group-0 leader and subscribes to prefixes, and the leader pushes
notifies over that stream. No separate notify-endpoint registration is
needed for notify delivery. Polling stays as a safety net at a raised
interval. Architecture decisions and rationale live here; the root
design context is `design-crow-diskdb.md` §8 and `design-crow-kv-rpc.md`
§3 (LearnerStream pattern).

## Table of Contents

- [1. WatchNotify crow-rpc Bidi Stream](#1-watchnotify-crow-rpc-bidi-stream)
  - [1.1 Why](#11-why)
  - [1.2 Schema](#12-schema)
  - [1.3 Server-side handler](#13-server-side-handler)
- [2. Watch Registry + Apply-Path Trigger](#2-watch-registry--apply-path-trigger)
  - [2.1 Why](#21-why)
  - [2.2 WatchRegistry](#22-watchregistry)
  - [2.3 PxLearner integration](#23-pxlearner-integration)
  - [2.4 PxGroup integration](#24-pxgroup-integration)
  - [2.5 Why not the proposal path](#25-why-not-the-proposal-path)
- [3. Coalescing (deferred)](#3-coalescing-deferred)
- [4. WatchNotify Client](#4-watchnotify-client)
  - [4.1 Why](#41-why)
  - [4.2 WatchNotifyClient](#42-watchnotifyclient)
- [5. diskdb Notify Handler + Polling Safety Net](#5-diskdb-notify-handler--polling-safety-net)
  - [5.1 Why](#51-why)
  - [5.2 Trigger extension](#52-trigger-extension)
  - [5.3 KeepAlive changes](#53-keepalive-changes)
  - [5.4 Notify handler task](#54-notify-handler-task)
  - [5.5 Polling safety net](#55-polling-safety-net)
- [6. Client Endpoint Cache Proactive Refresh](#6-client-endpoint-cache-proactive-refresh)
  - [6.1 Why](#61-why)
  - [6.2 Design](#62-design)
- [7. Configuration](#7-configuration)
  - [7.1 diskdb config](#71-diskdb-config)
  - [7.2 crow-diskdb-client config](#72-crow-diskdb-client-config)
- [8. Module Structure](#8-module-structure)
- [9. Server Wiring](#9-server-wiring)
- [10. Test Design](#10-test-design)

---

## 1. WatchNotify crow-rpc Bidi Stream

### 1.1 Why

crow-kv has no watch/notify capability without this extension. The only
change-detection mechanism available to external components is polling
(prefix scans of group 0). A push mechanism requires a long-lived
stream from the group-0 leader to each watcher, fired when a watched
prefix is written. The `LearnerStream` bidi pattern is the existing
precedent for a long-lived crow-rpc bidi stream multiplexing frames between
a leader and a peer. `WatchNotify` follows the same shape but serves
client-to-leader watch subscriptions rather than replica-to-leader
consensus traffic.

### 1.2 Schema

`lib/crow-kv/src/rpc/fbs/kv_client.fbs` carries the watch/notify messages
and one bidi RPC on `KvService`:

```fbs
// Client-to-leader watch subscription. The client opens a WatchNotify
// bidi stream to a node, sends WatchSubscribe frames for each
// (group_id, prefix) it wants to watch, and receives WatchNotify frames
// when watched keys change. If the node is not the leader of the
// subscribed group_id, it returns a WatchNotifyError with
// not_leader_hint and the client reconnects to the leader.
message WatchSubscribe {
  uint32 version   = 1;
  uint64 group_id  = 2;
  bytes  prefix    = 3;  // watch all keys with this byte prefix
}

message WatchUnsubscribe {
  uint64 group_id  = 1;
  bytes  prefix    = 2;
}

// Pushed from leader to watcher when a watched key changes. Carries
// the changed keys and their latest values so the watcher can act
// without a re-read. slot = the apply slot of the triggering write.
message WatchNotify {
  uint64 group_id      = 1;
  bytes  prefix        = 2;  // which watched prefix matched
  repeated bytes keys  = 3;  // changed keys (deduplicated)
  uint64 slot          = 4;
  repeated bytes values = 5; // values[i] = latest value for keys[i]
                              // (empty bytes for a Delete tombstone)
}

message WatchNotifyError {
  uint64 group_id        = 1;
  string not_leader_hint = 2;  // empty for non-leader errors
  string error           = 3;
}

message WatchNotifyRequest {
  oneof frame {
    WatchSubscribe   subscribe   = 1;
    WatchUnsubscribe unsubscribe = 2;
  }
}

message WatchNotifyResponse {
  oneof frame {
    WatchNotify       notify = 1;
    WatchNotifyError  error  = 2;
  }
}
```

Add to the `KvService` service:

```fbs
  rpc WatchNotify(stream WatchNotifyRequest) returns (stream WatchNotifyResponse);
```

- **Field numbers are append-only** — matches the RPC design's
  compatibility rule (`design-crow-kv-rpc.md` §5).
- **`bytes` fields** — `prefix` and `keys` are raw key bytes as stored
  in the engine. For group-0 sysdata these are UTF-8 text paths (e.g.
  `/hw/disk/1/100/5/abcd`) because `HardwareClient` puts keys via
  `key.to_path().as_bytes()` and scans via `prefix.as_bytes()`. For
  data groups the keys are binary-encoded (`BinaryKey::to_bytes()`). The
  schema is encoding-agnostic; the subscriber and the engine agree on the
  encoding because they both use the same key types. No `Bytes` mapping
  needed — group-0 keys are text paths < 128 bytes; `Vec<u8>` is fine.
- **Fat notify (keys + values)** — `values[i]` is the latest value for
  `keys[i]` (empty bytes for a `Delete` tombstone). `WatchRegistry::emit`
  extracts the value from each `BatchOp` (`Op::Put(v)` → value bytes,
  `Op::Delete` → empty) and includes it in the notify frame. The client
  can act on a notify without a re-read, eliminating the
  notify-storm → re-read-storm amplification. The `keys`-only contract
  is preserved as a subset (clients that only need invalidation ignore
  `values`).

### 1.3 Server-side handler

Implemented in `lib/crow-kv/src/rpc/kv_service.rs`, mirroring
`learner_stream`:

```rust
type WatchNotifyStream =
    Pin<Box<dyn Stream<Item = Result<WatchNotifyResponse, Status>> + Send + 'static>>;

async fn watch_notify(
    &self,
    request: Request<Streaming<WatchNotifyRequest>>,
) -> Result<Response<Self::WatchNotifyStream>, Status>
```

a. On stream open, allocate an `mpsc::channel::<Result<WatchNotifyResponse, Status>>(64)`
   (`tx`, `rx`) — the outbound frame queue, same capacity as
   `LearnerStream`.
b. Spawn a task that reads inbound frames in a loop:
   - `WatchSubscribe { group_id, prefix }` — look up the `PxGroup` for
     `group_id` via `self.store`. If this node is not the leader of
     that group (`local_replica.is_leader()`), send
     `WatchNotifyError { not_leader_hint }` and `continue` (do not
     close the stream — the client may subscribe to other groups). If
     leader, register a `Watcher { prefix, tx }` in the group's
     `WatchRegistry` and record the `watcher_id` for cleanup on stream
     end.
   - `WatchUnsubscribe { group_id, prefix }` — remove the matching
     watcher from the group registry.
c. On inbound stream end (client disconnect or error), remove all
   watchers registered by this stream from their group registries
   (`registry.remove_all(&watcher_ids)`).
d. The outbound stream is `ReceiverStream::new(rx)` boxed, returned to
   the crow-rpc server.

- **Leader check** — uses `local_replica.is_leader()`, the same atomic
  role check the propose path uses for its leadership gate. A
  non-leader node returns `not_leader_hint` per-subscribe rather than
  closing the stream, so a client subscribed to multiple groups gets
  hints only for the groups this node doesn't lead.
- **Leader change mid-stream** — on step-down, the old leader clears
  its `WatchRegistry` (see §2.4). Dropping all `Watcher` tx senders
  closes the clients' outbound streams. The client reconnects to the
  new leader and re-subscribes. The safety-net polling covers the gap.
- **`not_leader_hint` source** — `PxGroup::leader_endpoint()` returns
  the believed leader's endpoint (or empty if unknown). The handler
  sends this as the hint, matching the `KvResponse.not_leader_hint`
  pattern in the unary RPC path.

## 2. Watch Registry + Apply-Path Trigger

### 2.1 Why

The notify trigger fires on the leader's **apply path** (after a
value is Paxos-chosen AND applied to the engine), not on the proposal
path. This is the etcd model: watchers are fed from the apply stream,
not the proposal path. The proposal path only fires for slots the
leader proposes itself; slots the leader learns via heartbeat catch-up
or repair never trigger a notify on the proposal path. The apply path
fires for **every** chosen slot on **every** replica, covering all
three entry points:
- `Learner::learn` — sync path: leader proposals + follower learn.
- `spawn_learn_chosen`'s spawned task — async path: deferred engine
  apply.
- `apply_loop_task` — background catch-up: slots learned via heartbeat
  `known_commit_slot` advance or gap-fill.

The apply path's central function is `PxLearner::apply_entry`, which
decodes `entry.payload` via `Batch::decode` into `Batch { ops: Vec<BatchOp> }`.
The changed keys are already extracted there, so the notify trigger
incurs no extra decode. The registry therefore lives on `PxLearner` (set
via a setter at group construction) so the trigger fires from one hook
point with no cross-struct lookup.

### 2.2 WatchRegistry

`lib/crow-kv/src/cluster/watch_registry.rs`:

```rust
/// One watcher: a prefix + an outbound channel to push notify frames.
pub(crate) struct Watcher {
    pub prefix: Vec<u8>,
    pub tx: mpsc::Sender<Result<WatchNotifyResponse, Status>>,
}

/// Per-group watch registry. Wired into `PxLearner` via
/// `set_watch_registry`; the learner's `apply_entry` calls `emit`
/// after each successful engine apply, gated by `has_watchers`.
pub(crate) struct WatchRegistry {
    // Byte-level prefix trie (PrefixTrie): subscribe/unsubscribe/
    // remove_all take a parking_lot::RwLock write lock (rare;
    // watcher setup/teardown), emit takes a read lock. emit walks
    // each changed key through the trie once (O(key_len) per key),
    // collecting watchers at every node whose prefix matches.
    trie: PrefixTrie,
    next_id: AtomicU64,
    /// Atomic fast-path flag: true iff at least one watcher is
    /// registered. The apply path checks this (one Acquire load)
    /// before touching the trie — zero overhead when no watchers.
    has_watchers: AtomicBool,
    /// Counter incremented when try_send fails with Full; exposed
    /// via dropped_notifies() for metrics wiring. A critical log is
    /// emitted on each drop.
    dropped_notifies: AtomicU64,
}

impl WatchRegistry {
    pub fn new() -> Self;
    /// Register a watcher for `prefix`. Returns the `watcher_id` for
    /// later removal. Sets `has_watchers = true`.
    pub fn subscribe(&self, prefix: Vec<u8>, tx: mpsc::Sender<...>) -> u64;
    /// Remove a specific watcher by `(prefix, watcher_id)`. Updates
    /// `has_watchers` if the registry becomes empty.
    pub fn unsubscribe(&self, prefix: &[u8], watcher_id: u64);
    /// Remove all watchers whose `watcher_id` is in the list (stream-
    /// end cleanup). Updates `has_watchers`.
    pub fn remove_all(&self, watcher_ids: &[u64]);
    /// Clear all watchers (leader step-down). Drops all tx senders,
    /// closing client streams. Sets `has_watchers = false`.
    pub fn clear(&self);
    /// For a set of changed keys (from `Batch::decode`), find matching
    /// prefixes and enqueue notify frames. Non-blocking: uses
    /// `try_send`.
    pub fn emit(&self, group_id: u64, slot: u64, changed: &[BatchOp]);
    /// True if at least one watcher is registered. Atomic load — the
    /// apply-path fast path.
    pub fn has_watchers(&self) -> bool;
    pub fn dropped_notifies(&self) -> u64;
}
```

- **`has_watchers` atomic fast path** — the apply-path hook checks
  `registry.has_watchers()` (one `Acquire` load of an `AtomicBool`)
  before decoding or matching. When no watchers are registered (the
  common case in tests and non-diskdb clusters), the cost is one
  predicted-not-taken branch, cheaper than a trie-emptiness scan and
  the true zero-overhead gate. `has_watchers` is set to `true` on the
  first `subscribe` and recomputed on `unsubscribe` / `remove_all` /
  `clear`.
- **Trie-based prefix index** — `WatchRegistry` is backed by a
  byte-level prefix trie (`PrefixTrie`) instead of a
  `DashMap<Vec<u8>, Vec<(u64, Watcher)>>`. `emit` walks each changed
  key through the trie once (`O(key_len)` per key), collecting
  watchers at every node whose prefix matches, reducing the per-apply
  cost from `O(watchers × keys)` to `O(key_len × keys)`. No mature
  crate provided the exact "find all registered prefixes that are a
  prefix of this key" API, so a hand-rolled trie was the natural
  choice.
- **`emit` matching** — for each `BatchOp` in the decoded batch, walk
  the key through the trie and check `op.key.starts_with(&entry.prefix)`.
  Group matches by prefix, build one `WatchNotify { prefix, keys, values, slot }`
  per prefix per watcher, send via `tx.try_send` (non-blocking — if the
  watcher's channel is full, drop the notify; the safety-net polling
  covers missed notifies).
- **`try_send` not `send().await`** — the apply path must not block on
  a slow watcher. A full channel means the watcher is lagging; dropping
  the notify is correct (the safety-net polling will catch up). The
  drop is observable: `dropped_notifies` is incremented and a
  `tracing::error!` is emitted ("critical: watch notify dropped --
  watcher channel full, client will catch up via safety-net poller").
- **Delete ops** — `BatchOp { op: Op::Delete, key }` still carries a
  key; a delete on a watched prefix notifies the watcher with the key
  and an empty value (tombstone). The watcher re-reads and sees the
  deletion.

### 2.3 PxLearner integration

`PxLearner` (`lib/crow-kv/src/paxos/learner.rs`) holds:

```rust
/// Optional per-group watch registry. Set when the group is
/// constructed; None in tests that don't use watch/notify. The
/// apply-path hook in `apply_entry` checks `has_watchers()` before
/// touching the registry. RwLock<Option<...>> (not OnceLock) so it
/// can be re-wired when a group is rebuilt via
/// `inherit_local_state_from` — the learner is Arc::clone-shared
/// across rebuilds, but the new group has a new WatchRegistry.
watch_registry: RwLock<Option<(u64, Arc<WatchRegistry>)>>,  // (group_id, registry)
```

- **`(group_id, registry)` tuple** — `PxLogEntry` has no `group_id`
  field, and `apply_entry` takes only `(slot, &payload)`. The
  `group_id` is stored alongside the registry so `emit` can populate
  `WatchNotify.group_id` without threading `group_id` through every
  call site.
- **Set via `set_watch_registry(group_id, Arc<WatchRegistry>)`** —
  called once during `PxGroup` construction (after the learner is
  created) and again in `inherit_local_state_from` after constructing
  the inherited replica. Tests that don't use watch/notify leave it
  unset (`has_watchers()` returns false via the `None` fast path).
- **`apply_entry` hook** — after `self.engine.apply(slot, &batch).await`
  succeeds, before the method returns:

```rust
if let Some((group_id, registry)) = self.watch_registry.read().unwrap().as_ref() {
    if registry.has_watchers() {
        registry.emit(*group_id, slot, &batch.ops);
    }
}
```

  The `batch` is already decoded at the top of `apply_entry`, so the
  keys are available with no extra decode. When `has_watchers()` is
  false, the cost is one `RwLock` read + one `AtomicBool` load — zero
  overhead when unused.
- **Fires on ALL apply paths** — `apply_entry` is called from `learn`
  (sync), `spawn_learn_chosen` (async), and `apply_loop_task`
  (catch-up). All three paths trigger the hook, covering leader
  proposals, follower learn, heartbeat catch-up, and gap-fill repair:
  the etcd model.
- **Followers don't emit** — followers also call `apply_entry`, but
  their `WatchRegistry` is empty (cleared on step-down, or never
  populated if they were never leader). `has_watchers()` returns false
  → no emit. Only the leader (which holds the client streams) emits.
  The registry's emptiness is the gate, so no explicit leader check is
  needed in the hook.
- **Re-wire on group rebuild** — without the `RwLock<Option<...>>`
  (vs `OnceLock`), subscribes would go to the new registry while
  `emit` fired on the old (orphaned) registry, and notifies never reached
  the client. `inherit_local_state_from` calls `set_watch_registry`
  after constructing the inherited replica.

### 2.4 PxGroup integration

`PxGroup` (`lib/crow-kv/src/cluster/group.rs`) holds:

```rust
pub(crate) watch_registry: Arc<WatchRegistry>,
```

- Constructed in `PxGroup::new` as `Arc::new(WatchRegistry::new())`.
  Wired into the learner via
  `local_replica.learner.set_watch_registry(group_id, Arc::clone(&watch_registry))`
  right after construction.
- **Cleared on step-down** — the leader step-down path is
  `PxGroup::step_down`, called for `HigherTerm` / `LeaseUnrenewable` /
  `Admin` reasons. `self.watch_registry.clear()` runs at the end of
  `step_down` (after `become_follower`). This drops all `Watcher` tx
  senders, closing the clients' outbound streams (clean reconnect).
- **Also cleared on propose-path direct step-down** — the propose path
  has two direct `become_follower` calls that bypass `step_down`:
  one on a higher term during prepare and one on a higher term during
  accept. Both have `&self` (`PxGroup`), so `self.watch_registry.clear()`
  runs before each `replica.become_follower(...)` call. This covers the
  case where the leader discovers a higher term mid-proposal and steps
  down without going through `step_down`.
- **Not cleared on `shutdown`** — `shutdown` drops the whole `PxGroup`,
  which drops the `Arc<WatchRegistry>`, which drops all watchers.

### 2.5 Why not the proposal path

The apply-path hook was chosen over a proposal-path hook because the
proposal path has a critical gap: it only fires for slots the leader
**proposes itself**. Three classes of chosen slots never trigger a
notify on the proposal path:

- **Heartbeat catch-up slots** — when the leader advances
  `known_commit_slot` via heartbeat and the background `apply_loop_task`
  applies slots the leader accepted as a follower before winning the
  election. These slots are real writes to real keys; watchers must be
  notified.
- **Repair slots** — gap-fill repair calls `fan_out_chosen_notice` on
  the repair path, not the propose path. A repaired slot is a real
  write; watchers must be notified.
- **Foreign-value retry** — on the apply path, both the foreign value
  and the retried client value fire `apply_entry` → both notify. This
  is correct and automatic — no special handling needed.

The apply-path hook covers all three with one code point. The cost is
that `apply_entry` runs on followers too (where the registry is empty),
but the `has_watchers()` atomic check makes this a single predicted-
not-taken branch, zero overhead.

## 3. Coalescing (deferred)

Burst writes to the same prefix (e.g. a batch of disk-status updates
via `batch_write`) generate one notify per write. A debounce window
would coalesce writes to the same prefix into one notify, reducing
watcher wakeup + re-read amplification under burst load.

The watch/notify extension ships without coalescing. `apply_entry`
calls `registry.emit` directly (one notify per changed key per matching
prefix). Coalescing is deferred to a follow-up requirement (see
`doc/backlog/backlog.md` — watch/notify coalescing / debounce). The
safety-net poller covers any notify-drop scenarios; the missing
coalescing is a load optimization, not a correctness gap.

## 4. WatchNotify Client

### 4.1 Why

diskdb (and future clients) need a reusable client that opens the
`WatchNotify` stream to the group leader, subscribes to prefixes,
and delivers notify frames via a channel: the client-side mirror of
§1's server handler.

### 4.2 WatchNotifyClient

`lib/crow-kv-client/src/watch_notify.rs`:

```rust
/// A live watch subscription. Dropping it unsubscribes and closes the
/// stream.
pub struct WatchSubscription {
    /// Notify frames for this subscription. Receiver end of an mpsc;
    /// the client reads `WatchNotify` frames here.
    pub notify_rx: mpsc::Receiver<WatchNotify>,
    /// Internal handle to unsubscribe on drop.
    inner: WatchSubscriptionInner,
}

impl Drop for WatchSubscription {
    fn drop(&mut self) { /* send WatchUnsubscribe, or drop the stream */ }
}

/// Client for the crow-kv `WatchNotify` bidi stream.
pub struct WatchNotifyClient {
    kv: Arc<CrowkvClient>,
}

impl WatchNotifyClient {
    pub fn from_shared(kv: Arc<CrowkvClient>) -> Self;

    /// Subscribe to `(store_id, group_id, prefix)`. Opens a bidi stream
    /// to the group leader (discovered via the topology cache), sends a
    /// `WatchSubscribe` frame, and returns a `WatchSubscription` whose
    /// `notify_rx` yields `WatchNotify` frames.
    ///
    /// On leader-change (stream closes), the client automatically
    /// reconnects to the new leader and re-subscribes; the caller's
    /// `notify_rx` stays open across the reconnect. Missed notifies
    /// during the reconnect gap are caught by the caller's safety-net
    /// polling.
    pub async fn subscribe(
        &self,
        store_id: u64,
        group_id: u64,
        prefix: &[u8],
    ) -> Result<WatchSubscription>;
}
```

a. `subscribe` resolves the group leader endpoint via `CrowkvClient`'s
   topology cache (`kv.topology.leader(store_id, group_id)`, falling
   back to `kv.topology.refresh()` on cache miss — same mechanism as
   `Put`/`Get` via `resolve_leader`).
b. Gets a crow-rpc connection from the shared `ConnectionPool`
   (`kv.pool.get(&endpoint)`) — reuses the existing connection pool,
   no separate channel management.
c. Opens a `KvServiceClient::new(channel).watch_notify(...)` bidi
   stream to that endpoint.
d. Sends `WatchSubscribe { version: 1, group_id, prefix }`.
e. Spawns a reader task that loops on the inbound stream: on
   `WatchNotify` frame, forwards to the subscriber's `notify_rx`; on
   `WatchNotifyError { not_leader_hint }`, refreshes topology
   (`kv.topology.set_leader(store_id, group_id, hint)`), reconnects
   to the new leader, re-subscribes, and continues. On stream
   error/disconnect, same reconnect logic.
f. `WatchSubscription::drop` sends `WatchUnsubscribe` (if the stream
   is still open) and aborts the reader task.

- **`store_id` required** — the topology cache is keyed by
  `(store_id, group_id)`. Group 0 is `(0, 0)` (`CrowkvClient::SYSTEM_STORE`
  / `SYSTEM_GROUP`). The `subscribe` signature takes `store_id` so the
  client can resolve the leader endpoint.
- **One stream per subscription** — simpler than multiplexing multiple
  prefixes over one stream (each subscription is independent; a diskdb
  instance typically watches 3 prefixes). The schema already supports
  multiple `WatchSubscribe` frames on one stream, so a future
  optimization can multiplex subscriptions over a shared stream with
  no schema change.
- **Reconnect backoff** — capped exponential (50 ms → 2 s), matching
  `LearnerStream`'s reconnect policy (`design-crow-kv-rpc.md` §6).
- **Proactive topology refresh on reconnect** — the reader loop calls
  `topology.refresh()` unconditionally at the top of every (re)connect
  iteration, before looking up the leader endpoint, instead of only
  refreshing when the cached leader was `None`. This avoids connecting
  to a stale leader cached before a leader change that happened during
  the disconnect gap. The reactive `not_leader_hint` path is kept as a
  fallback for leader changes that happen between the refresh and the
  stream open.
- **`ConnectionPool` reuse** — `pool` is `pub(crate)` on `CrowkvClient`;
  `WatchNotifyClient` is in the same crate, so it accesses the pool
  directly. No new connection management code.

## 5. diskdb Notify Handler + Polling Safety Net

### 5.1 Why

diskdb's `KeepAlive` sync loop (`liveness/keepalive.rs`) wakes on a
fixed timer (`Trigger::TimerFn`). To trigger an immediate sync on
notify, the loop must also wake on an external signal. The `Trigger`
enum has an `Event(Notify)` variant but not a hybrid timer+event. The
notify handler (a `WatchNotifyClient` reader) must wake the keepalive
without going through the timer.

### 5.2 Trigger extension

`Trigger` in `app/crow-diskdb/src/bg_task.rs`:

```rust
pub enum Trigger {
    Timer(Duration),
    TimerFn(Box<dyn Fn() -> Duration + Send + Sync>),
    Event(Arc<tokio::sync::Notify>),
    /// Wake on either a timer tick (dynamic interval from config) OR
    /// an external notify signal. Used by keepalive when
    /// `notify_enabled` is true: the timer is the safety-net polling
    /// interval, the notify is woken by the WatchNotify handler.
    TimerOrEvent {
        interval_fn: Box<dyn Fn() -> Duration + Send + Sync>,
        notify: Arc<tokio::sync::Notify>,
    },
}
```

`wait_trigger`:

```rust
Trigger::TimerOrEvent { interval_fn, notify } => {
    tokio::select! {
        () = tokio::time::sleep(interval_fn()) => {}
        () = notify.notified() => {}
    }
}
```

- **Backward-compatible** — existing tasks keep `Timer`/`TimerFn`/
  `Event`; only keepalive uses `TimerOrEvent` when `notify_enabled`.
- **`select!` is race-safe** — both branches can fire; `select!` polls
  both and picks one. No panic if both are ready.

### 5.3 KeepAlive changes

In `app/crow-diskdb/src/liveness/keepalive.rs`:

a. Field `sync_trigger: Option<Arc<tokio::sync::Notify>>` — `Some` when
   `notify_enabled`, `None` otherwise (keeps the struct usable in tests
   without notify).
b. Builder method `.with_sync_trigger(notify: Arc<Notify>) -> Self`.
c. In `trigger()` (the `BackgroundTask` impl): if `sync_trigger` is
   `Some`, return `Trigger::TimerOrEvent { interval_fn, notify:
   sync_trigger }`; else fall back to the existing `TimerFn` path.
d. Method `pub fn trigger_now(&self)` — calls
   `self.sync_trigger.notify_one()` if set. Called by the notify
   handler on each `WatchNotify` frame.
e. **`rpc_endpoint` already populated** — `main.rs` calls
   `.with_rpc_endpoint(config.load().server.listen_addr.clone())`,
   and `keepalive.rs` passes `self.rpc_endpoint` to `heartbeat_diskdb`
   on every tick. The endpoint registration (item 1 of the original
   scope) is already done; no change needed here.

### 5.4 Notify handler task

`app/crow-diskdb/src/liveness/notify.rs`:

```rust
/// diskdb-side watch/notify handler. Subscribes to group-0 prefixes
/// and wakes the keepalive sync loop on notify.
pub struct NotifyHandler {
    watch: WatchNotifyClient,
    keepalive_trigger: Arc<tokio::sync::Notify>,
    /// (store_id, group_id, prefix) triples to subscribe to.
    subscriptions: Vec<(u64, u64, Vec<u8>)>,
}

impl NotifyHandler {
    pub fn new(
        watch: WatchNotifyClient,
        keepalive_trigger: Arc<tokio::sync::Notify>,
    ) -> Self;

    /// Open subscriptions and loop on notify frames. Each frame wakes
    /// the keepalive via `keepalive_trigger.notify_one()`. Runs until
    /// the stop signal.
    pub async fn run(self, stop: tokio::sync::Notify);
}
```

a. On `run`, subscribe to the group-0 prefixes diskdb cares about.
   The prefixes are **text-encoded** (matching the engine's group-0
   key encoding — `HardwareClient` puts/scans text paths), centralized
   as `DISKDB_WATCH_PREFIXES` in `crow-protocol` (`key/diskdb.rs`):
   - `/hw/dg_owner/` (ownership map — global; `list_owners` scans
     globally and filters to this instance)
   - `/hw/dg_bind/` (binding map — global; `list_binds` scans globally)
   - `/hw/disk/` (disk metadata + status — global; `observe_disks`
     scans per owned disk-group, but the notify is a coarse trigger)
   All three use `(G0_STORE, G0_GROUP)` = `(0, 0)`. Adding a new
   keepalive-relevant group-0 key kind requires updating only the
   constant in `crow-protocol`, not the diskdb handler.
b. For each subscription, spawn a reader loop: on `WatchNotify` frame,
   call `keepalive_trigger.notify_one()` (wakes keepalive →
   `KeepAlive::tick()` → re-reads the changed keys from group 0).
c. The notify is a **trigger only** — diskdb re-reads the actual values
   via the normal sync path (`observe_ownership`, `observe_disks`).
   This keeps the notify payload small and avoids a duplicated read
   path.
d. On subscription drop/reconnect, the `WatchNotifyClient` handles
   reconnect internally (§4.2); the handler just keeps reading.
e. On `stop.notified()`, drop all subscriptions (which closes the
   streams) and exit.

- **Global prefixes, not per-node** — `list_owners()` / `list_binds()`
  scan the global `/hw/dg_owner/` / `/hw/dg_bind/` prefixes and filter
  to `instance_id == self.instance_id`. Watching the global prefix
  means the keepalive wakes on any ownership/bind change anywhere, then
  re-scans and filters to this instance. An irrelevant change (another
  instance's ownership) causes a cheap no-op re-scan. Per-node prefixes
  would be more selective but require knowing `rack_id`/`node_id`
  before the first sync. The diskdb discovers its node identity from
  the ownership map, not from config, so global prefixes avoid this
  bootstrapping dependency.
- **Not a `BackgroundTask`** — `NotifyHandler` is a long-lived task
  with its own `run` method, not a `BgRunner` cycle task. It is spawned
  directly in `main.rs` alongside the bg runner, with the same stop
  signal. This avoids forcing the notify loop into the
  trigger→cycle→trigger model (it is event-driven, not periodic).
- **`StopHandle::notified()` helper** — supports long-lived tasks that
  don't follow the trigger→cycle model.

### 5.5 Polling safety net

When `notify_enabled` is true:
- The keepalive timer interval (from `sync.sync_interval_secs`) is the
  safety-net polling interval. The config default rises (e.g. 60 s)
  when notify is on; the operator sets it explicitly.
- The timer catches any missed notifies (reliability fallback): if a
  notify is dropped (full channel) or the WatchNotify stream is
  disconnected, the next timer tick still syncs.
- When `notify_enabled` is false (default), behavior is unchanged —
  fixed-interval polling at `sync_interval_secs` (default 10 s), no
  notify handler, no `WatchNotifyClient`.

## 6. Client Endpoint Cache Proactive Refresh

### 6.1 Why

The `DiskdbClient` endpoint cache (`lib/crow-diskdb-client/src/client.rs`)
refreshes **on demand**: at startup, on cache miss, and on error-retry
(`refresh_endpoints` reads `read_all_diskdb_instances`). When a diskdb
instance moves (endpoint change in its service-registry record at
`/srv/diskdb/<instance_id>`), a client holding the old endpoint cache
entry fails one `allocate_blocks`/`free_blocks` attempt, triggers
`refresh_endpoints` (on-demand safety net), and retries. This section
extends that to **proactive** refresh: the client subscribes to the
`/srv/diskdb/` prefix via `WatchNotifyClient`; on a notify (instance
register/deregister/move), it refreshes the affected cache entries so
the next call routes to the new endpoint without a failed attempt +
retry. The on-demand refresh remains as the safety net; proactive
refresh is an optimization, not a correctness requirement.

### 6.2 Design

Add a `WatchNotifyClient` subscription to `DiskdbClient`:

a. In `DiskdbClient::new` (or a new `with_watch_notify` builder), if
   a `WatchNotifyClient` is provided, subscribe to
   `(G0_STORE, G0_GROUP, "/srv/diskdb/")`.
b. Spawn a reader task: on `WatchNotify` frame, call
   `self.refresh_endpoints()` (the existing method). The notify is a
   coarse trigger — the refresh re-reads ALL diskdb instances from
   group 0 and updates the cache. This is the same "trigger, not
   transport" model as the diskdb notify handler (§5.4).
c. The reader task runs for the client's lifetime. On stream close
   (leader change), `WatchNotifyClient` reconnects automatically
   (§4.2); the reader task just keeps reading.
d. If no `WatchNotifyClient` is provided (default), the client uses
   on-demand refresh only (unchanged behavior).

- **`/srv/diskdb/` prefix** — the service-registry instance key path
  is `/srv/diskdb/<instance_id>` (`InstanceKey::to_path()`). The global
  prefix `/srv/diskdb/` covers all diskdb instances.
  `ServiceRegistryClient::read_all_diskdb_instances` scans this prefix.
- **On-demand refresh stays as safety net** — if a notify is missed
  (stream disconnected, channel full), the next `allocate_blocks` to a
  moved instance fails once, triggers `refresh_endpoints`, and
  retries. The cache is eventually consistent either way.

## 7. Configuration

### 7.1 diskdb config

`app/crow-diskdb/src/ddb_config.rs`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyConfig {
    /// static: enable watch/notify (default: false). When true,
    /// diskdb subscribes to group-0 prefixes and the keepalive timer
    /// serves as a safety-net poller at `sync.sync_interval_secs`.
    pub notify_enabled: bool,
}

impl Default for NotifyConfig {
    fn default() -> Self {
        Self { notify_enabled: false }
    }
}
```

- `pub notify: NotifyConfig` on `DdbConfig`.
- `validate()`: if `notify_enabled` and `sync.sync_interval_secs == 0`,
  error (same existing check; no new constraint).

### 7.2 crow-diskdb-client config

An optional `watch_notify` toggle on `DiskdbClient`'s config (or
builder):

```rust
/// When true, the client subscribes to `/srv/diskdb/` and proactively
/// refreshes the endpoint cache on notify. Default: false (on-demand
/// refresh only).
pub watch_notify_enabled: bool,
```

- **Default false** — proactive refresh is an optimization; the default
  ships with on-demand refresh. Operators opt in per client.

## 8. Module Structure

```
lib/crow-kv/src/
  rpc/
    fbs/kv_client.fbs            # WatchNotify messages + RPC
    kv_service.rs               # watch_notify handler
  cluster/
    watch_registry.rs           # WatchRegistry, Watcher, PrefixTrie
    group.rs                    # watch_registry field, wire into learner, clear on step_down
    group_election.rs           # clear registry in step_down
    group_propose.rs            # clear registry before direct become_follower calls
    mod.rs                      # export watch_registry
  paxos/
    learner.rs                  # watch_registry RwLock<Option> + set_* + apply-path hook

lib/crow-kv-client/src/
  watch_notify.rs               # WatchNotifyClient, WatchSubscription
  lib.rs                        # export watch_notify

app/crow-diskdb/src/
  liveness/
    notify.rs                   # NotifyHandler
    keepalive.rs                # sync_trigger, trigger_now, TimerOrEvent trigger
    mod.rs                      # export notify
  bg_task.rs                    # Trigger::TimerOrEvent
  ddb_config.rs                 # NotifyConfig
  main.rs                       # wire NotifyHandler when notify_enabled

lib/crow-diskdb-client/src/
  client.rs                     # optional WatchNotifyClient, proactive refresh on notify
  lib.rs                        # re-export WatchNotifyClient
```

## 9. Server Wiring

`app/crow-diskdb/src/main.rs` startup sequence (additions):

1. After building `keepalive`: if
   `config.load().notify.notify_enabled`, create an
   `Arc<tokio::sync::Notify>` (`sync_trigger`), call
   `keepalive.with_sync_trigger(Arc::clone(&sync_trigger))`.
2. After building the bg runner: if `notify_enabled`, construct
   `WatchNotifyClient::from_shared(Arc::clone(&kv_client))` and
   `NotifyHandler::new(watch, sync_trigger)`. Spawn
   `notify_handler.run(stop_notify)` as a background task with the
   same stop signal as the bg runner.
3. On shutdown: the stop signal aborts the notify handler (its `run`
   loop exits on `stop.notified()`); the bg runner stops keepalive +
   compaction; crow-rpc + HTTP servers drain.

## 10. Test Design

### Unit tests (UT)

**WatchRegistry** (`lib/crow-kv/src/cluster/watch_registry.rs`):
- `subscribe_emit` — register a watcher for prefix `/hw/disk/`, emit
  a changed key `/hw/disk/1/2/3/abcd`, assert the watcher channel
  receives a `WatchNotify` with the key. Assert a key outside the
  prefix (`/hw/rack/1`) does not notify.
- `multiple_watchers_same_prefix` — two watchers for the same prefix;
  emit one key; assert both receive the notify.
- `unsubscribe` — subscribe, unsubscribe by `watcher_id`, emit; assert
  no notify is received (channel stays empty).
- `remove_all` — subscribe 3 prefixes via one stream, `remove_all` by
  the recorded ids, emit to all 3; assert no notifies.
- `emit_full_channel_drops` — subscribe with a capacity-1 channel,
  fill it, emit 3 keys; assert no panic (`try_send` drops silently); the
  watcher misses notifies (safety-net covers this).
- `has_watchers` — false before subscribe, true after, false after
  `unsubscribe` + `clear`.
- `clear` — subscribe 3 watchers, `clear()`, assert `has_watchers`
  false and all channels are closed (sender dropped).
- `delete_op_notifies` — emit a `BatchOp { op: Op::Delete, key }`
  matching the prefix; assert the watcher receives a notify with the
  key.

**WatchNotify server handler** (`lib/crow-kv/src/rpc/kv_service.rs`):
- `subscribe_not_leader` — open a stream to a non-leader node, send
  `WatchSubscribe` for a group it does not lead; assert
  `WatchNotifyError { not_leader_hint }` is received; stream stays
  open (can subscribe to another group).
- `subscribe_leader_emits_on_write` — open a stream to the leader,
  subscribe to a prefix, `Put` a matching key; assert a `WatchNotify`
  frame is received with the key.
- `stream_end_removes_watchers` — open a stream, subscribe, drop the
  stream; `Put` a matching key; assert no notify (registry cleaned up).
- `leader_stepdown_closes_streams` — subscribe on the leader, force
  step-down; assert the client stream closes (sender dropped).
- `heartbeat_catchup_notifies` — force the leader to learn a slot via
  heartbeat catch-up (not via its own propose); assert the watcher
  receives a notify (proves the apply-path hook fires for catch-up
  slots, not just proposed slots).

**Trigger::TimerOrEvent** (`app/crow-diskdb/src/bg_task.rs`):
- `timer_fires` — `TimerOrEvent` with a 10 ms interval; assert the
  task wakes within 50 ms without a notify.
- `event_fires` — `TimerOrEvent` with a 60 s interval; send
  `notify_one()`; assert the task wakes immediately (< 50 ms).
- `both_race_safe` — `select!` does not panic if both fire
  simultaneously.

### End-to-end tests (E2E)

**diskdb notify-driven sync** (in-process `KvCluster` + diskdb):
- Start a 3-node kv-cluster (group 0) + one diskdb instance with
  `notify_enabled=true`. Wait for the initial sync. Add a disk via
  `HardwareClient::add_disk` (writes to `/hw/disk/...` in group 0).
  Assert the diskdb container sees the new disk within 1 s (notify
  woke keepalive), not 60 s (the safety-net interval). Then
  remove the disk; assert it transitions to `Missing` within 1 s.
- **Ownership transfer** — reassign a disk-group's owner in group 0
  (`HardwareClient` write to `/hw/dg_owner/...`); assert the diskdb
  container drops the disk-group within 1 s (notify-driven), not 60 s.
- **Safety-net catches missed notify** — subscribe with a capacity-1
  channel, fill it (drop the reader), then write to group 0; assert
  the diskdb container still syncs within 60 s (safety-net timer).
- **Leader change** — subscribe on the group-0 leader, force a leader
  change (step-down); assert the diskdb `WatchNotifyClient`
  reconnects to the new leader and subsequent writes are notified
  within 1 s. The safety-net covers the reconnect gap.
- **Notify disabled (default)** — `notify_enabled=false`; add a
  disk; assert the diskdb container sees it only on the next timer
  tick (10 s default), proving the feature is zero-overhead when
  disabled.
- **Heartbeat catch-up notify** — force a leader change where the new
  leader has slots to catch up via heartbeat (slots it accepted as a
  follower); assert the watcher receives notifies for those slots
  (proves the apply-path hook fires for catch-up slots in E2E).

**Client endpoint cache proactive refresh**:
- `DiskdbClient` with `watch_notify_enabled=true`, subscribed to
  `/srv/diskdb/`; change a diskdb instance's `rpc_endpoint` in group
  0 → client's `endpoint_cache` entry for the affected `disk_group_id`
  refreshes without a failed `allocate_blocks` attempt; next call
  routes to the new endpoint.
- Client misses a notify (stream disconnected) → next
  `allocate_blocks` to a moved instance fails once, triggers
  `refresh_endpoints` (on-demand safety net), retries successfully.

**Integration tests** (`lib/crow-kv/tests/group_test/watch_notify_test.rs`):
- `watch_notify_put_receives_key_and_value` — put a key under the
  watched prefix, assert the notify arrives with the correct key and
  value.
- `watch_notify_delete_receives_key_with_empty_value` — delete a key
  under the watched prefix, assert the notify arrives with the key
  and an empty value (tombstone).
- `watch_notify_non_matching_key_no_notify` — write a key outside the
  watched prefix, assert no notify arrives within a short window.
- `watch_notify_batch_write_multiple_keys` — batch-write two keys
  under the watched prefix, assert both appear in the notify with
  their correct values.
- `watch_notify_follower_redirects_to_leader` — subscribe from a
  follower, assert the follower returns an error frame with a
  non-empty `not_leader_hint`.
