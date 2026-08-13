<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# crow-kv Watch/Notify Extension (R78)

This draft covers the crow-kv watch/notify extension, the diskdb
server-side notify handler, and the client endpoint cache proactive
refresh that together replace fixed-interval polling (R71) as the
primary change-detection mechanism. The backlog doc is
[`doc/backlog/R78-diskdb-group0-notify-watch.md`](../backlog/R78-diskdb-group0-notify-watch.md);
the root design context is
[`doc/design/diskdb/design-crow-diskdb.md`](../design/diskdb/design-crow-diskdb.md)
§10 (Follow-up — group-0 notify/watch) and
[`doc/design/kv/design-crow-kv-rpc.md`](../design/kv/design-crow-kv-rpc.md)
§3 (LearnerStream pattern). Already landed: R71's `KeepAlive` sync loop
(`app/crow-diskdb/src/liveness/keepalive.rs`) with fixed-interval
polling and `heartbeat_diskdb` (endpoint populated from
`config.server.listen_addr` via `.with_grpc_endpoint(...)` in
`main.rs:131`); R71's `BackgroundTask` / `BgRunner` / `Trigger`
framework (`app/crow-diskdb/src/bg_task.rs`); the `LearnerStream`
bidi-stream pattern (`lib/crow-kv/src/rpc/px_service.rs:395`); R74's
`DiskdbClient` + endpoint cache + `refresh_endpoints` +
`read_all_diskdb_instances` (`lib/crow-diskdb-client/src/client.rs`).
Architecture decisions and rationale are in the root design; this doc
does not repeat them.

**Scope note — all 7 items in scope.** R74 has landed
(`DiskdbClient` + endpoint cache + `refresh_endpoints`), so item 7
(client endpoint cache proactive refresh in `crow-diskdb-client`) is
no longer blocked and is designed here alongside items 1–6.

## 1. WatchNotify gRPC Bidi Stream

### 1.1 Why

crow-kv has no watch/notify capability today. The only change-detection
mechanism available to external components is polling (prefix scans of
group 0). A push mechanism requires a long-lived stream from the
group-0 leader to each watcher, fired when a watched prefix is written.
The `LearnerStream` bidi pattern (`px_service.rs:395-459`) is the
existing precedent for a long-lived gRPC bidi stream multiplexing
frames between a leader and a peer; `WatchNotify` follows the same
shape but serves client-to-leader watch subscriptions rather than
replica-to-leader consensus traffic.

### 1.2 Proto

Add to `lib/crow-kv/src/rpc/proto/kv.proto` (new messages + one bidi
RPC on `KvService`):

```protobuf
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
// the changed keys (not values) so the watcher can re-read via the
// normal path. slot = the apply slot of the triggering write.
message WatchNotify {
  uint64 group_id      = 1;
  bytes  prefix        = 2;  // which watched prefix matched
  repeated bytes keys  = 3;  // changed keys (deduplicated, coalesced)
  uint64 slot          = 4;
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

```protobuf
  rpc WatchNotify(stream WatchNotifyRequest) returns (stream WatchNotifyResponse);
```

- **Field numbers are append-only** — matches the RPC design's
  compatibility rule (`design-crow-kv-rpc.md` §5).
- **`bytes` fields** — `prefix` and `keys` are raw key bytes as stored
  in the engine. For group-0 sysdata these are UTF-8 text paths (e.g.
  `/hw/disk/1/100/5/abcd`) because `HardwareClient` puts keys via
  `key.to_path().as_bytes()` and scans via `prefix.as_bytes()` (see
  `hardware.rs:57`, `hardware.rs:95`). For data groups the keys are
  binary-encoded (`BinaryKey::to_bytes()`). The proto is
  encoding-agnostic; the subscriber and the engine agree on the
  encoding because they both use the same key types. No `Bytes`
  mapping needed — group-0 keys are text paths < 128 bytes; `Vec<u8>`
  is fine.

### 1.3 Server-side handler

Implement in `lib/crow-kv/src/rpc/kv_service.rs`, mirroring
`learner_stream` (`px_service.rs:395-459`):

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
     watcher from the group's registry.
c. On inbound stream end (client disconnect or error), remove all
   watchers registered by this stream from their group registries
   (`registry.remove_all(&watcher_ids)`).
d. The outbound stream is `ReceiverStream::new(rx)` boxed, returned to
   tonic.

- **Leader check** — uses `local_replica.is_leader()`
  (`local_replica.rs:643`), the same atomic role check the propose
  path uses for its leadership gate. A non-leader node returns
  `not_leader_hint` per-subscribe rather than closing the stream, so a
  client subscribed to multiple groups gets hints only for the groups
  this node doesn't lead.
- **Leader change mid-stream** — on step-down, the old leader clears
  its `WatchRegistry` (see §2.3). Dropping all `Watcher` tx senders
  closes the clients' outbound streams. The client reconnects to the
  new leader and re-subscribes. The safety-net polling covers the gap.
- **`not_leader_hint` source** — `PxGroup::leader_endpoint()` returns
  the believed leader's endpoint (or empty if unknown). The handler
  sends this as the hint, matching the `KvResponse.not_leader_hint`
  pattern in the unary RPC path.

## 2. Watch Registry + Apply-Path Trigger

### 2.1 Why

The notify trigger must fire on the leader's **apply path** — after a
value is Paxos-chosen AND applied to the engine — not on the proposal
path. This is the etcd model: watchers are fed from the apply stream,
not the proposal path (R78 backlog, solution item 1). The proposal
path (`group_propose.rs:246`) only fires for slots the leader proposes
itself; slots the leader learns via heartbeat catch-up
(`apply_loop_task`, `local_replica_apply.rs:301`) or repair
(`group.rs:684`) never trigger a notify on the proposal path. The
apply path fires for **every** chosen slot on **every** replica,
covering all three entry points:
- `Learner::learn` (`learner.rs:554`) — sync path (R17 off): leader
  proposals + follower learn.
- `spawn_learn_chosen`'s spawned task (`local_replica_apply.rs:57`) —
  async path (R17 on): deferred engine apply.
- `apply_loop_task` (`local_replica_apply.rs:342`) — background
  catch-up: slots learned via heartbeat `known_commit_slot` advance
  (R65) or gap-fill.

The apply path's central function is `PxLearner::apply_entry`
(`learner.rs:522`), which decodes `entry.payload` via `Batch::decode`
(`op.rs:56`) into `Batch { ops: Vec<BatchOp> }` — the changed keys are
already extracted there, so the notify trigger incurs no extra decode.
The registry therefore lives on `PxLearner` (set via a setter at group
construction) so the trigger fires from one hook point with no
cross-struct lookup.

### 2.2 WatchRegistry

New module `lib/crow-kv/src/cluster/watch_registry.rs`:

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
    /// prefix -> list of watchers. DashMap for concurrent subscribe/
    /// unsubscribe/emit. Each watcher has a unique `watcher_id` (a
    /// counter) so a stream can remove its own watchers on disconnect.
    watchers: DashMap<Vec<u8>, Vec<(u64, Watcher)>>,
    next_id: AtomicU64,
    /// Atomic fast-path flag: true iff at least one watcher is
    /// registered. The apply path checks this (one Acquire load)
    /// before touching the DashMap — zero overhead when no watchers.
    has_watchers: AtomicBool,
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
    /// prefixes and enqueue notify frames. Called by the coalescer or
    /// directly (debounce=0). Non-blocking: uses `try_send`.
    pub fn emit(&self, group_id: u64, slot: u64, changed: &[BatchOp]);
    /// True if at least one watcher is registered. Atomic load — the
    /// apply-path fast path.
    pub fn has_watchers(&self) -> bool;
}
```

- **`has_watchers` atomic fast path** — the apply-path hook checks
  `registry.has_watchers()` (one `Acquire` load of an `AtomicBool`)
  before decoding or matching. When no watchers are registered (the
  common case in tests and non-diskdb clusters), the cost is one
  predicted-not-taken branch. This is cheaper than `DashMap::is_empty()`
  (which acquires read locks on all shards) and is the true
  zero-overhead gate. `has_watchers` is set to `true` on the first
  `subscribe` and recomputed (via `watchers.is_empty()` scan) on
  `unsubscribe` / `remove_all` / `clear`.
- **`emit` matching** — for each `BatchOp` in the decoded batch,
  iterate `watchers.iter()` and check `op.key.starts_with(&entry.prefix)`.
  Group matches by prefix, build one `WatchNotify { prefix, keys, slot }`
  per prefix per watcher, send via `tx.try_send` (non-blocking — if the
  watcher's channel is full, drop the notify; the safety-net polling
  covers missed notifies).
- **`try_send` not `send().await`** — the apply path must not block on
  a slow watcher. A full channel means the watcher is lagging; dropping
  the notify is correct (the safety-net polling will catch up).
- **Delete ops** — `BatchOp { op: Op::Delete, key }` still carries a
  key; a delete on a watched prefix notifies the watcher (the key
  changed — it's gone). The watcher re-reads and sees the deletion.

### 2.3 PxLearner integration

Add to `PxLearner` (`lib/crow-kv/src/paxos/learner.rs`):

```rust
/// Optional per-group watch registry. Set when the group is
/// constructed; None in tests that don't use watch/notify. The
/// apply-path hook in `apply_entry` checks `has_watchers()` before
/// touching the registry.
watch_registry: OnceLock<(u64, Arc<WatchRegistry>)>,  // (group_id, registry)
```

- **`(group_id, registry)` tuple** — `PxLogEntry` has no `group_id`
  field (`roles.rs:81`: `slot`, `ballot`, `term`, `payload`), and
  `apply_entry` takes only `(slot, &payload)`. The `group_id` is stored
  alongside the registry so `emit` can populate `WatchNotify.group_id`
  without threading `group_id` through every call site.
- **Set via `set_watch_registry(group_id, Arc<WatchRegistry>)`** —
  called once during `PxGroup` construction (after the learner is
  created). `OnceLock` ensures it's set exactly once; tests that don't
  use watch/notify leave it unset (`has_watchers()` returns false via
  the `None` fast path).
- **`apply_entry` hook** — after `self.engine.apply(slot, &batch).await`
  succeeds (`learner.rs:538`), before the method returns:

```rust
if let Some((group_id, registry)) = self.watch_registry.get() {
    if registry.has_watchers() {
        self.watch_coalescer
            .record_chosen(*group_id, slot, &batch.ops, registry);
    }
}
```

  The `batch` is already decoded at `learner.rs:533`
  (`let batch = Batch::decode(payload)`), so the keys are available
  with no extra decode. The coalescer is also wired via `OnceLock`
  (see §3). When `has_watchers()` is false, the cost is one `OnceLock::get`
  + one `AtomicBool` load — zero overhead when unused.
- **Fires on ALL apply paths** — `apply_entry` is called from `learn`
  (sync, `learner.rs:559`), `spawn_learn_chosen` (async,
  `local_replica_apply.rs:58`), and `apply_loop_task` (catch-up,
  `local_replica_apply.rs:342`). All three paths trigger the hook,
  covering leader proposals, follower learn, heartbeat catch-up, and
  gap-fill repair. This is the etcd model.
- **Followers don't emit** — followers also call `apply_entry`, but
  their `WatchRegistry` is empty (cleared on step-down, or never
  populated if they were never leader). `has_watchers()` returns false
  → no emit. Only the leader (which holds the client streams) emits.
  No explicit leader check needed in the hook — the registry's
  emptiness is the gate.

### 2.4 PxGroup integration

Add to `PxGroup` (`lib/crow-kv/src/cluster/group.rs`):

```rust
pub(crate) watch_registry: Arc<WatchRegistry>,
```

- Constructed in `PxGroup::new` as `Arc::new(WatchRegistry::new())`.
  Wired into the learner via
  `local_replica.learner.set_watch_registry(group_id, Arc::clone(&watch_registry))`
  right after construction.
- **Cleared on step-down** — the leader step-down path is
  `PxGroup::step_down` (`group_election.rs:385`), called for
  `HigherTerm` / `LeaseUnrenewable` / `Admin` reasons. Add
  `self.watch_registry.clear()` + `self.watch_coalescer.flush_and_clear()`
  at the end of `step_down` (after `become_follower`). This drops all
  `Watcher` tx senders, closing the clients' outbound streams (clean
  reconnect). The coalescer flush emits any pending notifies before
  the registry clears (no notify lost on leader change).
- **Also cleared on propose-path direct step-down** — the propose path
  has two direct `become_follower` calls that bypass `step_down`:
  `group_propose.rs:208` (higher term during prepare) and
  `group_propose.rs:293` (higher term during accept). Both have
  `&self` (`PxGroup`), so add `self.watch_registry.clear()` +
  `self.watch_coalescer.flush_and_clear()` before each
  `replica.become_follower(...)` call. This covers the case where the
  leader discovers a higher term mid-proposal and steps down without
  going through `step_down`.
- **Not cleared on `shutdown`** — `shutdown` drops the whole `PxGroup`,
  which drops the `Arc<WatchRegistry>`, which drops all watchers.

### 2.5 Why not the proposal path

The original draft hooked the trigger at `group_propose.rs:246` (after
`fan_out_chosen_notice`). That path has a critical gap: it only fires
for slots the leader **proposes itself**. Three classes of chosen
slots never trigger a notify on the proposal path:

- **Heartbeat catch-up slots** — when the leader advances
  `known_commit_slot` via heartbeat (R65, `group_election.rs:299`) and
  the background `apply_loop_task` applies slots the leader accepted as
  a follower before winning the election. These slots are real writes
  to real keys; watchers must be notified.
- **Repair slots** — `group.rs:684` and `group_election.rs:274` call
  `fan_out_chosen_notice` for gap-fill repair, but these are on the
  repair path, not the propose path. A repaired slot is a real write;
  watchers must be notified.
- **Foreign-value retry** — the draft correctly noted that the
  proposal path fires for foreign values, but the retried client value
  fires its own notify. On the apply path, both the foreign value and
  the retried value fire `apply_entry` → both notify. This is correct
  and automatic — no special handling needed.

The apply-path hook covers all three with one code point. The cost is
that `apply_entry` runs on followers too (where the registry is empty),
but the `has_watchers()` atomic check makes this a single predicted-
not-taken branch — zero overhead.

## 3. Coalescing

### 3.1 Why

Burst writes to the same prefix (e.g. a batch of disk-status updates
via `batch_write`) would generate one notify per write, flooding
watchers. A debounce window coalesces writes to the same prefix into
one notify. R78 specifies a default of 100 ms.

### 3.2 WatchCoalescer

New struct in `lib/crow-kv/src/cluster/watch_registry.rs`:

```rust
pub(crate) struct WatchCoalescer {
    debounce_ms: u64,
    /// prefix -> (pending keys set, timer handle). Protected by a
    /// parking_lot::Mutex — the apply path is low-contention (one
    /// leader, one coalescer).
    pending: Mutex<HashMap<Vec<u8>, (HashSet<Vec<u8>>, Option<JoinHandle<()>>)>>,
}
```

Wired into `PxLearner` via a second `OnceLock`:

```rust
watch_coalescer: OnceLock<Arc<WatchCoalescer>>,
```

Set via `set_watch_coalescer(Arc<WatchCoalescer>)` during `PxGroup`
construction. The `apply_entry` hook (§2.3) calls
`watch_coalescer.record_chosen(group_id, slot, &batch.ops, registry)`.

```rust
impl WatchCoalescer {
    pub fn new(debounce_ms: u64) -> Self;
    /// Called from the apply-path hook. For each BatchOp, find matching
    /// prefixes in the registry, and either emit immediately (debounce=0)
    /// or buffer into pending[prefix].
    pub fn record_chosen(
        &self,
        group_id: u64,
        slot: u64,
        ops: &[BatchOp],
        registry: &WatchRegistry,
    );
    /// Drain all pending sets (emitting final notifies) and cancel all
    /// timers. Called on leader step-down before registry.clear().
    pub fn flush_and_clear(&self);
}
```

a. If `debounce_ms == 0`: for each `BatchOp`, find matching prefixes in
   the registry, collect keys per prefix, call
   `registry.emit(group_id, slot, &keys)` immediately. No buffering.
b. If `debounce_ms > 0`: for each `BatchOp`, find matching prefixes in
   the registry, insert keys into `pending[prefix]`. If no timer is
   running for that prefix, spawn a `tokio::time::sleep` task that
   after `debounce_ms` locks the map, drains the pending set for that
   prefix, and calls `registry.emit`. The timer handle is stored so a
   subsequent write to the same prefix within the window just adds to
   the set (the existing timer will flush).
c. The timer task captures `Arc<WatchRegistry>` + `Arc<WatchCoalescer>`
   (weak) so it survives even if the group is dropped mid-debounce
   (the weak upgrade fails → no-op).
d. `record_chosen` skips ops whose key matches no registered prefix
   (the common case when the write is to an unwatched key range) —
   no pending entry is created.

- **Debounce = 0 is the test default** — tests want immediate notifies
  with no timer flakiness. Production sets 100 ms.
- **Coalescer flushed on step-down** — `step_down` and the propose-path
  direct step-downs (§2.4) call `watch_coalescer.flush_and_clear()`
  **before** `watch_registry.clear()`. This drains all pending sets
  (emitting final notifies) and cancels all timers. No notify is lost
  on leader change — the last burst is flushed before the registry
  clears and client streams close.
- **`slot` in coalesced notifies** — the coalesced notify carries the
  highest slot among the coalesced writes (the most recent). Watchers
  use it only for logging; the actual freshness is guaranteed by the
  re-read via the sync path.
- **`debounce_ms` is read from `CrowKVConfig`** — the coalescer is
  constructed in `PxGroup::new` with
  `CrowKVConfig::watch_notify_debounce_ms` (default 100). On
  `set_from_config`, the coalescer's debounce is updated live (the
  field is an `AtomicU64` on the coalescer, read by `record_chosen`).

## 4. WatchNotify Client

### 4.1 Why

diskdb (and future clients) need a reusable client that opens the
`WatchNotify` stream to the group leader, subscribes to prefixes,
and delivers notify frames via a channel. This is the client-side
mirror of §1's server handler.

### 4.2 WatchNotifyClient

New module `lib/crow-kv-client/src/watch_notify.rs`:

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

a. `subscribe` resolves the group leader endpoint via
   `CrowkvClient`'s topology cache (`kv.topology.leader(store_id, group_id)`,
   falling back to `kv.topology.refresh()` on cache miss — same
   mechanism as `Put`/`Get` via `resolve_leader`).
b. Gets a gRPC channel from the shared `ConnectionPool`
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
  / `SYSTEM_GROUP`, `client.rs:277-279`). The `subscribe` signature
  takes `store_id` so the client can resolve the leader endpoint.
- **One stream per subscription** — simpler than multiplexing multiple
  prefixes over one stream (each subscription is independent; a
  diskdb instance typically watches 3 prefixes). If this becomes a
  connection-count concern, a future optimization multiplexes
  subscriptions over a shared stream (the proto already supports
  multiple `WatchSubscribe` frames on one stream — no proto change
  needed).
- **Reconnect backoff** — capped exponential (50 ms → 2 s), matching
  `LearnerStream`'s reconnect policy (`design-crow-kv-rpc.md` §6:
  "capped exponential backoff (50 ms → 2 s)").
- **`ConnectionPool` reuse** — `pool` is `pub(crate)` on `CrowkvClient`
  (`client.rs:166`); `WatchNotifyClient` is in the same crate, so it
  accesses the pool directly. No new connection management code.

## 5. diskdb Notify Handler + Polling Safety Net

### 5.1 Why

diskdb's `KeepAlive` sync loop (`liveness/keepalive.rs`) currently
wakes on a fixed timer (`Trigger::TimerFn`). To trigger an immediate
sync on notify, the loop must also wake on an external signal. The
`Trigger` enum (`bg_task.rs:39`) has an `Event(Notify)` variant but
not a hybrid timer+event. The notify handler (a `WatchNotifyClient`
reader) must wake the keepalive without going through the timer.

### 5.2 Trigger extension

Extend `Trigger` in `app/crow-diskdb/src/bg_task.rs`:

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

Update `wait_trigger` (`bg_task.rs:191`):

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

a. Add field `sync_trigger: Option<Arc<tokio::sync::Notify>>` —
   `Some` when `notify_enabled`, `None` otherwise (keeps the struct
   usable in tests without notify).
b. Add builder method `.with_sync_trigger(notify: Arc<Notify>) -> Self`.
c. In `trigger()` (the `BackgroundTask` impl): if `sync_trigger` is
   `Some`, return `Trigger::TimerOrEvent { interval_fn, notify:
   sync_trigger }`; else fall back to the existing `TimerFn` path.
d. Add method `pub fn trigger_now(&self)` — calls
   `self.sync_trigger.notify_one()` if set. Called by the notify
   handler on each `WatchNotify` frame.
e. **`grpc_endpoint` already populated** — `main.rs:131` already
   calls `.with_grpc_endpoint(config.load().server.listen_addr.clone())`,
   and `keepalive.rs:234` passes `self.grpc_endpoint` to
   `heartbeat_diskdb` on every tick. R78 item 1 (endpoint
   registration) is already done; no change needed here.

### 5.4 Notify handler task

New module `app/crow-diskdb/src/liveness/notify.rs`:

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
   key encoding — `HardwareClient` puts/scans text paths):
   - `/hw/dg_owner/` (ownership map — global; `list_owners` scans
     globally and filters to this instance)
   - `/hw/dg_bind/` (binding map — global; `list_binds` scans globally)
   - `/hw/disk/` (disk metadata + status — global; `observe_disks`
     scans per owned disk-group, but the notify is a coarse trigger)
   All three use `(G0_STORE, G0_GROUP)` = `(0, 0)`.
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
  to `instance_id == self.instance_id` (`keepalive.rs:288`). Watching
  the global prefix means the keepalive wakes on any ownership/bind
  change anywhere, then re-scans and filters to this instance. An
  irrelevant change (another instance's ownership) causes a cheap
  no-op re-scan. Per-node prefixes (`OwnerMapKey::prefix_for_node`)
  would be more selective but require knowing `rack_id`/`node_id`
  before the first sync — the diskdb discovers its node identity from
  the ownership map, not from config. Global prefixes avoid this
  bootstrapping dependency.
- **Not a `BackgroundTask`** — `NotifyHandler` is a long-lived task
  with its own `run` method, not a `BgRunner` cycle task. It's spawned
  directly in `main.rs` alongside the bg runner, with the same stop
  signal. This avoids forcing the notify loop into the
  trigger→cycle→trigger model (it's event-driven, not periodic).

### 5.5 Polling safety net

When `notify_enabled` is true:
- The keepalive timer interval (from `sync.sync_interval_secs`) is the
  safety-net polling interval. The config default rises (e.g. 60 s)
  when notify is on; the operator sets it explicitly.
- The timer catches any missed notifies (reliability fallback): if a
  notify is dropped (full channel) or the WatchNotify stream is
  disconnected, the next timer tick still syncs.
- When `notify_enabled` is false (v1 default), behavior is unchanged —
  fixed-interval polling at `sync_interval_secs` (default 10 s), no
  notify handler, no `WatchNotifyClient`.

## 6. Client Endpoint Cache Proactive Refresh (Item 7)

### 6.1 Why

R74's `DiskdbClient` endpoint cache (`lib/crow-diskdb-client/src/client.rs`)
refreshes **on demand** today: at startup, on cache miss, and on
error-retry (`refresh_endpoints` reads `read_all_diskdb_instances`).
When a diskdb instance moves (endpoint change in its service-registry
record at `/srv/diskdb/<instance_id>`), a client holding the old
endpoint cache entry fails one `allocate_blocks`/`free_blocks` attempt,
triggers `refresh_endpoints` (on-demand safety net), and retries. R78
item 7 extends this to **proactive** refresh: the client subscribes to
the `/srv/diskdb/` prefix via `WatchNotifyClient`; on a notify
(instance register/deregister/move), it refreshes the affected cache
entries so the next call routes to the new endpoint without a failed
attempt + retry. The v1 on-demand refresh remains as the safety net;
proactive refresh is an optimization, not a correctness requirement.

### 6.2 Design

Add a `WatchNotifyClient` subscription to `DiskdbClient`:

a. In `DiskdbClient::new` (or a new `with_watch_notify` builder), if
   a `WatchNotifyClient` is provided, subscribe to
   `(G0_STORE, G0_GROUP, "/srv/diskdb/")`.
b. Spawn a reader task: on `WatchNotify` frame, call
   `self.refresh_endpoints()` (the existing R74 method). The notify
   is a coarse trigger — the refresh re-reads ALL diskdb instances
   from group 0 and updates the cache. This is the same "trigger, not
   transport" model as the diskdb notify handler (§5.4).
c. The reader task runs for the client's lifetime. On stream close
   (leader change), `WatchNotifyClient` reconnects automatically
   (§4.2); the reader task just keeps reading.
d. If no `WatchNotifyClient` is provided (v1 default), the client
   uses on-demand refresh only (R74 behavior, unchanged).

- **`/srv/diskdb/` prefix** — the service-registry instance key path
  is `/srv/diskdb/<instance_id>` (`InstanceKey::to_path()`,
  `key/diskdb.rs:642`). The global prefix `/srv/diskdb/` covers all
  diskdb instances. `ServiceRegistryClient::read_all_diskdb_instances`
  scans this prefix (`service_registry.rs:166-167`).
- **On-demand refresh stays as safety net** — if a notify is missed
  (stream disconnected, channel full), the next `allocate_blocks` to
  a moved instance fails once, triggers `refresh_endpoints`, and
  retries. The cache is eventually consistent either way.

## 7. Configuration

### 7.1 diskdb config

Add to `app/crow-diskdb/src/ddb_config.rs`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyConfig {
    /// static: enable watch/notify (default: false). When true,
    /// diskdb subscribes to group-0 prefixes and the keepalive timer
    /// serves as a safety-net poller at `sync.sync_interval_secs`.
    pub notify_enabled: bool,
    /// dynamic: crow-kv-side coalescing window in ms (default: 100).
    /// 0 = no coalescing (immediate emit). Read by the crow-kv
    /// leader's WatchCoalescer; configured here and passed to the
    /// kv-client on WatchNotifyClient construction (forwarded as a
    /// subscribe parameter or via a separate config path — see Open
    /// Questions).
    pub notify_debounce_ms: u64,
}

impl Default for NotifyConfig {
    fn default() -> Self {
        Self { notify_enabled: false, notify_debounce_ms: 100 }
    }
}
```

- Add `pub notify: NotifyConfig` to `DdbConfig`.
- `validate()`: if `notify_enabled` and `sync.sync_interval_secs == 0`,
  error (same existing check; no new constraint).

### 7.2 crow-kv config

The `WatchCoalescer`'s `debounce_ms` is a per-group setting. It's read
from the `CrowKVConfig` held by `PxGroup`:

```rust
// In CrowKVConfig (lib/crow-kv/src/cluster/group_config.rs):
pub watch_notify_debounce_ms: u64,  // default: 100
```

- **Why on crow-kv config, not just diskdb** — the coalescer runs on
  the crow-kv leader (group 0), not on diskdb. diskdb's
  `notify_debounce_ms` is the operator's intent, but the actual
  coalescing happens in crow-kv. The crow-kv config field is the
  authoritative source; diskdb's field is for documentation/operator
  convenience (they should match). See Open Questions.

### 7.3 crow-diskdb-client config

Add an optional `watch_notify` toggle to `DiskdbClient`'s config (or
builder):

```rust
/// When true, the client subscribes to `/srv/diskdb/` and proactively
/// refreshes the endpoint cache on notify. Default: false (on-demand
/// refresh only, R74 behavior).
pub watch_notify_enabled: bool,
```

- **Default false** — proactive refresh is an optimization; v1 ships
  with on-demand refresh (R74). Operators opt in per client.

## Scope

**lib/crow-kv** (watch/notify extension):
- `src/rpc/proto/kv.proto` — add `WatchSubscribe`, `WatchUnsubscribe`,
  `WatchNotify`, `WatchNotifyError`, `WatchNotifyRequest`,
  `WatchNotifyResponse` messages + `WatchNotify` bidi RPC on
  `KvService`.
- `src/rpc/kv_service.rs` — implement `watch_notify` bidi stream
  handler (subscribe/unsubscribe/leader-check/cleanup).
- `src/cluster/watch_registry.rs` — **new**: `WatchRegistry`,
  `Watcher`, `WatchCoalescer`.
- `src/paxos/learner.rs` — add `watch_registry` + `watch_coalescer`
  `OnceLock` fields + `set_watch_registry` / `set_watch_coalescer`
  setters; add apply-path emit hook in `apply_entry`.
- `src/cluster/group.rs` — add `watch_registry: Arc<WatchRegistry>`
  field to `PxGroup`; wire into learner in `PxGroup::new`; clear on
  `step_down`.
- `src/cluster/group_election.rs` — call `watch_registry.clear()` +
  `watch_coalescer.flush_and_clear()` in `PxGroup::step_down`.
- `src/cluster/group_propose.rs` — call `watch_registry.clear()` +
  `watch_coalescer.flush_and_clear()` before the two direct
  `become_follower` calls (lines 208, 293).
- `src/cluster/group_config.rs` — add `watch_notify_debounce_ms` to
  `CrowKVConfig`.
- `src/cluster/mod.rs` — export `watch_registry`.

**lib/crow-kv-client** (client API):
- `src/watch_notify.rs` — **new**: `WatchNotifyClient`,
  `WatchSubscription` (open stream, subscribe, reconnect, deliver
  frames).
- `src/lib.rs` — export `watch_notify` module.

**app/crow-diskdb** (notify handler + config):
- `src/liveness/notify.rs` — **new**: `NotifyHandler` (subscribe to
  group-0 prefixes, wake keepalive on notify).
- `src/liveness/keepalive.rs` — add `sync_trigger` field +
  `with_sync_trigger` builder + `trigger_now` method + `TimerOrEvent`
  trigger.
- `src/liveness/mod.rs` — export `notify`.
- `src/bg_task.rs` — add `Trigger::TimerOrEvent` variant +
  `wait_trigger` branch.
- `src/ddb_config.rs` — add `NotifyConfig` + `notify` field on
  `DdbConfig`.
- `src/main.rs` — wire `NotifyHandler` when `notify_enabled`; pass
  `sync_trigger` to keepalive.

**lib/crow-diskdb-client** (item 7 — proactive endpoint cache refresh):
- `src/client.rs` — add optional `WatchNotifyClient` to `DiskdbClient`;
  subscribe to `/srv/diskdb/` when enabled; call `refresh_endpoints`
  on notify.
- `src/lib.rs` — re-export `WatchNotifyClient` from `crow-kv-client`
  for convenience.

## Complexity

**High.** The crow-kv watch/notify extension is a new subsystem: a
bidi gRPC stream, a per-group watch registry with concurrent
subscribe/unsubscribe/emit, an apply-path trigger that fires on every
chosen slot (gated by an `AtomicBool` fast path), and a debounce
coalescer with timer tasks. The hardest parts are (1) the apply-path
hook — wiring the registry into `PxLearner` via `OnceLock` so all
three apply entry points (`learn`, `spawn_learn_chosen`,
`apply_loop_task`) trigger the hook, while keeping zero overhead when
no watchers are registered; (2) the leader-step-down lifecycle —
clearing the registry and flushing the coalescer without losing
notifies, while closing client streams cleanly, across three step-down
sites (`step_down` + two propose-path direct `become_follower` calls);
(3) the `WatchNotifyClient` reconnect logic — detecting stream close,
refreshing topology, re-subscribing, and keeping the subscriber's
channel open across reconnects. The diskdb side is medium complexity
(trigger extension + notify handler task). The `LearnerStream` pattern
is reused for the bidi stream shape; the `Batch::decode` is reused for
payload decoding (already done in `apply_entry`, no extra decode).

## Test Design

### Unit tests (UT)

**WatchRegistry** (`lib/crow-kv/src/cluster/watch_registry.rs`):
- `subscribe_emit` — register a watcher for prefix `/hw/disk/`, emit
  a changed key `/hw/disk/1/2/3/abcd`, assert the watcher's channel
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

**WatchCoalescer** (`lib/crow-kv/src/cluster/watch_registry.rs`):
- `debounce_zero_emits_immediately` — `debounce_ms=0`, record a
  chosen payload with 2 keys; assert immediate emit (no timer).
- `debounce_coalesces_burst` — `debounce_ms=50`, record 3 payloads
  to the same prefix within 10 ms; assert one `WatchNotify` with all
  keys after ~50 ms (use `tokio::time::pause` in tests).
- `debounce_different_prefixes_independent` — record writes to two
  prefixes; assert two separate notifies (one per prefix timer).
- `flush_and_clear_on_stepdown` — buffer 2 keys, `flush_and_clear`;
  assert one final notify with the buffered keys + no timers remain.
- `unwatched_key_skipped` — record a payload whose key matches no
  registered prefix; assert no pending entry is created and no notify.

**WatchNotify server handler** (`lib/crow-kv/src/rpc/kv_service.rs`):
- `subscribe_not_leader` — open a stream to a non-leader node, send
  `WatchSubscribe` for a group it doesn't lead; assert
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
- **Notify disabled (v1 default)** — `notify_enabled=false`; add a
  disk; assert the diskdb container sees it only on the next timer
  tick (10 s default), proving the feature is zero-overhead when
  disabled.
- **Coalescing** — `notify_debounce_ms=100`; burst-write 10 disks in
  one `batch_write`; assert one notify (coalesced) wakes the keepalive
  once, not 10 times.
- **Heartbeat catch-up notify** — force a leader change where the new
  leader has slots to catch up via heartbeat (slots it accepted as a
  follower); assert the watcher receives notifies for those slots
  (proves the apply-path hook fires for catch-up slots in E2E).

**Client endpoint cache proactive refresh (item 7)**:
- `DiskdbClient` with `watch_notify_enabled=true`, subscribed to
  `/srv/diskdb/`; change a diskdb instance's `grpc_endpoint` in group
  0 → client's `endpoint_cache` entry for the affected `disk_group_id`
  refreshes without a failed `allocate_blocks` attempt; next call
  routes to the new endpoint.
- Client misses a notify (stream disconnected) → next
  `allocate_blocks` to a moved instance fails once, triggers
  `refresh_endpoints` (on-demand safety net), retries successfully.

## Module Structure

```
lib/crow-kv/src/
  rpc/
    proto/kv.proto              # +WatchNotify messages + RPC
    kv_service.rs               # +watch_notify handler
  cluster/
    watch_registry.rs           # NEW: WatchRegistry, Watcher, WatchCoalescer
    group.rs                    # +watch_registry field, wire into learner, clear on step_down
    group_election.rs           # +clear registry + flush coalescer in step_down
    group_propose.rs            # +clear registry before direct become_follower calls
    group_config.rs             # +watch_notify_debounce_ms
    mod.rs                      # export watch_registry
  paxos/
    learner.rs                  # +watch_registry/coalescer OnceLock + set_* + apply-path hook

lib/crow-kv-client/src/
  watch_notify.rs               # NEW: WatchNotifyClient, WatchSubscription
  lib.rs                        # export watch_notify

app/crow-diskdb/src/
  liveness/
    notify.rs                   # NEW: NotifyHandler
    keepalive.rs                # +sync_trigger, +trigger_now, +TimerOrEvent trigger
    mod.rs                      # export notify
  bg_task.rs                    # +Trigger::TimerOrEvent
  ddb_config.rs                 # +NotifyConfig
  main.rs                       # wire NotifyHandler when notify_enabled

lib/crow-diskdb-client/src/
  client.rs                     # +optional WatchNotifyClient, proactive refresh on notify
  lib.rs                        # re-export WatchNotifyClient
```

## Config Extensions

- **`DdbConfig.notify`** (`NotifyConfig`):
  - `notify_enabled: bool` — default `false`. Static (requires restart).
  - `notify_debounce_ms: u64` — default `100`. Dynamic.
- **`CrowKVConfig.watch_notify_debounce_ms`** — default `100`.
  Dynamic (live-reload via `set_from_config`; the coalescer reads it
  from an `AtomicU64`).
- **`DiskdbClient` `watch_notify_enabled: bool`** — default `false`.
  Static (set at client construction).
- **`validate()`** — no new constraints; existing
  `sync.sync_interval_secs > 0` check covers the safety-net interval.

## Server Wiring

`app/crow-diskdb/src/main.rs` startup sequence (additions):

1. After building `keepalive` (line 127): if
   `config.load().notify.notify_enabled`, create an
   `Arc<tokio::sync::Notify>` (`sync_trigger`), call
   `keepalive.with_sync_trigger(Arc::clone(&sync_trigger))`.
2. After building the bg runner (line 197): if `notify_enabled`,
   construct `WatchNotifyClient::from_shared(Arc::clone(&kv_client))`
   and `NotifyHandler::new(watch, sync_trigger)`. Spawn
   `notify_handler.run(stop_notify)` as a background task with the
   same stop signal as the bg runner.
3. On shutdown: the stop signal aborts the notify handler (its `run`
   loop exits on `stop.notified()`); the bg runner stops keepalive +
   compaction; gRPC + HTTP servers drain.

## Open Questions

- **Q1: `notify_debounce_ms` — diskdb config vs crow-kv config.** The
  coalescer runs on the crow-kv leader (group 0), configured via
  `CrowKVConfig.watch_notify_debounce_ms`. diskdb's
  `NotifyConfig.notify_debounce_ms` is the operator's intent but is
  not directly consumed by the coalescer (diskdb is a client, not the
  leader). Options: (a) diskdb's field is documentation-only — the
  operator sets the real value in crow-kv config; (b) diskdb sends
  its desired debounce as a `WatchSubscribe` parameter and the leader
  uses the per-subscription value. Option (b) is more ergonomic but
  adds a per-watcher debounce timer (more state). Option (a) is
  simpler. **Recommendation: (a) for v1** — one global debounce on the
  leader; diskdb's field documents the operator's intent. Revisit if
  per-subscription debounce is needed.
- **Q2: One stream per subscription vs multiplexed.** §4.2 chooses one
  gRPC stream per `(store_id, group_id, prefix)` subscription. A
  diskdb instance watching 3 prefixes opens 3 streams. If connection
  count is a concern, multiplex all subscriptions over one stream
  (send multiple `WatchSubscribe` frames on one bidi stream). The
  proto already supports this (the inbound stream is a sequence of
  frames). **No change needed for v1** — the `WatchNotifyClient` can
  be extended later to multiplex without proto changes.
