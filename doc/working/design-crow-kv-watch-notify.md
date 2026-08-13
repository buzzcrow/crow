<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# crow-kv Watch/Notify Extension (R78)

This draft covers the crow-kv watch/notify extension and the diskdb
server-side notify handler that together replace fixed-interval polling
(R71) as the primary change-detection mechanism. The backlog doc is
[`doc/backlog/R78-diskdb-group0-notify-watch.md`](../backlog/R78-diskdb-group0-notify-watch.md);
the root design context is
[`doc/design/diskdb/design-crow-diskdb.md`](../design/diskdb/design-crow-diskdb.md)
§10 (Follow-up — group-0 notify/watch) and
[`doc/design/kv/design-crow-kv-rpc.md`](../design/kv/design-crow-kv-rpc.md)
§3 (LearnerStream pattern). Already landed: R71's `KeepAlive` sync loop
(`app/crow-diskdb/src/liveness/keepalive.rs`) with fixed-interval
polling and `heartbeat_diskdb` (endpoint slot wired but passing `""`);
R71's `BackgroundTask` / `BgRunner` / `Trigger` framework
(`app/crow-diskdb/src/bg_task.rs`); the `LearnerStream` bidi-stream
pattern (`lib/crow-kv/src/rpc/px_service.rs:395`). Architecture
decisions and rationale are in the root design; this doc does not
repeat them.

**Scope note — item 6 deferred.** R78 item 6 (client endpoint cache
proactive refresh in `crow-diskdb-client`) depends on R74's
`DiskdbClient` + endpoint cache, which is not yet implemented
(`QueryCapacityStats` still returns `Unimplemented`,
`crow-diskdb-client` is a skeleton). Items 1-5 (the crow-kv watch/notify
extension + diskdb server-side notify handler + polling safety net) are
independent of R74 and are implemented here. Item 6 is a follow-up that
builds on the watch/notify client API once R74 lands.

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
// normal path. slot = the commit slot of the triggering write.
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
- **`bytes` fields** — `prefix` and `keys` are small (group-0 keys are
  text paths < 128 bytes); no `Bytes` mapping needed (unlike hot-path
  KV fields). `Vec<u8>` is fine.

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
     that group, send `WatchNotifyError { not_leader_hint }` and
     `continue` (do not close the stream — the client may subscribe to
     other groups). If leader, register a `Watcher { prefix, tx }` in
     the group's `WatchRegistry` and record the `watcher_id` for
     cleanup on stream end.
   - `WatchUnsubscribe { group_id, prefix }` — remove the matching
     watcher from the group's registry.
c. On inbound stream end (client disconnect or error), remove all
   watchers registered by this stream from their group registries.
d. The outbound stream is `ReceiverStream::new(rx)` boxed, returned to
   tonic.

- **Leader check** — uses the same `role == Leader &&
  current_term == proposing_term` gate as the propose path. A
  non-leader node returns `not_leader_hint` per-subscribe rather than
  closing the stream, so a client subscribed to multiple groups gets
  hints only for the groups this node doesn't lead.
- **Leader change mid-stream** — on step-down, the old leader drops
  all watchers from its registry (the registry is cleared on
  `become_follower`). The outbound `mpsc::Sender` is dropped when the
  registry clears, which causes the client's stream to close. The
  client reconnects to the new leader and re-subscribes. The
  safety-net polling covers the gap.

## 2. Watch Registry + Notify Emission

### 2.1 Why

The notify trigger must fire on the leader's write path, after a value
is Paxos-chosen, with access to the written keys. The write path's
chosen hook is `group_propose.rs:246` (after `fan_out_chosen_notice`),
which has `&self` (`PxGroup`), the `entry.payload` (`Bytes`), and
`group_id`. The registry therefore lives on `PxGroup` so the trigger
incurs no cross-struct lookup.

### 2.2 WatchRegistry

New module `lib/crow-kv/src/cluster/watch_registry.rs`:

```rust
/// One watcher: a prefix + an outbound channel to push notify frames.
pub(crate) struct Watcher {
    pub prefix: Bytes,
    pub tx: mpsc::Sender<Result<WatchNotifyResponse, Status>>,
}

/// Per-group watch registry. Held by `PxGroup` as `Arc<WatchRegistry>`.
pub(crate) struct WatchRegistry {
    /// prefix -> list of watchers. DashMap for concurrent subscribe/
    /// unsubscribe/emit. Each watcher has a unique `watcher_id` (a
    /// counter) so a stream can remove its own watchers on disconnect.
    watchers: DashMap<Bytes, Vec<(u64, Watcher)>>,
    next_id: AtomicU64,
}

impl WatchRegistry {
    pub fn new() -> Self;
    /// Register a watcher for `prefix`. Returns the `watcher_id` for
    /// later removal.
    pub fn subscribe(&self, prefix: Bytes, tx: mpsc::Sender<...>) -> u64;
    /// Remove a specific watcher by `(prefix, watcher_id)`.
    pub fn unsubscribe(&self, prefix: &Bytes, watcher_id: u64);
    /// Remove all watchers whose `tx` is the given sender (stream-end
    /// cleanup). Identified by watcher_id set.
    pub fn remove_all(&self, watcher_ids: &[u64]);
    /// Clear all watchers (leader step-down).
    pub fn clear(&self);
    /// For a set of changed keys, find matching prefixes and enqueue
    /// notify frames. Called by the coalescer or directly (debounce=0).
    pub fn emit(&self, group_id: u64, slot: u64, changed: &[(Bytes, Bytes)]);
    /// True if no watchers are registered (skip decode + match on the
    /// write path — the common case when nobody is watching).
    pub fn is_empty(&self) -> bool;
}
```

- **`is_empty` fast path** — the write-path hook checks
  `registry.is_empty()` before decoding the payload. When no watchers
  are registered (the common case in tests and non-diskdb clusters),
  the cost is one atomic load. This keeps the feature zero-overhead
  when unused.
- **`emit` matching** — for each changed key, iterate
  `watchers.iter()` and check `key.starts_with(&entry.prefix)`. Group
  matches by prefix, build one `WatchNotify { prefix, keys, slot }`
  per prefix per watcher, send via `tx.try_send` (non-blocking — if the
  watcher's channel is full, drop the notify; the safety-net polling
  covers missed notifies).
- **`try_send` not `send().await`** — the write path must not block on
  a slow watcher. A full channel means the watcher is lagging; dropping
  the notify is correct (the safety-net polling will catch up).

### 2.3 PxGroup integration

Add to `PxGroup` (`lib/crow-kv/src/cluster/group.rs`):

```rust
pub(crate) watch_registry: Arc<WatchRegistry>,
```

- Constructed in `PxGroup::new` as `Arc::new(WatchRegistry::new())`.
- **Cleared on `become_follower`** — when the replica steps down from
  leader, call `watch_registry.clear()`. This drops all `Watcher` tx
  senders, closing the clients' outbound streams (clean reconnect).
- **Not cleared on `shutdown`** — `shutdown` drops the whole `PxGroup`,
  which drops the `Arc<WatchRegistry>`, which drops all watchers.

### 2.4 Write-path trigger

In `group_propose.rs`, after `self.fan_out_chosen_notice(&entry, group_id)`
(line 246), before the `trace!`:

```rust
if !self.watch_registry.is_empty() {
    self.watch_coalescer
        .record_chosen(group_id, entry.slot, &entry.payload, &self.watch_registry);
}
```

- **`is_empty` gate** — skips the payload decode + key extraction when
  nobody is watching. The `watch_coalescer` is a field on `PxGroup`
  (see §3).
- **`record_chosen`** — decodes `entry.payload` via `Batch::decode`
  (`lib/crow-kv/src/kv/op.rs:56`), extracts the keys from `ops`, and
  either emits immediately (debounce = 0) or buffers into the coalescer
  (debounce > 0).
- **Foreign-value retry** — the trigger fires for every chosen entry,
  including foreign values (the `adopted_foreign_value` path at line
  255 continues to `'slot_retry`). A foreign value chosen at this slot
  is still a real write to real keys; watchers should be notified. The
  client's own value is retried on a new slot, which fires its own
  notify. This is correct — the watcher sees both the foreign write and
  the retried write.
- **NoOp entries** — `Batch::decode` on an empty payload returns an
  empty `Batch` (no ops); `record_chosen` no-ops. No notify fires for
  repair/noop slots.

## 3. Coalescing

### 3.1 Why

Burst writes to the same prefix (e.g. a batch of disk-status updates)
would generate one notify per write, flooding watchers. A debounce
window coalesces writes to the same prefix into one notify. R78
specifies a default of 100 ms.

### 3.2 WatchCoalescer

New struct in `lib/crow-kv/src/cluster/watch_registry.rs`:

```rust
pub(crate) struct WatchCoalescer {
    debounce_ms: u64,
    /// prefix -> (pending keys set, timer handle). Protected by a
    /// parking_lot::Mutex — the write path is low-contention (one
    /// leader, one coalescer).
    pending: Mutex<HashMap<Bytes, (HashSet<Bytes>, Option<JoinHandle<()>)>>,
}

impl WatchCoalescer {
    pub fn new(debounce_ms: u64) -> Self;
    /// Called from the write-path trigger. Decodes the payload, extracts
    /// keys, and either emits immediately (debounce=0) or buffers.
    pub fn record_chosen(
        &self,
        group_id: u64,
        slot: u64,
        payload: &Bytes,
        registry: &WatchRegistry,
    );
}
```

a. If `debounce_ms == 0`: decode `Batch`, collect keys, call
   `registry.emit(group_id, slot, &keys)` immediately. No buffering.
b. If `debounce_ms > 0`: decode `Batch`, for each key find matching
   prefixes in the registry, insert keys into `pending[prefix]`. If no
   timer is running for that prefix, spawn a `tokio::time::sleep` task
   that after `debounce_ms` locks the map, drains the pending set for
   that prefix, and calls `registry.emit`. The timer handle is stored
   so a subsequent write to the same prefix within the window just adds
   to the set (the existing timer will flush).
c. The timer task captures `Arc<WatchRegistry>` + `Arc<WatchCoalescer>`
   (weak) so it survives even if the group is dropped mid-debounce
   (the weak upgrade fails → no-op).

- **Debounce = 0 is the test default** — tests want immediate notifies
  with no timer flakiness. Production sets 100 ms.
- **Coalescer cleared on step-down** — `become_follower` calls
  `watch_coalescer.flush_and_clear()` which drains all pending sets
  (emitting final notifies) and cancels all timers. This ensures no
  notify is lost on leader change (the last burst is flushed before the
  registry clears).
- **`slot` in coalesced notifies** — the coalesced notify carries the
  highest slot among the coalesced writes (the most recent). Watchers
  use it only for logging; the actual freshness is guaranteed by the
  re-read via the sync path.

## 4. WatchNotify Client

### 4.1 Why

diskdb (and future clients) need a reusable client that opens the
`WatchNotify` stream to the group-0 leader, subscribes to prefixes,
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

    /// Subscribe to `(group_id, prefix)`. Opens a bidi stream to the
    /// group leader (discovered via the topology cache), sends a
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
        group_id: u64,
        prefix: &[u8],
    ) -> Result<WatchSubscription>;
}
```

a. `subscribe` resolves the group-0 leader endpoint via
   `CrowkvClient`'s topology cache (same mechanism as `Put`/`Get`).
b. Opens a gRPC `WatchNotify` bidi stream to that endpoint.
c. Sends `WatchSubscribe { group_id, prefix }`.
d. Spawns a reader task that loops on the inbound stream: on
   `WatchNotify` frame, forwards to the subscriber's `notify_rx`; on
   `WatchNotifyError { not_leader_hint }`, refreshes topology,
   reconnects to the new leader, re-subscribes, and continues. On
   stream error/disconnect, same reconnect logic.
e. `WatchSubscription::drop` sends `WatchUnsubscribe` (if the stream
   is still open) and aborts the reader task.

- **One stream per subscription** — simpler than multiplexing multiple
  prefixes over one stream (each subscription is independent; a
  diskdb instance typically watches 3-4 prefixes). If this becomes a
  connection-count concern, a future optimization multiplexes
  subscriptions over a shared stream.
- **Reconnect backoff** — capped exponential (50 ms → 2 s), matching
  `LearnerStream`'s reconnect policy (`design-crow-kv-rpc.md` §6).

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

Update `wait_trigger`:

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

### 5.3 KeepAlive changes

In `app/crow-diskdb/src/liveness/keepalive.rs`:

a. Add field `sync_trigger: Option<Arc<tokio::sync::Notify>>` —
   `Some` when `notify_enabled`, `None` otherwise (keeps the struct
   usable in tests without notify).
b. Add builder method `.with_sync_trigger(notify: Arc<Notify>) -> Self`.
c. In `trigger()`: if `sync_trigger` is `Some`, return
   `Trigger::TimerOrEvent { interval_fn, notify: sync_trigger }`; else
   fall back to the existing `TimerFn` path.
d. Add method `pub fn trigger_now(&self)` — calls
   `self.sync_trigger.notify_one()` if set. Called by the notify
   handler on each `WatchNotify` frame.
e. **`heartbeat_diskdb` endpoint** — populate the `grpc_endpoint` arg
   (currently `""` at line 166) from the config's `server.listen_addr`.
   This fulfills R78 item 1 (endpoint registration).

### 5.4 Notify handler task

New module `app/crow-diskdb/src/liveness/notify.rs`:

```rust
/// diskdb-side watch/notify handler. Subscribes to group-0 prefixes
/// and wakes the keepalive sync loop on notify.
pub struct NotifyHandler {
    watch: WatchNotifyClient,
    keepalive_trigger: Arc<tokio::sync::Notify>,
    prefixes: Vec<(u64, Vec<u8>)>,  // (group_id, prefix)
}

impl NotifyHandler {
    pub fn new(
        watch: WatchNotifyClient,
        keepalive_trigger: Arc<tokio::sync::Notify>,
        group0_group_id: u64,
    ) -> Self;

    /// Open subscriptions and loop on notify frames. Each frame wakes
    /// the keepalive via `keepalive_trigger.notify_one()`. Runs until
    /// the stop signal.
    pub async fn run(self, stop: tokio::sync::Notify);
}
```

a. On `run`, subscribe to the group-0 prefixes diskdb cares about:
   - `/hw/dg_owner/` (ownership map)
   - `/hw/dg_bind/` (binding map)
   - `/hw/disk/` (disk metadata + status)
b. For each subscription, spawn a reader loop: on `WatchNotify` frame,
   call `keepalive_trigger.notify_one()` (wakes keepalive →
   `KeepAlive::tick()` → re-reads the changed keys from group 0).
c. The notify is a **trigger only** — diskdb re-reads the actual values
   via the normal sync path (`observe_ownership`, `observe_disks`).
   This keeps the notify payload small and avoids duplicating the read
   path.
d. On subscription drop/reconnect, the `WatchNotifyClient` handles
   reconnect internally (§4.2); the handler just keeps reading.

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

## 6. Configuration

### 6.1 diskdb config

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

### 6.2 crow-kv config

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
- `src/cluster/group.rs` — add `watch_registry: Arc<WatchRegistry>` +
  `watch_coalescer` fields to `PxGroup`; clear on `become_follower`.
- `src/cluster/group_propose.rs` — add notify trigger after
  `fan_out_chosen_notice` (line 246), gated by `is_empty`.
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
  trigger; populate `grpc_endpoint` in `heartbeat_diskdb`.
- `src/liveness/mod.rs` — export `notify`.
- `src/bg_task.rs` — add `Trigger::TimerOrEvent` variant +
  `wait_trigger` branch.
- `src/ddb_config.rs` — add `NotifyConfig` + `notify` field on
  `DdbConfig`.
- `src/main.rs` — wire `NotifyHandler` when `notify_enabled`; pass
  `sync_trigger` to keepalive.

**Deferred (item 6, post-R74)**:
- `lib/crow-diskdb-client/src/lib.rs` — `WatchNotifyClient`
  subscription on `/srv/diskdb/` for proactive endpoint cache refresh.
  Depends on R74's `DiskdbClient` + endpoint cache.

## Complexity

**High.** The crow-kv watch/notify extension is a new subsystem: a
bidi gRPC stream, a per-group watch registry with concurrent
subscribe/unsubscribe/emit, a write-path trigger that decodes every
chosen payload (gated by an `is_empty` fast path), and a debounce
coalescer with timer tasks. The hardest parts are (1) the leader-step-
down lifecycle — clearing the registry and flushing the coalescer
without losing notifies, while closing client streams cleanly; (2) the
`WatchNotifyClient` reconnect logic — detecting stream close, refreshing
topology, re-subscribing, and keeping the subscriber's channel open
across reconnects; (3) keeping the write-path overhead near-zero when
no watchers are registered (the `is_empty` gate + `try_send` non-
blocking emit). The diskdb side is medium complexity (trigger
extension + notify handler task). The `LearnerStream` pattern is
reused for the bidi stream shape; the `Batch::decode` is reused for
payload decoding.

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
  fill it, emit 3 keys; assert no panic (try_send drops silently); the
  watcher misses notifies (safety-net covers this).
- `is_empty` — true before subscribe, false after, true after
  unsubscribe + clear.
- `clear` — subscribe 3 watchers, `clear()`, assert `is_empty` and all
  channels are closed (sender dropped).

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
  woke the keepalive), not 60 s (the safety-net interval). Then
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

## Module Structure

```
lib/crow-kv/src/
  rpc/
    proto/kv.proto              # +WatchNotify messages + RPC
    kv_service.rs               # +watch_notify handler
  cluster/
    watch_registry.rs           # NEW: WatchRegistry, Watcher, WatchCoalescer
    group.rs                    # +watch_registry, +watch_coalescer fields
    group_propose.rs            # +notify trigger after fan_out_chosen_notice
    group_config.rs             # +watch_notify_debounce_ms
    mod.rs                      # export watch_registry

lib/crow-kv-client/src/
  watch_notify.rs               # NEW: WatchNotifyClient, WatchSubscription
  lib.rs                        # export watch_notify

app/crow-diskdb/src/
  liveness/
    notify.rs                   # NEW: NotifyHandler
    keepalive.rs                # +sync_trigger, +trigger_now, +endpoint
    mod.rs                      # export notify
  bg_task.rs                    # +Trigger::TimerOrEvent
  ddb_config.rs                 # +NotifyConfig
  main.rs                       # wire NotifyHandler when notify_enabled
```

## Config Extensions

- **`DdbConfig.notify`** (`NotifyConfig`):
  - `notify_enabled: bool` — default `false`. Static (requires restart).
  - `notify_debounce_ms: u64` — default `100`. Dynamic.
- **`CrowKVConfig.watch_notify_debounce_ms`** — default `100`.
  Dynamic (live-reload via `set_from_config`).
- **`validate()`** — no new constraints; existing
  `sync.sync_interval_secs > 0` check covers the safety-net interval.

## Server Wiring

`app/crow-diskdb/src/main.rs` startup sequence (additions):

1. After building `keepalive` (line 126): if
   `config.load().notify.notify_enabled`, create an
   `Arc<tokio::sync::Notify>` (`sync_trigger`), call
   `keepalive.with_sync_trigger(Arc::clone(&sync_trigger))`.
2. After building the bg runner (line 197): if `notify_enabled`,
   construct `WatchNotifyClient::from_shared(Arc::clone(&kv_client))`
   and `NotifyHandler::new(watch, sync_trigger, sys_group)`. Spawn
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
  gRPC stream per `(group_id, prefix)` subscription. A diskdb instance
  watching 4 prefixes opens 4 streams. If connection count is a
  concern, multiplex all subscriptions over one stream (send multiple
  `WatchSubscribe` frames on one bidi stream). The proto already
  supports this (the inbound stream is a sequence of frames). **No
  change needed for v1** — the `WatchNotifyClient` can be extended
  later to multiplex without proto changes.
