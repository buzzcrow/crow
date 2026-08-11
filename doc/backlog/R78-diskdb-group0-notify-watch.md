<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R78: diskdb — Group-0 Notify/Watch (Replace Polling Sync)

**Problem**: R71 implements the group-0 sync loop with fixed-interval
polling (default 10 s). Every diskdb instance polls group 0 on each
tick to detect ownership changes, new disks, and status updates. This
works for v1 but has two drawbacks:

- **Latency** — a status change (disk bad, disk-group reassigned) takes
  up to one sync interval (10 s) to be observed. For failure detection
  and ownership transfer, faster propagation is desirable.
- **Wasted reads** — every poll is a prefix scan of group 0 even when
  nothing changed. With many diskdb instances, this is redundant load
  on group 0.

The design doc (§10) raises this as an open question:

> a zookeeper-like notify/watch where group 0 pushes refresh
> notifications to registered diskdb endpoints. Each diskdb registers
> its endpoint on sync; group 0 notifies on change. This needs a
> design review of how a paxos group can support watch/notify (not a
> native crow-kv feature today).

**Solution**: Add a watch/notify mechanism so group 0 pushes
hw-status-change and ownership-change notifications to registered
diskdb endpoints, replacing fixed-interval polling as the primary
change-detection mechanism.

1. **diskdb endpoint registration** — each diskdb instance registers
   its notify endpoint (host:port) in group 0 on startup and on every
   sync tick (already in R71's instance heartbeat). The registration
   carries the endpoint URL and the `instance_id`.

2. **crow-kv watch/notify extension** — add a watch/notify capability
   to crow-kv so that group 0 can push notifications to registered
   endpoints when watched keys change. This is a crow-kv extension
   (new sub-design doc). Design questions to resolve:

   - **Watch scope** — per-key watch, per-prefix watch, or per-group
     watch? diskdb needs per-prefix (e.g. watch
     `/diskdb/node/{node_id}/`, `/diskdb/map/owner/`,
     `/diskdb/map/bind/`).
   - **Notify transport** — push over gRPC (a new `WatchNotify` stream
     from group 0 to diskdb), or HTTP POST to the registered endpoint?
     gRPC bidi stream is the natural fit (matches crow-kv's existing
     `LearnerStream` pattern); HTTP POST is simpler but less
     idiomatic.
   - **Reliability** — what happens if a diskdb endpoint is down when
     group 0 pushes a notification? Options: retry with backoff, fall
     back to polling on notify failure, or both (notify + polling as a
     safety net).
   - **Group-0 write path** — where does the notify trigger fire? On
     `Put` / `batch_write` to a watched prefix, the leader emits a
     notify to all registered watchers after the write is chosen. This
     is the main crow-kv design work.
   - **Coalescing** — burst writes to the same prefix should coalesce
     into one notify (debounce window, e.g. 100 ms) to avoid notify
     storms.

3. **diskdb notify handler** — add a notify handler to the diskdb
   server:

   - On receiving a notify for a watched prefix, trigger an immediate
     `sync_once()` (from R71's `SyncLoop`). The sync reads the
     changed keys from group 0 and applies them to the in-memory
     state.
   - The notify is a **trigger**, not a transport for the data itself
     — diskdb still reads the actual values from group 0 via the
     normal sync path. This keeps the notify payload small and avoids
     duplicating the read path.

4. **Polling as a safety net** — keep the fixed-interval sync loop
   (R71) as a safety net, but increase the interval (e.g. 60 s instead
   of 10 s) since notifies handle the common case. The polling loop
   catches any missed notifies (reliability fallback).

5. **Configuration** — add to the diskdb config:

   - `notify_enabled` (default false in v1, true when this feature
     ships) — toggle between polling-only and notify+polling.
   - `notify_poll_interval_secs` (default 60) — the safety-net polling
     interval when notify is enabled.
   - `notify_debounce_ms` (default 100) — crow-kv-side coalescing
     window.

**Scope** (expected changed files):

- `doc/design/kv/design-crow-kv-watch-notify.md` — new sub-design for
  the crow-kv watch/notify extension (watch scope, transport,
  reliability, coalescing, group-0 write-path trigger).
- `lib/crow-kv/src/...` — crow-kv watch/notify implementation (watch
  registry, notify emission on write, gRPC `WatchNotify` stream or
  HTTP POST).
- `app/crow-diskdb/src/sync/mod.rs` — notify handler that triggers
  `sync_once()` on notify; polling-as-safety-net mode.
- `app/crow-diskdb/src/config.rs` — notify config items.

**Dependencies**: R71 (sync loop, endpoint registration, instance
heartbeat).

**Non-goals**:

- Not a general-purpose pub/sub — scoped to group-0 sysdata changes
  (node/disk/disk-group metadata, ownership map, binding map).
- Not a replacement for the sync read path — diskdb still reads the
  actual values from group 0; the notify is only a trigger.
- Not in v1 — v1 ships with fixed-interval polling (R71). This
  requirement is a follow-up.
