<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R82: kv — Watch/Notify Coalescing (Debounce)

**Problem**:

- **Current behavior + impact** — the watch/notify extension ships
  without coalescing: `PxLearner::apply_entry` calls `WatchRegistry::emit`
  directly, producing one notify frame per changed key per matching
  prefix. A burst write to a watched prefix (e.g. a diskdb
  `batch_write` touching 10 disks under `/hw/disk/`) generates 10
  separate `WatchNotify` frames, each waking the subscriber's
  keepalive and triggering a re-read via the normal `Get` path. Under
  sustained burst load (periodic discovery scans, bulk status
  updates), this amplifies both notify traffic and downstream read
  load — the notify mechanism can be strictly worse than the
  fixed-interval polling it replaces. Not a correctness issue: the
  safety-net poller covers any missed notifies. The impact is purely
  load/efficiency under bursty workloads.
- **Design pointers** —
  [`doc/design/kv/design-crow-kv-watch-notify.md`](../design/kv/design-crow-kv-watch-notify.md)
  §3 (Coalescing — deferred) states the rationale and points
  back to this backlog item. The original coalescer design (struct
  shape, timer-task lifecycle, per-prefix debounce, step-down flush)
  was specified in the same design draft but removed from the
  watch/notify extension's scope after the timer-task wiring proved
  incomplete (the spawned timer captured no registry/coalescer refs, so
  buffered keys were silently dropped). New work; the reference model
  is etcd watch (which coalesces at the mvcc backend, not the apply
  path).
- **Use scenarios** —
  - **Disk discovery burst** — diskdb runs a periodic discovery
    scan; one `batch_write` updates 10+ disk records under
    `/hw/disk/`; the owning diskdb's keepalive is woken once (not 10
    times) and performs one re-read sweep.
  - **Bulk ownership transfer** — an operator reassigns 5
    disk-groups in one batch; the `/hw/dg_owner/` watcher receives
    one coalesced notify with all 5 changed keys, not 5 separate
    frames.
  - **Continuous churn** — a prefix under continuous write pressure
    (writes arriving faster than the debounce window) is
    rate-limited to one notify per `debounce_ms` interval, bounding
    subscriber wakeups.

**Solution**:

- **No clear solution yet — deferred to design.** The core design
  questions (timer-task ownership model, Arc/Weak capture, per-prefix
  vs per-watcher debounce, step-down flush semantics) need a design
  draft to resolve. The high-level shape is known from the original
  watch/notify design draft, but the timer-task wiring gap that caused
  the extension to drop the feature must be addressed properly.

- **One-line summary**: per-prefix debounce coalescer with
  timer-task flush, wired into the apply path between
  `apply_entry` and `WatchRegistry::emit`.

- **Numbered work items**:
  1. **`WatchCoalescer` struct**
     (`lib/crow-kv/src/cluster/watch_registry.rs`) — new struct
     holding `debounce_ms: AtomicU64` + `pending:
     Mutex<HashMap<Vec<u8>, (HashSet<Vec<u8>>,
     Option<JoinHandle<()>>)>>`. The apply-path hook calls
     `record_chosen` instead of `emit` directly; `record_chosen`
     either emits immediately (`debounce_ms == 0`) or buffers keys
     per prefix and spawns a timer task.
  2. **Timer task wiring** — the spawned `tokio::time::sleep` task
     must capture `Weak<WatchRegistry>` + `Weak<WatchCoalescer>` so
     it survives group drop mid-debounce (weak upgrade fails →
     no-op). On wake: lock `pending`, drain the prefix's key set,
     call `registry.emit`. This is the gap that caused the
     coalescer's removal from the initial watch/notify extension —
     the original code captured no refs and silently dropped buffered
     keys.
  3. **`PxLearner` wiring** (`lib/crow-kv/src/paxos/learner.rs`) —
     the `watch_registry` `OnceLock` gains a third element
     (`Arc<WatchCoalescer>`); `set_watch_registry` takes the
     coalescer; the apply-path hook calls `coalescer.record_chosen`
     instead of `registry.emit`.
  4. **`PxGroup` wiring** (`lib/crow-kv/src/cluster/group.rs`) —
     `watch_coalescer: Arc<WatchCoalescer>` field; constructed in
     `PxGroup::new` with the configured `debounce_ms`; wired into
     the learner via `set_watch_registry`.
  5. **Step-down flush**
     (`lib/crow-kv/src/cluster/group_election.rs`,
     `lib/crow-kv/src/cluster/group_propose.rs`) — `step_down` and
     the two propose-path direct `become_follower` calls call
     `watch_coalescer.flush_and_clear()` before
     `watch_registry.clear()`, emitting any pending notifies so no
     notify is lost on leader change.
  6. **Configuration** (`lib/crow-kv/src/common/config.rs`,
     `app/crow-diskdb/src/ddb_config.rs`) — add
     `watch_notify_debounce_ms` to `CrowKVConfig` (default 100,
     dynamic) and `notify_debounce_ms` to `NotifyConfig` (default
     100, documentation/operator intent — the crow-kv config is
     authoritative). Live reload via `set_from_config` propagates
     to the coalescer's `AtomicU64`.

- **Flow diagram**:

```
                   apply_entry (PxLearner)
                         │
                         ▼
               ┌─────────────────────┐
               │ WatchCoalescer      │
               │  debounce_ms == 0?  │
               │  ├─ yes → emit now  │
               │  └─ no  → buffer    │
               │           + spawn   │
               │           timer     │
               └────────┬────────────┘
                        │ timer fires (debounce_ms)
                        ▼
               ┌─────────────────────┐
               │ drain pending[pfx]  │
               │ registry.emit(...)  │
               └─────────────────────┘
                        │
                        ▼
               ┌─────────────────────┐
               │ WatchRegistry.emit  │
               │ → watcher channels  │
               └─────────────────────┘
```

- **Edge cases at a glance**:
  - Group dropped mid-debounce → timer task's `Weak` upgrade fails →
    no-op (no dangling emit).
  - Leader step-down with pending keys → `flush_and_clear` emits
    final notifies before registry clears.
  - Second write to same prefix within window → adds to existing
    `HashSet`, does not reset timer (fixed trailing-edge).
  - Write to unwatched key → no pending entry created, no timer
    spawned.
  - `debounce_ms` changed live → new calls read the new value;
    existing timers fire at their original schedule.

**Dependencies**:

- Depends on the watch/notify extension (`WatchRegistry`,
  `WatchCoalescer` apply-path hook, `WatchNotifyClient` — see
  [`doc/design/kv/design-crow-kv-watch-notify.md`](../design/kv/design-crow-kv-watch-notify.md)).
  That extension must be landed first; this item adds the coalescer
  layer on top.
- No items depend on R82 — it is a pure load optimization.

**Acceptance**:

**WatchCoalescer** (`lib/crow-kv/src/cluster/watch_registry.rs`):
- `debounce_ms=0`, `record_chosen` a payload with 2 keys →
  immediate emit (no timer). Unit test.
- `debounce_ms=50`, `record_chosen` 3 payloads to the same prefix
  within 10 ms → one `WatchNotify` with all keys after ~50 ms (use
  `tokio::time::pause`). Unit test.
- `record_chosen` writes to two prefixes → two separate notifies
  (one per prefix timer). Unit test.
- `flush_and_clear` with 2 buffered keys → one final notify with
  the buffered keys + no timers remain. Unit test.
- `record_chosen` a payload whose key matches no registered prefix
  → no pending entry created, no notify. Unit test.
- Group dropped mid-debounce (drop all `Arc`s, advance timer) → no
  panic, no emit (weak upgrade fails). Unit test.

**Step-down flush**:
- Buffer 2 keys, call `flush_and_clear` → assert one notify with
  both keys, then `registry.clear()` → channels closed. Unit test.

**E2E (diskdb)**:
- `notify_debounce_ms=100`, burst-write 10 disks in one
  `batch_write` → one coalesced notify wakes keepalive once, not 10
  times. E2E test.
- `notify_debounce_ms=0`, burst-write 10 disks → 10 separate
  notifies (proves coalescing is the debounce path, not emit).
  E2E test.

- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`

**Open Questions**:

- **Q1 — `debounce_ms` ownership: crow-kv config vs per-subscription.**
  The coalescer runs on the crow-kv leader (group 0). Options: (a)
  one global `debounce_ms` on `CrowKVConfig` — simple, but all
  watchers share the same window; (b) per-subscription debounce sent
  as a `WatchSubscribe` parameter — more ergonomic, adds per-watcher
  debounce timer state. Recommendation: (a) for v1 — one global
  debounce on the leader; revisit if per-subscription debounce is
  needed. Cannot be resolved autonomously — it is an API-ergonomics
  trade-off that needs a human decision.
