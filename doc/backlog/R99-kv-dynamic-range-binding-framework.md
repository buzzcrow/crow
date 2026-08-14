<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R99: kv — Dynamic Range Binding Framework + chunkdb Instance Sharding

**Problem**:

- **Current behavior + impact** — chunkdb is designed stateless (design
  §3.6) so that any instance can serve any request, and R90's client
  routes to "any registered instance" (R90 Open Question). This works
  for a small cluster but does not scale: a single chunkdb instance
  becomes a bottleneck for allocation throughput and metadata query
  load as the chunk count grows. There is no mechanism to shard chunks
  across multiple chunkdb instances — no hash-range → chunkdb-instance
  binding, no reject-and-retry protocol, no client routing to the
  correct instance. Without sharding, chunkdb capacity is limited to
  what one instance can handle.

  Separately, diskdb has the same binding pattern but operator-manual:
  `OwnerMapValue` (disk-group → diskdb instance) and `BindMapValue`
  (disk-group → paxos group) are written by the operator via the
  console (design §3.2, §5 "Map semantics"). There is no dynamic
  monitor thread that automatically rebinds when instances join or
  leave — the operator must do it manually. This is operationally
  fragile: a chunkdb/diskdb instance crash leaves its ranges
  un-served until the operator manually rebinds.

  Both problems share the same shape: a **key → service-instance
  binding** stored in group-0, read by clients, updated dynamically
  when instances change. A common framework avoids duplicating the
  binding manager + binding client + reject-and-retry protocol across
  chunkdb and diskdb.

- **Design pointers** —
  [`doc/design/chunkdb/design-crow-chunkdb.md`](../design/chunkdb/design-crow-chunkdb.md)
  §3.6 (stateless with KV persistence — any instance can serve, but
  sharding is needed for scale), §5.4a (logical hash bucket system —
  chunk ID → 16-bit bucket → KV group; the same hash can drive
  chunkdb instance sharding),
  [`doc/design/diskdb/design-crow-diskdb.md`](../design/diskdb/design-crow-diskdb.md)
  §3.2 (disk-group → paxos group binding via a table, not hash —
  dynamic scaling without rehashing), §5 ("Map semantics" —
  `OwnerMapValue`, `BindMapValue` are operator-manual today),
  [`doc/design/kv/design-crow-kv-group0.md`](../design/kv/design-crow-kv-group0.md)
  §2.1 (`crow-kv-client` is the single sysdata API surface),
  §2.6 (two monitoring models: push + pull), §3.1 (key layout —
  `/hw/dg_owner/...`, `/hw/dg_bind/...`),
  [`doc/design/kv/design-crow-kv-watch-notify.md`](../design/kv/design-crow-kv-watch-notify.md)
  §4 (`WatchNotifyClient` for real-time binding updates),
  `lib/crow-kv-client/src/config.rs` (`NotLeaderHint` retry pattern —
  the reject-and-retry protocol to follow). aioss analog: aioss
  hashes chunk IDs to metadb partitions directly (no chunkdb instance
  sharding — aioss chunkdb instances are all stateless and share the
  same metadb); CROW adds chunkdb instance sharding (new work beyond
  aioss) because CROW's KV groups are partitioned and chunkdb
  instances can be dedicated to ranges for throughput.

- **Use scenarios** —
  - **Client routes to the correct chunkdb instance** — a client
    hashes a chunk ID → hash range → looks up the chunkdb instance
    binding in group-0 (cached) → sends the RPC to that instance.
    The instance processes the request (routing to the correct KV
    group for persistence via R88). No random instance selection.
  - **chunkdb rejects out-of-range request** — a client's binding
    cache is stale (range ownership moved); the client sends a
    request to the old instance; the old instance detects the chunk
    ID is outside its owned range, rejects with a `NotMyRange` error
    that includes the current owner's range + instance endpoint; the
    client refreshes its binding cache and retries against the
    correct instance. Follows the `NotLeaderHint` pattern.
  - **New chunkdb instance joins** — an operator starts a new chunkdb
    instance; the group-0 binding monitor detects the new instance
    (via service registry), splits an existing hash range, updates the
    binding table in group-0; watch/notify pushes the update to all
    chunkdb instances + clients; the new instance starts serving its
    range; the old instance rejects out-of-range requests with the
    new owner hint.
  - **chunkdb instance crashes** — a chunkdb instance crashes; the
    group-0 binding monitor detects the loss (service registry
    expiry), reassigns its hash range to a surviving instance, updates
    the binding table; clients refresh + retry against the new owner.
    No operator intervention needed.
  - **diskdb disk-group rebinding** — the same framework dynamically
    rebinds a disk-group's paxos group when a paxos group is added or
    a diskdb instance moves — replacing the operator-manual
    `BindMapValue` write (design §5 "Map semantics") with automatic
    monitoring + rebinding.
  - **Range split for load balancing** — one chunkdb instance is
    overloaded (high allocation rate); the binding monitor splits its
    range in half, assigns one half to a new instance; load is
    rebalanced without moving existing chunk metadata (the KV group
    routing is unchanged — only the serving instance changes).

**Solution**:

**No clear solution yet — deferred to design.** The high-level shape
is known (hash-range → instance binding in group-0, reject-and-retry
protocol, dynamic monitor), but two design questions are unsettled:
(1) whether to build a common framework shared between chunkdb and
diskdb or two separate implementations, and (2) where the framework
library lives (`crow-kv-client`, `crow-common`, `crow-kv-server`, or a
new crate). These need a design draft before implementation.

- **One-line summary**: shard chunkdb instances by chunk hash range
  with a group-0 binding table, a reject-and-retry protocol
  (`NotMyRange` → refresh → retry), and a dynamic binding monitor
  thread — built as a common framework reusable by diskdb's
  disk-group binding.

- **Numbered work items**:
  1. **chunkdb instance binding schema** —
     `lib/crow-protocol/src/proto/sysdata_type.proto` (extend):
     - `ChunkdbRangeBindingValue` — hash range (`range_start`,
       `range_end`) → chunkdb `instance_id` + `grpc_endpoint`; stored
       in group-0 with key pattern
       `/chunkdb/range_bind/<range_start>`.
     - `ChunkdbRangeMigrationValue` — migration state for range
       ownership transfers (old instance, new instance, state:
       `Copying`/`Cutover`/`Complete`); follows the R88
       `BucketMigrationState` pattern.
     - Reuses the 16-bit logical bucket from design §5.4a as the
       range unit (ranges are bucket ranges, 0-65535).
  2. **Binding client** (read + cache + retry) —
     `lib/crow-kv-client/src/` (extend, per design group0 §2.1
     "crow-kv-client is the single sysdata API surface"):
     - `RangeBindingClient` — fetches the binding table from group-0,
       caches it locally (`DashMap<range, instance_endpoint>`),
       subscribes to watch/notify for real-time updates. Follows the
       R86 `BindingCache` pattern.
     - `route(key) -> (instance_endpoint, range)` — hash the key
       (chunk ID) → bucket → find the owning range → return the
       instance endpoint.
     - Reject-and-retry: on `NotMyRange` error, refresh the binding
       cache, re-route, retry. Follows the `NotLeaderHint` retry
       pattern in `crow-kv-client/src/config.rs`.
  3. **chunkdb server range enforcement** —
     `app/crow-chunkdb/src/range_guard.rs` (new):
     - On every RPC, check the chunk ID's bucket is within the
       instance's owned ranges; if not, return a `NotMyRange` error
       with the current owner's range + endpoint (from the binding
       cache).
     - Range ownership is read from group-0 at startup + updated via
       watch/notify.
  4. **Dynamic binding monitor** —
     location TBD (see Open Questions):
     - A background thread/loop that monitors the service registry for
       chunkdb instance join/leave events; on change, computes a new
       range assignment (split/merge ranges), writes the updated
       binding table to group-0, and triggers migration for moved
       ranges.
     - Algorithm: consistent hashing or range splitting — see Open
       Questions.
     - The same monitor handles diskdb disk-group → paxos group
       rebinding (replacing the operator-manual `BindMapValue` write).
  5. **chunkdb client routing integration** —
     `lib/crow-chunkdb-client/src/client.rs` (update R90):
     - Replace the "any registered instance" routing (R90 Open
       Question) with `RangeBindingClient::route(chunk_id)` →
       specific instance.
     - On `NotMyRange`, refresh + retry against the hinted instance.
  6. **diskdb disk-group binding migration** —
     `app/crow-diskdb/src/` (update, future — may be a separate
     follow-up):
     - Migrate the operator-manual `BindMapValue` write (design §5)
       to the dynamic binding monitor; the monitor automatically
       rebinds disk-groups when paxos groups are added or diskdb
       instances move.
     - This is a diskdb change, not a chunkdb change — filed here
       because it shares the framework. May be split into a separate
       requirement if the scope grows.

- **Flow diagram**:

```
  client ──► RangeBindingClient::route(chunk_id)
       │  hash chunk_id → bucket → find owning range
       ▼
  chunkdb instance (owner of the range)
       │
       ├── chunk_id in owned range ──► process RPC
       │                              (route to KV group via R88 for persist)
       │
       └── chunk_id NOT in owned range ──► NotMyRange error
                                           (includes current owner range + endpoint)
                │
                ▼
  client refreshes binding cache ──► re-route ──► retry against correct instance


  group-0 binding monitor (item 4)
       │
       ├── monitor service registry (chunkdb instance join/leave)
       ├── on change: compute new range assignment (split/merge)
       ├── write updated binding table to group-0
       ├── watch/notify pushes update to all instances + clients
       └── trigger migration for moved ranges (dual-serve during cutover)
```

- **Edge cases at a glance**:
  - Binding cache empty on client startup → synchronous fetch from
    group-0 on first `route`; no error to caller.
  - All chunkdb instances for a range are down → client exhausts
    retries; returns `Unavailable`; the binding monitor reassigns the
    range to a surviving instance (if any).
  - Range split mid-request → the old instance still serves until
    cutover; after cutover, it rejects with `NotMyRange` pointing to
    the new owner; the client refreshes + retries.
  - chunkdb instance receives a request for a range it just acquired
    (cutover just happened) → it processes it (the range is in its
    updated binding); no issue.
  - Binding monitor itself crashes → bindings stay at their last
    state (no automatic rebinding until the monitor restarts); no
    data loss, just no scaling events.
  - Hash collision (two chunk IDs in the same bucket) → the bucket
    maps to one range → one instance; no issue (bucket granularity is
    the routing unit, not individual chunk IDs).
  - diskdb disk-group rebinding mid-allocation → the diskdb instance
    finishes the in-flight allocation on the old paxos group, then
    switches to the new binding for subsequent allocations; no torn
    zone records (the binding change is a routing change, not a data
    migration).

**Dependencies**:

- **R85** (foundation) — chunkdb server + client crates must exist.
- **R88** (storage/routing) — chunk → KV group routing is separate
  from chunk → chunkdb instance routing; R99 adds the instance
  routing layer on top of R88's KV group routing. R88 is not blocked
  by R99 (R88 works without sharding — all instances route to the
  same KV groups).
- **R90** (client) — R99 resolves R90's Open Question (routing);
  the client uses `RangeBindingClient` instead of "any instance".
- **R86** (topology) — the binding cache follows the same watch/notify
  + periodic refresh pattern as the topology cache; may share
  infrastructure.
- **`WatchNotifyClient`** in `crow-kv-client` — real-time binding
  updates.
- **`ServiceRegistryClient`** in `crow-kv-client` — instance
  discovery for the binding monitor.
- **diskdb core (R70-R76)** — item 6 (diskdb binding migration) depends
  on diskdb's existing `BindMapValue` + `OwnerMapValue` schema.
- **R91** (E2E) — E2E tests must cover sharding (multiple instances,
  reject-and-retry, range split); R91 should be updated after R99
  lands.

**Acceptance**:

**chunkdb instance routing**:
- `RangeBindingClient::route(chunk_id)` with a binding table mapping
  buckets 0-32767 → instance A, 32768-65535 → instance B → a chunk
  ID hashing to bucket 20000 routes to instance A; a chunk ID hashing
  to bucket 40000 routes to instance B. Unit test.
- Binding table updated in group-0 (split range); watch/notify fires;
  `RangeBindingClient` cache updates within 1s; the next `route` for
  an affected bucket returns the new instance. Integration test.

**Reject-and-retry**:
- Client sends a request to instance A for a chunk ID whose range was
  moved to instance B; instance A returns `NotMyRange` with the
  current owner (instance B's range + endpoint); client refreshes +
  retries against instance B; succeeds. Integration test.
- `NotMyRange` error includes the current owner's range start, range
  end, and instance endpoint → client can route directly without a
  full cache refresh. Unit test.

**Dynamic binding monitor**:
- A new chunkdb instance joins the service registry; the monitor
  detects it within the polling interval, splits an existing range,
  writes the updated binding to group-0; the new instance starts
  serving its range within 2× the polling interval. Integration test.
- A chunkdb instance crashes (service registry expiry); the monitor
  reassigns its range to a surviving instance; clients refresh +
  retry; requests succeed against the new owner. Integration test.

**Range split for load balancing**:
- One instance is overloaded; the monitor splits its range; the new
  instance takes half the load; existing chunk metadata is not moved
  (KV group routing unchanged — only the serving instance changes).
  Integration test.

**diskdb disk-group rebinding (item 6, if in scope)**:
- The monitor rebinds a disk-group's paxos group; diskdb switches to
  the new binding for subsequent allocations; in-flight allocations
  complete on the old group. Integration test.

**Edge cases**:
- Binding cache empty on startup → first `route` triggers synchronous
  fetch; no error. Unit test.
- All instances for a range down → client returns `Unavailable` after
  exhausting retries. Unit test.
- Range split mid-request → old instance serves until cutover, then
  rejects with `NotMyRange`; client retries against new owner.
  Integration test.

**Lint + test commands**:
- `pixi run cargo fmt --all -- --check` passes.
- `pixi run cargo clippy --all-targets -- -D warnings` passes.
- `pixi run test-chunkdb` (sharding + reject-and-retry integration
  tests pass).
- `pixi run test-diskdb` (disk-group rebinding tests pass, if item 6
  is in scope).

**Open Questions**:

- **Common framework or separate implementations?** chunkdb instance
  sharding and diskdb disk-group binding share the same shape (key →
  instance binding in group-0, dynamic monitor, reject-and-retry).
  Options: (a) build one common framework with pluggable algorithms
  (range-based for chunkdb, table-based for diskdb) — more upfront
  work, less duplication; (b) build two separate implementations that
  share the group-0 binding schema pattern but not code — simpler,
  some duplication. Trade-off: (a) is cleaner if diskdb migration is
  in scope; (b) is faster if chunkdb sharding is urgent and diskdb
  migration can wait. Recommendation: (a) — the user explicitly asked
  for a common framework, and the diskdb binding is operator-manual
  today (a pain point). Design decision — needs human confirmation
  on scope (is diskdb migration in R99 or a follow-up?).

- **Where does the framework library live?** Options: (a)
  `crow-kv-client` — the binding client (read + cache + retry) lives
  here (already the sysdata API surface per design group0 §2.1); the
  binding monitor lives in `crow-kv-server` (group-0 is the natural
  home for cluster-wide binding management); (b) `crow-common` —
  shared binding utilities (but `crow-common` is for low-level
  primitives like EC, not sysdata clients); (c) a new
  `crow-binding` crate — cleanest separation but adds a crate.
  Trade-off: (a) reuses the existing sysdata API surface and keeps
  the monitor in group-0 (where the service registry + watch/notify
  already live); (b) is a poor fit (wrong layer); (c) is clean but
  premature if the framework is small. Recommendation: (a) — binding
  client in `crow-kv-client`, binding monitor in `crow-kv-server`
  (group-0). Design decision.

- **Range assignment algorithm — consistent hashing or explicit
  ranges?** Options: (a) consistent hashing (instances own virtual
  nodes on a ring; adding/removing an instance moves only adjacent
  virtual nodes) — minimal data movement, but ranges are implicit;
  (b) explicit bucket ranges (instances own contiguous bucket ranges;
  adding an instance splits a range, removing merges) — simple to
  reason about, ranges are explicit in the binding table. Trade-off:
  (a) minimizes migration but is harder to debug; (b) is simpler and
  matches the R88 bucket-range pattern. Recommendation: (b) —
  explicit bucket ranges, matching R88's design §5.4a. Design
  decision.

- **Binding monitor location — group-0 leader or separate service?**
  The monitor writes to group-0, so it must run where it can write to
  group-0. Options: (a) a background task in the `crow-kv-server`
  group-0 leader (the leader already runs sysdata management loops);
  (b) a separate `crow-binding-manager` service. Trade-off: (a) is
  simpler (no new service) and the group-0 leader is the natural
  cluster-wide coordinator; (b) is cleaner separation but adds a
  service to deploy. Recommendation: (a) — background task in the
  group-0 leader. Design decision.

- **diskdb migration in R99 or a follow-up?** Item 6 (diskdb
  disk-group rebinding) shares the framework but is a diskdb change.
  Options: (a) include it in R99 (one requirement, one framework,
  two use cases); (b) split it into a separate R-item (R99 is
  chunkdb-only, diskdb migration is a follow-up). Trade-off: (a)
  delivers the common framework value immediately but increases R99
  scope; (b) is faster to land for chunkdb but delays the diskdb
  benefit. Recommendation: (b) — R99 lands the framework + chunkdb
  sharding; diskdb migration is a follow-up requirement that reuses
  the framework. Design decision — needs human confirmation.
