# CrowKV - Plan: P4 RPC / Client

Depends on: [`plan.md`](plan.md) §1 P4, [`requirement.md`](requirement.md) §7.1, §7.4,
§10, [`design.md`](design.md) §2, §7, [`design/design-rpc.md`](design/design-rpc.md)

Temporary per-milestone plan per [`plan.md`](plan.md)'s own convention ("detailed
per-milestone plans are created temporarily before each step"). Verified against
code, not just design docs, on 2026-07-11 — same method as the just-closed
`todo-sm.md` gap analysis for P3.

## 0. tl;dr

**Resolved (2026-07): Model B.** `requirement.md`/`design.md`/`design/design-rpc.md`
described a **Group-0** system group (static `num_groups`, `hash(key) -> group_id`,
`DescribeCluster` RPC) that had **zero implementation**. What was actually built
and tested is a different model: an operator-driven HTTP management API
(`crowkv-server/src/mgmt_api.rs`) for creating stores/groups, with every KV RPC
taking an explicit `group_id`, and per-group config files for persistence. §6
records the decision to keep that model and retire the Group-0 design; the
core docs have been amended accordingly. §5 has the resulting milestones
(C1-C3) and the new `crowkv-client` crate design. M1/M2/M4 were already done
ahead of schedule (§1); M3 ("client library") is the remaining work, now in
progress against this plan.

---

## 1. What's already there (verified in code)

| Piece | Evidence |
| --- | --- |
| Full `.proto` message set for consensus + KV, `version: u32` at tag 1, append-only | `crowkv/src/rpc/proto/pxos.proto`, `crowkv/src/rpc/proto/kv.proto` |
| `PxService`: `Prepare`/`Accept`(deprecated)/`PreVote`/`RequestVote`/`Heartbeat`/`StepDown`/`LearnerStream` (bidi) | `crowkv/src/rpc/px_service.rs`, `pxos.proto` — this *is* `plan.md`'s "VoteService", just consolidated into one service rather than split out. A deliberate, already-shipped deviation from the milestone table's literal wording, same class of thing as `todo-sm.md`'s findings for P3. |
| `SnapshotService` (`StreamSnapshot`, server-streaming) — new-member bootstrap | `crowkv/src/rpc/snapshot_service.rs`; wired into `mgmt_api.rs`'s add-replica flow |
| `KvService`: `Put`/`Get`/`Delete`/`BatchWrite`/`Scan`, `client_id`/`seq` dedup, `request_id`/`request_create_ms` trace fields | `crowkv/src/rpc/kv_service.rs`, `kv.proto` |
| `NotLeaderHint` (as a response field, not a distinct message) on every `KvResponse` | `kv.proto:KvResponse.not_leader_hint`; set in `crowkv/src/rpc/kv_response.rs::not_leader` |
| Read-mode routing: `ReadMode::{Linearizable, ReadYourWrites, BoundedStale, BestEffort}` enum, fully wired | `kv.proto:ReadMode`, `crowkv/src/cluster/px_kv_store.rs::resolve_read_point` — covers `plan.md` M4's `SafeSlot`/`AtSlot(N)`/`BestEffortStale` (renamed/refined at implementation time; `ReadYourWrites` is an addition beyond the milestone table) |
| Lease fast-path + `ReadIndex` quorum-heartbeat fallback for linearizable reads, on the **real monotonic clock** (`Instant::now()`), not `TestTimer` | `crowkv/src/cluster/group_election.rs::linearizable_read_barrier`, `crowkv/src/cluster/local_replica.rs::lease_read_valid` — `plan.md` M4's acceptance criterion is met |
| Transparent server-side leader-forward for `Get`/`Scan` when local replica isn't leader (loop-guarded via `x-crowkv-forwarded` header) | `crowkv/src/rpc/kv_service.rs::get`/`scan`, `forward_kv_get`/`forward_kv_scan` |
| A real (if thin) gRPC KV client, used by `crowkv-cli kv {put,get,delete,scan}` and the bench runner | `crowkv-console/shared/src/clients/grpc.rs::KvClient` |
| Cluster topology discovery — but over **HTTP**, not gRPC `DescribeCluster`, and driven by `crowkv-console`'s own monitor, not by `crowkv`'s client library | `crowkv-server/src/mgmt_api.rs` (`/topology`, `/api/stores`, `/api/health`), `crowkv-console/shared/src/monitor.rs` |

## 2. What's missing

### 2.1 `DescribeCluster` / `AdminService`
Stubbed as a proto sketch only, in a design doc, never implemented:

```protobuf
service AdminService {
  rpc DescribeCluster(google.protobuf.Empty) returns (DescribeClusterResponse);
}
```
(`design/design-rpc.md` §4.2). Grepped the whole repo: zero hits for `AdminService`
or `DescribeCluster` outside that one doc. `DescribeClusterResponse`'s shape isn't
even sketched.

### 2.2 Group-0 / static `num_groups` / hash partitioning
`requirement.md` §7.1 and §7.4, and `design.md` §2/§7, all describe this as core
architecture: a special system group (`Group-0`) whose log is the source of truth
for the node registry, per-group membership, `num_groups`, and the
`hash(key) -> group_id` partitioning rule, bootstrapped once from a static seed
config and self-managed thereafter via normal Paxos. `plan.md`'s decision log
even resolves *when*: "Group-0 bootstrap timing — **static in P4**, dynamic in P5."

**None of this exists.** Grepped `crowkv/` and `crowkv-server/`: zero hits for
`Group-0`, `group_zero`, `num_groups`, or any hash-partitioning logic anywhere.

### 2.3 Real client library
`crowkv-console/shared/src/clients/grpc.rs::KvClient` is a single-endpoint
wrapper: `connect(endpoint)`, then `put`/`get`/`delete`/`scan` all go to that one
endpoint. There is no:
- Seed list / bootstrap discovery.
- Topology cache (`group_id -> leader_endpoint`).
- Key-hash routing (there's no hash function to route with — see §2.2).
- Client-side retry loop on `NotLeaderHint` or timeout (today, callers like
  `crowkv-cli`/the bench runner just surface the error; the *only* existing
  auto-retry-equivalent is the server-side `Get`/`Scan` forward in §1, which
  doesn't cover `Put`/`Delete`/`BatchWrite` at all).
- `safe_slot` caching across calls (the field is returned per-response but
  nothing reads/stores it client-side).

---

## 3. The fork this plan can't resolve on its own

Two coherent-but-different models exist in this codebase today, and P4 M3 as
literally written assumes the first one:

**Model A — Group-0-managed (`requirement.md`/`design.md` as written).** Cluster
topology is self-hosted inside the cluster itself (Group-0's log). `num_groups`
is fixed at cluster creation. A client never talks to an "admin API"; it
bootstraps from a seed list, calls `DescribeCluster`, and hashes keys to groups
itself. There is no operator-driven "create a group" HTTP call at runtime in
steady state (`ConfigChange` entries in Group-0's own log are the only way
membership changes, and that's P5's joint-consensus machinery, not P4's).

**Model B — operator-managed (what's actually built and tested today).**
`crowkv-server`'s HTTP management API is the source of truth: an operator (or
`crowkv-console`) explicitly creates stores and groups (`POST /api/stores`,
`POST /api/stores/:s/groups`), every KV RPC takes an explicit `group_id`, and
topology is discovered by polling that HTTP API — which is exactly how
`crowkv-console`'s monitor and CLI already work today, with real tests
(`crowkv-console/web/tests/*`) passing against it.

Building the client library as `plan.md` M3 literally specifies means building
Model A **first** (Group-0 group, its `ConfigChange`/topology log-entry kind,
bootstrap sequencing, `DescribeCluster` RPC served from Group-0's leader) — a
new architectural component, not "just" a client crate, and one that doesn't
obviously coexist with Model B's already-working, already-console-integrated
HTTP-driven flow without a real reconciliation decision (does the HTTP mgmt API
become a thin front-end that issues `ConfigChange`s into Group-0 instead of
calling `add_group` directly? Do both models run in parallel indefinitely?).

This is the single biggest thing this plan needs from you — see Issue 1 in §6.

---

## 4. Do we need a new design doc first?

**No new *design* document — resolved by §6 Issue 1: Model B.**

- The wire protocol is already designed (`design/design-rpc.md`) and fully
  implemented (§1) — the `AdminService`/`DescribeCluster` stub was removed
  from that doc rather than built (§6 Issue 3).
- The consistency/read-routing model the client needs to respect is already
  fully designed *and implemented* (`design/design-leader-election.md`,
  `requirement.md` §6.4, §1 above) — the client library consumes it, doesn't
  design it.
- Model B (operator-managed HTTP model, explicit `group_id` + retry/topology-cache
  over the existing HTTP API for discovery) is a bounded, well-understood
  client-side state-machine (topology cache + retry loop) layered on
  already-solid, already-tested foundations. The design *section* below (§5)
  is the design doc for this milestone; `requirement.md` §7.1/§7.4/§10.1 and
  `design.md` §2/§7 have been amended in place to describe Model B as the
  accepted architecture (no more "aspirational Group-0" text).

---

## 5. Proposed milestones (Model B, resolved)

Written against Model B (explicit `group_id`, operator/HTTP-driven topology).
C4 (`DescribeCluster`) is dropped per §6 Issue 3 — the HTTP `/topology`
endpoint is the permanent discovery mechanism.

**Crate:** new standalone `crowkv-client` (workspace member), depending only
on `crowkv` (proto/RPC types + `crowkv::cluster::status::{StoreStatus, GroupStatus,
ReplicaStatus, RemoteStatus}` for deserializing `/topology`, which is already a
public `crowkv` type — no dependency on `crowkv-server` or `crowkv-console`
needed). `crowkv-console/shared/src/clients/grpc.rs::KvClient` is deleted;
`crowkv-console` depends on `crowkv-client` instead (§6 Issue 4).

| Milestone | Scope | Acceptance |
|---|---|---|
| **C1** | `crowkv-client` crate skeleton + topology cache: `(store_id, group_id) -> leader_endpoint`, seeded/refreshed from HTTP `/topology` on any seed, refreshed on `NotLeaderHint` and on a configurable interval. Per-endpoint connection pool (`tonic::Channel`, configurable pool size per endpoint, round-robin — §6 Issue 4) replacing the single-channel cache in the old `KvClient`. | Cache hit avoids the HTTP round-trip; cache miss/`NotLeaderHint` triggers exactly one refresh, not a storm. |
| **C2** | Retry policy per `requirement.md` §10.2: immediate retry on `NotLeaderHint` (follow the hint), 1s-then-retry on unknown leader, exponential backoff on timeout, 3-retry cap on other errors, all configurable. Applied uniformly to `Put`/`Delete`/`BatchWrite` (today only `Get`/`Scan` get any redirect at all, and that's server-side). Client mints one `(client_id, seq)` per logical write and reuses it across all retries of that write (§6 Issue 6). | Client survives a forced leader step-down mid-request with auto-retry, returns the same result; retry counts/intervals configurable and covered by a fake-leader-change test. |
| **C3** | Expose all four `ReadMode`s on the client API (today `crowkv-cli`/bench hard-code `Linearizable` implicitly via default `0`). `ReadYourWrites` support: client tracks a per-`(store_id, group_id)` last-write-slot watermark (bounded — one `u64` per group ever written to, not per key; see §6 Issue 5), auto-attached as `client_slot` on `Get`/`Scan` unless the caller overrides it explicitly. | Round-trip test exercising all four modes against a 3-node cluster; a `ReadYourWrites` read immediately after a `Put` to the same client observes the write on a replica the caller didn't pin, without an explicit `client_slot` argument. |

**Freeze gate (unchanged from `plan.md`):** `.proto` schema append-only,
version at tag 1 — already the case for everything added in C1-C3.

**Status (2026-07): C1-C3 implemented in `crowkv-client`**, with unit tests
(`topology.rs`) and two real e2e suites:
`crowkv-client/tests/e2e_single_node_test.rs` (put/get/delete/batch_write/
scan, topology-cache-driven discovery, `ReadYourWrites` auto-watermark) and
`crowkv-client/tests/e2e_retry_test.rs` (C2's own acceptance line — a real
2-node group, `put` seeded at the follower on purpose, asserts the raw
gRPC response really carries a `not_leader_hint` pointing at the real
leader, then asserts `CrowkvClient::put` transparently follows it and
completes). A live kill/re-elect step-down is flakier than this
deterministic variant and exercises the identical `follow_not_leader` code
path, so it was preferred over standing up a real election timer in the
e2e suite. `crowkv-client::CrowkvClient::put`/`delete` also accept an
`ids: Option<(client_id, seq)>` override (needed by callers that expose
explicit idempotency keys to their own users) and `set_mgmt_seeds`/
`seed_leader` for callers with their own discovery path.

**Consumer migration — scoped narrower than "delete `KvClient`" (§6 Issue 4's
original framing):**
- **`crowkv-cli` (`commands/kv.rs`) — migrated.** Endpoint resolution still
  goes through the console (`ConsoleClient::resolve_endpoint` — the CLI has
  no direct `crowkv-server` mgmt connection and isn't meant to); the raw
  `KvClient` dial+call is replaced by a `CrowkvClient` pre-seeded with that
  one endpoint via `seed_leader`, gaining pooling/retry for free. All
  `crowkv-cli`/`crowkv-console-shared` tests pass unchanged
  (`kv_cli_test.rs::kv_put_get_delete_round_trip` exercises this live).
- **`crowkv-cli`'s bench runner (`bench/runner.rs`) — deliberately left on
  `KvClient`.** Its entire purpose is single-attempt latency/error-rate
  measurement (`t0 = Instant::now()`, one RPC, no retry) — exactly what
  `requirement.md` §10.2's retry policy and `CrowkvClient`'s internal retry
  loop would corrupt (a transient `NotLeaderHint` would silently become a
  slower success instead of a counted error). This is a real, structural
  mismatch, not a migration gap: a benchmark tool and a resilient client
  library want opposite behavior on the same RPC.
- **`crowkv-web` (`src/kv.rs`) — migrated.** `with_leader_retry`'s whole
  candidate-endpoint queue (`monitor_cache` lookup, `NotLeader`/transport
  branching, per-endpoint attempt caps) is deleted; each request builds a
  `CrowkvClient` seeded with the group's replica nodes' *management* URLs
  (`ServerEntry::url`, not `grpc_url`) via a new `mgmt_seeds_for_group`
  helper, and lets `CrowkvClient` do all discovery/retry itself. Any one
  reachable replica's own `/topology` carries the real leader's endpoint
  via its `remotes` list (`design/design-rpc.md`), so this needs no
  `monitor_cache`-specific bookkeeping. `AppState.kv_retry`/
  `KvRetryConfig` (only ever used by `with_leader_retry`) are deleted.
  `http_kv_endpoint` (the CLI bench's raw-endpoint-discovery route) is
  untouched -- it never used `KvClient` and still resolves via
  `monitor_cache` directly, which is correct for that use case (see
  `bench/runner.rs` note below).

  Fixing this migration surfaced two real `crowkv-client` bugs (not
  console-specific workarounds -- fixed at the root in `src/client.rs`),
  both found via `crowkv-web/tests/cluster_restart_incremental_test.rs`'s
  real multi-node restart-and-reconverge scenarios:
  1. `resolve_leader` never retried an "unknown leader" outcome -- it
     fetched `/topology` exactly once and gave up permanently if the
     group had no leader yet (e.g. a live election right after a
     restart), even though `RetryConfig::unknown_leader_wait` and
     `requirement.md` §10.2 both specify this case should retry with
     backoff. Now retries up to `max_retries` times.
  2. A `not leader` response with an *empty* hint (the responding
     replica doesn't know who's leader either) fell through to the
     generic `count_other` retry with **no** wait and **no** endpoint
     change -- a zero-delay busy-loop against the same non-answering
     replica that could never let an in-flight election converge before
     exhausting the retry budget. Added `is_unknown_leader` +
     `wait_and_refresh_leader` to give this case the same
     `unknown_leader_wait` backoff + topology refresh as the initial
     resolve.

  Both fixes are covered by the now-passing `cluster_restart_incremental_test.rs`
  (5/5 tests: 1/3/5/6-node restarts, single- and multi-group).

`KvClient` stays in `crowkv-console-shared` for one remaining call site
(`crowkv-cli`'s bench runner, deliberately, per above) and is not yet fully
retired from that crate.

---

## 6. Open issues — resolved (2026-07)

All six resolved via the `ai-todo` answers below; kept verbatim as the decision record. Docs (`requirement.md`, `design.md`, `design/design-rpc.md`, `plan.md`) have been amended accordingly.

1. **Model A vs. Model B (§3).** Do we (a) build Group-0 + hash-partitioning for
   real, matching `requirement.md`/`design.md` literally, or (b) treat the
   already-built operator-managed HTTP model as the accepted implementation and
   scope the client library around it (§5), documenting the deviation the same
   way P3's crowtree deviations got documented in the now-closed `todo-sm.md`?
   Everything else in this plan depends on this answer.

   ai-todo: We choose ModelB. Change requirment and design to use current model B impl. We use http and mgmt api to cotrol everything. Each node has a config file to maintain the node info. Cluster has it's own metadata mgmt the cluster store/group info (it now write to a config file)

2. **If (b): does `requirement.md §7.1/§7.4` get amended**, or left as
   aspirational/superseded-in-practice text (same treatment `design-rpc.md §4.2`
   already got — a stub nobody built)? I'd lean toward amending it once we
   agree, so the doc set stops describing an architecture that doesn't exist,
   but that's a documentation-policy call, not mine to make unilaterally.
   ai-todo: yes need refine it. The crowkv lib or server keep API to manage the cluster.  If other storage system need use it, they can design the system group-0 in their concept. 

3. **Scope of `DescribeCluster`.** If (b), is a gRPC `DescribeCluster` RPC still
   worth building (so gRPC-only clients don't need an HTTP dependency), or is
   "the client library depends on the HTTP mgmt API for discovery" an
   acceptable permanent shape, given `crowkv-console` already works that way?
   ai-todo: no

4. **Crate boundary.** Should the new client logic live in a new top-level
   `crowkv-client` crate (reusable by anyone embedding CrowKV, matches
   `plan.md`'s "client library" framing), or should
   `crowkv-console/shared/src/clients/grpc.rs::KvClient` simply grow the
   topology-cache/retry logic in place (faster, but ties the "official" client
   to the console's crate, which today is scoped/named as console-internal)?
   ai-todo: we should provide a standard alone crowkv-client  and let crowkv-console code use it. The client lib can do some connection pool and aggregation to improve the performance. And let other componet easy to use kv cluster.  What's your suggestion? 

   **Resolved:** standalone `crowkv-client` crate, `crowkv-console` becomes a consumer (§5). Connection pool: kept, sized per-endpoint (configurable, default 1 — a single `tonic::Channel` already multiplexes HTTP/2 concurrently; the pool knob exists for perf testing to find where that stops being true). Aggregation/batching: **dropped**, per your follow-up ("don't need do extra batch, remove it") — `BatchWrite` remains available for callers to invoke directly; the client does not auto-coalesce independent callers' writes (that would change per-caller failure semantics for no proven benefit yet).

5. **Read-your-writes client_slot plumbing.** `ReadMode::ReadYourWrites` needs
   the client to remember `revision` from its last write per key (or globally)
   to pass as `client_slot`. Per-key tracking is more correct but is unbounded
   memory for an unbounded keyspace; a single last-write high-watermark is
   simpler but stricter than necessary. Which default, and is it configurable?
   ai-todo: do we need it? please explain to me with examples. 

   **Resolved:** yes, needed, implemented now (not deferred, per your follow-up).
   Example: client `Put("session:42", "active")` gets back `revision = 105`.
   An immediate `Get("session:42")` load-balanced to a follower still at slot
   103 would return stale/missing data under `BestEffort`, and would pay a
   full leader round-trip under `Linearizable` even though the client only
   needs to see *its own* write. `ReadYourWrites { client_slot: 105 }` lets
   that follower serve locally once it reaches slot 105 — cheap and correct
   for the common "read what I just wrote" pattern (session stores, UI
   post-write confirmation, etc).
   **Default chosen:** per-`(store_id, group_id)` last-write-slot watermark
   (bounded — one `u64` per group the client has written to, not one per key,
   avoiding the unbounded-keyspace memory problem). `CrowkvClient` updates
   this watermark on every successful `Put`/`Delete`/`BatchWrite` response and
   auto-attaches it as `client_slot` on a `ReadYourWrites` `Get`/`Scan` unless
   the caller passes an explicit override. Landed in C3.

6. **Retry idempotency below the RPC layer.** `client_id`/`seq` dedup already
   exists server-side (`design.md §3` Dedup Cache, wired). Confirming: the new
   client-side retry loop (C2) should always reuse the *same* `(client_id, seq)`
   across retries of one logical write (not mint a new `seq` per attempt) —
   this seems obviously required for the dedup cache to do anything, but
   flagging since it's a correctness-critical default worth stating explicitly
   rather than assuming.
   ai-todo: yes, we should reuse the same `(client_id, seq)` across retries of one logical write. Put it in client lib. 
