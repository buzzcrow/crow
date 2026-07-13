# CrowKV - Plan: P4 RPC / Client

Depends on: [`plan.md`](plan.md) §1 P4, [`requirement.md`](requirement.md) §7.1, §7.4,
§10, [`design.md`](design.md) §2, §7, [`design/design-rpc.md`](design/design-rpc.md)

Temporary per-milestone plan per [`plan.md`](plan.md)'s own convention ("detailed
per-milestone plans are created temporarily before each step"). Verified against
code, not just design docs, on 2026-07-11 — same method as the just-closed
`todo-sm.md` gap analysis for P3.

## 0. tl;dr

**P4 is much further along than `plan.md`'s phase table suggests — M1/M2/M4 are
essentially done, built ahead of schedule alongside P1/P2/P3.** M3 ("client
library") is the real gap, and it is bigger than "write a client crate": the
client-discovery model `requirement.md §7.1/§7.4` and `design.md §7` specify
(**Group-0** system group, static `num_groups`, `hash(key) -> group_id`,
`DescribeCluster` RPC) has **zero implementation** — not partially done, not
stubbed, just absent. What exists instead is a different, already-working,
already-tested model: an operator-driven HTTP management API
(`crowkv-server/src/mgmt_api.rs`) for creating stores/groups, with every KV RPC
taking an explicit `group_id`. These two models were never reconciled. See §3
for why this matters and §6 for the decision this plan needs from you before
any code gets written.

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

**No new *design* document, conditional on the answer to Issue 1 (§6).**

- The wire protocol is already designed (`design/design-rpc.md`) and mostly
  implemented (§1) — no rework needed there beyond appending `DescribeCluster`'s
  messages (append-only field numbers, same as everything else in that doc).
- The consistency/read-routing model the client needs to respect is already
  fully designed *and implemented* (`design/design-leader-election.md`,
  `requirement.md` §6.4, §1 above) — the client library consumes it, doesn't
  design it.
- If the answer to Issue 1 is **Model B** (keep the operator-managed HTTP model,
  scope the client library to explicit `group_id` + retry/topology-cache over
  the existing HTTP API for discovery): this is a bounded, well-understood
  client-side state-machine (topology cache + retry loop) layered on
  already-solid, already-tested foundations. A design *section* in this plan
  (§5 below) is enough; a whole new `design-client.md` would be overkill for
  what's fundamentally a caching/retry wrapper.
- If the answer is **Model A** (build Group-0 for real): that *is* new
  architecture — a new log-entry kind, a bootstrap sequencing protocol, a
  reconciliation with the existing HTTP mgmt API — and would deserve a proper
  `design/design-group0.md` before any milestone plan, the same way crowtree got
  `design/design-crowtree.md` before its plan-tree milestones. I have **not**
  written that doc; I'd want your read on Issue 1 first, since it changes
  whether that document needs to exist at all.

---

## 5. Proposed milestones (Model B scope — pending your answer to Issue 1)

Written against Model B (explicit `group_id`, operator/HTTP-driven topology)
since it requires no new server-side architecture and everything it needs
already exists and is tested. If you pick Model A instead, this section gets
replaced, not patched.

| Milestone | Scope | Acceptance |
|---|---|---|
| **C1** | New `crowkv-client` crate (or a promoted/generalized `crowkv-console/shared/src/clients/grpc.rs`, see Issue 4): topology cache keyed by `(store_id, group_id) -> leader_endpoint`, seeded from the HTTP mgmt API's `/topology` (reusing `crowkv-console/shared`'s existing deserialization types rather than re-inventing them), refreshed on `NotLeaderHint` and on a configurable interval. | Cache hit avoids the HTTP round-trip; cache miss/`NotLeaderHint` triggers exactly one refresh, not a storm. |
| **C2** | Retry policy per `requirement.md` §10.2: immediate retry on `NotLeaderHint` (follow the hint), 1s-then-retry on unknown leader, exponential backoff on timeout, 3-retry cap on other errors, all configurable. Applied uniformly to `Put`/`Delete`/`BatchWrite` (today only `Get`/`Scan` get any redirect at all, and that's server-side). | Client survives a forced leader step-down mid-request with auto-retry, returns the same result; retry counts/intervals configurable and covered by a fake-leader-change test. |
| **C3** | `safe_slot`/`read_slot` caching per response; expose the four `ReadMode`s on the client API (today `crowkv-cli`/bench hard-code `Linearizable` implicitly via default `0`). | Round-trip test exercising all four modes against a 3-node cluster; `ReadYourWrites` honors a cached `client_slot`. |
| **C4** | `DescribeCluster`-equivalent: **either** a real gRPC RPC backed by the HTTP mgmt API's existing topology data (thin gRPC facade, no Group-0 needed) **or** deferred entirely in favor of the HTTP `/topology` endpoint the client already has to hit anyway (Issue 3). | Depends on Issue 3's answer. |

**Freeze gate (unchanged from `plan.md`):** `.proto` schema append-only,
version at tag 1 — already the case for everything added in C1-C4.

---

## 6. Open issues — need your decision before implementation starts

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

5. **Read-your-writes client_slot plumbing.** `ReadMode::ReadYourWrites` needs
   the client to remember `revision` from its last write per key (or globally)
   to pass as `client_slot`. Per-key tracking is more correct but is unbounded
   memory for an unbounded keyspace; a single last-write high-watermark is
   simpler but stricter than necessary. Which default, and is it configurable?
   ai-todo: do we need it? please explain to me with examples. 


6. **Retry idempotency below the RPC layer.** `client_id`/`seq` dedup already
   exists server-side (`design.md §3` Dedup Cache, wired). Confirming: the new
   client-side retry loop (C2) should always reuse the *same* `(client_id, seq)`
   across retries of one logical write (not mint a new `seq` per attempt) —
   this seems obviously required for the dedup cache to do anything, but
   flagging since it's a correctness-critical default worth stating explicitly
   rather than assuming.
   ai-todo: yes, we should reuse the same `(client_id, seq)` across retries of one logical write. Put it in client lib. 
