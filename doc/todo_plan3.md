# CrowKV - Plan: Refactor TODO 3
Depends on: [`requirement.md`](requirement.md), [`design.md`](design.md)

## 1. Finished

- **Store name:** `PxNodeServer` was first renamed to `PxKVStore`, then finalized as `KvStore` because the KV-facing runtime is not Paxos-specific.
- **Store/server split:** Store state and KV routing now live in `crowkv/src/cluster/kv_store.rs`; gRPC lifecycle logic now lives in `crowkv/src/cluster/kv_server.rs`.
- **Server lifecycle trait:** `NodeServer` was renamed to `KvServer`.
- **Paxos RPC wrapper:** `PxNodeService` was renamed to `PxReplicaService`.
- **Remote replica type:** `RemoteReplica` was renamed to `PxRemoteReplica` and moved to `px_remote_replica.rs`.
- **KV service ownership:** `KvService` delegates KV operations to `KvStore`.
- **Explicit group routing:** `KvStore` methods route by explicit `group_id`, and KV protobuf requests now carry append-only `group_id` fields.
- **Design alignment:** `design.md` defines the current target responsibilities for `KvStore`, `PxGroup`, `PxLocalReplica`, and `PxRemoteReplica`.
- **Paxos group dispatch:** Paxos protobuf requests now carry append-only `group_id` fields, and `PxReplicaService` dispatches `Prepare`/`Accept` to the requested `PxGroup`.
- **Topology attachment fix:** `KvStore::add_group_with_local_replica` attaches group topology to the local replica, and test clusters refresh bound endpoints after server startup.
- **Test coverage added:** Added group remote-replica composition coverage and a 99-member scale-shape test; existing KV/Paxos integration tests now cover `group_id` request routing.

## 2. Completed refactor work (session 2)

- **PxLocalReplica stripped to pure acceptor/learner:** Removed `kv_put`, `kv_delete`, `kv_batch_write`, `propose_kv`, `propose_kv_payload`, `run_prepare_phase`, `run_accept_phase`, `prepare_remote`, `accept_remote`, `peer_endpoints`, `check_leader_or_redirect`, `peer_pool`, `group`, `config`, `next_slot`, `PxPaxosMode`, and all KV encoding. `PxLocalReplica::new` now takes `(id, role)` only. Remaining public API: `on_prepare`, `on_accept`, `learn`, `accepted_at`, `promised_at`, `is_leader`, `set_role`.
- **PxGroup is now the proposer:** `PxGroup::propose(payload, client_id, seq) -> ProposeResult` drives the full Paxos lifecycle: slot allocation, Phase-1 Prepare (when `force_classic` or after rejection), Phase-2 Accept fanout, learner notification, slot-retry with foreign-value detection. Uses `PxRemoteReplica` for remote RPC calls.
- **KV semantics moved to KvStore:** `KvStore` owns `encode_kv_payload`, `encode_kv_batch_items`, leader/not-leader check (via `ProposeResult::NotLeader`), and `KvResponse` construction. `KvStore.kv_put/kv_delete/kv_batch_write` encode an opaque payload and call `PxGroup.propose()`.
- **Connection pool moved into PxRemoteReplica:** Each `PxRemoteReplica` owns a `PxPeerConnectionPool` and exposes `send_prepare` / `send_accept` RPC methods. `PxGroup` calls these directly instead of managing raw endpoints and peer pools.
- **force_classic flag:** `PxGroup::with_force_classic(true)` configures always-prepare mode (classic Paxos). Default is Leader-mode (Phase-2-only). Test helper `start_cluster_classic` creates classic-mode clusters.
- **All tests updated:** Removed all references to `PxPaxosMode`. Tests use `KvStore` for KV operations and `PxGroup.propose()` path. All 34 tests pass.

## 3. Completed (session 3)

- **Code review** applied `/code-review-refactor` workflow across all `src/` and `tests/`:
  - Removed `Arc<PxLocalReplica>` and `Arc<AtomicU64>` from `PxGroup` (parent `Arc<PxGroup>` in DashMap suffices); `local_replica()` now returns `&PxLocalReplica`.
  - Removed dead types `PxOperation`/`PxOpKind` and unused `serde` dependency.
  - Removed dead `SocketAddr` import in `config.rs`, dead `RemoteReplicaKind::id()` method.
  - Simplified `RemoteReplicaKind::Placeholder { id: u64 }` to unit variant.
  - Deduplicated `optional_u64` into `common/mod.rs`.
  - Replaced `.unwrap()` panic in `PxRemoteReplica::get_or_init` with `get_or_try_init` error propagation.
  - Removed unnecessary `Clone` derive from `PxPrepareResult`, `PxAcceptResult`, `ProposeResult`.
  - Refined `.windsurf/workflows/code-review-refactor.md` to common rules.
- **Multi-group KV store tests:** Added `tests/cluster/multi_group.rs` — `multi_group_routes_to_correct_group`, `missing_group_returns_error`, `add_and_remove_group_dynamic`.
- **PxRemoteReplica error handling tests:** Added `tests/cluster/remote_error.rs` — `send_prepare_to_unreachable_endpoint_returns_error`, `send_accept_to_unreachable_endpoint_returns_error`, `send_prepare_to_invalid_endpoint_returns_error`.
- **PxGroup unit tests:** Added `tests/cluster/group_propose.rs` — `single_local_propose_succeeds`, `single_local_propose_learns_entry`, `follower_group_rejects_proposal`, `single_local_classic_propose_succeeds`, `propose_with_no_client_id`, `sequential_proposals_allocate_increasing_slots`.
- **All 56 tests pass** (22 cluster + 34 paxos).
