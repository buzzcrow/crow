# CrowKV - Plan: Follow-up Node Cleanup
Depends on: [`requirement.md`](requirement.md), [`design.md`](design.md), [`design/design-rpc.md`](design/design-rpc.md), [`design/design-leader-election.md`](design/design-leader-election.md), [`plan.md`](plan.md)

This document records cleanup progress after closing the original `todo_plan.md` and reviewing user `ai-todo` feedback.

## Done

1. `KvConfig::new(listen_addr)` added and `PxNode::with_config(...)` added.
2. `PxNode::start()` binds `self.config.listen_addr` instead of hard-coded `127.0.0.1:0`.
3. Prepare/accept retry paths carry `PxPaxosError` and use `PxRetryAction` instead of raw string-only retry reasons.
4. Public direct KV APIs were cleaned up: `kv_put`, `kv_delete`, and `kv_batch_write` now require `client_id`, `seq`, `request_id`, and `request_create_ms`; old `*_with_meta` helpers were removed.
5. Request handlers were renamed from `prepare`/`accept` to `on_prepare`/`on_accept`.
6. `PxNode::set_role(...)` was added for tests before leader election is implemented.
7. Direct in-process `peers` fallback and `with_peers` were removed from `PxNode`; tests now use started gRPC nodes and `PxGroup` endpoints.
8. A reusable `PxPeerConnectionPool` was added under `rpc::connection_pool` and node peer RPCs use cached `PxServiceClient` handles.
9. `KvConfig`, `GrpcTaskState`, and server lifecycle implementation were moved out of `node.rs` into `node::config` and `node::server`.
10. Unused Paxos compatibility files `paxos/common.rs` and `paxos/protocol.rs` were removed; call sites now import canonical types from `paxos::roles`.
11. Initial high-performance logging support was added with `tracing`; node/server/RPC/connection-pool consensus flow now emits:
    - `info` for major state changes such as server start, role update, group update, and chosen entries.
    - `debug` for important sub-steps such as KV/Paxos RPC receipt, connection-pool reuse, and prepare/accept attempts.
    - `warn` for retryable quorum/ballot/foreign-value paths, temporary `PxNode.next_slot` ownership, malformed accept RPCs, unimplemented binary entry points, and other not-yet-correct behavior with next-step guidance.
    - `error` with `CRITICAL:` prefix for exhausted retry budget or invariant-like operational failures.
12. `plan.md` M4 now explicitly owns removal of temporary `PxNode.next_slot` slot allocation and replacement of the temporary P1 M2 unary gRPC client cache with full proposer/replicator connection policy.
13. File logging initialization was added through `crowkv::logging::init_file_logging(...)` using `tracing-subscriber` and `tracing-appender`. Each process start writes to a new `log/<process>-<unix-seconds>.log` file. Size-based 30M rotation was not implemented because `tracing-appender` supports time/static rolling writers, not size-triggered rotation.
14. Integration test layout was refactored so Paxos-specific integration cases are under `crowkv/tests/paxos/` (`preemption_retry.rs`, `kv_slot_retry.rs`) with `crowkv/tests/paxos.rs` as a thin crate entrypoint.
15. `group/group.rs` now owns both group topology (`PxGroup`) and endpoint cache behavior directly (merged from standalone cache type), with explicit `refresh_endpoint_cache`, `update_leader_id`, `update_member_endpoint`, and `member_endpoint` support.
16. A test subscriber initialization policy was added for integration tests via `crowkv/tests/common/logging.rs`: `init_test_subscriber()` performs one-time `tracing` setup only when `CROWKV_TEST_LOG` is set; shared cluster startup now calls it.

## Remaining / Not Done

1. Move slot allocation from `PxNode.next_slot` into P1 M4 proposer/group ownership.
2. Replace the temporary P1 M2 unary peer RPC path with the full P1 M4 `Replicator` and connection/pool policy, including liveness, reconnect/backoff policy, max connection policy, per-peer flow control, and possibly bidi streams.

## Verification

Ran successfully:

```bash
cargo test -p crowkv --test paxos --test kv
cargo test -p crowkv --test paxos_error
cargo test -p crowkv --test group
```

Known warning left unchanged: Rust warns about `async fn` in public Paxos traits in `paxos/roles.rs`.
