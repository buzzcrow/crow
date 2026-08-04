<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R43 Plan — Write-Path Fan-Out Hardening

Implementation plan for R43. Tracks task breakdown, file-level changes, and
test checklist. Design: `design-R43-write-path-fanout-hardening.md`.

Ordered E6 → E1 → E2 → E3 → E4 → E5 (E6 de-risks E1; E3's first-quorum
metric presumes E1; E2/E4/E5 independent).

## Task Breakdown

### E6 — ReplyFold accumulator (pure refactor)
- [ ] Add `ReplyFold` struct to `group.rs` with fields: `accepted`,
      `highest_rejected_round`, `highest_seen_term`, `epoch_mismatch`,
      `adopted`, `local_folded`.
- [ ] Add fold methods: `fold_prepare_local`, `fold_prepare_remote`,
      `fold_accept_local_result` (R16a `Result`), `fold_accept_local_reply`
      (R16b infallible), `fold_accept_remote`. Each updates counters +
      `highest_*`; prepare folds call `consider_accepted`.
- [ ] Rewrite `run_prepare_phase` local + remote fold loops to use
      `ReplyFold`. No behavior change.
- [ ] Rewrite `run_accept_phase` R16b + R16a fold loops to use `ReplyFold`.
      No behavior change.
- [ ] Verify: `pixi run test-core` (group/paxos suite) passes unchanged.

### E1 — Quorum short-circuit
- [ ] Add test-util `accept_delay_ms` hook on `PxLocalReplica`
      (`AtomicU64`, `set_accept_delay_for_tests`); insert `sleep` in
      `on_accept_inner` under `cfg(feature = "test-util")` before the CAS.
- [ ] Rewrite `run_prepare_phase` fan-out: build `FuturesUnordered` with
      local future (tagged `Local`) + remote futures (tagged
      `Remote(idx, voting)`); `StreamExt::next` loop folding into
      `ReplyFold`; short-circuit on `accepted >= quorum && local_folded`;
      on short-circuit, move remaining futures into a detached drain task
      (captures `self_weak`, honors `tenure_cancel`) that folds late
      replies for side effects (`become_follower` on TermStale,
      `adopt_membership_epoch` on EpochMismatch).
- [ ] Rewrite `run_accept_phase` R16b + R16a fan-out the same way (local
      future type differs: R16b `on_accept_inner` infallible, R16a
      `on_accept` `Result`).
- [ ] Preserve W6: success short-circuit gated on `local_folded`; R16b
      `spawn_accept_persist` still fires after return.
- [ ] Test: 3-node cluster, delay one follower's accept, assert proposal
      latency tracks the fast follower; assert late TermStale still steps
      leader down.

### E2 — RPC deadline
- [ ] Add `learner_stream_rpc_timeout_ms: u64` to `PxElectionConfig`
      (DEFAULT 2000, for_tests 500, for_e2e 1000).
- [ ] Store `learner_stream_rpc_timeout_ms` on `PxRemoteReplica`; plumb
      into `PxLearnerStream::new` (extend the cfg struct consumed).
- [ ] Wrap `send_accept` / `send_heartbeat` `rx.await` with
      `tokio::time::timeout`; on expiry remove pending-map entry + return
      retryable `PxReplicaError::Internal("learner_stream: rpc timeout")`.
- [ ] Wrap unary `send_prepare` `client.prepare(...).await` with
      `tokio::time::timeout`; on expiry return retryable error.
- [ ] Add h2 keepalive (`keep_alive_while_idle(true)` +
      `http2_keep_alive_interval`) on `get_client` `Endpoint` and the
      learner_stream connect `Endpoint`.
- [ ] Test: hung-peer scenario (peer accepts connection but never replies)
      → accept RPC fails within timeout; proposals commit via remaining
      quorum; pending map does not leak.

### E3 — Write-path phase metrics
- [ ] Add `WriteRegistryHandles` struct to `group.rs`: `propose_e2e`,
      `prepare_phase`, `accept_phase`, `accept_quorum_rpc` (all
      `Arc<LatencySummary>`). Store in `OnceLock` on `PxGroup`.
- [ ] Register in `set_metrics_registry` with names
      `s.{store}.g.{group}.write.{metric}.l`.
- [ ] Time `propose_inner` (e2e), `run_prepare_phase`, `run_accept_phase`;
      record `accept_quorum_rpc` at the E1 short-circuit point.
- [ ] Add `engine_apply: LatencySummary` to local replica metrics; observe
      in `learner.rs apply_entry`.
- [ ] Test: run a short cluster write workload; assert registry snapshot
      shows non-zero counts on all five summaries.

### E4 — Backoff jitter
- [ ] Add thread-local `XorShift64` (reuse `group_election::XorShift64`)
      seeded from `(node_id, now_nanos)` in `group.rs`.
- [ ] Apply ±50% jitter in `retry_backoff`: `base * 2^attempt * (1 + r)/2`
      where `r = next_u64() % 1000 / 1000`.
- [ ] Add one-line comment at call sites noting the permit is held during
      backoff intentionally (dedup ordering).
- [ ] Test: unit test with a fixed-seed `XorShift64` asserts backoff varies
      across attempts and differs from the deterministic baseline.

### E5 — Heartbeat reserved capacity
- [ ] Add `learner_stream_heartbeat_reserve: usize` to `PxElectionConfig`
      (DEFAULT 8, for_tests 2, for_e2e 4).
- [ ] Add `FrameKind { Accept, Heartbeat, Chosen }` to `OutboundCmd` in
      `learner_stream.rs`; set it at each `dispatch` call site.
- [ ] Store `heartbeat_reserve` on `PxLearnerStream`; in `dispatch`, reject
      non-heartbeat frames when queue depth `>= window - reserve`; always
      allow heartbeats up to full capacity.
- [ ] Plumb reserve through `PxRemoteReplica` → `PxLearnerStream::new`.
- [ ] Test: saturate the accept queue; assert `send_heartbeat` still
      succeeds; existing `g4_learner_stream_test` ordering tests pass.

### Finalize
- [ ] Run pre-commit gate: `cargo fmt --check`, `cargo clippy -- -D
      warnings`, `clang-format`/`ct-lint` (no C++ changes expected).
- [ ] Run relevant tests: `pixi run test-core` (group/paxos/learner).
- [ ] Commit implementation + design draft + plan doc.
- [ ] Run full `pixi run test-suite`.
- [ ] Merge design into `design-rpc.md` (§3 ordering note, §6 flow control,
      §7 timeout error) + `design-observability.md` (write-path
      instrumentation points).
- [ ] Cleanup: delete R43 backlog doc + index entry + working docs; commit.
- [ ] Local CI: fmt, clippy, test-ct, test-ffi, test-core.

## File List

| File | Changes |
| --- | --- |
| `crowkv/src/cluster/group.rs` | `ReplyFold` (E6); `FuturesUnordered` short-circuit + detached drain (E1); `WriteRegistryHandles` + timing (E3); `retry_backoff` jitter (E4) |
| `crowkv/src/cluster/learner_stream.rs` | `OutboundCmd::kind` + reserved-capacity `dispatch` (E5); `timeout` on `send_accept`/`send_heartbeat` + pending cleanup (E2); keepalive on connect (E2) |
| `crowkv/src/cluster/remote_replica.rs` | `timeout` on `send_prepare` (E2); keepalive on `get_client` (E2); store + plumb timeout/reserve (E2/E5) |
| `crowkv/src/common/config.rs` | `learner_stream_rpc_timeout_ms` + `learner_stream_heartbeat_reserve` on `PxElectionConfig` (E2/E5) |
| `crowkv/src/paxos/learner.rs` | `engine_apply` summary observe in `apply_entry` (E3) |
| `crowkv/src/cluster/local_replica.rs` | test-util `accept_delay_ms` hook (E1 test infra) |
| `crowkv/tests/group/proposer_test.rs` or new test | E1/E2/E4 unit/integration tests |

## Test Checklist

- [ ] E1: delayed-follower proposal latency test
- [ ] E1: late TermStale still steps leader down
- [ ] E2: hung-peer timeout + no pending-map leak
- [ ] E3: write-path summary metrics populated
- [ ] E4: jitter varies backoff (fixed-seed unit test)
- [ ] E5: heartbeat succeeds under saturated accept queue
- [ ] E5: existing LearnerStream ordering tests pass
- [ ] No regression in `g4_learner_stream_test`, `proposer_test`,
      `group_propose_test`, `kv_correctness_test`, `t1_early_ack_crash_test`
