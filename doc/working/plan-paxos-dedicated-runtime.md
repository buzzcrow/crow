# Plan: Paxos Dedicated Runtime (R64)

## Task Breakdown

- [ ] T1: Expose dedicated runtime handle on PxGroup
  - Add `election_runtime_handle: OnceLock<tokio::runtime::Handle>` field
  - Add `election_handle()` accessor returning `Option<&Handle>`
  - Initialize in `PxGroup::new` (empty `OnceLock`)
  - Set in `spawn()` (group_election.rs) after runtime creation
- [ ] T2: Thread handle to PxRemoteReplica
  - Add `election_handle: OnceLock<tokio::runtime::Handle>` field on PxRemoteReplica
  - Set in `spawn()` for all existing remotes
  - No need to set in `add_remote_replica` / `set_remote_replicas` — those
    take `&mut self` and are construction-only (runtime does not exist yet)
- [ ] T3: LearnerStream dispatch loop → dedicated runtime
  - `PxLearnerStream::new` accepts `Option<Handle>`, uses `handle.spawn()` when `Some`,
    falls back to `tokio::spawn()` when `None`
  - `PxRemoteReplica::learner_stream()` passes `self.election_handle.get()`
  - `None` fallback covers test groups and `election_driver_disabled` groups
    that never create the dedicated runtime but still use `send_accept`
- [ ] T4: Propose path → dedicated runtime
  - `PxKvStore::propose_and_respond` spawns `group.propose(...)` on
    `group.election_handle()`, awaits JoinHandle
  - Fallback to inline `propose().await` when no handle (test groups)
- [ ] T5: Follower gRPC handlers → dedicated runtime
  - Unary handlers (prepare, heartbeat, request_vote, pre_vote, step_down):
    spawn core work on `group.election_handle()`, await JoinHandle
  - `prepare`: move the membership-epoch fence check inside the spawned task
    (avoids TOCTOU race between main-runtime check and dedicated-runtime `on_prepare`)
  - LearnerStream server-side loop: spawn on `group.election_handle()`
  - Fallback to inline when no handle
- [ ] T6: Runtime sizing — increase dedicated runtime to 4 workers
- [ ] T6a: Shutdown timeout — switch `drop(rt)` to `rt.shutdown_timeout(5s)`
  in `PxGroup::shutdown()`; update stale shutdown comment
  (propose/coalescer watchdog do not check `tenure_cancel`, so bare `drop`
  can block; `shutdown_timeout` bounds it)
- [ ] T7: New unit test — propose runs on dedicated runtime
  - Test-only `AtomicBool` on PxGroup, set in `propose_inner_impl` via
    `Handle::id()` comparison
  - Test: set up group with election runtime, propose, assert flag
- [ ] T8: Run relevant tests (election, paxos, server), fix failures
- [ ] T9: Pre-commit quality gate (fmt, clippy, relevant tests)
- [ ] T10: Commit implementation + design/plan docs
- [ ] T11: Full test suite (pixi run test-suite)
- [ ] T12: Merge design into design-crow-kv.md §13, cleanup backlog
- [ ] T13: Local CI check (fmt, clippy, test-ct, test-ffi, test-core)
- [ ] T14: Run read + scan regression benchmarks, update flow-analysis docs

## File-Level Changes

| File | Change |
|------|--------|
| `lib/crow-kv/src/cluster/group.rs` | `election_runtime_handle` field + accessor; set remote handles in `spawn()`; `shutdown()` switch to `rt.shutdown_timeout()` + update stale comment; test-only `AtomicBool` for propose-runtime check |
| `lib/crow-kv/src/cluster/group_election.rs` | Set `election_runtime_handle` + remote handles in `spawn()`; 4 workers |
| `lib/crow-kv/src/cluster/learner_stream.rs` | `PxLearnerStream::new` takes `Option<Handle>` param; `handle.spawn()` when `Some`, `tokio::spawn()` fallback when `None` |
| `lib/crow-kv/src/cluster/remote_replica.rs` | `election_handle: OnceLock<Handle>` field; `learner_stream()` passes `self.election_handle.get()` |
| `lib/crow-kv/src/cluster/px_kv_store.rs` | `propose_and_respond` spawns on dedicated runtime |
| `lib/crow-kv/src/rpc/px_service.rs` | Unary + LearnerStream handlers spawn on dedicated runtime; `prepare` epoch check moved inside spawned task |
| `lib/crow-kv/tests/election/apply_and_runtime_test.rs` | New test: propose on dedicated runtime |
| `doc/design/kv/design-crow-kv.md` | §13 update (Step 7 merge) |
| `doc/design/kv/kv-read-flow-analysis.md` | Post-R64 benchmark results |
| `doc/design/kv/kv-scan-flow-analysis.md` | Post-R64 benchmark results |

## Dependency Ordering

T1 → T2 → T3 (LearnerStream needs handle on remote)
T1 → T4 (propose needs group handle)
T1 → T5 (follower handlers need group handle)
T6 independent (one-line change in spawn())
T6a after T6 (shutdown change depends on runtime sizing decision)
T7 after T4 (test needs propose on dedicated runtime)
T8 after T1-T7, T6a
T9-T13 sequential

## Test Checklist

- [ ] New unit test: `propose_runs_on_dedicated_runtime`
- [ ] `pixi run test-core` (election + paxos suites)
- [ ] `pixi run test-server`
- [ ] `pixi run test-cli`
- [ ] `pixi run test-suite` (full)
- [ ] `cargo fmt --check` + `cargo clippy -- -D warnings`
- [ ] Read benchmark: `bash tools/bench-read-regression.sh`
- [ ] Scan benchmark: `bash tools/bench-scan-regression.sh`
