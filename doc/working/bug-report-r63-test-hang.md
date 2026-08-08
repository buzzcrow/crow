# Bug Report: R63 dedicated election runtime causes test-suite hangs and panics

## Status
Pre-existing from commit `791d6ae` (R63: Election driver on dedicated runtime).
Partial fixes applied; full suite still hangs. Individual tests pass.

## Symptoms

### 1. `Cannot drop a runtime` panics (FIXED)
Every test that calls `add_group` twice (two-pass cluster setup) panics:
```
Cannot drop a runtime in a context where blocking is not allowed.
This happens when a runtime is dropped from within an asynchronous context.
```
**Root cause**: `add_group_inner` replaces the old `Arc<PxGroup>` via `DashMap::insert`.
The old `Arc` drops with `election_runtime: Some(Runtime)` still inside.
`Runtime::drop` blocks (waits for tasks), which panics in an async context.

**Fix applied**: In `add_group_inner`, take `election_runtime` + `election_runtime_handle`
out of the old arc before it drops, and drop the `Runtime` via `spawn_blocking`.
Also moved `tokio::spawn(start_election_loop)` AFTER the replacement so the old arc's
queued `start_election_loop` sees `tenure_cancel.is_cancelled()` and bails out
instead of creating a new `Runtime` on a dead group.

### 2. Election unit tests fail with `start_paused` (FIXED)
7 `#[tokio::test(flavor = "current_thread", start_paused = true)]` tests fail:
- `election_driver_scaffold_starts_and_stops`
- `election_driver_exits_when_group_dropped`
- `single_voter_candidate_becomes_leader`
- `single_voter_with_prevote_enabled_becomes_leader`
- `leader_heartbeat_tick_renews_lease`
- `admin_step_down_drops_leader_to_follower`
- `propose_after_admin_step_down_returns_not_leader`

**Root cause**: `tokio::time::advance` only advances the test's `current_thread`
runtime clock. The dedicated multi-threaded runtime has its own clock that
`time::advance` cannot reach, so election timers never fire. Additionally,
`Runtime::drop` panics in `current_thread` async context.

**Fix applied**: Added `spawn_on_current()` in `group_election.rs` — spawns the
driver on the current runtime (no dedicated runtime), sets
`election_runtime_handle` to current handle. Updated all 7 tests to use it.

### 3. Full `group` test suite hangs (NOT FIXED)
The `group` test binary hangs after ~5 tests pass. The hang manifests as an
infinite snapshot-commit loop (`persist.cpp:695`), ~3 snapshots/second.

**Isolation findings**:
- All individual tests pass alone (0.1-0.2s each).
- Hang only occurs when running the full suite (even with `--test-threads=1`).
- Skipping `g2` does not help — hang still occurs at `g3_leader_change`.
- Last test to start before hang: `g3_leader_change::leader_change_simulation`.
- `Cannot drop` panics are eliminated (0 count with fixes).

**Root cause analysis** (not yet confirmed — most likely):

The `PxGroup::shutdown()` flow at `group.rs:1085-1087`:
```rust
let rt_to_drop = self.election_runtime.lock().take();
if let Some(rt) = rt_to_drop {
    tokio::task::spawn_blocking(move || drop(rt)).await.ok();
}
```
If the election driver didn't exit within `per_layer_timeout` (line 1061),
`drop(rt)` blocks forever — `Runtime::drop` waits for all spawned tasks to
complete, and the driver is still running. The `spawn_blocking(...).await`
hangs `shutdown()` indefinitely.

The driver may not exit because `run_leader_state` (line 1329) calls
`run_heartbeat_round_only(...).await` **inside** a `select!` branch — once
the `ticker.tick()` branch is chosen, `cancel.cancelled()` is not checked
until `run_heartbeat_round_only` returns. If a gRPC heartbeat call to a
dead peer hangs (tonic default timeout is infinite), the driver is stuck.

**Previous test's `shutdown()` hangs → `TestCluster::shutdown()` hangs →
test never completes → but the test framework moves on?** Actually, with
`--test-threads=1`, the next test should not start until the current one
completes. But we see g3 starting, so g1's `shutdown()` did return. The
hang is IN g3 itself, not in a previous test's shutdown.

**Alternative theory**: A previous test leaks a maintenance loop or apply
loop that keeps committing snapshots on a stale engine. The
`TestCluster::shutdown()` fix (calling `PxKvStore::shutdown()` instead of
just `stop()` + `join()`) was applied but did not resolve the hang. This
suggests the leak is not from `TestCluster` but from another source —
possibly the election unit tests that use `spawn_on_current` and don't
call `cluster.shutdown()` (they just cancel + await the driver handle
with a paused-clock timeout that never fires).

## Files changed (partial fixes)

- `lib/crow-kv/src/cluster/group.rs`:
  - Changed `election_runtime_handle` from `OnceLock<Handle>` to `Mutex<Option<Handle>>`
    so `add_group_inner` can `take()` it before the old arc drops.
  - Added `election_handle()` accessor.

- `lib/crow-kv/src/cluster/group_election.rs`:
  - Added `spawn_on_current()` for `current_thread` + `start_paused` tests.
  - Added `tenure_cancel.is_cancelled()` guard in `start_election_loop()`.
  - Bumped worker threads from 2 to 4.

- `lib/crow-kv/src/cluster/group_maintenance.rs`:
  - Added `tenure_cancel.is_cancelled()` guard in `start()`.

- `lib/crow-kv/src/cluster/px_kv_store.rs`:
  - `add_group_inner`: take runtime + handle from old arc, drop via `spawn_blocking`.
  - Moved `tokio::spawn(start_election_loop)` after replacement so cancel happens first.

- `lib/crow-kv/tests/testkit/cluster.rs`:
  - `TestCluster::shutdown()`: call `PxKvStore::shutdown()` instead of just `stop()` + `join()`.

- `lib/crow-kv/tests/group/election_test.rs`:
  - All 7 election unit tests: `spawn()` → `spawn_on_current()`.

### 4. KV tests fail with "value missing" (PRE-EXISTING from R53, NOT R63)
16 kv tests fail (e.g., `kv_correctness::put_overwrite_keeps_latest`):
```
panicked at kv_correctness_test.rs:96:36: value missing
```
**Root cause**: R53 (`9b52420`) changed heartbeats/chosen-notices to use a
dedicated LearnerStream (bidi gRPC stream) instead of inline RPCs. The
LearnerStream connects lazily on first use. When the leader proposes and
chooses a value, `fan_out_chosen_notice` enqueues chosen notices on the
LearnerStream's mpsc channel. If the stream hasn't connected to the
follower yet, the notice is queued but not delivered. The test checks
`engine_get` on all nodes immediately after `put` returns, before the
LearnerStream connects and delivers the notice.

**Confirmed**: Test passes on `d001428` (before R53), fails on `9b52420`
(R53). Not caused by R63 or R64 changes.

**Fix needed**: Either (a) wait for LearnerStream connection before
returning from `propose`, (b) add a retry/wait in `assert_cluster_value`,
or (c) eagerly connect LearnerStreams during `start_cluster`.

## Recommended next steps

1. **Add a hard timeout to `spawn_blocking(drop(rt))` in `PxGroup::shutdown()`**:
   Use `std::thread::spawn` + `JoinHandle::join_timeout` (or a channel) instead
   of `spawn_blocking(...).await` so shutdown can't hang forever. If the runtime
   doesn't drop within N seconds, leak it (warn + move on).

2. **Add `cancel` checks inside `run_heartbeat_round_only`**: Break long gRPC
   calls into cancel-aware `select!` branches so the driver exits promptly
   on cancel.

3. **Set a default gRPC timeout on heartbeat/propose RPCs**: Tonic's default
   is infinite. A 5-10s timeout would prevent stuck calls from hanging the
   driver.

4. **Investigate election unit test cleanup**: The `spawn_on_current` tests
   use `let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;`
   with `start_paused` — the timeout never fires if the driver doesn't exit.
   If the driver is stuck, the test hangs until the runtime drops. Verify
   the driver always exits on cancel in the `current_thread` + `start_paused`
   environment.

5. **Consider not creating a dedicated runtime in test contexts**: Add a
   config flag or `Handle::try_current()` check so tests that don't need
   runtime isolation skip the dedicated runtime entirely.
