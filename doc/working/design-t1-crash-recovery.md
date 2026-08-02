<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# T1 Design — Crash-recovery hardening for R16b (enable `wal_early_ack`)

Companion plan: `plan-io-clean.md` § T1. Backlog context:
`R35-apply-fence.md` (shared control-surface wiring). Formal design
home: `design/design-wal.md` (merge target after implementation).

## Problem

R16b (`wal_early_ack`) is implemented and merged but ships default-off
(`group.rs` L228). The mechanism drops the local fsync off the write
critical path: the proposer declares `Chosen` as soon as remote quorum +
local CAS succeed, deferring the local WAL persist to a fire-and-forget
`tokio::spawn` (`spawn_accept_persist`, `local_replica.rs` L1171). On
NVMe the local fsync is ~10–100 µs — the larger single latency
component once R16a overlapped it with the quorum RPC. The blocker is
not code; it is **durability-ordering validation**: a crash between the
CAS and the deferred persist must not violate any externally visible
guarantee.

`wal_early_ack` is currently exercised by **zero** tests (grep of
`crowkv/tests/` for `wal_early_ack`/`set_wal_early_ack`/`early_ack`
returns no matches). The default flip cannot proceed without
fault-injection tests proving the crash window is Paxos-safe.

## Current behavior (the window)

```
run_accept_phase (wal_early_ack = true)
  tokio::join!(
    on_accept_inner(entry)         // CAS only: term fence + acceptor.accept
    join_all(remote send_accept)   // quorum RPC fan-out
  )
  if local_accepted && accepted >= quorum:
    replica.spawn_accept_persist(entry.clone())  // fire-and-forget tokio::spawn
    return AcceptAttempt::Chosen                  // caller returns Chosen to client
```

The window is between `on_accept_inner` returning `Accepted` (the CAS
lands in memory) and `spawn_accept_persist`'s `wal.append()` completing
the fsync. If the process dies in that window:

- **Paxos safety holds.** The value is chosen — remote quorum accepted.
  The leader's CAS is in memory; the acceptor state is lost on crash,
  but the value lives on the followers and a new leader's `run_prepare_phase`
  (classic, always) re-adopts the highest-ballot accepted value for the
  slot.
- **Local durability is lost for that slot.** The WAL has no `Accepted`
  record for it. On restart, `replay_group` rebuilds the acceptor from
  what's on disk — the slot is absent. If the restarted node becomes
  leader again, `repair_once` (L1400) fills the gap by re-running
  prepare + accept for `contiguous_chosen + 1`, re-adopting the value
  from the followers (or re-choosing if no follower has it either).
- **The client already received `Chosen`.** This is the key
  externally-visible event. The value is committed from the client's
  perspective; the crash must not make it disappear.

The guarantee to validate: **a value the client observed as `Chosen`
remains readable after a crash + restart, either because the local WAL
has it, or because repair re-adopts it from the quorum.**

## Proposed approach

### Decision 1 — Hitting the CAS→persist window deterministically

**Chosen: test-only gate on `spawn_accept_persist` (option (a)).**

Add a `#[cfg(feature = "test-util")]` gate on `PxLocalReplica` — a
`parking_lot::Mutex<Option<tokio::sync::Notify>>` — that, when set,
makes `spawn_accept_persist`'s background task `await` the `Notify`
before calling `wal.append()`. The test:

1. Installs the gate on the leader's local replica.
2. Fires a `put` (which returns `Chosen` once quorum + CAS land, before
   the gated persist).
3. Verifies the client got `Chosen` (the value is committed).
4. Kills the leader (`store.shutdown()` + drop, mirroring
   `g2_crash_restart_no_data_loss_test.rs`'s `kill`) — the persist is
   still blocked on the `Notify`, so no `Accepted` record is on disk.
5. Restarts the leader from the same WAL dir (mirroring `restart`).
6. Asserts the value is recoverable: either the restarted leader's WAL
   replay has the slot (persist raced the kill — re-run to hit the
   no-persist path), or `repair_once` re-adopts it from the followers.

This mirrors the existing `readindex_round_gate` test hook pattern
(`group.rs` L201–202, L2061) — a `test-util`-gated `Mutex<Option<…>>`
installed by the test, consumed by the hot path, `None` in production.
Zero production overhead (the gate is `cfg`-out in release builds).

**Alternatives considered:**

- **(b) WAL backend that fails/sleeps on a specific slot's append.**
  Rejected — requires a new test-only `IoBackend` variant or a wrapper
  that inspects the record, more invasive than a `Notify` gate and
  couples the test to the WAL record format.
- **(c) Race-y: kill immediately after `Chosen` returns, run many
  iterations.** Rejected — non-deterministic; the persist may complete
  before the kill on most runs, making the test flaky and the
  no-persist case unprovable. T1's whole point is proving the
  no-persist case is safe; it must be hit deterministically.

### Decision 2 — Where `wal_early_ack` config lives

**Chosen: keep it as an internal `PxGroup` field, flip the default in
`PxGroup::new`, carry across rebuild in `mgmt_api`. No config struct,
no CLI flag, no env var.**

R35 (`R35-apply-fence.md` L48–63) already established this: both
`wal_early_ack` and `async_engine_apply` are "internal config, not
operator-tunable." The existing `set_wal_early_ack` setter (L280)
remains for test override. The work is:

1. Flip `wal_early_ack: false` → `true` at `group.rs` L228 (after tests
   pass).
2. Add a `wal_early_ack()` getter (mirrors `force_classic()` at L1572)
   and carry it across rebuild in `mgmt_api.rs` next to the
   `force_classic` block — so add/remove/promote replica does not
   silently reset it to the struct default. R35 will carry
   `async_engine_apply` in the same spot; doing `wal_early_ack` first
   establishes the pattern.

**Alternatives considered:**

- **Add to `WalConfig` or `PxElectionConfig`.** Rejected — these are
  operator-facing structs (serialized, CLI-exposed). `wal_early_ack` is
  an internal optimization flag, not a tunable; exposing it invites
  misconfiguration (a user disabling it loses the latency win with no
  safety benefit, since the crash tests prove it's safe).
- **Keep `false` default, flip via test-only setter in the crash tests
  only.** Rejected — the goal of T1 is to enable R16b by default in
  production, not just test it. The default must flip.

### Test 2 — Persist-failure window (background task logs the error)

The plan's second test: `wal_early_ack` is on, the local persist *fails*
(background task logs the error), confirm the value is still chosen
(Paxos-safe) and `repair_once` re-drives the slot.

This does not need the `Notify` gate — it needs the WAL append to fail.
Approach: use a `File` backend rooted in a temp dir, then `chmod 000`
the WAL segment file (or the dir) after the `put` returns `Chosen`, so
the background `wal.append()` fails. The test asserts:

1. The client got `Chosen` (Paxos-safe — the value is chosen regardless
   of local persist).
2. The error is logged (the background task's `tracing::error!` at
   L1177).
3. `repair_once_for_tests` (L2016) re-drives the slot and the value
   becomes durable.

**Alternative considered:** a test-only `IoBackend` that injects a
failure on the Nth append. Rejected — `chmod 000` is simpler and tests
the real failure path; a mock backend tests the mock. The `chmod` approach
mirrors how the existing WAL tests exercise real I/O (the `File` backend
in `a3_crash_restart_no_data_loss_test.rs`).

### Test 3 — Default-flip benchmark

After tests 1 + 2 pass, flip the default and benchmark. This is
measurement, not a pass/fail test — document the per-proposal latency
drop in `write-flow-analysis.md`. The critical path becomes
`quorum RPC` only (local fsync removed); expect ~10–100 µs improvement
on NVMe. Run on the Linux bench box (same platform as the existing
write-flow benchmarks). **Blocked on Linux access** — same as T3.

## Acceptance criteria

- **T1.1** — A crash-recovery test deterministically hits the
  CAS→persist window (via the `test-util` `Notify` gate), kills the
  leader, restarts it, and asserts the chosen value is recoverable
  (either from WAL or via `repair_once`). Passes with `wal_early_ack =
  true`.
- **T1.2** — A persist-failure test confirms that a failed background
  persist (logged error) does not affect Paxos safety: the value is
  chosen, and `repair_once` re-drives the slot to durability. Passes
  with `wal_early_ack = true`.
- **T1.3** — `wal_early_ack` defaults to `true` in `PxGroup::new` and
  survives a group rebuild (add/remove/promote replica) in `mgmt_api`
  (carry-forward block next to `force_classic`).
- **T1.4** — No regression: existing crash-restart tests
  (`g2_crash_restart_no_data_loss_test`,
  `a3_crash_restart_no_data_loss_test`) pass with the new default.
- **T1.5** (deferred to Linux bench) — Per-proposal latency drop
  documented in `write-flow-analysis.md`; no throughput regression at
  the regression sentinel configs.

## Files

- `crowkv/src/cluster/local_replica.rs` — `test-util` `Notify` gate on
  `spawn_accept_persist`; `wal_early_ack` is already wired here.
- `crowkv/src/cluster/group.rs` — flip `wal_early_ack` default (L228);
  add `wal_early_ack()` getter.
- `crowkv-server/src/mgmt_api.rs` — carry `wal_early_ack` across rebuild
  (next to `force_classic` block, L1572).
- `crowkv/tests/group/` — new crash-recovery test file(s) for T1.1 +
  T1.2, mirroring `g2_crash_restart_no_data_loss_test.rs`'s
  `kill`/`restart` harness.
- `doc/working/write-flow-analysis.md` — T1.5 benchmark results
  (deferred).

## Dependencies

- **R35** — shares the `mgmt_api` rebuild-carry pattern. T1 carries
  `wal_early_ack`; R35 carries `async_engine_apply`. T1's carry block
  establishes the pattern R35 will mirror. T1's default flip is
  independent of R35's fence (R35 enables R17; T1 enables R16b).
- **Linux bench box** — T1.5 (benchmark) is deferred until Linux
  access, same as T3. T1.1–T1.4 (tests + default flip) are not
  platform-gated.
