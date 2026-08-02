<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R35: Apply fence for async engine apply (enable R17 by default)

**Problem**: R17 (`async_engine_apply`) moves `learn_chosen` (FFI +
memtable insert) off the write critical path via `spawn_learn_chosen`
(fire-and-forget `tokio::spawn`), so the proposer returns `Chosen`
before the local engine has applied the value. This is the biggest
remaining write-path win — engine apply is no longer on the per-proposal
critical path — but R17 ships default-off because it breaks the
**Linearizable** read mode's read-your-writes guarantee.

Read-your-writes is an explicit guarantee of both read modes
(`kv.proto` L27/L33): Linearizable by definition of linearizability
("reflect every committed write"), MinSlot explicitly ("the write
watermark gives read-your-writes"). The two modes handle apply-fencing
differently today:

- **MinSlot already has the fence.** `resolve_read_point` gates on
  `contiguous_applied >= min_slot` — the client passes `min_slot` = the
  slot its write returned, and the replica won't serve until that slot
  is applied (else redirects to the leader). R17 does **not** break
  MinSlot read-your-writes; the read just waits (via redirect) until the
  local applied frontier catches up. No change needed for MinSlot.
- **Linearizable lacks the fence.** `linearizable_read_barrier` captures
  `read_slot = contiguous_chosen`, but the engine get returns the latest
  **applied** value, not a `read_slot`-pinned value. With R17 off,
  `apply == learn` (V1), so `contiguous_applied` tracks
  `contiguous_chosen` and the gap is invisible. With R17 on, a chosen
  slot may not yet be applied when the read lands — a linearizable read
  can miss a just-chosen write, violating linearizability.

**Approach** (Linearizable-only fix):
- Add an apply fence to the Linearizable read path: after the barrier
  confirms leadership and captures `read_slot`, await
  `contiguous_applied >= read_slot` before serving the engine get. The
  learner already tracks `contiguous_applied` as an `AtomicU64`
  (`learner.rs`), updated in `learn`/`apply_entry`.
- Awaiting the frontier: a `Notify`-per-group (or a small wait queue
  keyed on the awaited slot) wakes the read when `contiguous_applied`
  advances past `read_slot`. Bounded wait — apply is async but fast
  (memtable insert); under normal load the slot is already applied.
- Preserve R27 ReadIndex batching: the fence applies after the barrier
  resolves, before the engine get. Batched reads share the barrier
  outcome but each awaits its own `read_slot` (or the batch's max).
- MinSlot is untouched — it already gates on `contiguous_applied`.
- **Control surface (prerequisite to enabling R17)** — the
  `async_engine_apply` flag is implemented in the hot path but
  currently **unwired**: `set_async_engine_apply` is called nowhere and
  the struct default is `false`. Same gap applies to R16b's
  `wal_early_ack`. No CLI flag or public API is needed — these are
  internal config, not operator-tunable. R35 owns:
  1. Flip the `async_engine_apply` struct default to `true` once the
     fence lands and benchmarks show no regression. The existing
     `set_async_engine_apply` setter remains for internal/test override.
  2. Carry `async_engine_apply` across group rebuild in `mgmt_api`
     (mirror the `force_classic` block at L1572) so a rebuild
     (add/remove/promote replica) does not silently reset it to the
     struct default.
  3. R16b's `wal_early_ack` default flip is gated on T1 (crash tests),
     not R35; R35 carries it across rebuild in the same way so T1's
     flip sticks.

**Performance impact** (correctness is the priority; this characterizes
the real cost so the tradeoff is explicit, not hidden):
- **With R17 off (current default) — the fence is a no-op.** The
  barrier captures `read_slot = contiguous_chosen`. Under V1 (apply ==
  learn), `contiguous_applied` and `contiguous_chosen` are set in the
  same `store` call (`learner.rs` L251-253), so they are always equal and
  the fence check `contiguous_applied >= read_slot` is already
  satisfied at the instant the barrier resolves. Cost: one
  `AtomicU64::load(Acquire)` + compare (~1 ns). No wait, no `Notify`,
  no wake. The read path is unchanged from today.
- **With R17 on — the fence only waits in the race window.** R17
  spawns `learn_chosen` as a background task, so `contiguous_applied` can
  lag `contiguous_chosen`. The fence waits only when a read lands in the
  narrow window between "chosen" and "applied":
  - Apply is a memtable insert (microseconds).
  - The read arrives *after* the barrier, which itself takes time
    (lease check, or a ReadIndex round-trip of hundreds of µs to ms).
  - Under normal load the apply has usually completed before the read
    arrives — the fence check is already satisfied, no wait.
  - The wait only happens on a read that races a just-chosen write, and
    the wait duration is the apply latency (µs) — which is exactly the
    latency R17 *removed* from the write path. The fence shifts that µs
    from the write path to an occasional read; it does not add new
    latency, it redistributes it.
- **Await/wake mechanism cost (slow path only).** The `Notify`-per-group
  / wait queue is only touched when the fast-path check fails (the race
  window). The fast path — load, compare, return — never touches it.
  Zero overhead to the common case.
- **Net.** The fence is a single atomic load on the read fast path
  (~1 ns, dwarfed by the barrier's µs-to-ms). It adds real latency only
  when R17 is on AND a read races a just-chosen-but-not-applied write,
  and that latency is the apply cost R17 took off the write path — the
  intended tradeoff. With R17 off it is free. No scenario where the
  fence makes reads slower than today.
- **Case to benchmark.** Under write-heavy + read-heavy concurrent
  load with R17 on, a burst of reads landing right after a burst of
  writes could all hit the race window and queue on the `Notify`. The
  wait is bounded by apply throughput (memtable insert is fast and
  parallelizable), not by the quorum path. If a tail appears, the
  optional `NotAppliedYet { slot }` hint path (client retries instead
  of blocking) is the escape hatch — a fallback, not the default.

**Dependencies**: R17 (implemented, unwired), R27 (ReadIndex batching —
the fence must compose with the coalesced barrier). T1 (R16b crash
tests) shares the control-surface wiring but does not block R35's
enable; R16b's own enable is gated on T1.

**Priority**: Medium-high — unblocks the largest remaining write-path
latency win.

**Complexity**: Medium — the frontier is already tracked; the work is
the await/wake mechanism, ensuring it composes with R27 batching and the
lease fast path, plus flipping the `async_engine_apply` default and
carrying it across group rebuild. No CLI flags, no public API, no
getters needed beyond internal rebuild-carry. Confined to the
Linearizable read path + learner + `mgmt_api`; no consensus, C++,
or MinSlot changes.

**Files**: `crowkv/src/cluster/group_election.rs` (read barrier),
`crowkv/src/paxos/learner.rs` (apply frontier notify),
`crowkv/src/cluster/px_kv_store.rs` (Linearizable serve point only),
`crowkv/src/cluster/group.rs` (`async_engine_apply` default flip),
`crowkv-server/src/mgmt_api.rs` (rebuild-carry for both flags).

**Acceptance**:
- `async_engine_apply` defaults to `true` and survives a group rebuild
  (add/remove/promote replica) in `mgmt_api`.
- No regression: write-path and read-path benchmarks at the regression
  sentinel configs show no throughput/latency regression vs R17-off.
- With the fence implemented and `async_engine_apply = true`, a `put`
  followed by a linearizable `get` of the same key on the leader returns
  the written value (Linearizable read-your-writes holds). MinSlot
  read-your-writes continues to hold (unchanged). Write-path benchmark
  shows reduced per-proposal latency vs R17-off (engine apply off
  critical path).
