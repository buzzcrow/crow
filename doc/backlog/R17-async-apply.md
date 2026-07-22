<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R17: Async engine apply after quorum

**Problem**: After `AcceptAttempt::Chosen`, the leader calls
`replica.learn_chosen(&entry, client_id, seq).await` which decodes the
payload and applies it to the KV engine **before** returning
`ProposeResult::Chosen` to the client. For `InMemKV` this is trivial,
but for `CrowtreeEngine` the apply involves FFI + memtable insert,
potentially triggering a memtable flush. This puts engine apply latency
on the write critical path.

`fan_out_chosen_notice` (item 7) runs after `learn_chosen` but is a
non-blocking mpsc enqueue — negligible cost, can stay where it is.

**Approach**: Return `ProposeResult::Chosen` to the client immediately
after quorum is confirmed, then apply the entry to the local engine
asynchronously (spawn a task or use a apply queue). The
`fan_out_chosen_notice` can fire immediately after quorum as well
(before the async apply completes) since it only carries the slot/term
watermark, not the payload.

**Concept change (highlighted)**: The client receives "chosen" before
the local engine has applied the value. This breaks read-your-writes
semantics: a client that writes a key and then immediately reads it
may not see the written value if the async apply has not completed.
Mitigations:
- **Apply fence**: Track the highest slot applied in the local engine.
  Reads on the leader check that the applied frontier >= the slot being
  read; if not, the read waits (or returns a stale-read indicator).
- **Sync mode**: Gate behind a feature flag `async_engine_apply`
  (default off). When disabled, the current synchronous apply behavior
  is preserved.
- **Client-visible flag**: The `KvResponse` could carry an
  `applied_locally: bool` field so the client knows whether the value
  is immediately readable.

**Feature flag**: `async_engine_apply` (default off).

**Testing**:
- Read-after-write test: write a key, immediately read it; with flag
  off, must see the value; with flag on, may see stale until apply
  catches up (verify eventual consistency).
- Apply-ordering test: multiple writes to the same key; verify the
  final applied value is the highest-slot value (per-key
  highest-slot-wins in `KVEngine::apply`).
- Crash-recovery test: leader crash after quorum but before async
  apply → on restart, the slot is re-learned and applied from the WAL
  / peer replication.

**Priority**: Medium — removes engine apply from the write critical
path, which is significant for `CrowtreeEngine` under load.

**Complexity**: Medium — spawn apply task, track applied frontier, add
read barrier / fence, feature flag, tests. No protocol change (the
value is Paxos-chosen regardless of local apply).

**Files**: `crowkv/src/cluster/group.rs` (`propose` — return before
`learn_chosen`), `crowkv/src/paxos/learner.rs` (`learn` / `apply_entry`
— async dispatch), `crowkv/src/cluster/local_replica.rs`
(`learn_chosen` — split into notify + async apply).

**Acceptance**:
- With feature flag off: all existing tests pass, behavior unchanged.
- With feature flag on: Paxos tests pass; write latency reduced by
  engine apply time.
- Read-after-write test: with flag on, eventual consistency verified
  (read eventually returns the written value after apply catches up).
- Apply-ordering test: highest-slot-wins semantics preserved under
  async apply.
- Crash-recovery test: slot re-learned and applied after restart.
