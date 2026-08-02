<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R41: Bounded per-client dedup window (fix single-entry false-positive)

**Problem**: `design.md` promises *"Bounded idempotency for retried
requests via `(client_id, seq)` dedup... Retention: ≥ 64 requests per
client AND ≥ 60s."* The implementation does not match: `PxLearner`
keeps exactly **one** `DedupEntry { last_seq, last_slot }` per client
("latest wins"), and `dedup_lookup(client_id, seq)` hits whenever
`seq <= last_seq`, returning `last_slot` — the slot of whichever
request happened to commit last, not necessarily the slot of the
`seq` being looked up.

`PxGroup::propose` calls `dedup_lookup` unconditionally at entry,
before slot allocation — not only on retries of the same logical
write. `CrowkvClient` is designed to be shared (`Arc<CrowkvClient>`)
across many concurrent callers under one `client_id`, with `seq`
assigned via a shared `AtomicU64` (`next_seq.fetch_add`) on the
default `ids=None` write path (`crowkv-client/src/client.rs` `put` /
`delete` / `batch_write`). This is the documented/intended pipelining
pattern: one shared client, many in-flight writes, all stamped with
the same `client_id` and a strictly-increasing shared `seq`. Multi-slot
concurrency explicitly allows slots to be **chosen out of order**
(`write-flow-analysis.md` § Multi-Slot Concurrency).

Combining these: if a higher-`seq` request from one caller commits
before a lower-`seq` request from another concurrent caller (same
`client_id`) reaches `propose` — plausible any time completion order
diverges from issue order, e.g. during an election, a slow/retried
RPC, or just scheduling jitter under load — the lower-`seq` request's
own `dedup_lookup` sees `seq <= last_seq` and returns
`ProposeResult::Chosen { slot: <higher-seq's slot> }` **without ever
running Paxos for its own payload.** The caller receives a success
response; its actual key/value write is silently dropped. This is a
correctness (data-loss) bug, not a performance issue — existing tests
(`dedup_test.rs`, `learner_dedup_test.rs`) only exercise the
sequential-retry case and the "older seq maps to latest slot" behavior
is asserted as *intended*, so nothing currently catches the concurrent
false-positive case.

Note: the bench runner (`crowkv-console/cli/src/bench/runner.rs`) does
*not* trigger this — it bypasses the shared `client_id`/`next_seq` by
passing an explicit per-worker `client_id = worker_id + 1` and a
per-worker monotonic `iter` counter, so each `(client_id, seq)` pair
is unique to one worker and strictly monotonic within it. The bug is
reachable via the client API's default `ids=None` write path on a
shared `Arc<CrowkvClient>`, which is the documented pipelining
contract.

**Approach**:
- Replace the single `DedupEntry` with a small bounded per-client
  structure that retains the last N `(seq -> slot)` mappings (N ≈ 64,
  matching the documented retention) instead of only the highest seq.
  A `VecDeque<(u64, SlotIndex)>` or small ring buffer per client is
  sufficient; keep the DashMap<client_id, _> outer structure.
- `dedup_lookup(client_id, seq)` must only report a hit for a `seq`
  that is *actually recorded* (exact match), not any `seq <=` some
  unrelated higher entry. A `seq` older than the retained window with
  no exact record falls into the existing "outside the window, outcome
  is unknown" case already called out in `design.md` — safe to
  re-propose (idempotency lost, but no false success).
- Time-based retention (`>= 60s`) can be approximate — e.g. evict the
  oldest ring entry once the count exceeds N, no wall-clock tracking
  needed if N is sized generously relative to `max_inflight_proposals`
  (default 32) so a full window of concurrent same-client requests
  never evicts an unresolved entry prematurely.
- Preserve the existing idempotent-retry fast path and semantics for
  the sequential case (`dedup_test.rs`'s existing assertions for
  same-seq retry must still pass); only the "older seq maps to latest
  slot" test's *expectation* changes — that behavior was masking the
  bug and must be corrected to "an unrecorded older seq is a miss, not
  a hit against a newer slot."

**Target**:
- No false-positive dedup hit: `dedup_lookup(client_id, seq)` returns
  `Some(slot)` only if `seq` was itself recorded at `slot` (or is
  within the per-client window and was recorded), never the slot of a
  different seq.
- Concurrent same-`client_id` writes (as issued by the bench runner
  and any pipelining client) never silently drop a payload.
- Retention matches `design.md`: at least 64 requests per client
  retained; behavior outside the window is documented as
  "outcome unknown, safe to re-propose" (unchanged from today).

**Acceptance**:
- Existing same-seq retry tests (`dedup_test.rs`,
  `learner_dedup_test.rs`) still pass for the exact-match case.
- The "older/lower seq maps to latest slot" assertions are corrected to
  reflect exact-seq lookup (a lower, never-recorded seq is a miss, not
  a hit against a higher seq's slot). Four existing assertions encode
  the buggy behavior and must be flipped:
  - `crowkv/tests/replica/dedup_test.rs::learn_chosen_populates_dedup_cache`
    — `dedup_lookup(42, 4) == Some(1)` ("Lower seq also hits").
  - `crowkv/tests/replica/dedup_test.rs::dedup_tracks_highest_seq_per_client_across_slots`
    — `dedup_lookup(7, 1) == Some(2)` ("older seq maps to latest slot").
  - `crowkv/tests/paxos/learner_dedup_test.rs::dedup_lookup_returns_slot_for_already_applied_seq`
    — `dedup_lookup(42, 2) == Some(5)` ("lower seq also returns commit slot").
  - `crowkv/tests/paxos/learner_dedup_test.rs::dedup_tracks_highest_seq_per_client`
    — `dedup_lookup(7, 1) == Some(3)` ("older seq maps to latest slot").
- No regression in write-path throughput benchmarks (the dedup check
  is off the common Paxos critical path either way; only the lookup
  structure changes from O(1) single-entry to O(1) amortized
  ring-buffer scan over N ≈ 64 entries).
- New repro test proving the false-positive is closed, added to
  `crowkv/tests/replica/dedup_test.rs`:

```rust
#[tokio::test]
async fn dedup_does_not_false_positive_on_out_of_order_higher_seq() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    // Same client, two logically distinct writes: seq=100 (key "a") and
    // seq=105 (key "b"). seq=105 is learned FIRST (out-of-order slot
    // choice -- e.g. seq=105's proposal happened to win its quorum
    // round before seq=100's did).
    let entry_b = write_entry(2, b"b", b"vb"); // slot 2, seq 105
    replica.learn_chosen(&entry_b, Some(77), Some(105)).await;

    // BUG (pre-fix): seq=100 has never been recorded, but the old
    // single-entry "latest wins" dedup treats 100 <= 105 as a hit and
    // would return `Some(2)` here -- the slot of an unrelated write,
    // for a payload ("a") that was never proposed. That is a silent
    // data-loss false positive: `propose` would short-circuit to
    // `Chosen { slot: 2 }` for the seq=100 caller without ever running
    // Paxos for key "a".
    //
    // FIXED behavior: an unrecorded seq is a miss, regardless of any
    // higher seq already committed for the same client.
    assert!(
        replica.learner.dedup_lookup(77, 100).is_none(),
        "seq=100 was never recorded; a higher committed seq=105 must not \
         produce a false-positive hit against its slot"
    );

    // Now seq=100 (key "a") is actually proposed and learned at slot 3.
    let entry_a = write_entry(3, b"a", b"va");
    replica.learn_chosen(&entry_a, Some(77), Some(100)).await;

    // A genuine retry of seq=100 now correctly hits its own slot (3),
    // not seq=105's slot (2).
    assert_eq!(replica.learner.dedup_lookup(77, 100), Some(3));
    // seq=105's own record is unaffected.
    assert_eq!(replica.learner.dedup_lookup(77, 105), Some(2));
}

#[tokio::test]
async fn dedup_retains_at_least_64_requests_per_client() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    // Commit seq=1..=80 for one client, each at its own slot.
    for seq in 1u64..=80 {
        let entry = write_entry(seq, format!("k{seq}").as_bytes(), b"v");
        replica.learn_chosen(&entry, Some(5), Some(seq)).await;
    }

    // The most recent >= 64 requests must still be individually
    // retrievable by their own seq (not just the latest).
    for seq in 17u64..=80 {
        assert_eq!(
            replica.learner.dedup_lookup(5, seq),
            Some(seq),
            "seq={seq} should still be in the retained window"
        );
    }
}
```

  The first test is the direct regression test for this bug: written
  against the *current* (buggy) code it fails (`dedup_lookup(77, 100)`
  returns `Some(2)`); after the fix it passes. The second test pins the
  `design.md`-documented "≥ 64 requests per client" retention.

**Dependencies**: None — self-contained to `PxLearner`'s dedup
structure.

**Priority**: High — correctness/data-loss bug, reachable by the
documented concurrent-pipelining client usage pattern (shared
`Arc<CrowkvClient>` with the default `ids=None` write path).

**Complexity**: Low-medium — confined to `PxLearner`'s dedup map and
its two call sites (`dedup_lookup`, `record_dedup`); touches four
existing test assertions' expectations.

**Files**: `crowkv/src/paxos/learner.rs` (dedup structure + lookup/record),
`crowkv/tests/replica/dedup_test.rs`,
`crowkv/tests/paxos/learner_dedup_test.rs`,
`doc/design/design.md` (confirm retention wording still matches once
fixed).
