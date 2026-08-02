<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R41 Design — Bounded per-client dedup window

Companion plan: `plan-dedup-window.md`. Backlog context:
`R41-dedup-window.md`. Formal design home: `design/design.md` §10
Idempotency (merge target after implementation).

## Problem

`design.md` §10 promises *"Bounded idempotency for retried requests
via `(client_id, seq)` dedup … Retention: ≥ 64 requests per client
AND ≥ 60s."* The implementation does not match. `PxLearner` keeps
exactly **one** `DedupEntry { last_seq, last_slot }` per client
("latest wins"):

- `dedup` field: `DashMap<u64, DedupEntry>` (`paxos/learner.rs` L62).
- `dedup_lookup` (`paxos/learner.rs` L286): `.filter(|e| seq <=
  e.last_seq).map(|e| e.last_slot)` — returns the *latest* slot for
  any `seq <= last_seq`, even one never recorded.
- `record_dedup` (`paxos/learner.rs` L299): `if seq > e.last_seq {
  e.last_seq = seq; e.last_slot = slot; }` — only the highest seq is
  retained.

`PxGroup::propose` calls `dedup_lookup` unconditionally at entry,
before slot allocation (`cluster/group.rs` L1182, before
`next_slot.fetch_add` at L1217). `CrowkvClient` is designed to be
shared (`Arc<CrowkvClient>`) across concurrent callers under one
`client_id`, with `seq` assigned via a shared `AtomicU64::fetch_add`
on the default `ids=None` write path (`crowkv-client/src/client.rs`
`put` L273, `delete` L432, `batch_write` L505). Multi-slot
concurrency lets slots be chosen out of order
(`write-flow-analysis.md` § Multi-Slot Concurrency).

Combining these: if a higher-`seq` request commits before a
lower-`seq` request from the same `client_id` reaches `propose`
(plausible during an election, a slow/retried RPC, or scheduling
jitter), the lower-`seq` request's `dedup_lookup` sees
`seq <= last_seq` and returns `ProposeResult::Chosen { slot:
<higher-seq's slot> }` **without ever running Paxos for its own
payload.** The caller receives a success response; its actual
key/value write is silently dropped. Data-loss correctness bug.

The bench runner does **not** trigger this — it bypasses the shared
`client_id`/`next_seq` by passing an explicit per-worker
`client_id = worker_id + 1` and a per-worker monotonic `iter` counter
(`bench/runner.rs` L560-562, L577-579). The bug is reachable via the
client API's default `ids=None` write path on a shared
`Arc<CrowkvClient>`.

## Current behavior (the false positive)

```
PxGroup::propose(payload, client_id=Some(C), seq=Some(100))
  // entry: leadership gate passes
  if let Some(slot) = learner.dedup_lookup(C, 100):   // seq=100 <= last_seq=105?
      return ProposeResult::Chosen { slot }           // YES → returns slot of seq=105's write
  // (Paxos for key "a" never runs; payload dropped)
```

`record_dedup` only ever raises `last_seq`; a lower seq learned later
does not regress the record, so once a higher seq is recorded, every
lower seq for that client is permanently a false-positive hit
against the higher seq's slot.

## Proposed approach

Replace the single `DedupEntry` with a small bounded per-client
window of `(seq -> slot)` mappings, exact-match lookup, count-based
eviction.

- **Structure**: `DashMap<u64, DedupWindow>` where `DedupWindow` is a
  `VecDeque<(u64, SlotIndex)>` capped at `N = 64`. Keep the outer
  `DashMap` so per-client isolation and the `client_id == 0` sentinel
  short-circuit are unchanged. `VecDeque` (not a hash map) is fine:
  N is tiny and the common case is a hit on the most-recent entry,
  scanned first.
- **`dedup_lookup(client_id, seq)`**: exact-match scan of the
  client's window. Returns `Some(slot)` only if `seq` is itself
  recorded; an unrecorded seq (lower or otherwise) is a miss.
  `client_id == 0` still returns `None` immediately.
- **`record_dedup(client_id, seq, slot)`**: append `(seq, slot)` to
  the client's window. If `seq` is already present (idempotent
  re-`learn` of the same slot, e.g. a duplicate `Chosen` notice),
  leave the existing entry in place — no duplicate, no slot
  overwrite. If the window exceeds N after the append, drop the
  oldest entry. No wall-clock tracking.
- **N = 64** matches the `design.md` "≥ 64 requests per client"
  floor and is sized generously relative to
  `max_inflight_proposals` (default 32): a full window of concurrent
  same-client requests never evicts an unresolved entry prematurely.
- **Time-based retention (`≥ 60s`)** is dropped. With N=64 and the
  default inflight cap of 32, a same-client request that is still
  in-flight when its record would be evicted cannot exist (at most
  32 are in flight; the 33rd-oldest committed record is safely
  evictable). The `design.md` wording is updated to match (count-only
  retention). Outside the window, the existing "outcome is unknown,
  safe to re-propose" semantics apply unchanged.

### Alternatives considered

- **`BTreeMap<u64, SlotIndex>` per client** — O(log N) lookup, but
  supports range queries we don't need. `VecDeque` is simpler and
  faster for N=64 with most-recent-first access.
- **Hash map per client** — O(1) lookup, but per-client allocation
  overhead for a 64-entry table is not worth it; the linear scan of
  a 64-entry `VecDeque` is cache-friendly and the fast path (retry
  of the latest seq) hits on the first comparison.
- **Keep `last_seq`/`last_slot` plus a window** — the `last_seq`
  fast path is exactly the bug; removing it is the fix. The window's
  most-recent entry already serves the common sequential-retry case
  on the first comparison.
- **Wall-clock eviction** — adds a `Instant` per entry and a sweep
  trigger. Unnecessary given the N=64 vs inflight=32 sizing; would
  re-introduce the very `≥ 60s` wording we're simplifying away.

## Acceptance test plan

- Existing exact-match retry tests still pass: `dedup_test.rs`
  `dedup_suppresses_retried_request_at_same_slot`,
  `dedup_is_per_client`, `dedup_ignores_client_id_zero`,
  `dedup_ignores_entries_without_client_id`;
  `learner_dedup_test.rs` `dedup_lookup_returns_none_for_fresh_client`,
  `dedup_lookup_returns_none_for_client_id_zero`,
  `dedup_lookup_returns_none_for_higher_seq`,
  `dedup_is_per_client`,
  `dedup_ignores_entries_without_client_id`.
- Four "older/lower seq maps to latest slot" assertions are flipped
  to expect a miss (an unrecorded lower seq is a miss, not a hit
  against a higher seq's slot):
  - `dedup_test.rs::learn_chosen_populates_dedup_cache` —
    `dedup_lookup(42, 4)` → `None`.
  - `dedup_test.rs::dedup_tracks_highest_seq_per_client_across_slots`
    — `dedup_lookup(7, 1)` → `None`.
  - `learner_dedup_test.rs::dedup_lookup_returns_slot_for_already_applied_seq`
    — `dedup_lookup(42, 2)` → `None`.
  - `learner_dedup_test.rs::dedup_tracks_highest_seq_per_client` —
    `dedup_lookup(7, 1)` → `None`.
- Two new repro tests added to `dedup_test.rs`:
  - `dedup_does_not_false_positive_on_out_of_order_higher_seq` —
    seq=105 learned at slot 2 first, then seq=100 at slot 3; asserts
    the unrecorded seq=100 misses, then both exact lookups hit their
    own slots.
  - `dedup_retains_at_least_64_requests_per_client` — insert
    seq=1..=80, assert seq=17..=80 (last 64) still retrievable by
    exact seq.
- `design.md` §10 Idempotency retention wording updated to match
  (count-only, N=64).

## Files

- `crowkv/src/paxos/learner.rs` — replace `DedupEntry` with
  `DedupWindow` (`VecDeque<(u64, SlotIndex)>`, cap 64); rewrite
  `dedup_lookup` (exact-match scan) and `record_dedup` (append +
  evict-oldest, idempotent on duplicate seq).
- `crowkv/tests/replica/dedup_test.rs` — flip one assertion in
  `learn_chosen_populates_dedup_cache`, flip one in
  `dedup_tracks_highest_seq_per_client_across_slots`, add two new
  repro tests.
- `crowkv/tests/paxos/learner_dedup_test.rs` — flip one assertion in
  `dedup_lookup_returns_slot_for_already_applied_seq`, flip one in
  `dedup_tracks_highest_seq_per_client`.
- `doc/design/design.md` §10 Idempotency — update retention wording.
