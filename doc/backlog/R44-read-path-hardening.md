<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R44: Read-path hardening (scan hint parity, error propagation, catch-up off the barrier)

**Problem**: the read-path review (see
`doc/working/read-flow-analysis.md`) confirmed the big items are
already tracked (R37 scan push-down, R38 scan zero-copy, R39 endpoint
policy, R32 transport) but found eight smaller gaps that are not.
Grouped in one requirement because they are all small, touch the same
read-path files, and are best done as one coherent hardening pass:

- **E1 — Scan forward-fail path drops the leader hint.** When
  `forward_kv_scan` fails, `KvStoreService::scan` falls through to a
  local scan without setting `not_leader_hint`
  (`kv_service.rs` L446-459), while the `get` handler explicitly sets
  `resp.not_leader_hint = endpoint` on the same path (L315). The
  store-level `scan_err` usually fills the hint, but the forwarder's
  known-good endpoint is lost when the store's own hint is empty, and
  the two handlers have drifted. The in-code comment (L455-458) also
  no longer matches the `get` behavior.
- **E2 — Scan errors silently swallowed.**
  `decode_scan_with_start_after` collapses every FFI error — including
  `CtError::Corruption` from the packed-result bounds checks — to an
  empty, non-truncated result (`crowtree_engine.rs` L367). A corrupt
  page reads as "no data" with `ok = true`. Partially mitigated by the
  latched `io_failed` health flag, but the client gets a silently
  wrong answer instead of an error.
- **E3 — Client retry matches errors by string.** The retry loop
  detects redirects via `error == "not leader"` exact string compare
  (`crowkv-client/src/client.rs` L698). Fragile against any
  server-side message change; a structured error code on `KvResponse`
  / `KvScanResponse` would make the contract explicit.
- **E4 — Client ignores topology refresh failures.**
  `wait_and_refresh_leader` and the transport-error handler both do
  `let _ = self.topology.refresh().await` (`client.rs` L709, L728);
  when all seeds are unreachable the retry loop keeps burning attempts
  against a known-stale endpoint instead of failing fast with a
  distinguishable error.
- **E5 — Peer catch-up runs on the ReadIndex critical path.**
  `run_heartbeat_round` replays accepts inline for any peer whose
  `contiguous_applied` lags `committed_safe_slot`
  (`group_election.rs` L527-628). A ReadIndex-fallback linearizable
  read therefore pays for follower recovery: one lagging replica
  inflates read latency exactly when the cluster is degraded. The
  catch-up work should be bounded per round or moved to a background
  task; quorum confirmation itself does not need it.
- **E6 — C++ `scan_async` restarts the whole scan on any cold
  leaf.** `crowtree.cpp` L2060-2067 retries the entire scan after each
  demand-load instead of resuming from a cursor, so a scan over many
  cold leaves re-traverses already-resolved leaves per retry
  (quadratic in cold-leaf count). Composes with R37: the
  lower-bound-seek cursor added there is the natural resume point.
- **E7 — Client-side response copies.** `get` returns
  `resp.value.to_vec()` and `scan` re-owns every entry via
  `(i.key.to_vec(), i.value.to_vec())` (`client.rs` L372, L617-621),
  copying data that already sits in prost `Bytes`; request keys are
  also copied via `Bytes::copy_from_slice`. A `Bytes`-based API
  variant (or switching the outcome types to `Bytes`) removes one
  allocation per get and two per scan entry.
- **E8 — Scan observability gaps.** `KvMetrics` splits get latency
  per mode (`get_linearizable_lh` / `get_min_slot_lh`) but scan has
  only the combined `scan_l` summary (`kv_service.rs` L89, L144-156).
  Also no over-fetch metric: fetched-vs-returned entry counts on the
  `start_after` path would quantify R37's win before building it.

**Approach** (independent items; E1/E2/E8 are the quick wins):

- **E1**: extract a shared forward helper for `get`/`scan` (same
  loop-guard, target lookup, metrics, hint-on-failure semantics), or
  minimally set `resp.not_leader_hint = endpoint` after the local
  scan on the forward-fail path and fix the stale comment.
- **E2**: change `decode_scan_with_start_after` to propagate errors;
  `KVEngine::scan`'s future resolves to a `Result`, and
  `PxKvStore::kv_scan` maps it to `scan_err` (internal error, not
  `NotLeader`). Callers that want the old lenient behavior do not
  exist — empty-on-corruption was never intentional API.
- **E3**: add an `error_code` enum field to `KvResponse` /
  `KvScanResponse` (proto3 default `0 = none` keeps the change
  wire-compatible); server sets it alongside the existing `error`
  string; client switches on the code and falls back to the string
  for old servers.
- **E4**: propagate the refresh result; after N consecutive refresh
  failures surface a `TopologyUnavailable`-style error instead of
  exhausting `max_retries` against the stale endpoint.
- **E5**: bound catch-up per heartbeat round (cap replayed slots per
  peer per round) and/or hand the replay to a background task fed by
  the round's lag observations. The quorum ack for ReadIndex must not
  wait on replay completion — only on the heartbeat replies.
- **E6**: add cursor resumption to `scan_async`: remember the last
  key resolved before the cold leaf and re-enter the scan at that
  lower bound after the demand-load completes. Best done together
  with (or immediately after) R37's lower-bound seek.
- **E7**: add `Bytes`-returning variants (or migrate the outcome
  structs) in `crowkv-client`; accept `impl Into<Bytes>` for keys and
  prefixes so callers holding `Bytes` pay no copy.
- **E8**: add `scan_linearizable_l` / `scan_min_slot_l` summaries
  mirroring the get split, plus `scan_overfetch_fetched_c` /
  `scan_overfetch_returned_c` counters on the `start_after` path.

Out of scope (already tracked elsewhere): scan `start_after`
push-down (R37), scan value zero-copy (R38), least-conn / latency
endpoint policy (R39), custom RPC transport (R32), redundant
forward-target lookup in `resolve_read_point` (R42).

**Performance impact**:
- E5 removes follower-recovery work from linearizable read latency —
  a p99 fix for the degraded-cluster case, no steady-state change.
- E6 turns cold-scan retry cost from quadratic to linear in
  cold-leaf count.
- E7 removes per-request/per-entry client allocations; matters for
  large values and wide scans.
- E1-E4, E8 are correctness, robustness, and observability; no
  hot-path cost.

**Dependencies**: none hard. E6 composes with R37 (shared cursor
machinery); E8's over-fetch counters become obsolete once R37 lands
(keep the per-mode summaries). E3 touches the proto used by both
read and write responses — coordinate with any concurrent proto
change.

**Priority**: Medium — E1/E2 are small correctness fixes worth doing
soon; E5 is the only latency item and only bites during recovery.

**Complexity**: Low-medium — E1/E2/E8 are mechanical; E3 is a proto
addition with fallback; E4 is client-local; E5 needs care to keep
quorum semantics intact; E6 is C++ engine work gated on R37's shape.

**Files**: `crowkv/src/rpc/kv_service.rs` (forward helper, hint,
metrics), `crowkv/src/kv/crowtree_engine.rs` (error propagation,
over-fetch counters), `crowkv/src/cluster/px_kv_store.rs` (scan error
mapping), `crowkv/src/cluster/group_election.rs` (bounded catch-up),
`crowkv/src/rpc/proto/kv.proto` (error code), `crowkv-client/src/client.rs`
(+ `topology.rs`) (structured errors, refresh handling, `Bytes` API),
`crowtree/src/crowtree.cpp` + `crowtree/ffi` (scan cursor resume).

**Acceptance**:
- E1: a MinSlot scan hitting a follower whose forward to the leader
  fails returns a response carrying the leader endpoint in
  `not_leader_hint`; get and scan share the forward code path.
- E2: an injected corrupt packed scan result surfaces as a scan
  error to the client, not an empty `ok` result.
- E3: client follows redirects driven by the error code with the
  string fallback covered by a test against the old-format response.
- E4: with all seeds unreachable, the client fails with a topology
  error before exhausting `max_retries`.
- E5: with one follower lagging by many slots, ReadIndex-fallback
  read latency stays within the heartbeat RTT budget (test-util
  injected lag); the follower still converges via the bounded /
  background replay.
- E6: a scan spanning N cold leaves performs O(N) leaf loads with no
  re-traversal of resolved leaves (observable via demand-load
  counters).
- E7: get/scan through the `Bytes` API show no per-value allocations
  beyond the transport frame (verified by inspection/benchmark).
- E8: per-mode scan latency and over-fetch counters visible via the
  metrics registry.
