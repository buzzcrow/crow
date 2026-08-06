<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R51: S3-style scan pagination + server byte budget, drop ScanStream

**Status**: Proposed.

**Problem**: The `ScanStream` server-streaming RPC exists solely to
bypass gRPC's unary 4 MiB message-size cap for large scans. It is "fake
streaming": the server awaits the full `KVFuture<Vec<...>>` before
emitting chunk 1, then slices the complete result into `KvScanChunk`
frames (256 entries / 1 MiB each); the client reassembles all chunks
back into one `Vec` before returning. Peak server and client memory are
both O(total); time-to-first-item equals the full scan latency. For
scans that already fit under 4 MiB (the majority), the chunking,
reassembly loop, and extra `KvScanChunk` allocations are pure overhead
with no benefit — negligible vs the ~4.3 ms per-scan fixed cost, but
real complexity for zero gain.

The wire protocol already supports S3-style pagination:
- `KvScanRequest.start_after` — exclusive lower bound (S3 marker /
  continuation key); empty = start from beginning.
- `KvScanRequest.limit` — entry-count cap; `0` = unlimited.
- `KvScanResponse.truncated` — set when more matches existed than were
  returned (S3 `IsTruncated`).

The unary `scan` client method already implements the pagination loop
(`start_after = last_key_returned`, repeat until `truncated == false`).
The engine applies both `start_after` and `limit` natively (descent
targets the leaf containing `start_after`, merge loop skips keys
`<= start_after`, `limit` applied without over-fetching — O(limit), not
O(prefix range)).

What is missing: a **server-side byte budget** so every unary response
is provably bounded regardless of value sizes. The server today caps
only by entry count (`limit as usize`); response size =
Σ(key.len + value.len), which the client cannot predict from `limit`
because each KV has a different length — `1000 × 1 KiB` cannot be
estimated. A `limit=1000` scan with 16 KiB values produces a 16 MiB
response that blows the 4 MiB unary cap even with pagination, because
the *page itself* is too big. This is the `valuesize_16KiB` config's
309 residual errors.

**Is the 4 MiB limit hard?** No. It is tonic's *default* for
`max_decoding_message_size` / `max_encoding_message_size`, configurable
on both `Server` (per-service) and `Channel` (per-client). The codebase
does not set these today — `Server::builder()` in
`lib/crow-kv/src/cluster/kv_server.rs` uses the 4 MiB default. The
`max_message_size: 67108864` (64 MiB) field in
`crow-kv-config.sample.json` is **never read** (zero references in
source). So the cap is configurable today without replacing gRPC; the
byte-budget design is the transport-independent fix, and raising the
tonic limit is an orthogonal knob.

**Target**:
- Add a `byte_budget` parameter to the C++ scan path: `scan`,
  `scan_async`, `try_scan_no_load`, `scan_async_attempt`. In the merge
  loop, accumulate `key.size() + value.size()` per emitted entry and
  stop (set `truncated = true`) when the accumulated size exceeds the
  budget — alongside the existing entry-count `limit` check. This stops
  at the source: no over-fetch, no discard. `scan_entry` already carries
  `std::string key/value` (`crow-tree.h:62`), so sizes are known in-loop.
  The budget is **measured incrementally**, not estimated from
  `limit × avg_size` — estimation would either overshoot (blow the cap)
  or undershoot (waste round-trips).
- Thread `byte_budget` through the FFI (`try_scan` / `Crowtree::scan`),
  the `KVEngine::scan` trait, and `px_kv_store::kv_scan`. A server-fixed
  budget (e.g. 3.5 MiB, leaving margin for proto framing under the 4 MiB
  cap) is the minimal design; a client-controlled `byte_budget` field on
  `KvScanRequest` is the flexible variant. Start with server-fixed.
- **Oversized-single-entry policy: always return at least one entry,
  even if it alone exceeds the budget.** When a single entry's
  `key.len + value.len` is larger than the budget, the server emits
  that one entry (so the client makes progress), sets `truncated = true`
  if more matches remain, and emits a **warning log** identifying the
  oversized key. This means the response bound is
  `max(byte_budget, largest_single_entry)`, not a strict byte_budget
  ceiling. The warning surfaces the pathological case (a value larger
  than the budget) so operators can investigate; the key is logged so
  the offending entry is identifiable. Rationale: the alternative
  (reject the entry) makes the key permanently unscannable and forces
  every scan to error out on it; returning it with a warning keeps the
  scan functional while flagging the anomaly.
- **Delete** `rpc ScanStream`, `chunk_scan_response`, the client
  `scan_stream` method, and the reassembly loop. The unary `scan` path
  already handles `start_after` + `truncated` pagination correctly; it
  only needs the byte budget to guarantee the response fits the cap.
- **After gRPC is replaced** (R32, custom Rust RPC): the byte budget
  **stays in both roles** — it is not just a gRPC workaround. The
  hard-stop cap is a safety boundary that protects against injection /
  abuse / runaway values regardless of transport, so it persists after
  R32; only its *constraint value* changes (no longer forced to 4 MiB
  by gRPC's default — the operator sets it as a safety bound, e.g. 16
  or 64 MiB). The large-value warning also stays. What R32 removes is
  the *coupling* between the cap and gRPC's 4 MiB default — the cap
  becomes an independent operator knob, not a transport-imposed
  ceiling. The `start_after` + `truncated` pagination is
  transport-independent and stays unchanged. Concretely: post-R32, the
  engine still stops on `min(limit, byte_budget)` and still warns on
  oversized entries; the only difference is `byte_budget` is no longer
  pinned to ~3.5 MiB to fit under gRPC's 4 MiB default.

**Acceptance**:
- `full_100k` (100k entries, 64 B values) completes via unary `scan`
  with client pagination — no transport errors. Previously 0 scans/s
  with 6 transport errors under the unary cap; the stream fixed this
  to 20 scans/s. The unary+pagination path must match or exceed 20
  scans/s (pagination adds per-page RTT, but each page is now a cheap
  unary call; the engine re-descends per page, O(log N) each, unless
  paired with an opaque continuation token — out of scope here, see
  R48 lazy-resolver follow-up).
- `valuesize_16KiB` (limit=1000, 16 KiB values): the byte budget stops
  the scan early (one page ≈ 200 entries under 3.5 MiB), sets
  `truncated = true`, client pages to completion. Zero transport errors
  (improves on the stream's 309 residual errors). A single 16 KiB
  value is well under the budget, so the oversized-entry path is not
  exercised here.
- Oversized-entry path: a synthetic test with a value larger than the
  budget (e.g. 5 MiB value, 3.5 MiB budget) returns that one entry,
  sets `truncated` correctly, and emits the warning log with the key.
- No regression in `tools/bench-scan-regression.sh` for in-cap configs
  (`bounded_*`, `full_1k/10k`, 64 B / 1 KiB values): unary `scan` with
  no byte-budget stop should match the prior unary numbers (the stream
  added negligible overhead, so removing it recovers negligible
  overhead).
- Existing scan tests pass (`test-tree-ct` ReadPath.* + AsyncScan.*,
  Rust scan tests).
- `cargo fmt --check`, `cargo clippy -- -D warnings`,
  `clang-format --dry-run --Werror`, `tree-lint` all pass.

**Complexity**: Medium.
- C++ engine: add `byte_budget` to 4 scan entry points + the merge
  loop's stop condition. The merge loop already visits entries one at a
  time, so the accumulation is a local counter + one comparison. The
  oversized-entry "always return ≥1" rule needs a guard: the budget
  check fires only when `batch` is non-empty (mirroring
  `chunk_scan_response`'s existing `!batch.is_empty()` guard at
  `kv_service.rs:790`).
- FFI + trait + `px_kv_store`: thread one `usize` parameter through
  ~5 call sites.
- Deletion: `ScanStream` rpc, `chunk_scan_response` (~75 lines),
  client `scan_stream` + reassembly (~90 lines), proto `ScanStream` +
  `KvScanChunk` messages.
- Warning log: one `warn!` in the engine stop path when the emitted
  batch contains a single entry exceeding the budget.

**Dependencies**: None blocking. Orthogonal to R48 (lazy L1 leaf
resolver, scan *cost*) and R50 (epoch-protected MemTable, scan/get
*copy cost*) — this item is about scan response *size* and transport,
not scan engine cost. Pairs naturally with R32 (custom Rust RPC):
after R32 lands, the byte budget stays (both hard-stop and warning) —
R32 only decouples the cap value from gRPC's 4 MiB default, making it
an independent operator safety knob. The pagination design is
unchanged. A follow-up item (opaque continuation token / live engine
cursor) would remove the per-page O(log N) re-descent cost that
pagination introduces; that is the same engine-cursor work R48 touches
and is out of scope here.

**Open questions**:
- Server-fixed vs client-controlled `byte_budget`: start server-fixed
  (simpler, one config knob). Promote to a `KvScanRequest` field only
  if a use case needs per-scan control.
- Budget value: 3.5 MiB leaves ~0.5 MiB for proto framing + metadata
  under the 4 MiB default. If tonic's `max_decoding_message_size` is
  raised (configurable today, unused), the budget can scale up
  proportionally to reduce pagination round-trips for large scans.
- Post-R32, should the warning threshold be the same value as the
  hard-stop cap, or a separate `scan_warn_value_bytes` knob? Same
  value is simpler (one knob, dual role); separate is cleaner if the
  safety cap and the anomaly threshold want to diverge (e.g. cap at
  64 MiB for safety, warn at 1 MiB for anomaly detection).
