<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R51 Design: S3-style scan pagination + server byte budget, drop ScanStream

## Problem

The `ScanStream` server-streaming RPC is "fake streaming": the server
materializes the full `KVFuture<Vec<...>>` before emitting chunk 1, then
slices the complete result into `KvScanChunk` frames; the client
reassembles all chunks back into one `Vec` before returning. Peak server
and client memory are both O(total); time-to-first-item equals the full
scan latency. For scans that already fit under 4 MiB (the majority), the
chunking, reassembly loop, and extra `KvScanChunk` allocations are pure
overhead.

The wire protocol already supports S3-style pagination
(`start_after` + `truncated` + `limit`), and the engine applies both
`start_after` and `limit` natively (descent targets the leaf containing
`start_after`, merge loop skips keys `<= start_after`, `limit` applied
without over-fetching). What is missing is a **server-side byte budget**
so every unary response is provably bounded regardless of value sizes.
The server today caps only by entry count (`limit as usize`); response
size = sum(key.len + value.len), which the client cannot predict from
`limit` because each KV has a different length. A `limit=1000` scan with
16 KiB values produces a 16 MiB response that blows the 4 MiB unary cap
even with pagination, because the *page itself* is too big.

The 4 MiB cap is tonic's *default* for
`max_decoding_message_size` / `max_encoding_message_size`, configurable
but unused today. The byte budget is the transport-independent fix.

## Current behavior (code-grounded)

- C++ engine: `Crowtree::scan` (`crow-tree.cpp:1720`) and
  `try_scan_no_load` (`crow-tree.cpp:1937`) each have a `consider` lambda
  that applies only the entry-count `limit` check
  (`if (limit != 0 && out->size() >= limit)`). No byte-size
  accumulation.
- `scan_async` / `scan_async_attempt` (`crow-tree.cpp:2143/2156`):
  `scan_async_attempt` calls `try_scan_no_load` and adjusts
  `remaining_limit` across cold-leaf retries; no byte-budget analog.
- C API: `ct_scan` (`c_api.cpp:897`) and `ct_scan_async`
  (`c_api.cpp:786`) pass `limit` only.
- FFI: `Crowtree::scan` (`ffi/src/lib.rs:1044`),
  `AsyncCrowtree::scan` (`ffi/src/lib.rs:1677`),
  `AsyncCrowtree::try_scan` (`ffi/src/lib.rs:1704`) pass `limit` only.
- `KVEngine::scan` trait (`kv_engine.rs:65`): `limit: usize` only.
- `CrowTreeEngine::scan` (`crow_tree_engine.rs:203`): calls
  `try_scan(prefix, start_after, limit)`.
- `PxLearner::engine_scan` (`learner.rs:220`): passes `limit` only.
- `PxKvStore::kv_scan` (`px_kv_store.rs:149`): calls
  `engine_scan(prefix, start_after, limit as usize)`.
- Proto: `KvScanRequest` has `limit` (entry count), `start_after`,
  `truncated`. `KvScanChunk` (`kv.proto:162`) is the streaming chunk
  message. `rpc ScanStream` (`kv.proto:183`).
- Server: `KvStoreService::scan` (`kv_service.rs:427`) is the unary
  handler; `scan_stream` (`kv_service.rs:558`) is the streaming handler
  that calls `chunk_scan_response` (`kv_service.rs:760`).
- Client: `CrowkvClient::scan` (`client.rs:758`) sends one unary request
  and returns — **no pagination loop**. `scan_stream` (`client.rs:839`)
  reassembles chunks into one `Vec`.
- Bench: `bench/runner.rs:658` calls `scan_stream` for `OpKind::List`.
- CLI: `commands/kv.rs:293` calls `scan` (single page, shows
  `truncated`).

## Proposed approach

### 1. C++ engine: `byte_budget` parameter

Add `size_t byte_budget` (0 = unlimited) to `scan`, `scan_async`,
`try_scan_no_load`, `scan_async_attempt`. In each `consider` lambda,
accumulate `accumulated_bytes += key.size() + value.size()` after
pushing an entry, and stop (set `truncated = true`, return false) when
`accumulated_bytes > byte_budget && out->size() > 1` — the
`out->size() > 1` guard implements the always-return-at-least-1 rule
(mirrors `chunk_scan_response`'s `!batch.is_empty()` guard at
`kv_service.rs:790`). A `byte_budget == 0` check makes the entire
accumulation path a no-op for unlimited scans (zero overhead for
`iter_all` / `live_key_count` / existing tests that pass 0).

Oversized-entry warning: after pushing an entry, if
`byte_budget != 0 && key.size() + value.size() > byte_budget`, emit
`CR_LOG_WARN` with the key size, value size, and budget. This covers
both the first-entry-alone-exceeds-budget case and the later-entry case.

### 2. `scan_async_attempt`: remaining byte budget across cold-leaf retries

`scan_async_attempt` accumulates entries across cold-leaf retries via
the `accumulated` vector. Before calling `try_scan_no_load`, compute
`accumulated_bytes = sum(e.key.size() + e.value.size())` over
`*accumulated`. If `byte_budget != 0 && accumulated_bytes >=
byte_budget && !accumulated->empty()`, deliver immediately with
`truncated = true` (the budget is already exhausted by prior retries'
entries). Otherwise pass `remaining_byte_budget = byte_budget -
accumulated_bytes` (or 0 if unlimited) to `try_scan_no_load`. This
mirrors the existing `remaining_limit` adjustment.

### 3. C API: `byte_budget` parameter

Add `size_t byte_budget` to `ct_scan` and `ct_scan_async` signatures.
`ct_scan` passes it to `Crowtree::scan`; `ct_scan_async` passes it to
`Crowtree::scan_async`.

### 4. FFI: `byte_budget` parameter

Add `byte_budget: usize` to `Crowtree::scan`, `AsyncCrowtree::scan`,
`AsyncCrowtree::try_scan`. Pass through to the C API. Default 0 for
callers that don't need it (`iter_all`, `live_key_count`).

### 5. `KVEngine::scan` trait: `byte_budget` parameter

Add `byte_budget: usize` to the trait method. Both implementors update:
- `CrowTreeEngine::scan`: passes `byte_budget` to `try_scan`.
- `InMemKV::scan`: accumulates `key.len() + value.len()` in its
  sort-and-truncate loop, stops with `truncated = true` when the budget
  is exceeded (same always-return-1 guard). InMemKV is test-only, so
  this is a simple loop accumulation, not a merge-loop change.

### 6. `PxLearner::engine_scan` + `PxKvStore::kv_scan`: thread the budget

`engine_scan` gets a `byte_budget: usize` parameter.
`PxKvStore::kv_scan` applies a server-fixed constant
`SCAN_BYTE_BUDGET` (3.5 MiB = 3 * 1024 * 1024 + 512 * 1024, leaving
~0.5 MiB for proto framing under tonic's 4 MiB default). The budget is
server-internal — not on the wire — so `KvScanRequest`, the
`kv_store::kv_scan` trait, and the `kv_service` handlers are unchanged.

### 7. Client `scan`: internal pagination loop

The current `scan` method sends one unary request and returns. Add an
inner pagination loop: after receiving a page, if `truncated` and the
total collected is below the caller's `limit` (or `limit == 0`), set
`start_after = last_key` and send the next page with
`limit = remaining`. Stop when `!truncated`, total >= caller's `limit`,
or a page returns 0 items (safety). The outer retry/redirect loop is
unchanged — on transport error or not-leader redirect, the inner
pagination restarts from the beginning with the (possibly new)
endpoint. The `ScanOutcome.truncated` flag in the result means "more
entries exist beyond the caller's `limit`".

### 8. Delete `ScanStream`

Remove:
- Proto: `rpc ScanStream`, `message KvScanChunk`.
- Server: `scan_stream` handler, `chunk_scan_response` function,
  `ScanStreamStream` type alias, `KvScanChunk` import.
- Client: `scan_stream` method, `KvScanChunk` import,
  `tokio_stream` usage in the scan path.
- Bench: `scan_stream` call → `scan`.

## Alternatives considered

- **Client-controlled `byte_budget` on `KvScanRequest`**: more flexible
  (per-scan control) but adds a wire field and client complexity with
  no current use case. Server-fixed is simpler (one constant). Promote
  to a wire field only if a use case needs per-scan control (R51 open
  question).
- **Raise tonic's `max_decoding_message_size` instead**: orthogonal —
  raises the transport ceiling but does not bound the response. A
  `limit=1000` scan with 1 MiB values still produces a 1 GiB response.
  The byte budget bounds the response at the source; raising the tonic
  limit is an independent operator knob that can be combined with a
  larger budget to reduce pagination round-trips.
- **Opaque continuation token / live cursor**: removes the per-page
  O(log N) re-descent cost that pagination introduces. Out of scope
  (same engine-cursor work R48 touches). The byte budget is orthogonal
  to the cursor mechanism.
- **Keep `ScanStream` alongside the byte budget**: the stream adds
  complexity (chunking, reassembly, `KvScanChunk` proto) for zero gain
  once the unary path is bounded. The unary + pagination path is
  strictly simpler.

## Acceptance test plan

- `full_100k` (100k entries, 64 B values): completes via unary `scan`
  with client pagination — no transport errors. Must match or exceed
  20 scans/s.
- `valuesize_16KiB` (limit=1000, 16 KiB values): byte budget stops the
  scan early (one page ~200 entries under 3.5 MiB), sets `truncated`,
  client pages to completion. Zero transport errors.
- Oversized-entry path: synthetic C++ test with a value larger than the
  budget (e.g. 5 MiB value, 3.5 MiB budget) returns that one entry,
  sets `truncated` correctly, and emits the `CR_LOG_WARN` warning.
- No regression in `tools/bench-scan-regression.sh` for in-cap configs.
- Existing scan tests pass: `test-tree-ct` ReadPath.* + AsyncScan.*,
  Rust scan tests (conformance, crow_tree_engine, mem_kv, kv_forward).
- `cargo fmt --check`, `cargo clippy -- -D warnings`,
  `clang-format --dry-run --Werror`, `tree-lint` all pass.
