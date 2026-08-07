<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R55: Scan — Carry Page-1 `read_slot` Forward as `min_slot`

**Problem**: a multi-page linearizable scan pays the read barrier once
per page. `PxKvStore::kv_scan` calls `resolve_read_point` on every page
(`px_kv_store.rs:183`); for `Linearizable` mode that is a lease check
(or a quorum heartbeat round on the `ReadIndex` fallback) per page. A
full-keyspace scan over 100k keys at 3.5 MiB/page is ~30 barrier rounds.

The barrier exists to prove freshness of **the first page**. Subsequent
pages do not need a fresh snapshot — a paginated scan is already a
sequence of per-page-consistent slices, not one snapshot (a value can
change or a key vanish between pages; this matches S3-list semantics).
Each later page only needs to be **at least as fresh as page 1**, which
is exactly what `MinSlot` with `min_slot = page1.read_slot` guarantees:
the store serves locally when `contiguous_applied >= min_slot`
(`px_kv_store.rs:575`), skipping the barrier entirely, and redirects to
the leader (`NotLeader`) only if the chosen replica hasn't caught up to
page 1's slot.

**Solution**: after the first page of a `Linearizable` scan, switch
subsequent pages to `MinSlot` using page 1's `read_slot` as the
`min_slot` floor.

- The client already receives `read_slot` in every `KvScanResponse`
  (`kv.proto:149`, field 8) and already sends `min_slot` on every
  `KvScanRequest` (`kv.proto:130`, field 9). No proto change.
- In `CrowkvClient::scan` (`client.rs`), once page 1 returns
  successfully with `read_slot = S`, set the per-page request's
  `read_mode = MinSlot` and `min_slot = S` for pages 2..N. Keep the
  caller's original `read_mode` for page 1.
- `MinSlot` endpoint selection is round-robin across replicas when
  distributed; a replica that hasn't applied `S` returns
  `not_leader_hint`, which the existing retry path already follows
  (`client.rs:851-861`). So correctness is preserved: a stale replica
  falls back to the leader, which has `S` applied.
- If the caller originally requested `MinSlot`, behavior is unchanged
  (the client already passes the caller's `min_slot` on every page).

**Semantics**: unchanged. Cross-page results were never a single
snapshot; each page remains at least as fresh as page 1. The only
observable difference is that later pages may be served by a follower
(distributed read) instead of the leader — which is the same shape as an
explicit `MinSlot` scan and is safe under the slot floor.

**Scope**:
- `lib/crow-kv-client/src/client.rs` — `scan` pagination loop: capture
  page-1 `read_slot`, switch `read_mode`/`min_slot` for subsequent
  pages. ~10-20 lines.
- No proto, FFI, engine, or store changes.
- Tests: extend `lib/crow-kv/tests/store.rs` (or a scan-specific test)
  to assert that a multi-page linearizable scan against a 3-node
  cluster serves pages 2..N without a leader barrier (e.g. via a
  `test-util` hook counting `resolve_read_point` quorum rounds, or by
  observing follower-served pages).

**Complexity**: Low–medium. The change is small and client-local, but
the correctness argument (slot-floor freshness vs per-page snapshot
semantics) must be documented and tested carefully.

**Dependencies**: none. The `MinSlot` serving path and
`read_slot`/`min_slot` proto fields already exist.

**Acceptance**:
- A multi-page `Linearizable` scan returns the same key set as before
  (no duplicates, no gaps) across a redirect mid-scan.
- Pages 2..N of a `Linearizable` scan are served via the `MinSlot` path
  (no per-page quorum barrier); a test confirms the barrier count for
  an N-page scan is 1, not N.
- A `MinSlot` scan's behavior is unchanged.
- Existing scan tests and `tools/bench-scan-regression.sh` pass; the
  multi-page linearizable config shows reduced per-page latency.

**Note**: the gap lives in
`doc/design/kv/kv-scan-flow-analysis.md` Gap Analysis → Performance →
"Per-page linearizable read barrier".
