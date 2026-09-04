<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R132: diskdb — Compaction Slot Ordering

## Problem

### Current behavior + impact

DiskDB orders free records with `FreeBlockValue.freed_ts`, generated before
the asynchronous KV write starts. Paxos permits several slots to progress
and apply concurrently, so timestamp creation order does not establish KV
commit or apply order. A lower-timestamp free can stall while a later free
is chosen, applied, scanned, and used to advance `compact_ts`. When the
stalled free eventually commits, the next compaction classifies it as stale
and deletes it without clearing its allocated bitmap range. Capacity remains
permanently busy even though the free was acknowledged.

The KV engine already tracks the highest applied Paxos slot for each key,
and point gets expose that value as a revision. Scan items omit it. A normal
linearizable scan waits for an applied freshness floor, but it reads current
single-version state and later pages may observe a newer frontier. Its
`read_slot` is not a fixed upper visibility boundary. Filtering records by a
watermark after such a scan therefore does not establish a complete,
gap-free set.

This is a correctness issue, not only leaked-space accounting. Repeated mix
workloads can strand freed capacity, report busy-space drift, and eventually
reach `NoSpace` while reclaimable blocks exist.

### Design pointers

- `doc/design/kv/design-crowdb-kv-slot.md` §6–§9A and §13–§14 define
  per-key resolved slots, out-of-order apply, gaps, and the contiguous
  applied frontier.
- `doc/design/kv/design-crowdb-kv-state-machine.md` §3–§4 defines
  single-version reads, per-key revision state, and the apply fence.
- `doc/design/diskdb/design-crowdb-diskdb-zone-management.md` §3, §5,
  §6, §8, and §10 define free records, compaction, recovery, ownership,
  and bitmap invariants.

### Use scenarios

- **Out-of-order free apply** — free A starts first but its lower Paxos slot
  is delayed; free B commits and applies at a higher slot. Compaction does
  not process B past the gap, then processes both after the contiguous-
  applied frontier covers A and B.
- **Concurrent free during compaction** — compaction captures a fixed
  contiguous-applied cutoff while new free records continue committing.
  It merges only the complete bounded set and leaves later revisions for
  the next pass.
- **Paginated zone scan** — a zone has more free records than one scan page.
  Every page uses the same cutoff, and compaction advances its watermark only
  after the complete prefix has been read and persisted.
- **Restart after compaction** — DiskDB restarts from a zone value carrying
  a compaction slot, replays later records, and reconstructs exactly the
  durable busy set without depending on wall-clock or process-local
  timestamp state.
- **Mixed-version rollout** — a stored legacy zone value has `compact_ts`
  but no compaction slot. DiskDB takes a conservative recovery path and does
  not lose a free record during upgrade.
- **Non-owner attempt** — an instance without the disk group's immutable
  owner record cannot compact or delete its records. The assigned owner
  remains the only compactor.

## Solution

Expose record revision slots through KV reads, add a fixed-cutoff scan over
the contiguous-applied prefix, and use its cutoff as DiskDB's compaction
watermark.

1. **KV record revision surface** —
   `lib/crowdb-kv/src/kv/`, `lib/crowdb-protocol/src/types/kv_client.rs`,
   the KV FlatBuffers schema and transport adapters, and
   `lib/crowdb-kv-client/src/client.rs`: preserve the engine's per-key
   resolved slot on every returned live record as its `commit_slot` (the
   record revision). Keep point-get revision behavior consistent and add the
   slot to scan items without copying record values.
2. **Contiguous-applied bounded scan** — `lib/crowdb-kv/src/rpc/`, KV server
   scan handling, protocol types, and `crowdb-kv-client`: add a scan mode
   that captures one fixed cutoff from the leader's contiguous-applied
   frontier after the linearizable barrier. Return only current live records
   whose revision is at or below that cutoff, carry the same cutoff through
   all pages, and return it to the caller. The API is a bounded current-
   version scan, not a historical MVCC snapshot.
3. **Pagination and failure contract** — `lib/crowdb-kv-client/src/client.rs`:
   reject a changed or missing cutoff on later pages and distinguish a
   complete prefix from timeout, truncation, leader change, and decode
   failure. A caller may publish a watermark only from a successfully
   completed bounded scan.
4. **Slot-based DiskDB compaction** —
   `app/crowdb-diskdb/src/ddb_kv_client.rs`, `recovery/compaction.rs`, and
   `model/zone.rs`: scan free records at one contiguous-applied cutoff,
   merge records in `(compact_slot, scan_slot]`, and atomically persist the
   updated `ZoneValue` with `compact_slot = scan_slot` while deleting all
   scanned free keys whose record revision is at or below `scan_slot`,
   including already-merged stale keys. Remove correctness dependence on
   `freed_ts` and the per-process timestamp generator.
5. **Persisted schema and recovery** —
   `lib/crowdb-protocol/src/types/diskdb.rs`, `diskdb_type_util.rs`, and
   `app/crowdb-diskdb/src/recovery/`: add the slot watermark to the zone
   record and its integrity check. Decode existing records conservatively,
   rebuild or replay as required, and make journal/full-scan recovery use
   Paxos slots rather than reconstructing a timestamp source.
6. **Free-key and owner fencing invariants** — DiskDB allocation/free,
   group-0 synchronization, and compaction entry points: prove and enforce
   that a `FreeBlockKey` is immutable until compaction deletes it, that a
   later operation cannot overwrite the same key while it is eligible for a
   bounded scan, and that only the immutable disk-group owner may compact.
   This requirement does not implement owner change or data migration.
7. **Feature-grouped correctness coverage** — KV engine/client tests,
   `app/crowdb-diskdb/tests/recovery_test.rs`, and the compaction feature
   suite under `lib/crowdb-diskdb-client/tests/`: cover slot visibility,
   pagination, failures, delayed apply, restart, ownership rejection, and
   exact capacity accounting through the public DiskDB client.

### Flow diagram

```text
linearizable scan barrier
          │
          ▼
capture S = contiguous_applied
          │
          ▼
scan every page with fixed upper cutoff S
          │
          ├── incomplete / cutoff changed ──► abort, keep compact_slot
          │
          ▼
merge immutable free records in (compact_slot, S]
          │
          ▼
atomic KV batch: ZoneValue(compact_slot=S) + exact-key deletes
          │
          ▼
later revisions remain for the next compaction
```

### Edge cases at a glance

- Slot N+1 applies while slot N is missing → neither record can move the
  compaction watermark past N-1.
- The missing slot closes during pagination → all pages retain the original
  cutoff; the newly reachable range waits for the next scan.
- A current key version has revision above the cutoff → omit it; the API does
  not reconstruct an overwritten historical value.
- A free arrives above the cutoff while compaction runs → retain it for the
  next pass and do not include its key in the delete batch.
- Timeout, decode error, leader change, or incomplete pagination → abort
  without modifying the bitmap, deleting keys, or advancing `compact_slot`.
- Empty bounded scan → advance to the fixed cutoff only after the complete
  prefix scan succeeds and the immutable-key invariant proves no qualifying
  record can appear later at or below that cutoff.
- Legacy zone value lacks `compact_slot` → use a conservative zero watermark
  and recovery validation; never translate wall-clock `compact_ts` into a
  Paxos slot.
- A non-owner starts compaction → reject before scanning or deleting data.
- Reuse would generate the same free key before its prior record is deleted →
  reject the state transition or use a generation-distinct key; never
  overwrite a pending free record.

## Dependencies

- R130's feature review and mix benchmark expose the failure and provide the
  DiskDB client E2E structure used here. R132 owns the non-trivial fix rather
  than extending R130's conformance cleanup.
- Uses the existing engine per-key resolved slot, Paxos
  `contiguous_applied` frontier, linearizable apply fence, atomic KV batch,
  and immutable disk-group ownership established by R130.
- R101 conditional writes are not required if free keys are proven immutable
  and compaction is owner-fenced. If either invariant cannot be enforced,
  R132 must depend on R101 and delete each scanned key conditionally by its
  expected revision.
- R102 owner rebinding and record migration remain out of scope. R102 must
  preserve the compaction fencing and watermark contract when it later adds
  owner or KV-group transitions.

## Acceptance

### KV record revisions

- Put key K at slot N → get K and scan its prefix → assert both return value K
  with revision N and the scan does not allocate a second value buffer.
  Integration test.
- Apply slots N+1 then N to different keys → read both after the gap closes →
  assert each record reports its own Paxos slot rather than physical apply or
  response order. Unit test.
- Put, overwrite, and delete one key through increasing slots → query after
  each applied operation → assert the live version carries the highest
  resolved slot and a deleted key is absent. Unit test.

### Contiguous-applied bounded scan

- Delay slot N and apply a matching-prefix record at N+1 → start bounded scan
  before repairing N → assert its cutoff does not exceed N-1 and the N+1
  record is absent; repair N and rescan → assert the cutoff covers N+1 and
  both eligible records appear with revisions. Integration test.
- Capture cutoff S with enough records for three pages, then apply records
  above S between pages → finish pagination → assert every response retains S,
  every returned revision is at most S, no qualifying immutable record is
  missing or duplicated, and later records are absent. Integration test.
- Change leader or return a different cutoff between pages → continue the
  client scan → assert it fails as incomplete and does not return a result
  eligible for watermark advancement. Integration test.
- Force a timeout or malformed item on the final page → scan → assert the
  partial items are diagnosed but cannot be consumed as a complete bounded
  result. Integration test.
- Overwrite a key above cutoff S after its earlier version was visible → scan
  at S → assert the bounded current-version contract omits that key rather
  than claiming to return its historical value. Unit test.

### DiskDB compaction ordering

- Start free A, delay its lower Paxos slot, commit and apply free B at the next
  slot, then trigger compaction → assert `compact_slot` cannot advance past
  the gap and neither free is lost; close the gap and compact again → assert
  both ranges are clear and both records are deleted. Integration test.
- Capture compaction cutoff S, commit another free above S while paginating,
  then persist compaction → assert only records at or below S are merged and
  deleted, the later free remains durable, and the next compaction reclaims
  it. Integration test.
- Fail any scan page before the atomic batch → inspect bitmap, zone value, and
  free records → assert all remain at their pre-compaction durable state and
  retry reprocesses the same records. Integration test.
- Fail the final atomic batch before or after Paxos chooses it, then restart →
  assert recovery observes either the old bitmap plus all free records or the
  new bitmap plus their deletion, never a mixed state. Integration test.
- Load a legacy `ZoneValue` containing only `compact_ts`, with free records on
  both sides of that timestamp → recover and compact → assert all durable free
  ranges are reclaimed exactly once and the persisted replacement carries a
  valid `compact_slot` and checksum. Integration test.

### Ownership, reuse, and end-to-end accounting

- Present the same disk group to its assigned owner and a non-owner instance →
  trigger compaction on both → assert only the assigned owner can scan and
  commit the compaction batch. E2E test.
- Attempt to create a second pending free record with the same key before the
  first is compacted → assert the operation cannot overwrite the first
  record, and both bitmap and capacity accounting remain conservative.
  Integration test.
- Through `DiskdbClient`, allocate to confirmed `NoSpace`, free a deterministic
  multi-unit subset under concurrent compaction, compact, and reallocate
  exactly the freed units → assert no interval overlaps, no additional
  allocation succeeds, and busy space agrees per disk and disk group. E2E
  test.
- Run the deterministic 70/30 mix workload with forced out-of-order KV apply,
  compact until no eligible free records remain, and restart DiskDB → assert
  acknowledged allocations minus frees equals the durable live set and exact
  busy units before and after restart. E2E test.

### Test commands

- `pixi run test-protocol`
- `pixi run test-kv-core`
- `pixi run test-kv-client`
- `pixi run test-diskdb`
- `pixi run test-diskdb-client`
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`
