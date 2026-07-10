# CrowKV - Test Design: Storage Engine

Depends on: [`test-design.md`](test/test-design.md), [`design-storage-engine.md`](design-storage-engine.md)
Satisfies: [requirement.md §8.3](requirement.md#83-learner-storage), [requirement.md §8.4](requirement.md#84-snapshot-and-install)

Invariants for the engine trait and backends.

## 1. Invariants

| ID | Claim | Trigger | Ref |
|---|---|---|---|
| S1 | Apply atomic | Every `apply(slot, batch)` | [`design-storage-engine.md`](design-storage-engine.md) §4.3 |
| S2 | Per-key resolved-slot monotone | Every `apply` | [`design-storage-engine.md`](design-storage-engine.md) §3.3 |
| S3 | Compare logical not byte-level | `compare(other)` | [`design-storage-engine.md`](design-storage-engine.md) §8 |
| S4 | Snapshot round-trip equal | `export` then `import` | [`design-storage-engine.md`](design-storage-engine.md) §6 |

## 2. Unit Tests

| Module | Test | Assertion |
|---|---|---|
| `in_memory` | `apply_get` | Written key retrievable with correct slot |
| `in_memory` | `apply_idempotent` | Re-apply same slot: no state change |
| `in_memory` | `scan_no_tombstones` | Deleted key absent from scan |
| `ordered_file` | `compare_with_in_memory` | After identical ops, diff is empty |
| `snapshot` | `export_import_roundtrip` | Imported engine `compare()` equal to original |

## 3. Failure Injection

| Failure | Sim | Invariant | Assertion |
|---|---|---|---|
| Apply mid-batch error | `TestEngine::fail_on_apply()` | S1 | Batch left fully unapplied (no partial state) |
| Snapshot stream truncated | `TestStream::truncate_at(offset)` | S4 | Import detects mismatch via CRC; engine state unchanged |
| Snapshot stream corrupted | `TestStream::corrupt_at(offset)` | S4 | Import rejects; engine state unchanged |

## 4. Integration Scenarios

**S-S1 — Cross-engine equivalence:**
1. Replay identical 1000-op sequence into in-memory and ordered-file engines.
2. `compare()` returns empty diff.
3. Both pass `iter_all()` with identical key/slot/value sequences.

**S-S2 — Snapshot resume:**
1. Begin export of 100 MiB engine state.
2. Interrupt at 60 MiB.
3. Resume from offset 60 MiB; complete import.
4. Imported engine `compare()` equal to original.

## 5. Backends to Validate

- In-memory (P1)
- Ordered-file (P3)
- crowtree placeholder (P3, `#[ignore]`)

## 6. Resolved Decisions

- **`compare()` complexity:** `O(n)` full iteration only for first cut; Merkle-tree digest path deferred until profiling shows it is needed.
- **Tombstone retention:** `tombstone_grace_slots = 0` (immediately reclaimable once both snapshot-slot and safe-slot watermarks pass) per [`design-storage-engine.md`](design-storage-engine.md) §10.
