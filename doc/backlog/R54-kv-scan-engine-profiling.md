<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R54: Scan Engine Profiling — Identify the 32T Saturation Bottleneck

**Problem**: the scan bench shows both read modes (Linearizable and
MinSlot) saturate near ~38k scans/s at 32T:32C, regardless of the read
barrier — the read-mode split (R26) moved the bottleneck from the read
barrier to the storage engine, but the specific hot spot inside the
engine is unknown. The scan path is:

`KV RPC` → `PxKvStore` → `KVEngine::scan` → `CrowdbTreeEngine::scan`
(`crowdb_tree_engine.rs`) → `try_scan` (FFI) → **C++ crowdb-tree merge
loop**.

The merge loop walks L0 (the `ConcurrentSkipList` from R50,
epoch-protected, lock-free) and L1 (the B+tree `LeafChainCursor` from
R48, lazy leaf traversal) in key order, filters tombstones, and packs
results into the FFI buffer. Candidate hot spots (unverified):
- L0 skip-list traversal (pointer-chasing through the tower).
- L1 B+tree leaf descent / page faults on cold scans.
- The merge comparison loop (per-key min/max selection across L0+L1).
- Tombstone filtering overhead.
- Packed-result buffer allocation / FFI boundary cost.

Without a flamegraph, optimizing any one of these is guessing.

**Solution**: profile the scan path and document the findings.

- **Tooling**: add `tools/profile-scan.sh`, mirroring the existing
  `tools/profile-write.sh` pattern (samply + perf + flamegraph). Same
  build flags (debug symbols + frame pointers for Rust + C++ via cc
  crate), same sampler options, but driving a scan benchmark instead of
  a write benchmark. The scan bench config should target the saturating
  regime: 32T:32C with `valuesize_256B` (the config where both modes
  hit ~38k scans/s and the engine is the confirmed bottleneck).
- **Profiling targets**:
  - The C++ merge loop (`crowdb-tree` scan path) — the primary suspect.
  - The FFI boundary (`try_scan` → C++ → packed result → Rust decode).
  - The Rust scan-response serialization (`kv_service.rs::scan` → fbs
    encode → crowdb-rpc send), to confirm the engine is truly dominant over
    the response path.
- **Deliverable**: a working doc (`doc/working/scan-profile-findings.md`)
  with the top hot stacks, a ranked list of bottlenecks by CPU time,
  and a recommendation for the first optimization target. If a clear
  optimization emerges (e.g. "skip-list traversal is 40% of scan CPU,
  a cache-aligned node layout would halve it"), file a follow-up
  requirement with the profiling evidence as justification.

**Scope**:
- `tools/profile-scan.sh` — new script, adapted from
  `tools/profile-write.sh`.
- `doc/working/scan-profile-findings.md` — profiling results + analysis
  (working doc, deleted after findings are acted on or merged into a
  design doc).
- No code changes to the scan path itself — this is investigation only.

**Complexity**: Low. The profiling infrastructure already exists for
the write path; this extends it to scan. The analysis is the bulk of
the work, not the tooling.

**Dependencies**: none. The scan path (R48 lazy cursor, R50 skip-list
L0) is stable; profiling it now reflects the production code shape.

**Acceptance**:
- `tools/profile-scan.sh` produces a flamegraph (perf/inferno) or
  samply profile for the 32T:32C scan bench.
- The findings doc identifies the top CPU-consuming stack(s) in the
  scan path with percentage breakdowns.
- The findings doc states whether the bottleneck is in the C++ engine
  (merge loop / cursor / page load), the FFI boundary, or the Rust
  response path — with evidence, not speculation.
- If an optimization target is identified, a follow-up requirement is
  filed with the profiling evidence referenced.

**Note**: the saturation observation lives in
`doc/design/kv/kv-scan-flow-analysis.md` benchmark and flow analysis.
