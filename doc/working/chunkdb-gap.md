<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# chunkdb Implementation Gaps

Gaps encountered during R85-R91 + R99 implementation that need user feedback.

---

## R85: chunkdb foundation

### GAP-1: ChunkId width — 128-bit (design) vs 192-bit (proto)

- **Design doc §5.4** specifies a 128-bit ChunkId (8 type + 48 timestamp + 72 random).
- **Existing proto** `common_type.proto` defines a 192-bit ChunkId (3 × uint64: high, mid, low).
- The 192-bit proto is already used by diskdb's `BusyBlockValue.owner_chunk` field.
- **Decision taken**: kept the 192-bit proto as-is (changing it would break diskdb). The chunk ID generator packs type bits into the high byte and uses timestamp + random across all 192 bits. The design doc §5.4 should be updated to match the 192-bit proto.
- **Action needed**: update `doc/design/chunkdb/design-crow-chunkdb.md` §5.4 to reflect 192-bit ChunkId.


ai-todo : use 128 bit and follow the design, write UUID util class in crow-protocol, change proto


### GAP-2: EC backend — isa-l (design) vs pure-Rust reed-solomon-erasure

- **Design doc §3.5/§10** specifies isa-l via FFI for AVX2/AVX512 performance.
- **isa-l is not installed** on this system (`libisal-dev` available via apt but no sudo access).
- isa-l FFI would require `unsafe` in `crow-common`, conflicting with the workspace `unsafe_code = deny` (only `crow-tree-ffi` is excepted).
- **Decision taken**: used the pure-Rust `reed-solomon-erasure` crate (v6.0.0, GF(2^8)) as the EC backend. The public API (`EcScheme`, `encode`, `decode`) is backend-agnostic — isa-l can be swapped in later behind the same API when it's available and the `unsafe` exception is granted.
- **Action needed**: decide whether to (a) install isa-l + grant `crow-common` an `unsafe` exception, or (b) keep the pure-Rust backend and update the design doc.


ai-todo: use isa-l, we have depends on it in pixi, wrap EC functionality in crow-common and write UT test it. 

### GAP-3: ChunkType enum — not in existing proto

- The existing `chunkdb_type.proto` has no `ChunkType` enum. R85 adds it per design §5.5.
- Added `ChunkType` enum (Repo=0, WAL=1, BTreePage=2, PageIndex=3, reserved 4-255) and `ChunkType chunk_type` field to `Chunk`.
- Also added `CHUNK_STATE_INIT = 0` per design §9, renumbering `ChunkState` values (ACTIVE=1, SEALED=2, DELETED=3). No existing code uses these enum values yet.

ai-todo : add it.
---

## R87: placement and allocation

### GAP-4: DiskdbClientPool endpoint routing — v1 uses broadcast free

- The `DiskdbClientPool.free_blocks` implementation in v1 tries freeing via all known channels (broadcast), rather than precisely routing each segment to its owning diskdb instance via `disk_id → disk_group_id` mapping.
- **Reason**: the `Segment` proto has `disk_id` but not `disk_group_id`; mapping `disk_id → disk_group_id` requires a reverse lookup that is not yet available in the topology cache.
- **Impact**: in a multi-diskdb-instance cluster, free calls may be rejected by non-owning instances (logged as warnings). The owning instance accepts the free. This is functionally correct but generates noise.
- **Action needed**: add `disk_id → disk_group_id` reverse mapping to the topology cache (R86) or the diskdb client pool, then route free calls precisely.


ai-todo:  no, you can broadcase.  R99 fix it.

### GAP-5: ChunkAllocator does not verify segment count per diskdb response

- The `allocate_blocks_parallel` method checks the total segment count across all responses, but does not verify that each individual diskdb response returned the requested number of segments.
- **Reason**: the diskdb `AllocateBlocks` RPC may return fewer segments than requested if the disk-group is near-full. The current code treats total count mismatch as a failure (rollback), which is correct, but a partial response from one diskdb instance could be masked by a full response from another.
- **Impact**: low — the total count check catches the mismatch. A more precise per-instance check would provide better error messages.
- **Action needed**: add per-instance segment count verification in `allocate_blocks_parallel`.


ai-todo: fix it. the handle the partial allocate. The review all allocation result and continue allocate lack blocks. Try some time and failed to allocate if can not get left blocks.
If fail, we need free all allocated blocks.

It bring another design change: when diskdb first allocate the block, it should mark the disk block as allocate. After chunk is persistent to kvGroup, it will send a commit message to diskdb to mark the disk block as allocated. Please design and fix it.

---

## R88: storage and routing

### GAP-6: put_chunk_if_absent is not atomic (check-then-write)

- The KV client API does not expose a CAS/put-if-absent operation. `put_chunk_if_absent` is implemented as a `get` + `put` sequence (check-then-write).
- **Impact**: in theory, two concurrent allocations with the same chunk ID could both pass the `get` check and both `put`, with the second overwriting the first. In practice, chunk ID collisions are vanishingly rare (88 random bits), so this is not a real concern.
- **Action needed**: if strict atomicity is required, add a CAS/put-if-absent RPC to the KV service and use it here. Otherwise, document the check-then-write semantics as sufficient for v1.

ai-todo: we do not need put if absent , just PUT to override.  chunk has one owner chunkdb instance at a time. There is no race condition across different chunkdb instance. Change the function name. 

### GAP-7: Binding table loaded from group-0 not implemented

- The `BindingCache` is populated with a `default_binding_table(0, 0)` in `main.rs` — all buckets route to store 0, group 0.
- The full implementation should fetch the binding table from group-0 via `KVClusterMetaClient` on startup, and watch/notify for updates.
- **Reason**: the binding table schema in group-0 is not yet defined in the proto. The `KVClusterMetaClient` API does not have binding-table-specific methods.
- **Impact**: chunkdb works for single-KV-group deployments. Multi-group routing + migration requires the binding table to be loaded from group-0.
- **Action needed**: define the binding table proto schema, add `KVClusterMetaClient` methods for reading/writing binding table entries, and wire the watch/notify in `main.rs`.

ai-todo: R99 should define and impl it. Review current status and check gaps.
---

## R89: lifecycle management

### GAP-8: No CAS on state transitions (concurrent seal/delete)

- The lifecycle handler reads the chunk, validates the state transition, and writes it back. There is no compare-and-swap on the `state` field — two concurrent transitions could both read `Active`, both validate, and both write (last-writer-wins).
- **Reason**: the KV client API does not expose CAS. The design §9 specifies "KV CAS on state" but the KV service does not have a CAS RPC.
- **Impact**: in a concurrent seal+delete scenario, both could succeed (one overwrites the other). The design specifies one should win and the other get `StateConflict`.
- **Action needed**: add a CAS RPC to the KV service (compare revision or compare state), or use a distributed lock. For v1, the low concurrency of lifecycle operations makes this acceptable.

ai-todo : design a lock mechanism for chunkdb lifecycle operations. the lock should be scoped to the chunk id. The lock can be wait (avoid blocking the thread), and should have a way to wake up on timeout or when the lock is released. We need a high performance and low cost design for it.  Create sperate requirment if needed.

### GAP-9: DeleteChunk is idempotent (design decision)

- Per R89 Open Questions, `DeleteChunk` on an already-deleted chunk returns the existing `Deleted` chunk (idempotent), matching aioss.
- **Decision taken**: implemented as idempotent — if the chunk is already `Deleted`, return it without error.
- **Action needed**: none — this is a conscious design decision.

ai-todo: return not-exist. The error handling can treat it success. We should avoid return true/false for API / rpc call, need use return code/ error code show real status.
---

## R91: E2E tests

### GAP-10: Full-stack E2E tests require crow-kv-server binary

- The E2E tests in `e2e_test.rs` verify component-level integration (topology, selector, routing, state machine, handler construction, service wiring) with mock components. Full-stack E2E tests that start a real KV cluster + diskdb + chunkdb in-process (following the diskdb E2E pattern) are not yet implemented.
- **Reason**: the full-stack harness requires the `crow-kv-server` binary to be built and the `KvCluster` test helper to be adapted for chunkdb. This is a significant integration effort.
- **Impact**: component-level integration is tested, but cross-component integration (e.g. topology cache feeding stale data to placement, routing sending writes to the wrong KV group during migration) is not verified.
- **Action needed**: implement the full-stack E2E harness (`ChunkdbCluster` helper) following the diskdb pattern, with real KV + diskdb + chunkdb in-process.

ai-todo: then use it. diskdb already use it.