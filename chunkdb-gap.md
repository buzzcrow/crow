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

### GAP-2: EC backend — isa-l (design) vs pure-Rust reed-solomon-erasure

- **Design doc §3.5/§10** specifies isa-l via FFI for AVX2/AVX512 performance.
- **isa-l is not installed** on this system (`libisal-dev` available via apt but no sudo access).
- isa-l FFI would require `unsafe` in `crow-common`, conflicting with the workspace `unsafe_code = deny` (only `crow-tree-ffi` is excepted).
- **Decision taken**: used the pure-Rust `reed-solomon-erasure` crate (v6.0.0, GF(2^8)) as the EC backend. The public API (`EcScheme`, `encode`, `decode`) is backend-agnostic — isa-l can be swapped in later behind the same API when it's available and the `unsafe` exception is granted.
- **Action needed**: decide whether to (a) install isa-l + grant `crow-common` an `unsafe` exception, or (b) keep the pure-Rust backend and update the design doc.

### GAP-3: ChunkType enum — not in existing proto

- The existing `chunkdb_type.proto` has no `ChunkType` enum. R85 adds it per design §5.5.
- Added `ChunkType` enum (Repo=0, WAL=1, BTreePage=2, PageIndex=3, reserved 4-255) and `ChunkType chunk_type` field to `Chunk`.
- Also added `CHUNK_STATE_INIT = 0` per design §9, renumbering `ChunkState` values (ACTIVE=1, SEALED=2, DELETED=3). No existing code uses these enum values yet.
