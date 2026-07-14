<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan: crowtree Persistent Storage Refactor

Parent: [`design-crowtree-storage.md`](design/design-crowtree-storage.md)

## Goal

Refactor crowtree's `PageStore` backend layer to:

1. **Unify memory into block device** — `MemPageStore` becomes a special case of `BlockPageStore` with IU=1 (byte-aligned). SCM/PMEM SSD also uses byte-level IU. Only NVMe SSD needs 4K/8K/16K alignment handling.
2. **Array-of-blocks growth** — a group owns an array of fixed-size block files, not one pre-allocated device. When the current block fills up, `allocate_new_block` creates the next file. Test sizes: 8–16 MiB; production: 64–128 MiB.
3. **Text-encoded file backend for debugging** — `TextPageStore` writes each B-tree page and each mapping-table segment as a separate human-readable `.ck` text file. Anchor A/B also in text. This is the debug/test backend, mirroring WAL's `TextLine` record format. `FilePageStore` is removed.

## Design Reference

The WAL storage layer (`crowkv/src/wal/`) already implements the same patterns:

- **`WalBlockAlignment`** (`Unaligned` for RAM/SCM/PMEM, `Aligned { io_unit_bytes }` for SSD) — `crowkv/src/common/config.rs:74-82`
- **`BlockDevice`** simulation with unaligned vs aligned write paths, RMW for sub-block writes — `crowkv/src/wal/block_backend.rs:87-115`
- **Segment rotation** — `create_segment()` allocates next segment file when current is full — `crowkv/src/wal/pipeline_writer.rs:449-458`
- **Text-line encoding** — `WALRecord::encode_text_line()` / `decode_text_line()` with `CROW_WAL_TEXT` prefix, CRC32C, human-readable fields — `crowkv/src/wal/record.rs:313-402`
- **Segment header in text** — `SEG_TEXT_PREFIX` with `key=value` fields — `crowkv/src/wal/segment.rs:92-99`

crowtree already has a **debug codec** (`crowtree/include/crowtree/debug_codec.h`) that renders page frames to annotated text and round-trips exactly. `DebugPageStore` (`crowtree/include/crowtree/page_store.h:93-134`) wraps an inner store with IU=1. We build on this foundation.

## Current State

| Component | File | Status |
| --- | --- | --- |
| `PageStore` interface | `crowtree/include/crowtree/page_store.h:36-56` | **Make async-only** — remove sync methods, upper layer always uses async API |
| `MemPageStore` | `crowtree/src/page_store.cpp:19-45` | **Remove** — subsumed by `BlockPageStore(mem)` |
| `FilePageStore` | `crowtree/src/page_store.cpp:47-119` | **Remove** — `TextPageStore` replaces debug role; `BlockPageStore` replaces production role |
| `FileAsyncPageStore` | `crowtree/include/crowtree/async_page_store.h:60`, `crowtree/src/file_async_page_store.cpp` | **Remove** — superseded by `BlockPageStore` + I/O engine (DirectIo or IoUring) |
| `BlockPageStore` | `crowtree/src/block_page_store.cpp` | **Extend** — Medium abstraction + array-of-blocks + IU=1 + pluggable I/O engine (DirectIo/IoUring) |
| `DebugPageStore` | `crowtree/include/crowtree/page_store.h:93-134` | Keep (pass-through wrapper) |
| `debug_codec` | `crowtree/include/crowtree/debug_codec.h` | Reuse for text page encoding |
| `PageCodec` (binary) | `crowtree/src/page_codec.cpp` | Keep for block backend |
| `persist.cpp` (snapshot) | `crowtree/src/persist.cpp` | Adapt to array-of-blocks addressing |
| `c_api.cpp` (`ct_open`) | `crowtree/src/c_api.cpp:194-277` | Update backend selection + I/O engine selection + `sync_mode` |
| `ct_options` | `crowtree/include/crowtree/c_api.h:74-92` | Update `backend` field + add `sync_mode`, `block_size`, `store_id`, `partition_id` |

---

## Tasks

### Task 1: IoEngine interface + DirectIoEngine (async foundation)

**Goal**: Define the async `IoEngine` interface and implement `DirectIoEngine` (blocking I/O wrapped as immediately-ready async completions). All subsequent tasks use this async API from the start. No deletion of old code — cleanup is in Task 11.

**Design**:
- `IoEngine` interface (new, `crowtree/include/crowtree/io_engine.h`):
  ```
  class IoEngine {
  public:
      virtual ~IoEngine() = default;
      virtual void submit_read(int fd, void* buf, size_t len, off_t offset,
                               std::function<void(Status)> cb) = 0;
      virtual void submit_write(int fd, const void* buf, size_t len, off_t offset,
                                std::function<void(Status)> cb) = 0;
      virtual void submit_fsync(int fd, std::function<void(Status)> cb) = 0;
  };
  ```
- `DirectIoEngine` implements `IoEngine`: calls `pread`/`pwrite`/`fdatasync` synchronously, then invokes the callback immediately (ready completion). Uses `O_DIRECT` when `iu_size > 1`.
- `PageStore` interface: add async methods matching the existing `AsyncPageStore` signature (`submit_read`/`submit_write`/`submit_fsync`/`cancel` returning op id). Keep old sync methods temporarily — deletion in Task 11. `DirectIoEngine` is the fd-based backend that async methods delegate to.
- `IoEngine` is used only by `BlockPageStore` (file/block-device path). `TextPageStore` does synchronous per-file I/O wrapped as immediately-ready async completions (no `IoEngine`). `BlockPageStore::open_mem()` uses a separate in-memory medium path (see Task 2, `BlockPageStoreMedium`).

**Changes**:
- [ ] `crowtree/include/crowtree/io_engine.h` (new): `IoEngine` interface + `DirectIoEngine` declaration.
- [ ] `crowtree/src/io_engine.cpp` (new): `DirectIoEngine` implementation.
- [ ] `crowtree/include/crowtree/page_store.h`: Add async methods to `PageStore` (keep sync methods temporarily).
- [ ] `crowtree/CMakeLists.txt`: Add `io_engine.cpp`.

**Files**:
- `crowtree/include/crowtree/io_engine.h` (new)
- `crowtree/src/io_engine.cpp` (new)
- `crowtree/include/crowtree/page_store.h`
- `crowtree/CMakeLists.txt`

**Test**:
- [ ] `DirectIoEngine` read/write round-trip via async callback API.
- [ ] Callbacks are invoked immediately (ready completion) on macOS.

---

### Task 2: BlockPageStore — Medium abstraction + IU=1 (byte-aligned) mode

**Goal**: Introduce the `BlockPageStoreMedium` interface so `BlockPageStore` supports memory, file, block device, and future SCM media through a single code path. `MemoryMedium` with `iu_size=1` replaces `MemPageStore`. No alignment constraints, no O_DIRECT, no bounce path for IU=1.

**Design**:
- `BlockPageStoreMedium` interface (new, `crowtree/include/crowtree/block_page_store.h`):
  ```
  class BlockPageStoreMedium {
  public:
      virtual ~BlockPageStoreMedium() = default;
      virtual Status pwrite_at(uint64_t off, const uint8_t* buf, size_t len) = 0;
      virtual Status pread_at(uint64_t off, uint8_t* buf, size_t len) const = 0;
      virtual Status fsync() = 0;
      virtual uint64_t size() const = 0;
  };
  ```
- `MemoryMedium`: backs onto `std::vector<uint8_t>` + mutex. `fsync()` is no-op. This is the test/SCM path.
- `FileMedium`: wraps a single fd with `pwrite`/`pread`/`fdatasync`/`lseek`. Used by array-of-blocks (one `FileMedium` per block extent).
- `BlockPageStore` delegates all I/O to `BlockPageStoreMedium*`. When `iu_size == 1`, short-circuit to raw `pwrite`/`pread` (skip alignment check + bounce path).
- Future SCM support will add an `ScmMedium` variant — same interface, different backing store.

**Changes**:
- [ ] `crowtree/include/crowtree/block_page_store.h`: Add `BlockPageStoreMedium` interface + `MemoryMedium` declaration. `BlockPageStore` holds a `unique_ptr<BlockPageStoreMedium>` instead of `fd_`.
- [ ] `crowtree/src/block_page_store.cpp`: Refactor to use medium. `MemoryMedium` implementation. Short-circuit alignment when `iu_size_ == 1`.
- [ ] Add `BlockPageStore::open_mem(iu_size=1)` factory that creates a `MemoryMedium`-backed store.
- [ ] Update all test references from `MemPageStore` to `BlockPageStore::open_mem()` (actual class deletion in Task 11).

**Files**:
- `crowtree/include/crowtree/block_page_store.h`
- `crowtree/src/block_page_store.cpp`
- All tests referencing `MemPageStore` → `BlockPageStore::open_mem()` (deletion in Task 11)

**Test**:
- [ ] Existing `page_store_test.cpp` tests pass with `BlockPageStore::open_mem()` replacing `MemPageStore`.
- [ ] New test: `BlockPageStore` with IU=1 does no RMW (byte-exact writes, no amplification).
- [ ] **Content verification**: Write known data via `write_at`, then read back raw bytes from the underlying `MemoryMedium` buffer and assert they match exactly. Verify IU=1 means no padding bytes between writes.

---

### Task 3: BlockPageStore — Array-of-blocks growth

**Goal**: A group's storage is an array of fixed-size block files, not one file. When the current block is full, `allocate_new_block()` creates the next file. The `PageStore` interface stays the same; the array is managed internally.

**Design**:
- Block file naming: `{store_id}-{partition_id}.blk-{NNNN}` (e.g., `1-3.blk-0000`, `1-3.blk-0001`). Files are self-identifying — copied anywhere, the store and partition ownership is clear from the filename.
- `block_size` is configured at open time (default 64 MiB; tests use 8–16 MiB).
- Internal state:
  ```
  struct BlockExtent {
      std::unique_ptr<FileMedium> medium;  // one FileMedium per block file
      uint64_t base_offset;   // global offset = block_idx * block_size
      uint64_t used;          // high-water mark within this block
      bool dirty;
  };
  std::vector<BlockExtent> extents_;
  ```
- `write_at(global_off, buf, len)`: map `global_off` to `(extent_idx, local_off)`. If the write crosses an extent boundary, split. If `global_off + len > total_capacity`, call `allocate_new_block()` first.
- `read_at(global_off, buf, len)`: same mapping.
- `size()`: return the logical high-water mark = `extents_.back().base_offset + extents_.back().used` (maximum written offset across all extents). This is what `SpaceAllocator` needs for gap computation.
- `sync()`: `fdatasync`/`fsync` all dirty extents (track dirty flag per extent).
- `allocate_new_block()`: create `{store_id}-{partition_id}.blk-{NNNN}` with `O_RDWR | O_CREAT` (+ `O_DIRECT` if `iu_size > 1`), push to `extents_`.
- Recovery: on `open()`, scan the directory for `{store_id}-{partition_id}.blk-*` files, sort by index, open all. The anchor in block 0 tells us which blocks are live.

**Changes**:
- [ ] `crowtree/include/crowtree/block_page_store.h`: Add `block_size`, `extents_`, `allocate_new_block()`. New `open()` signature taking `block_size` parameter.
- [ ] `crowtree/src/block_page_store.cpp`: Implement array management, cross-extent write splitting, multi-extent sync.
- [ ] **Dump utility**: Add `dump_block_file(path, iu_size, out)` — an annotated hex dump that parses known structures at fixed offsets (anchor at offset 0 and `superblock_slot_bytes`, page blob envelopes `[plen u32][payload][crc32c u32]`) and renders them as human-readable text. Unknown regions are shown as hex. This is lighter than a full binary parser but sufficient for test assertions and debugging.

**Files**:
- `crowtree/include/crowtree/block_page_store.h`
- `crowtree/src/block_page_store.cpp`

**Test**:
- [ ] Write data exceeding one block size → verify second block file is created.
- [ ] Cross-extent write (write spanning block boundary) → verify data integrity.
- [ ] Reopen after all blocks written → verify all data readable.
- [ ] Block size = 8 MiB, write 20 MiB → expect 3 block files.
- [ ] **Content verification**: After a snapshot, use the dump utility to read `.blk-0000` and verify: anchor A/B fields (magic, snapshot_seq, root_page_id, next_page_id) match expected values; page blob headers (plen, CRC) are present and valid; gap regions are correctly marked. Assert specific byte offsets contain expected anchor magic bytes.

---

### Task 4: TextPageStore + text codecs — debug file backend

**Goal**: Text-encoded debug backend. Each durable object (page, anchor, segment image, segment directory) is a separate human-readable `.ck` file. Uses `debug_codec` for page frames and new text codecs (below) for other blob types.

**Text codec design**:
- `encode_anchor_text()` / `decode_anchor_text()`: Format mirrors WAL's text segment header:
  ```
  CROW_CT_ANCHOR magic=0x41435443 format_version=2 snapshot_seq=123 root_page_id=42
  last_applied_slot=99 next_page_id=100 segment_slots=1024 segdir_addr=4096
  segdir_len=2048 segdir_crc=deadbeef anchor_crc=cafebabe
  ```
- `encode_seg_image_text()` / `decode_seg_image_text()`: `CROW_CT_SEGIMG` header with `seg_idx`, `generation`, `slot_count`, `live_count`, followed by one line per slot word (`slot[N] = (iu_index, iu_count)` or `empty`).
- `encode_segdir_text()` / `decode_segdir_text()`: `CROW_CT_SEGDIR` header, followed by one line per `DirEntry`: `seg_idx=N generation=G image_addr=A image_len=L crc=C`.

**TextPageStore design**:
- `TextPageStore : public PageStore` with `iu_size() = 1`.
- Directory layout at `{path}/{store_id}-{partition_id}/`:
  ```
  {path}/{store_id}-{partition_id}/
    manifest.ck       # addr → (type, filename) mapping
    anchor-A.ck       # CommitAnchor A
    anchor-B.ck       # CommitAnchor B
    page-{addr}.ck    # B-tree page (debug_codec text)
    seg-{addr}.ck     # Segment image
    segdir.ck         # Segment directory
  ```
- `write_at(addr, buf, len)`: inspect `buf` magic bytes + `addr` to determine blob type and filename. For anchors, slot 0 (addr at offset 0) → `anchor-A.ck`, slot 1 (addr at `superblock_slot_bytes`) → `anchor-B.ck`. For other types, magic alone suffices. Select the appropriate text codec, encode to text, write to the corresponding `.ck` file. Record `(addr, len, type, filename)` in manifest.
- `read_at(addr, buf, len)`: read `manifest.ck` on open to reconstruct addr→file mapping. Read text file, decode back to binary blob, return.
- `size()`: scan manifest for max addr + len.
- `sync()`: flush `manifest.ck`, fsync all written files.
- Manifest accumulates entries from `write_at` calls and is flushed at `sync()` time.

**Limitation**: Compression is always `kNone` (text mode is for debugging). `debug_codec` operates on uncompressed frames only.

**Changes**:
- [ ] `crowtree/include/crowtree/text_page_store.h` (new): `TextPageStore` declaration.
- [ ] `crowtree/src/text_page_store.cpp` (new): Implementation with magic+address-based type detection, manifest management, synchronous file I/O wrapped as immediately-ready async completions (no `IoEngine`).
- [ ] `crowtree/include/crowtree/text_codec.h` (new): Declarations for `encode_anchor_text`/`decode_anchor_text`, `encode_seg_image_text`/`decode_seg_image_text`, `encode_segdir_text`/`decode_segdir_text`.
- [ ] `crowtree/src/text_codec.cpp` (new): Implementations.
- [ ] `crowtree/CMakeLists.txt`: Add `text_page_store.cpp` + `text_codec.cpp`.

**Files**:
- `crowtree/include/crowtree/text_page_store.h` (new)
- `crowtree/src/text_page_store.cpp` (new)
- `crowtree/include/crowtree/text_codec.h` (new)
- `crowtree/src/text_codec.cpp` (new)
- `crowtree/CMakeLists.txt`

**Test**:
- [ ] Write a page → verify `page-{addr}.ck` exists and is human-readable.
- [ ] Write anchor → verify `anchor-A.ck` / `anchor-B.ck` is text.
- [ ] Round-trip: write page, reopen, read page → exact bytes match.
- [ ] Manifest: write multiple blobs, sync, reopen → manifest correctly maps all addresses.
- [ ] **Content verification (text)**: After a snapshot, `read_file()` the `anchor-A.ck` file and assert it contains `CROW_CT_ANCHOR` prefix, correct `snapshot_seq`, `root_page_id`, `next_page_id` values. `read_file()` a `page-{addr}.ck` file and assert it contains `crowtree-frame-text` header, `type leaf` / `type inner` / `type overflow`, and the expected key/value entries. `read_file()` `segdir.ck` and assert it contains the expected `DirEntry` lines with correct `seg_idx`, `image_addr`, `image_len`.
- [ ] Anchor codec: encode → decode → binary match. Verify all fields + CRC round-trip.
- [ ] Segment image codec: encode → decode → binary match. Verify slot words round-trip.
- [ ] Segment directory codec: encode → decode → binary match. Verify DirEntry array round-trip.
- [ ] All text outputs are human-readable (no binary bytes).

---

### Task 5: WAL segment file extension change (`.log` → `.ck`)

**Goal**: Change WAL segment file extension from `.log` to `.ck` to match the CrowKV-wide file naming convention. This is a standalone, low-risk change.

**Changes**:
- [ ] `crowkv/src/wal/segment.rs`: Change `format!("seg-{segment_id:07}.log")` → `format!("seg-{segment_id:07}.ck")`.
- [ ] `crowkv/src/wal/segment.rs`: Update `parse_segment_filename` to strip `.ck` instead of `.log`.
- [ ] `crowkv/src/wal/replay.rs`: Update doc comment referencing `seg-*.log` → `seg-*.ck`.
- [ ] `crowkv/src/wal/gc.rs`: Update `format!("seg-{:07}.log", ...)` → `format!("seg-{:07}.ck", ...)`.
- [ ] `crowkv/tests/wal/`: Update all test references to `seg-*.log` → `seg-*.ck`.
- [ ] `doc/design/design-wal.md`: Update segment file naming references.

**Files**:
- `crowkv/src/wal/segment.rs`
- `crowkv/src/wal/replay.rs`
- `crowkv/src/wal/gc.rs`
- `crowkv/tests/wal/block_backend_tests.rs`
- `crowkv/tests/wal/wal_engine_tests.rs`
- `doc/design/design-wal.md`

**Test**:
- [ ] Existing WAL tests pass with `.ck` extension.
- [ ] Replay discovers `seg-*.ck` files correctly.
- [ ] GC unlinks `seg-*.ck` files correctly.

---

### Task 6: Update persist.cpp for array-of-blocks + async migration

**Goal**: `persist.cpp`'s `SpaceAllocator` and snapshot/recovery logic must work with the array-of-blocks `BlockPageStore`. The global address space is linear (block_idx * block_size + local_offset), so `SpaceAllocator` is unchanged. Recovery must scan all block files.

**Changes**:
- [ ] `persist.cpp` `read_best_anchor`: Read anchor from block 0 (offset 0 and offset `superblock_slot_bytes`). No change needed — `BlockPageStore::read_at` handles the mapping.
- [ ] `persist.cpp` `build_allocator`: `file_size` comes from `BlockPageStore::size()` which returns the logical high-water mark. `collect_live_extents_from_directory` reads via `read_at` which maps across extents. No change needed.
- [ ] `persist.cpp` snapshot write: Migrate from sync `write_at`/`sync` calls to async `submit_write`/`submit_fsync` + callback. `write_at` may trigger `allocate_new_block()` if writing past current capacity — transparent to `persist.cpp`.
- [ ] Recovery: `BlockPageStore::open()` scans for all `*.blk-*` files, opens them, and reconstructs `extents_`. The anchor in block 0 determines which blocks are live (via segment directory → segment images → page addresses).

**Files**: Minimal changes to `persist.cpp` — mainly ensuring `size()` and `write_at` work correctly with the new backend. Most logic is transparent.

**Test**:
- [ ] Integration test — snapshot with data spanning multiple blocks, reopen, recover, verify all data.
- [ ] **Content verification**: Use the dump utility on all `.blk-*` files after a multi-block snapshot. Verify anchor is in block 0, segment directory is readable, page blobs span across block boundaries correctly. Assert that the dump output shows expected page count, segment count, and live extent addresses.

---

### Task 7: Update c_api.cpp and ct_options

**Goal**: Update the C API to reflect the new backend model.

**Changes**:
- [ ] `ct_options` — add new fields with enum types and defaults:
  ```c
  enum ct_backend {
      CT_BACKEND_TEXT  = 0,  // TextPageStore (debug, text files)
      CT_BACKEND_BLOCK = 1,  // BlockPageStore (production, array-of-blocks)
  };

  enum ct_sync_mode {
      CT_SYNC_FULL  = 0,  // fdatasync after every flush (default)
      CT_SYNC_SKIP  = 1,  // no fsync (tests/CI)
      CT_SYNC_BATCH = 2,  // fsync once per snapshot commit
  };

  // New fields in ct_options:
  enum ct_backend  backend;       // default CT_BACKEND_BLOCK
  uint64_t         block_size;    // 0 => default 64 MiB; ignored for text
  uint32_t         store_id;      // default 0; block file naming
  uint32_t         partition_id;  // default 0; maps to PxGroupId in CrowKV
  enum ct_sync_mode sync_mode;    // default CT_SYNC_FULL
  ```
  No struct packing directives needed — `ct_options` is not on a hot path (used once at `ct_open`). Plain C struct with natural padding is fine. FFI mapping in `crowtree/ffi` uses `#[repr(C)]` which matches.
- [ ] `ct_open`:
  - `CT_BACKEND_TEXT` + `path` non-empty → `TextPageStore` (debug, text files)
  - `CT_BACKEND_BLOCK` + `path` non-empty → `BlockPageStore` (array-of-blocks, O_DIRECT if `iu_size > 1`)
  - `path` null/empty → `BlockPageStore::open_mem(iu_size=1)` (in-memory, byte-aligned; backend ignored)
  - Engine selection is automatic: `DirectIoEngine` on macOS/fallback, `IoUringEngine` on Linux when `liburing` available. Not user-selectable.
  - Remove `FilePageStore` path entirely.

**Files**:
- `crowtree/include/crowtree/c_api.h`
- `crowtree/src/c_api.cpp`

**Test**:
- [ ] `ct_open` with each backend mode, basic put/get/snapshot/reopen cycle.
- [ ] **Content verification**: After `ct_open` + put + snapshot with `CT_BACKEND_TEXT` (text), `read_file()` the output directory and verify `anchor-A.ck` contains expected `snapshot_seq` and `root_page_id`. After `CT_BACKEND_BLOCK` (block), use the dump utility to verify `.blk-0000` anchor fields match.

---

### Task 8: Update crowkv FFI bindings

**Goal**: The Rust FFI layer (`crowtree/ffi`) must pass the new `ct_options` fields and updated backend semantics.

**Changes**:
- [ ] `crowtree/ffi/src/lib.rs`: Update `sys::ct_options` struct mapping, add `backend`, `block_size: u64`, `store_id: u32`, `partition_id: u32`, `sync_mode` fields.
- [ ] `crowtree/ffi/src/lib.rs`: Update `PageStoreBackend` enum — rename `File` variant to `Text` (debug text files), keep `Block` variant. Update `Options` struct to include all new fields.
- [ ] `crowtree/ffi/src/lib.rs`: Update `Crowtree::open()` to pass all new fields into `sys::ct_options`.
- [ ] `crowkv/src/kv/crowtree_engine.rs`: Map `PxGroupId` to `partition_id` when constructing `CrowtreeOptions`.
- [ ] `crowkv/src/kv/crowtree_engine.rs`: Re-export updated `PageStoreBackend` (now `Text` + `Block`).
- [ ] `crowkv` callers that construct `CrowtreeOptions`: pass `block_size` (default 64 MiB for production, 8 MiB for tests). Currently all callers use `..Default::default()` which defaults to `PageStoreBackend::File` → must be updated to `Text`.

**Files**:
- `crowtree/ffi/src/lib.rs`
- `crowkv/src/kv/crowtree_engine.rs`
- `crowkv/src/kv/mod.rs` (re-exports)
- All `CrowtreeOptions { ... }` construction sites in `crowkv` tests and source

**Test**:
- [ ] Existing crowkv integration tests pass with the new backend.
- [ ] `crowtree/ffi` tests (if any) pass with updated `ct_options` struct.

---

### Task 9: Configurable fsync policy (crowtree + WAL)

**Goal**: `fsync`/`fdatasync` is configurable via `ct_options.sync_mode`. On macOS, `fsync` costs ~3ms per call — `CT_SYNC_SKIP` or `CT_SYNC_BATCH` eliminates this for tests/CI. Same policy applies to WAL file sync.

**Design**:
- `ct_options.sync_mode` uses `enum ct_sync_mode` (defined in Task 7): `CT_SYNC_FULL` (default), `CT_SYNC_SKIP`, `CT_SYNC_BATCH`.
- `CT_SYNC_FULL`: `fdatasync` after every flush (current behavior, production default).
- `CT_SYNC_SKIP`: No fsync at all — OS page cache handles durability. For tests only.
- `CT_SYNC_BATCH`: fsync once per snapshot commit, not per flush. For throughput-sensitive testing.
- `IoEngine::submit_fsync` becomes a no-op when `sync_mode = CT_SYNC_SKIP`.
- `BlockPageStore::sync()` respects `sync_mode` — `CT_SYNC_BATCH` defers to snapshot commit.
- WAL: `WalEngine` / `WalPipeline` gets the same `sync_mode` option. WAL segment fsync after seal respects the policy.

**Changes**:
- [ ] `crowtree/src/c_api.cpp`: Pass `sync_mode` to `BlockPageStore` / `IoEngine` (field added to `ct_options` in Task 7).
- [ ] `crowtree/src/io_engine.cpp`: `DirectIoEngine` respects `sync_mode` in `submit_fsync`.
- [ ] `crowtree/src/block_page_store.cpp`: `sync()` respects `sync_mode`.
- [ ] `crowtree/src/persist.cpp`: Snapshot commit path respects `CT_SYNC_BATCH` (single fsync at commit).
- [ ] `crowtree/ffi/src/lib.rs`: Add `sync_mode` to `ct_options` mapping and `Options` struct.
- [ ] `crowkv/src/wal/`: Add `sync_mode` to WAL engine options. WAL segment fsync respects policy.
- [ ] `crowkv/src/common/config.rs`: Add `SyncMode` enum.

**Files**:
- `crowtree/src/c_api.cpp`
- `crowtree/src/io_engine.cpp`
- `crowtree/src/block_page_store.cpp`
- `crowtree/src/persist.cpp`
- `crowtree/ffi/src/lib.rs`
- `crowkv/src/common/config.rs`
- `crowkv/src/wal/wal_engine.rs`
- `crowkv/src/wal/pipeline.rs`

**Test**:
- [ ] `CT_SYNC_SKIP`: snapshot + reopen → data present (OS page cache). Kill -9 → data may be lost (expected).
- [ ] `CT_SYNC_BATCH`: snapshot + reopen → data present. Only one fsync per snapshot.
- [ ] `CT_SYNC_FULL`: snapshot + reopen → data present. fsync called per flush.
- [ ] WAL: same three modes, verify segment durability behavior.
- [ ] Benchmark: `CT_SYNC_SKIP` vs `CT_SYNC_FULL` on macOS → verify ~3ms/call savings.

---

### Task 10: Update tests

**Goal**: All existing tests pass with the refactored backends.

**Changes**:
- [ ] Replace all `MemPageStore` references with `BlockPageStore::open_mem()`.
- [ ] Replace all `FilePageStore` references with `TextPageStore` or `BlockPageStore::open_mem()` depending on test purpose.
- [ ] `page_store_test.cpp`: Update backend construction.
- [ ] `persist_test.cpp`: Update to use `BlockPageStore` with small `block_size` (e.g., 1 MiB) to test array-of-blocks.
- [ ] `alignment_test.cpp`: `DebugPageStore` wrapper still works over `BlockPageStore::open_mem()`.
- [ ] `debug_codec_test.cpp`: Unchanged (tests codec, not store).
- [ ] **New test file** `disk_format_verify_test.cpp`: Integration tests that verify on-disk file contents for each backend format:
  - **Text backend**: `read_file()` anchor (`anchor-A.ck`), page (`page-{addr}.ck`), segdir (`segdir.ck`) files → assert expected text fields and values.
  - **Block backend**: Use dump utility on `.blk-*` files → assert anchor magic, page blob headers, CRC validity, gap markers.
  - **In-memory backend**: Inspect raw buffer → assert byte-exact contents with no padding.
  - Each test writes a known set of keys, snapshots, then inspects the durable artifacts.

**Files**:
- `crowtree/tests/unit/page_store_test.cpp`
- `crowtree/tests/integration/persist_test.cpp`
- `crowtree/tests/integration/alignment_test.cpp`
- `crowtree/tests/integration/disk_format_verify_test.cpp` (new)
- Any other test files referencing `MemPageStore` or `FilePageStore`

---

### Task 11: Delete old code + cleanup sync API

**Goal**: Remove `FilePageStore`, `FileAsyncPageStore`, `MemPageStore`, and sync `PageStore` methods. All callers should already have been migrated to async in Task 6 (persist.cpp) and Task 10 (tests). This task only removes class declarations and dead code.

**Changes**:
- [ ] `crowtree/include/crowtree/page_store.h`: Remove sync `write_at`/`read_at`/`sync` methods. Remove `MemPageStore` class. Remove `FilePageStore` class.
- [ ] `crowtree/src/page_store.cpp`: Remove `MemPageStore` and `FilePageStore` implementations.
- [ ] `crowtree/include/crowtree/async_page_store.h`: Remove `FileAsyncPageStore`.
- [ ] `crowtree/src/file_async_page_store.cpp`: Delete file.
- [ ] `crowtree/src/c_api.cpp`: Remove `FileAsyncPageStore` construction. Select `DirectIoEngine` by default.
- [ ] `crowtree/CMakeLists.txt`: Remove `file_async_page_store.cpp`.
- [ ] Verify no remaining callers of sync `PageStore` methods (should be zero after Task 6 + Task 10).

**Files**:
- `crowtree/include/crowtree/page_store.h`
- `crowtree/src/page_store.cpp`
- `crowtree/include/crowtree/async_page_store.h`
- `crowtree/src/file_async_page_store.cpp` (delete)
- `crowtree/src/c_api.cpp`
- `crowtree/CMakeLists.txt`

**Test**:
- [ ] Verify `FilePageStore`, `FileAsyncPageStore`, `MemPageStore` are fully removed (no references).
- [ ] All tests pass with async-only API + `DirectIoEngine`.

---

### Task 12: Update documentation

**Goal**: Update design docs to reflect the new backend model.

**Changes**:
- [x] `doc/design/design-crowtree-storage.md` §2 (Backends): Updated backend table — `TextPageStore` (debug, text files) + `BlockPageStore` (production, array-of-blocks). Removed `FilePageStore` and `MemPageStore`.
- [x] `doc/design/design-crowtree-storage.md` §2.1 (TextPageStore Layout): Added directory layout, manifest file, anchor/page/segimage/segdir text formats.
- [x] `doc/design/design-crowtree-storage.md` §2.2 (BlockPageStore Layout): Added array-of-blocks design, block file naming, binary on-disk layout diagram, anchor/page/segment/gap descriptions, recovery and sync.
- [x] `doc/design/design-crowtree-storage.md` §8 (Mapping Table): Updated on-disk format to reference both binary (§2.2) and text (§2.1) layouts. Fixed anchor field list to match actual `CommitAnchor` struct.
- [ ] `doc/design/design-crowtree-storage.md` §3.3 (Alignment): Update to mention `TextPageStore` always uses IU=1 (no alignment).
- [ ] `doc/doc_index.md`: Update the storage design row if scope changes.
- [ ] `doc/todo_code.md`: Remove any items related to this refactor.

**Files**:
- `doc/design/design-crowtree-storage.md`
- `doc/doc_index.md`
- `doc/todo_code.md`

---

### Task 13: IoUringEngine (Stage 2 — Linux only)

**Goal**: Implement `IoUringEngine` as the production Linux I/O engine. Submits `io_uring` SQEs for read/write/fsync, completions via CQ polling in the `Reactor` event loop. This task is deferred to Stage 2 — requires Linux build and debug environment.

**Design**:
- `IoUringEngine` implements `IoEngine`:
  - `submit_read`/`submit_write` → push SQE with `IORING_OP_READ`/`IORING_OP_WRITE`, register callback in CQ.
  - `submit_fsync` → push SQE with `IORING_OP_FSYNC`.
  - Completions polled by `Reactor` event loop, callbacks invoked on CQ events.
- `BlockPageStore` dispatches to the correct extent's fd — `IoUringEngine` handles multi-fd naturally (each SQE carries its own fd).
- `ct_open`: selects `IoUringEngine` when `CROWTREE_HAVE_LIBURING` is defined.
- Build: `CMakeLists.txt` detects `liburing` dev package, defines `CROWTREE_HAVE_LIBURING`.

**Changes**:
- [ ] `crowtree/include/crowtree/io_engine.h`: Add `IoUringEngine` declaration.
- [ ] `crowtree/src/io_uring_engine.cpp` (new): `IoUringEngine` implementation.
- [ ] `crowtree/src/reactor.cpp`: Integrate `io_uring` CQ polling into event loop.
- [ ] `crowtree/src/c_api.cpp`: Select `IoUringEngine` when `CROWTREE_HAVE_LIBURING`.
- [ ] `crowtree/CMakeLists.txt`: Detect `liburing`, add `io_uring_engine.cpp`.
- [ ] `crowtree/ffi/build.rs`: Link `liburing` when available.

**Files**:
- `crowtree/include/crowtree/io_engine.h`
- `crowtree/src/io_uring_engine.cpp` (new)
- `crowtree/src/reactor.cpp`
- `crowtree/src/c_api.cpp`
- `crowtree/CMakeLists.txt`
- `crowtree/ffi/build.rs`

**Test**:
- [ ] Async read/write round-trip via `IoUringEngine` on Linux.
- [ ] Multi-extent I/O: write spanning 2+ block files, verify correct fd dispatch.
- [ ] fsync via `io_uring` (`IORING_OP_FSYNC`).
- [ ] Reactor integration: completions arrive via CQ polling.
- [ ] All existing tests pass with `IoUringEngine` on Linux.

**Note**: This task requires a Linux machine with `liburing` installed. Build and debug on Linux. Not blocking Stage 1 — `DirectIoEngine` covers all platforms.

---

### Task 14: Block compaction / merge — analysis & design (no implementation)

**Goal**: Analyze whether and how block files should be merged/compacted after GC creates significant free space within blocks. Produce a design document, not code. This is a follow-up to the array-of-blocks design (Task 3).

**Problem statement**:

After GC runs (`collect_garbage` in `crowtree.cpp`), dead pages are retired and their addresses become gaps in `SpaceAllocator`. Over time, a block file may have most of its space as gaps — logically free but physically occupying disk. The question is: should we merge sparsely-used blocks into denser ones, and if so, how?

**Core challenge — page relocation**:

Merging blocks requires moving live pages to new locations. This changes page addresses, which are stored in the mapping table's slot words (`slot_word::unloaded_iu_index`). Every relocated page's slot word must be updated. This is not a simple file-level operation — it touches the entire mapping table.

**Design analysis**:

1. **When to trigger compaction**:
   - After GC, compute per-block free ratio: `gaps / block_size`.
   - If a block's free ratio exceeds a threshold (e.g., 70%), it's a compaction candidate.
   - Compaction should be batched — don't compact one block at a time. Collect N sparse blocks, allocate one new dense block, copy live pages, update mapping table, then delete old blocks.

2. **Page relocation mechanics**:
   - For each live page in a candidate block:
     a. Allocate new address in `SpaceAllocator` (in the new dense block).
     b. `read_at(old_addr)` → `write_at(new_addr)` — copy the page blob.
     c. Update the mapping table slot word: `slot_word::unloaded_iu_index` → new address.
     d. The old address becomes a gap.
   - Segment images and segment directory may also need relocation if they reside in candidate blocks.

3. **Mapping table update**:
   - The mapping table (`mapping_`) stores slot words in segments. Each slot word encodes `(iu_index, iu_count)` for unloaded pages.
   - Relocating a page means updating its slot word's `iu_index` to the new address.
   - If the page is resident (in-memory), the slot word holds a pointer — no address change needed until eviction.
   - **This is the expensive part**: scanning all segments to find and update slot words for relocated pages.
   - Optimization: build a relocation map `{old_addr → new_addr}` before starting, then walk segments once.

4. **Crash safety**:
   - Compaction must be crash-safe. If the process dies mid-compaction:
     - Old blocks still exist (not yet deleted) → old addresses still valid.
     - New block has copied pages → new addresses valid.
     - The mapping table may have a mix of old and new addresses.
     - On recovery, the anchor's snapshot determines which addresses are live. If compaction wasn't committed (no new snapshot), old addresses are used.
   - **Approach**: compaction is a multi-step operation that completes atomically with a new snapshot:
     1. Copy live pages to new block.
     2. Update mapping table slot words.
     3. Write new snapshot (anchor + segment images + segment directory).
     4. After snapshot is durable, delete old blocks.
   - Steps 1-3 are the same as a normal snapshot — the compaction just changes *where* pages live before snapshotting.

5. **Interaction with ongoing I/O**:
   - Compaction should hold `write_mutex_` (same as GC and snapshot).
   - Resident pages are unaffected — they're in memory. Only unloaded (on-disk) pages have addresses that change.
   - After compaction, resident pages' eventual eviction writes to the new address (slot word already updated).

6. **Cost vs benefit**:
   - Compaction reads all live pages from sparse blocks + writes them to a dense block = I/O cost proportional to live data.
   - Benefit: frees disk space (deletes sparse block files).
   - For write-heavy workloads with high churn, compaction reclaims significant space.
   - For read-heavy or append-mostly workloads, blocks are naturally dense — compaction rarely needed.
   - **Recommendation**: implement as an explicit operator-triggered operation (not automatic), similar to LSM compaction triggers. Add a `compact_blocks()` API that the operator calls when disk usage is high.

7. **Alternative: online relocation during snapshot**:
   - Instead of a separate compaction pass, integrate relocation into the normal snapshot path.
   - During snapshot, if a page's current block is sparse, relocate it to a denser block as part of the snapshot write.
   - This amortizes compaction cost across snapshots — no separate I/O burst.
   - Risk: increases snapshot latency unpredictably. Better as a follow-up optimization.

**Deliverable**: A design section in `doc/design/design-crowtree-storage.md` (new §2.5 "Block Compaction") documenting the analysis above. No code changes in this plan.

**Files**:
- `doc/design/design-crowtree-storage.md` (add §2.5)

**Test**: N/A (design only)

---

## Task Dependency Graph

```
Task 1 (IoEngine + async) ──┐
                             ├──→ Task 2 (IU=1 mode) ──→ Task 3 (array-of-blocks) ──→ Task 6 (persist.cpp)
                             │                                                        │
                             ├──→ Task 4 (TextPageStore + text codecs)                 │
                             │                                                        │
                             ├──→ Task 5 (WAL .ck ext, standalone)                     │
                             │                                                        │
                             └──────────────────────────────────────────→ Task 7 (c_api) ←──┘
                                                                          │
                                                                          ├──→ Task 8 (FFI)
                                                                          │
                                                                          ├──→ Task 9 (fsync policy)
                                                                          │
                                                                          ├──→ Task 10 (tests)
                                                                          │         │
                                                                          │         └──→ Task 11 (delete old code)
                                                                          │                    │
                                                                          │                    └──→ Task 12 (docs)
                                                                          │
                                                                          └──→ Task 13 (IoUring, Stage 2)
                                                                          │
                                                                          └──→ Task 14 (block compaction design)
```

## Execution Order

- [x] **Task 1** — IoEngine interface + DirectIoEngine (async foundation, first)
- [ ] **Task 2** — BlockPageStore Medium abstraction + IU=1 mode (builds on Task 1)
- [ ] **Task 3** — Array-of-blocks growth + dump utility (builds on Task 2)
- [ ] **Task 4** — TextPageStore + text codecs (builds on Task 1)
- [ ] **Task 5** — WAL `.log` → `.ck` extension (standalone, no dependencies)
- [ ] **Task 6** — persist.cpp adaptation + async migration (builds on Task 3)
- [ ] **Task 7** — c_api update + backend selection (builds on Tasks 3 + 4)
- [ ] **Task 8** — FFI bindings (builds on Task 7)
- [ ] **Task 9** — Configurable fsync policy (builds on Task 7, crowtree + WAL)
- [ ] **Task 10** — Test updates (builds on all above)
- [ ] **Task 11** — Delete old code + cleanup sync API (builds on Task 10)
- [ ] **Task 12** — Documentation (last)
- [ ] **Task 13** — IoUringEngine (Stage 2, Linux only, builds on Task 1)
- [ ] **Task 14** — Block compaction / merge analysis & design (no implementation, builds on Task 3)

## Notes

- **WAL alignment**: WAL uses `WalBlockAlignment::Unaligned` (IU=1, byte-addressable) vs `Aligned { io_unit_bytes }` (SSD). crowtree's `BlockPageStore` follows the same model: `iu_size=1` for mem/SCM, `iu_size=4096+` for NVMe.
- **WAL segment rotation**: WAL creates `seg-{NNNNNNN}.ck` files on rotation (Task 5 changes from `.log` to `.ck`). crowtree's array-of-blocks creates `{store_id}-{partition_id}.blk-{NNNN}` files on growth. Same pattern, with self-identifying names and CrowKV-specific extensions.
- **WAL text encoding**: WAL uses `CROW_WAL_TEXT` prefix with `key=value` fields + CRC32C. crowtree's `TextPageStore` uses `CROW_CT_ANCHOR` prefix for anchors and `debug_codec` for page frames. Same pattern.
- **Compression**: `TextPageStore` always uses `compression = kNone` (text mode is for debugging). `BlockPageStore` supports LZ4 compression as before.
- **Async I/O**: Upper layer always uses async API. `DirectIoEngine` (Stage 1, all platforms) wraps blocking I/O as immediately-ready async. `IoUringEngine` (Stage 2, Linux) uses `io_uring` via `Reactor`. `FilePageStore` and `FileAsyncPageStore` are removed in Task 11. See Task 1 + Task 13.
- **fsync**: Configurable `sync_mode` (`CT_SYNC_FULL`/`CT_SYNC_SKIP`/`CT_SYNC_BATCH`). macOS `fsync` ~3ms; `CT_SYNC_SKIP`/`CT_SYNC_BATCH` for tests/CI. Same policy applies to WAL. See Task 9.
- **Text codecs**: anchor/segimage/segdir text codecs are in Task 4 (merged with TextPageStore). `debug_codec` handles page frames.

---
