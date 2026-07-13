// Options: tunables for consolidation, flush triggers, and page split/merge.
// Defaults follow the core engine design.
#pragma once

#include "crowtree/compressor.h"

#include <cstddef>
#include <cstdint>
#include <string>

namespace crowtree
{

class PageStore;

struct Options
{
    // ── Consolidation (core doc §7) ──
    // Fold a leaf's delta chain into a fresh base when either threshold trips.
    uint32_t max_delta_len   = 8;
    size_t   max_delta_bytes = 256ULL * 1024; // 256 KiB

    // ── Leaf split / merge (core doc §8) ──
    // Split when a consolidated leaf exceeds leaf_split_bytes; merge when it
    // drops below leaf_merge_bytes. Hysteresis: merge threshold is well below
    // split to avoid oscillation.
    size_t leaf_split_bytes = 64ULL * 1024; // 64 KiB target page size
    size_t leaf_merge_bytes = 16ULL * 1024; // split/4

    // Inner-page fanout bound (separator count) before an inner split.
    uint32_t inner_max_keys = 256;
    // Inner-page underflow threshold: when a non-root inner page drops below this
    // many separators (after a child merge), it is merged with its left sibling so
    // delete-heavy workloads don't leave a sparse upper tree. 0 = derive
    // max(1, inner_max_keys / 4) (well below the post-split size for hysteresis).
    uint32_t inner_merge_keys = 0;

    // ── Overflow pages (design §3 / PT11) ──
    // A value larger than this spills out of its leaf into a chain of fixed
    // overflow frames; the leaf keeps a small fixed pointer cell, so every base
    // page stays <= frame_bytes (pool-resident + evictable). 0 = derive a default
    // of frame_bytes / 4 at engine construction.
    size_t max_inline_value = 0;

    // ── Key size limit (plan-tree #15) ──
    // A key larger than this is rejected at apply() with kInvalidArgument (an
    // oversized key is assumed to be a caller bug: keys are copied into every
    // delta and inner separator, so a huge key would blow up the tree). 0 =
    // derive a default of frame_bytes / 2 at engine construction, which keeps a
    // key comfortably within a single leaf/inner frame alongside its cell.
    size_t max_key_size = 0;

    // ── In-frame delta region (design §5A / PT12) ──
    // When enabled, a small flush appends its mutations as in-frame deltas via a
    // cheap frame COW (memcpy + append) instead of building a fresh sorted base or
    // a heap delta node; the leaf folds to a fresh base once delta_count reaches
    // max_inframe_delta or the deltas no longer fit. default_env off (the heap delta
    // overlay already works; this is an optimization measured against COW-rebuild).
    bool     inframe_delta     = false;
    uint32_t max_inframe_delta = 8;

    // ── MemTable flush triggers (core doc §6.2) ──
    size_t   memtable_flush_bytes   = 4ULL * 1024 * 1024; // 4 MiB
    uint32_t memtable_flush_entries = 100000;
    uint64_t flush_interval_ms      = 200; // time-based flush

    // Run the flusher on a background thread. Tests often set this false and
    // drive flush() synchronously for determinism.
    bool background_flush = false;

    // ── MemTable double buffering (plan-tree #3) ──
    // Once active_ crosses memtable_flush_bytes/_entries, it is frozen (no
    // longer accepts writes) and a fresh, empty MemTable takes over as
    // active_; the frozen table is drained into L1 by flush() (explicit call
    // or the background thread). This bounds how large any *one* buffer can
    // grow and lets new writes proceed without contending with an in-progress
    // drain (they land in a different MemTable object entirely). This is the
    // count of buffers total: 1 active_ + up to (max_memtable_count - 1)
    // queued frozen buffers. Must be >= 2 (2 = the common active_/flushing_
    // case); values are not currently validated by the engine. When the
    // frozen queue is already at (max_memtable_count - 1), a further
    // threshold-triggered freeze is skipped (active_ is allowed to keep
    // growing past its threshold) rather than stalling the writer -- an
    // explicit flush()/the background thread is expected to catch up and
    // free a slot. See Crowtree's active_/frozen_ member comment for the
    // full design, including how non-contiguous (slot > the current
    // contiguous frontier) leftovers are handled when a frozen buffer is
    // drained.
    uint32_t max_memtable_count = 2;

    // ── Retention GC (plan-tree #21) ──
    // Periodic collect_garbage() sweep cadence on the same background thread as
    // background_flush (no second thread). 0 = disabled: collect_garbage() then
    // only runs when a caller invokes it explicitly.
    uint64_t gc_interval_ms = 0;

    // ── Persistence ──
    // Durable backend. Non-owning; nullptr = pure in-memory engine (no
    // snapshot/recovery). When set, snapshot() writes the materialized L1
    // state and open() recovers it.
    PageStore *page_store = nullptr;

    // ── Buffer pool (design §4) ──
    // The arena that holds base-page frames is a flat array of equal-size frames.
    // `frame_bytes` is that fixed size; a base page larger than a frame (rare;
    // overflow pages are PT11) or built when the pool is full falls back to a
    // heap buffer, so correctness is independent of these knobs. `frame_bytes`
    // should be >= leaf_split_bytes so normal leaves are pool-resident.
    uint32_t frame_bytes       = 64ULL * 1024;        // fixed arena frame size
    size_t   buffer_pool_bytes = 64ULL * 1024 * 1024; // arena capacity (frames * frame_bytes)

    // ── Page compression (design §3.5/§3.6, PT10) ──
    // On-disk only: each durable base page is wrapped in a self-describing blob
    // ([algo][raw_len][stored_len][crc][stored]); the buffer pool always holds
    // uncompressed frames. The algo is recorded per page, so a page is decoded
    // correctly regardless of the option in force when it is read back (mixed
    // pages across incremental snapshots decode fine). default_env kNone keeps the
    // stored bytes equal to the raw frame; kLz4 compresses when it shrinks the
    // page (falling back to stored-raw when LZ4 is unavailable or unhelpful).
    compress_algo compression = compress_algo::kNone;

    // ── Logging (plan-tree #10) ──
    // When log_dir is non-empty, Crowtree::open() initializes an async, size-
    // rotating file logger at <log_dir>/crowtree.log (no-op if the library was
    // built without spdlog). log_level is an spdlog level name
    // (trace/debug/info/warn/error/off). Rotation defaults: 100 MiB × 5 files.
    std::string log_dir;                  // empty = logging disabled
    std::string log_level       = "info"; // spdlog level name
    size_t      log_max_file_mb = 100;    // per-file rotation size
    size_t      log_max_files   = 5;      // rotated files kept
};

} // namespace crowtree
