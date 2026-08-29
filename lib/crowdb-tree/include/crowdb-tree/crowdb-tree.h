// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Crowdbtree: one ordered, single-version-per-key store per consensus group.
// Two-level write path: apply() lands in the MemTable
// (L0); flush() merges the contiguous-applied prefix into the COW B+tree (L1).
#pragma once

#include "crowdb-common/metrics/metrics.h"
#include "crowdb-tree/cell.h"
#include "crowdb-tree/epoch.h"
#include "crowdb-tree/mapping_table.h"
#include "crowdb-tree/memtable.h"
#include "crowdb-tree/options.h"
#include "crowdb-tree/page.h"
#include "crowdb-tree/scan_packed.h"
#include "crowdb-tree/snapshot.h"
#include "crowdb-tree/status.h"

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <vector>

namespace crowdb::tree
{

// The metrics core moved to crowdb-common::metrics (R12); bridge the moved types
// into `crowdb-tree` with per-type using-declarations so existing
// `Counter*`/`Gauge*`/`LatencySummary*`/`MetricsRegistry`/`Bandwidth`
// references compile unchanged. (Not a `namespace crowdb::tree =
// crowdb::common::metrics;` alias — only the specific types are bridged.)
using crowdb::common::metrics::Bandwidth;
using crowdb::common::metrics::Counter;
using crowdb::common::metrics::Gauge;
using crowdb::common::metrics::LatencySummary;
using crowdb::common::metrics::MetricsRegistry;

#ifdef CROWDB_HAVE_LIBURING
using crowdb::common::DiskIOUring;
class AsyncPageStore;
#endif

// One mutation in a batch. All ops in a batch share the batch's slot.
struct batch_op
{
    std::string key;
    OpKind      kind;
    std::string value; // empty for Delete
};

struct Batch
{
    std::vector<batch_op> ops;
};

struct scan_entry
{
    std::string key;
    uint64_t    slot;
    std::string value;
    bool        tombstone = false;
};

struct get_result
{
    bool        found = false;
    uint64_t    slot  = 0;
    std::string value;
};

// Zero-copy point-read result (plan-tree #5 B3 remaining). `value()` is a
// borrowed `Slice` for an L1 hit resolved to a non-overflow cell -- it
// points directly into the resident leaf's frame, kept alive for this
// object's lifetime by the epoch guard it owns (no copy). An L0 hit (R50:
// the MemTable's skip-list node is epoch-protected the same way an L1 frame
// is) also borrows directly off the node's cell version. An overflow value
// (assembled from multiple pages, no single frame to borrow) is materialized
// into an owned `buffer` instead; `value()` is transparent to the caller
// either way.
//
// Move-only (like `EpochManager::Guard`): copying would either double-free
// the guard or silently let a caller outlive it. `get()`/`multi_get()` are
// thin wrappers over `get_view()` that clone `value()` into a `std::string`
// and let the guard drop before returning, preserving their existing owned-
// copy contract for every other caller.
class GetView
{
  public:
    GetView() = default;

    // Not defaulted (found this the hard way, via
    // ASan): `owned_` (a `buffer`) relocates its bytes on move when small
    // enough to be inline (SBO, buffer::kInlineCap) -- but `value_` is a
    // *separate* field, a plain Slice pointer+len that a defaulted move
    // would blindly copy byte-for-byte, still aliasing the just-moved-from
    // `owned_`'s old (now-stale, for an inline buffer) storage. Any
    // resolved GetView whose value is owned (owned_ non-empty) must have
    // `value_` re-derived from *this* object's own (possibly relocated)
    // `owned_` after the move -- a borrowed (frame-pointing) value_ is
    // untouched either way, since it aliases external storage the move
    // never touches.
    GetView(GetView &&o) noexcept
        : guard_(std::move(o.guard_)),
          found_(o.found_),
          slot_(o.slot_),
          value_(o.value_),
          owned_(std::move(o.owned_)),
          pins_(std::move(o.pins_))
    {
        if (!owned_.empty()) {
            value_ = owned_.slice();
        }
    }

    GetView &operator=(GetView &&o) noexcept
    {
        if (this != &o) {
            release_pins();
            guard_ = std::move(o.guard_);
            found_ = o.found_;
            slot_  = o.slot_;
            value_ = o.value_;
            owned_ = std::move(o.owned_);
            pins_  = std::move(o.pins_);
            if (!owned_.empty()) {
                value_ = owned_.slice();
            }
        }
        return *this;
    }

    GetView(const GetView &)            = delete;
    GetView &operator=(const GetView &) = delete;

    ~GetView()
    {
        release_pins();
    }

    [[nodiscard]] bool found() const
    {
        return found_;
    }

    [[nodiscard]] uint64_t slot() const
    {
        return slot_;
    }

    // Valid only while this GetView is alive.
    [[nodiscard]] Slice value() const
    {
        return value_;
    }

    // R6 debug-only: the frame address a borrowed value points into, or
    // nullptr for an owned (L0 / overflow) value. Used by tests to verify
    // the get_async slow path returns a borrowed Slice (no copy).
    [[nodiscard]] const uint8_t *frame_base() const
    {
        return owned_.empty() ? value_.bytes() : nullptr;
    }

  private:
    friend class Crowdbtree;
    EpochManager::Guard guard_; // keeps an L1 hit's frame resident
    bool                found_ = false;
    uint64_t            slot_  = 0;
    Slice               value_; // borrowed (L1) or owned_.slice() (L0 / overflow)
    buffer              owned_; // backing storage when the value can't be borrowed
    // R6: cross-thread pins holding the borrowed value's chain alive after
    // the epoch guard is released (get_async slow path). Empty on the fast
    // path (guard_ alone keeps the frame resident) and for owned values.
    std::vector<PageBase *> pins_;
    // R6: the chain head whose frame/entries back the borrowed value_, set
    // by try_get_view_no_load when the value is borrowed (not L0/overflow).
    // Used by the slow path to walk + pin the chain before releasing guard_.
    PageBase *borrowed_chain_head_ = nullptr;

    void release_pins()
    {
        for (PageBase *p : pins_) {
            p->unpin();
        }
        pins_.clear();
    }
};

// Result of an explicit collect_garbage() sweep (plan-tree #21).
struct GcStats
{
    uint64_t tombstones_dropped = 0; // tombstone cells physically dropped
    uint64_t pages_freed        = 0; // resident pages (deltas + old leaf bases) retired
    uint64_t bytes_freed        = 0; // logical key+cell bytes of the dropped tombstones
};

// Point-in-time diagnostics snapshot: batches every
// cheap (O(1)) internal counter worth exposing to an operator into one
// struct, so a caller/FFI/console poll costs one call instead of many
// small ones. Deliberately excludes anything that requires walking the
// tree (height()/leaf_count()) or the full keyspace (live key count,
// already exposed separately via KVEngine::live_key_count) -- every field
// here is already an atomic counter or BufferPool::stats(), also O(1).
struct EngineStats
{
    uint64_t last_applied_slot         = 0;     // durable watermark (see last_applied_slot())
    uint64_t contiguous_slot           = 0;     // gap-free-applied watermark (see contiguous_slot())
    uint64_t gc_watermark              = 0;     // min(snapshot_slot, safe_slot) (see gc_watermark())
    bool     io_failed                 = false; // latched media fault (see io_failed())
    uint64_t snapshot_pages_written    = 0;     // last snapshot()'s dirty base pages written
    uint64_t snapshot_pages_total      = 0;     // cumulative pages written across all snapshots
    uint64_t snapshot_segments_written = 0;     // last snapshot()'s dirty mapping segments written
    // BufferPool::Stats as of this call -- see buffer_pool.h.
    uint64_t buffer_pool_hits       = 0;
    uint64_t buffer_pool_misses     = 0;
    uint64_t buffer_pool_evictions  = 0;
    uint64_t buffer_pool_writebacks = 0;
    uint32_t buffer_pool_resident   = 0;
    uint32_t buffer_pool_dirty      = 0;
    uint32_t buffer_pool_used       = 0;
    uint32_t buffer_pool_num_frames = 0;
    // MemTable (L0) / flush / L1 cumulative counters (monotonic since open).
    uint64_t mt_upsert_total     = 0; // apply() writes into L0
    uint64_t mt_get_total        = 0; // get() lookups in L0
    uint64_t mt_get_hit_total    = 0; // L0 lookups that found a cell
    uint64_t flush_drain_total   = 0; // drain_memtable_into_l1 calls
    uint64_t flush_entries_total = 0; // entries drained from L0 to L1
    uint64_t snapshot_total      = 0; // snapshot() calls (durable checkpoints)
    uint64_t l1_get_total        = 0; // get() lookups that descended to L1
    uint64_t l1_get_hit_total    = 0; // L1 lookups that found a cell
    uint64_t map_lookup_total    = 0; // mapping table lookups
    uint64_t demand_load_total   = 0; // demand-load page faults
};

// Per-step scan profile: each step's aggregate over the window since the last
// scan_profile() call (the underlying LatencySummary handles are flushed, so
// this is a destructive read -- the window resets on each call). `count` is the
// number of scans in the window; `entries` is the total entries returned. Each
// step's `sum_ns` / `max_ns` cover only that step; `avg_ns` is sum_ns / count.
// Steps: l0_snapshot (MemTable::snapshot copy), l0_skip (upper_bound pass),
// l1_descent (find_leaf_page_id), l1_resolve (per-leaf LeafChainCursor setup +
// cursor seek, summed across all leaves touched), merge (min-key select +
// winner + per-entry cursor step + consider/decode, excluding l1_resolve),
// total (whole scan). The per-entry leaf work is lazy, so it is
// counted under merge -- timing each cursor step would cost more than the step
// itself; l1_resolve is now per-leaf setup only and scales with leaves
// touched, not entries per leaf.
struct ScanProfile
{
    uint64_t count   = 0; // scans in the window
    uint64_t entries = 0; // total entries returned

    struct Step
    {
        uint64_t sum_ns = 0;
        uint64_t max_ns = 0;
        uint64_t avg_ns = 0; // sum_ns / count (filled by scan_profile)
    };

    Step l0_snapshot;
    Step l0_skip;
    Step l1_descent;
    Step l1_resolve;
    Step merge;
    Step total;
};

// One durable blob to write at a fixed offset, computed ahead of time by
// prepare_snapshot_locked() (persist.cpp) so the actual store->write_at()/
// submit_write() call is a pure I/O op with no further encoding logic --
// shared by snapshot()'s synchronous writes and snapshot_async()'s async
// ones.
struct PreparedSnapshotWrite
{
    uint64_t             addr = 0;
    std::vector<uint8_t> blob; // already IU-padded
};

// A page write plus enough identity to safely mark the *live* page durable
// once the write actually lands (see prepare_snapshot_locked's doc comment
// on why this can't happen eagerly at prepare time for the async path).
// `page` is never dereferenced except as an opaque identity check under
// write_mutex_ (mapping_.get_resident(page_id) == page) -- it may have been retired
// and its frame reused by the time the write completes (a concurrent
// consolidate/flush/split replaced this page_id's mapping entry with a
// fresh COW page in the meantime), in which case the identity check simply
// fails and this write's durable-bookkeeping is skipped (harmless: the old
// blob is still correctly on disk and referenced by *this* generation's
// segment image; the fresh page is independently dirty and picked up by
// the next snapshot).
struct PreparedPageWrite
{
    uint64_t             page_id     = 0;
    PageBase            *page        = nullptr; // opaque identity only
    uint64_t             addr        = 0;
    uint32_t             logical_len = 0; // unpadded; mirrors PageBase::durable_plen
    std::vector<uint8_t> blob;            // already IU-padded
};

// A dirty MappingSegment's fresh image write, plus enough identity to
// safely mark it durable at commit time (mirrors PreparedPageWrite's
// identity-check pattern, extended with `seen_write_seq` -- see
// MappingSegment's doc comment on why a segment needs a seq check, not just
// a pointer identity check: unlike a page, whose whole *pointer* is
// replaced on any change, a segment's pointer stays the same across a
// slot mutation, so identity alone can't detect "written again during the
// prepare-to-commit gap").
struct PreparedSegmentWrite
{
    uint64_t             seg_idx        = 0;
    MappingSegment      *seg            = nullptr; // opaque identity only
    uint64_t             seen_write_seq = 0;
    uint64_t             new_generation = 0;
    uint64_t             addr           = 0;
    uint32_t             logical_len    = 0; // unpadded
    uint32_t             image_crc      = 0; // body-only CRC, matches the directory entry prepare wrote
    std::vector<uint8_t> blob;               // already IU-padded
};

// Output of prepare_snapshot_locked(): every byte this snapshot generation
// needs written, computed synchronously under write_mutex_ (the segment
// scan + delta-fold + page/segment-image/directory encode is CPU/memory-only
// -- see the "Lock scope" note on #11). The caller writes
// `page_writes` and `segment_writes` (any order/concurrency) then
// `directory_write`, then a durability barrier, then `anchor_write` --
// writing the anchor before that barrier would violate the crash-safety
// invariant persist.cpp's header comment documents (a crash mid-snapshot
// must fall back intact to the last *committed* anchor) -- then
// commit_prepared_snapshot() to mark each page/segment durable and publish
// the new version.
struct PreparedSnapshot
{
    std::vector<PreparedPageWrite>    page_writes;
    std::vector<PreparedSegmentWrite> segment_writes;
    PreparedSnapshotWrite             directory_write;
    PreparedSnapshotWrite             anchor_write;
    uint64_t                          last_applied_slot = 0;
    // Diagnostics for the "snapshot committed" log line (matches the
    // pre-refactor synchronous snapshot()'s log fields exactly).
    uint64_t           seq             = 0;
    uint64_t           live_page_count = 0; // live slots across every present segment
    uint64_t           pages_written   = 0;
    uint64_t           segdir_len      = 0;
    std::set<uint32_t> empty_blocks; // block indices with zero live bytes (block compaction)
};

// One page's raw frame bytes, tagged with its logical PID (plan-tree #16
// native snapshot format). Unlike the portable format's `leaf_entry`
// (decoded key/cell tuples), this is the frame verbatim -- no
// encode/decode, no cell-by-cell rebuild on import, so a leaf/inner/
// overflow page round-trips as one `memcpy`-equivalent copy.
struct NativeFrame
{
    uint64_t             page_id = kInvalidPageId;
    std::vector<uint8_t> frame; // raw in-memory frame bytes (page_bytes() length)
};

class Crowdbtree
{
  public:
    explicit Crowdbtree(Options opt = Options());
    ~Crowdbtree();

    Crowdbtree(const Crowdbtree &)            = delete;
    Crowdbtree &operator=(const Crowdbtree &) = delete;

    // open a tree, recovering durable state from opt.page_store if a valid
    // snapshot exists; otherwise start empty. Requires opt.page_store != null.
    static Status open(const Options &opt, std::unique_ptr<Crowdbtree> *out);

    // Persist the materialized L1 state durably. Folds delta chains, writes
    // dirty base pages plus a fresh image for each dirty mapping-table
    // segment and the segment directory, then commits the inactive A/B
    // anchor slot. Returns the durable last_applied_slot via out (if
    // non-null). Requires opt.page_store != null.
    Status snapshot(uint64_t *out_last_applied = nullptr);

    // Async twin of snapshot(). Always genuinely
    // async from *this* caller's perspective when Options::async_uring/
    // async_page_store are wired (flush/snapshot are
    // *always* slow-path, unlike get/scan): snapshot_async() returns
    // immediately after kicking off the walk + first I/O submission, and
    // on_done fires later from the Reactor thread with the same result
    // snapshot() would have returned.
    //
    // Lock discipline (this is the one place in the engine where a
    // completion legitimately runs on a *different* thread than the one
    // that started the operation, so it gets its own note): write_mutex_
    // itself is only ever locked and unlocked on the *same* thread, for the
    // brief synchronous prepare_snapshot_locked() walk, exactly like every
    // other writer entry point -- std::mutex has no defined behavior for a
    // cross-thread unlock, so it is never held across the async write
    // phase. What *does* span the whole prepare-through-commit sequence is
    // snapshot_inflight_, a plain std::atomic<bool> spin-gate (a mutex's
    // "same thread unlocks it" restriction is a pthread_mutex_t property,
    // not a general lock property -- an atomic has no such restriction):
    // it serializes this generation's SpaceAllocator against a *second*
    // overlapping snapshot(_async) call (which would otherwise rebuild an
    // allocator from the same last-*committed* anchor and could hand
    // out the same "free" byte range to two different pages -- silent
    // corruption), without blocking apply()/flush()/evict_clean_leaves(),
    // which remain free to run concurrently against write_mutex_ as usual.
    // That safety hinges on prepare_snapshot_locked() never eagerly setting
    // a dirty page's PageBase::durable_addr -- see its doc comment --
    // because evict_clean_leaves_locked() treats durable_addr != kNoAddr as
    // "safe to evict, a durable copy already exists"; commit_prepared_snapshot()
    // is what actually sets it, one write_mutex_ critical section per page,
    // only once that page's specific byte write has landed.
    //
    // Falls back to running the existing synchronous snapshot() in the
    // caller's stack frame (still correct, just not async) when no async
    // backend is wired -- e.g. a MemPageStore-backed tree.
    void snapshot_async(std::function<void(Status, uint64_t last_applied)> on_done);

    // Ingest a batch at `slot`. The tree internally tracks received slots and
    // computes the contiguous prefix (how far the flusher may flush) itself, so
    // callers no longer pass Paxos/learner state. Lands in L0; may trigger a
    // size-based flush. For a slot with no data (a NoOp), call force_advance_slot.
    Status apply(uint64_t slot, const Batch &batch);

    // One already-encoded op for apply_encoded (plan-tree #5 B2d): `cell` is
    // a slot+kind+value cell already packed via encode_cell_buf. Lets a
    // caller that owns the raw bytes up front (the C API boundary) allocate
    // the key and cell buffers exactly once and move them straight down to
    // MemTable::upsert, instead of building a Batch (plain key/kind/value
    // strings) that apply_batch would otherwise re-encode into a cell here.
    struct encoded_op
    {
        std::string key;
        buffer      cell;
    };

    // Same semantics as apply() (oversized-key rejection, slot bookkeeping,
    // maybe_swap_active), but for pre-encoded ops -- no encode_cell_buf call
    // in here, no intermediate Batch/batch_op. Intra-batch: last occurrence
    // (by vector order) wins, same as apply_batch.
    Status apply_encoded(uint64_t slot, std::vector<encoded_op> ops);

    // One zero-copy op for apply_external (R30): `value` is a kExternal buffer
    // borrowing bytes from a Rust `bytes::Bytes` (Put) or an empty buffer
    // (Delete, `flags = kFlagTombstone`). The 9-byte cell header is NOT in the
    // buffer — it is stored as the `flags` field here plus the `slot` argument,
    // and materialized into a contiguous cell at MemTable drain/get. Lets the
    // consensus apply path skip the value memcpy that encode_cell_buf performs.
    struct external_op
    {
        std::string key;
        uint8_t     flags = 0; // kPut (0) or kFlagTombstone
        buffer      value;     // kExternal (borrowed) for Put; empty for Delete
    };

    // Same semantics as apply_encoded (oversized-key rejection, slot
    // bookkeeping, maybe_swap_active, intra-batch last-key-wins) but stores
    // split cells via MemTable::upsert_external — no encode_cell_buf, no value
    // memcpy on the apply critical path. The value copy is deferred to flush
    // (off the critical path).
    Status apply_external(uint64_t slot, std::vector<external_op> ops);

    // Advance the contiguous frontier up to `slot`, filling any intervening slots
    // as NoOps (e.g. after learner NoOp slots or during restore). Explicit and
    // free of learner jargon.
    void force_advance_slot(uint64_t slot);

    // Convenience methods: auto-assign the next slot (max_seen + 1) and apply.
    // Intended for single-writer use; do not mix with explicit-slot apply calls.
    Status put(Slice key, Slice value);
    Status del(Slice key);
    Status batch_put(const Batch &batch);

    // Logical retention GC watermark:
    // stores both slots and computes gc_floor_ = min(snapshot_slot, safe_slot).
    // Tombstones with slot <= gc_floor_ may be dropped, during consolidation or
    // by an explicit collect_garbage() sweep. Using the min of the two (rather
    // than safe_slot alone) is what makes it safe to call this before #20's
    // learner wiring: a tombstone whose deletion isn't yet durable on a quorum
    // (snapshot_slot) is never dropped early just because every member has
    // locally applied it (safe_slot). Monotonic: gc_floor_ never regresses even
    // if a later call passes a smaller min.
    void set_gc_watermark(uint64_t snapshot_slot, uint64_t safe_slot);

    [[nodiscard]] uint64_t gc_watermark() const
    {
        return gc_floor_.load();
    }

    // Explicit tombstone-retention GC sweep (plan-tree #21). Force-consolidates
    // every *resident* leaf holding a tombstone <= gc_watermark(), independent
    // of the delta-chain-length/bytes consolidation trigger and of snapshot()'s
    // dirty-only rebuild -- both of those only touch a leaf that's already
    // dirty, so a leaf that receives a delete and then no further writes would
    // otherwise keep its tombstone past gc_floor_ indefinitely. Skips a leaf
    // whose resolved state has no tombstone to drop (cheap no-op sweep once the
    // tree is fully swept), and skips evicted (demand-load-unloaded) leaves
    // without paging them back in -- a background sweep must not defeat
    // eviction (#17); a cold leaf becomes eligible again once next reloaded.
    // Same retire_page()/epoch-guard mechanism as consolidate(), so it is safe
    // to run concurrently with lock-free readers (get/scan/snapshot_view).
    // Serialized against writers by write_mutex_.
    GcStats collect_garbage();

    // Drain the contiguous-applied prefix of every live MemTable (active_ +
    // any queued frozen_ buffers, plan-tree #3) into L1 and publish the
    // result. Always freezes whatever is currently in active_ first (even if
    // it hasn't crossed the size/entry threshold) so a single flush() call
    // fully drains all pending writes, same contract as before double
    // buffering. Non-contiguous leftovers (slot above the current contiguous
    // frontier) are relocated onto the live active_ MemTable rather than
    // lost -- see the active_/frozen_ member comment for the full design.
    Status flush();

    // Async twin of flush(). flush() only drains
    // L0 (MemTable) into L1 (in-memory B+tree) -- it never touches
    // Options::page_store (only snapshot() writes durable bytes), so unlike
    // snapshot_async() there is no genuine I/O to submit to the reactor
    // here: this always invokes on_done synchronously with flush()'s result
    // before returning. Exists so C API callers have a uniform
    // ct_flush_async/ct_snapshot_async shape even though
    // flush's own fast-path-vs-slow-path split is trivial today.
    void flush_async(std::function<void(Status)> on_done);

    // Point read (L0 overlay then L1). Returns true if a live value is found;
    // tombstones return false.
    [[nodiscard]] bool get(Slice key, uint64_t *out_slot, std::string *out_value) const;

    // Zero-copy point read (plan-tree #5 B3 remaining): same lookup as
    // get(), but the returned GetView borrows an L1 hit's value directly
    // from its resident frame instead of copying it out. See GetView's doc.
    [[nodiscard]] GetView get_view(Slice key) const;

    // Async twin of get(). Fast path (every page needed to resolve `key` is already
    // resident, or no async backend is wired -- see Options::async_uring/
    // async_page_store) invokes on_done synchronously, before this call
    // returns, exactly like get(). A genuine miss on the L1 descent (some
    // base page along the root->leaf path is tagged unloaded) submits
    // exactly one page load via the reactor and resumes automatically; on
    // completion it either resolves (calls on_done) or, if that page was
    // itself only a step deeper into the tree, hits another miss and
    // repeats -- on_done fires exactly once regardless, but for a miss it
    // runs on the Reactor thread, not the caller's.
    //
    // Scope boundary (deliberate, matches the miss scenario): only
    // the L1 base-page descent is async. A value spilled into an overflow
    // chain (large values, PT11) still resolves its chain synchronously via
    // the existing assemble_overflow_value()/resident() path -- overflow
    // chains are the less common case and the miss
    // walkthrough only describes "the leaf page is unloaded", not an
    // overflow page.
    //
    // Zero-copy fast path: `on_done`
    // receives the resolved `GetView` itself, not a copied-out std::string.
    // For the *first* attempt's synchronous resolution (this call's own
    // thread, no I/O), the GetView's epoch guard is still live -- it's safe
    // to keep borrowing an L1 hit's frame bytes all the way out to the C
    // ABI, since ct_future_free (which finally drops the guard) is
    // guaranteed to run on this same thread too. Any resolution that
    // crosses to the Reactor thread instead (a genuine miss) materializes
    // an owned copy and releases its guard before calling on_done -- see
    // get_async_attempt's `same_thread` parameter and `materialize_owned`.
    void get_async(Slice key, std::function<void(GetView)> on_done) const;

    // Batched point read.
    [[nodiscard]] std::vector<get_result> multi_get(const std::vector<Slice> &keys) const;

    // Ordered range scan over keys with `prefix` (empty = whole keyspace), latest
    // state (L0 overlaid on L1). When `include_tombstones` is false (default),
    // tombstones are skipped. Returns up to `limit` entries in key order; sets
    // *truncated if more matched beyond the limit. `start_after` (empty = start
    // from the beginning) is an exclusive lower bound: only keys strictly
    // greater than `start_after` are returned. When non-empty, the descent
    // targets the leaf that would contain `start_after` instead of `prefix`,
    // so a deep-pagination scan starts at the cursor rather than walking every
    // earlier leaf in the prefix range. `end_key` (empty = unbounded) is an
    // exclusive upper bound: only keys strictly less than `end_key` are
    // returned, and the merge loop early-stops once the winner key reaches it.
    // `byte_budget` (0 = unlimited) caps the total key+value bytes emitted;
    // the scan stops with *truncated = true when exceeded, always returning at
    // least one entry (so a single oversized entry still makes progress). A
    // warning is logged for any single entry whose key+value size alone
    // exceeds the budget. `keys_only` skips value materialization (no
    // overflow-chain assembly, no value copy): entries are staged with empty
    // values and the byte budget accounts for key bytes only, so a page fits
    // more entries. Default false.
    Status scan(Slice prefix, Slice start_after, Slice end_key, size_t limit, size_t byte_budget, bool keys_only,
                uint64_t deadline_ms, std::vector<scan_entry> *out, bool *truncated, bool include_tombstones = false,
                ScanPackedBuf *out_packed = nullptr, size_t *out_count = nullptr) const;

    // Async twin of scan(). Unlike get_async,
    // which has exactly one possible miss point (the root->leaf descent for
    // a single key), scan() walks a whole range of leaves via
    // right_sibling and any of them -- or an inner page on the initial
    // descent to the first leaf -- can be cold. Rather than a resumable
    // cursor, a miss simply retries the *whole* scan from scratch once the
    // blocking page resolves (matches get_async_attempt's own "retry, still
    // correct, not maximally efficient" trade-off) -- each retry is pure
    // in-memory work except for exactly one more page becoming permanently
    // resident, so this always terminates and does no redundant I/O.
    // on_done fires exactly once, synchronously if the whole scan was
    // already resident (matching scan()'s cost exactly), or from the
    // Reactor thread after however many page loads were needed. `start_after`
    // is the same exclusive lower bound as scan()'s. `end_key` is the same
    // exclusive upper bound as scan()'s. `byte_budget` is the same total
    // key+value byte cap as scan()'s. `keys_only` is the same value-skip flag
    // as scan()'s.
    void scan_async(Slice prefix, Slice start_after, Slice end_key, size_t limit, size_t byte_budget, bool keys_only,
                    uint64_t deadline_ms, std::function<void(Status, ScanPackedBuf, bool truncated)> on_done) const;

    // pin a consistent point-in-time view at `last_applied_slot` (the durable L1
    // state). Used for scan-at / compare / iter_all / snapshot export.
    // R6: returns a PinnedSnapshot (zero-copy, page refcount pins keep frames
    // alive across threads). The return type is shared_ptr<Snapshot> for ABI
    // compatibility with existing callers; PinnedSnapshot inherits from Snapshot.
    [[nodiscard]] std::shared_ptr<Snapshot> snapshot_view();

    // Replace the entire engine state with `sorted_entries` (key-sorted, including
    // tombstones) at `at_slot`, used by snapshot import. Clears L0/L1 and rebuilds
    // a fresh tree, then sets last_applied_slot = at_slot. Serialized against other
    // writers by write_mutex_. Concurrent lock-free readers are **safe** (#13): the
    // old tree is epoch-retired, not freed, so a reader mid-walk keeps its pages
    // under its guard (it may observe a transient empty/partly-replaced tree — a
    // consistent snapshot swap via a pinned RootVersion is a later refinement).
    Status install_snapshot(std::vector<leaf_entry> sorted_entries, uint64_t at_slot);

    // plan-tree #16: native snapshot format. `collect_native_frames` walks
    // the reachable tree (root -> inner children -> leaf overflow chains,
    // folding any delta chain into a fresh consolidated base first -- same
    // side effect `snapshot()` already has, unlike the read-only
    // `snapshot_view()`) and returns every base/overflow page's *raw frame
    // bytes* verbatim, tagged with its PID -- no cell decode, no tuple
    // encoding. `install_snapshot_native` is the inverse: installs each
    // frame directly as a resident page under its original PID (via
    // `from_frame_copy`, the same reconstruction demand-load already uses),
    // no entry-by-entry tree rebuild. Both intended for crowdb-tree-to-crowdb-tree
    // transfer (Raft InstallSnapshot); `install_snapshot`/`snapshot_view`'s
    // portable tuple format remains available for cross-engine scenarios
    // and testing (comparable against a non-crowdb-tree oracle).
    Status collect_native_frames(std::vector<NativeFrame> *out, uint64_t *out_root_page_id, uint64_t *out_at_slot);
    Status install_snapshot_native(std::vector<NativeFrame> frames, uint64_t root_page_id, uint64_t at_slot);

    // Wipe every key/value and reset watermarks back to a fresh, empty tree
    // (the same wipe `install_snapshot` performs on the live tree before
    // loading imported entries, factored out for a caller that wants an
    // empty tree with nothing to load afterward -- e.g. resetting a
    // diverged/corrupted replica in place before a snapshot import).
    // Serialized against other writers by write_mutex_; concurrent
    // lock-free readers are safe (#13), same as `install_snapshot`. Not
    // durable by itself -- an explicit `snapshot()`/`flush()` afterward is
    // required to persist the wipe to a file-backed store.
    Status clear();

    // Reassemble a large value spilled into an overflow chain headed at `head_page_id`
    // (PT11). Walks the chain via resident under the caller's read epoch guard.
    [[nodiscard]] std::string assemble_overflow_value(uint64_t head_page_id, uint64_t total_len) const;

    [[nodiscard]] uint64_t last_applied_slot() const
    {
        return last_applied_slot_.load();
    }

    [[nodiscard]] uint64_t contiguous_slot() const
    {
        return contiguous_slot_.load();
    }

    [[nodiscard]] uint64_t version() const
    {
        return version_.load();
    }

    [[nodiscard]] uint64_t root_page_id() const
    {
        return root_page_id_.load();
    }

    // Latched media-fault flag (design follow-up). A demand-load that fails to
    // read or validate a durable page (`resident`) cannot return an error through
    // the lock-free read path, so it latches this flag (and the page reads as a
    // miss). A caller can poll this after reads to detect on-disk corruption /
    // I/O faults and fail the node out of the group. `clear_io_error` resets it.
    [[nodiscard]] bool io_failed() const
    {
        return io_failed_.load();
    }

    void clear_io_error()
    {
        io_failed_.store(false);
    }

    // Diagnostics: total entries across every live MemTable (active_ + any
    // not-yet-drained frozen_ buffers), not just active_.
    [[nodiscard]] size_t memtable_count() const;

    MappingTable &mapping()
    {
        return mapping_;
    }

    // Diagnostics/tests: the tree-owned epoch manager (plan-tree #7).
    EpochManager &epoch()
    {
        return epoch_;
    }

    [[nodiscard]] const BufferPool *buffer_pool() const
    {
        return pool_.get();
    }

    [[nodiscard]] int    height() const;     // 1 = single-leaf root
    [[nodiscard]] size_t leaf_count() const; // live leaves reachable from the root

    // # of base pages physically written by the most recent snapshot (the rest
    // were clean and retained their durable addr). For incremental-snapshot tests.
    [[nodiscard]] uint64_t last_snapshot_pages_written() const
    {
        return snapshot_pages_written_.load();
    }

    // # of mapping-table segment images physically written by the most
    // recent snapshot (plan-tree #14c/#14d: the rest were !is_dirty() and
    // reused their existing image_addr/generation as-is). For
    // incremental-snapshot tests -- the segment-level analogue of
    // last_snapshot_pages_written().
    [[nodiscard]] uint64_t last_snapshot_segments_written() const
    {
        return snapshot_segments_written_.load();
    }

    // Batched diagnostics snapshot -- see EngineStats. O(1): every field
    // reads an already-tracked atomic counter or BufferPool::stats() (also
    // O(1)), so this is safe to poll periodically (e.g. from a metrics
    // scrape or console panel refresh).
    [[nodiscard]] EngineStats stats() const;

    // Destructive read of the per-step scan profile since the last call: flushes
    // the scan LatencySummary/Counter handles and returns per-step sum/max/avg.
    // Returns an all-zero profile if init_metrics() was never called.
    [[nodiscard]] ScanProfile scan_profile() const;

    // Create the internal MetricsRegistry and register all handles
    // using the provided name prefix (e.g. "s.1.g.0"). Called from open().
    void init_metrics(const std::string &prefix);

    // Flush all C++ metrics into a formatted string (for FFI return to
    // Rust). Uses open_memstream internally. `width` overrides the
    // per-section max name length for column alignment with the Rust
    // section (0 = use internal max).
    std::string flush_metrics_str(double window_secs, const char *timestamp, size_t width = 0, size_t count_w = 0,
                                  size_t tps_w = 0);

    // Return the current max metric name length (for Rust's shared-width
    // computation).
    size_t max_name_len() const;

    // Evict clean, delta-free resident leaf bases down to at most
    // `max_resident_leaves`, re-tagging their slots unloaded and epoch-retiring the
    // pages; returns the number evicted. Safe against lock-free
    // readers (epoch-deferred frame reuse); evicted pages reload on next access.
    [[nodiscard]] size_t evict_clean_leaves(size_t max_resident_leaves);

    // plan-tree #17 D3: same contract as evict_clean_leaves, but for clean,
    // delta-free resident *inner* bases, ranked and budgeted entirely
    // separately -- see evict_clean_inner_locked's doc comment (crowdb-tree.cpp)
    // for why a combined leaf+inner budget is unsafe (breaks the
    // just-touched-leaf-survives-eviction guarantee). Never evicts a leaf;
    // evict_clean_leaves never evicts an inner base.
    [[nodiscard]] size_t evict_clean_inner(size_t max_resident_inner);

    // Effective key size limit (opt_.max_key_size or frame_bytes/2). Keys larger
    // than this are rejected at apply() (plan-tree #15).
    [[nodiscard]] size_t max_key_size() const
    {
        return opt_.max_key_size != 0 ? opt_.max_key_size : opt_.frame_bytes / 2;
    }

  private:
    // apply a batch's ops into L0 at `slot` (intra-batch last-op-wins).
    void apply_batch(uint64_t slot, const Batch &batch);
    // Shared apply()/apply_encoded() tail: slot bookkeeping (max_seen_slot_,
    // received_slots_, contiguous frontier) then a possible L0 size-based swap.
    void note_applied_slot(uint64_t slot);
    // Fold newly received slots into the contiguous prefix, then prune the
    // tracker below the new frontier. Caller holds slot_mutex_.
    void recompute_contiguous_locked();

    // -- MemTable double buffering (plan-tree #3); see the active_/frozen_
    // member comment below for the full design. --
    // Snapshot the current active_ pointer (shared_lock on memtable_mutex_;
    // O(1), just bumps the shared_ptr refcount).
    [[nodiscard]] std::shared_ptr<MemTable> current_active() const;
    // Snapshot every live MemTable (frozen_ oldest-first, then active_ last)
    // as a list of shared_ptrs so get()/scan() can read them after releasing
    // memtable_mutex_ -- the shared_ptrs keep each table alive even if it is
    // concurrently drained-to-empty-and-dropped from frozen_ by a flush() on
    // another thread (drain empties a table's *contents*; it does not free
    // the MemTable object out from under a reader still holding a ref).
    [[nodiscard]] std::vector<std::shared_ptr<MemTable>> all_memtables() const;
    // If active_ meets the size/entry threshold (or `force`), freeze it
    // (push onto frozen_) and install a fresh active_. `force` also bypasses
    // the max_memtable_count cap on the frozen_ queue depth (flush() always
    // needs to freeze+drain whatever is pending, regardless of size) but
    // still no-ops on an empty active_. Returns true if a freeze happened.
    bool maybe_freeze_active(bool force);
    // Threshold-triggered swap only (no drain) -- called after every
    // apply()/force_advance_slot(). Draining is the separate, explicit job
    // of flush() (background thread or caller-invoked).
    void maybe_swap_active();
    // Drain `mt`'s slot <= cs eligible entries into L1 via the normal
    // per-leaf delta-append path (same mechanism regardless of which
    // MemTable -- active_ or a frozen_ entry -- they came from). Returns
    // true if anything was written. Caller holds write_mutex_.
    bool drain_memtable_into_l1_locked(MemTable *mt, uint64_t cs);
    // Snapshot import (install_snapshot): drop every live MemTable's content
    // and install one fresh, empty active_. Caller holds write_mutex_.
    void reset_memtables_locked();

    void consolidate_locked(uint64_t page_id);          // caller holds write_mutex_
    void maybe_split_or_merge_locked(uint64_t page_id); // dispatch on leaf size
    // Inner PIDs from root down to (but excluding) the leaf `target_page_id`.
    std::vector<uint64_t> path_to_page_id_locked(uint64_t target_page_id) const;
    void                  split_leaf_locked(uint64_t leaf_page_id, std::vector<uint64_t> path);
    void                  propagate_split_locked(std::vector<uint64_t> path, uint64_t child_page_id, std::string sep,
                                                 uint64_t right_page_id);
    void                  try_merge_leaf_locked(uint64_t leaf_page_id, const std::vector<uint64_t> &path);
    // Merge an underfull non-root inner page with its left sibling (mirrors leaf
    // merge), recursing up; collapses the root when it drops to a single child.
    // `path` is the inner PIDs from root down to (but excluding) `inner_page_id`.
    void try_merge_inner_locked(uint64_t inner_page_id, std::vector<uint64_t> path);

    // Separator-count threshold below which a non-root inner page is merged.
    [[nodiscard]] uint32_t inner_merge_keys() const
    {
        if (opt_.inner_merge_keys != 0) {
            return opt_.inner_merge_keys;
        }
        uint32_t q = opt_.inner_max_keys / 4;
        return q != 0 ? q : 1;
    }

    void retire_page(PageBase *p);
    // Retire a page that becomes entirely unreachable by new readers with no
    // replacement under its own PID (a merged-away leaf/inner, or a
    // root-collapse's old root) -- as opposed to retire_page()'s usual
    // "superseded by a fresh mapping_.store() under the same page_id"
    // pattern, which needs no slot update at all. Clears `page_id`'s mapping
    // slot to empty *inside the epoch deleter*, at the same deferred point
    // `p` itself becomes safe to delete (plan-tree #14b/mapping-table design
    // §6's "slot clearing runs in the epoch deleter") -- not immediately,
    // which would race a straggler reader still walking in via a stale
    // parent reference from before this retirement (see this method's call
    // sites for the full argument on why the deferred point is race-free).
    // Without this, the PID's slot keeps a dangling pointer once `p` is
    // freed -- harmless for the old root-walk-based snapshot (which only
    // ever visits tree-reachable PIDs) but a use-after-free for #14c/#14d's
    // segment-scan-driven snapshot, which reads every slot directly.
    void retire_orphaned_page(uint64_t page_id, PageBase *p);
    // Recursively drop a subtree. `retire=false` frees pages immediately (teardown
    // / no concurrent readers). `retire=true` epoch-retires each page and overflow
    // chain and clears its mapping slot, so a lock-free reader still holding a page
    // under its guard is never freed underneath it (used by install_snapshot on the
    // live tree). Caller holds write_mutex_ for the retire path.
    //
    // CAUTION: this is a top-down, root->children walk -- it bails out (nothing to
    // do) the moment it reaches an *unloaded* slot, which used to be a safe
    // assumption (a leaf has no descendants, and only leaves were ever
    // independently evictable) but no longer is now that plan-tree #17 D3's
    // evict_clean_inner can leave an *inner* page unloaded while a resident
    // descendant remains fully live underneath it -- that descendant would be
    // silently skipped (leaked, for retire=false; never epoch-retired, for
    // retire=true) by a call rooted above it. Only ever call this on a page_id
    // known to still be resident (e.g. persist.cpp's freshly-built, never-evicted
    // empty root during open()); everywhere else, use free_all_resident_pages.
    void free_subtree(uint64_t page_id, bool retire);
    // Drop *every* resident page, regardless of tree reachability: enumerates
    // MappingTable's present segments/slots directly (same technique
    // persist.cpp's snapshot uses to discover dirty pages without a
    // reachable-page walk -- see prepare_snapshot_locked's header comment)
    // instead of a top-down root->children walk, so it cannot miss a resident
    // page merely because one of its *ancestors* happens to be unloaded (see
    // free_subtree's caution above). Same retire=false/true contract as
    // free_subtree otherwise. Caller holds write_mutex_ for the retire path.
    void free_all_resident_pages(bool retire);

    // Effective overflow spill threshold (opt_.max_inline_value or frame_bytes/4).
    [[nodiscard]] size_t max_inline_value() const
    {
        return opt_.max_inline_value != 0 ? opt_.max_inline_value : opt_.frame_bytes / 4;
    }

    // Fold a leaf chain (deltas + base) to key-sorted storage entries by
    // highest-slot-wins, dropping tombstones with slot <= gc_floor. Overflow
    // pointer cells are carried forward unchanged; any overflow chain that a
    // higher-slot write supersedes is appended to *dead_overflow (if non-null) so
    // the caller can retire it. If out_tombstones_dropped/out_bytes_dropped are
    // non-null, they are set to the number of tombstones dropped and their total
    // key+cell byte size (collect_garbage() uses the count to skip rebuilding a
    // leaf that has nothing to reclaim, and the bytes for GcStats::bytes_freed --
    // the resident *frame* size is a poor proxy since pool-backed frames are
    // fixed-size regardless of live content). Caller holds write_mutex_.
    [[nodiscard]] static std::vector<leaf_entry>
    resolve_leaf_chain_for_rebuild(PageBase *head, uint64_t gc_floor, std::vector<uint64_t> *dead_overflow,
                                   size_t *out_tombstones_dropped = nullptr, size_t *out_bytes_dropped = nullptr);
    // Spill `value` into a fresh overflow page chain; returns the head PID. Caller
    // holds write_mutex_.
    [[nodiscard]] uint64_t spill_value_to_overflow_chain_locked(const std::string &value);
    // build a leaf base from storage entries, spilling any inline value larger
    // than max_inline_value() into an overflow chain and replacing it with a pointer
    // cell. Entries already in overflow-pointer form are carried forward as-is.
    [[nodiscard]] LeafBase *build_leaf_spilling_locked(std::vector<leaf_entry> entries, uint64_t right_sibling);
    // Epoch-retire an overflow chain (a superseded large value). Caller holds
    // write_mutex_.
    void retire_overflow_chain_locked(uint64_t head_page_id);
    // Evict an overflow chain alongside its owning leaf: re-tag each resident,
    // clean overflow page unloaded and epoch-retire it (it demand-loads on next
    // access). Stops at the first already-unloaded/dirty link (chains evict whole,
    // so the tail is already unloaded). Caller holds write_mutex_.
    void evict_overflow_chain_locked(uint64_t head_page_id);
    // Immediately free an overflow chain's resident pages (teardown / clear; no
    // concurrent readers). Caller holds write_mutex_.
    void   free_overflow_chain(uint64_t head_page_id);
    size_t evict_clean_leaves_locked(size_t max_resident_leaves); // caller holds write_mutex_
    size_t evict_clean_inner_locked(size_t max_resident_inner);   // caller holds write_mutex_
    void   maybe_evict_locked(); // capacity-driven auto-evict (caller holds write_mutex_)
    // Resolve a PID to its resident chain head, demand-loading an unloaded slot
    // (demand-load). Hot (resident) path is lock-free; the cold path locks
    // load_mutex_ and double-checks. Returns nullptr if the slot is unset.
    [[nodiscard]] PageBase *resident(uint64_t page_id) const;

    // R6: capture all pages in an overflow chain (for PinnedSnapshot pinning).
    void capture_overflow_chain(uint64_t head_page_id, std::vector<PageBase *> &out);

    // Shared by resident()'s synchronous cold path and get_async's async
    // completion handler: decodes+validates a
    // just-read durable blob and installs it as the resident page for
    // `page_id`, exactly like resident()'s cold path did inline before this
    // was factored out. Returns the installed page, or nullptr on a
    // decode/CRC/validation failure (io_failed_ is latched first, matching
    // resident()). Caller holds load_mutex_ and has already re-verified the
    // slot is still tagged unloaded (double-checked locking, same
    // requirement resident()'s cold path has).
    [[nodiscard]] PageBase *install_loaded_page(uint64_t page_id, uint64_t addr, uint32_t plen,
                                                const std::vector<uint8_t> &blob) const;

    // One attempt at get_view()'s L0-then-L1 resolution, but NEVER performs
    // I/O: a mapping slot tagged unloaded aborts the attempt immediately
    // (releasing the epoch guard first, since a genuine miss is about to
    // hand off to the reactor or fall back to a blocking load -- either way
    // this attempt is over) instead of demand-loading it, and reports which
    // page_id blocked via *out_pending_page_id. get_async's orchestration
    // (get_async_attempt) re-verifies that page_id under load_mutex_ before
    // touching its unloaded slot-word descriptor -- see the safety note on
    // get_async_attempt's definition (mirrors resident()'s double-checked
    // lock; the descriptor is inline in the atomic word, not epoch-protected,
    // so it is never safe to unpack outside load_mutex_).
    //
    // Returns true if the attempt fully resolved (found or definitively not
    // found) -- `*result` is populated exactly like get_view() would.
    // Returns false on a genuine miss -- `*result` must be ignored.
    [[nodiscard]] bool try_get_view_no_load(Slice key, GetView *result, uint64_t *out_pending_page_id) const;

    // get_async's retry loop: one try_get_view_no_load() attempt, then
    // either calls on_done (resolved) or resolves the blocked page_id (via
    // the reactor, or synchronously if no async backend is wired) and
    // recurses. `key_owned` is a heap copy of the lookup key -- unlike
    // get_view()'s Slice (borrowed, valid only for one synchronous call),
    // get_async's key must survive across an arbitrary number of async
    // round trips, each on a different call stack.
    //
    // `same_thread`: true iff this specific
    // attempt is guaranteed to resolve (if it resolves at all) on the same
    // thread that will eventually call ct_future_free -- i.e. every call
    // except the one made from inside the io_uring completion callback
    // below, which runs on the Reactor's own thread. Threaded through every
    // recursive call so it stays correct across an arbitrary number of
    // hops. Guards whether a resolved GetView's epoch guard may be
    // deferred (zero-copy) or must be released immediately via
    // materialize_owned() -- see EpochManager::Guard's "do not move across
    // threads" contract.
    void get_async_attempt(std::shared_ptr<std::string> key_owned, std::function<void(GetView)> on_done,
                           bool same_thread) const;

    // Converts a resolved GetView into a fully-owned copy with its epoch
    // guard already released on the calling thread -- safe to hand off to
    // a different thread afterward (get_async_attempt's io_uring
    // completion path). A borrowed L1 hit is materialized via a fresh
    // buffer::copy_of(); an already-owned (L0/overflow) or not-found
    // GetView is untouched except for releasing the guard.
    static GetView materialize_owned(GetView &&v);

    // scan()'s non-blocking twin: identical logic (same L0 snapshot, same
    // right_sibling leaf walk, same merge), but the initial descent and
    // every leaf probe use a non-blocking check (mirrors
    // try_get_view_no_load's `probe`) instead of resident()'s demand-load.
    // The moment *any* page along the way is unloaded, bails out
    // immediately (discarding whatever was collected into *out so far --
    // scan_async_attempt retries the whole call once that page resolves)
    // and reports it via *out_pending_page_id. Returns true if the scan
    // fully resolved with no cold page encountered (*out/*truncated are
    // then exactly what scan() itself would have produced).
    [[nodiscard]] bool try_scan_no_load(Slice prefix, Slice start_after, Slice end_key, size_t limit,
                                        size_t byte_budget, bool keys_only, uint64_t deadline_ms,
                                        std::vector<scan_entry> *out, bool *truncated, uint64_t *out_pending_page_id,
                                        ScanPackedBuf *out_packed = nullptr, size_t *out_count = nullptr) const;

    // scan_async's retry loop, structurally identical to get_async_attempt:
    // one try_scan_no_load() attempt, then either calls on_done (resolved)
    // or resolves the one blocking page_id (via the reactor, or
    // synchronously if no async backend is wired) and recurses. `prefix`,
    // `start_after`, and `end_key` are heap copies (unlike scan()'s Slice,
    // must survive across an arbitrary number of async round trips). Entries
    // resolved before the cold leaf are accumulated across retries and the
    // last resolved key becomes the resume `start_after`, so a scan over N
    // cold leaves performs O(N) leaf loads with no re-traversal of already-
    // resolved leaves (was quadratic). `byte_budget` is the remaining total
    // key+value byte cap (adjusted by entries already in `accumulated`).
    void scan_async_attempt(std::shared_ptr<std::string>        prefix_owned,
                            const std::shared_ptr<std::string> &start_after_owned,
                            const std::shared_ptr<std::string> &end_key_owned, size_t limit, size_t byte_budget,
                            bool keys_only, uint64_t deadline_ms, std::shared_ptr<ScanPackedBuf> accumulated,
                            std::shared_ptr<std::string> last_key, size_t accumulated_count,
                            std::function<void(Status, ScanPackedBuf, bool)> on_done) const;

    // Shared by snapshot() and snapshot_async() (persist.cpp,
    // #11 Phase 2, #14c/#14d): runs the segment scan / delta-fold /
    // page+segment-image+directory+anchor encode that snapshot() used to do
    // inline, but defers every actual write into the returned *out instead
    // of calling opt_.page_store->write_at() itself -- caller holds
    // write_mutex_ for just this call, exactly like every other writer
    // entry point (see snapshot_async's doc comment for the full
    // lock-discipline rationale).
    // Deliberately does *not* set PageBase::durable_addr for a dirty page
    // it persists (unlike the pre-refactor inline version): that would let
    // evict_clean_leaves_locked() -- which only takes write_mutex_, not
    // snapshot_inflight_ -- evict a page whose bytes aren't durable yet on
    // the async path, and a subsequent demand-load would then read
    // whatever garbage/stale content actually occupies that address today.
    // commit_prepared_snapshot() sets it instead, only once each page's
    // specific write has actually landed.
    Status prepare_snapshot_locked(PreparedSnapshot *out);

    // Marks every PreparedSnapshot::page_writes/segment_writes entry
    // durable (see prepare_snapshot_locked's doc comment) and publishes the
    // new version, once every byte of this generation is confirmed on disk.
    // Re-resolves each page_id fresh under write_mutex_ and checks identity
    // before touching it -- see PreparedPageWrite/PreparedSegmentWrite's
    // doc comments for why a mismatch (skip, not an error) is possible and
    // safe.
    void commit_prepared_snapshot(const PreparedSnapshot &prepared);

    // Cross-thread-safe (unlike write_mutex_) spin-gate serializing this
    // snapshot generation's prepare-through-commit sequence against a
    // second overlapping snapshot(_async) call; see snapshot_async's doc
    // comment. Acquired by snapshot()/snapshot_async() before
    // prepare_snapshot_locked(), released after commit_prepared_snapshot()
    // (success) or the first failing step (error) -- from whichever thread
    // that happens to be, which is exactly why this is an atomic spin-gate
    // and not a std::mutex.
    void acquire_snapshot_slot();
    void release_snapshot_slot();

    // snapshot_async's write-and-commit chain: writes
    // prepared->page_writes[idx..] then prepared->segment_writes[idx..]
    // one at a time (recursing on each completion), then the directory,
    // then a durability barrier, then the anchor, then a second barrier,
    // then commit_prepared_snapshot() + release_snapshot_slot(), then
    // fires on_done -- the exact sequence snapshot() runs inline, just
    // each I/O step dispatched through opt_.async_page_store instead of
    // blocking.
    void snapshot_write_next_async(std::shared_ptr<PreparedSnapshot> prepared, size_t idx,
                                   std::function<void(Status, uint64_t last_applied)> on_done);

    Options opt_;
    // Human-readable engine label for CT_LOG context (e.g. "s1.g1").
    // Copied from opt_.name at construction; empty means "[unnamed]".
    std::string name_;
    // Base-page frame arena. shared_ptr because epoch-retired pages
    // co-own it; the tree-owned EpochManager (epoch_, declared last so it is
    // destroyed first) reclaims those pages before pool_ is destroyed. Declared
    // before mapping_ so it is destroyed after the pages it backs.
    std::shared_ptr<BufferPool> pool_;
    MappingTable                mapping_;

    // -- MemTable double buffering (plan-tree #3) --
    //
    // active_ is the single MemTable that apply()/apply_batch() writes land
    // in. Once it crosses opt_.memtable_flush_bytes/_entries,
    // maybe_swap_active() freezes it (pushes it onto frozen_, no longer
    // reachable for new writes) and installs a fresh, empty MemTable as
    // active_ -- a fast, memtable_mutex_-only pointer swap, decoupled from
    // the (potentially much slower) B+tree drain. frozen_ holds zero or more
    // frozen, write-closed MemTables awaiting drain into L1, oldest first;
    // it normally holds at most one entry (drained to empty by the very next
    // flush() call) but can hold up to (opt_.max_memtable_count - 1) if
    // writes keep tripping the threshold faster than flush() (explicit call
    // or the background thread) drains them -- see max_memtable_count's
    // comment in options.h for the capacity/back-pressure behavior.
    //
    // Both members are guarded by memtable_mutex_, but *only* for the
    // pointer/queue values themselves -- MemTable has its own internal
    // mutex, so once a caller has copied out a shared_ptr<MemTable> (via
    // current_active()/all_memtables()) it reads/writes/drains that table
    // without holding memtable_mutex_ at all. This means apply_batch()
    // (writer) and get()/scan() (lock-free readers, epoch-guarded) never
    // contend with each other OR with an in-progress flush() drain on a
    // *different* table -- the concurrency benefit double buffering is for.
    //
    // Read-side correctness (get()/scan()): because slots can arrive
    // out of order (a Paxos-style caller may apply() a higher slot before a
    // lower one that fills an earlier gap), the SAME key can legitimately be
    // resident in more than one live MemTable at once with *different*
    // slots when a freeze happens to land between two out-of-order writes to
    // that key. Unlike the pre-#3 single-buffer design (where upsert()'s
    // highest-slot-wins dedup made "the" MemTable hit unambiguous), reads
    // must check every live table (active_ + all of frozen_, any order) and
    // keep the highest-slot cell -- see get()'s and scan()'s implementation.
    // Every live table's cell for a key is still guaranteed strictly newer
    // than L1's (each table's durable_floor_ rejects writes for slots
    // already folded into L1), so a hit in any live table never needs an L1
    // fallback.
    //
    // Write-side correctness (flush()/drain_memtable_into_l1_locked()): a
    // drained key is appended to its target leaf's delta chain, never
    // written in place, and every reader of that chain (resolve_chain /
    // resolve_chain_sorted) resolves highest-slot-wins across the *whole*
    // chain regardless of append order -- so draining two frozen tables that
    // happen to hold different slots for the same key, in either order, is
    // safe: L1 always converges to the higher slot once both are drained
    // (see resolve_chain's header comment in delta.h).
    //
    // Non-contiguous slots (documented per an explicit design requirement --
    // do not lose track of this): when a frozen table is drained
    // (drain_up_to(cs)), any entries with slot > cs are stuck behind a gap
    // that hasn't become contiguous yet and are NOT written to L1. Rather
    // than leaving that frozen table sitting half-drained in the queue
    // indefinitely (which would both leak a MemTable object and prevent the
    // queue from ever shrinking back down), flush() extracts those leftover
    // entries and re-upserts them into the *current* active_ MemTable, then
    // discards the now-fully-vacated frozen table. upsert()'s highest-slot-
    // wins makes this safe even if active_ has independently received a
    // newer (or, for that matter, older) write for the same key in the
    // meantime. The relocated entries simply ride along in whichever table
    // is active_ until a later flush() (once contiguous_slot_ has advanced
    // past their slot) finally drains them for real -- they may bounce
    // through several freeze/relocate cycles under a sustained out-of-order
    // write pattern, which is expected and bounded by how long the
    // underlying gap stays open, not by this mechanism.
    mutable std::shared_mutex             memtable_mutex_;
    std::shared_ptr<MemTable>             active_;
    std::deque<std::shared_ptr<MemTable>> frozen_;
    std::atomic<uint64_t>                 memtable_next_id_{1}; // monotonic MemTable id for logging

    // internal_error slot tracker (replaces the caller-supplied contiguous_slot). Holds
    // received-but-not-yet-contiguous slots above contiguous_slot_; the contiguous
    // prefix is folded forward on each apply/force_advance_slot and pruned below
    // the frontier to stay bounded. Guarded by slot_mutex_.
    mutable std::mutex    slot_mutex_;
    std::set<uint64_t>    received_slots_;
    uint64_t              max_seen_slot_ = 0;
    std::atomic<uint64_t> auto_slot_{0}; // next auto-assigned slot for put/del/batch_put

    std::atomic<uint64_t>     root_page_id_{kInvalidPageId};
    std::atomic<uint64_t>     contiguous_slot_{0};
    std::atomic<uint64_t>     last_applied_slot_{0};
    std::atomic<uint64_t>     version_{0};
    std::atomic<uint64_t>     gc_floor_{0};
    std::atomic<uint64_t>     snapshot_pages_written_{0};    // pages written by last snapshot
    std::atomic<uint64_t>     snapshot_pages_total_{0};      // cumulative pages written across all snapshots
    std::atomic<uint64_t>     snapshot_segments_written_{0}; // segment images written by last snapshot
    mutable std::atomic<bool> io_failed_{false};             // latched demand-load media fault

    // Cumulative operation counters (monotonic since open, exposed via stats()).
    // mutable: get_view() is const but increments these counters.
    mutable std::atomic<uint64_t> mt_upsert_total_{0};     // apply() writes into L0
    mutable std::atomic<uint64_t> mt_get_total_{0};        // get() lookups in L0
    mutable std::atomic<uint64_t> mt_get_hit_total_{0};    // L0 lookups that found a cell
    mutable std::atomic<uint64_t> flush_drain_total_{0};   // drain_memtable_into_l1 calls
    mutable std::atomic<uint64_t> flush_entries_total_{0}; // entries drained from L0 to L1
    mutable std::atomic<uint64_t> snapshot_total_{0};      // snapshot() calls (durable checkpoints)
    mutable std::atomic<uint64_t> l1_get_total_{0};        // get() lookups that descended to L1
    mutable std::atomic<uint64_t> l1_get_hit_total_{0};    // L1 lookups that found a cell
    mutable std::atomic<uint64_t> map_lookup_total_{0};    // mapping table lookups (find_leaf_page_id / resident)
    mutable std::atomic<uint64_t> demand_load_total_{0};   // demand-load page faults

    // Logical clock for CLOCK-informed eviction ranking (plan-tree #17).
    // `resident()`'s hot path bumps this and stamps the touched page's own
    // `PageBase::last_touch_tick` on every access (a single relaxed atomic
    // fetch_add + store -- no lock, so the existing lock-free read path
    // stays lock-free). `evict_clean_leaves_locked` then ranks its
    // DFS-gathered evictable set by that stamp (oldest first) instead of
    // arbitrary DFS order. This is deliberately *not* `BufferPool::pin`'s
    // own mutex-guarded page_id/CLOCK tracking: wiring every `resident()`
    // hit through that would mean taking a global pool mutex on every page
    // access, regressing the lock-free read path #5 B3/#12/#13 built (see
    // "residency/eviction driven by real access recency, not arbitrary
    // order" goal without that cost.
    mutable std::atomic<uint64_t> touch_tick_{0};

    mutable std::mutex write_mutex_; // serializes flush / consolidate / split-merge
    mutable std::mutex load_mutex_;  // serializes cold-path demand loads
    // Serializes snapshot(_async) generations against each other across
    // snapshot_async's async write phase, where write_mutex_ itself can't be
    // held (see acquire_snapshot_slot's doc comment and snapshot_async's).
    std::atomic<bool> snapshot_inflight_{false};

    // Block indices that were empty in the previous snapshot (two-generation
    // rule for block deletion). A block is only deleted after it's empty in
    // two consecutive snapshots — the crash fallback anchor still references it.
    std::set<uint32_t> prev_empty_blocks_;

    // Tree-owned epoch-based reclamation (plan-tree #7; formerly on CrowdbtreeEnv).
    // Declared last so it is destroyed first: ~Crowdbtree frees the live tree via
    // free_subtree(root, /*retire=*/false) (no readers at teardown), then epoch_'s
    // destructor reclaims any pages still pending from earlier retire()s (eviction,
    // consolidation, install_snapshot) while pool_ / mapping_ are still alive.
    // mutable: readers take a guard in const get().
    mutable EpochManager epoch_;

    // ── Metrics handles (registered in init_metrics) ──
    struct MetricsHandles
    {
        Counter        *buf_hits       = nullptr;
        Counter        *buf_misses     = nullptr;
        Counter        *buf_evictions  = nullptr;
        Counter        *buf_writebacks = nullptr;
        Gauge          *buf_resident   = nullptr;
        Gauge          *buf_dirty      = nullptr;
        LatencySummary *apply_l        = nullptr;
        LatencySummary *snapshot_l     = nullptr;
        LatencySummary *flush_l        = nullptr;
        // MemTable (L0) operation counters
        Counter *mt_upsert_c  = nullptr;
        Counter *mt_get_c     = nullptr;
        Counter *mt_get_hit_c = nullptr;
        // Flush (L0 → L1) counters
        Counter *flush_drain_c   = nullptr;
        Counter *flush_entries_c = nullptr;
        // L1 (B-tree) query counters
        Counter *l1_get_c     = nullptr;
        Counter *l1_get_hit_c = nullptr;
        // B+tree page mutation counters (during drain/split/merge/consolidate)
        Counter        *page_write_c = nullptr;
        LatencySummary *page_write_l = nullptr;
        // Mapping table lookup counter
        Counter *page_map_lookup_c = nullptr;
        // Demand-load (page fault I/O) counter + latency
        Counter        *demand_load_c = nullptr;
        LatencySummary *demand_load_l = nullptr;
        // Snapshot sub-metrics (new)
        LatencySummary *snapshot_apply_l            = nullptr; // prepare_snapshot_locked latency
        LatencySummary *snapshot_page_write_l       = nullptr; // per-page write_at latency
        Counter        *snapshot_page_write_cache_c = nullptr; // clean pages (no write)
        Bandwidth      *snapshot_page_write_bw      = nullptr; // per-page write bytes
        Bandwidth      *snapshot_meta_write_bw      = nullptr; // metadata write bytes (seg+dir+anchor)
        Bandwidth      *page_read_bw                = nullptr; // demand-load read bytes
        Counter        *snapshot_pages_c            = nullptr; // cumulative pages written
        // Scan per-step profile: counters + per-step LatencySummary.
        Counter        *scan_c             = nullptr; // scan calls
        Counter        *scan_entries_c     = nullptr; // entries returned
        LatencySummary *scan_l             = nullptr; // total scan latency
        LatencySummary *scan_l0_snapshot_l = nullptr;
        LatencySummary *scan_l0_skip_l     = nullptr;
        LatencySummary *scan_l1_descent_l  = nullptr;
        LatencySummary *scan_l1_resolve_l  = nullptr;
        LatencySummary *scan_merge_l       = nullptr;
    };

    MetricsHandles metrics_;

    // Internal metrics registry (owned by the engine).
    std::unique_ptr<MetricsRegistry> metrics_registry_;
};

} // namespace crowdb::tree
