// Crowtree: one ordered, single-version-per-key store per consensus group.
// Two-level write path: apply() lands in the MemTable
// (L0); flush() merges the contiguous-applied prefix into the COW B+tree (L1).
#pragma once

#include "crowtree/cell.h"
#include "crowtree/epoch.h"
#include "crowtree/mapping_table.h"
#include "crowtree/memtable.h"
#include "crowtree/options.h"
#include "crowtree/page.h"
#include "crowtree/snapshot.h"
#include "crowtree/status.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

namespace crowtree
{

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
};

struct get_result
{
    bool        found = false;
    uint64_t    slot  = 0;
    std::string value;
};

// Result of an explicit collect_garbage() sweep (plan-tree #21).
struct GcStats
{
    uint64_t tombstones_dropped = 0; // tombstone cells physically dropped
    uint64_t pages_freed        = 0; // resident pages (deltas + old leaf bases) retired
    uint64_t bytes_freed        = 0; // logical key+cell bytes of the dropped tombstones
};

class Crowtree
{
  public:
    explicit Crowtree(Options opt = Options());
    ~Crowtree();

    Crowtree(const Crowtree &)            = delete;
    Crowtree &operator=(const Crowtree &) = delete;

    // open a tree, recovering durable state from opt.page_store if a valid
    // snapshot exists; otherwise start empty. Requires opt.page_store != null.
    static Status open(const Options &opt, std::unique_ptr<Crowtree> *out);

    // Persist the materialized L1 state durably. Folds delta chains, appends the
    // reachable base pages + a manifest past the current end of the page store,
    // then commits the inactive A/B superblock slot. Returns the durable
    // last_applied_slot via out (if non-null). Requires opt.page_store != null.
    Status snapshot(uint64_t *out_last_applied = nullptr);

    // Ingest a batch at `slot`. The tree internally tracks received slots and
    // computes the contiguous prefix (how far the flusher may flush) itself, so
    // callers no longer pass Paxos/learner state. Lands in L0; may trigger a
    // size-based flush. For a slot with no data (a NoOp), call force_advance_slot.
    Status apply(uint64_t slot, const Batch &batch);

    // Advance the contiguous frontier up to `slot`, filling any intervening slots
    // as NoOps (e.g. after learner NoOp slots or during restore). Explicit and
    // free of learner jargon.
    void force_advance_slot(uint64_t slot);

    // Convenience methods: auto-assign the next slot (max_seen + 1) and apply.
    // Intended for single-writer use; do not mix with explicit-slot apply calls.
    Status put(Slice key, Slice value);
    Status del(Slice key);
    Status batch_put(const Batch &batch);

    // Logical retention GC watermark (design-crowtree-snapshot-gc.md §1/§4):
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

    // Point read (L0 overlay then L1). Returns true if a live value is found;
    // tombstones return false.
    [[nodiscard]] bool get(Slice key, uint64_t *out_slot, std::string *out_value) const;

    // Batched point read.
    [[nodiscard]] std::vector<get_result> multi_get(const std::vector<Slice> &keys) const;

    // Ordered range scan over keys with `prefix` (empty = whole keyspace), latest
    // state (L0 overlaid on L1), skipping tombstones. Returns up to `limit`
    // entries in key order; sets *truncated if more matched beyond the limit.
    Status scan(Slice prefix, size_t limit, std::vector<scan_entry> *out, bool *truncated) const;

    // pin a consistent point-in-time view at `last_applied_slot` (the durable L1
    // state). Used for scan-at / compare / iter_all / snapshot export.
    [[nodiscard]] std::shared_ptr<Snapshot> snapshot_view();

    // Replace the entire engine state with `sorted_entries` (key-sorted, including
    // tombstones) at `at_slot`, used by snapshot import. Clears L0/L1 and rebuilds
    // a fresh tree, then sets last_applied_slot = at_slot. Serialized against other
    // writers by write_mutex_. Concurrent lock-free readers are **safe** (#13): the
    // old tree is epoch-retired, not freed, so a reader mid-walk keeps its pages
    // under its guard (it may observe a transient empty/partly-replaced tree — a
    // consistent snapshot swap via a pinned RootVersion is a later refinement).
    Status install_snapshot(std::vector<leaf_entry> sorted_entries, uint64_t at_slot);

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

    // Evict clean, delta-free resident leaf bases down to at most
    // `max_resident_leaves`, re-tagging their slots unloaded and epoch-retiring the
    // pages (design §4.6); returns the number evicted. Safe against lock-free
    // readers (epoch-deferred frame reuse); evicted pages reload on next access.
    [[nodiscard]] size_t evict_clean_leaves(size_t max_resident_leaves);

  private:
    // apply a batch's ops into L0 at `slot` (intra-batch last-op-wins).
    void apply_batch(uint64_t slot, const Batch &batch);
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

    // Start the background flush thread if `opt_.background_flush` is set (open-
    // issue fix, plan-tree.md §C). Safe to call at most once; no-op otherwise.
    // Callers that go through a two-phase construction (Crowtree::open()'s
    // construct-then-recover sequence in persist.cpp) must delay this call until
    // *after* recovery finishes, since recovery mutates the freshly-constructed
    // tree directly (no write_mutex_) under a single-threaded assumption.
    void start_background_flush_thread();
    // Background thread body: periodically calls flush() (cheap no-op when L0 is
    // empty; see flush()) so a low/no-write-rate workload still becomes durable-
    // eligible on a timer, not only on the size thresholds. Reuses flush()'s
    // existing write_mutex_/MemTable-mutex_ locking — no new synchronization
    // between this thread and the apply()-driving thread (design note in
    // plan-tree.md Open Issues §C). Also runs collect_garbage() every
    // opt_.gc_interval_ms (plan-tree #21) on this same thread/loop -- no second
    // thread for the periodic GC trigger.
    void background_flush_loop();

    void retire_page(PageBase *p);
    // Recursively drop a subtree. `retire=false` frees pages immediately (teardown
    // / no concurrent readers). `retire=true` epoch-retires each page and overflow
    // chain and clears its mapping slot, so a lock-free reader still holding a page
    // under its guard is never freed underneath it (used by install_snapshot on the
    // live tree). Caller holds write_mutex_ for the retire path.
    void free_subtree(uint64_t page_id, bool retire);

    // Effective overflow spill threshold (opt_.max_inline_value or frame_bytes/4).
    [[nodiscard]] size_t max_inline_value() const
    {
        return opt_.max_inline_value != 0 ? opt_.max_inline_value : opt_.frame_bytes / 4;
    }

    // Effective key size limit (opt_.max_key_size or frame_bytes/2). Keys larger
    // than this are rejected at apply() (plan-tree #15).
    [[nodiscard]] size_t max_key_size() const
    {
        return opt_.max_key_size != 0 ? opt_.max_key_size : opt_.frame_bytes / 2;
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
    void   maybe_evict_locked(); // capacity-driven auto-evict (caller holds write_mutex_)
    // Resolve a PID to its resident chain head, demand-loading an unloaded slot
    // (design §4.5). Hot (resident) path is lock-free; the cold path locks
    // load_mutex_ and double-checks. Returns nullptr if the slot is unset.
    [[nodiscard]] PageBase *resident(uint64_t page_id) const;

    Options opt_;
    // Base-page frame arena (design §4). shared_ptr because epoch-retired pages
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
    std::shared_ptr<MemTable>             active_{std::make_shared<MemTable>()};
    std::deque<std::shared_ptr<MemTable>> frozen_;

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
    std::atomic<uint64_t>     snapshot_pages_written_{0}; // pages written by last snapshot
    mutable std::atomic<bool> io_failed_{false};          // latched demand-load media fault

    mutable std::mutex write_mutex_; // serializes flush / consolidate / split-merge
    mutable std::mutex load_mutex_;  // serializes cold-path demand loads (design §4.5)

    // Background flush thread (Options.background_flush / flush_interval_ms),
    // also driving the periodic collect_garbage() sweep (Options.gc_interval_ms,
    // plan-tree #21). Not started unless opt_.background_flush is set; see
    // start_background_flush_thread(). Joined in ~Crowtree before the tree is
    // torn down.
    std::thread             flush_thread_;
    std::mutex              flush_thread_mu_;
    std::condition_variable flush_thread_cv_;
    std::atomic<bool>       stop_flush_thread_{false};

    // Tree-owned epoch-based reclamation (plan-tree #7; formerly on CrowtreeEnv).
    // Declared last so it is destroyed first: ~Crowtree frees the live tree via
    // free_subtree(root, /*retire=*/false) (no readers at teardown), then epoch_'s
    // destructor reclaims any pages still pending from earlier retire()s (eviction,
    // consolidation, install_snapshot) while pool_ / mapping_ are still alive.
    // mutable: readers take a guard in const get().
    mutable EpochManager epoch_;
};

} // namespace crowtree
