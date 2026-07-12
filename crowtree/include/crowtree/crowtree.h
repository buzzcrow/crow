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
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace crowtree {

// One mutation in a batch. All ops in a batch share the batch's slot.
struct batch_op {
  std::string key;
  OpKind kind;
  std::string value;  // empty for Delete
};

struct Batch {
  std::vector<batch_op> ops;
};

struct scan_entry {
  std::string key;
  uint64_t slot;
  std::string value;
};

struct get_result {
  bool found = false;
  uint64_t slot = 0;
  std::string value;
};

class Crowtree {
 public:
  explicit Crowtree(const Options& opt = Options());
  ~Crowtree();

  Crowtree(const Crowtree&) = delete;
  Crowtree& operator=(const Crowtree&) = delete;

  // open a tree, recovering durable state from opt.page_store if a valid
  // snapshot exists; otherwise start empty. Requires opt.page_store != null.
  static Status open(const Options& opt, std::unique_ptr<Crowtree>* out);

  // Persist the materialized L1 state durably. Folds delta chains, appends the
  // reachable base pages + a manifest past the current end of the page store,
  // then commits the inactive A/B superblock slot. Returns the durable
  // last_applied_slot via out (if non-null). Requires opt.page_store != null.
  Status snapshot(uint64_t* out_last_applied = nullptr);

  // Ingest a batch at `slot`. The tree internally tracks received slots and
  // computes the contiguous prefix (how far the flusher may flush) itself, so
  // callers no longer pass Paxos/learner state. Lands in L0; may trigger a
  // size-based flush. For a slot with no data (a NoOp), call force_advance_slot.
  Status apply(uint64_t slot, const Batch& batch);

  // Advance the contiguous frontier up to `slot`, filling any intervening slots
  // as NoOps (e.g. after learner NoOp slots or during restore). Explicit and
  // free of learner jargon.
  void force_advance_slot(uint64_t slot);

  // Convenience methods: auto-assign the next slot (max_seen + 1) and apply.
  // Intended for single-writer use; do not mix with explicit-slot apply calls.
  Status put(Slice key, Slice value);
  Status del(Slice key);
  Status batch_put(const Batch& batch);

  // Logical retention GC watermark: tombstones with slot <= safe_slot may be
  // dropped during consolidation once consensus no longer needs them.
  void set_gc_watermark(uint64_t safe_slot);
  uint64_t gc_watermark() const { return gc_floor_.load(); }

  // Drain the contiguous-applied prefix of L0 into L1 and publish a new root.
  Status flush();

  // Point read (L0 overlay then L1). Returns true if a live value is found;
  // tombstones return false.
  bool get(Slice key, uint64_t* out_slot, std::string* out_value) const;

  // Batched point read.
  std::vector<get_result> multi_get(const std::vector<Slice>& keys) const;

  // Ordered range scan over keys with `prefix` (empty = whole keyspace), latest
  // state (L0 overlaid on L1), skipping tombstones. Returns up to `limit`
  // entries in key order; sets *truncated if more matched beyond the limit.
  Status scan(Slice prefix, size_t limit, std::vector<scan_entry>* out, bool* truncated) const;

  // pin a consistent point-in-time view at `last_applied_slot` (the durable L1
  // state). Used for scan-at / compare / iter_all / snapshot export.
  std::shared_ptr<Snapshot> snapshot_view();

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
  std::string assemble_overflow_value(uint64_t head_page_id, uint64_t total_len) const;

  uint64_t last_applied_slot() const { return last_applied_slot_.load(); }
  uint64_t contiguous_slot() const { return contiguous_slot_.load(); }
  uint64_t version() const { return version_.load(); }
  uint64_t root_page_id() const { return root_page_id_.load(); }

  // Latched media-fault flag (design follow-up). A demand-load that fails to
  // read or validate a durable page (`resident`) cannot return an error through
  // the lock-free read path, so it latches this flag (and the page reads as a
  // miss). A caller can poll this after reads to detect on-disk corruption /
  // I/O faults and fail the node out of the group. `clear_io_error` resets it.
  bool io_failed() const { return io_failed_.load(); }
  void clear_io_error() { io_failed_.store(false); }

  // Diagnostics.
  size_t memtable_count() const { return memtable_.count(); }
  MappingTable& mapping() { return mapping_; }
  // Diagnostics/tests: the tree-owned epoch manager (plan-tree #7).
  EpochManager& epoch() { return epoch_; }
  const BufferPool* buffer_pool() const { return pool_.get(); }
  int height() const;         // 1 = single-leaf root
  size_t leaf_count() const;  // live leaves reachable from the root
  // # of base pages physically written by the most recent snapshot (the rest
  // were clean and retained their durable addr). For incremental-snapshot tests.
  uint64_t last_snapshot_pages_written() const { return snapshot_pages_written_.load(); }
  // Evict clean, delta-free resident leaf bases down to at most
  // `max_resident_leaves`, re-tagging their slots unloaded and epoch-retiring the
  // pages (design §4.6); returns the number evicted. Safe against lock-free
  // readers (epoch-deferred frame reuse); evicted pages reload on next access.
  size_t evict_clean_leaves(size_t max_resident_leaves);

 private:
  // apply a batch's ops into L0 at `slot` (intra-batch last-op-wins).
  void apply_batch(uint64_t slot, const Batch& batch);
  // Fold newly received slots into the contiguous prefix, then prune the
  // tracker below the new frontier. Caller holds slot_mutex_.
  void recompute_contiguous_locked();
  void maybe_flush();
  void consolidate_locked(uint64_t page_id);           // caller holds write_mutex_
  void maybe_split_or_merge_locked(uint64_t page_id);  // dispatch on leaf size
  // Inner PIDs from root down to (but excluding) the leaf `target_page_id`.
  std::vector<uint64_t> path_to_page_id_locked(uint64_t target_page_id) const;
  void split_leaf_locked(uint64_t leaf_page_id, std::vector<uint64_t> path);
  void propagate_split_locked(std::vector<uint64_t> path, uint64_t child_page_id, std::string sep,
                              uint64_t right_page_id);
  void try_merge_leaf_locked(uint64_t leaf_page_id, const std::vector<uint64_t>& path);
  // Merge an underfull non-root inner page with its left sibling (mirrors leaf
  // merge), recursing up; collapses the root when it drops to a single child.
  // `path` is the inner PIDs from root down to (but excluding) `inner_page_id`.
  void try_merge_inner_locked(uint64_t inner_page_id, std::vector<uint64_t> path);
  // Separator-count threshold below which a non-root inner page is merged.
  uint32_t inner_merge_keys() const {
    if (opt_.inner_merge_keys != 0)
    {
      return opt_.inner_merge_keys;
    }
    uint32_t q = opt_.inner_max_keys / 4;
    return q != 0 ? q : 1;
  }
  void retire_page(PageBase* p);
  // Recursively drop a subtree. `retire=false` frees pages immediately (teardown
  // / no concurrent readers). `retire=true` epoch-retires each page and overflow
  // chain and clears its mapping slot, so a lock-free reader still holding a page
  // under its guard is never freed underneath it (used by install_snapshot on the
  // live tree). Caller holds write_mutex_ for the retire path.
  void free_subtree(uint64_t page_id, bool retire);

  // Effective overflow spill threshold (opt_.max_inline_value or frame_bytes/4).
  size_t max_inline_value() const {
    return opt_.max_inline_value != 0 ? opt_.max_inline_value : opt_.frame_bytes / 4;
  }
  // Effective key size limit (opt_.max_key_size or frame_bytes/2). Keys larger
  // than this are rejected at apply() (plan-tree #15).
  size_t max_key_size() const {
    return opt_.max_key_size != 0 ? opt_.max_key_size : opt_.frame_bytes / 2;
  }
  // Fold a leaf chain (deltas + base) to key-sorted storage entries by
  // highest-slot-wins, dropping tombstones with slot <= gc_floor. Overflow
  // pointer cells are carried forward unchanged; any overflow chain that a
  // higher-slot write supersedes is appended to *dead_overflow (if non-null) so
  // the caller can retire it. Caller holds write_mutex_.
  std::vector<leaf_entry> resolve_leaf_chain_for_rebuild(
      PageBase* head, uint64_t gc_floor, std::vector<uint64_t>* dead_overflow) const;
  // Spill `value` into a fresh overflow page chain; returns the head PID. Caller
  // holds write_mutex_.
  uint64_t spill_value_to_overflow_chain_locked(const std::string& value);
  // build a leaf base from storage entries, spilling any inline value larger
  // than max_inline_value() into an overflow chain and replacing it with a pointer
  // cell. Entries already in overflow-pointer form are carried forward as-is.
  LeafBase* build_leaf_spilling_locked(std::vector<leaf_entry> entries, uint64_t right_sibling);
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
  void free_overflow_chain(uint64_t head_page_id);
  size_t evict_clean_leaves_locked(size_t max_resident_leaves);  // caller holds write_mutex_
  void maybe_evict_locked();  // capacity-driven auto-evict (caller holds write_mutex_)
  // Resolve a PID to its resident chain head, demand-loading an unloaded slot
  // (design §4.5). Hot (resident) path is lock-free; the cold path locks
  // load_mutex_ and double-checks. Returns nullptr if the slot is unset.
  PageBase* resident(uint64_t page_id) const;

  Options opt_;
  // Base-page frame arena (design §4). shared_ptr because epoch-retired pages
  // co-own it; the tree-owned EpochManager (epoch_, declared last so it is
  // destroyed first) reclaims those pages before pool_ is destroyed. Declared
  // before mapping_ so it is destroyed after the pages it backs.
  std::shared_ptr<BufferPool> pool_;
  MappingTable mapping_;
  MemTable memtable_;

  // internal_error slot tracker (replaces the caller-supplied contiguous_slot). Holds
  // received-but-not-yet-contiguous slots above contiguous_slot_; the contiguous
  // prefix is folded forward on each apply/force_advance_slot and pruned below
  // the frontier to stay bounded. Guarded by slot_mutex_.
  mutable std::mutex slot_mutex_;
  std::set<uint64_t> received_slots_;
  uint64_t max_seen_slot_ = 0;
  std::atomic<uint64_t> auto_slot_{0};  // next auto-assigned slot for put/del/batch_put

  std::atomic<uint64_t> root_page_id_{kInvalidPageId};
  std::atomic<uint64_t> contiguous_slot_{0};
  std::atomic<uint64_t> last_applied_slot_{0};
  std::atomic<uint64_t> version_{0};
  std::atomic<uint64_t> gc_floor_{0};
  std::atomic<uint64_t> snapshot_pages_written_{0};  // pages written by last snapshot
  mutable std::atomic<bool> io_failed_{false};   // latched demand-load media fault

  mutable std::mutex write_mutex_;  // serializes flush / consolidate / split-merge
  mutable std::mutex load_mutex_;   // serializes cold-path demand loads (design §4.5)

  // Tree-owned epoch-based reclamation (plan-tree #7; formerly on CrowtreeEnv).
  // Declared last so it is destroyed first: ~Crowtree frees the live tree via
  // free_subtree(root, /*retire=*/false) (no readers at teardown), then epoch_'s
  // destructor reclaims any pages still pending from earlier retire()s (eviction,
  // consolidation, install_snapshot) while pool_ / mapping_ are still alive.
  // mutable: readers take a guard in const get().
  mutable EpochManager epoch_;
};

}  // namespace crowtree
