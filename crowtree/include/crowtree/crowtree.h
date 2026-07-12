// Crowtree: one ordered, single-version-per-key store per consensus group
// (design-crowtree-core.md). Two-level write path: apply() lands in the MemTable
// (L0); flush() merges the contiguous-applied prefix into the COW B+tree (L1).
#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include "crowtree/cell.h"
#include "crowtree/env.h"
#include "crowtree/mapping_table.h"
#include "crowtree/memtable.h"
#include "crowtree/options.h"
#include "crowtree/page.h"
#include "crowtree/snapshot.h"
#include "crowtree/status.h"

#include <memory>

namespace crowtree {

// One mutation in a batch. All ops in a batch share the batch's slot.
struct BatchOp {
  std::string key;
  OpKind kind;
  std::string value;  // empty for Delete
};

struct Batch {
  std::vector<BatchOp> ops;
};

class Crowtree {
 public:
  explicit Crowtree(CrowtreeEnv& env, const Options& opt = Options());
  ~Crowtree();

  Crowtree(const Crowtree&) = delete;
  Crowtree& operator=(const Crowtree&) = delete;

  // Ingest a batch at `slot`; `contiguous_slot` is the learner's contiguous
  // applied frontier (how far the flusher may flush). Lands in L0; may trigger a
  // size-based flush.
  Status Apply(uint64_t slot, const Batch& batch, uint64_t contiguous_slot);

  // Advance the contiguous frontier without a batch (e.g. after NoOp slots).
  void AdvanceContiguous(uint64_t contiguous_slot);

  // Logical retention GC watermark: tombstones with slot <= safe_slot may be
  // dropped during consolidation (design-crowtree.md two-GC model).
  void SetGcWatermark(uint64_t safe_slot);
  uint64_t gc_watermark() const { return gc_floor_.load(); }

  // Drain the contiguous-applied prefix of L0 into L1 and publish a new root.
  Status Flush();

  // Point read (L0 overlay then L1). Returns true if a live value is found;
  // tombstones return false.
  bool Get(Slice key, uint64_t* out_slot, std::string* out_value) const;

  // Pin a consistent point-in-time view at `last_applied_slot` (the durable L1
  // state). Used for scan-at / compare / iter_all / snapshot export.
  std::shared_ptr<Snapshot> SnapshotView();

  uint64_t last_applied_slot() const { return last_applied_slot_.load(); }
  uint64_t contiguous_slot() const { return contiguous_slot_.load(); }
  uint64_t version() const { return version_.load(); }
  uint64_t root_pid() const { return root_pid_.load(); }

  // Diagnostics.
  size_t MemTableCount() const { return memtable_.Count(); }
  MappingTable& mapping() { return mapping_; }
  int Height() const;       // 1 = single-leaf root
  size_t LeafCount() const; // live leaves reachable from the root

 private:
  void MaybeFlush();
  void ConsolidateLocked(uint64_t pid);        // caller holds write_mutex_
  void MaybeSplitOrMergeLocked(uint64_t pid);  // dispatch on leaf size
  // Inner PIDs from root down to (but excluding) the leaf `target_pid`.
  std::vector<uint64_t> PathToPidLocked(uint64_t target_pid) const;
  void SplitLeafLocked(uint64_t leaf_pid, std::vector<uint64_t> path);
  void PropagateSplitLocked(std::vector<uint64_t> path, uint64_t child_pid,
                            std::string sep, uint64_t right_pid);
  void TryMergeLeafLocked(uint64_t leaf_pid, const std::vector<uint64_t>& path);
  void RetirePage(PageBase* p);
  void FreeSubtree(uint64_t pid);

  CrowtreeEnv& env_;
  Options opt_;
  MappingTable mapping_;
  MemTable memtable_;

  std::atomic<uint64_t> root_pid_{kInvalidPID};
  std::atomic<uint64_t> contiguous_slot_{0};
  std::atomic<uint64_t> last_applied_slot_{0};
  std::atomic<uint64_t> version_{0};
  std::atomic<uint64_t> gc_floor_{0};

  std::mutex write_mutex_;  // serializes flush / consolidate / split-merge
};

}  // namespace crowtree
