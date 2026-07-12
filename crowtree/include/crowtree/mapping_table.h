// Mapping table (design-crowtree-core.md §4).
//
// PID -> atomic<PageBase*>. All structural references (root, sibling links,
// inner children) are PIDs, so a page can be replaced by swapping one slot.
// Readers do a lock-free atomic load; the single writer (flusher) does a plain
// atomic store (no CAS, per D2). PID allocation/free is mutex-guarded.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "crowtree/page.h"

namespace crowtree {

class MappingTable {
 public:
  static constexpr uint64_t kSegmentSize = 1024;        // PIDs per segment
  static constexpr uint64_t kMaxSegments = 1u << 16;    // -> 64M PIDs

  MappingTable();
  ~MappingTable();

  MappingTable(const MappingTable&) = delete;
  MappingTable& operator=(const MappingTable&) = delete;

  // Reader: lock-free atomic load. Returns nullptr if unset/invalid.
  PageBase* Get(uint64_t pid) const;

  // Writer: plain atomic store (single-writer; no CAS).
  void Store(uint64_t pid, PageBase* page);

  // Allocate a fresh PID (recycled from the free list when available).
  uint64_t AllocatePID();

  // Return a PID to the free list and clear its slot.
  void FreePID(uint64_t pid);

  // Number of segments currently allocated (diagnostics).
  size_t SegmentsAllocated() const;

 private:
  using Slot = std::atomic<PageBase*>;
  struct Segment {
    Slot slots[kSegmentSize];
    Segment() {
      for (auto& s : slots) s.store(nullptr, std::memory_order_relaxed);
    }
  };

  Segment* EnsureSegment(uint64_t seg_idx);

  std::vector<std::atomic<Segment*>> segments_;  // fixed-size top-level array

  mutable std::mutex alloc_mu_;
  uint64_t next_pid_ = 0;
  std::vector<uint64_t> free_list_;
};

}  // namespace crowtree
