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

// Placeholder for an on-disk base page that is not resident (design §4.5). A
// mapping slot may hold a *tagged* pointer to one of these (low bit set) instead
// of a real PageBase*; `Get` of such a slot is a demand-load miss. The slot owns
// the descriptor: it is freed when the slot transitions to resident or when the
// table is destroyed.
struct UnloadedPage {
  uint64_t addr = 0;   // durable PageAddr
  uint32_t plen = 0;   // frame length to read
};

class MappingTable {
 public:
  static constexpr uint64_t kSegmentSize = 1024;        // PIDs per segment
  static constexpr uint64_t kMaxSegments = 1u << 16;    // -> 64M PIDs
  static constexpr uintptr_t kUnloadedBit = 1;

  // Tagged-slot helpers. A slot value with the low bit set is a tagged
  // UnloadedPage*; otherwise it is a real PageBase* (8-byte aligned).
  static bool IsUnloaded(PageBase* v) {
    return (reinterpret_cast<uintptr_t>(v) & kUnloadedBit) != 0;
  }
  static PageBase* TagUnloaded(UnloadedPage* u) {
    return reinterpret_cast<PageBase*>(reinterpret_cast<uintptr_t>(u) | kUnloadedBit);
  }
  static UnloadedPage* AsUnloaded(PageBase* v) {
    return reinterpret_cast<UnloadedPage*>(reinterpret_cast<uintptr_t>(v) & ~kUnloadedBit);
  }

  MappingTable();
  ~MappingTable();

  MappingTable(const MappingTable&) = delete;
  MappingTable& operator=(const MappingTable&) = delete;

  // Reader: lock-free atomic load. May return a real PageBase*, a tagged
  // UnloadedPage* (see IsUnloaded), or nullptr if unset/invalid.
  PageBase* Get(uint64_t pid) const;

  // Writer: plain atomic store (single-writer; no CAS).
  void Store(uint64_t pid, PageBase* page);

  // Install an unloaded (on-disk, not-resident) tag for `pid` (recovery / 5.4
  // eviction). Allocates the descriptor; the slot owns it.
  void StoreUnloaded(uint64_t pid, uint64_t addr, uint32_t plen);

  // Allocate a fresh PID (recycled from the free list when available).
  uint64_t AllocatePID();

  // Return a PID to the free list and clear its slot.
  void FreePID(uint64_t pid);

  // Number of segments currently allocated (diagnostics).
  size_t SegmentsAllocated() const;

  // Recovery: resume fresh PID allocation past the highest persisted PID.
  void SetNextPid(uint64_t next);
  uint64_t NextPid() const;

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
