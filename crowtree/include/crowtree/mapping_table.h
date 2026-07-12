// Mapping table.
//
// PID -> atomic<PageBase*>. All structural references (root, sibling links,
// inner children) are PIDs, so a page can be replaced by swapping one slot.
// Readers do a lock-free atomic load; the single writer (flusher) does a plain
// atomic store (no CAS, per D2). PID allocation/free is mutex-guarded.
#pragma once

#include "crowtree/page.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

namespace crowtree {

// Placeholder for an on-disk base page that is not resident (design §4.5). A
// mapping slot may hold a *tagged* pointer to one of these (low bit set) instead
// of a real PageBase*; `get` of such a slot is a demand-load miss. The slot owns
// the descriptor: it is freed when the slot transitions to resident or when the
// table is destroyed.
struct unloaded_page {
  uint64_t addr = 0;  // durable PageAddr
  uint32_t plen = 0;  // frame length to read
};

class MappingTable {
 public:
  static constexpr uint64_t kSegmentSize = 1024;      // PIDs per segment
  static constexpr uint64_t kMaxSegments = 1u << 16;  // -> 64M PIDs
  static constexpr uintptr_t kUnloadedBit = 1;

  // Tagged-slot helpers. A slot value with the low bit set is a tagged
  // unloaded_page*; otherwise it is a real PageBase* (8-byte aligned).
  static bool is_unloaded(PageBase* v) {
    return (reinterpret_cast<uintptr_t>(v) & kUnloadedBit) != 0;
  }
  static PageBase* tag_unloaded(unloaded_page* u) {
    return reinterpret_cast<PageBase*>(reinterpret_cast<uintptr_t>(u) | kUnloadedBit);
  }
  static unloaded_page* as_unloaded(PageBase* v) {
    return reinterpret_cast<unloaded_page*>(reinterpret_cast<uintptr_t>(v) & ~kUnloadedBit);
  }

  MappingTable();
  ~MappingTable();

  MappingTable(const MappingTable&) = delete;
  MappingTable& operator=(const MappingTable&) = delete;

  // Reader: lock-free atomic load. May return a real PageBase*, a tagged
  // unloaded_page* (see is_unloaded), or nullptr if unset/invalid.
  PageBase* get(uint64_t page_id) const;

  // Writer: plain atomic store (single-writer; no CAS).
  void store(uint64_t page_id, PageBase* page);

  // Install an unloaded (on-disk, not-resident) tag for `page_id` (recovery / 5.4
  // eviction). Allocates the descriptor; the slot owns it.
  void store_unloaded(uint64_t page_id, uint64_t addr, uint32_t plen);

  // Allocate a fresh PID (recycled from the free list when available).
  uint64_t allocate_page_id();

  // Return a PID to the free list and clear its slot.
  void free_page_id(uint64_t page_id);

  // Number of segments currently allocated (diagnostics).
  size_t segments_allocated() const;

  // Recovery: resume fresh PID allocation past the highest persisted PID.
  void set_next_page_id(uint64_t next);
  uint64_t next_page_id() const;

 private:
  using Slot = std::atomic<PageBase*>;
  struct Segment {
    Slot slots[kSegmentSize];
    Segment() {
      for (auto& s : slots)
      {
        s.store(nullptr, std::memory_order_relaxed);
      }
    }
  };

  Segment* ensure_segment(uint64_t seg_idx);

  std::vector<std::atomic<Segment*>> segments_;  // fixed-size top-level array

  mutable std::mutex alloc_mu_;
  uint64_t next_page_id_ = 0;
  std::vector<uint64_t> free_list_;
};

}  // namespace crowtree
