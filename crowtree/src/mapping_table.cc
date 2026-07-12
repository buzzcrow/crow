#include "crowtree/mapping_table.h"

#include <cassert>

namespace crowtree {

MappingTable::MappingTable() : segments_(kMaxSegments) {
  for (auto& s : segments_) s.store(nullptr, std::memory_order_relaxed);
}

MappingTable::~MappingTable() {
  // The mapping table does not own pages (epoch manager frees them); it only
  // owns the segment arrays.
  for (auto& s : segments_) {
    Segment* seg = s.load(std::memory_order_relaxed);
    delete seg;
  }
}

MappingTable::Segment* MappingTable::EnsureSegment(uint64_t seg_idx) {
  Segment* seg = segments_[seg_idx].load(std::memory_order_acquire);
  if (seg != nullptr) return seg;
  // Allocate under alloc_mu_ (caller already holds it during allocation).
  Segment* fresh = new Segment();
  Segment* expected = nullptr;
  if (segments_[seg_idx].compare_exchange_strong(expected, fresh,
                                                 std::memory_order_acq_rel)) {
    return fresh;
  }
  // Lost the race; another thread installed one.
  delete fresh;
  return expected;
}

PageBase* MappingTable::Get(uint64_t pid) const {
  if (pid == kInvalidPID) return nullptr;
  uint64_t seg_idx = pid / kSegmentSize;
  if (seg_idx >= kMaxSegments) return nullptr;
  Segment* seg = segments_[seg_idx].load(std::memory_order_acquire);
  if (seg == nullptr) return nullptr;
  return seg->slots[pid % kSegmentSize].load(std::memory_order_acquire);
}

void MappingTable::Store(uint64_t pid, PageBase* page) {
  assert(pid != kInvalidPID);
  uint64_t seg_idx = pid / kSegmentSize;
  assert(seg_idx < kMaxSegments);
  Segment* seg = segments_[seg_idx].load(std::memory_order_acquire);
  if (seg == nullptr) {
    std::lock_guard<std::mutex> lk(alloc_mu_);
    seg = EnsureSegment(seg_idx);
  }
  if (page != nullptr) page->pid = pid;
  seg->slots[pid % kSegmentSize].store(page, std::memory_order_release);
}

uint64_t MappingTable::AllocatePID() {
  std::lock_guard<std::mutex> lk(alloc_mu_);
  uint64_t pid;
  if (!free_list_.empty()) {
    pid = free_list_.back();
    free_list_.pop_back();
  } else {
    pid = next_pid_++;
  }
  uint64_t seg_idx = pid / kSegmentSize;
  EnsureSegment(seg_idx);
  return pid;
}

void MappingTable::FreePID(uint64_t pid) {
  if (pid == kInvalidPID) return;
  Store(pid, nullptr);
  std::lock_guard<std::mutex> lk(alloc_mu_);
  free_list_.push_back(pid);
}

void MappingTable::SetNextPid(uint64_t next) {
  std::lock_guard<std::mutex> lk(alloc_mu_);
  next_pid_ = next;
  free_list_.clear();
}

uint64_t MappingTable::NextPid() const {
  std::lock_guard<std::mutex> lk(alloc_mu_);
  return next_pid_;
}

size_t MappingTable::SegmentsAllocated() const {
  size_t n = 0;
  for (auto& s : segments_) {
    if (s.load(std::memory_order_relaxed) != nullptr) ++n;
  }
  return n;
}

}  // namespace crowtree
