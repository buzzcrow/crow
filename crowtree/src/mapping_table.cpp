#include "crowtree/mapping_table.h"

#include <cassert>

namespace crowtree {

MappingTable::MappingTable() : segments_(kMaxSegments) {
  for (auto& s : segments_)
  {
    s.store(nullptr, std::memory_order_relaxed);
  }
}

MappingTable::~MappingTable() {
  // The mapping table does not own resident pages (the epoch manager frees
  // them); it owns the segment arrays and any *unloaded* descriptors still
  // tagged into slots (those never demand-loaded during this run).
  for (auto& s : segments_)
  {
    Segment* seg = s.load(std::memory_order_relaxed);
    if (seg != nullptr)
    {
      for (auto& slot : seg->slots)
      {
        PageBase* v = slot.load(std::memory_order_relaxed);
        if (v != nullptr && is_unloaded(v))
        {
          delete as_unloaded(v);
        }
      }
    }
    delete seg;
  }
}

MappingTable::Segment* MappingTable::ensure_segment(uint64_t seg_idx) {
  Segment* seg = segments_[seg_idx].load(std::memory_order_acquire);
  if (seg != nullptr)
  {
    return seg;
  }
  // Allocate under alloc_mu_ (caller already holds it during allocation).
  Segment* fresh = new Segment();
  Segment* expected = nullptr;
  if (segments_[seg_idx].compare_exchange_strong(expected, fresh, std::memory_order_acq_rel))
  {
    return fresh;
  }
  // Lost the race; another thread installed one.
  delete fresh;
  return expected;
}

PageBase* MappingTable::get(uint64_t page_id) const {
  if (page_id == kInvalidPageId)
  {
    return nullptr;
  }
  uint64_t seg_idx = page_id / kSegmentSize;
  if (seg_idx >= kMaxSegments)
  {
    return nullptr;
  }
  Segment* seg = segments_[seg_idx].load(std::memory_order_acquire);
  if (seg == nullptr)
  {
    return nullptr;
  }
  return seg->slots[page_id % kSegmentSize].load(std::memory_order_acquire);
}

void MappingTable::store(uint64_t page_id, PageBase* page) {
  assert(page_id != kInvalidPageId);
  uint64_t seg_idx = page_id / kSegmentSize;
  assert(seg_idx < kMaxSegments);
  Segment* seg = segments_[seg_idx].load(std::memory_order_acquire);
  if (seg == nullptr)
  {
    std::lock_guard<std::mutex> lk(alloc_mu_);
    seg = ensure_segment(seg_idx);
  }
  if (page != nullptr && !is_unloaded(page))
  {
    page->page_id = page_id;
  }
  PageBase* old = seg->slots[page_id % kSegmentSize].exchange(page, std::memory_order_acq_rel);
  if (old != nullptr && is_unloaded(old))
  {
    delete as_unloaded(old);
  }
}

void MappingTable::store_unloaded(uint64_t page_id, uint64_t addr, uint32_t plen) {
  auto* u = new unloaded_page();
  u->addr = addr;
  u->plen = plen;
  store(page_id, tag_unloaded(u));
}

uint64_t MappingTable::allocate_page_id() {
  std::lock_guard<std::mutex> lk(alloc_mu_);
  uint64_t page_id;
  if (!free_list_.empty())
  {
    page_id = free_list_.back();
    free_list_.pop_back();
  } else
  {
    page_id = next_page_id_++;
  }
  uint64_t seg_idx = page_id / kSegmentSize;
  ensure_segment(seg_idx);
  return page_id;
}

void MappingTable::free_page_id(uint64_t page_id) {
  if (page_id == kInvalidPageId)
  {
    return;
  }
  store(page_id, nullptr);
  std::lock_guard<std::mutex> lk(alloc_mu_);
  free_list_.push_back(page_id);
}

void MappingTable::set_next_page_id(uint64_t next) {
  std::lock_guard<std::mutex> lk(alloc_mu_);
  next_page_id_ = next;
  free_list_.clear();
}

uint64_t MappingTable::next_page_id() const {
  std::lock_guard<std::mutex> lk(alloc_mu_);
  return next_page_id_;
}

size_t MappingTable::segments_allocated() const {
  size_t n = 0;
  for (auto& s : segments_)
  {
    if (s.load(std::memory_order_relaxed) != nullptr)
    {
      ++n;
    }
  }
  return n;
}

}  // namespace crowtree
