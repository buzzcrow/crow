// Delta records.
//
// A flush prepends one BatchDelta per affected leaf, all stamped with the
// flushed slot. A BatchDelta holds the slot's mutations for a single leaf
// (sorted by key, one cell each) and links in front of the rest of the chain
// via PageBase::next. The chain is resolved newest-first / highest-slot-wins.
#pragma once

#include "crowtree/cell.h"
#include "crowtree/page.h"
#include "crowtree/slice.h"

#include <vector>

namespace crowtree {

class BatchDelta : public PageBase {
 public:
  BatchDelta() : PageBase(page_type::kBatchDelta) {}

  // build a delta over `next` (the existing chain head, or a LeafBase).
  // `sorted` must be key-sorted with one cell per key.
  static BatchDelta* build(uint64_t slot, std::vector<leaf_entry> sorted, PageBase* next) {
    auto* d = new BatchDelta();
    d->slot_ = slot;
    d->entries_ = std::move(sorted);
    d->self_bytes_ = 0;
    for (auto& e : d->entries_)
    {
      d->self_bytes_ += e.key.size() + e.cell.size();
    }
    d->next = next;
    d->delta_len = next ? next->delta_len + 1 : 1;
    d->chain_bytes = (next ? next->chain_bytes : 0) + d->self_bytes_;
    d->page_id = next ? next->page_id : kInvalidPageId;
    return d;
  }

  uint64_t slot() const { return slot_; }
  size_t count() const { return entries_.size(); }
  const leaf_entry& entry(size_t i) const { return entries_[i]; }
  const std::vector<leaf_entry>& entries() const { return entries_; }
  size_t self_bytes() const { return self_bytes_; }

  // Binary search within this delta. Returns index or -1.
  int find_key(Slice key) const {
    size_t lo = 0, hi = entries_.size();
    while (lo < hi)
    {
      size_t mid = lo + (hi - lo) / 2;
      int c = Slice(entries_[mid].key).compare(key);
      if (c == 0)
      {
        return static_cast<int>(mid);
      }
      if (c < 0)
      {
        lo = mid + 1;
      } else
      {
        hi = mid;
      }
    }
    return -1;
  }

 private:
  uint64_t slot_ = 0;
  size_t self_bytes_ = 0;
  std::vector<leaf_entry> entries_;
};

// Resolve a key against a leaf chain (head -> ... -> LeafBase) by highest slot.
// Returns true and sets *out to the winning cell (which may be a tombstone).
inline bool resolve_chain(PageBase* head, Slice key, CellView* out) {
  bool found = false;
  CellView best;
  for (PageBase* node = head; node != nullptr; node = node->next)
  {
    if (node->type == page_type::kBatchDelta)
    {
      auto* d = static_cast<BatchDelta*>(node);
      int i = d->find_key(key);
      if (i >= 0)
      {
        CellView c{Slice(d->entry(i).cell)};
        if (!found || c.slot() > best.slot())
        {
          best = c;
          found = true;
        }
      }
    } else if (node->type == page_type::kLeafBase)
    {
      auto* leaf = static_cast<LeafBase*>(node);
      CellView c;
      if (leaf->lookup(key, &c))
      {
        if (!found || c.slot() > best.slot())
        {
          best = c;
          found = true;
        }
      }
    }
  }
  if (found)
  {
    *out = best;
  }
  return found;
}

// find the LeafBase at the tail of a chain (nullptr if none).
inline LeafBase* chain_leaf_base(PageBase* head) {
  for (PageBase* node = head; node != nullptr; node = node->next)
  {
    if (node->type == page_type::kLeafBase)
    {
      return static_cast<LeafBase*>(node);
    }
  }
  return nullptr;
}

}  // namespace crowtree
