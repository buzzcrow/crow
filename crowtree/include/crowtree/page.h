// Pages (design-crowtree-core.md §3).
//
// The core in-memory engine represents pages as C++ objects (not the byte-packed
// on-disk offset-array layout; that lives in the persistence plan). Semantics
// match the design: leaves hold sorted (key, cell) entries with a right-sibling
// link; inner pages hold separator keys + child PIDs. Delta records (CT8) link
// in front of a LeafBase via the chain fields in PageBase.
#pragma once

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "crowtree/bloom.h"
#include "crowtree/cell.h"
#include "crowtree/slice.h"

namespace crowtree {

inline constexpr uint64_t kInvalidPID = ~0ull;

enum class PageType : uint8_t {
  kLeafBase = 1,
  kInnerBase = 2,
  kBatchDelta = 3,
};

inline bool IsBase(PageType t) {
  return t == PageType::kLeafBase || t == PageType::kInnerBase;
}

// Common header for every node reachable through the mapping table. Delta nodes
// (CT8) chain in front of a base via `next`; bases have next == nullptr.
struct PageBase {
  explicit PageBase(PageType t) : type(t) {}
  virtual ~PageBase() = default;

  PageType type;
  PageBase* next = nullptr;   // chain link: delta -> ... -> base
  uint32_t delta_len = 0;     // # of deltas above the base (0 on a base)
  size_t chain_bytes = 0;     // approx bytes of this node + below (triggers)
  uint64_t pid = kInvalidPID; // logical page id (set on install)
};

// One leaf entry: key + encoded slot-aware cell payload (see cell.h).
struct LeafEntry {
  std::string key;
  std::string cell;  // encoded CellView payload
};

// Immutable, sorted leaf base page.
class LeafBase : public PageBase {
 public:
  LeafBase() : PageBase(PageType::kLeafBase) {}

  // Build from already key-sorted, deduplicated entries.
  static LeafBase* Build(std::vector<LeafEntry> sorted_entries,
                         uint64_t right_sibling = kInvalidPID) {
    auto* p = new LeafBase();
    p->entries_ = std::move(sorted_entries);
    p->right_sibling_ = right_sibling;
    p->RebuildIndex();
    return p;
  }

  size_t count() const { return entries_.size(); }
  bool empty() const { return entries_.empty(); }
  uint64_t right_sibling() const { return right_sibling_; }
  void set_right_sibling(uint64_t pid) { right_sibling_ = pid; }

  const LeafEntry& entry(size_t i) const { return entries_[i]; }
  const std::vector<LeafEntry>& entries() const { return entries_; }

  Slice low_key() const { return entries_.empty() ? Slice() : Slice(entries_.front().key); }
  Slice high_key() const { return entries_.empty() ? Slice() : Slice(entries_.back().key); }

  // Approximate payload size for split/merge decisions.
  size_t data_bytes() const { return data_bytes_; }

  // Binary search. Returns index of `key`, or -1 if absent.
  int Find(Slice key) const {
    if (!bloom_.MaybeContains(key)) return -1;
    size_t lo = 0, hi = entries_.size();
    while (lo < hi) {
      size_t mid = lo + (hi - lo) / 2;
      int c = Slice(entries_[mid].key).compare(key);
      if (c == 0) return static_cast<int>(mid);
      if (c < 0) lo = mid + 1; else hi = mid;
    }
    return -1;
  }

  // Lookup returning the decoded cell. Returns false if absent.
  bool Lookup(Slice key, CellView* out) const {
    int i = Find(key);
    if (i < 0) return false;
    *out = CellView{Slice(entries_[i].cell)};
    return true;
  }

  // First index whose key >= `key` (lower_bound), for range scans.
  size_t LowerBound(Slice key) const {
    size_t lo = 0, hi = entries_.size();
    while (lo < hi) {
      size_t mid = lo + (hi - lo) / 2;
      if (Slice(entries_[mid].key).compare(key) < 0) lo = mid + 1; else hi = mid;
    }
    return lo;
  }

 private:
  void RebuildIndex() {
    bloom_.Init(entries_.size());
    data_bytes_ = 0;
    for (auto& e : entries_) {
      bloom_.Add(Slice(e.key));
      data_bytes_ += e.key.size() + e.cell.size();
    }
  }

  std::vector<LeafEntry> entries_;
  uint64_t right_sibling_ = kInvalidPID;
  size_t data_bytes_ = 0;
  BloomFilter bloom_;
};

// Immutable inner (index) page. Holds `n` child PIDs and `n-1` separator keys.
// children_[i] covers keys k with separators_[i-1] <= k < separators_[i]
// (with -inf / +inf at the ends). Inner pages carry no values and are rebuilt
// eagerly on change (no delta chain) in the in-memory core.
class InnerBase : public PageBase {
 public:
  InnerBase() : PageBase(PageType::kInnerBase) {}

  static InnerBase* Build(std::vector<std::string> separators,
                          std::vector<uint64_t> children) {
    auto* p = new InnerBase();
    p->separators_ = std::move(separators);
    p->children_ = std::move(children);
    return p;
  }

  size_t num_children() const { return children_.size(); }
  size_t num_separators() const { return separators_.size(); }
  uint64_t child_at(size_t i) const { return children_[i]; }
  const std::string& separator_at(size_t i) const { return separators_[i]; }
  const std::vector<std::string>& separators() const { return separators_; }
  const std::vector<uint64_t>& children() const { return children_; }

  // Index of the child subtree that should contain `key`.
  size_t ChildIndexFor(Slice key) const {
    // upper_bound: first separator strictly greater than key.
    size_t lo = 0, hi = separators_.size();
    while (lo < hi) {
      size_t mid = lo + (hi - lo) / 2;
      if (Slice(separators_[mid]).compare(key) <= 0) lo = mid + 1; else hi = mid;
    }
    return lo;  // == child index
  }

  uint64_t ChildFor(Slice key) const { return children_[ChildIndexFor(key)]; }

 private:
  std::vector<std::string> separators_;  // size = children - 1
  std::vector<uint64_t> children_;       // PIDs, size = separators + 1
};

}  // namespace crowtree
