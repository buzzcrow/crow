// Consistent point-in-time view (design-crowtree-core.md §9, design-crowtree.md
// §4 EngineView). A Snapshot is an immutable, key-sorted materialization of the
// L1 tree at a given slot, used for scan-at / compare / iter_all / export.
//
// NOTE (deviation): the design specifies a snapshot as a pinned immutable COW
// root (zero-copy). The in-memory core materializes the keyspace into an
// independent immutable copy instead; this is correct and keeps writes lock-free
// for latest reads, at O(N) snapshot cost. Path-copy COW is a later optimization.
#pragma once

#include "crowtree/cell.h"
#include "crowtree/page.h"
#include "crowtree/slice.h"

#include <cstdint>
#include <string>
#include <vector>

namespace crowtree {

// A difference between two snapshots (for compare / parity).
struct EngineDiff {
  std::string key;
  enum Kind { kOnlyLeft, kOnlyRight, kSlotDiffers, kValueDiffers } kind;
};

class Snapshot {
 public:
  Snapshot(uint64_t at_slot, std::vector<LeafEntry> sorted_with_tombstones)
      : at_slot_(at_slot), entries_(std::move(sorted_with_tombstones)) {}

  uint64_t at_slot() const { return at_slot_; }

  // All entries including tombstones, key-sorted.
  const std::vector<LeafEntry>& entries() const { return entries_; }
  size_t size() const { return entries_.size(); }

  int Find(Slice key) const {
    size_t lo = 0, hi = entries_.size();
    while (lo < hi) {
      size_t mid = lo + (hi - lo) / 2;
      int c = Slice(entries_[mid].key).compare(key);
      if (c == 0) return static_cast<int>(mid);
      if (c < 0)
        lo = mid + 1;
      else
        hi = mid;
    }
    return -1;
  }

  // Live read: false for absent or tombstoned keys.
  bool Get(Slice key, uint64_t* out_slot, std::string* out_value) const {
    int i = Find(key);
    if (i < 0) return false;
    CellView v{Slice(entries_[i].cell)};
    if (v.is_tombstone()) return false;
    if (out_slot) *out_slot = v.slot();
    if (out_value) *out_value = v.value().ToString();
    return true;
  }

  // Structural comparison including tombstones (used by parity tests).
  std::vector<EngineDiff> Compare(const Snapshot& other) const {
    std::vector<EngineDiff> diffs;
    size_t i = 0, j = 0;
    const auto& a = entries_;
    const auto& b = other.entries_;
    while (i < a.size() && j < b.size()) {
      int c = Slice(a[i].key).compare(Slice(b[j].key));
      if (c < 0) {
        diffs.push_back({a[i].key, EngineDiff::kOnlyLeft});
        ++i;
      } else if (c > 0) {
        diffs.push_back({b[j].key, EngineDiff::kOnlyRight});
        ++j;
      } else {
        CellView va{Slice(a[i].cell)}, vb{Slice(b[j].cell)};
        if (va.slot() != vb.slot()) {
          diffs.push_back({a[i].key, EngineDiff::kSlotDiffers});
        } else if (va.raw() != vb.raw()) {
          diffs.push_back({a[i].key, EngineDiff::kValueDiffers});
        }
        ++i;
        ++j;
      }
    }
    for (; i < a.size(); ++i) diffs.push_back({a[i].key, EngineDiff::kOnlyLeft});
    for (; j < b.size(); ++j) diffs.push_back({b[j].key, EngineDiff::kOnlyRight});
    return diffs;
  }

 private:
  uint64_t at_slot_;
  std::vector<LeafEntry> entries_;  // key-sorted, includes tombstones
};

}  // namespace crowtree
