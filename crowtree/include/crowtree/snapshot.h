// Consistent point-in-time view. A Snapshot is an immutable, key-sorted materialization of the
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
struct engine_diff {
  std::string key;
  enum Kind { kOnlyLeft, kOnlyRight, kSlotDiffers, kValueDiffers } kind;
};

class Snapshot {
 public:
  Snapshot(uint64_t at_slot, std::vector<leaf_entry> sorted_with_tombstones)
      : at_slot_(at_slot), entries_(std::move(sorted_with_tombstones)) {}

  uint64_t at_slot() const { return at_slot_; }

  // All entries including tombstones, key-sorted.
  const std::vector<leaf_entry>& entries() const { return entries_; }
  size_t size() const { return entries_.size(); }

  int find(Slice key) const {
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

  // Live read: false for absent or tombstoned keys.
  bool get(Slice key, uint64_t* out_slot, std::string* out_value) const {
    int i = find(key);
    if (i < 0)
    {
      return false;
    }
    CellView v{Slice(entries_[i].cell)};
    if (v.is_tombstone())
    {
      return false;
    }
    if (out_slot)
    {
      *out_slot = v.slot();
    }
    if (out_value)
    {
      *out_value = v.value().to_string();
    }
    return true;
  }

  // Structural comparison including tombstones (used by parity tests).
  std::vector<engine_diff> compare(const Snapshot& other) const {
    std::vector<engine_diff> diffs;
    size_t i = 0, j = 0;
    const auto& a = entries_;
    const auto& b = other.entries_;
    while (i < a.size() && j < b.size())
    {
      int c = Slice(a[i].key).compare(Slice(b[j].key));
      if (c < 0)
      {
        diffs.push_back({a[i].key, engine_diff::kOnlyLeft});
        ++i;
      } else if (c > 0)
      {
        diffs.push_back({b[j].key, engine_diff::kOnlyRight});
        ++j;
      } else
      {
        CellView va{Slice(a[i].cell)}, vb{Slice(b[j].cell)};
        if (va.slot() != vb.slot())
        {
          diffs.push_back({a[i].key, engine_diff::kSlotDiffers});
        } else if (va.raw() != vb.raw())
        { diffs.push_back({a[i].key, engine_diff::kValueDiffers}); }
        ++i;
        ++j;
      }
    }
    for (; i < a.size(); ++i)
    {
      diffs.push_back({a[i].key, engine_diff::kOnlyLeft});
    }
    for (; j < b.size(); ++j)
    {
      diffs.push_back({b[j].key, engine_diff::kOnlyRight});
    }
    return diffs;
  }

 private:
  uint64_t at_slot_;
  std::vector<leaf_entry> entries_;  // key-sorted, includes tombstones
};

}  // namespace crowtree
