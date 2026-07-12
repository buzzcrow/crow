// MemTable (L0) — design-crowtree-core.md §1, §6.1.
//
// A concurrent in-memory ordered map `key -> encoded cell` that absorbs apply()
// (concurrent, possibly out-of-order by slot). It keeps one (highest-slot) cell
// per key and drops writes already durable in L1 (slot <= durable_floor), so any
// key present in L0 is strictly newer than L1 -> L0-first reads are correct.
//
// v1 uses a std::map under a mutex (sharded/skiplist is a later optimization).
#pragma once

#include "crowtree/cell.h"
#include "crowtree/slice.h"

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace crowtree {

struct MemEntry {
  std::string key;
  std::string cell;  // encoded cell payload (slot/flags/value)
  uint64_t slot;
};

class MemTable {
 public:
  MemTable() = default;

  // Insert/replace with highest-slot-wins. Returns true if the table changed.
  // Drops the write if slot <= durable_floor (already in L1) or if an existing
  // entry has a >= slot.
  bool Upsert(Slice key, uint64_t slot, Slice cell_payload);

  // Set the durable floor (engine's last_applied_slot). Writes at or below it are
  // already in L1 and are rejected by Upsert. Does not retroactively evict.
  void SetDurableFloor(uint64_t slot);
  uint64_t durable_floor() const;

  // Point read: copies the encoded cell into *out_cell. Returns false if absent.
  bool Get(Slice key, std::string* out_cell) const;

  // Remove and return, in key order, all entries with slot <= cs. Entries with
  // slot > cs are retained (not yet contiguous / durable-eligible).
  std::vector<MemEntry> DrainUpTo(uint64_t cs);

  // Ordered immutable copy of the current contents (for scan merge cursors).
  std::vector<MemEntry> Snapshot() const;

  size_t ApproxBytes() const;
  size_t Count() const;
  bool Empty() const;

 private:
  mutable std::mutex mu_;
  std::map<std::string, std::string, std::less<>> map_;  // key -> encoded cell
  size_t bytes_ = 0;
  uint64_t durable_floor_ = 0;
};

}  // namespace crowtree
