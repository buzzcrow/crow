// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// MemTable (L0).
//
// A concurrent in-memory ordered map `key -> encoded cell` that absorbs apply()
// (concurrent, possibly out-of-order by slot). It keeps one (highest-slot) cell
// per key and drops writes already durable in L1 (slot <= durable_floor), so any
// key present in L0 is strictly newer than L1 -> L0-first reads are correct.
//
// v1 uses an absl::btree_map under a mutex (sharded/skiplist is a later
// optimization). btree_map is cache-friendlier than std::map's red-black tree
// for the point-get / ordered-drain workload (D-Q10, plan-tree #9).
#pragma once

#include "crowtree/buffer.h"
#include "crowtree/cell.h"
#include "crowtree/slice.h"

#include <absl/container/btree_map.h>

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

namespace crowtree
{

struct mem_entry
{
    std::string key; // key stays std::string: it must be COPYABLE (a btree relocates
                     // its const-key slots on split/merge, which a move-only buffer key
                     // cannot satisfy) and SSO already inlines small keys optimally.
    buffer   cell;   // encoded cell payload (single-alloc buffer; SBO-inline for small)
    uint64_t slot;
};

class MemTable
{
  public:
    explicit MemTable(uint64_t id = 0) : id_(id)
    {
    }

    [[nodiscard]] uint64_t id() const
    {
        return id_;
    }

    // Insert/replace with highest-slot-wins. Returns true if the table changed.
    // Drops the write if an existing entry has a >= slot. Also drops writes with
    // slot <= durable_floor (already in L1) unless allow_old_slots is set.
    bool upsert(Slice key, uint64_t slot, Slice cell_payload);
    // Move the pre-encoded cell buffer in (no cell copy); the key is copied once.
    // Used by apply's per-batch dedup (single-allocation encoded cell via
    // encode_cell_buf) and snapshot import.
    bool upsert(Slice key, uint64_t slot, buffer &&cell_payload);

    // Set the durable floor (engine's last_applied_slot). Writes at or below it are
    // already in L1 and are rejected by upsert (unless allow_old_slots). Does not
    // retroactively evict.
    void                   set_durable_floor(uint64_t slot);
    [[nodiscard]] uint64_t durable_floor() const;

    // Allow upsert to accept slots <= durable_floor. Needed during restore, when
    // Paxos may re-learn an old slot whose value differs from L1. With this set,
    // L0 is no longer strictly newer than L1, so get must always consult L0 first
    // (it already does) and use the L0 cell when present.
    void set_allow_old_slots(bool v);

    // Range of slots currently held in L0 ([min,max]; empty when no entries). A
    // reader that knows a key's expected slot can skip the L0 lookup when that
    // slot falls outside this range and go straight to L1.
    struct slot_range_t
    {
        uint64_t min   = UINT64_MAX;
        uint64_t max   = 0;
        bool     empty = true;
    };

    [[nodiscard]] slot_range_t slot_range() const;

    // Drop all entries and reset the durable floor to 0 (snapshot import: the L0
    // overlay is fully replaced along with L1). Also resets allow_old_slots and
    // slot-range tracking.
    void reset();

    // Point read: copies the encoded cell into *out_cell. Returns false if absent.
    [[nodiscard]] bool get(Slice key, std::string *out_cell) const;

    // Remove and return, in key order, all entries with slot <= cs. Entries with
    // slot > cs are retained (not yet contiguous / durable-eligible).
    [[nodiscard]] std::vector<mem_entry> drain_up_to(uint64_t cs);

    // Ordered immutable copy of the current contents (for scan merge cursors).
    [[nodiscard]] std::vector<mem_entry> snapshot() const;

    [[nodiscard]] size_t approx_bytes() const;
    [[nodiscard]] size_t count() const;
    [[nodiscard]] bool   empty() const;

  private:
    mutable std::mutex mu_;
    // key (std::string, SSO) -> encoded cell (owned buffer, SBO-inline for small
    // values). std::less<> is transparent, enabling heterogeneous lookup by
    // std::string_view without allocating a temporary key.
    absl::btree_map<std::string, buffer, std::less<>> map_;
    size_t                                            bytes_           = 0;
    uint64_t                                          durable_floor_   = 0;
    bool                                              allow_old_slots_ = false;
    uint64_t min_slot_ = UINT64_MAX; // slot range of current entries; tracked on
    uint64_t max_slot_ = 0;          // upsert, reset when the map empties
    uint64_t id_       = 0;          // monotonic id for logging (mt0, mt1, …)
};

} // namespace crowtree
