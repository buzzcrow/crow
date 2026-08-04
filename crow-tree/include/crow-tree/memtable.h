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

#include "crow-tree/buffer.h"
#include "crow-tree/cell.h"
#include "crow-tree/slice.h"

#include <absl/container/btree_map.h>

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

namespace crow::tree
{

struct mem_entry
{
    std::string key; // key stays std::string: it must be COPYABLE (a btree relocates
                     // its const-key slots on split/merge, which a move-only buffer key
                     // cannot satisfy) and SSO already inlines small keys optimally.
    buffer   cell;   // encoded cell payload (single-alloc buffer; SBO-inline for small)
    uint64_t slot;
};

// Internal MemTable value (R30). A cell is stored in one of two forms:
//   - contiguous: `cell` is a kOwned buffer holding the full [header][value]
//     payload (the pre-R30 path, used by snapshot import and the legacy
//     `upsert(Slice, slot, buffer&&)` overload). `slot`/`flags` are decoded
//     from the cell via `CellView` and kept as fields for fast
//     highest-slot-wins checks without re-parsing.
//   - split: `cell` is a kExternal buffer holding ONLY the value bytes,
//     borrowed from a Rust `bytes::Bytes` (zero-copy apply path). The 9-byte
//     header is NOT stored as bytes — `slot`/`flags` are the fields, and the
//     contiguous cell is materialized at the memtable API boundary
//     (`get`/`drain_up_to`/`snapshot`) where a copy already exists.
// Tag: `cell.ownership() == buffer::mode::kExternal` -> split; else contiguous.
// (The memtable never stores kBorrowed cells, so the tag is unambiguous.)
struct cell_entry
{
    uint64_t slot  = 0;
    uint8_t  flags = 0;
    buffer   cell; // contiguous (kOwned): full [header][value]; split (kExternal): value-only

    // Materialize a contiguous [header][value] cell (deep copy). Used by
    // `snapshot()` and any path that must leave the map intact.
    [[nodiscard]] buffer materialize() const
    {
        if (cell.ownership() != buffer::mode::kExternal) {
            return cell.clone(); // already contiguous
        }
        size_t   vlen = cell.size();
        buffer   b    = buffer::alloc(vlen, kCellHeaderSize);
        uint8_t *p    = b.data();
        for (int i = 0; i < 8; ++i) {
            p[i] = static_cast<uint8_t>((slot >> (8 * i)) & 0xff);
        }
        p[8] = flags;
        if (vlen > 0) {
            std::memcpy(b.data() + kCellHeaderSize, cell.data(), vlen);
        }
        return b;
    }

    // Materialize, moving the contiguous cell out when possible (drain path —
    // the entry is erased right after, so the owned cell can be moved instead
    // of cloned). Split cells still copy (borrowed bytes cannot be moved).
    [[nodiscard]] buffer materialize_move()
    {
        if (cell.ownership() != buffer::mode::kExternal) {
            return std::move(cell); // contiguous: move the whole cell out
        }
        return materialize(); // split: copy value + build header
    }
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

    // Zero-copy apply path (R30): store a split cell — the value is borrowed
    // from a Rust `bytes::Bytes` via a kExternal buffer (no value memcpy), and
    // the 9-byte cell header is stored as `slot`/`flags` fields. The
    // contiguous cell is materialized at `get`/`drain_up_to`/`snapshot`.
    // `value` must be a kExternal buffer (Put) or an empty buffer (Delete,
    // `flags = kFlagTombstone`).
    bool upsert_external(Slice key, uint64_t slot, uint8_t flags, buffer &&value);

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
    // key (std::string, SSO) -> cell_entry (contiguous or split). std::less<> is
    // transparent, enabling heterogeneous lookup by std::string_view without
    // allocating a temporary key.
    absl::btree_map<std::string, cell_entry, std::less<>> map_;
    size_t                                                bytes_           = 0;
    uint64_t                                              durable_floor_   = 0;
    bool                                                  allow_old_slots_ = false;
    uint64_t min_slot_ = UINT64_MAX; // slot range of current entries; tracked on
    uint64_t max_slot_ = 0;          // upsert, reset when the map empties
    uint64_t id_       = 0;          // monotonic id for logging (mt0, mt1, …)
};

} // namespace crow::tree
