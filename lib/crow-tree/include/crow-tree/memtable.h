// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// MemTable (L0) — epoch-protected lock-free (R50).
//
// A concurrent in-memory ordered map `key -> encoded cell` that absorbs apply()
// (concurrent, possibly out-of-order by slot). It keeps one (highest-slot) cell
// per key and drops writes already durable in L1 (slot <= durable_floor), so any
// key present in L0 is strictly newer than L1 -> L0-first reads are correct.
//
// R50: the backing structure is a ConcurrentSkipList with inline keys and
// versioned cells. Readers (scan, get) traverse lock-free under an epoch guard
// with zero copy — a cursor borrows key/cell Slices directly off the node,
// and the epoch guard keeps the node alive past any concurrent drain/overwrite.
// Writers are serialized by the skip list's internal spinlock. Every freed
// node and overwritten cell version is epoch-retired through the engine's
// EpochManager (passed in at construction), so reclamation defers past every
// in-flight reader guard — the same EBR scheme L1 pages already use.
#pragma once

#include "crow-tree/buffer.h"
#include "crow-tree/cell.h"
#include "crow-tree/epoch.h"
#include "crow-tree/skip_list.h"
#include "crow-tree/slice.h"

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

namespace crow::tree
{

struct mem_entry
{
    std::string key;
    buffer      cell; // materialized contiguous [header][value]
    uint64_t    slot;
};

class MemTable
{
  public:
    // `epoch` is the engine's EpochManager — used to retire unlinked nodes
    // and overwritten cell versions. Must outlive this MemTable (owned by
    // the Crowtree, which outlives all MemTables via shared_ptr).
    explicit MemTable(uint64_t id = 0, EpochManager *epoch = nullptr) : epoch_(epoch), id_(id)
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
    bool upsert(Slice key, uint64_t slot, buffer &&cell_payload);

    // Zero-copy apply path (R30): store a split cell — the value is borrowed
    // from a Rust `bytes::Bytes` via a kExternal buffer (no value memcpy), and
    // the 9-byte cell header is stored as `slot`/`flags` fields.
    bool upsert_external(Slice key, uint64_t slot, uint8_t flags, buffer &&value);

    void set_durable_floor(uint64_t slot);

    [[nodiscard]] uint64_t durable_floor() const
    {
        return durable_floor_.load(std::memory_order_relaxed);
    }

    void set_allow_old_slots(bool v)
    {
        allow_old_slots_.store(v, std::memory_order_relaxed);
    }

    struct slot_range_t
    {
        uint64_t min   = UINT64_MAX;
        uint64_t max   = 0;
        bool     empty = true;
    };

    [[nodiscard]] slot_range_t slot_range() const
    {
        uint64_t mn = min_slot_.load(std::memory_order_relaxed);
        uint64_t mx = max_slot_.load(std::memory_order_relaxed);
        if (mn == UINT64_MAX) {
            return slot_range_t{};
        }
        return {.min = mn, .max = mx, .empty = false};
    }

    // Drop all entries and reset the durable floor to 0 (snapshot import).
    // Epoch-retires every node and cell version.
    void reset();

    // Point lookup (lock-free, zero-copy): returns the CellVersion* for `key`,
    // or nullptr. The returned pointer is valid only while the caller's epoch
    // guard is held — a concurrent overwrite retires the old version via epoch.
    [[nodiscard]] const CellVersion *find(Slice key) const
    {
        return list_.find(key);
    }

    // Ordered cursor (lock-free, zero-copy): positioned at the first live
    // node with key > `start_after`. The cursor borrows key/cell Slices
    // directly off the node; valid only while the caller's epoch guard is held.
    [[nodiscard]] ConcurrentSkipList::Cursor cursor(Slice start_after) const
    {
        return list_.cursor(start_after);
    }

    // Remove and return, in key order, all entries with slot <= cs. Entries
    // with slot > cs are retained. The returned entries have materialized
    // contiguous cells (copied — this is the drain/flush path, not the hot
    // read path). Unlinked nodes and old cell versions are epoch-retired.
    [[nodiscard]] std::vector<mem_entry> drain_up_to(uint64_t cs);

    // Ordered immutable copy of the current contents (for full-set paths:
    // iter_all, compare, snapshot_export). O(N) copy is correct there.
    [[nodiscard]] std::vector<mem_entry> snapshot() const;

    [[nodiscard]] size_t approx_bytes() const
    {
        return list_.approx_bytes();
    }

    [[nodiscard]] size_t count() const
    {
        return list_.count();
    }

    [[nodiscard]] bool empty() const
    {
        return list_.empty();
    }

  private:
    void update_slot_range(uint64_t slot)
    {
        // Relaxed: only the writer updates these, and readers use them as hints.
        uint64_t mn = min_slot_.load(std::memory_order_relaxed);
        uint64_t mx = max_slot_.load(std::memory_order_relaxed);
        if (slot < mn) {
            min_slot_.store(slot, std::memory_order_relaxed);
        }
        if (slot > mx) {
            max_slot_.store(slot, std::memory_order_relaxed);
        }
    }

    void reset_slot_range()
    {
        min_slot_.store(UINT64_MAX, std::memory_order_relaxed);
        max_slot_.store(0, std::memory_order_relaxed);
    }

    // Create a CellVersion from a contiguous cell buffer.
    [[nodiscard]] static CellVersion *make_version(uint64_t slot, uint8_t flags, buffer &&cell)
    {
        auto *cv  = new CellVersion{};
        cv->slot  = slot;
        cv->flags = flags;
        cv->cell  = std::move(cell);
        return cv;
    }

    // Retire a CellVersion via epoch (the deleter destroys the buffer,
    // firing the R30 drop_fn for kExternal cells).
    void retire_version(CellVersion *cv)
    {
        if (cv == nullptr || epoch_ == nullptr) {
            delete cv; // no epoch manager (tests) — immediate free
            return;
        }
        epoch_->retire(cv, [](void *p) { delete static_cast<CellVersion *>(p); });
    }

    // Retire a Node via epoch.
    void retire_node(Node *n)
    {
        if (n == nullptr || epoch_ == nullptr) {
            ConcurrentSkipList::free_node(n); // no epoch manager (tests) — immediate free
            return;
        }
        epoch_->retire(n, ConcurrentSkipList::free_node);
    }

    ConcurrentSkipList    list_;
    EpochManager         *epoch_;
    uint64_t              id_ = 0;
    std::atomic<uint64_t> durable_floor_{0};
    std::atomic<bool>     allow_old_slots_{false};
    std::atomic<uint64_t> min_slot_{UINT64_MAX};
    std::atomic<uint64_t> max_slot_{0};
};

} // namespace crow::tree
