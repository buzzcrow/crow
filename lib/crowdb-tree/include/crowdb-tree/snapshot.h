// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Consistent point-in-time view. A Snapshot is an immutable, key-sorted materialization of the
// L1 tree at a given slot, used for scan-at / compare / iter_all / export.
//
// NOTE (deviation): the design specifies a snapshot as a pinned immutable COW
// root (zero-copy). The in-memory core materializes the keyspace into an
// independent immutable copy instead; this is correct and keeps writes lock-free
// for latest reads, at O(N) snapshot cost. Path-copy COW is a later optimization.
//
// R6: PinnedSnapshot (below) is the zero-copy variant for the cross-thread
// handoff paths. It inherits from Snapshot so callers that hold
// shared_ptr<Snapshot> and call entries()/get()/compare() work unchanged; the
// difference is that entries() materializes lazily from pinned frames on first
// call, and the pages stay alive via per-page refcount (pin_state_ on PageBase)
// until the PinnedSnapshot is dropped — on any thread.
#pragma once

#include "crowdb-tree/cell.h"
#include "crowdb-tree/page.h"
#include "crowdb-tree/page_types.h"
#include "crowdb-tree/slice.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace crowdb::tree
{

// A difference between two snapshots (for compare / parity).
struct engine_diff
{
    std::string key;

    enum Kind : uint8_t { kOnlyLeft, kOnlyRight, kSlotDiffers, kValueDiffers } kind;
};

class Snapshot
{
  public:
    Snapshot(uint64_t at_slot, std::vector<leaf_entry> sorted_with_tombstones)
        : at_slot_(at_slot),
          entries_(std::move(sorted_with_tombstones))
    {
    }

    // R6: PinnedSnapshot lazily materializes entries_ from pinned frames on
    // first call. Base Snapshot already has entries_ populated in the ctor.
    virtual ~Snapshot() = default;

    [[nodiscard]] uint64_t at_slot() const
    {
        return at_slot_;
    }

    // All entries including tombstones, key-sorted. Virtual so PinnedSnapshot
    // can lazily materialize from pinned frames on first call.
    [[nodiscard]] virtual const std::vector<leaf_entry> &entries() const
    {
        return entries_;
    }

    [[nodiscard]] virtual size_t size() const
    {
        return entries().size();
    }

    [[nodiscard]] virtual int find(Slice key) const
    {
        const auto &e  = entries();
        size_t      lo = 0;
        size_t      hi = e.size();
        while (lo < hi) {
            size_t mid = lo + ((hi - lo) / 2);
            int    c   = Slice(e[mid].key).compare(key);
            if (c == 0) {
                return static_cast<int>(mid);
            }
            if (c < 0) {
                lo = mid + 1;
            }
            else {
                hi = mid;
            }
        }
        return -1;
    }

    // Live read: false for absent or tombstoned keys.
    [[nodiscard]] virtual bool get(Slice key, uint64_t *out_slot, std::string *out_value) const
    {
        int i = find(key);
        if (i < 0) {
            return false;
        }
        CellView v{Slice(entries()[i].cell)};
        if (v.is_tombstone()) {
            return false;
        }
        if (out_slot != nullptr) {
            *out_slot = v.slot();
        }
        if (out_value != nullptr) {
            *out_value = v.value().to_string();
        }
        return true;
    }

    // Structural comparison including tombstones (used by parity tests).
    [[nodiscard]] std::vector<engine_diff> compare(const Snapshot &other) const
    {
        std::vector<engine_diff> diffs;
        size_t                   i = 0;
        size_t                   j = 0;
        const auto              &a = entries();
        const auto              &b = other.entries();
        while (i < a.size() && j < b.size()) {
            int c = Slice(a[i].key).compare(Slice(b[j].key));
            if (c < 0) {
                diffs.push_back({a[i].key, engine_diff::kOnlyLeft});
                ++i;
            }
            else if (c > 0) {
                diffs.push_back({b[j].key, engine_diff::kOnlyRight});
                ++j;
            }
            else {
                CellView va{Slice(a[i].cell)};
                CellView vb{Slice(b[j].cell)};
                if (va.slot() != vb.slot()) {
                    diffs.push_back({a[i].key, engine_diff::kSlotDiffers});
                }
                else if (va.raw() != vb.raw()) {
                    diffs.push_back({a[i].key, engine_diff::kValueDiffers});
                }
                ++i;
                ++j;
            }
        }
        for (; i < a.size(); ++i) {
            diffs.push_back({a[i].key, engine_diff::kOnlyLeft});
        }
        for (; j < b.size(); ++j) {
            diffs.push_back({b[j].key, engine_diff::kOnlyRight});
        }
        return diffs;
    }

  protected:
    // R6: PinnedSnapshot populates entries_ lazily; mutable so entries() can
    // materialize on first call through a const method.
    uint64_t                        at_slot_;
    mutable std::vector<leaf_entry> entries_;
};

// R6: zero-copy pinned snapshot. Captures PageBase* pointers during the
// snapshot_view() walk (under an epoch guard), pins each via pin_state_, then
// releases the guard. The pages stay alive via refcount until this snapshot is
// dropped — on any thread. entries() materializes lazily from the pinned frames
// on first call (callers that only use get()/find()/size() never pay the O(N)
// materialization cost; callers that need the flat vector pay it once).
//
// The pinned pages include the full leaf chain (head → ... → base for each
// leaf visited — every delta node, not just the head) and any overflow pages
// whose values are referenced by leaf entries. Overflow values are assembled
// from pinned overflow frames during materialization.
class PinnedSnapshot : public Snapshot
{
  public:
    // `leaf_chain_heads` is the ordered list of chain heads (one per leaf,
    // left-to-right). `all_pinned_pages` is every page to pin (all chain
    // nodes + overflow pages). `overflow_pages` are the overflow chain pages
    // referenced by leaf entries (subset of all_pinned_pages, used by
    // materialize() to assemble overflow values).
    PinnedSnapshot(uint64_t at_slot, std::vector<PageBase *> leaf_chain_heads, std::vector<PageBase *> all_pinned_pages,
                   std::vector<PageBase *> overflow_pages)
        : Snapshot(at_slot, {}),
          leaf_chain_heads_(std::move(leaf_chain_heads)),
          all_pinned_pages_(std::move(all_pinned_pages)),
          overflow_pages_(std::move(overflow_pages))
    {
        for (PageBase *p : all_pinned_pages_) {
            p->pin();
        }
    }

    ~PinnedSnapshot() override
    {
        for (PageBase *p : all_pinned_pages_) {
            p->unpin();
        }
    }

    PinnedSnapshot(const PinnedSnapshot &)            = delete;
    PinnedSnapshot &operator=(const PinnedSnapshot &) = delete;
    PinnedSnapshot(PinnedSnapshot &&)                 = delete;
    PinnedSnapshot &operator=(PinnedSnapshot &&)      = delete;

    // Lazily materialize entries_ from the pinned leaf chain frames on first
    // call. Walks the captured leaf chain heads (in order), resolves each
    // chain via resolve_chain_sorted, and assembles overflow values from the
    // pinned overflow pages. Subsequent calls return the cached vector.
    [[nodiscard]] const std::vector<leaf_entry> &entries() const override
    {
        if (entries_.empty() && !leaf_chain_heads_.empty()) {
            materialize();
        }
        return entries_;
    }

    [[nodiscard]] size_t size() const override
    {
        return entries().size();
    }

  private:
    // The ordered list of leaf chain heads (one per leaf, left-to-right).
    // materialize() calls resolve_chain_sorted on each, following head->next
    // (the delta chain) which is kept alive by all_pinned_pages_.
    std::vector<PageBase *> leaf_chain_heads_;
    // Every page to pin/unpin (all chain nodes + overflow pages).
    std::vector<PageBase *> all_pinned_pages_;
    // Overflow chain pages (subset of all_pinned_pages_), used by materialize()
    // to assemble overflow values from pinned frames.
    std::vector<PageBase *> overflow_pages_;

    void materialize() const;
};

} // namespace crowdb::tree
