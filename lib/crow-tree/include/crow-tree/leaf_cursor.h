// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Lazy leaf-chain cursor.
//
// Produces one leaf chain's live entries in key order, on demand, without
// materializing the page. Every chain input is already key-sorted (a
// BatchDelta's entries, the LeafBase's main frame slots) except the in-frame
// delta overlay, which is small and pre-sorted at reset(). The cursor merges
// them k-way and resolves highest-slot-wins per key, so producing an entry
// costs O(chain length) instead of O(entries-per-leaf) -- a limit-bounded scan
// stops after `limit` entries instead of rebuilding the whole page.
//
// Keys and cells are returned as Slices borrowed from the chain's own storage;
// they stay valid as long as the caller keeps the chain alive (an epoch guard
// on the read paths, direct ownership on a write path).
#pragma once

#include "crow-tree/cell.h"
#include "crow-tree/delta.h"
#include "crow-tree/page.h"
#include "crow-tree/slice.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace crow::tree
{

class LeafChainCursor
{
  public:
    LeafChainCursor() = default;

    LeafChainCursor(PageBase *head, uint64_t gc_floor)
    {
        reset(head, gc_floor);
    }

    // Point the cursor at a chain (head -> ... -> LeafBase) and position it on
    // the first live entry. Tombstones with slot <= gc_floor are skipped
    // (logical retention GC); all other tombstones are produced.
    void reset(PageBase *head, uint64_t gc_floor)
    {
        sources_.clear();
        inframe_order_.clear();
        gc_floor_ = gc_floor;
        valid_    = false;
        // Rank encodes the position at which a source's entries were visited by
        // the original fold (BatchDeltas head->tail, then the base's main
        // entries, then the base's in-frame overlay), so an equal-slot tie can
        // be broken the same way: lowest rank -- i.e. first visited -- wins.
        uint32_t node = 0;
        for (PageBase *n = head; n != nullptr; n = n->next, ++node) {
            if (n->type == page_type::kBatchDelta) {
                const auto &entries = static_cast<BatchDelta *>(n)->entries();
                if (!entries.empty()) {
                    sources_.push_back({.kind          = SourceKind::kDelta,
                                        .delta         = &entries,
                                        .leaf          = nullptr,
                                        .inframe_begin = 0,
                                        .idx           = 0,
                                        .count         = static_cast<uint32_t>(entries.size()),
                                        .rank          = 2 * node});
                }
            }
            else if (n->type == page_type::kLeafBase) {
                auto         *leaf = static_cast<LeafBase *>(n);
                LeafFrameView v    = leaf->view();
                if (v.count() > 0) {
                    sources_.push_back({.kind          = SourceKind::kBase,
                                        .delta         = nullptr,
                                        .leaf          = leaf,
                                        .inframe_begin = 0,
                                        .idx           = 0,
                                        .count         = v.count(),
                                        .rank          = 2 * node});
                }
                if (v.delta_count() > 0) {
                    auto begin = static_cast<uint32_t>(inframe_order_.size());
                    sort_inframe(v);
                    sources_.push_back({.kind          = SourceKind::kInframe,
                                        .delta         = nullptr,
                                        .leaf          = leaf,
                                        .inframe_begin = begin,
                                        .idx           = 0,
                                        .count         = static_cast<uint32_t>(inframe_order_.size()) - begin,
                                        .rank          = (2 * node) + 1});
                }
            }
        }
        advance();
    }

    // Position on the first entry >= `key` (or > `key` when `exclusive`), by
    // binary search per source -- earlier entries are never touched.
    void seek(Slice key, bool exclusive)
    {
        for (Source &s : sources_) {
            s.idx = lower_bound(s, key);
            while (exclusive && s.idx < s.count && key_at(s, s.idx).compare(key) == 0) {
                ++s.idx;
            }
        }
        advance();
    }

    [[nodiscard]] bool valid() const
    {
        return valid_;
    }

    [[nodiscard]] Slice key() const
    {
        return key_;
    }

    [[nodiscard]] Slice cell() const
    {
        return cell_;
    }

    void next()
    {
        advance();
    }

    // Upper bound on the entries left (duplicates across sources and GC'd
    // tombstones may make the real count smaller). For reserve() only.
    [[nodiscard]] size_t remaining_hint() const
    {
        size_t n = valid_ ? 1 : 0;
        for (const Source &s : sources_) {
            n += s.count - s.idx;
        }
        return n;
    }

  private:
    enum class SourceKind : uint8_t { kDelta, kBase, kInframe };

    // One key-sorted, key-unique stream of the chain. kInframe indexes
    // inframe_order_ rather than the frame's delta slots directly.
    struct Source
    {
        SourceKind                     kind          = SourceKind::kDelta;
        const std::vector<leaf_entry> *delta         = nullptr;
        const LeafBase                *leaf          = nullptr;
        uint32_t                       inframe_begin = 0;
        uint32_t                       idx           = 0;
        uint32_t                       count         = 0;
        uint32_t                       rank          = 0;
    };

    [[nodiscard]] Slice key_at(const Source &s, uint32_t i) const
    {
        switch (s.kind) {
        case SourceKind::kDelta:
            return {(*s.delta)[i].key};
        case SourceKind::kBase:
            return s.leaf->view().key(i);
        case SourceKind::kInframe:
            return s.leaf->view().delta_key(inframe_order_[s.inframe_begin + i]);
        }
        return {};
    }

    [[nodiscard]] Slice cell_at(const Source &s, uint32_t i) const
    {
        switch (s.kind) {
        case SourceKind::kDelta:
            return Slice((*s.delta)[i].cell);
        case SourceKind::kBase:
            return s.leaf->view().cell(i);
        case SourceKind::kInframe:
            return s.leaf->view().delta_cell(inframe_order_[s.inframe_begin + i]);
        }
        return {};
    }

    [[nodiscard]] uint32_t lower_bound(const Source &s, Slice key) const
    {
        if (s.kind == SourceKind::kBase) {
            return s.leaf->view().lower_bound(key);
        }
        uint32_t lo = 0;
        uint32_t hi = s.count;
        while (lo < hi) {
            uint32_t mid = lo + ((hi - lo) / 2);
            if (key_at(s, mid).compare(key) < 0) {
                lo = mid + 1;
            }
            else {
                hi = mid;
            }
        }
        return lo;
    }

    // The in-frame overlay (PT12) is appended in slot order, so it is the only
    // unsorted -- and the only key-duplicating -- input. Sort its indices by
    // key (ties keeping append order) and keep one winner per key, matching the
    // fold's rule: highest slot, equal slot resolved by the lower index.
    // Bounded by Options::max_inframe_delta (default 8).
    void sort_inframe(const LeafFrameView &v)
    {
        size_t   begin = inframe_order_.size();
        uint32_t dc    = v.delta_count();
        inframe_order_.reserve(begin + dc);
        for (uint32_t i = 0; i < dc; ++i) {
            inframe_order_.push_back(i);
        }
        auto first = inframe_order_.begin() + static_cast<ptrdiff_t>(begin);
        std::stable_sort(first, inframe_order_.end(),
                         [&v](uint32_t a, uint32_t b) { return v.delta_key(a).compare(v.delta_key(b)) < 0; });
        size_t out = begin;
        for (size_t i = begin; i < inframe_order_.size(); ++i) {
            if (out > begin && v.delta_key(inframe_order_[out - 1]).compare(v.delta_key(inframe_order_[i])) == 0) {
                if (CellView{v.delta_cell(inframe_order_[i])}.slot() >
                    CellView{v.delta_cell(inframe_order_[out - 1])}.slot()) {
                    inframe_order_[out - 1] = inframe_order_[i];
                }
                continue;
            }
            inframe_order_[out++] = inframe_order_[i];
        }
        inframe_order_.resize(out);
    }

    // Produce the next live entry: smallest key across the source heads, then
    // highest-slot-wins (lowest rank on a tie) among the sources sitting on it.
    void advance()
    {
        while (true) {
            Slice min_key;
            bool  has_any = false;
            for (const Source &s : sources_) {
                if (s.idx >= s.count) {
                    continue;
                }
                Slice k = key_at(s, s.idx);
                if (!has_any || k.compare(min_key) < 0) {
                    min_key = k;
                    has_any = true;
                }
            }
            if (!has_any) {
                valid_ = false;
                return;
            }

            Slice    win_cell;
            uint64_t win_slot = 0;
            uint32_t win_rank = 0;
            bool     have_win = false;
            for (Source &s : sources_) {
                if (s.idx >= s.count || key_at(s, s.idx).compare(min_key) != 0) {
                    continue;
                }
                Slice    c    = cell_at(s, s.idx);
                uint64_t slot = CellView{c}.slot();
                if (!have_win || slot > win_slot || (slot == win_slot && s.rank < win_rank)) {
                    win_cell = c;
                    win_slot = slot;
                    win_rank = s.rank;
                    have_win = true;
                }
                ++s.idx;
            }

            CellView v{win_cell};
            if (v.is_tombstone() && v.slot() <= gc_floor_) {
                continue; // GC drop
            }
            key_   = min_key;
            cell_  = win_cell;
            valid_ = true;
            return;
        }
    }

    std::vector<Source>   sources_;
    std::vector<uint32_t> inframe_order_;
    uint64_t              gc_floor_ = 0;
    Slice                 key_;
    Slice                 cell_;
    bool                  valid_ = false;
};

} // namespace crow::tree
