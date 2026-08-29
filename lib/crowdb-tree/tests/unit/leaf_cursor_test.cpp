// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Lazy leaf-chain cursor. Covers key ordering, highest-slot-wins across
// BatchDeltas / base / in-frame deltas, equal-slot tie-breaks in each rank
// position, tombstone GC drop, seek, and equivalence with a reference fold of
// the whole chain (the eager algorithm the cursor replaced).
#include "crowdb-tree/cell.h"
#include "crowdb-tree/delta.h"
#include "crowdb-tree/frame_page.h"
#include "crowdb-tree/leaf_cursor.h"
#include "crowdb-tree/page.h"

#include <gtest/gtest.h>

#include <map>
#include <random>
#include <string>
#include <utility>
#include <vector>

using namespace crowdb::tree;

namespace
{
leaf_entry entry(const std::string &k, uint64_t slot, const std::string &v)
{
    return {.key = k, .cell = encode_cell_buf(slot, OpKind::kPut, Slice(v))};
}

leaf_entry tomb(const std::string &k, uint64_t slot)
{
    return {.key = k, .cell = encode_cell_buf(slot, OpKind::kDelete)};
}

template <class... Es> std::vector<leaf_entry> entries(Es &&...es)
{
    std::vector<leaf_entry> v;
    v.reserve(sizeof...(es));
    (v.push_back(std::forward<Es>(es)), ...);
    return v;
}

void free_chain(PageBase *head)
{
    while (head != nullptr) {
        PageBase *next = head->next;
        delete head;
        head = next;
    }
}

// A LeafBase carrying `deltas` as an in-frame delta overlay (PT12) over
// `sorted_entries`, built the same way apply_batch's in-frame path does.
LeafBase *base_with_inframe(const std::vector<leaf_entry> &sorted_entries, const std::vector<leaf_entry> &deltas)
{
    // LeafBase::build sizes the frame to fit exactly, leaving no room for the
    // overlay, so build the frame at a fixed page size first (as a real leaf
    // frame is, out of the buffer pool) and COW-append the deltas into it.
    constexpr uint32_t   kPageBytes = 4096;
    std::vector<uint8_t> frame(kPageBytes);
    LeafFrameBuilder     b(frame.data(), kPageBytes);
    for (const auto &e : sorted_entries) {
        EXPECT_TRUE(b.try_append_sorted(Slice(e.key), e.cell.slice()));
    }
    b.finish(1, kInvalidPageId);
    std::vector<uint8_t> out(kPageBytes);
    EXPECT_TRUE(leaf_frame_append_deltas(frame.data(), kPageBytes, deltas, out.data()));
    return LeafBase::from_frame_copy(out.data(), kPageBytes);
}

// (key, slot, value-or-empty-for-tombstone) of everything the cursor yields.
struct resolved_entry
{
    std::string key;
    uint64_t    slot;
    bool        tombstone;
    std::string value;

    bool operator==(const resolved_entry &o) const = default;
};

std::vector<resolved_entry> drain(PageBase *head, uint64_t gc_floor)
{
    std::vector<resolved_entry> out;
    for (LeafChainCursor c(head, gc_floor); c.valid(); c.next()) {
        CellView v{c.cell()};
        out.push_back({.key       = c.key().to_string(),
                       .slot      = v.slot(),
                       .tombstone = v.is_tombstone(),
                       .value     = v.is_tombstone() ? "" : v.value().to_string()});
    }
    return out;
}

// Reference fold: the eager whole-chain algorithm the cursor replaced --
// BatchDeltas head to tail, then the base's main entries, then its in-frame
// overlay, first-seen-wins unless a strictly higher slot supersedes.
std::vector<resolved_entry> reference_fold(PageBase *head, uint64_t gc_floor)
{
    std::map<Slice, Slice> resolved;
    auto                   consider = [&](Slice key, Slice cell) {
        auto it = resolved.find(key);
        if (it == resolved.end()) {
            resolved.emplace(key, cell);
        }
        else if (CellView{cell}.slot() > CellView{it->second}.slot()) {
            it->second = cell;
        }
    };
    for (PageBase *node = head; node != nullptr; node = node->next) {
        if (node->type == page_type::kBatchDelta) {
            for (const leaf_entry &e : static_cast<BatchDelta *>(node)->entries()) {
                consider(Slice(e.key), Slice(e.cell));
            }
        }
        else if (node->type == page_type::kLeafBase) {
            LeafFrameView v = static_cast<LeafBase *>(node)->view();
            for (uint32_t i = 0; i < v.count(); ++i) {
                consider(v.key(i), v.cell(i));
            }
            for (uint32_t i = 0; i < v.delta_count(); ++i) {
                consider(v.delta_key(i), v.delta_cell(i));
            }
        }
    }
    std::vector<resolved_entry> out;
    for (auto &kv : resolved) {
        CellView v{kv.second};
        if (v.is_tombstone() && v.slot() <= gc_floor) {
            continue;
        }
        out.push_back({.key       = kv.first.to_string(),
                       .slot      = v.slot(),
                       .tombstone = v.is_tombstone(),
                       .value     = v.is_tombstone() ? "" : v.value().to_string()});
    }
    return out;
}

std::vector<std::string> keys_of(const std::vector<resolved_entry> &v)
{
    std::vector<std::string> out;
    out.reserve(v.size());
    for (const auto &e : v) {
        out.push_back(e.key);
    }
    return out;
}
} // namespace

TEST(LeafCursor, EmptyChainYieldsNothing)
{
    EXPECT_TRUE(drain(nullptr, 0).empty());
    auto *base = LeafBase::build({});
    EXPECT_TRUE(drain(base, 0).empty());
    free_chain(base);
}

TEST(LeafCursor, BaseOnlyInKeyOrder)
{
    auto *base = LeafBase::build(entries(entry("a", 1, "a1"), entry("b", 2, "b2"), entry("c", 3, "c3")));
    EXPECT_EQ(keys_of(drain(base, 0)), (std::vector<std::string>{"a", "b", "c"}));
    free_chain(base);
}

TEST(LeafCursor, DeltaOverlayHighestSlotWins)
{
    auto *base = LeafBase::build(entries(entry("a", 1, "old"), entry("c", 1, "c1")));
    auto *d1   = BatchDelta::build(4, entries(entry("a", 4, "new"), entry("b", 4, "b4")), base);
    auto  got  = drain(d1, 0);
    ASSERT_EQ(got.size(), 3U);
    EXPECT_EQ(got[0].key, "a");
    EXPECT_EQ(got[0].value, "new");
    EXPECT_EQ(got[1].key, "b");
    EXPECT_EQ(got[2].key, "c");
    free_chain(d1);
}

TEST(LeafCursor, LowerSlotDeltaDoesNotShadowBase)
{
    // A stale re-apply prepended over a newer base entry: the base still wins.
    auto *base = LeafBase::build(entries(entry("a", 10, "ten")));
    auto *d1   = BatchDelta::build(4, entries(entry("a", 4, "four")), base);
    auto  got  = drain(d1, 0);
    ASSERT_EQ(got.size(), 1U);
    EXPECT_EQ(got[0].slot, 10U);
    EXPECT_EQ(got[0].value, "ten");
    free_chain(d1);
}

TEST(LeafCursor, MultiDeltaChainMerges)
{
    auto *base = LeafBase::build(entries(entry("a", 1, "a1"), entry("e", 1, "e1")));
    auto *d1   = BatchDelta::build(2, entries(entry("b", 2, "b2"), entry("e", 2, "e2")), base);
    auto *d2   = BatchDelta::build(3, entries(entry("c", 3, "c3"), entry("e", 3, "e3")), d1);
    auto *d3   = BatchDelta::build(4, entries(entry("d", 4, "d4")), d2);
    auto  got  = drain(d3, 0);
    EXPECT_EQ(keys_of(got), (std::vector<std::string>{"a", "b", "c", "d", "e"}));
    EXPECT_EQ(got[4].value, "e3"); // highest slot across the chain
    free_chain(d3);
}

TEST(LeafCursor, EqualSlotTieBreaksToTheEarlierChainPosition)
{
    // Same slot in a BatchDelta and in the base: the delta is visited first.
    auto *base = LeafBase::build(entries(entry("a", 7, "base")));
    auto *d1   = BatchDelta::build(7, entries(entry("a", 7, "delta")), base);
    auto  got  = drain(d1, 0);
    ASSERT_EQ(got.size(), 1U);
    EXPECT_EQ(got[0].value, "delta");
    free_chain(d1);
}

TEST(LeafCursor, EqualSlotBaseBeatsInframeDelta)
{
    // Base main entries are visited before the in-frame overlay, so on an equal
    // slot the base wins (only a strictly higher slot supersedes).
    LeafBase *leaf = base_with_inframe(entries(entry("a", 7, "base")), entries(entry("a", 7, "inframe")));
    auto      got  = drain(leaf, 0);
    ASSERT_EQ(got.size(), 1U);
    EXPECT_EQ(got[0].value, "base");
    free_chain(leaf);
}

TEST(LeafCursor, InframeDeltaOverlaySortsAndWins)
{
    // Appended out of key order and with a duplicate key: the overlay is sorted
    // and deduped, and its higher slots shadow the base.
    LeafBase *leaf =
        base_with_inframe(entries(entry("a", 1, "a1"), entry("c", 1, "c1")),
                          entries(entry("d", 5, "d5"), entry("a", 6, "a6"), entry("a", 9, "a9"), entry("b", 7, "b7")));
    auto got = drain(leaf, 0);
    EXPECT_EQ(keys_of(got), (std::vector<std::string>{"a", "b", "c", "d"}));
    EXPECT_EQ(got[0].value, "a9"); // highest slot among the duplicate overlay keys
    EXPECT_EQ(got[2].value, "c1"); // untouched base entry
    free_chain(leaf);
}

TEST(LeafCursor, EqualSlotInframeDuplicateKeepsTheEarlierAppend)
{
    LeafBase *leaf =
        base_with_inframe(entries(entry("z", 1, "z1")), entries(entry("a", 5, "first"), entry("a", 5, "second")));
    auto got = drain(leaf, 0);
    ASSERT_EQ(got.size(), 2U);
    EXPECT_EQ(got[0].value, "first");
    free_chain(leaf);
}

TEST(LeafCursor, TombstonesKeptUntilGcFloor)
{
    auto *base = LeafBase::build(entries(entry("a", 1, "a1"), tomb("b", 5), entry("c", 6, "c6")));
    auto  kept = drain(base, 4);
    ASSERT_EQ(kept.size(), 3U);
    EXPECT_TRUE(kept[1].tombstone);
    // gc_floor at/above the tombstone's slot drops it.
    EXPECT_EQ(keys_of(drain(base, 5)), (std::vector<std::string>{"a", "c"}));
    free_chain(base);
}

TEST(LeafCursor, AllEntriesDroppedYieldsNothing)
{
    auto *base = LeafBase::build(entries(tomb("a", 1), tomb("b", 2)));
    EXPECT_TRUE(drain(base, 9).empty());
    free_chain(base);
}

TEST(LeafCursor, SeekInclusiveAndExclusive)
{
    auto *base = LeafBase::build(entries(entry("a", 1, "a1"), entry("c", 1, "c1"), entry("e", 1, "e1")));
    auto *d1   = BatchDelta::build(2, entries(entry("b", 2, "b2"), entry("d", 2, "d2")), base);

    LeafChainCursor cur(d1, 0);
    cur.seek(Slice("c"), /*exclusive=*/false);
    std::vector<std::string> got;
    for (; cur.valid(); cur.next()) {
        got.push_back(cur.key().to_string());
    }
    EXPECT_EQ(got, (std::vector<std::string>{"c", "d", "e"}));

    cur.reset(d1, 0);
    cur.seek(Slice("c"), /*exclusive=*/true);
    got.clear();
    for (; cur.valid(); cur.next()) {
        got.push_back(cur.key().to_string());
    }
    EXPECT_EQ(got, (std::vector<std::string>{"d", "e"}));

    cur.reset(d1, 0);
    cur.seek(Slice("zz"), /*exclusive=*/false);
    EXPECT_FALSE(cur.valid());
    free_chain(d1);
}

TEST(LeafCursor, SeekPastAGcDroppedTombstone)
{
    auto           *base = LeafBase::build(entries(entry("a", 1, "a1"), tomb("b", 2), entry("c", 3, "c3")));
    LeafChainCursor cur(base, 2);
    cur.seek(Slice("b"), /*exclusive=*/false);
    ASSERT_TRUE(cur.valid());
    EXPECT_EQ(cur.key().to_string(), "c");
    free_chain(base);
}

TEST(LeafCursor, MatchesReferenceFoldOnRandomChains)
{
    std::mt19937 rng(20260806);
    for (int iter = 0; iter < 200; ++iter) {
        std::uniform_int_distribution<int> key_d(0, 25);
        std::uniform_int_distribution<int> slot_d(1, 6);
        std::uniform_int_distribution<int> n_d(0, 8);

        auto make = [&](int n) {
            std::map<std::string, leaf_entry> uniq; // BatchDelta/base require sorted unique keys
            for (int i = 0; i < n; ++i) {
                std::string k(1, static_cast<char>('a' + key_d(rng)));
                auto        slot = static_cast<uint64_t>(slot_d(rng));
                leaf_entry  e    = (slot_d(rng) == 1) ? tomb(k, slot) : entry(k, slot, k + std::to_string(slot));
                uniq.insert_or_assign(k, std::move(e));
            }
            std::vector<leaf_entry> out;
            out.reserve(uniq.size());
            for (auto &kv : uniq) {
                out.push_back(std::move(kv.second));
            }
            return out;
        };

        // In-frame deltas may repeat keys and are appended out of order.
        std::vector<leaf_entry> inframe;
        for (int i = 0, n = n_d(rng); i < n; ++i) {
            std::string k(1, static_cast<char>('a' + key_d(rng)));
            auto        slot = static_cast<uint64_t>(slot_d(rng));
            inframe.push_back(entry(k, slot, k + "#" + std::to_string(slot)));
        }
        PageBase *head = inframe.empty() ? LeafBase::build(make(n_d(rng))) : base_with_inframe(make(n_d(rng)), inframe);
        for (int i = 0, n = n_d(rng); i < n; ++i) {
            head = BatchDelta::build(static_cast<uint64_t>(slot_d(rng)), make(n_d(rng)), head);
        }

        for (uint64_t gc_floor : {uint64_t{0}, uint64_t{3}, uint64_t{9}}) {
            EXPECT_EQ(drain(head, gc_floor), reference_fold(head, gc_floor)) << "iter=" << iter;
        }
        free_chain(head);
    }
}
