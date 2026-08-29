// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// CT8: delta record tests (build, find_key, chain resolve, tombstone shadow).
#include "crowdb-tree/cell.h"
#include "crowdb-tree/delta.h"
#include "crowdb-tree/page.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
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

// Free an entire chain (deltas + base).
void free_chain(PageBase *head)
{
    while (head != nullptr) {
        PageBase *next = head->next;
        delete head;
        head = next;
    }
}
} // namespace

TEST(Delta, BuildAndFindKey)
{
    auto *d = BatchDelta::build(5, entries(entry("a", 5, "x"), entry("c", 5, "y"), entry("e", 5, "z")), nullptr);
    std::unique_ptr<BatchDelta> guard(d);
    EXPECT_EQ(d->slot(), 5U);
    EXPECT_EQ(d->count(), 3U);
    EXPECT_EQ(d->find_key("a"), 0);
    EXPECT_EQ(d->find_key("c"), 1);
    EXPECT_EQ(d->find_key("e"), 2);
    EXPECT_EQ(d->find_key("b"), -1);
    EXPECT_EQ(d->find_key("z"), -1);
}

TEST(Delta, ChainLinkage)
{
    auto *base = LeafBase::build(entries(entry("a", 1, "a1")));
    auto *d1   = BatchDelta::build(2, entries(entry("b", 2, "b2")), base);
    auto *d2   = BatchDelta::build(3, entries(entry("c", 3, "c3")), d1);
    EXPECT_EQ(d2->delta_len, 2U);
    EXPECT_GT(d2->chain_bytes, d1->chain_bytes);
    EXPECT_EQ(d2->next, d1);
    EXPECT_EQ(d1->next, base);
    free_chain(d2);
}

TEST(Delta, ResolveHighestSlotWins)
{
    // base has a@1; d1 overwrites a@4; d2 has b@5.
    auto *base = LeafBase::build(entries(entry("a", 1, "old")));
    auto *d1   = BatchDelta::build(4, entries(entry("a", 4, "new")), base);
    auto *d2   = BatchDelta::build(5, entries(entry("b", 5, "b5")), d1);

    CellView v;
    ASSERT_TRUE(resolve_chain(d2, "a", &v));
    EXPECT_EQ(v.slot(), 4U);
    EXPECT_EQ(v.value().to_string(), "new");

    ASSERT_TRUE(resolve_chain(d2, "b", &v));
    EXPECT_EQ(v.value().to_string(), "b5");

    EXPECT_FALSE(resolve_chain(d2, "missing", &v));
    free_chain(d2);
}

TEST(Delta, ResolveOutOfOrderSlots)
{
    // A lower-slot delta prepended above a higher-slot base entry: highest wins.
    auto    *base = LeafBase::build(entries(entry("a", 10, "ten")));
    auto    *d1   = BatchDelta::build(4, entries(entry("a", 4, "four")), base); // stale re-apply
    CellView v;
    ASSERT_TRUE(resolve_chain(d1, "a", &v));
    EXPECT_EQ(v.slot(), 10U); // base's higher slot still wins
    EXPECT_EQ(v.value().to_string(), "ten");
    free_chain(d1);
}

TEST(Delta, TombstoneShadows)
{
    auto    *base = LeafBase::build(entries(entry("a", 1, "v")));
    auto    *d1   = BatchDelta::build(2, entries(tomb("a", 2)), base);
    CellView v;
    ASSERT_TRUE(resolve_chain(d1, "a", &v));
    EXPECT_TRUE(v.is_tombstone());
    EXPECT_EQ(v.slot(), 2U);
    free_chain(d1);
}

TEST(Delta, ChainLeafBaseHelper)
{
    auto *base = LeafBase::build(entries(entry("a", 1, "v")));
    auto *d1   = BatchDelta::build(2, entries(entry("b", 2, "w")), base);
    EXPECT_EQ(chain_leaf_base(d1), base);
    EXPECT_EQ(chain_leaf_base(base), base);
    free_chain(d1);
}
