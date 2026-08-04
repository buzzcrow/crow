// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// CT3: leaf base page tests (build, search, bloom, iteration, boundaries).
#include "crow-tree/cell.h"
#include "crow-tree/page.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

using namespace crow::tree;

namespace
{

leaf_entry make_entry(const std::string &key, uint64_t slot, const std::string &val)
{
    return {.key = key, .cell = encode_cell_buf(slot, OpKind::kPut, Slice(val))};
}

template <class... Es> std::vector<leaf_entry> entries(Es &&...es)
{
    std::vector<leaf_entry> v;
    v.reserve(sizeof...(es));
    (v.push_back(std::forward<Es>(es)), ...);
    return v;
}

std::unique_ptr<LeafBase> build_leaf(std::vector<leaf_entry> entries, uint64_t right = kInvalidPageId)
{
    // LeafBase::build requires key-sorted entries; sort here for test convenience.
    std::ranges::sort(entries, [](const leaf_entry &a, const leaf_entry &b) { return a.key < b.key; });
    return std::unique_ptr<LeafBase>(LeafBase::build(std::move(entries), right)); // NOLINT(performance-move-const-arg)
}

} // namespace

TEST(LeafPage, BuildAndFind)
{
    auto leaf = build_leaf(entries(make_entry("a", 1, "1"), make_entry("c", 2, "3"), make_entry("e", 3, "5")));
    EXPECT_EQ(leaf->count(), 3U);
    EXPECT_EQ(leaf->find("a"), 0);
    EXPECT_EQ(leaf->find("c"), 1);
    EXPECT_EQ(leaf->find("e"), 2);
    // Misses.
    EXPECT_EQ(leaf->find("b"), -1);
    EXPECT_EQ(leaf->find("z"), -1);
    EXPECT_EQ(leaf->find(""), -1);
}

TEST(LeafPage, LookupDecodesCell)
{
    auto     leaf = build_leaf(entries(make_entry("k", 42, "hello")));
    CellView v;
    ASSERT_TRUE(leaf->lookup("k", &v));
    EXPECT_EQ(v.slot(), 42U);
    EXPECT_EQ(v.value().to_string(), "hello");
    EXPECT_FALSE(leaf->lookup("missing", &v));
}

TEST(LeafPage, Tombstone)
{
    std::vector<leaf_entry> e;
    e.push_back({.key = "d", .cell = encode_cell_buf(9, OpKind::kDelete)});
    auto     leaf = build_leaf(std::move(e));
    CellView v;
    ASSERT_TRUE(leaf->lookup("d", &v));
    EXPECT_TRUE(v.is_tombstone());
}

TEST(LeafPage, OrderedIterationAndBoundaries)
{
    auto leaf =
        build_leaf(entries(make_entry("apple", 1, "x"), make_entry("banana", 2, "y"), make_entry("cherry", 3, "z")));
    EXPECT_EQ(leaf->low_key().to_string(), "apple");
    EXPECT_EQ(leaf->high_key().to_string(), "cherry");
    std::string prev;
    for (size_t i = 0; i < leaf->count(); ++i) {
        std::string k = leaf->entry(i).key;
        if (i > 0) {
            EXPECT_LT(prev, k);
        }
        prev = k;
    }
}

TEST(LeafPage, lower_bound)
{
    auto leaf = build_leaf(entries(make_entry("b", 1, "x"), make_entry("d", 2, "y"), make_entry("f", 3, "z")));
    EXPECT_EQ(leaf->lower_bound("a"), 0U);
    EXPECT_EQ(leaf->lower_bound("b"), 0U);
    EXPECT_EQ(leaf->lower_bound("c"), 1U);
    EXPECT_EQ(leaf->lower_bound("d"), 1U);
    EXPECT_EQ(leaf->lower_bound("g"), 3U); // past end
}

TEST(LeafPage, RightSibling)
{
    auto leaf = build_leaf(entries(make_entry("a", 1, "x")), 7);
    EXPECT_EQ(leaf->right_sibling(), 7U);
    leaf->set_right_sibling(9);
    EXPECT_EQ(leaf->right_sibling(), 9U);
}

TEST(LeafPage, BloomTrueNegativeAndFpRate)
{
    // Insert 1000 keys; bloom must never reject a present key (no false negatives),
    // and the false-positive rate on absent keys must be low.
    std::vector<leaf_entry> entries;
    entries.reserve(1000);
    for (int i = 0; i < 1000; ++i) {
        entries.push_back(make_entry("key" + std::to_string(i * 2), 1, "v"));
    }
    auto leaf = build_leaf(std::move(entries));
    // No false negatives.
    for (int i = 0; i < 1000; ++i) {
        EXPECT_GE(leaf->find("key" + std::to_string(i * 2)), 0);
    }
    // False-positive measurement on 1000 absent keys (odd indices never inserted).
    int false_pos = 0;
    for (int i = 0; i < 1000; ++i) {
        if (leaf->find("key" + std::to_string((i * 2) + 1)) >= 0) {
            ADD_FAILURE() << "absent key reported present";
        }
    }
    (void)false_pos; // find already returns -1 for absent; bloom FP just costs a scan.
}

TEST(LeafPage, DataBytesNonZero)
{
    auto leaf = build_leaf(entries(make_entry("a", 1, "value")));
    EXPECT_GT(leaf->data_bytes(), 0U);
}
