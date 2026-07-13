// CT4: inner base page + tree descent tests.
#include "crowtree/descent.h"
#include "crowtree/mapping_table.h"
#include "crowtree/page.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

using namespace crowtree;

namespace
{
// Dummy leaf entry for descent tests (the cell bytes are never decoded here).
leaf_entry make_entry(const char *k)
{
    return {.key = k, .cell = buffer::copy_of(Slice("x"))};
}

template <class... Es> std::vector<leaf_entry> make_entries(Es &&...es)
{
    std::vector<leaf_entry> v;
    v.reserve(sizeof...(es));
    (v.push_back(std::forward<Es>(es)), ...);
    return v;
}
} // namespace

TEST(InnerPage, child_index_for)
{
    // separators = [d, k, q]; children = [c0, c1, c2, c3]
    auto                      *inner = InnerBase::build({"d", "k", "q"}, {10, 11, 12, 13});
    std::unique_ptr<InnerBase> guard(inner);
    EXPECT_EQ(inner->child_index_for("a"), 0U); // < d
    EXPECT_EQ(inner->child_index_for("d"), 1U); // == d -> right
    EXPECT_EQ(inner->child_index_for("e"), 1U); // d <= e < k
    EXPECT_EQ(inner->child_index_for("k"), 2U); // == k
    EXPECT_EQ(inner->child_index_for("p"), 2U);
    EXPECT_EQ(inner->child_index_for("q"), 3U); // == q
    EXPECT_EQ(inner->child_index_for("z"), 3U); // > q
    EXPECT_EQ(inner->child_for("e"), 11U);
    EXPECT_EQ(inner->child_for("z"), 13U);
}

TEST(Descent, SingleLeafRoot)
{
    MappingTable mt;
    uint64_t     leaf_page_id = mt.allocate_page_id();
    mt.store(leaf_page_id, LeafBase::build(make_entries(make_entry("a"))));
    EXPECT_EQ(find_leaf_page_id([&](uint64_t p) { return mt.get_resident(p); }, leaf_page_id, "a"), leaf_page_id);
    EXPECT_EQ(find_leaf_page_id([&](uint64_t p) { return mt.get_resident(p); }, leaf_page_id, "zzz"), leaf_page_id);
    delete mt.get_resident(leaf_page_id);
}

TEST(Descent, TwoLevelTree)
{
    MappingTable mt;
    // Three leaves.
    uint64_t l0 = mt.allocate_page_id();
    uint64_t l1 = mt.allocate_page_id();
    uint64_t l2 = mt.allocate_page_id();
    mt.store(l0, LeafBase::build(make_entries(make_entry("a"))));
    mt.store(l1, LeafBase::build(make_entries(make_entry("k"))));
    mt.store(l2, LeafBase::build(make_entries(make_entry("q"))));
    // Root inner: separators [k, q] -> children [l0, l1, l2].
    uint64_t root = mt.allocate_page_id();
    mt.store(root, InnerBase::build({"k", "q"}, {l0, l1, l2}));

    EXPECT_EQ(find_leaf_page_id([&](uint64_t p) { return mt.get_resident(p); }, root, "a"), l0);
    EXPECT_EQ(find_leaf_page_id([&](uint64_t p) { return mt.get_resident(p); }, root, "j"), l0);
    EXPECT_EQ(find_leaf_page_id([&](uint64_t p) { return mt.get_resident(p); }, root, "k"), l1);
    EXPECT_EQ(find_leaf_page_id([&](uint64_t p) { return mt.get_resident(p); }, root, "p"), l1);
    EXPECT_EQ(find_leaf_page_id([&](uint64_t p) { return mt.get_resident(p); }, root, "q"), l2);
    EXPECT_EQ(find_leaf_page_id([&](uint64_t p) { return mt.get_resident(p); }, root, "zz"), l2);

    for (uint64_t page_id : {l0, l1, l2, root}) {
        delete mt.get_resident(page_id);
    }
}

TEST(Descent, ThreeLevelTree)
{
    MappingTable mt;
    // Leaves under two inner nodes, joined by a root.
    uint64_t la = mt.allocate_page_id();
    uint64_t lb = mt.allocate_page_id();
    uint64_t lc = mt.allocate_page_id();
    uint64_t ld = mt.allocate_page_id();
    mt.store(la, LeafBase::build(make_entries(make_entry("a"))));
    mt.store(lb, LeafBase::build(make_entries(make_entry("e"))));
    mt.store(lc, LeafBase::build(make_entries(make_entry("m"))));
    mt.store(ld, LeafBase::build(make_entries(make_entry("t"))));
    uint64_t left  = mt.allocate_page_id(); // sep [e] -> [la, lb]
    uint64_t right = mt.allocate_page_id(); // sep [t] -> [lc, ld]
    mt.store(left, InnerBase::build({"e"}, {la, lb}));
    mt.store(right, InnerBase::build({"t"}, {lc, ld}));
    uint64_t root = mt.allocate_page_id(); // sep [m] -> [left, right]
    mt.store(root, InnerBase::build({"m"}, {left, right}));

    EXPECT_EQ(find_leaf_page_id([&](uint64_t p) { return mt.get_resident(p); }, root, "a"), la);
    EXPECT_EQ(find_leaf_page_id([&](uint64_t p) { return mt.get_resident(p); }, root, "e"), lb);
    EXPECT_EQ(find_leaf_page_id([&](uint64_t p) { return mt.get_resident(p); }, root, "f"), lb);
    EXPECT_EQ(find_leaf_page_id([&](uint64_t p) { return mt.get_resident(p); }, root, "m"), lc);
    EXPECT_EQ(find_leaf_page_id([&](uint64_t p) { return mt.get_resident(p); }, root, "t"), ld);
    EXPECT_EQ(find_leaf_page_id([&](uint64_t p) { return mt.get_resident(p); }, root, "zz"), ld);

    for (uint64_t page_id : {la, lb, lc, ld, left, right, root}) {
        delete mt.get_resident(page_id);
    }
}

TEST(Descent, EmptyRoot)
{
    MappingTable mt;
    EXPECT_EQ(find_leaf_page_id([&](uint64_t p) { return mt.get_resident(p); }, kInvalidPageId, "a"), kInvalidPageId);
    EXPECT_EQ(find_leaf_page_id([&](uint64_t p) { return mt.get_resident(p); }, 999, "a"),
              kInvalidPageId); // unset page_id
}
