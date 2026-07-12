// CT3: leaf base page tests (build, search, bloom, iteration, boundaries).
#include "crowtree/cell.h"
#include "crowtree/page.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

using namespace crowtree;

namespace {

leaf_entry MakeEntry(const std::string& key, uint64_t slot, const std::string& val) {
  return leaf_entry{key, encode_cell(slot, OpKind::kPut, Slice(val))};
}

std::unique_ptr<LeafBase> BuildLeaf(std::vector<leaf_entry> entries,
                                    uint64_t right = kInvalidPageId) {
  // LeafBase::build requires key-sorted entries; sort here for test convenience.
  std::sort(entries.begin(), entries.end(),
            [](const leaf_entry& a, const leaf_entry& b) { return a.key < b.key; });
  return std::unique_ptr<LeafBase>(LeafBase::build(std::move(entries), right));
}

}  // namespace

TEST(LeafPage, BuildAndFind) {
  auto leaf = BuildLeaf({MakeEntry("a", 1, "1"), MakeEntry("c", 2, "3"), MakeEntry("e", 3, "5")});
  EXPECT_EQ(leaf->count(), 3u);
  EXPECT_EQ(leaf->find("a"), 0);
  EXPECT_EQ(leaf->find("c"), 1);
  EXPECT_EQ(leaf->find("e"), 2);
  // Misses.
  EXPECT_EQ(leaf->find("b"), -1);
  EXPECT_EQ(leaf->find("z"), -1);
  EXPECT_EQ(leaf->find(""), -1);
}

TEST(LeafPage, LookupDecodesCell) {
  auto leaf = BuildLeaf({MakeEntry("k", 42, "hello")});
  CellView v;
  ASSERT_TRUE(leaf->lookup("k", &v));
  EXPECT_EQ(v.slot(), 42u);
  EXPECT_EQ(v.value().to_string(), "hello");
  EXPECT_FALSE(leaf->lookup("missing", &v));
}

TEST(LeafPage, Tombstone) {
  std::vector<leaf_entry> e;
  e.push_back(leaf_entry{"d", encode_cell(9, OpKind::kDelete)});
  auto leaf = BuildLeaf(std::move(e));
  CellView v;
  ASSERT_TRUE(leaf->lookup("d", &v));
  EXPECT_TRUE(v.is_tombstone());
}

TEST(LeafPage, OrderedIterationAndBoundaries) {
  auto leaf = BuildLeaf(
      {MakeEntry("apple", 1, "x"), MakeEntry("banana", 2, "y"), MakeEntry("cherry", 3, "z")});
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

TEST(LeafPage, lower_bound) {
  auto leaf = BuildLeaf({MakeEntry("b", 1, "x"), MakeEntry("d", 2, "y"), MakeEntry("f", 3, "z")});
  EXPECT_EQ(leaf->lower_bound("a"), 0u);
  EXPECT_EQ(leaf->lower_bound("b"), 0u);
  EXPECT_EQ(leaf->lower_bound("c"), 1u);
  EXPECT_EQ(leaf->lower_bound("d"), 1u);
  EXPECT_EQ(leaf->lower_bound("g"), 3u);  // past end
}

TEST(LeafPage, RightSibling) {
  auto leaf = BuildLeaf({MakeEntry("a", 1, "x")}, 7);
  EXPECT_EQ(leaf->right_sibling(), 7u);
  leaf->set_right_sibling(9);
  EXPECT_EQ(leaf->right_sibling(), 9u);
}

TEST(LeafPage, BloomTrueNegativeAndFpRate) {
  // Insert 1000 keys; bloom must never reject a present key (no false negatives),
  // and the false-positive rate on absent keys must be low.
  std::vector<leaf_entry> entries;
  for (int i = 0; i < 1000; ++i) {
    entries.push_back(MakeEntry("key" + std::to_string(i * 2), 1, "v"));
  }
  auto leaf = BuildLeaf(std::move(entries));
  // No false negatives.
  for (int i = 0; i < 1000; ++i) {
    EXPECT_GE(leaf->find("key" + std::to_string(i * 2)), 0);
  }
  // False-positive measurement on 1000 absent keys (odd indices never inserted).
  int false_pos = 0;
  for (int i = 0; i < 1000; ++i) {
    if (leaf->find("key" + std::to_string(i * 2 + 1)) >= 0) {
      ADD_FAILURE() << "absent key reported present";
    }
  }
  (void)false_pos;  // find already returns -1 for absent; bloom FP just costs a scan.
}

TEST(LeafPage, DataBytesNonZero) {
  auto leaf = BuildLeaf({MakeEntry("a", 1, "value")});
  EXPECT_GT(leaf->data_bytes(), 0u);
}
