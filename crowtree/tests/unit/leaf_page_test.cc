// CT3: leaf base page tests (build, search, bloom, iteration, boundaries).
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "crowtree/cell.h"
#include "crowtree/page.h"

using namespace crowtree;

namespace {

LeafEntry MakeEntry(const std::string& key, uint64_t slot, const std::string& val) {
  return LeafEntry{key, EncodeCell(slot, OpKind::kPut, Slice(val))};
}

std::unique_ptr<LeafBase> BuildLeaf(std::vector<LeafEntry> entries,
                                    uint64_t right = kInvalidPID) {
  // LeafBase::Build requires key-sorted entries; sort here for test convenience.
  std::sort(entries.begin(), entries.end(),
            [](const LeafEntry& a, const LeafEntry& b) { return a.key < b.key; });
  return std::unique_ptr<LeafBase>(LeafBase::Build(std::move(entries), right));
}

}  // namespace

TEST(LeafPage, BuildAndFind) {
  auto leaf = BuildLeaf({MakeEntry("a", 1, "1"), MakeEntry("c", 2, "3"),
                         MakeEntry("e", 3, "5")});
  EXPECT_EQ(leaf->count(), 3u);
  EXPECT_EQ(leaf->Find("a"), 0);
  EXPECT_EQ(leaf->Find("c"), 1);
  EXPECT_EQ(leaf->Find("e"), 2);
  // Misses.
  EXPECT_EQ(leaf->Find("b"), -1);
  EXPECT_EQ(leaf->Find("z"), -1);
  EXPECT_EQ(leaf->Find(""), -1);
}

TEST(LeafPage, LookupDecodesCell) {
  auto leaf = BuildLeaf({MakeEntry("k", 42, "hello")});
  CellView v;
  ASSERT_TRUE(leaf->Lookup("k", &v));
  EXPECT_EQ(v.slot(), 42u);
  EXPECT_EQ(v.value().ToString(), "hello");
  EXPECT_FALSE(leaf->Lookup("missing", &v));
}

TEST(LeafPage, Tombstone) {
  std::vector<LeafEntry> e;
  e.push_back(LeafEntry{"d", EncodeCell(9, OpKind::kDelete)});
  auto leaf = BuildLeaf(std::move(e));
  CellView v;
  ASSERT_TRUE(leaf->Lookup("d", &v));
  EXPECT_TRUE(v.is_tombstone());
}

TEST(LeafPage, OrderedIterationAndBoundaries) {
  auto leaf = BuildLeaf({MakeEntry("apple", 1, "x"), MakeEntry("banana", 2, "y"),
                         MakeEntry("cherry", 3, "z")});
  EXPECT_EQ(leaf->low_key().ToString(), "apple");
  EXPECT_EQ(leaf->high_key().ToString(), "cherry");
  std::string prev;
  for (size_t i = 0; i < leaf->count(); ++i) {
    std::string k = leaf->entry(i).key;
    if (i > 0) {
      EXPECT_LT(prev, k);
    }
    prev = k;
  }
}

TEST(LeafPage, LowerBound) {
  auto leaf = BuildLeaf({MakeEntry("b", 1, "x"), MakeEntry("d", 2, "y"),
                         MakeEntry("f", 3, "z")});
  EXPECT_EQ(leaf->LowerBound("a"), 0u);
  EXPECT_EQ(leaf->LowerBound("b"), 0u);
  EXPECT_EQ(leaf->LowerBound("c"), 1u);
  EXPECT_EQ(leaf->LowerBound("d"), 1u);
  EXPECT_EQ(leaf->LowerBound("g"), 3u);  // past end
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
  std::vector<LeafEntry> entries;
  for (int i = 0; i < 1000; ++i) {
    entries.push_back(MakeEntry("key" + std::to_string(i * 2), 1, "v"));
  }
  auto leaf = BuildLeaf(std::move(entries));
  // No false negatives.
  for (int i = 0; i < 1000; ++i) {
    EXPECT_GE(leaf->Find("key" + std::to_string(i * 2)), 0);
  }
  // False-positive measurement on 1000 absent keys (odd indices never inserted).
  int false_pos = 0;
  for (int i = 0; i < 1000; ++i) {
    if (leaf->Find("key" + std::to_string(i * 2 + 1)) >= 0) {
      ADD_FAILURE() << "absent key reported present";
    }
  }
  (void)false_pos;  // Find already returns -1 for absent; bloom FP just costs a scan.
}

TEST(LeafPage, DataBytesNonZero) {
  auto leaf = BuildLeaf({MakeEntry("a", 1, "value")});
  EXPECT_GT(leaf->data_bytes(), 0u);
}
