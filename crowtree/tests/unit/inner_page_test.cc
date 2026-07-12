// CT4: inner base page + tree descent tests.
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "crowtree/descent.h"
#include "crowtree/mapping_table.h"
#include "crowtree/page.h"

using namespace crowtree;

TEST(InnerPage, ChildIndexFor) {
  // separators = [d, k, q]; children = [c0, c1, c2, c3]
  auto* inner = InnerBase::Build({"d", "k", "q"}, {10, 11, 12, 13});
  std::unique_ptr<InnerBase> guard(inner);
  EXPECT_EQ(inner->ChildIndexFor("a"), 0u);  // < d
  EXPECT_EQ(inner->ChildIndexFor("d"), 1u);  // == d -> right
  EXPECT_EQ(inner->ChildIndexFor("e"), 1u);  // d <= e < k
  EXPECT_EQ(inner->ChildIndexFor("k"), 2u);  // == k
  EXPECT_EQ(inner->ChildIndexFor("p"), 2u);
  EXPECT_EQ(inner->ChildIndexFor("q"), 3u);  // == q
  EXPECT_EQ(inner->ChildIndexFor("z"), 3u);  // > q
  EXPECT_EQ(inner->ChildFor("e"), 11u);
  EXPECT_EQ(inner->ChildFor("z"), 13u);
}

TEST(Descent, SingleLeafRoot) {
  MappingTable mt;
  uint64_t leaf_pid = mt.AllocatePID();
  mt.Store(leaf_pid, LeafBase::Build({LeafEntry{"a", "x"}}));
  EXPECT_EQ(FindLeafPID(mt, leaf_pid, "a"), leaf_pid);
  EXPECT_EQ(FindLeafPID(mt, leaf_pid, "zzz"), leaf_pid);
  delete mt.Get(leaf_pid);
}

TEST(Descent, TwoLevelTree) {
  MappingTable mt;
  // Three leaves.
  uint64_t l0 = mt.AllocatePID();
  uint64_t l1 = mt.AllocatePID();
  uint64_t l2 = mt.AllocatePID();
  mt.Store(l0, LeafBase::Build({LeafEntry{"a", "x"}}));
  mt.Store(l1, LeafBase::Build({LeafEntry{"k", "x"}}));
  mt.Store(l2, LeafBase::Build({LeafEntry{"q", "x"}}));
  // Root inner: separators [k, q] -> children [l0, l1, l2].
  uint64_t root = mt.AllocatePID();
  mt.Store(root, InnerBase::Build({"k", "q"}, {l0, l1, l2}));

  EXPECT_EQ(FindLeafPID(mt, root, "a"), l0);
  EXPECT_EQ(FindLeafPID(mt, root, "j"), l0);
  EXPECT_EQ(FindLeafPID(mt, root, "k"), l1);
  EXPECT_EQ(FindLeafPID(mt, root, "p"), l1);
  EXPECT_EQ(FindLeafPID(mt, root, "q"), l2);
  EXPECT_EQ(FindLeafPID(mt, root, "zz"), l2);

  for (uint64_t pid : {l0, l1, l2, root}) delete mt.Get(pid);
}

TEST(Descent, ThreeLevelTree) {
  MappingTable mt;
  // Leaves under two inner nodes, joined by a root.
  uint64_t la = mt.AllocatePID(), lb = mt.AllocatePID();
  uint64_t lc = mt.AllocatePID(), ld = mt.AllocatePID();
  mt.Store(la, LeafBase::Build({LeafEntry{"a", "x"}}));
  mt.Store(lb, LeafBase::Build({LeafEntry{"e", "x"}}));
  mt.Store(lc, LeafBase::Build({LeafEntry{"m", "x"}}));
  mt.Store(ld, LeafBase::Build({LeafEntry{"t", "x"}}));
  uint64_t left = mt.AllocatePID();   // sep [e] -> [la, lb]
  uint64_t right = mt.AllocatePID();  // sep [t] -> [lc, ld]
  mt.Store(left, InnerBase::Build({"e"}, {la, lb}));
  mt.Store(right, InnerBase::Build({"t"}, {lc, ld}));
  uint64_t root = mt.AllocatePID();   // sep [m] -> [left, right]
  mt.Store(root, InnerBase::Build({"m"}, {left, right}));

  EXPECT_EQ(FindLeafPID(mt, root, "a"), la);
  EXPECT_EQ(FindLeafPID(mt, root, "e"), lb);
  EXPECT_EQ(FindLeafPID(mt, root, "f"), lb);
  EXPECT_EQ(FindLeafPID(mt, root, "m"), lc);
  EXPECT_EQ(FindLeafPID(mt, root, "t"), ld);
  EXPECT_EQ(FindLeafPID(mt, root, "zz"), ld);

  for (uint64_t pid : {la, lb, lc, ld, left, right, root}) delete mt.Get(pid);
}

TEST(Descent, EmptyRoot) {
  MappingTable mt;
  EXPECT_EQ(FindLeafPID(mt, kInvalidPID, "a"), kInvalidPID);
  EXPECT_EQ(FindLeafPID(mt, 999, "a"), kInvalidPID);  // unset pid
}
