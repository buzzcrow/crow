// CT11: versioned root / snapshot view tests.
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "crowtree/crowtree.h"
#include "crowtree/env.h"

using namespace crowtree;

namespace {
Batch Put1(const std::string& k, const std::string& v) {
  return Batch{{BatchOp{k, OpKind::kPut, v}}};
}
}  // namespace

TEST(Version, FlushBumpsVersionAndTagsSlot) {
  CrowtreeEnv env;
  Crowtree t(env);
  EXPECT_EQ(t.version(), 0u);
  ASSERT_TRUE(t.Apply(1, Put1("a", "A1"), 1).ok());
  ASSERT_TRUE(t.Flush().ok());
  EXPECT_EQ(t.version(), 1u);
  ASSERT_TRUE(t.Apply(2, Put1("b", "B2"), 2).ok());
  ASSERT_TRUE(t.Flush().ok());
  EXPECT_EQ(t.version(), 2u);
}

TEST(Version, SnapshotTagEqualsFlushedSlot) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.Apply(7, Put1("a", "A7"), 7).ok());
  ASSERT_TRUE(t.Flush().ok());
  auto snap = t.SnapshotView();
  EXPECT_EQ(snap->at_slot(), 7u);
}

TEST(Version, SnapshotIsStableWhileWriterChurns) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.Apply(1, Put1("a", "A1"), 1).ok());
  ASSERT_TRUE(t.Apply(1, Put1("b", "B1"), 1).ok());
  ASSERT_TRUE(t.Flush().ok());
  auto snap = t.SnapshotView();
  EXPECT_EQ(snap->at_slot(), 1u);
  ASSERT_EQ(snap->size(), 2u);

  // Mutate the tree heavily after pinning the snapshot.
  for (uint64_t s = 2; s <= 50; ++s) {
    ASSERT_TRUE(t.Apply(s, Put1("a", "A" + std::to_string(s)), s).ok());
    ASSERT_TRUE(t.Flush().ok());
  }
  // The pinned snapshot still reflects slot 1.
  uint64_t slot;
  std::string v;
  ASSERT_TRUE(snap->Get(Slice("a"), &slot, &v));
  EXPECT_EQ(v, "A1");
  EXPECT_EQ(slot, 1u);
  EXPECT_EQ(snap->size(), 2u);

  // A fresh snapshot reflects the latest.
  auto snap2 = t.SnapshotView();
  ASSERT_TRUE(snap2->Get(Slice("a"), &slot, &v));
  EXPECT_EQ(v, "A50");
}

TEST(Version, SnapshotIncludesTombstonesButGetSkips) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.Apply(1, Put1("a", "A1"), 1).ok());
  ASSERT_TRUE(t.Flush().ok());
  Batch del{{BatchOp{"a", OpKind::kDelete, ""}}};
  ASSERT_TRUE(t.Apply(2, del, 2).ok());
  ASSERT_TRUE(t.Flush().ok());
  auto snap = t.SnapshotView();
  // iter_all (entries) includes the tombstone...
  EXPECT_EQ(snap->size(), 1u);
  // ...but Get skips it.
  uint64_t slot;
  std::string v;
  EXPECT_FALSE(snap->Get(Slice("a"), &slot, &v));
}

TEST(Version, CompareDetectsDiffs) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.Apply(1, Put1("a", "A1"), 1).ok());
  ASSERT_TRUE(t.Apply(1, Put1("b", "B1"), 1).ok());
  ASSERT_TRUE(t.Flush().ok());
  auto s1 = t.SnapshotView();
  EXPECT_TRUE(s1->Compare(*s1).empty());  // identical

  ASSERT_TRUE(t.Apply(2, Put1("c", "C2"), 2).ok());
  ASSERT_TRUE(t.Flush().ok());
  auto s2 = t.SnapshotView();
  auto diffs = s1->Compare(*s2);
  ASSERT_EQ(diffs.size(), 1u);
  EXPECT_EQ(diffs[0].key, "c");
  EXPECT_EQ(diffs[0].kind, EngineDiff::kOnlyRight);
}

TEST(Version, RefcountLifecycle) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.Apply(1, Put1("a", "A1"), 1).ok());
  ASSERT_TRUE(t.Flush().ok());
  std::shared_ptr<Snapshot> a = t.SnapshotView();
  EXPECT_EQ(a.use_count(), 1);
  {
    std::shared_ptr<Snapshot> b = a;
    EXPECT_EQ(a.use_count(), 2);
  }
  EXPECT_EQ(a.use_count(), 1);
}
