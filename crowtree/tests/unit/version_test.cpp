// CT11: versioned root / snapshot view tests.
#include "crowtree/crowtree.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

using namespace crowtree;

namespace {
Batch Put1(const std::string& k, const std::string& v) {
  return Batch{{batch_op{k, OpKind::kPut, v}}};
}
}  // namespace

TEST(Version, FlushBumpsVersionAndTagsSlot) {
  Crowtree t;
  EXPECT_EQ(t.version(), 0u);
  ASSERT_TRUE(t.apply(1, Put1("a", "A1")).ok());
  ASSERT_TRUE(t.flush().ok());
  EXPECT_EQ(t.version(), 1u);
  ASSERT_TRUE(t.apply(2, Put1("b", "B2")).ok());
  ASSERT_TRUE(t.flush().ok());
  EXPECT_EQ(t.version(), 2u);
}

TEST(Version, SnapshotTagEqualsFlushedSlot) {
  Crowtree t;
  ASSERT_TRUE(t.apply(7, Put1("a", "A7")).ok());
  t.force_advance_slot(7);
  ASSERT_TRUE(t.flush().ok());
  auto snap = t.snapshot_view();
  EXPECT_EQ(snap->at_slot(), 7u);
}

TEST(Version, SnapshotIsStableWhileWriterChurns) {
  Crowtree t;
  ASSERT_TRUE(t.apply(1, Put1("a", "A1")).ok());
  ASSERT_TRUE(t.apply(1, Put1("b", "B1")).ok());
  ASSERT_TRUE(t.flush().ok());
  auto snap = t.snapshot_view();
  EXPECT_EQ(snap->at_slot(), 1u);
  ASSERT_EQ(snap->size(), 2u);

  // Mutate the tree heavily after pinning the snapshot.
  for (uint64_t s = 2; s <= 50; ++s) {
    ASSERT_TRUE(t.apply(s, Put1("a", "A" + std::to_string(s))).ok());
    ASSERT_TRUE(t.flush().ok());
  }
  // The pinned snapshot still reflects slot 1.
  uint64_t slot;
  std::string v;
  ASSERT_TRUE(snap->get(Slice("a"), &slot, &v));
  EXPECT_EQ(v, "A1");
  EXPECT_EQ(slot, 1u);
  EXPECT_EQ(snap->size(), 2u);

  // A fresh snapshot reflects the latest.
  auto snap2 = t.snapshot_view();
  ASSERT_TRUE(snap2->get(Slice("a"), &slot, &v));
  EXPECT_EQ(v, "A50");
}

TEST(Version, SnapshotIncludesTombstonesButGetSkips) {
  Crowtree t;
  ASSERT_TRUE(t.apply(1, Put1("a", "A1")).ok());
  ASSERT_TRUE(t.flush().ok());
  Batch del{{batch_op{"a", OpKind::kDelete, ""}}};
  ASSERT_TRUE(t.apply(2, del).ok());
  ASSERT_TRUE(t.flush().ok());
  auto snap = t.snapshot_view();
  // iter_all (entries) includes the tombstone...
  EXPECT_EQ(snap->size(), 1u);
  // ...but get skips it.
  uint64_t slot;
  std::string v;
  EXPECT_FALSE(snap->get(Slice("a"), &slot, &v));
}

TEST(Version, CompareDetectsDiffs) {
  Crowtree t;
  ASSERT_TRUE(t.apply(1, Put1("a", "A1")).ok());
  ASSERT_TRUE(t.apply(1, Put1("b", "B1")).ok());
  ASSERT_TRUE(t.flush().ok());
  auto s1 = t.snapshot_view();
  EXPECT_TRUE(s1->compare(*s1).empty());  // identical

  ASSERT_TRUE(t.apply(2, Put1("c", "C2")).ok());
  ASSERT_TRUE(t.flush().ok());
  auto s2 = t.snapshot_view();
  auto diffs = s1->compare(*s2);
  ASSERT_EQ(diffs.size(), 1u);
  EXPECT_EQ(diffs[0].key, "c");
  EXPECT_EQ(diffs[0].kind, engine_diff::kOnlyRight);
}

TEST(Version, RefcountLifecycle) {
  Crowtree t;
  ASSERT_TRUE(t.apply(1, Put1("a", "A1")).ok());
  ASSERT_TRUE(t.flush().ok());
  std::shared_ptr<Snapshot> a = t.snapshot_view();
  EXPECT_EQ(a.use_count(), 1);
  {
    std::shared_ptr<Snapshot> b = a;
    EXPECT_EQ(a.use_count(), 2);
  }
  EXPECT_EQ(a.use_count(), 1);
}
