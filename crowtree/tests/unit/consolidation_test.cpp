// CT10: consolidation tests (fold by highest slot, triggers, tombstone keep,
// old-chain retirement via the epoch manager).
#include "crowtree/crowtree.h"
#include "crowtree/page.h"

#include <gtest/gtest.h>

#include <string>

using namespace crowtree;

namespace {
Batch Put1(const std::string& k, const std::string& v) {
  return Batch{{batch_op{k, OpKind::kPut, v}}};
}
page_type HeadType(Crowtree& t) { return t.mapping().get(t.root_page_id())->type; }
}  // namespace

TEST(Consolidation, FoldsChainAtDeltaLenThreshold) {
  Options opt;
  opt.max_delta_len = 4;
  Crowtree t(opt);
  // Each flush adds one delta to the single root leaf.
  for (uint64_t s = 1; s <= 4; ++s) {
    ASSERT_TRUE(t.apply(s, Put1("k" + std::to_string(s), "v")).ok());
    ASSERT_TRUE(t.flush().ok());
  }
  // 4 deltas: not yet over threshold (> 4).
  EXPECT_EQ(HeadType(t), page_type::kBatchDelta);
  // 5th delta trips consolidation -> head becomes a fresh LeafBase.
  ASSERT_TRUE(t.apply(5, Put1("k5", "v")).ok());
  ASSERT_TRUE(t.flush().ok());
  EXPECT_EQ(HeadType(t), page_type::kLeafBase);
  // All keys survive the fold.
  for (int i = 1; i <= 5; ++i) {
    std::string v;
    uint64_t slot;
    EXPECT_TRUE(t.get(Slice("k" + std::to_string(i)), &slot, &v));
  }
}

TEST(Consolidation, FoldKeepsHighestSlotPerKey) {
  Options opt;
  opt.max_delta_len = 3;
  Crowtree t(opt);
  for (uint64_t s = 1; s <= 10; ++s) {
    ASSERT_TRUE(t.apply(s, Put1("k", "v" + std::to_string(s))).ok());
    ASSERT_TRUE(t.flush().ok());
  }
  std::string v;
  uint64_t slot;
  ASSERT_TRUE(t.get(Slice("k"), &slot, &v));
  EXPECT_EQ(v, "v10");
  EXPECT_EQ(slot, 10u);
}

TEST(Consolidation, TombstonePreservedThroughFold) {
  Options opt;
  opt.max_delta_len = 2;
  Crowtree t(opt);
  ASSERT_TRUE(t.apply(1, Put1("a", "A")).ok());
  ASSERT_TRUE(t.flush().ok());
  Batch del{{batch_op{"a", OpKind::kDelete, ""}}};
  ASSERT_TRUE(t.apply(2, del).ok());
  ASSERT_TRUE(t.flush().ok());
  // Drive enough flushes to force consolidation.
  for (uint64_t s = 3; s <= 6; ++s) {
    ASSERT_TRUE(t.apply(s, Put1("b" + std::to_string(s), "x")).ok());
    ASSERT_TRUE(t.flush().ok());
  }
  // Tombstone for "a" still honored (delete wins; not resurrected).
  std::string v;
  uint64_t slot;
  EXPECT_FALSE(t.get(Slice("a"), &slot, &v));
}

TEST(Consolidation, OldChainRetiredViaEpoch) {
  Options opt;
  opt.max_delta_len = 3;
  Crowtree t(opt);
  for (uint64_t s = 1; s <= 3; ++s) {
    ASSERT_TRUE(t.apply(s, Put1("k", "v" + std::to_string(s))).ok());
    ASSERT_TRUE(t.flush().ok());
  }
  // Hold a read guard so retired pages cannot be freed during consolidation.
  {
    EpochManager::Guard g = t.epoch().enter();
    ASSERT_TRUE(t.apply(4, Put1("k", "v4")).ok());
    ASSERT_TRUE(t.flush().ok());  // consolidation retires the old chain
    EXPECT_GT(t.epoch().pending_retired(), 0u);
  }
  // Guard dropped -> retired pages become reclaimable.
  t.epoch().try_reclaim();
  EXPECT_EQ(t.epoch().pending_retired(), 0u);
}
