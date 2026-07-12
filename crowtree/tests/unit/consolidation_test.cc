// CT10: consolidation tests (fold by highest slot, triggers, tombstone keep,
// old-chain retirement via the epoch manager).
#include <gtest/gtest.h>

#include <string>

#include "crowtree/crowtree.h"
#include "crowtree/env.h"
#include "crowtree/page.h"

using namespace crowtree;

namespace {
Batch Put1(const std::string& k, const std::string& v) {
  return Batch{{BatchOp{k, OpKind::kPut, v}}};
}
PageType HeadType(Crowtree& t) {
  return t.mapping().Get(t.root_pid())->type;
}
}  // namespace

TEST(Consolidation, FoldsChainAtDeltaLenThreshold) {
  Options opt;
  opt.max_delta_len = 4;
  CrowtreeEnv env;
  Crowtree t(env, opt);
  // Each flush adds one delta to the single root leaf.
  for (uint64_t s = 1; s <= 4; ++s) {
    ASSERT_TRUE(t.Apply(s, Put1("k" + std::to_string(s), "v"), s).ok());
    ASSERT_TRUE(t.Flush().ok());
  }
  // 4 deltas: not yet over threshold (> 4).
  EXPECT_EQ(HeadType(t), PageType::kBatchDelta);
  // 5th delta trips consolidation -> head becomes a fresh LeafBase.
  ASSERT_TRUE(t.Apply(5, Put1("k5", "v"), 5).ok());
  ASSERT_TRUE(t.Flush().ok());
  EXPECT_EQ(HeadType(t), PageType::kLeafBase);
  // All keys survive the fold.
  for (int i = 1; i <= 5; ++i) {
    std::string v;
    uint64_t slot;
    EXPECT_TRUE(t.Get(Slice("k" + std::to_string(i)), &slot, &v));
  }
}

TEST(Consolidation, FoldKeepsHighestSlotPerKey) {
  Options opt;
  opt.max_delta_len = 3;
  CrowtreeEnv env;
  Crowtree t(env, opt);
  for (uint64_t s = 1; s <= 10; ++s) {
    ASSERT_TRUE(t.Apply(s, Put1("k", "v" + std::to_string(s)), s).ok());
    ASSERT_TRUE(t.Flush().ok());
  }
  std::string v;
  uint64_t slot;
  ASSERT_TRUE(t.Get(Slice("k"), &slot, &v));
  EXPECT_EQ(v, "v10");
  EXPECT_EQ(slot, 10u);
}

TEST(Consolidation, TombstonePreservedThroughFold) {
  Options opt;
  opt.max_delta_len = 2;
  CrowtreeEnv env;
  Crowtree t(env, opt);
  ASSERT_TRUE(t.Apply(1, Put1("a", "A"), 1).ok());
  ASSERT_TRUE(t.Flush().ok());
  Batch del{{BatchOp{"a", OpKind::kDelete, ""}}};
  ASSERT_TRUE(t.Apply(2, del, 2).ok());
  ASSERT_TRUE(t.Flush().ok());
  // Drive enough flushes to force consolidation.
  for (uint64_t s = 3; s <= 6; ++s) {
    ASSERT_TRUE(t.Apply(s, Put1("b" + std::to_string(s), "x"), s).ok());
    ASSERT_TRUE(t.Flush().ok());
  }
  // Tombstone for "a" still honored (delete wins; not resurrected).
  std::string v;
  uint64_t slot;
  EXPECT_FALSE(t.Get(Slice("a"), &slot, &v));
}

TEST(Consolidation, OldChainRetiredViaEpoch) {
  Options opt;
  opt.max_delta_len = 3;
  CrowtreeEnv env;
  Crowtree t(env, opt);
  for (uint64_t s = 1; s <= 3; ++s) {
    ASSERT_TRUE(t.Apply(s, Put1("k", "v" + std::to_string(s)), s).ok());
    ASSERT_TRUE(t.Flush().ok());
  }
  // Hold a read guard so retired pages cannot be freed during consolidation.
  {
    EpochManager::Guard g = env.epoch().Enter();
    ASSERT_TRUE(t.Apply(4, Put1("k", "v4"), 4).ok());
    ASSERT_TRUE(t.Flush().ok());  // consolidation retires the old chain
    EXPECT_GT(env.epoch().PendingRetired(), 0u);
  }
  // Guard dropped -> retired pages become reclaimable.
  env.epoch().TryReclaim();
  EXPECT_EQ(env.epoch().PendingRetired(), 0u);
}
