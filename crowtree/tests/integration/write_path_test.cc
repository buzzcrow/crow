// CT9: write path (apply + flush) integration tests.
#include <gtest/gtest.h>

#include <string>

#include "crowtree/crowtree.h"
#include "crowtree/env.h"

using namespace crowtree;

namespace {

Batch Put1(const std::string& k, const std::string& v) {
  return Batch{{BatchOp{k, OpKind::kPut, v}}};
}

std::string GetOr(Crowtree& t, const std::string& k, const std::string& dflt) {
  std::string v;
  uint64_t slot;
  return t.Get(Slice(k), &slot, &v) ? v : dflt;
}

}  // namespace

TEST(WritePath, ApplyThenFlushVisible) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.Apply(1, Put1("a", "A1"), 1).ok());
  ASSERT_TRUE(t.Apply(2, Put1("b", "B2"), 2).ok());
  // Before flush, values are visible from L0.
  EXPECT_EQ(GetOr(t, "a", "?"), "A1");
  EXPECT_EQ(GetOr(t, "b", "?"), "B2");
  ASSERT_TRUE(t.Flush().ok());
  EXPECT_EQ(t.last_applied_slot(), 2u);
  EXPECT_EQ(t.MemTableCount(), 0u);  // fully drained
  // After flush, still visible from L1.
  EXPECT_EQ(GetOr(t, "a", "?"), "A1");
  EXPECT_EQ(GetOr(t, "b", "?"), "B2");
}

TEST(WritePath, IntraBatchLastWins) {
  CrowtreeEnv env;
  Crowtree t(env);
  Batch b{{BatchOp{"k", OpKind::kPut, "first"},
           BatchOp{"k", OpKind::kPut, "second"},
           BatchOp{"k", OpKind::kPut, "third"}}};
  ASSERT_TRUE(t.Apply(1, b, 1).ok());
  ASSERT_TRUE(t.Flush().ok());
  EXPECT_EQ(GetOr(t, "k", "?"), "third");
}

TEST(WritePath, OutOfOrderApplyConverges) {
  CrowtreeEnv env;
  Crowtree t(env);
  // Slot 3 arrives before slot 2 (parallel window). contiguous lags until 2.
  ASSERT_TRUE(t.Apply(3, Put1("a", "A3"), 0).ok());
  ASSERT_TRUE(t.Apply(2, Put1("a", "A2"), 3).ok());
  ASSERT_TRUE(t.Flush().ok());
  EXPECT_EQ(GetOr(t, "a", "?"), "A3");  // highest slot wins
  EXPECT_EQ(t.last_applied_slot(), 3u);
}

TEST(WritePath, FlushOnlyContiguousPrefix) {
  CrowtreeEnv env;
  Crowtree t(env);
  // a@2 contiguous; b@5 not yet contiguous (gap at 3,4).
  ASSERT_TRUE(t.Apply(2, Put1("a", "A2"), 2).ok());
  ASSERT_TRUE(t.Apply(5, Put1("b", "B5"), 2).ok());
  ASSERT_TRUE(t.Flush().ok());
  EXPECT_EQ(t.last_applied_slot(), 2u);
  EXPECT_EQ(t.MemTableCount(), 1u);  // b@5 retained in L0
  // Both still readable (a from L1, b from L0).
  EXPECT_EQ(GetOr(t, "a", "?"), "A2");
  EXPECT_EQ(GetOr(t, "b", "?"), "B5");
}

TEST(WritePath, NoOpAdvancesFrontier) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.Apply(1, Put1("a", "A1"), 1).ok());
  // NoOp/empty batch advances contiguous to 5 (slots 2-5 were NoOps).
  ASSERT_TRUE(t.Apply(5, Batch{}, 5).ok());
  ASSERT_TRUE(t.Flush().ok());
  EXPECT_EQ(t.last_applied_slot(), 5u);
  EXPECT_EQ(GetOr(t, "a", "?"), "A1");
}

TEST(WritePath, ReApplyBelowDurableDropped) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.Apply(3, Put1("a", "A3"), 3).ok());
  ASSERT_TRUE(t.Flush().ok());
  EXPECT_EQ(t.last_applied_slot(), 3u);
  // A stale re-apply of slot 2 must be dropped (already durable in L1).
  ASSERT_TRUE(t.Apply(2, Put1("a", "A2"), 3).ok());
  EXPECT_EQ(t.MemTableCount(), 0u);
  EXPECT_EQ(GetOr(t, "a", "?"), "A3");
}

TEST(WritePath, DeleteTombstone) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.Apply(1, Put1("a", "A1"), 1).ok());
  ASSERT_TRUE(t.Flush().ok());
  Batch del{{BatchOp{"a", OpKind::kDelete, ""}}};
  ASSERT_TRUE(t.Apply(2, del, 2).ok());
  ASSERT_TRUE(t.Flush().ok());
  std::string v;
  uint64_t slot;
  EXPECT_FALSE(t.Get(Slice("a"), &slot, &v));  // tombstone -> not found
}

TEST(WritePath, ConsolidationOnLongChain) {
  Options opt;
  opt.max_delta_len = 4;  // force consolidation quickly
  CrowtreeEnv env;
  Crowtree t(env, opt);
  for (uint64_t s = 1; s <= 20; ++s) {
    ASSERT_TRUE(t.Apply(s, Put1("k", "v" + std::to_string(s)), s).ok());
    ASSERT_TRUE(t.Flush().ok());
  }
  EXPECT_EQ(GetOr(t, "k", "?"), "v20");
  EXPECT_EQ(t.last_applied_slot(), 20u);
}
