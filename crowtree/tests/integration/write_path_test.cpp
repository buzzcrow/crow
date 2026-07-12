// CT9: write path (apply + flush) integration tests.
#include "crowtree/crowtree.h"
#include "crowtree/env.h"

#include <gtest/gtest.h>

#include <string>

using namespace crowtree;

namespace {

Batch Put1(const std::string& k, const std::string& v) {
  return Batch{{batch_op{k, OpKind::kPut, v}}};
}

std::string Key(int i) {
  char b[16];
  snprintf(b, sizeof(b), "k%05d", i);
  return b;
}

std::string GetOr(Crowtree& t, const std::string& k, const std::string& dflt) {
  std::string v;
  uint64_t slot;
  return t.get(Slice(k), &slot, &v) ? v : dflt;
}

}  // namespace

TEST(WritePath, BasePagesLiveInBufferPool) {
  Options opt;
  opt.max_delta_len = 1;       // consolidate into base frames quickly
  opt.leaf_split_bytes = 200;  // small leaves -> multiple leaf + inner frames
  opt.frame_bytes = 4096;      // small frames so a few hold these tiny pages
  opt.buffer_pool_bytes = 64 * 4096;
  CrowtreeEnv env;
  Crowtree t(env, opt);
  for (int i = 0; i < 60; ++i) {
    uint64_t s = i + 1;
    ASSERT_TRUE(t.apply(s, Put1(Key(i), "payload-" + std::to_string(i))).ok());
    ASSERT_TRUE(t.flush().ok());
  }
  // The tree split into multiple leaves under one or more inner pages; every
  // such base page is built into a pool frame (held resident by its page).
  ASSERT_GT(t.leaf_count(), 1u);
  ASSERT_NE(t.buffer_pool(), nullptr);
  auto s = t.buffer_pool()->stats();
  EXPECT_GE(s.used, t.leaf_count());  // at least one frame per live leaf
  // Values remain correct when read straight out of the frames.
  for (int i = 0; i < 60; ++i) {
    EXPECT_EQ(GetOr(t, Key(i), "?"), "payload-" + std::to_string(i));
  }
}

TEST(WritePath, ApplyThenFlushVisible) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.apply(1, Put1("a", "A1")).ok());
  ASSERT_TRUE(t.apply(2, Put1("b", "B2")).ok());
  // Before flush, values are visible from L0.
  EXPECT_EQ(GetOr(t, "a", "?"), "A1");
  EXPECT_EQ(GetOr(t, "b", "?"), "B2");
  ASSERT_TRUE(t.flush().ok());
  EXPECT_EQ(t.last_applied_slot(), 2u);
  EXPECT_EQ(t.memtable_count(), 0u);  // fully drained
  // After flush, still visible from L1.
  EXPECT_EQ(GetOr(t, "a", "?"), "A1");
  EXPECT_EQ(GetOr(t, "b", "?"), "B2");
}

TEST(WritePath, IntraBatchLastWins) {
  CrowtreeEnv env;
  Crowtree t(env);
  Batch b{{batch_op{"k", OpKind::kPut, "first"}, batch_op{"k", OpKind::kPut, "second"},
           batch_op{"k", OpKind::kPut, "third"}}};
  ASSERT_TRUE(t.apply(1, b).ok());
  ASSERT_TRUE(t.flush().ok());
  EXPECT_EQ(GetOr(t, "k", "?"), "third");
}

TEST(WritePath, OutOfOrderApplyConverges) {
  CrowtreeEnv env;
  Crowtree t(env);
  // Slot 3 arrives before slot 2 (parallel window). contiguous lags until 2.
  ASSERT_TRUE(t.apply(3, Put1("a", "A3")).ok());
  ASSERT_TRUE(t.apply(2, Put1("a", "A2")).ok());
  t.force_advance_slot(3);
  ASSERT_TRUE(t.flush().ok());
  EXPECT_EQ(GetOr(t, "a", "?"), "A3");  // highest slot wins
  EXPECT_EQ(t.last_applied_slot(), 3u);
}

TEST(WritePath, FlushOnlyContiguousPrefix) {
  CrowtreeEnv env;
  Crowtree t(env);
  // a@2 contiguous; b@5 not yet contiguous (gap at 3,4).
  ASSERT_TRUE(t.apply(2, Put1("a", "A2")).ok());
  ASSERT_TRUE(t.apply(5, Put1("b", "B5")).ok());
  t.force_advance_slot(2);
  ASSERT_TRUE(t.flush().ok());
  EXPECT_EQ(t.last_applied_slot(), 2u);
  EXPECT_EQ(t.memtable_count(), 1u);  // b@5 retained in L0
  // Both still readable (a from L1, b from L0).
  EXPECT_EQ(GetOr(t, "a", "?"), "A2");
  EXPECT_EQ(GetOr(t, "b", "?"), "B5");
}

TEST(WritePath, NoOpAdvancesFrontier) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.apply(1, Put1("a", "A1")).ok());
  // NoOp/empty batch advances contiguous to 5 (slots 2-5 were NoOps).
  ASSERT_TRUE(t.apply(5, Batch{}).ok());
  t.force_advance_slot(5);
  ASSERT_TRUE(t.flush().ok());
  EXPECT_EQ(t.last_applied_slot(), 5u);
  EXPECT_EQ(GetOr(t, "a", "?"), "A1");
}

TEST(WritePath, ReApplyBelowDurableDropped) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.apply(3, Put1("a", "A3")).ok());
  t.force_advance_slot(3);
  ASSERT_TRUE(t.flush().ok());
  EXPECT_EQ(t.last_applied_slot(), 3u);
  // A stale re-apply of slot 2 must be dropped (already durable in L1).
  ASSERT_TRUE(t.apply(2, Put1("a", "A2")).ok());
  EXPECT_EQ(t.memtable_count(), 0u);
  EXPECT_EQ(GetOr(t, "a", "?"), "A3");
}

TEST(WritePath, DeleteTombstone) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.apply(1, Put1("a", "A1")).ok());
  ASSERT_TRUE(t.flush().ok());
  Batch del{{batch_op{"a", OpKind::kDelete, ""}}};
  ASSERT_TRUE(t.apply(2, del).ok());
  ASSERT_TRUE(t.flush().ok());
  std::string v;
  uint64_t slot;
  EXPECT_FALSE(t.get(Slice("a"), &slot, &v));  // tombstone -> not found
}

TEST(WritePath, ConsolidationOnLongChain) {
  Options opt;
  opt.max_delta_len = 4;  // force consolidation quickly
  CrowtreeEnv env;
  Crowtree t(env, opt);
  for (uint64_t s = 1; s <= 20; ++s) {
    ASSERT_TRUE(t.apply(s, Put1("k", "v" + std::to_string(s))).ok());
    ASSERT_TRUE(t.flush().ok());
  }
  EXPECT_EQ(GetOr(t, "k", "?"), "v20");
  EXPECT_EQ(t.last_applied_slot(), 20u);
}
