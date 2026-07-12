// CT12: page split & merge integration tests.
#include <gtest/gtest.h>

#include <map>
#include <random>
#include <string>

#include "crowtree/crowtree.h"
#include "crowtree/env.h"

using namespace crowtree;

namespace {
Batch Put1(const std::string& k, const std::string& v) {
  return Batch{{BatchOp{k, OpKind::kPut, v}}};
}
Batch Del1(const std::string& k) {
  return Batch{{BatchOp{k, OpKind::kDelete, ""}}};
}
std::string Key(int i) {
  char buf[16];
  snprintf(buf, sizeof(buf), "key%05d", i);
  return buf;
}
}  // namespace

TEST(SplitMerge, SplitGrowsMultiLevelTree) {
  Options opt;
  opt.max_delta_len = 1;       // consolidate aggressively
  opt.leaf_split_bytes = 200;  // small leaves -> force splits
  CrowtreeEnv env;
  Crowtree t(env, opt);

  const int N = 300;
  for (int i = 0; i < N; ++i) {
    uint64_t s = i + 1;
    ASSERT_TRUE(t.Apply(s, Put1(Key(i), "value-payload-" + std::to_string(i)), s).ok());
    ASSERT_TRUE(t.Flush().ok());
  }
  // The tree must have grown beyond a single leaf.
  EXPECT_GT(t.Height(), 1);
  EXPECT_GT(t.LeafCount(), 1u);

  // All keys present and readable.
  for (int i = 0; i < N; ++i) {
    std::string v;
    uint64_t slot;
    ASSERT_TRUE(t.Get(Slice(Key(i)), &slot, &v)) << "missing " << Key(i);
    EXPECT_EQ(v, "value-payload-" + std::to_string(i));
  }

  // Snapshot is globally key-sorted and complete.
  auto snap = t.SnapshotView();
  ASSERT_EQ(snap->size(), static_cast<size_t>(N));
  for (size_t i = 1; i < snap->size(); ++i) {
    EXPECT_LT(snap->entries()[i - 1].key, snap->entries()[i].key);
  }
}

TEST(SplitMerge, MergeAndRootCollapse) {
  Options opt;
  opt.max_delta_len = 0;  // consolidate (and check merge) on every flush
  opt.leaf_split_bytes = 200;
  opt.leaf_merge_bytes = 60;
  CrowtreeEnv env;
  Crowtree t(env, opt);

  const int N = 200;
  uint64_t slot = 0;
  for (int i = 0; i < N; ++i) {
    ++slot;
    ASSERT_TRUE(t.Apply(slot, Put1(Key(i), "payload" + std::to_string(i)), slot).ok());
    ASSERT_TRUE(t.Flush().ok());
  }
  ASSERT_GT(t.Height(), 1);
  size_t leaves_before = t.LeafCount();
  EXPECT_GT(leaves_before, 1u);

  // Allow tombstone GC so deletes actually shrink leaves.
  t.SetGcWatermark(1000000);
  // Delete all but the first two keys.
  for (int i = 2; i < N; ++i) {
    ++slot;
    ASSERT_TRUE(t.Apply(slot, Del1(Key(i)), slot).ok());
    ASSERT_TRUE(t.Flush().ok());
  }

  // Tree shrank: fewer leaves, ideally collapsed back to a single-leaf root.
  EXPECT_LT(t.LeafCount(), leaves_before);
  EXPECT_EQ(t.Height(), 1);

  // Surviving keys readable; deleted keys gone.
  std::string v;
  uint64_t s;
  EXPECT_TRUE(t.Get(Slice(Key(0)), &s, &v));
  EXPECT_TRUE(t.Get(Slice(Key(1)), &s, &v));
  for (int i = 2; i < N; ++i) {
    EXPECT_FALSE(t.Get(Slice(Key(i)), &s, &v)) << "should be deleted: " << Key(i);
  }
  auto snap = t.SnapshotView();
  EXPECT_EQ(snap->size(), 2u);
}

TEST(SplitMerge, ParityWithOracleUnderSplits) {
  Options opt;
  opt.max_delta_len = 2;
  opt.leaf_split_bytes = 150;
  opt.leaf_merge_bytes = 40;
  CrowtreeEnv env;
  Crowtree t(env, opt);

  std::map<std::string, std::string> oracle;
  std::mt19937 rng(12345);
  uint64_t slot = 0;
  for (int round = 0; round < 2000; ++round) {
    int k = rng() % 150;
    std::string key = Key(k);
    ++slot;
    if (rng() % 4 == 0) {
      ASSERT_TRUE(t.Apply(slot, Del1(key), slot).ok());
      oracle.erase(key);
    } else {
      std::string val = "v" + std::to_string(slot);
      ASSERT_TRUE(t.Apply(slot, Put1(key, val), slot).ok());
      oracle[key] = val;
    }
    if (round % 7 == 0) {
      ASSERT_TRUE(t.Flush().ok());
    }
  }
  ASSERT_TRUE(t.Flush().ok());

  // Compare every key.
  for (int k = 0; k < 150; ++k) {
    std::string key = Key(k);
    std::string v;
    uint64_t s;
    bool found = t.Get(Slice(key), &s, &v);
    auto it = oracle.find(key);
    if (it == oracle.end()) {
      EXPECT_FALSE(found) << "extra key " << key;
    } else {
      ASSERT_TRUE(found) << "missing key " << key;
      EXPECT_EQ(v, it->second) << "value mismatch " << key;
    }
  }
}
