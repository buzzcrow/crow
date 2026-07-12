// Combined stress: compression + overflow + in-frame deltas + a small buffer
// pool (forces eviction) + periodic snapshots, validated against an in-mem
// oracle live and after reopen. Plus a focused test that an overflow chain whose
// pages were evicted is still fully retired on overwrite (no leak; ASan covers).
#include "crowtree/crowtree.h"
#include "crowtree/page_store.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <random>
#include <string>
#include <vector>

using namespace crowtree;

namespace {
Batch Put1(const std::string& k, const std::string& v) {
  return Batch{{batch_op{k, OpKind::kPut, v}}};
}
Batch Del1(const std::string& k) { return Batch{{batch_op{k, OpKind::kDelete, ""}}}; }
std::string Key(int i) {
  char b[16];
  snprintf(b, sizeof(b), "k%04d", i);
  return b;
}
std::string Val(size_t n, uint32_t seed) {
  std::mt19937 rng(seed);
  std::string s(n, 0);
  for (auto& c : s) {
    c = static_cast<char>('a' + (rng() % 26));
  }
  return s;
}
}  // namespace

TEST(KitchenSink, AllFeaturesRandomizedReopen) {
  MemPageStore store(1);
  Options opt;
  opt.page_store = &store;
  opt.compression = compress_algo::kLz4;
  opt.frame_bytes = 4096;
  opt.buffer_pool_bytes = 64 * 1024;  // ~16 frames -> eviction under pressure
  opt.max_inline_value = 80;          // mix inline + overflow
  opt.inframe_delta = true;
  opt.max_inframe_delta = 6;
  opt.max_delta_len = 3;
  opt.leaf_split_bytes = 1024;

  std::map<std::string, std::string> oracle;
  std::mt19937 rng(20260701);
  uint64_t slot = 0;
  {
    Crowtree t(opt);
    for (int round = 0; round < 1500; ++round) {
      int k = rng() % 80;
      std::string key = Key(k);
      ++slot;
      if (rng() % 9 == 0) {
        ASSERT_TRUE(t.apply(slot, Del1(key)).ok());
        oracle.erase(key);
      } else {
        // ~1/3 large (overflow), else small (inline / in-frame delta).
        size_t n = (rng() % 3 == 0) ? (200 + rng() % 9000) : (1 + rng() % 60);
        std::string v = Val(n, static_cast<uint32_t>(slot));
        ASSERT_TRUE(t.apply(slot, Put1(key, v)).ok());
        oracle[key] = v;
      }
      if (round % 4 == 0) {
        ASSERT_TRUE(t.flush().ok());
      }
      if (round % 250 == 0) {
        ASSERT_TRUE(t.snapshot(nullptr).ok());
      }
    }
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot(nullptr).ok());
    EXPECT_FALSE(t.io_failed());

    // Live parity.
    for (int k = 0; k < 80; ++k) {
      std::string v;
      uint64_t s;
      bool found = t.get(Slice(Key(k)), &s, &v);
      auto it = oracle.find(Key(k));
      if (it == oracle.end()) {
        EXPECT_FALSE(found) << "unexpected " << Key(k);
      } else {
        ASSERT_TRUE(found) << "missing " << Key(k);
        EXPECT_EQ(v, it->second) << "mismatch " << Key(k);
      }
    }
  }

  // Reopen parity (lazy recovery + demand-load + decompress + overflow assemble).
  std::unique_ptr<Crowtree> t2;
  ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
  for (const auto& kv : oracle) {
    std::string v;
    uint64_t s;
    ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing after reopen " << kv.first;
    EXPECT_EQ(v, kv.second) << "reopen mismatch " << kv.first;
  }
  EXPECT_FALSE(t2->io_failed());
}

TEST(KitchenSink, OverwriteEvictedOverflowChainNoLeak) {
  MemPageStore store(1);
  Options opt;
  opt.page_store = &store;
  opt.frame_bytes = 4096;
  opt.max_inline_value = 64;
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 1024;
  Crowtree t(opt);

  const std::string k = Key(1);
  uint64_t slot = 0;
  ++slot;
  ASSERT_TRUE(t.apply(slot, Put1(k, Val(12000, 1))).ok());  // ~3 overflow frames
  ASSERT_TRUE(t.flush().ok());
  ASSERT_TRUE(t.snapshot(nullptr).ok());

  // Evict everything: the old overflow chain's pages become unloaded.
  t.evict_clean_leaves(0);

  // Overwrite the key: consolidation supersedes the old (evicted) overflow chain,
  // which retire_overflow_chain_locked must demand-load to fully retire.
  ++slot;
  ASSERT_TRUE(t.apply(slot, Put1(k, Val(9000, 2))).ok());
  ASSERT_TRUE(t.flush().ok());
  ASSERT_TRUE(t.snapshot(nullptr).ok());

  std::string v;
  uint64_t s;
  ASSERT_TRUE(t.get(Slice(k), &s, &v));
  EXPECT_EQ(v, Val(9000, 2));
  EXPECT_FALSE(t.io_failed());
}
