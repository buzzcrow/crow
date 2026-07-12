// Inner-node underflow merge: a delete-heavy workload must collapse the upper
// tree (merge underfull inner pages, dropping height) while preserving data,
// across reopen, and stay parity-correct vs an in-mem oracle.
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
  snprintf(b, sizeof(b), "key%06d", i);
  return b;
}
Options TallTreeOpts(PageStore* s) {
  Options o;
  o.page_store = s;
  o.frame_bytes = 4096;
  o.max_delta_len = 1;       // consolidate every flush
  o.leaf_split_bytes = 160;  // tiny leaves -> many of them
  o.leaf_merge_bytes = 60;
  o.inner_max_keys = 8;    // low fanout -> tall tree
  o.inner_merge_keys = 3;  // merge inner pages below 3 separators
  return o;
}
}  // namespace

TEST(InnerMerge, DeleteHeavyCollapsesTreeAndReopens) {
  MemPageStore store(1);
  Options opt = TallTreeOpts(&store);

  const int N = 600;
  std::map<std::string, std::string> oracle;
  int tall_height = 0;
  {
    Crowtree t(opt);
    uint64_t slot = 0;
    for (int i = 0; i < N; ++i) {
      ++slot;
      std::string v = "v" + std::to_string(i);
      ASSERT_TRUE(t.apply(slot, Put1(Key(i), v)).ok());
      ASSERT_TRUE(t.flush().ok());
      oracle[Key(i)] = v;
    }
    tall_height = t.height();
    ASSERT_GE(tall_height, 3);  // genuinely multi-level

    // Allow tombstone GC so deleted leaves shrink + merge away.
    t.set_gc_watermark(1000000);
    // Delete all but a sparse handful, driving leaf merges -> inner underflow.
    for (int i = 0; i < N; ++i) {
      if (i % 50 == 0) {
        continue;  // keep ~12 keys
      }
      ++slot;
      ASSERT_TRUE(t.apply(slot, Del1(Key(i))).ok());
      ASSERT_TRUE(t.flush().ok());
      oracle.erase(Key(i));
    }

    int collapsed_height = t.height();
    EXPECT_LT(collapsed_height, tall_height) << "tree did not collapse";

    // Surviving keys read correctly; deleted keys are gone.
    for (int i = 0; i < N; ++i) {
      std::string v;
      uint64_t s;
      bool found = t.get(Slice(Key(i)), &s, &v);
      if (i % 50 == 0) {
        ASSERT_TRUE(found) << "missing " << Key(i);
        EXPECT_EQ(v, "v" + std::to_string(i));
      } else {
        EXPECT_FALSE(found) << "resurrected " << Key(i);
      }
    }
    ASSERT_TRUE(t.snapshot(nullptr).ok());
    EXPECT_FALSE(t.io_failed());
  }

  // Reopen: the collapsed tree round-trips.
  std::unique_ptr<Crowtree> t2;
  ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
  for (const auto& kv : oracle) {
    std::string v;
    uint64_t s;
    ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing after reopen " << kv.first;
    EXPECT_EQ(v, kv.second);
  }
  // Spot-check a deleted key stays gone.
  std::string v;
  uint64_t s;
  EXPECT_FALSE(t2->get(Slice(Key(1)), &s, &v));
}

TEST(InnerMerge, RandomizedInsertDeleteParity) {
  Options opt;  // pure in-memory
  opt.frame_bytes = 4096;
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 200;
  opt.leaf_merge_bytes = 70;
  opt.inner_max_keys = 8;
  opt.inner_merge_keys = 3;
  Crowtree t(opt);

  std::map<std::string, std::string> oracle;
  std::mt19937 rng(424242);
  uint64_t slot = 0;
  // build up, then churn with a delete bias to force inner merges.
  for (int round = 0; round < 4000; ++round) {
    int k = rng() % 800;
    std::string key = Key(k);
    ++slot;
    bool del = (round > 1500) ? (rng() % 3 != 0) : (rng() % 5 == 0);
    if (del) {
      ASSERT_TRUE(t.apply(slot, Del1(key)).ok());
      oracle.erase(key);
    } else {
      std::string val = "val" + std::to_string(slot);
      ASSERT_TRUE(t.apply(slot, Put1(key, val)).ok());
      oracle[key] = val;
    }
    if (round % 3 == 0) {
      ASSERT_TRUE(t.flush().ok());
    }
    if (round % 500 == 499) {
      t.set_gc_watermark(slot);
    }
  }
  ASSERT_TRUE(t.flush().ok());

  for (int k = 0; k < 800; ++k) {
    std::string key = Key(k);
    std::string v;
    uint64_t s;
    bool found = t.get(Slice(key), &s, &v);
    auto it = oracle.find(key);
    if (it == oracle.end()) {
      EXPECT_FALSE(found) << "unexpected " << key;
    } else {
      ASSERT_TRUE(found) << "missing " << key;
      EXPECT_EQ(v, it->second) << "mismatch " << key;
    }
  }
}
