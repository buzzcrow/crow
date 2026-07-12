// PT6c-5.4: writer-driven eviction of clean resident bases (design §4.6). An
// evicted leaf re-tags its mapping slot `unloaded` and epoch-retires the page;
// the next access demand-loads it. Run under TSan for the eviction-vs-reader
// race (epoch-deferred frame reuse).
#include "crowtree/crowtree.h"
#include "crowtree/page_store.h"

#include <gtest/gtest.h>

#include <atomic>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace crowtree;

namespace {
Batch Put1(const std::string& k, const std::string& v) {
  return Batch{{batch_op{k, OpKind::kPut, v}}};
}
std::string Key(int i) {
  char buf[16];
  snprintf(buf, sizeof(buf), "key%05d", i);
  return buf;
}
void Fill(Crowtree* t, int K, std::map<std::string, std::string>* oracle) {
  for (int i = 0; i < K; ++i) {
    std::string v = "val" + std::to_string(i);
    ASSERT_TRUE(t->apply(i + 1, Put1(Key(i), v)).ok());
    ASSERT_TRUE(t->flush().ok());
    (*oracle)[Key(i)] = v;
  }
}
}  // namespace

TEST(Eviction, EvictedLeavesFreeMemoryAndReloadCorrectly) {
  MemPageStore store(1);
  Options opt;
  opt.page_store = &store;
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 160;
  opt.frame_bytes = 4096;
  Crowtree t(opt);

  std::map<std::string, std::string> oracle;
  Fill(&t, 200, &oracle);
  ASSERT_TRUE(t.snapshot(nullptr).ok());  // all reachable pages now clean

  uint32_t before = t.buffer_pool()->stats().used;
  size_t evicted = t.evict_clean_leaves(2);  // keep at most 2 resident leaves
  EXPECT_GT(evicted, 0u);

  // No reader guards are open, so the epoch manager reclaims the retired pages
  // synchronously and their frames return to the pool: residency drops.
  uint32_t after = t.buffer_pool()->stats().used;
  EXPECT_LT(after, before);

  // Every value is still readable — evicted leaves demand-load on access.
  for (const auto& kv : oracle) {
    std::string v;
    uint64_t s;
    ASSERT_TRUE(t.get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
    EXPECT_EQ(v, kv.second);
  }
}

TEST(Eviction, EvictIsIdempotentAndSkipsDirty) {
  MemPageStore store(1);
  Options opt;
  opt.page_store = &store;
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 160;
  opt.frame_bytes = 4096;
  Crowtree t(opt);

  std::map<std::string, std::string> oracle;
  Fill(&t, 120, &oracle);
  // No snapshot yet: every built leaf is dirty (no durable addr) -> nothing
  // is evictable.
  EXPECT_EQ(t.evict_clean_leaves(0), 0u);

  ASSERT_TRUE(t.snapshot(nullptr).ok());  // pages become clean
  size_t first = t.evict_clean_leaves(1);
  EXPECT_GT(first, 0u);
  // A second pass with everything already unloaded evicts nothing more.
  EXPECT_EQ(t.evict_clean_leaves(1), 0u);

  for (const auto& kv : oracle) {
    std::string v;
    uint64_t s;
    ASSERT_TRUE(t.get(Slice(kv.first), &s, &v));
    EXPECT_EQ(v, kv.second);
  }
}

TEST(Eviction, ConcurrentReadersWhileEvicting) {
  MemPageStore store(1);
  Options opt;
  opt.page_store = &store;
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 160;
  opt.frame_bytes = 4096;
  Crowtree t(opt);

  const int K = 250;
  std::map<std::string, std::string> oracle;
  Fill(&t, K, &oracle);
  ASSERT_TRUE(t.snapshot(nullptr).ok());

  std::atomic<bool> stop{false};
  std::atomic<bool> fail{false};
  std::vector<std::thread> readers;
  for (int r = 0; r < 6; ++r) {
    readers.emplace_back([&, r] {
      std::mt19937 rng(9000 + r);
      std::string v;
      uint64_t s;
      while (!stop.load(std::memory_order_relaxed)) {
        int i = rng() % K;
        if (!t.get(Slice(Key(i)), &s, &v) || v != "val" + std::to_string(i)) {
          fail.store(true);
          return;
        }
      }
    });
  }

  // Churn: repeatedly evict almost everything while readers demand-load it back.
  for (int it = 0; it < 400; ++it) {
    t.evict_clean_leaves(2);
    std::this_thread::yield();
  }
  stop.store(true);
  for (auto& th : readers) {
    th.join();
  }
  EXPECT_FALSE(fail.load());
}
