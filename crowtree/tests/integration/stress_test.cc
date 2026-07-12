// CT14: concurrent readers while a single writer applies/flushes/splits/merges.
// Run under TSan/ASan to catch races and use-after-free in epoch reclamation.
#include <gtest/gtest.h>

#include <atomic>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "crowtree/crowtree.h"
#include "crowtree/env.h"

using namespace crowtree;

namespace {
std::string Key(int i) {
  char buf[16];
  snprintf(buf, sizeof(buf), "key%04d", i);
  return buf;
}
}  // namespace

TEST(Stress, ConcurrentReadersSingleWriter) {
  Options opt;
  opt.max_delta_len = 2;
  opt.leaf_split_bytes = 160;
  opt.leaf_merge_bytes = 50;
  CrowtreeEnv env;
  Crowtree t(env, opt);

  const int K = 200;
  std::atomic<bool> stop{false};
  std::atomic<long> reads{0};

  // Reader threads: point reads + range scans concurrent with all SMOs.
  std::vector<std::thread> readers;
  for (int r = 0; r < 4; ++r) {
    readers.emplace_back([&, r] {
      std::mt19937 rng(1000 + r);
      std::string v;
      uint64_t s;
      std::vector<ScanEntry> out;
      bool trunc;
      long iter = 0;
      while (!stop.load(std::memory_order_relaxed)) {
        // Exercise the point-read path; scan (consistent, takes the write lock)
        // only occasionally. Yield so the single writer isn't starved (the v1
        // epoch manager serializes guards on one mutex).
        for (int g = 0; g < 8; ++g) {
          t.Get(Slice(Key(rng() % K)), &s, &v);
        }
        if ((iter++ % 32) == 0) {
          t.Scan(Slice("key0"), 16, &out, &trunc);
        }
        reads.fetch_add(8, std::memory_order_relaxed);
        std::this_thread::yield();
      }
    });
  }

  // Single writer owns the oracle.
  std::map<std::string, std::string> oracle;
  std::mt19937 rng(42);
  uint64_t slot = 0;
  for (int step = 0; step < 8000; ++step) {
    ++slot;
    std::string key = Key(rng() % K);
    if (rng() % 4 == 0) {
      ASSERT_TRUE(t.Apply(slot, Batch{{BatchOp{key, OpKind::kDelete, ""}}}, slot).ok());
      oracle.erase(key);
    } else {
      std::string val = "v" + std::to_string(slot);
      ASSERT_TRUE(t.Apply(slot, Batch{{BatchOp{key, OpKind::kPut, val}}}, slot).ok());
      oracle[key] = val;
    }
    if (rng() % 6 == 0) {
      ASSERT_TRUE(t.Flush().ok());
    }
    if (step % 5000 == 4999) {
      t.SetGcWatermark(slot);
    }
  }
  ASSERT_TRUE(t.Flush().ok());

  stop.store(true);
  for (auto& th : readers) th.join();
  EXPECT_GT(reads.load(), 0);

  // Final state matches the oracle.
  std::vector<ScanEntry> out;
  bool trunc = false;
  ASSERT_TRUE(t.Scan(Slice(""), 0, &out, &trunc).ok());
  ASSERT_EQ(out.size(), oracle.size());
  size_t i = 0;
  for (auto& kv : oracle) {
    EXPECT_EQ(out[i].key, kv.first);
    EXPECT_EQ(out[i].value, kv.second);
    ++i;
  }
}
