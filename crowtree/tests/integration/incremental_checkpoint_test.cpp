// PT6d: incremental snapshot writes only dirty pages, retains clean pages'
// durable addrs, and reopens to identical state (design §5, §4.6).
#include "crowtree/crowtree.h"
#include "crowtree/page_store.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>
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
// build a multi-level tree by inserting K keys, flushing incrementally so leaves
// stay small (one bulk flush would make a single oversized leaf).
void Fill(Crowtree* t, int K, std::map<std::string, std::string>* oracle) {
  for (int i = 0; i < K; ++i) {
    std::string v = "val" + std::to_string(i);
    ASSERT_TRUE(t->apply(i + 1, Put1(Key(i), v)).ok());
    ASSERT_TRUE(t->flush().ok());
    (*oracle)[Key(i)] = v;
  }
}
}  // namespace

TEST(IncrementalCheckpoint, SecondCheckpointWithoutChangesWritesNothing) {
  MemPageStore store(1);
  Options opt;
  opt.page_store = &store;
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 160;
  opt.frame_bytes = 4096;
  Crowtree t(opt);

  std::map<std::string, std::string> oracle;
  Fill(&t, 200, &oracle);

  ASSERT_TRUE(t.snapshot(nullptr).ok());
  uint64_t first = t.last_snapshot_pages_written();
  EXPECT_GT(first, 1u);  // a multi-page tree: everything was dirty

  // No mutations between snapshots -> nothing should be rewritten.
  ASSERT_TRUE(t.snapshot(nullptr).ok());
  EXPECT_EQ(t.last_snapshot_pages_written(), 0u);
}

TEST(IncrementalCheckpoint, SingleKeyEditRewritesOnlyItsPath) {
  MemPageStore store(1);
  Options opt;
  opt.page_store = &store;
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 160;
  opt.frame_bytes = 4096;
  Crowtree t(opt);

  std::map<std::string, std::string> oracle;
  Fill(&t, 200, &oracle);

  ASSERT_TRUE(t.snapshot(nullptr).ok());
  uint64_t total = t.last_snapshot_pages_written();
  ASSERT_GT(total, 4u);  // several leaves + inner level(s)

  // Touch exactly one key, flush, snapshot again.
  uint64_t slot = 100000;
  ASSERT_TRUE(t.apply(slot, Put1(Key(7), "updated")).ok());
  t.force_advance_slot(slot);
  ASSERT_TRUE(t.flush().ok());
  oracle[Key(7)] = "updated";
  ASSERT_TRUE(t.snapshot(nullptr).ok());

  uint64_t rewritten = t.last_snapshot_pages_written();
  EXPECT_GE(rewritten, 1u);     // the touched leaf was folded + written
  EXPECT_LT(rewritten, total);  // unchanged leaves/inners were retained
}

TEST(IncrementalCheckpoint, SpaceIsReusedAcrossManyCheckpoints) {
  MemPageStore store(1);
  Options opt;
  opt.page_store = &store;
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 160;
  opt.frame_bytes = 4096;

  std::map<std::string, std::string> oracle;
  uint64_t early = 0;
  {
    Crowtree t(opt);
    Fill(&t, 100, &oracle);
    ASSERT_TRUE(t.snapshot(nullptr).ok());

    // Rewrite the SAME small key set every round and snapshot. With durable-page
    // GC, freed extents from two snapshots ago are reused, so the file reaches a
    // steady size instead of growing ~linearly in the number of rounds.
    uint64_t slot = 100000;
    t.force_advance_slot(99999);
    for (int round = 0; round < 50; ++round) {
      for (int i : {1, 2, 3, 4, 5}) {
        std::string v = "r" + std::to_string(round);
        ASSERT_TRUE(t.apply(slot, Put1(Key(i), v)).ok());
        ASSERT_TRUE(t.flush().ok());
        oracle[Key(i)] = v;
        ++slot;
      }
      ASSERT_TRUE(t.snapshot(nullptr).ok());
      if (round == 9) {
        early = store.size();
      }
    }
    uint64_t late = store.size();
    ASSERT_GT(early, 0u);
    // Steady state: rewriting fixed-size pages reuses exactly-sized freed gaps,
    // so the file barely grows over the last 40 rounds (a small slack covers
    // two-generation retention / manifest jitter).
    EXPECT_LE(late, early + 8u * 4096u);
  }

  std::unique_ptr<Crowtree> t2;
  ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
  for (const auto& kv : oracle) {
    std::string v;
    uint64_t s;
    ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
    EXPECT_EQ(v, kv.second) << "key " << kv.first;
  }
}

TEST(IncrementalCheckpoint, ReopenAfterIncrementalSeesAllValues) {
  MemPageStore store(1);
  Options opt;
  opt.page_store = &store;
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 160;
  opt.frame_bytes = 4096;

  std::map<std::string, std::string> oracle;
  {
    Crowtree t(opt);
    Fill(&t, 200, &oracle);
    ASSERT_TRUE(t.snapshot(nullptr).ok());  // snapshot 1: full image

    // Mutate a spread of keys across different leaves, then snapshot again.
    uint64_t slot = 100000;
    t.force_advance_slot(99999);
    for (int i : {3, 50, 99, 150, 199}) {
      ASSERT_TRUE(t.apply(slot, Put1(Key(i), "v2_" + std::to_string(i))).ok());
      ASSERT_TRUE(t.flush().ok());
      oracle[Key(i)] = "v2_" + std::to_string(i);
      ++slot;
    }
    ASSERT_TRUE(t.snapshot(nullptr).ok());             // snapshot 2: incremental
    EXPECT_LT(t.last_snapshot_pages_written(), 200u);  // not a full rewrite
  }

  // Reopen from the incremental snapshot: unchanged keys are read from the
  // first snapshot's region (retained addrs); mutated keys from the second.
  std::unique_ptr<Crowtree> t2;
  ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
  for (const auto& kv : oracle) {
    std::string v;
    uint64_t s;
    ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
    EXPECT_EQ(v, kv.second) << "key " << kv.first;
  }
}
