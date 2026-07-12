// PT9: IU block alignment (9.1-9.3) + debug store/codec on real frames (9.5).
#include "crowtree/crowtree.h"
#include "crowtree/debug_codec.h"
#include "crowtree/env.h"
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
  char b[16];
  snprintf(b, sizeof(b), "key%05d", i);
  return b;
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

TEST(Alignment, Iu4096CheckpointReopenEquals) {
  MemPageStore store(4096);  // aligned block device
  Options opt;
  opt.page_store = &store;
  opt.frame_bytes = 4096;  // frame_bytes % iu == 0
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 200;  // multi-level tree
  CrowtreeEnv env;

  std::map<std::string, std::string> oracle;
  {
    Crowtree t(env, opt);
    Fill(&t, 200, &oracle);
    ASSERT_GT(t.height(), 1);
    ASSERT_TRUE(t.checkpoint(nullptr).ok());
    // Every durable extent is IU-aligned + IU-sized, so the file is a 4 KiB
    // multiple.
    EXPECT_EQ(store.size() % 4096u, 0u);
  }

  std::unique_ptr<Crowtree> t2;
  ASSERT_TRUE(Crowtree::open(env, opt, &t2).ok());
  for (const auto& kv : oracle) {
    std::string v;
    uint64_t s;
    ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
    EXPECT_EQ(v, kv.second);
  }
}

TEST(Alignment, Iu4096AllocatorReuseStaysAligned) {
  MemPageStore store(4096);
  Options opt;
  opt.page_store = &store;
  opt.frame_bytes = 4096;
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 200;
  CrowtreeEnv env;

  std::map<std::string, std::string> oracle;
  Crowtree t(env, opt);
  Fill(&t, 80, &oracle);
  ASSERT_TRUE(t.checkpoint(nullptr).ok());
  uint64_t early = store.size();

  // Rewrite the same keys repeatedly; aligned gaps are reused so the file stays
  // a 4 KiB multiple and roughly flat.
  uint64_t slot = 100000;
  for (int round = 0; round < 30; ++round) {
    for (int i : {1, 2, 3, 4, 5}) {
      ASSERT_TRUE(t.apply(slot, Put1(Key(i), "r" + std::to_string(round))).ok());
      ASSERT_TRUE(t.flush().ok());
      ++slot;
    }
    ASSERT_TRUE(t.checkpoint(nullptr).ok());
    EXPECT_EQ(store.size() % 4096u, 0u);
  }
  EXPECT_LE(store.size(), early + 16u * 4096u);
}

TEST(Alignment, RejectsFrameNotIuAligned) {
  // The only geometry constraint now is frame_bytes % iu == 0 (the superblock
  // slot is IU-rounded, so any IU is supported).
  MemPageStore store(512);
  Options opt;
  opt.page_store = &store;
  opt.frame_bytes = 4097;  // not a multiple of 512
  CrowtreeEnv env;
  std::unique_ptr<Crowtree> t;
  EXPECT_EQ(Crowtree::open(env, opt, &t).code(), Code::kInvalidArgument);
}

// Larger-than-4096 IU (e.g. 16 KiB SSD): the superblock slot grows to the IU,
// so checkpoint/reopen round-trips with IU-sized, IU-aligned extents.
TEST(Alignment, LargeIu16KCheckpointReopen) {
  MemPageStore store(16384);
  Options opt;
  opt.page_store = &store;
  opt.frame_bytes = 16384;  // frame_bytes % iu == 0
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 200;  // multi-level tree
  CrowtreeEnv env;

  std::map<std::string, std::string> oracle;
  {
    Crowtree t(env, opt);
    Fill(&t, 150, &oracle);
    ASSERT_GT(t.height(), 1);
    ASSERT_TRUE(t.checkpoint(nullptr).ok());
    EXPECT_EQ(store.size() % 16384u, 0u);  // every extent IU-aligned + IU-sized
    // Two superblock slots of 16 KiB precede the page region.
    EXPECT_GE(store.size(), 2u * 16384u);
  }
  std::unique_ptr<Crowtree> t2;
  ASSERT_TRUE(Crowtree::open(env, opt, &t2).ok());
  for (const auto& kv : oracle) {
    std::string v;
    uint64_t s;
    ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
    EXPECT_EQ(v, kv.second);
  }
}

// A non-power-of-two IU that does not divide 4096 (previously rejected) now
// works because the superblock slot is rounded up to the IU.
TEST(Alignment, NonPowerOfTwoIuRoundTrip) {
  MemPageStore store(5000);
  Options opt;
  opt.page_store = &store;
  opt.frame_bytes = 10000;  // 10000 % 5000 == 0
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 300;
  CrowtreeEnv env;
  std::map<std::string, std::string> oracle;
  {
    Crowtree t(env, opt);
    Fill(&t, 80, &oracle);
    ASSERT_TRUE(t.checkpoint(nullptr).ok());
    EXPECT_EQ(store.size() % 5000u, 0u);
  }
  std::unique_ptr<Crowtree> t2;
  ASSERT_TRUE(Crowtree::open(env, opt, &t2).ok());
  for (const auto& kv : oracle) {
    std::string v;
    uint64_t s;
    ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
    EXPECT_EQ(v, kv.second);
  }
}

TEST(Alignment, DebugStoreTransparentRoundTrip) {
  MemPageStore inner(1);
  DebugPageStore dbg(&inner);
  Options opt;
  opt.page_store = &dbg;
  opt.frame_bytes = 4096;
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 200;
  CrowtreeEnv env;

  std::map<std::string, std::string> oracle;
  {
    Crowtree t(env, opt);
    Fill(&t, 120, &oracle);
    ASSERT_TRUE(t.checkpoint(nullptr).ok());
    EXPECT_GT(dbg.writes(), 0u);
  }
  std::unique_ptr<Crowtree> t2;
  ASSERT_TRUE(Crowtree::open(env, opt, &t2).ok());
  for (const auto& kv : oracle) {
    std::string v;
    uint64_t s;
    ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
    EXPECT_EQ(v, kv.second);
  }
}
