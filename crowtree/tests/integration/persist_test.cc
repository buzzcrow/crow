// PT3-PT5: checkpoint + recovery + durable round-trip integration tests.
#include <gtest/gtest.h>

#include <cstdio>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "crowtree/crowtree.h"
#include "crowtree/env.h"
#include "crowtree/page_store.h"

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
// Superblock slot size from persist.cc (each A/B slot is 4 KiB).
constexpr uint64_t kSbBytes = 4096;
}  // namespace

TEST(Persist, CheckpointThenReopenRestoresKeys) {
  MemPageStore store(1);
  Options opt;
  opt.page_store = &store;
  CrowtreeEnv env;

  {
    Crowtree t(env, opt);
    for (int i = 0; i < 50; ++i) {
      ASSERT_TRUE(t.Apply(i + 1, Put1(Key(i), "v" + std::to_string(i)), i + 1).ok());
    }
    ASSERT_TRUE(t.Flush().ok());
    uint64_t durable = 0;
    ASSERT_TRUE(t.Checkpoint(&durable).ok());
    EXPECT_EQ(durable, 50u);
  }

  std::unique_ptr<Crowtree> t2;
  ASSERT_TRUE(Crowtree::Open(env, opt, &t2).ok());
  EXPECT_EQ(t2->last_applied_slot(), 50u);
  for (int i = 0; i < 50; ++i) {
    std::string v;
    uint64_t s;
    ASSERT_TRUE(t2->Get(Slice(Key(i)), &s, &v)) << "missing " << Key(i);
    EXPECT_EQ(v, "v" + std::to_string(i));
  }
}

TEST(Persist, MultiLevelTreeSurvivesAndComparesEqual) {
  MemPageStore store(1);
  Options opt;
  opt.page_store = &store;
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 200;  // force a multi-level tree
  CrowtreeEnv env;

  std::shared_ptr<Snapshot> before;
  {
    Crowtree t(env, opt);
    for (int i = 0; i < 300; ++i) {
      ASSERT_TRUE(t.Apply(i + 1, Put1(Key(i), "payload-" + std::to_string(i)), i + 1).ok());
      ASSERT_TRUE(t.Flush().ok());
    }
    ASSERT_GT(t.Height(), 1);
    ASSERT_TRUE(t.Checkpoint().ok());
    before = t.SnapshotView();
  }

  std::unique_ptr<Crowtree> t2;
  ASSERT_TRUE(Crowtree::Open(env, opt, &t2).ok());
  EXPECT_GT(t2->Height(), 1);
  auto after = t2->SnapshotView();
  EXPECT_TRUE(before->Compare(*after).empty());
  EXPECT_EQ(before->size(), after->size());
}

TEST(Persist, ReapplyOldSlotsIsNoOp) {
  MemPageStore store(1);
  Options opt;
  opt.page_store = &store;
  CrowtreeEnv env;
  {
    Crowtree t(env, opt);
    ASSERT_TRUE(t.Apply(5, Put1("a", "A5"), 5).ok());
    ASSERT_TRUE(t.Flush().ok());
    ASSERT_TRUE(t.Checkpoint().ok());
  }
  std::unique_ptr<Crowtree> t2;
  ASSERT_TRUE(Crowtree::Open(env, opt, &t2).ok());
  // Re-applying a slot <= last_applied_slot must not regress the value.
  ASSERT_TRUE(t2->Apply(3, Put1("a", "STALE"), 5).ok());
  ASSERT_TRUE(t2->Flush().ok());
  std::string v;
  uint64_t s;
  ASSERT_TRUE(t2->Get(Slice("a"), &s, &v));
  EXPECT_EQ(v, "A5");
  EXPECT_EQ(s, 5u);
}

TEST(Persist, FreshOpenWithNoSuperblock) {
  MemPageStore store(1);
  Options opt;
  opt.page_store = &store;
  CrowtreeEnv env;
  std::unique_ptr<Crowtree> t;
  ASSERT_TRUE(Crowtree::Open(env, opt, &t).ok());
  EXPECT_EQ(t->last_applied_slot(), 0u);
  std::string v;
  uint64_t s;
  EXPECT_FALSE(t->Get(Slice("nope"), &s, &v));
  // Usable after a fresh open.
  ASSERT_TRUE(t->Apply(1, Put1("x", "X"), 1).ok());
  ASSERT_TRUE(t->Flush().ok());
  ASSERT_TRUE(t->Get(Slice("x"), &s, &v));
  EXPECT_EQ(v, "X");
}

TEST(Persist, CorruptNewestSuperblockFallsBackToPrevious) {
  MemPageStore store(1);
  Options opt;
  opt.page_store = &store;
  CrowtreeEnv env;
  {
    Crowtree t(env, opt);
    ASSERT_TRUE(t.Apply(1, Put1("a", "first"), 1).ok());
    ASSERT_TRUE(t.Flush().ok());
    ASSERT_TRUE(t.Checkpoint().ok());  // seq 1 -> slot 0

    ASSERT_TRUE(t.Apply(2, Put1("a", "second"), 2).ok());
    ASSERT_TRUE(t.Flush().ok());
    ASSERT_TRUE(t.Checkpoint().ok());  // seq 2 -> slot kSbBytes
  }
  // Corrupt the newest superblock slot (seq 2 lives at the second slot).
  std::vector<uint8_t> garbage(kSbBytes, 0xab);
  ASSERT_TRUE(store.WriteAt(kSbBytes, garbage.data(), garbage.size()).ok());

  std::unique_ptr<Crowtree> t2;
  ASSERT_TRUE(Crowtree::Open(env, opt, &t2).ok());
  // Recovery falls back to seq 1.
  EXPECT_EQ(t2->last_applied_slot(), 1u);
  std::string v;
  uint64_t s;
  ASSERT_TRUE(t2->Get(Slice("a"), &s, &v));
  EXPECT_EQ(v, "first");
}

TEST(Persist, FileBackendRoundTrip) {
  char tmpl[] = "/tmp/crowtree_persist_XXXXXX";
  int fd = mkstemp(tmpl);
  ASSERT_GE(fd, 0);
  close(fd);
  std::string path(tmpl);

  std::map<std::string, std::string> oracle;
  CrowtreeEnv env;
  {
    std::unique_ptr<FilePageStore> store;
    ASSERT_TRUE(FilePageStore::Open(path, 4096, &store).ok());
    Options opt;
    opt.page_store = store.get();
    opt.leaf_split_bytes = 256;
    Crowtree t(env, opt);
    std::mt19937 rng(7);
    uint64_t slot = 0;
    for (int i = 0; i < 200; ++i) {
      ++slot;
      std::string k = Key(rng() % 120);
      if (rng() % 5 == 0) {
        ASSERT_TRUE(t.Apply(slot, Del1(k), slot).ok());
        oracle.erase(k);
      } else {
        std::string val = "v" + std::to_string(slot);
        ASSERT_TRUE(t.Apply(slot, Put1(k, val), slot).ok());
        oracle[k] = val;
      }
      if (i % 9 == 0) {
        ASSERT_TRUE(t.Flush().ok());
      }
    }
    ASSERT_TRUE(t.Flush().ok());
    ASSERT_TRUE(t.Checkpoint().ok());
  }

  // Reopen the file in a brand-new store + engine.
  {
    std::unique_ptr<FilePageStore> store;
    ASSERT_TRUE(FilePageStore::Open(path, 4096, &store).ok());
    Options opt;
    opt.page_store = store.get();
    std::unique_ptr<Crowtree> t;
    ASSERT_TRUE(Crowtree::Open(env, opt, &t).ok());
    for (auto& kv : oracle) {
      std::string v;
      uint64_t s;
      ASSERT_TRUE(t->Get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
      EXPECT_EQ(v, kv.second);
    }
    // The snapshot retains tombstones (gc_floor = 0), so compare live entries.
    auto snap = t->SnapshotView();
    size_t live = 0;
    for (const auto& e : snap->entries()) {
      if (!CellView{Slice(e.cell)}.is_tombstone()) ++live;
    }
    EXPECT_EQ(live, oracle.size());
  }
  std::remove(path.c_str());
}
