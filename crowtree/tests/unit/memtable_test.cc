// CT7: MemTable (L0) tests.
#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "crowtree/cell.h"
#include "crowtree/memtable.h"

using namespace crowtree;

namespace {
bool Put(MemTable& mt, const std::string& k, uint64_t slot, const std::string& v) {
  std::string cell = EncodeCell(slot, OpKind::kPut, Slice(v));
  return mt.Upsert(Slice(k), slot, Slice(cell));
}
uint64_t SlotOf(const std::string& cell) { return CellView{Slice(cell)}.slot(); }
std::string ValOf(const std::string& cell) {
  return CellView{Slice(cell)}.value().ToString();
}
}  // namespace

TEST(MemTable, HighestSlotWins) {
  MemTable mt;
  EXPECT_TRUE(Put(mt, "k", 5, "v5"));
  EXPECT_FALSE(Put(mt, "k", 3, "v3"));  // lower slot rejected
  EXPECT_TRUE(Put(mt, "k", 8, "v8"));   // higher slot wins
  EXPECT_FALSE(Put(mt, "k", 8, "v8b")); // equal slot rejected (idempotent)
  std::string cell;
  ASSERT_TRUE(mt.Get("k", &cell));
  EXPECT_EQ(SlotOf(cell), 8u);
  EXPECT_EQ(ValOf(cell), "v8");
  EXPECT_EQ(mt.Count(), 1u);
}

TEST(MemTable, DurableFloorDrops) {
  MemTable mt;
  mt.SetDurableFloor(10);
  EXPECT_FALSE(Put(mt, "k", 5, "old"));   // <= floor, already in L1
  EXPECT_FALSE(Put(mt, "k", 10, "eq"));   // == floor dropped
  EXPECT_TRUE(Put(mt, "k", 11, "new"));   // > floor accepted
  EXPECT_EQ(mt.Count(), 1u);
}

TEST(MemTable, DrainUpToPrefix) {
  MemTable mt;
  Put(mt, "a", 1, "x");
  Put(mt, "b", 5, "y");
  Put(mt, "c", 3, "z");
  Put(mt, "d", 9, "w");
  // Drain slots <= 5: a(1), c(3), b(5). d(9) retained.
  auto drained = mt.DrainUpTo(5);
  ASSERT_EQ(drained.size(), 3u);
  // Returned in key order.
  EXPECT_EQ(drained[0].key, "a");
  EXPECT_EQ(drained[1].key, "b");
  EXPECT_EQ(drained[2].key, "c");
  EXPECT_EQ(mt.Count(), 1u);
  std::string cell;
  EXPECT_FALSE(mt.Get("a", &cell));
  EXPECT_TRUE(mt.Get("d", &cell));
  EXPECT_EQ(SlotOf(cell), 9u);
}

TEST(MemTable, SnapshotOrdered) {
  MemTable mt;
  Put(mt, "c", 1, "x");
  Put(mt, "a", 2, "y");
  Put(mt, "b", 3, "z");
  auto snap = mt.Snapshot();
  ASSERT_EQ(snap.size(), 3u);
  EXPECT_EQ(snap[0].key, "a");
  EXPECT_EQ(snap[1].key, "b");
  EXPECT_EQ(snap[2].key, "c");
  EXPECT_EQ(snap[0].slot, 2u);
}

TEST(MemTable, BytesAccounting) {
  MemTable mt;
  EXPECT_EQ(mt.ApproxBytes(), 0u);
  Put(mt, "key", 1, "value");
  size_t b1 = mt.ApproxBytes();
  EXPECT_GT(b1, 0u);
  Put(mt, "key", 2, "value-longer");
  EXPECT_NE(mt.ApproxBytes(), 0u);
  mt.DrainUpTo(100);
  EXPECT_EQ(mt.ApproxBytes(), 0u);
}

TEST(MemTable, HotKeyCollapse) {
  MemTable mt;
  for (uint64_t s = 1; s <= 1000; ++s) {
    Put(mt, "hot", s, "v" + std::to_string(s));
  }
  EXPECT_EQ(mt.Count(), 1u);  // collapses to one cell
  std::string cell;
  ASSERT_TRUE(mt.Get("hot", &cell));
  EXPECT_EQ(SlotOf(cell), 1000u);
}

TEST(MemTable, Tombstone) {
  MemTable mt;
  Put(mt, "k", 1, "v");
  std::string del = EncodeCell(2, OpKind::kDelete);
  EXPECT_TRUE(mt.Upsert(Slice("k"), 2, Slice(del)));
  std::string cell;
  ASSERT_TRUE(mt.Get("k", &cell));
  EXPECT_TRUE(CellView{Slice(cell)}.is_tombstone());
}

TEST(MemTable, ConcurrentUpsertAndGet) {
  MemTable mt;
  std::atomic<bool> stop{false};
  std::vector<std::thread> writers;
  for (int w = 0; w < 4; ++w) {
    writers.emplace_back([&, w] {
      for (uint64_t s = 1; s <= 2000; ++s) {
        Put(mt, "key" + std::to_string(w), s, "v");
      }
    });
  }
  std::thread reader([&] {
    std::string cell;
    while (!stop.load(std::memory_order_relaxed)) {
      mt.Get("key0", &cell);
    }
  });
  for (auto& t : writers) t.join();
  stop.store(true);
  reader.join();
  // Each key collapsed to its highest slot (2000).
  for (int w = 0; w < 4; ++w) {
    std::string cell;
    ASSERT_TRUE(mt.Get("key" + std::to_string(w), &cell));
    EXPECT_EQ(SlotOf(cell), 2000u);
  }
}
