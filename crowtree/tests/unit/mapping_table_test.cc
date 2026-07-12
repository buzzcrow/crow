// CT5: mapping table tests (alloc/free/recycle, growth, atomic store/load).
#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "crowtree/mapping_table.h"
#include "crowtree/page.h"

using namespace crowtree;

namespace {
// A trivial page used purely as an identity marker in the mapping table.
PageBase* MakeLeaf() { return new LeafBase(); }
}  // namespace

TEST(MappingTable, AllocStoreGet) {
  MappingTable mt;
  uint64_t pid = mt.AllocatePID();
  EXPECT_NE(pid, kInvalidPID);
  EXPECT_EQ(mt.Get(pid), nullptr);

  PageBase* page = MakeLeaf();
  mt.Store(pid, page);
  EXPECT_EQ(mt.Get(pid), page);
  EXPECT_EQ(page->pid, pid);
  delete page;
}

TEST(MappingTable, InvalidPid) {
  MappingTable mt;
  EXPECT_EQ(mt.Get(kInvalidPID), nullptr);
  EXPECT_EQ(mt.Get(123456), nullptr);  // never allocated
}

TEST(MappingTable, FreeAndRecycle) {
  MappingTable mt;
  uint64_t a = mt.AllocatePID();
  uint64_t b = mt.AllocatePID();
  EXPECT_NE(a, b);
  mt.FreePID(a);
  EXPECT_EQ(mt.Get(a), nullptr);
  uint64_t c = mt.AllocatePID();
  EXPECT_EQ(c, a);  // recycled from the free list (LIFO)
}

TEST(MappingTable, SegmentGrowth) {
  MappingTable mt;
  // Allocate enough PIDs to span multiple segments.
  std::vector<uint64_t> pids;
  for (uint64_t i = 0; i < MappingTable::kSegmentSize * 3 + 5; ++i) {
    pids.push_back(mt.AllocatePID());
  }
  EXPECT_GE(mt.SegmentsAllocated(), 4u);
  // Store + read back across segment boundaries.
  PageBase* page = MakeLeaf();
  uint64_t cross = pids[MappingTable::kSegmentSize + 1];
  mt.Store(cross, page);
  EXPECT_EQ(mt.Get(cross), page);
  delete page;
}

TEST(MappingTable, ConcurrentReadersSingleWriter) {
  MappingTable mt;
  uint64_t pid = mt.AllocatePID();
  PageBase* p1 = MakeLeaf();
  mt.Store(pid, p1);

  std::atomic<bool> stop{false};
  std::atomic<long> reads{0};
  std::vector<std::thread> readers;
  for (int i = 0; i < 4; ++i) {
    readers.emplace_back([&] {
      while (!stop.load(std::memory_order_relaxed)) {
        PageBase* got = mt.Get(pid);
        if (got != nullptr) reads.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  // Single writer swaps the slot repeatedly.
  std::vector<PageBase*> garbage;
  for (int i = 0; i < 1000; ++i) {
    PageBase* np = MakeLeaf();
    mt.Store(pid, np);
    garbage.push_back(np);
  }
  stop.store(true);
  for (auto& t : readers) t.join();
  EXPECT_GT(reads.load(), 0);

  delete p1;
  for (auto* g : garbage) delete g;
}
