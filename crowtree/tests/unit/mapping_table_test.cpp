// CT5: mapping table tests (alloc/free/recycle, growth, atomic store/load).
#include "crowtree/mapping_table.h"
#include "crowtree/page.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

using namespace crowtree;

namespace
{
// A trivial page used purely as an identity marker in the mapping table.
PageBase *make_leaf()
{
    return new LeafBase();
}
} // namespace

TEST(MappingTable, AllocStoreGet)
{
    MappingTable mt;
    uint64_t     page_id = mt.allocate_page_id();
    EXPECT_NE(page_id, kInvalidPageId);
    EXPECT_EQ(mt.get(page_id), nullptr);

    PageBase *page = make_leaf();
    mt.store(page_id, page);
    EXPECT_EQ(mt.get(page_id), page);
    EXPECT_EQ(page->page_id, page_id);
    delete page;
}

TEST(MappingTable, Invalidpage_id)
{
    MappingTable mt;
    EXPECT_EQ(mt.get(kInvalidPageId), nullptr);
    EXPECT_EQ(mt.get(123456), nullptr); // never allocated
}

TEST(MappingTable, FreeAndRecycle)
{
    MappingTable mt;
    uint64_t     a = mt.allocate_page_id();
    uint64_t     b = mt.allocate_page_id();
    EXPECT_NE(a, b);
    mt.free_page_id(a);
    EXPECT_EQ(mt.get(a), nullptr);
    uint64_t c = mt.allocate_page_id();
    EXPECT_EQ(c, a); // recycled from the free list (LIFO)
}

TEST(MappingTable, FreePidClearsUnloadedSlot)
{
    MappingTable mt;
    uint64_t     page_id = mt.allocate_page_id();
    mt.store_unloaded(page_id, 123, 4096);
    ASSERT_NE(mt.get(page_id), nullptr);
    ASSERT_TRUE(MappingTable::is_unloaded(mt.get(page_id)));

    mt.free_page_id(page_id);
    EXPECT_EQ(mt.get(page_id), nullptr);

    uint64_t again = mt.allocate_page_id();
    EXPECT_EQ(again, page_id);
}

TEST(MappingTable, SegmentGrowth)
{
    MappingTable mt;
    // Allocate enough PIDs to span multiple segments.
    std::vector<uint64_t> pids;
    pids.reserve((MappingTable::kSegmentSize * 3) + 5);
    for (uint64_t i = 0; i < (MappingTable::kSegmentSize * 3) + 5; ++i) {
        pids.push_back(mt.allocate_page_id());
    }
    EXPECT_GE(mt.segments_allocated(), 4U);
    // store + read back across segment boundaries.
    PageBase *page  = make_leaf();
    uint64_t  cross = pids[MappingTable::kSegmentSize + 1];
    mt.store(cross, page);
    EXPECT_EQ(mt.get(cross), page);
    delete page;
}

TEST(MappingTable, ConcurrentReadersSingleWriter)
{
    MappingTable mt;
    uint64_t     page_id = mt.allocate_page_id();
    PageBase    *p1      = make_leaf();
    mt.store(page_id, p1);

    std::atomic<bool>        stop{false};
    std::atomic<long>        reads{0};
    std::vector<std::thread> readers;
    readers.reserve(4);
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                PageBase *got = mt.get(page_id);
                if (got != nullptr) {
                    reads.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    // Single writer swaps the slot repeatedly.
    std::vector<PageBase *> garbage;
    for (int i = 0; i < 1000; ++i) {
        PageBase *np = make_leaf();
        mt.store(page_id, np);
        garbage.push_back(np);
    }
    stop.store(true);
    for (auto &t : readers) {
        t.join();
    }
    EXPECT_GT(reads.load(), 0);

    delete p1;
    for (auto *g : garbage) {
        delete g;
    }
}
