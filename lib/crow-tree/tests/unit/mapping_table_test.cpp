// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// CT5: mapping table tests (alloc/free/recycle, growth, atomic store/load).
#include "crow-tree/epoch.h"
#include "crow-tree/mapping_slot.h"
#include "crow-tree/mapping_table.h"
#include "crow-tree/page.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

using namespace crow::tree;
using namespace crow::tree::slot_word;

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
    EXPECT_EQ(mt.get_resident(page_id), nullptr);

    PageBase *page = make_leaf();
    mt.store(page_id, page);
    EXPECT_EQ(mt.get_resident(page_id), page);
    EXPECT_EQ(page->page_id, page_id);
    delete page;
}

TEST(MappingTable, Invalidpage_id)
{
    MappingTable mt;
    EXPECT_EQ(mt.get_resident(kInvalidPageId), nullptr);
    EXPECT_EQ(mt.get_resident(123456), nullptr); // never allocated
}

TEST(MappingTable, AllocatePageIdIsMonotonic)
{
    // Plan-tree #14 D1: PIDs are never recycled, even across many allocations.
    MappingTable mt;
    uint64_t     prev = mt.allocate_page_id();
    for (int i = 0; i < 100; ++i) {
        uint64_t next = mt.allocate_page_id();
        EXPECT_GT(next, prev);
        prev = next;
    }
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
    EXPECT_EQ(mt.get_resident(cross), page);
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
                PageBase *got = mt.get_resident(page_id);
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

TEST(MappingTable, StoreUnloadedGetWord)
{
    MappingTable mt;
    uint64_t     page_id = mt.allocate_page_id();
    // Store an unloaded descriptor (addr=4096, plen=512, iu=512).
    uint32_t iu   = 512;
    uint64_t addr = 4096;
    uint32_t plen = 512;
    mt.store_unloaded(page_id, addr, plen, iu);

    uint64_t w = mt.get_word(page_id);
    EXPECT_TRUE(is_unloaded(w));
    EXPECT_FALSE(is_empty(w));
    EXPECT_FALSE(is_resident(w));
    EXPECT_EQ(unloaded_iu_index(w), addr / iu);
    EXPECT_EQ(unloaded_iu_count(w), plen / iu); // no rounding needed

    // get_resident should return nullptr for an unloaded slot.
    EXPECT_EQ(mt.get_resident(page_id), nullptr);
}

TEST(MappingTable, StoreUnloadedRoundUpIu)
{
    MappingTable mt;
    uint64_t     page_id = mt.allocate_page_id();
    uint32_t     iu      = 4096;
    uint64_t     addr    = 8192;
    uint32_t     plen    = 100; // smaller than one IU -> rounds up to 1 IU
    mt.store_unloaded(page_id, addr, plen, iu);

    uint64_t w = mt.get_word(page_id);
    EXPECT_TRUE(is_unloaded(w));
    EXPECT_EQ(unloaded_iu_index(w), addr / iu);
    EXPECT_EQ(unloaded_iu_count(w), 1U); // round_up_to_iu(100, 4096) / 4096 = 1
}

TEST(MappingTable, ClearSlot)
{
    MappingTable mt;
    uint64_t     page_id = mt.allocate_page_id();
    PageBase    *page    = make_leaf();
    mt.store(page_id, page);
    EXPECT_TRUE(is_resident(mt.get_word(page_id)));

    mt.clear(page_id);
    EXPECT_TRUE(is_empty(mt.get_word(page_id)));
    EXPECT_EQ(mt.get_resident(page_id), nullptr);
    delete page;
}

TEST(MappingTable, StoreWordPackedUnloaded)
{
    MappingTable mt;
    uint64_t     page_id = mt.allocate_page_id();
    uint64_t     word    = pack_unloaded(42, 7);
    mt.store_word(page_id, word);

    uint64_t got = mt.get_word(page_id);
    EXPECT_EQ(got, word);
    EXPECT_TRUE(is_unloaded(got));
    EXPECT_EQ(unloaded_iu_index(got), 42U);
    EXPECT_EQ(unloaded_iu_count(got), 7U);
}

// -- #14b: segment recycling --------------------------------------------

TEST(MappingTable, SegmentRecycledWhenLastSlotCleared)
{
    // No epoch manager wired: recycled segments are deleted immediately on
    // the writer thread (fine -- no concurrent readers in this test).
    MappingTable            mt;
    std::vector<uint64_t>   pids;
    std::vector<PageBase *> pages;
    pids.reserve(MappingTable::kSegmentSize);
    pages.reserve(MappingTable::kSegmentSize);
    for (uint64_t i = 0; i < MappingTable::kSegmentSize; ++i) {
        uint64_t  pid = mt.allocate_page_id();
        PageBase *p   = make_leaf();
        mt.store(pid, p);
        pids.push_back(pid);
        pages.push_back(p);
    }
    ASSERT_EQ(mt.segments_allocated(), 1U);

    for (uint64_t i = 0; i + 1 < pids.size(); ++i) {
        mt.clear(pids[i]);
    }
    EXPECT_EQ(mt.segments_allocated(), 1U); // one live slot left -> not recyclable

    mt.clear(pids.back()); // last live slot -> segment recycles
    EXPECT_EQ(mt.segments_allocated(), 0U);

    // A PID inside a recycled segment reads back as empty, never UB / stale data.
    EXPECT_TRUE(is_empty(mt.get_word(pids.front())));
    EXPECT_EQ(mt.get_resident(pids.front()), nullptr);

    for (PageBase *p : pages) {
        delete p;
    }
}

TEST(MappingTable, SegmentNotRecycledWhilePartiallyLive)
{
    MappingTable mt;
    uint64_t     a  = mt.allocate_page_id();
    uint64_t     b  = mt.allocate_page_id();
    PageBase    *pa = make_leaf();
    PageBase    *pb = make_leaf();
    mt.store(a, pa);
    mt.store(b, pb);
    ASSERT_EQ(mt.segments_allocated(), 1U);

    mt.clear(a);
    EXPECT_EQ(mt.segments_allocated(), 1U); // b still live
    EXPECT_EQ(mt.get_resident(b), pb);

    mt.clear(b);
    EXPECT_EQ(mt.segments_allocated(), 0U);

    delete pa;
    delete pb;
}

// -- #14c/#14d: segment-image persistence bookkeeping -------------------

TEST(MappingTable, SegmentAtReflectsAllocationAndRecycling)
{
    MappingTable mt;
    EXPECT_EQ(mt.segment_at(0), nullptr);

    uint64_t  page_id = mt.allocate_page_id();
    PageBase *page    = make_leaf();
    mt.store(page_id, page);
    MappingSegment *seg = mt.segment_at(0);
    ASSERT_NE(seg, nullptr);
    EXPECT_TRUE(seg->is_dirty());

    mt.clear(page_id); // last live slot -> recycles
    EXPECT_EQ(mt.segment_at(0), nullptr);
    delete page;
}

TEST(MappingTable, CommitSegmentPersistMarksClean)
{
    MappingTable mt;
    uint64_t     page_id = mt.allocate_page_id();
    PageBase    *page    = make_leaf();
    mt.store(page_id, page);

    MappingSegment *seg = mt.segment_at(0);
    ASSERT_NE(seg, nullptr);
    uint64_t seen_seq = seg->write_seq.load();
    ASSERT_TRUE(seg->is_dirty());

    EXPECT_TRUE(mt.commit_segment_persist(0, seg, seen_seq, /*new_generation=*/1, /*new_image_addr=*/4096,
                                          /*new_image_len=*/8224, /*new_image_crc=*/0xABCD));
    EXPECT_FALSE(seg->is_dirty());
    EXPECT_EQ(seg->generation.load(), 1U);
    EXPECT_EQ(seg->image_addr, 4096U);
    EXPECT_EQ(seg->image_len, 8224U);
    EXPECT_EQ(seg->image_crc, 0xABCDU);

    delete page;
}

TEST(MappingTable, CommitSegmentPersistSkippedIfWrittenAgainDuringGap)
{
    MappingTable mt;
    uint64_t     page_id = mt.allocate_page_id();
    PageBase    *page    = make_leaf();
    mt.store(page_id, page);

    MappingSegment *seg       = mt.segment_at(0);
    uint64_t        seen_seq  = seg->write_seq.load();
    uint64_t        page_id_2 = mt.allocate_page_id();
    PageBase       *page2     = make_leaf();
    mt.store(page_id_2, page2); // "concurrent" write during the prepare->commit gap

    // The image we "prepared" (at seen_seq) is now stale -- commit must refuse.
    EXPECT_FALSE(mt.commit_segment_persist(0, seg, seen_seq, 1, 4096, 8224, 0xABCD));
    EXPECT_TRUE(seg->is_dirty()); // still dirty -- next snapshot must re-image it

    delete page;
    delete page2;
}

TEST(MappingTable, CommitSegmentPersistSkippedIfRecycledDuringGap)
{
    MappingTable mt;
    uint64_t     page_id = mt.allocate_page_id();
    PageBase    *page    = make_leaf();
    mt.store(page_id, page);

    MappingSegment *seg      = mt.segment_at(0);
    uint64_t        seen_seq = seg->write_seq.load();

    mt.clear(page_id); // recycles segment 0 (no epoch wired -> deleted immediately)
    EXPECT_EQ(mt.segment_at(0), nullptr);

    // `seg` is dangling now for a real epoch-backed table, but here (no epoch
    // wired) it's already freed -- commit must not dereference it beyond the
    // top-level identity check, which fails first.
    EXPECT_FALSE(mt.commit_segment_persist(0, seg, seen_seq, 1, 4096, 8224, 0xABCD));

    delete page;
}

TEST(MappingTable, InstallRecoveredSegmentIsNotDirty)
{
    MappingTable          mt;
    std::vector<uint64_t> words(MappingTable::kSegmentSize, 0);
    words[5] = pack_unloaded(42, 3);

    mt.install_recovered_segment(2, /*generation=*/7, /*live_count=*/1, words, /*image_addr=*/8192,
                                 /*image_len=*/8224, /*image_crc=*/0x99);

    MappingSegment *seg = mt.segment_at(2);
    ASSERT_NE(seg, nullptr);
    EXPECT_FALSE(seg->is_dirty());
    EXPECT_EQ(seg->generation.load(), 7U);
    EXPECT_EQ(seg->live_count.load(), 1U);
    EXPECT_EQ(seg->image_addr, 8192U);
    EXPECT_EQ(seg->image_len, 8224U);
    EXPECT_EQ(seg->image_crc, 0x99U);

    // Recovered PID 2*kSegmentSize + 5 reads back as the installed descriptor.
    uint64_t recovered_pid = (2 * MappingTable::kSegmentSize) + 5;
    uint64_t w             = mt.get_word(recovered_pid);
    EXPECT_TRUE(is_unloaded(w));
    EXPECT_EQ(unloaded_iu_index(w), 42U);
    EXPECT_EQ(unloaded_iu_count(w), 3U);
}

TEST(MappingTable, RecycledSegmentFreedOnlyAfterEpochGuardDrains)
{
    MappingTable mt;
    EpochManager epoch;
    mt.set_epoch_manager(&epoch);

    uint64_t  page_id = mt.allocate_page_id();
    PageBase *page    = make_leaf();
    mt.store(page_id, page);
    ASSERT_EQ(mt.segments_allocated(), 1U);

    // Simulate a concurrent reader that entered before the segment recycles.
    // Guard::release() is private (RAII-only); scope the guard to drop it.
    {
        EpochManager::Guard guard = epoch.enter();

        mt.clear(page_id); // last live slot -> segment CAS'd out and retired
        EXPECT_EQ(mt.segments_allocated(), 0U);
        EXPECT_EQ(epoch.pending_retired(), 1U);
        EXPECT_EQ(epoch.try_reclaim(), 0U); // the open guard could still see it -> not freed
    }
    EXPECT_EQ(epoch.try_reclaim(), 1U); // now safe to free
    EXPECT_EQ(epoch.pending_retired(), 0U);

    delete page;
}
