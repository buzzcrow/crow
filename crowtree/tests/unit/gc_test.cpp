// plan-tree #21: GC sweep + dual watermark + GcStats.
#include "crowtree/crowtree.h"
#include "crowtree/page.h"
#include "crowtree/page_store.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

using namespace crowtree;

namespace
{
Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

Batch del_one(const std::string &k)
{
    return Batch{{{.key = k, .kind = OpKind::kDelete, .value = ""}}};
}

page_type head_type(Crowtree &t)
{
    return t.mapping().get_resident(t.root_page_id())->type;
}
} // namespace

TEST(Gc, SetWatermarkTakesMinAndIsMonotonic)
{
    Crowtree t;
    t.set_gc_watermark(10, 5);
    EXPECT_EQ(t.gc_watermark(), 5U);
    // A later call whose min is *below* the current floor must not regress it.
    t.set_gc_watermark(3, 20);
    EXPECT_EQ(t.gc_watermark(), 5U);
    // A later call whose min genuinely advances the floor does move it.
    t.set_gc_watermark(8, 8);
    EXPECT_EQ(t.gc_watermark(), 8U);
}

TEST(Gc, CollectGarbageBelowWatermarkIsNoOp)
{
    Crowtree t;
    ASSERT_TRUE(t.apply(1, put_one("a", "A")).ok());
    ASSERT_TRUE(t.apply(2, del_one("a")).ok());
    ASSERT_TRUE(t.flush().ok());
    // gc_watermark() defaults to 0; the tombstone at slot 2 is not yet eligible.
    GcStats stats = t.collect_garbage();
    EXPECT_EQ(stats.tombstones_dropped, 0U);
    EXPECT_EQ(stats.pages_freed, 0U);
    EXPECT_EQ(stats.bytes_freed, 0U);
}

// This is the exact gap plan-tree #21 fixes: a leaf that receives a delete and
// then no further writes previously kept its tombstone past gc_floor_
// indefinitely, because both consolidate()'s delta-length trigger and
// snapshot()'s dirty-only rebuild only ever touch a leaf that's already
// dirty. Here the delete is folded into a fresh, clean LeafBase by the
// *last* consolidation this leaf will ever see, then nothing else ever
// touches it -- only an explicit collect_garbage() sweep can reclaim it.
TEST(Gc, CollectGarbageSweepsLeafWithoutFurtherWrites)
{
    Options opt;
    opt.max_delta_len = 4;
    Crowtree t(opt);
    for (uint64_t s = 1; s <= 4; ++s) {
        ASSERT_TRUE(t.apply(s, put_one("k" + std::to_string(s), "v")).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    // 5th delta (a delete of k1) trips consolidation -> folds into a fresh,
    // clean LeafBase with no BatchDelta chain on top.
    ASSERT_TRUE(t.apply(5, del_one("k1")).ok());
    ASSERT_TRUE(t.flush().ok());
    ASSERT_EQ(head_type(t), page_type::kLeafBase);

    // Nothing else ever touches this leaf again. Advance the watermark past
    // the delete's slot and sweep explicitly.
    t.set_gc_watermark(5, 5);
    GcStats stats = t.collect_garbage();
    EXPECT_EQ(stats.tombstones_dropped, 1U);
    EXPECT_GE(stats.pages_freed, 1U);
    EXPECT_GT(stats.bytes_freed, 0U);
    ASSERT_EQ(head_type(t), page_type::kLeafBase); // rebuilt leaf, still clean

    // Idempotent: a second sweep has nothing left to drop.
    GcStats stats2 = t.collect_garbage();
    EXPECT_EQ(stats2.tombstones_dropped, 0U);
    EXPECT_EQ(stats2.pages_freed, 0U);
    EXPECT_EQ(stats2.bytes_freed, 0U);

    // Delete still honored (not resurrected); other keys unaffected.
    std::string v;
    uint64_t    slot;
    EXPECT_FALSE(t.get(Slice("k1"), &slot, &v));
    for (int i = 2; i <= 4; ++i) {
        EXPECT_TRUE(t.get(Slice("k" + std::to_string(i)), &slot, &v));
    }
}

// A background sweep must not demand-load evicted leaves just to check GC
// eligibility -- that would defeat eviction (#17).
TEST(Gc, CollectGarbageSkipsEvictedLeaves)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    for (int i = 0; i < 200; ++i) {
        std::string key = "key" + std::to_string(1000 + i);
        ASSERT_TRUE(t.apply(i + 1, put_one(key, "v")).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    ASSERT_TRUE(t.snapshot(nullptr).ok()); // all reachable pages now clean
    EXPECT_GT(t.evict_clean_leaves(2), 0U);

    uint32_t used_after_evict = t.buffer_pool()->stats().used;
    t.set_gc_watermark(1000000, 1000000); // would make any tombstone eligible
    GcStats stats = t.collect_garbage();
    // No tombstones exist in this dataset, but the real assertion is that the
    // sweep did not page anything back in to find that out.
    EXPECT_EQ(stats.tombstones_dropped, 0U);
    EXPECT_EQ(t.buffer_pool()->stats().used, used_after_evict);
}

// plan-tree #21's periodic trigger reuses the background-flush-thread machinery
// (Options.gc_interval_ms) rather than a second thread. This confirms it
// actually reclaims the same "delete with no further writes" leaf as
// CollectGarbageSweepsLeafWithoutFurtherWrites, but via the timer instead of
// an explicit collect_garbage() call.
TEST(Gc, PeriodicSweepReclaimsInBackground)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store        = &store;
    opt.max_delta_len     = 4;
    opt.background_flush  = true;
    opt.flush_interval_ms = 5;
    opt.gc_interval_ms    = 5;

    std::unique_ptr<Crowtree> t;
    ASSERT_TRUE(Crowtree::open(opt, &t).ok());
    for (uint64_t s = 1; s <= 4; ++s) {
        ASSERT_TRUE(t->apply(s, put_one("k" + std::to_string(s), "v")).ok());
        ASSERT_TRUE(t->flush().ok());
    }
    ASSERT_TRUE(t->apply(5, del_one("k1")).ok());
    ASSERT_TRUE(t->flush().ok());
    ASSERT_EQ(head_type(*t), page_type::kLeafBase);
    PageBase *before = t->mapping().get_resident(t->root_page_id());

    t->set_gc_watermark(5, 5);

    // Poll for the background thread to rebuild the leaf (a distinct PageBase*
    // at the same page_id) instead of calling collect_garbage() ourselves,
    // which would make it impossible to tell whether the timer ever fired.
    bool swept = false;
    for (int i = 0; i < 100 && !swept; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        swept = t->mapping().get_resident(t->root_page_id()) != before;
    }
    EXPECT_TRUE(swept) << "background GC thread never swept the stale tombstone";

    std::string v;
    uint64_t    slot;
    EXPECT_FALSE(t->get(Slice("k1"), &slot, &v));
}
