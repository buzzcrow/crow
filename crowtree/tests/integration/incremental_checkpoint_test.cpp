// PT6d: incremental snapshot writes only dirty pages, retains clean pages'
// durable addrs, and reopens to identical state.
#include "crowtree/crowtree.h"
#include "crowtree/page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <map>
#include <memory>
#include <string>
#include <vector>

using namespace crowtree;

namespace
{
Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

std::string key(int i)
{
    std::array<char, 16> buf{};
    snprintf(buf.data(), buf.size(), "key%05d", i);
    return buf.data();
}

// build a multi-level tree by inserting K keys, flushing incrementally so leaves
// stay small (one bulk flush would make a single oversized leaf).
void fill(Crowtree *t, int K, std::map<std::string, std::string> *oracle)
{
    for (int i = 0; i < K; ++i) {
        std::string v = "val" + std::to_string(i);
        ASSERT_TRUE(t->apply(i + 1, put_one(key(i), v)).ok());
        ASSERT_TRUE(t->flush().ok());
        (*oracle)[key(i)] = v;
    }
}
} // namespace

TEST(IncrementalCheckpoint, SecondCheckpointWithoutChangesWritesNothing)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    std::map<std::string, std::string> oracle;
    fill(&t, 200, &oracle);

    ASSERT_TRUE(t.snapshot(nullptr).ok());
    uint64_t first = t.last_snapshot_pages_written();
    EXPECT_GT(first, 1U); // a multi-page tree: everything was dirty

    // No mutations between snapshots -> nothing should be rewritten.
    ASSERT_TRUE(t.snapshot(nullptr).ok());
    EXPECT_EQ(t.last_snapshot_pages_written(), 0U);
}

TEST(IncrementalCheckpoint, SingleKeyEditRewritesOnlyItsPath)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    std::map<std::string, std::string> oracle;
    fill(&t, 200, &oracle);

    ASSERT_TRUE(t.snapshot(nullptr).ok());
    uint64_t total = t.last_snapshot_pages_written();
    ASSERT_GT(total, 4U); // several leaves + inner level(s)

    // Touch exactly one key, flush, snapshot again.
    uint64_t slot = 100000;
    ASSERT_TRUE(t.apply(slot, put_one(key(7), "updated")).ok());
    t.force_advance_slot(slot);
    ASSERT_TRUE(t.flush().ok());
    oracle[key(7)] = "updated";
    ASSERT_TRUE(t.snapshot(nullptr).ok());

    uint64_t rewritten = t.last_snapshot_pages_written();
    EXPECT_GE(rewritten, 1U);    // the touched leaf was folded + written
    EXPECT_LT(rewritten, total); // unchanged leaves/inners were retained
}

// Regression (plan-tree #14c/#14d/#18 D4): snapshot's dirty-tracking is
// segment-level, not page-level -- confirm only the *dirty* segment(s) get a
// fresh image written, not every present segment. With kSegmentSize == 1024
// PIDs, a 200-key tree comfortably fits in one segment, so this also
// exercises the single-segment steady-state case explicitly.
TEST(IncrementalCheckpoint, OnlyDirtySegmentsAreRewritten)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    std::map<std::string, std::string> oracle;
    fill(&t, 200, &oracle);

    ASSERT_TRUE(t.snapshot(nullptr).ok());
    uint64_t first_segments = t.last_snapshot_segments_written();
    EXPECT_GE(first_segments, 1U); // first snapshot: at least the one segment in use

    // No mutations -> no segment should need re-imaging.
    ASSERT_TRUE(t.snapshot(nullptr).ok());
    EXPECT_EQ(t.last_snapshot_segments_written(), 0U);

    // A single-key edit still only touches PIDs within one segment (this
    // tree never grows past kSegmentSize PIDs) -- expect exactly that one
    // segment re-imaged, not zero and not "every segment".
    uint64_t slot = 100000;
    ASSERT_TRUE(t.apply(slot, put_one(key(7), "updated")).ok());
    t.force_advance_slot(slot);
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot(nullptr).ok());
    EXPECT_EQ(t.last_snapshot_segments_written(), 1U);
}

// Same regression, but with a tree big enough (kSegmentSize == 1024 PIDs per
// segment) to actually span multiple mapping-table segments: a single-key
// edit must re-image only the one segment holding the touched leaf's PID,
// leaving every other segment's image/generation untouched.
TEST(IncrementalCheckpoint, MultiSegmentTreeOnlyRewritesTouchedSegment)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    std::map<std::string, std::string> oracle;
    fill(&t, 3000, &oracle); // enough splits to allocate well past 1024 PIDs

    ASSERT_TRUE(t.snapshot(nullptr).ok());
    size_t total_segments = t.mapping().segments_allocated();
    ASSERT_GT(total_segments, 1U) << "test needs a multi-segment tree to be meaningful";

    uint64_t slot = 1000000;
    ASSERT_TRUE(t.apply(slot, put_one(key(7), "updated")).ok());
    t.force_advance_slot(slot);
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot(nullptr).ok());
    EXPECT_EQ(t.last_snapshot_segments_written(), 1U);
    EXPECT_LT(t.last_snapshot_segments_written(), total_segments);
}

TEST(IncrementalCheckpoint, SpaceIsReusedAcrossManyCheckpoints)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;

    std::map<std::string, std::string> oracle;
    uint64_t                           early = 0;
    {
        Crowtree t(opt);
        fill(&t, 100, &oracle);
        ASSERT_TRUE(t.snapshot(nullptr).ok());

        // Rewrite the SAME small key set every round and snapshot. With durable-page
        // GC, freed extents from two snapshots ago are reused, so the file reaches a
        // steady size instead of growing ~linearly in the number of rounds.
        uint64_t slot = 100000;
        t.force_advance_slot(99999);
        for (int round = 0; round < 50; ++round) {
            for (int i : {1, 2, 3, 4, 5}) {
                std::string v = "r" + std::to_string(round);
                ASSERT_TRUE(t.apply(slot, put_one(key(i), v)).ok());
                ASSERT_TRUE(t.flush().ok());
                oracle[key(i)] = v;
                ++slot;
            }
            ASSERT_TRUE(t.snapshot(nullptr).ok());
            if (round == 9) {
                early = store.size();
            }
        }
        uint64_t late = store.size();
        ASSERT_GT(early, 0U);
        // Steady state: rewriting fixed-size pages reuses exactly-sized freed gaps,
        // so the file barely grows over the last 40 rounds (a small slack covers
        // two-generation retention / manifest jitter).
        EXPECT_LE(late, early + (static_cast<uint64_t>(8) * 4096U));
    }

    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second) << "key " << kv.first;
    }
}

TEST(IncrementalCheckpoint, ReopenAfterIncrementalSeesAllValues)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;

    std::map<std::string, std::string> oracle;
    {
        Crowtree t(opt);
        fill(&t, 200, &oracle);
        ASSERT_TRUE(t.snapshot(nullptr).ok()); // snapshot 1: full image

        // Mutate a spread of keys across different leaves, then snapshot again.
        uint64_t slot = 100000;
        t.force_advance_slot(99999);
        for (int i : {3, 50, 99, 150, 199}) {
            ASSERT_TRUE(t.apply(slot, put_one(key(i), "v2_" + std::to_string(i))).ok());
            ASSERT_TRUE(t.flush().ok());
            oracle[key(i)] = "v2_" + std::to_string(i);
            ++slot;
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok());            // snapshot 2: incremental
        EXPECT_LT(t.last_snapshot_pages_written(), 200U); // not a full rewrite
    }

    // Reopen from the incremental snapshot: unchanged keys are read from the
    // first snapshot's region (retained addrs); mutated keys from the second.
    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second) << "key " << kv.first;
    }
}
