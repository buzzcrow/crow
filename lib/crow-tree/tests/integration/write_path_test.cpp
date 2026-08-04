// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// CT9: write path (apply + flush) integration tests.
#include "crow-tree/crow-tree.h"

#include <gtest/gtest.h>

#include <string>

using namespace crow::tree;

namespace
{

Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

std::string key(int i)
{
    std::array<char, 16> b{};
    snprintf(b.data(), b.size(), "k%05d", i);
    return b.data();
}

std::string get_or(Crowtree &t, const std::string &k, const std::string &dflt)
{
    std::string v;
    uint64_t    slot;
    return t.get(Slice(k), &slot, &v) ? v : dflt;
}

} // namespace

TEST(WritePath, BasePagesLiveInBufferPool)
{
    Options opt;
    opt.max_delta_len     = 1;    // consolidate into base frames quickly
    opt.leaf_split_bytes  = 200;  // small leaves -> multiple leaf + inner frames
    opt.frame_bytes       = 4096; // small frames so a few hold these tiny pages
    opt.buffer_pool_bytes = static_cast<size_t>(64) * 4096;
    Crowtree t(opt);
    for (int i = 0; i < 60; ++i) {
        uint64_t s = i + 1;
        ASSERT_TRUE(t.apply(s, put_one(key(i), "payload-" + std::to_string(i))).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    // The tree split into multiple leaves under one or more inner pages; every
    // such base page is built into a pool frame (held resident by its page).
    ASSERT_GT(t.leaf_count(), 1U);
    ASSERT_NE(t.buffer_pool(), nullptr);
    auto s = t.buffer_pool()->stats();
    EXPECT_GE(s.used, t.leaf_count()); // at least one frame per live leaf
    // Values remain correct when read straight out of the frames.
    for (int i = 0; i < 60; ++i) {
        EXPECT_EQ(get_or(t, key(i), "?"), "payload-" + std::to_string(i));
    }
}

// plan-tree #18 D5/D6: dirty ("anonymous", durable_addr == kNoAddr) frames
// are pinned-resident until a snapshot -- "a dirty frame is never evicted
// until written". D5 (model
// reconciliation) is a no-op: the live model (PageBase::durable_addr
// directly, no separate Pin/PinNew abstraction) already satisfies that
// invariant -- evict_clean_leaves_locked requires durable_addr != kNoAddr,
// so a dirty page is never a candidate.
//
// D6 asks for a "back-pressure test under a write storm (eager snapshot)".
// No eager-snapshot-on-memory-pressure trigger actually exists anywhere in
// the engine (verified by inspection: flush() only drains L0 to L1, never
// snapshot(); maybe_evict_locked only evicts *clean* frames) -- "a write
// storm that outruns snapshot triggers an eager snapshot" was never implemented, so there is
// nothing to test *as originally scoped*. What's real and testable without
// inventing a new feature: for a write storm against a *bounded* key set
// (the design's actual back-pressure concern -- unbounded dirty growth),
// dirty memory stays bounded on its own, because each repeat write to an
// existing key replaces that key's *same* dirty leaf via consolidation
// rather than adding a new one -- never needing a snapshot to bound it.
TEST(WritePath, WriteStormToBoundedKeySetKeepsDirtyMemoryBounded)
{
    Options opt;
    opt.max_delta_len    = 1;    // consolidate into base frames every write
    opt.leaf_split_bytes = 4000; // generous -- this test wants zero splits, ever
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    // Fixed-width value so the leaf's serialized byte size truly never
    // changes round to round (a growing-length value could tip the leaf
    // over leaf_split_bytes partway through and split -- a one-time
    // threshold crossing, not the unbounded-growth-with-write-count this
    // test is checking for).
    auto fixed_value = [](int round) {
        std::array<char, 8> b{};
        snprintf(b.data(), b.size(), "v%06d", round);
        return std::string(b.data());
    };

    constexpr int kKeys = 20;
    for (int i = 0; i < kKeys; ++i) {
        ASSERT_TRUE(t.apply(i + 1, put_one(key(i), fixed_value(0))).ok());
    }
    ASSERT_TRUE(t.flush().ok());
    uint32_t dirty_after_initial_fill = t.buffer_pool()->stats().dirty;
    ASSERT_GT(dirty_after_initial_fill, 0U); // never snapshotted -- still all dirty
    ASSERT_EQ(t.leaf_count(), 1U);           // sanity: this test wants exactly one leaf throughout

    // Write storm: many more rounds over the *same* kKeys keys, no
    // snapshot() call anywhere in the loop.
    uint64_t slot = kKeys;
    for (int round = 1; round <= 500; ++round) {
        for (int i = 0; i < kKeys; ++i) {
            ++slot;
            ASSERT_TRUE(t.apply(slot, put_one(key(i), fixed_value(round))).ok());
        }
        ASSERT_TRUE(t.flush().ok());
    }
    ASSERT_EQ(t.leaf_count(), 1U); // confirms no split snuck in during the storm

    // Dirty frame count must not have grown with the number of writes --
    // only with the number of *distinct* keys, which didn't change.
    EXPECT_EQ(t.buffer_pool()->stats().dirty, dirty_after_initial_fill);

    for (int i = 0; i < kKeys; ++i) {
        EXPECT_EQ(get_or(t, key(i), "?"), fixed_value(500));
    }
}

TEST(WritePath, ApplyThenFlushVisible)
{
    Crowtree t;
    ASSERT_TRUE(t.apply(1, put_one("a", "A1")).ok());
    ASSERT_TRUE(t.apply(2, put_one("b", "B2")).ok());
    // Before flush, values are visible from L0.
    EXPECT_EQ(get_or(t, "a", "?"), "A1");
    EXPECT_EQ(get_or(t, "b", "?"), "B2");
    ASSERT_TRUE(t.flush().ok());
    EXPECT_EQ(t.last_applied_slot(), 2U);
    EXPECT_EQ(t.memtable_count(), 0U); // fully drained
    // After flush, still visible from L1.
    EXPECT_EQ(get_or(t, "a", "?"), "A1");
    EXPECT_EQ(get_or(t, "b", "?"), "B2");
}

TEST(WritePath, IntraBatchLastWins)
{
    Crowtree t;
    Batch    b{
        {{.key = "k", .kind = OpKind::kPut, .value = "first"},
         {.key = "k", .kind = OpKind::kPut, .value = "second"},
         {.key = "k", .kind = OpKind::kPut, .value = "third"}}
    };
    ASSERT_TRUE(t.apply(1, b).ok());
    ASSERT_TRUE(t.flush().ok());
    EXPECT_EQ(get_or(t, "k", "?"), "third");
}

TEST(WritePath, OutOfOrderApplyConverges)
{
    Crowtree t;
    // Slot 3 arrives before slot 2 (parallel window). contiguous lags until 2.
    ASSERT_TRUE(t.apply(3, put_one("a", "A3")).ok());
    ASSERT_TRUE(t.apply(2, put_one("a", "A2")).ok());
    t.force_advance_slot(3);
    ASSERT_TRUE(t.flush().ok());
    EXPECT_EQ(get_or(t, "a", "?"), "A3"); // highest slot wins
    EXPECT_EQ(t.last_applied_slot(), 3U);
}

TEST(WritePath, FlushOnlyContiguousPrefix)
{
    Crowtree t;
    // a@2 contiguous; b@5 not yet contiguous (gap at 3,4).
    ASSERT_TRUE(t.apply(2, put_one("a", "A2")).ok());
    ASSERT_TRUE(t.apply(5, put_one("b", "B5")).ok());
    t.force_advance_slot(2);
    ASSERT_TRUE(t.flush().ok());
    EXPECT_EQ(t.last_applied_slot(), 2U);
    EXPECT_EQ(t.memtable_count(), 1U); // b@5 retained in L0
    // Both still readable (a from L1, b from L0).
    EXPECT_EQ(get_or(t, "a", "?"), "A2");
    EXPECT_EQ(get_or(t, "b", "?"), "B5");
}

TEST(WritePath, NoOpAdvancesFrontier)
{
    Crowtree t;
    ASSERT_TRUE(t.apply(1, put_one("a", "A1")).ok());
    // NoOp/empty batch advances contiguous to 5 (slots 2-5 were NoOps).
    ASSERT_TRUE(t.apply(5, Batch{}).ok());
    t.force_advance_slot(5);
    ASSERT_TRUE(t.flush().ok());
    EXPECT_EQ(t.last_applied_slot(), 5U);
    EXPECT_EQ(get_or(t, "a", "?"), "A1");
}

TEST(WritePath, ReApplyBelowDurableDropped)
{
    Crowtree t;
    ASSERT_TRUE(t.apply(3, put_one("a", "A3")).ok());
    t.force_advance_slot(3);
    ASSERT_TRUE(t.flush().ok());
    EXPECT_EQ(t.last_applied_slot(), 3U);
    // A stale re-apply of slot 2 must be dropped (already durable in L1).
    ASSERT_TRUE(t.apply(2, put_one("a", "A2")).ok());
    EXPECT_EQ(t.memtable_count(), 0U);
    EXPECT_EQ(get_or(t, "a", "?"), "A3");
}

TEST(WritePath, DeleteTombstone)
{
    Crowtree t;
    ASSERT_TRUE(t.apply(1, put_one("a", "A1")).ok());
    ASSERT_TRUE(t.flush().ok());
    Batch del{{{.key = "a", .kind = OpKind::kDelete, .value = ""}}};
    ASSERT_TRUE(t.apply(2, del).ok());
    ASSERT_TRUE(t.flush().ok());
    std::string v;
    uint64_t    slot;
    EXPECT_FALSE(t.get(Slice("a"), &slot, &v)); // tombstone -> not found
}

TEST(WritePath, ConsolidationOnLongChain)
{
    Options opt;
    opt.max_delta_len = 4; // force consolidation quickly
    Crowtree t(opt);
    for (uint64_t s = 1; s <= 20; ++s) {
        ASSERT_TRUE(t.apply(s, put_one("k", "v" + std::to_string(s))).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    EXPECT_EQ(get_or(t, "k", "?"), "v20");
    EXPECT_EQ(t.last_applied_slot(), 20U);
}
