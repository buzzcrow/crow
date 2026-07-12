// CT9: write path (apply + flush) integration tests.
#include "crowtree/crowtree.h"

#include <gtest/gtest.h>

#include <string>

using namespace crowtree;

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
        {{.key = "k", .kind = OpKind::kPut, .value = "first"}, {.key = "k", .kind = OpKind::kPut, .value = "second"},
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
