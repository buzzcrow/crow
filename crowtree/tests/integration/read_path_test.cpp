// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// CT13: read path (get, multi_get, scan with L0 overlay, iter_all via snapshot).
#include "crowtree/crowtree.h"
#include "crowtree/page_store.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

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

std::string make_key(int i)
{
    std::array<char, 16> buf{};
    snprintf(buf.data(), buf.size(), "k%04d", i);
    return buf.data();
}
} // namespace

TEST(ReadPath, GetAfterPutAndDelete)
{
    Crowtree t;
    ASSERT_TRUE(t.apply(1, put_one("a", "A")).ok());
    ASSERT_TRUE(t.flush().ok());
    std::string v;
    uint64_t    s;
    ASSERT_TRUE(t.get(Slice("a"), &s, &v));
    EXPECT_EQ(v, "A");
    ASSERT_TRUE(t.apply(2, del_one("a")).ok());
    ASSERT_TRUE(t.flush().ok());
    EXPECT_FALSE(t.get(Slice("a"), &s, &v));
}

// plan-tree #5 B3 remaining: get_view() is the zero-copy primitive get()
// now wraps.
TEST(ReadPath, GetViewNotFound)
{
    Crowtree t;
    GetView  v = t.get_view(Slice("missing"));
    EXPECT_FALSE(v.found());
}

TEST(ReadPath, GetViewL0HitIsCorrect)
{
    Crowtree t;
    // No flush(): the value stays in L0 (the MemTable), never epoch-borrowed
    // (see GetView's doc) but still must resolve correctly.
    ASSERT_TRUE(t.apply(1, put_one("a", "A")).ok());
    GetView v = t.get_view(Slice("a"));
    ASSERT_TRUE(v.found());
    EXPECT_EQ(v.slot(), 1U);
    EXPECT_EQ(v.value().to_string(), "A");
}

TEST(ReadPath, GetViewL1HitBorrowsFrameSurvivingConcurrentEviction)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(t.apply(i + 1, put_one(make_key(i), "val" + std::to_string(i))).ok());
    }
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot(nullptr).ok()); // clean + evictable

    // Hold a GetView (and thus its epoch guard) open across an eviction pass
    // that would otherwise unload and retire the very frame this view
    // borrows its value from. The guard must keep that frame's memory alive
    // regardless -- this is the core safety property the zero-copy read
    // path depends on.
    GetView v = t.get_view(Slice(make_key(0)));
    ASSERT_TRUE(v.found());
    (void)t.evict_clean_leaves(1); // aggressive: unload almost everything
    EXPECT_EQ(v.value().to_string(), "val0") << "borrowed value must survive a concurrent eviction of its frame";
}

TEST(ReadPath, GetViewOverflowValueIsMaterialized)
{
    Options opt;
    opt.max_inline_value = 8; // force any value above 8 bytes to spill to overflow
    Crowtree    t(opt);
    std::string big(500, 'z');
    ASSERT_TRUE(t.apply(1, put_one("a", big)).ok());
    ASSERT_TRUE(t.flush().ok());
    GetView v = t.get_view(Slice("a"));
    ASSERT_TRUE(v.found());
    EXPECT_EQ(v.value().to_string(), big);
}

TEST(ReadPath, L0OverridesL1)
{
    Crowtree t;
    ASSERT_TRUE(t.apply(1, put_one("a", "A1")).ok());
    ASSERT_TRUE(t.flush().ok());                      // A1 in L1
    ASSERT_TRUE(t.apply(2, put_one("a", "A2")).ok()); // A2 in L0 (not flushed)
    std::string v;
    uint64_t    s;
    ASSERT_TRUE(t.get(Slice("a"), &s, &v));
    EXPECT_EQ(v, "A2");
    EXPECT_EQ(s, 2U);
    // scan reflects L0 too.
    std::vector<scan_entry> out;
    bool                    trunc;
    ASSERT_TRUE(t.scan(Slice(""), 0, &out, &trunc).ok());
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0].value, "A2");
}

TEST(ReadPath, L0TombstoneHidesL1)
{
    Crowtree t;
    ASSERT_TRUE(t.apply(1, put_one("a", "A1")).ok());
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.apply(2, del_one("a")).ok()); // tombstone in L0
    std::string v;
    uint64_t    s;
    EXPECT_FALSE(t.get(Slice("a"), &s, &v));
    std::vector<scan_entry> out;
    bool                    trunc;
    ASSERT_TRUE(t.scan(Slice(""), 0, &out, &trunc).ok());
    EXPECT_TRUE(out.empty()); // tombstone excluded
}

TEST(ReadPath, ScanOrderLimitTruncatedAcrossLeaves)
{
    Options opt;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 120; // force multiple leaves
    Crowtree  t(opt);
    const int N = 100;
    for (int i = 0; i < N; ++i) {
        uint64_t s = i + 1;
        ASSERT_TRUE(t.apply(s, put_one(make_key(i), "val" + std::to_string(i))).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    ASSERT_GT(t.leaf_count(), 1U);

    // Full scan: sorted, complete.
    std::vector<scan_entry> out;
    bool                    trunc;
    ASSERT_TRUE(t.scan(Slice(""), 0, &out, &trunc).ok());
    ASSERT_EQ(out.size(), static_cast<size_t>(N));
    EXPECT_FALSE(trunc);
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(out[i].key, make_key(i));
    }

    // Limited scan: truncated.
    ASSERT_TRUE(t.scan(Slice(""), 10, &out, &trunc).ok());
    EXPECT_EQ(out.size(), 10U);
    EXPECT_TRUE(trunc);
    EXPECT_EQ(out[0].key, make_key(0));
    EXPECT_EQ(out[9].key, make_key(9));
}

TEST(ReadPath, ScanPrefix)
{
    Crowtree t;
    ASSERT_TRUE(t.apply(1, put_one("apple", "1")).ok());
    ASSERT_TRUE(t.apply(2, put_one("apricot", "2")).ok());
    ASSERT_TRUE(t.apply(3, put_one("banana", "3")).ok());
    ASSERT_TRUE(t.flush().ok());
    std::vector<scan_entry> out;
    bool                    trunc;
    ASSERT_TRUE(t.scan(Slice("ap"), 0, &out, &trunc).ok());
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(out[0].key, "apple");
    EXPECT_EQ(out[1].key, "apricot");
}

TEST(ReadPath, multi_get)
{
    Crowtree t;
    ASSERT_TRUE(t.apply(1, put_one("a", "A")).ok());
    ASSERT_TRUE(t.apply(2, put_one("c", "C")).ok());
    ASSERT_TRUE(t.flush().ok());
    std::vector<Slice> keys = {Slice("a"), Slice("b"), Slice("c")};
    auto               res  = t.multi_get(keys);
    ASSERT_EQ(res.size(), 3U);
    EXPECT_TRUE(res[0].found);
    EXPECT_EQ(res[0].value, "A");
    EXPECT_FALSE(res[1].found);
    EXPECT_TRUE(res[2].found);
    EXPECT_EQ(res[2].value, "C");
}

TEST(ReadPath, IterAllIncludesTombstones)
{
    Crowtree t;
    ASSERT_TRUE(t.apply(1, put_one("a", "A")).ok());
    ASSERT_TRUE(t.apply(1, put_one("b", "B")).ok());
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.apply(2, del_one("a")).ok());
    ASSERT_TRUE(t.flush().ok());
    auto snap = t.snapshot_view();
    // iter_all (entries) includes the tombstone for "a".
    EXPECT_EQ(snap->size(), 2U);
    // scan (live) excludes it.
    std::vector<scan_entry> out;
    bool                    trunc;
    ASSERT_TRUE(t.scan(Slice(""), 0, &out, &trunc).ok());
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0].key, "b");
}
