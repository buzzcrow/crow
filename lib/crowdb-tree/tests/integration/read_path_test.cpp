// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// CT13: read path (get, multi_get, scan with L0 overlay, iter_all via snapshot).
#include "crowdb-tree/crowdb-tree.h"
#include "crowdb-tree/page_store.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace crowdb::tree;

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
    Crowdbtree t;
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
    Crowdbtree t;
    GetView    v = t.get_view(Slice("missing"));
    EXPECT_FALSE(v.found());
}

TEST(ReadPath, GetViewL0HitIsCorrect)
{
    Crowdbtree t;
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
    Crowdbtree t(opt);

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
    Crowdbtree  t(opt);
    std::string big(500, 'z');
    ASSERT_TRUE(t.apply(1, put_one("a", big)).ok());
    ASSERT_TRUE(t.flush().ok());
    GetView v = t.get_view(Slice("a"));
    ASSERT_TRUE(v.found());
    EXPECT_EQ(v.value().to_string(), big);
}

TEST(ReadPath, L0OverridesL1)
{
    Crowdbtree t;
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
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 0, false, 0, &out, &trunc).ok());
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0].value, "A2");
}

TEST(ReadPath, L0TombstoneHidesL1)
{
    Crowdbtree t;
    ASSERT_TRUE(t.apply(1, put_one("a", "A1")).ok());
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.apply(2, del_one("a")).ok()); // tombstone in L0
    std::string v;
    uint64_t    s;
    EXPECT_FALSE(t.get(Slice("a"), &s, &v));
    std::vector<scan_entry> out;
    bool                    trunc;
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 0, false, 0, &out, &trunc).ok());
    EXPECT_TRUE(out.empty()); // tombstone excluded
}

TEST(ReadPath, ScanOrderLimitTruncatedAcrossLeaves)
{
    Options opt;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 120; // force multiple leaves
    Crowdbtree t(opt);
    const int  N = 100;
    for (int i = 0; i < N; ++i) {
        uint64_t s = i + 1;
        ASSERT_TRUE(t.apply(s, put_one(make_key(i), "val" + std::to_string(i))).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    ASSERT_GT(t.leaf_count(), 1U);

    // Full scan: sorted, complete.
    std::vector<scan_entry> out;
    bool                    trunc;
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 0, false, 0, &out, &trunc).ok());
    ASSERT_EQ(out.size(), static_cast<size_t>(N));
    EXPECT_FALSE(trunc);
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(out[i].key, make_key(i));
    }

    // Limited scan: truncated.
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 10, 0, false, 0, &out, &trunc).ok());
    EXPECT_EQ(out.size(), 10U);
    EXPECT_TRUE(trunc);
    EXPECT_EQ(out[0].key, make_key(0));
    EXPECT_EQ(out[9].key, make_key(9));
}

TEST(ReadPath, ScanPrefix)
{
    Crowdbtree t;
    ASSERT_TRUE(t.apply(1, put_one("apple", "1")).ok());
    ASSERT_TRUE(t.apply(2, put_one("apricot", "2")).ok());
    ASSERT_TRUE(t.apply(3, put_one("banana", "3")).ok());
    ASSERT_TRUE(t.flush().ok());
    std::vector<scan_entry> out;
    bool                    trunc;
    ASSERT_TRUE(t.scan(Slice("ap"), Slice(), Slice(), 0, 0, false, 0, &out, &trunc).ok());
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(out[0].key, "apple");
    EXPECT_EQ(out[1].key, "apricot");
}

// start_after cursor: a sync scan with a non-empty start_after returns
// only keys strictly greater than the cursor, in order, up to the
// limit. The descent lands on the leaf containing the cursor instead
// of the first prefix leaf, so earlier entries are never visited (the
// deep-pagination pushdown §1.7 claims). Mirrors the async twin
// AsyncScan.StartAfterCursorSkipsEarlierEntries.
TEST(ReadPath, ScanStartAfterCursorSkipsEarlierEntries)
{
    Options opt;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 120; // force multiple leaves
    Crowdbtree t(opt);
    const int  N = 30;
    for (int i = 0; i < N; ++i) {
        ASSERT_TRUE(t.apply(i + 1, put_one(make_key(i), "v" + std::to_string(i))).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    ASSERT_GT(t.leaf_count(), 1U);

    // Cursor at key0009: only key0010..key0029 should come back (20 keys).
    std::string             cursor = make_key(9);
    std::vector<scan_entry> out;
    bool                    trunc;
    ASSERT_TRUE(t.scan(Slice(""), Slice(cursor), Slice(), 0, 0, false, 0, &out, &trunc).ok());
    EXPECT_FALSE(trunc);
    ASSERT_EQ(out.size(), 20U);
    for (const auto &e : out) {
        EXPECT_GT(e.key, cursor);
    }
    EXPECT_EQ(out[0].key, make_key(10));
    EXPECT_EQ(out[19].key, make_key(29));

    // Cursor + limit: a page of 5 starting after key0009.
    ASSERT_TRUE(t.scan(Slice(""), Slice(cursor), Slice(), 5, 0, false, 0, &out, &trunc).ok());
    ASSERT_EQ(out.size(), 5U);
    EXPECT_TRUE(trunc) << "20 keys after cursor, limit=5 should truncate";
    EXPECT_EQ(out[0].key, make_key(10));
    EXPECT_EQ(out[4].key, make_key(14));

    // Cursor near the end: deep pagination returns only the tail.
    std::string tail_cursor = make_key(N - 11); // key0019 -> key0020..key0029 (10 keys)
    ASSERT_TRUE(t.scan(Slice(""), Slice(tail_cursor), Slice(), 0, 0, false, 0, &out, &trunc).ok());
    EXPECT_FALSE(trunc);
    ASSERT_EQ(out.size(), 10U);
    EXPECT_EQ(out[0].key, make_key(20));
    EXPECT_EQ(out[9].key, make_key(29));
}

// byte_budget: the scan stops when accumulated key+value bytes exceed the
// budget, setting truncated. Always returns at least one entry even if it
// alone exceeds the budget (so the client makes progress). A single entry
// larger than the budget is returned with truncated set if more remain.
TEST(ReadPath, ScanByteBudgetStopsAndTruncates)
{
    Crowdbtree t;
    // 5 entries: key=3B ("k00".."k04"), value=10B ("vvvvvvvvv0".."vvvvvvvvv4")
    // = 13B per entry. Budget=30B allows 2 entries (26B) before the 3rd
    // (39B) exceeds the budget.
    for (int i = 0; i < 5; ++i) {
        std::string key = "k0" + std::to_string(i);
        std::string val = "vvvvvvvvv" + std::to_string(i);
        ASSERT_TRUE(t.apply(i + 1, put_one(key, val)).ok());
    }
    ASSERT_TRUE(t.flush().ok());

    std::vector<scan_entry> out;
    bool                    trunc;
    // Budget=30: 2 entries (26B) fit, 3rd would be 39B > 30.
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 30, false, 0, &out, &trunc).ok());
    ASSERT_EQ(out.size(), 2U);
    EXPECT_TRUE(trunc);
    EXPECT_EQ(out[0].key, "k00");
    EXPECT_EQ(out[1].key, "k01");

    // Budget=0 (unlimited): all 5 entries, no truncation.
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 0, false, 0, &out, &trunc).ok());
    ASSERT_EQ(out.size(), 5U);
    EXPECT_FALSE(trunc);

    // Budget smaller than a single entry: always return >= 1 entry.
    // Entry "k00" = 3 + 10 = 13B; budget=5 < 13.
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 5, false, 0, &out, &trunc).ok());
    ASSERT_EQ(out.size(), 1U);
    EXPECT_TRUE(trunc); // more entries remain
    EXPECT_EQ(out[0].key, "k00");
}

// keys_only projection: the engine skips value materialization (no
// overflow-chain assembly) and stages empty values. Keys match a full scan;
// values are all empty. The byte budget accounts for key bytes only, so a
// keys_only page fits more entries than a full scan with the same budget.
TEST(ReadPath, ScanKeysOnlySkipsValues)
{
    Crowdbtree t;
    for (int i = 0; i < 5; ++i) {
        std::string key = "k0" + std::to_string(i);
        std::string val = "vvvvvvvvv" + std::to_string(i);
        ASSERT_TRUE(t.apply(i + 1, put_one(key, val)).ok());
    }
    ASSERT_TRUE(t.flush().ok());

    // keys_only: same keys as full scan, all values empty.
    std::vector<scan_entry> keys_out;
    bool                    keys_trunc;
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 0, true, 0, &keys_out, &keys_trunc).ok());
    std::vector<scan_entry> full_out;
    bool                    full_trunc;
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 0, false, 0, &full_out, &full_trunc).ok());
    ASSERT_EQ(keys_out.size(), 5U);
    EXPECT_EQ(keys_trunc, full_trunc);
    for (size_t i = 0; i < keys_out.size(); ++i) {
        EXPECT_EQ(keys_out[i].key, full_out[i].key);
        EXPECT_TRUE(keys_out[i].value.empty()) << "keys_only value must be empty";
        EXPECT_FALSE(full_out[i].value.empty()) << "full scan value must be non-empty";
    }

    // keys_only with byte_budget: key bytes only (3B per key), so budget=7
    // fits 2 keys (6B) before the 3rd (9B) exceeds — vs a full scan where
    // 13B per entry would fit only 1 entry in 7B.
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 7, true, 0, &keys_out, &keys_trunc).ok());
    ASSERT_EQ(keys_out.size(), 2U);
    EXPECT_TRUE(keys_trunc);
    EXPECT_EQ(keys_out[0].key, "k00");
    EXPECT_EQ(keys_out[1].key, "k01");
    EXPECT_TRUE(keys_out[0].value.empty());
    EXPECT_TRUE(keys_out[1].value.empty());
}

TEST(ReadPath, ScanDeadlineReturnsPartialResult)
{
    Crowdbtree t;
    // Populate enough keys that the merge loop's periodic deadline check
    // (every 1024 entries) fires mid-scan.
    for (int i = 0; i < 3000; ++i) {
        char key[16];
        std::snprintf(key, sizeof(key), "k%05d", i);
        ASSERT_TRUE(t.apply(static_cast<uint64_t>(i) + 1, put_one(key, "v")).ok());
    }

    // deadline_ms = 0 (no deadline): full scan returns all 3000 entries.
    std::vector<scan_entry> all;
    bool                    all_trunc = false;
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 0, false, 0, &all, &all_trunc).ok());
    ASSERT_EQ(all.size(), 3000U);
    EXPECT_FALSE(all_trunc);

    // Tight deadline (already expired): the merge loop checks periodically
    // (every 1024 entries) and breaks early with truncated = true. The
    // result is a partial, correctly-ordered prefix (no gaps).
    std::vector<scan_entry> partial;
    bool                    partial_trunc = false;
    auto                    deadline_ms   = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 0, false, deadline_ms, &partial, &partial_trunc).ok());
    EXPECT_TRUE(partial_trunc);
    EXPECT_LT(partial.size(), all.size());
    // The partial result is a correctly-ordered prefix of the full scan.
    for (size_t i = 0; i < partial.size(); ++i) {
        ASSERT_EQ(partial[i].key, all[i].key) << "partial result must be a prefix";
    }
}

TEST(ReadPath, multi_get)
{
    Crowdbtree t;
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
    Crowdbtree t;
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
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 0, false, 0, &out, &trunc).ok());
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0].key, "b");
}
