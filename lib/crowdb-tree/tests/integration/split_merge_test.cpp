// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// CT12: page split & merge integration tests.
#include "crowdb-tree/crowdb-tree.h"
#include "crowdb-tree/page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <map>
#include <random>
#include <string>

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
    snprintf(buf.data(), buf.size(), "key%05d", i);
    return buf.data();
}
} // namespace

TEST(SplitMerge, SplitGrowsMultiLevelTree)
{
    Options opt;
    opt.max_delta_len    = 1;   // consolidate aggressively
    opt.leaf_split_bytes = 200; // small leaves -> force splits
    Crowdbtree t(opt);

    const int N = 300;
    for (int i = 0; i < N; ++i) {
        uint64_t s = i + 1;
        ASSERT_TRUE(t.apply(s, put_one(make_key(i), "value-payload-" + std::to_string(i))).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    // The tree must have grown beyond a single leaf.
    EXPECT_GT(t.height(), 1);
    EXPECT_GT(t.leaf_count(), 1U);

    // All keys present and readable.
    for (int i = 0; i < N; ++i) {
        std::string v;
        uint64_t    slot;
        ASSERT_TRUE(t.get(Slice(make_key(i)), &slot, &v)) << "missing " << make_key(i);
        EXPECT_EQ(v, "value-payload-" + std::to_string(i));
    }

    // Snapshot is globally key-sorted and complete.
    auto snap = t.snapshot_view();
    ASSERT_EQ(snap->size(), static_cast<size_t>(N));
    for (size_t i = 1; i < snap->size(); ++i) {
        EXPECT_LT(snap->entries()[i - 1].key, snap->entries()[i].key);
    }
}

TEST(SplitMerge, MergeAndRootCollapse)
{
    Options opt;
    opt.max_delta_len    = 0; // consolidate (and check merge) on every flush
    opt.leaf_split_bytes = 200;
    opt.leaf_merge_bytes = 60;
    Crowdbtree t(opt);

    const int N    = 200;
    uint64_t  slot = 0;
    for (int i = 0; i < N; ++i) {
        ++slot;
        ASSERT_TRUE(t.apply(slot, put_one(make_key(i), "payload" + std::to_string(i))).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    ASSERT_GT(t.height(), 1);
    size_t leaves_before = t.leaf_count();
    EXPECT_GT(leaves_before, 1U);

    // Allow tombstone GC so deletes actually shrink leaves.
    t.set_gc_watermark(1000000, 1000000);
    // Delete all but the first two keys.
    for (int i = 2; i < N; ++i) {
        ++slot;
        ASSERT_TRUE(t.apply(slot, del_one(make_key(i))).ok());
        ASSERT_TRUE(t.flush().ok());
    }

    // Tree shrank: fewer leaves, ideally collapsed back to a single-leaf root.
    EXPECT_LT(t.leaf_count(), leaves_before);
    EXPECT_EQ(t.height(), 1);

    // Surviving keys readable; deleted keys gone.
    std::string v;
    uint64_t    s;
    EXPECT_TRUE(t.get(Slice(make_key(0)), &s, &v));
    EXPECT_TRUE(t.get(Slice(make_key(1)), &s, &v));
    for (int i = 2; i < N; ++i) {
        EXPECT_FALSE(t.get(Slice(make_key(i)), &s, &v)) << "should be deleted: " << make_key(i);
    }
    auto snap = t.snapshot_view();
    EXPECT_EQ(snap->size(), 2U);
}

// Regression (plan-tree #14c/#14d): a merged-away leaf/inner's own PID is
// orphaned (its mapping slot never gets a replacement store()) -- see
// Crowdbtree::retire_orphaned_page's doc comment. snapshot() discovers dirty
// pages/segments by scanning every mapping-table slot directly (no
// reachable-page tree walk), so a stale slot left pointing at a since-freed
// page is a use-after-free the moment a merge-heavy tree gets snapshotted.
TEST(SplitMerge, SnapshotSucceedsAfterHeavyMergeAndRootCollapse)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 0; // consolidate (and check merge) on every flush
    opt.leaf_split_bytes = 200;
    opt.leaf_merge_bytes = 60;
    opt.inner_max_keys   = 4; // force inner splits/merges too, not just leaves
    Crowdbtree t(opt);

    const int N    = 200;
    uint64_t  slot = 0;
    for (int i = 0; i < N; ++i) {
        ++slot;
        ASSERT_TRUE(t.apply(slot, put_one(make_key(i), "payload" + std::to_string(i))).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    ASSERT_GT(t.height(), 1);

    t.set_gc_watermark(1000000, 1000000);
    for (int i = 2; i < N; ++i) {
        ++slot;
        ASSERT_TRUE(t.apply(slot, del_one(make_key(i))).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    EXPECT_GT(t.leaf_count(), 0U); // sanity: still a valid tree after the merge storm

    ASSERT_TRUE(t.snapshot().ok());

    std::unique_ptr<Crowdbtree> t2;
    ASSERT_TRUE(Crowdbtree::open(opt, &t2).ok());
    std::string v;
    uint64_t    s;
    EXPECT_TRUE(t2->get(Slice(make_key(0)), &s, &v));
    EXPECT_TRUE(t2->get(Slice(make_key(1)), &s, &v));
    for (int i = 2; i < N; ++i) {
        EXPECT_FALSE(t2->get(Slice(make_key(i)), &s, &v)) << "should be deleted: " << make_key(i);
    }
}

TEST(SplitMerge, LargeFlushSpanningLeavesSplitsMidFlush)
{
    // Regression: one flush() drains keys spanning many existing leaves and
    // triggers splits mid-flush. Each per-leaf group must be routed against the
    // CURRENT tree (after prior groups' SMOs), not a routing snapshot captured
    // before the flush began. Otherwise later keys land in a just-split leaf.
    Options opt;
    opt.max_delta_len    = 0;   // consolidate on every flush
    opt.leaf_split_bytes = 200; // small leaves -> splits during the big flush
    opt.leaf_merge_bytes = 40;
    // Keep auto-flush from firing so we control exactly when the big flush runs.
    opt.memtable_flush_bytes   = 1ULL << 40;
    opt.memtable_flush_entries = 1U << 30;
    Crowdbtree t(opt);

    std::map<std::string, std::string> oracle;
    uint64_t                           slot = 0;

    // Phase A: build a multi-level tree with incremental flushes.
    const int N = 400;
    for (int i = 0; i < N; i += 2) { // even keys first
        ++slot;
        std::string val = "a" + std::to_string(slot);
        ASSERT_TRUE(t.apply(slot, put_one(make_key(i), val)).ok());
        oracle[make_key(i)] = val;
        ASSERT_TRUE(t.flush().ok());
    }
    ASSERT_GT(t.height(), 1);
    ASSERT_GT(t.leaf_count(), 2U);

    // Phase B: stage many keys interleaved across the whole keyspace WITHOUT
    // flushing, so a single flush() drains a set that spans every existing leaf
    // and grows several of them past the split threshold in one pass.
    for (int i = 1; i < N; i += 2) { // odd keys interleave between existing keys
        ++slot;
        std::string val = "b" + std::to_string(slot);
        ASSERT_TRUE(t.apply(slot, put_one(make_key(i), val)).ok());
        oracle[make_key(i)] = val;
    }
    // Also overwrite a spread of even keys so groups are non-trivial.
    for (int i = 0; i < N; i += 8) {
        ++slot;
        std::string val = "c" + std::to_string(slot);
        ASSERT_TRUE(t.apply(slot, put_one(make_key(i), val)).ok());
        oracle[make_key(i)] = val;
    }
    // The single flush that exercises mid-flush re-routing.
    ASSERT_TRUE(t.flush().ok());

    // Every key must be present and correct.
    for (int i = 0; i < N; ++i) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t.get(Slice(make_key(i)), &s, &v)) << "missing " << make_key(i);
        EXPECT_EQ(v, oracle[make_key(i)]) << "wrong value " << make_key(i);
    }

    // Snapshot is globally key-sorted and complete (no entry lost to a stale
    // route into a split leaf).
    auto snap = t.snapshot_view();
    ASSERT_EQ(snap->size(), oracle.size());
    for (size_t i = 1; i < snap->size(); ++i) {
        EXPECT_LT(snap->entries()[i - 1].key, snap->entries()[i].key);
    }
}

TEST(SplitMerge, ParityWithOracleUnderSplits)
{
    Options opt;
    opt.max_delta_len    = 2;
    opt.leaf_split_bytes = 150;
    opt.leaf_merge_bytes = 40;
    Crowdbtree t(opt);

    std::map<std::string, std::string> oracle;
    std::mt19937                       rng(12345);
    uint64_t                           slot = 0;
    for (int round = 0; round < 2000; ++round) {
        int         k   = static_cast<int>(rng() % 150);
        std::string key = make_key(k);
        ++slot;
        if (rng() % 4 == 0) {
            ASSERT_TRUE(t.apply(slot, del_one(key)).ok());
            oracle.erase(key);
        }
        else {
            std::string val = "v" + std::to_string(slot);
            ASSERT_TRUE(t.apply(slot, put_one(key, val)).ok());
            oracle[key] = val;
        }
        if (round % 7 == 0) {
            ASSERT_TRUE(t.flush().ok());
        }
    }
    ASSERT_TRUE(t.flush().ok());

    // compare every key.
    for (int k = 0; k < 150; ++k) {
        std::string key = make_key(k);
        std::string v;
        uint64_t    s;
        bool        found = t.get(Slice(key), &s, &v);
        auto        it    = oracle.find(key);
        if (it == oracle.end()) {
            EXPECT_FALSE(found) << "extra key " << key;
        }
        else {
            ASSERT_TRUE(found) << "missing key " << key;
            EXPECT_EQ(v, it->second) << "value mismatch " << key;
        }
    }
}

// Regression: consolidation folds an entire delta chain into one leaf, which
// can be many times larger than leaf_split_bytes (up to max_delta_bytes).
// maybe_split_or_merge_locked must split iteratively until every child fits
// under the threshold — a single halving leaves oversized leaves when the
// consolidated leaf is > 2x the split threshold.
TEST(SplitMerge, ConsolidationSplitsIterativelyToThreshold)
{
    Options opt;
    opt.max_delta_len    = 0;   // consolidate on every flush
    opt.leaf_split_bytes = 200; // small threshold so many splits are needed
    opt.leaf_merge_bytes = 50;  // well below split to avoid merge-after-split
    // Prevent auto-freeze so we control exactly when each flush runs.
    opt.memtable_flush_bytes   = 1ULL << 40;
    opt.memtable_flush_entries = 1U << 30;
    Crowdbtree t(opt);

    // Phase 1: write the highest key first and flush. With max_delta_len=0,
    // the delta immediately consolidates into the base leaf, giving it
    // high_key="key00999" so that subsequent (smaller) keys are grouped by
    // sort-aware descent instead of published one-per-key (which happens
    // when the base leaf is empty and high_key is an empty Slice).
    ASSERT_TRUE(t.apply(1, put_one("key00999", "first")).ok());
    ASSERT_TRUE(t.flush().ok());

    // Phase 2: write 200 keys in one batch, all < "key00999". They're
    // grouped into one delta and immediately consolidated (max_delta_len=0).
    // The consolidated leaf has ~201 entries (~7000 bytes = ~35x the
    // 200-byte split threshold).
    Batch big;
    for (int i = 0; i < 200; ++i) {
        big.ops.push_back({.key = make_key(i), .kind = OpKind::kPut, .value = "val-" + std::to_string(i)});
    }
    ASSERT_TRUE(t.apply(2, std::move(big)).ok());
    ASSERT_TRUE(t.flush().ok());

    // With iterative splitting, ~7000 bytes / 200-byte threshold => ~35
    // leaves. With the bug (single split), only 2 leaves of ~3500 bytes
    // each remain.
    EXPECT_GE(t.leaf_count(), 10U);

    // All keys must be present and readable.
    for (int i = 0; i < 200; ++i) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t.get(Slice(make_key(i)), &s, &v)) << "missing " << make_key(i);
        EXPECT_EQ(v, "val-" + std::to_string(i));
    }
    std::string v;
    uint64_t    s;
    ASSERT_TRUE(t.get(Slice("key00999"), &s, &v));
    EXPECT_EQ(v, "first");
}

// Count the inner pages reachable from the root (test helper).
static size_t inner_count_walk(Crowdbtree &t)
{
    std::function<size_t(uint64_t)> rec = [&](uint64_t page_id) -> size_t {
        PageBase *head = t.mapping().get_resident(page_id);
        if (head == nullptr) {
            return 0;
        }
        PageBase *base = head;
        while (base != nullptr && base->type == page_type::kBatchDelta) {
            base = base->next;
        }
        if (base == nullptr || base->type == page_type::kLeafBase) {
            return 0;
        }
        size_t n = 1;
        for (uint64_t c : static_cast<InnerBase *>(base)->children()) {
            n += rec(c);
        }
        return n;
    };
    return rec(t.root_page_id());
}

// O(1) atomic leaf/inner counters must match the tree walk after splits.
TEST(SplitMerge, LeafInnerCountParityAfterSplits)
{
    Options opt;
    opt.max_delta_len      = 1;   // consolidate aggressively
    opt.leaf_split_bytes   = 200; // small leaves -> force splits
    opt.max_memtable_count = 6;
    Crowdbtree t(opt);

    // Fresh tree: 1 leaf, 0 inner.
    EXPECT_EQ(t.leaf_count_atomic(), 1U);
    EXPECT_EQ(t.inner_count_atomic(), 0U);

    const int N = 300;
    for (int i = 0; i < N; ++i) {
        ASSERT_TRUE(t.apply(static_cast<uint64_t>(i + 1), put_one(make_key(i), "val-" + std::to_string(i))).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    EXPECT_GT(t.height(), 1);
    EXPECT_EQ(t.leaf_count_atomic(), t.leaf_count());
    EXPECT_EQ(t.inner_count_atomic(), inner_count_walk(t));
}

// O(1) atomic leaf/inner counters must match the tree walk after merges
// and root collapse.
TEST(SplitMerge, LeafInnerCountParityAfterMerges)
{
    Options opt;
    opt.max_delta_len      = 1;
    opt.leaf_split_bytes   = 200;
    opt.leaf_merge_bytes   = 40;
    opt.max_memtable_count = 6;
    Crowdbtree t(opt);

    // Build a multi-level tree.
    std::map<std::string, std::string> oracle;
    uint64_t                           slot = 0;
    for (int i = 0; i < 200; ++i) {
        ++slot;
        ASSERT_TRUE(t.apply(slot, put_one(make_key(i), "v" + std::to_string(slot))).ok());
        oracle[make_key(i)] = "v" + std::to_string(slot);
        ASSERT_TRUE(t.flush().ok());
    }
    ASSERT_GT(t.height(), 1);

    // Delete half to trigger merges + root collapse.
    int deleted = 0;
    for (auto it = oracle.begin(); it != oracle.end() && deleted < 150; ++it, ++deleted) {
        ++slot;
        Batch del{{{.key = it->first, .kind = OpKind::kDelete, .value = ""}}};
        ASSERT_TRUE(t.apply(slot, del).ok());
        ASSERT_TRUE(t.flush().ok());
    }

    // Counters must match the walk regardless of how many merges/collapses
    // happened.
    EXPECT_EQ(t.leaf_count_atomic(), t.leaf_count());
    EXPECT_EQ(t.inner_count_atomic(), inner_count_walk(t));
}
