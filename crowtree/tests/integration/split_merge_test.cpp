// CT12: page split & merge integration tests.
#include "crowtree/crowtree.h"
#include "crowtree/page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <map>
#include <random>
#include <string>

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
    snprintf(buf.data(), buf.size(), "key%05d", i);
    return buf.data();
}
} // namespace

TEST(SplitMerge, SplitGrowsMultiLevelTree)
{
    Options opt;
    opt.max_delta_len    = 1;   // consolidate aggressively
    opt.leaf_split_bytes = 200; // small leaves -> force splits
    Crowtree t(opt);

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
    Crowtree t(opt);

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
// Crowtree::retire_orphaned_page's doc comment. snapshot() discovers dirty
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
    Crowtree t(opt);

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

    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
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
    Crowtree t(opt);

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
    Crowtree t(opt);

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
