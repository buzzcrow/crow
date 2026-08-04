// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// PT6c-5.4: writer-driven eviction of clean resident bases. An
// evicted leaf re-tags its mapping slot `unloaded` and epoch-retires the page;
// the next access demand-loads it. Run under TSan for the eviction-vs-reader
// race (epoch-deferred frame reuse).
#include "crow-tree/crow-tree.h"
#include "crow-tree/page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace crow::tree;

namespace
{
Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

std::string make_key(int i)
{
    std::array<char, 16> buf{};
    snprintf(buf.data(), buf.size(), "key%05d", i);
    return buf.data();
}

void fill_buf(Crowtree *t, int K, std::map<std::string, std::string> *oracle)
{
    for (int i = 0; i < K; ++i) {
        std::string v = "val" + std::to_string(i);
        ASSERT_TRUE(t->apply(i + 1, put_one(make_key(i), v)).ok());
        ASSERT_TRUE(t->flush().ok());
        (*oracle)[make_key(i)] = v;
    }
}

// Counts read_at calls so a test can observe whether a specific page was
// demand-loaded (evicted, then reloaded) vs. still resident (plan-tree #17
// recency-ranked eviction).
class CountingPageStore : public MemPageStore
{
  public:
    explicit CountingPageStore(uint32_t iu_size = 1) : MemPageStore(iu_size)
    {
    }

    Status read_at(uint64_t off, uint8_t *buf, size_t len) const override
    {
        ++reads;
        return MemPageStore::read_at(off, buf, len);
    }

    mutable std::atomic<int> reads{0};
};
} // namespace

TEST(Eviction, EvictedLeavesFreeMemoryAndReloadCorrectly)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    std::map<std::string, std::string> oracle;
    fill_buf(&t, 200, &oracle);
    ASSERT_TRUE(t.snapshot(nullptr).ok()); // all reachable pages now clean

    uint32_t before  = t.buffer_pool()->stats().used;
    size_t   evicted = t.evict_clean_leaves(2); // keep at most 2 resident leaves
    EXPECT_GT(evicted, 0U);

    // No reader guards are open, so the epoch manager reclaims the retired pages
    // synchronously and their frames return to the pool: residency drops.
    uint32_t after = t.buffer_pool()->stats().used;
    EXPECT_LT(after, before);

    // Every value is still readable — evicted leaves demand-load on access.
    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t.get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
}

TEST(Eviction, EvictIsIdempotentAndSkipsDirty)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    std::map<std::string, std::string> oracle;
    fill_buf(&t, 120, &oracle);
    // No snapshot yet: every built leaf is dirty (no durable addr) -> nothing
    // is evictable.
    EXPECT_EQ(t.evict_clean_leaves(0), 0U);

    ASSERT_TRUE(t.snapshot(nullptr).ok()); // pages become clean
    size_t first = t.evict_clean_leaves(1);
    EXPECT_GT(first, 0U);
    // A second pass with everything already unloaded evicts nothing more.
    EXPECT_EQ(t.evict_clean_leaves(1), 0U);

    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t.get(Slice(kv.first), &s, &v));
        EXPECT_EQ(v, kv.second);
    }
}

TEST(Eviction, ConcurrentReadersWhileEvicting)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    const int                          K = 250;
    std::map<std::string, std::string> oracle;
    fill_buf(&t, K, &oracle);
    ASSERT_TRUE(t.snapshot(nullptr).ok());

    std::atomic<bool>        stop{false};
    std::atomic<bool>        fail{false};
    std::vector<std::thread> readers;
    readers.reserve(6);
    for (int r = 0; r < 6; ++r) {
        readers.emplace_back([&, r] {
            std::mt19937 rng(9000 + r);
            std::string  v;
            uint64_t     s;
            while (!stop.load(std::memory_order_relaxed)) {
                int i = static_cast<int>(rng() % K);
                if (!t.get(Slice(make_key(i)), &s, &v) || v != "val" + std::to_string(i)) {
                    fail.store(true);
                    return;
                }
            }
        });
    }

    // Churn: repeatedly evict almost everything while readers demand-load it back.
    for (int it = 0; it < 400; ++it) {
        (void)t.evict_clean_leaves(2);
        std::this_thread::yield();
    }
    stop.store(true);
    for (auto &th : readers) {
        th.join();
    }
    EXPECT_FALSE(fail.load());
}

// plan-tree #17: evict_clean_leaves ranks its candidates by real access
// recency (PageBase::last_touch_tick, stamped on every resident() touch)
// instead of arbitrary DFS order.
TEST(Eviction, RecentlyTouchedLeafSurvivesEvictionOverColderOnes)
{
    CountingPageStore store(1);
    Options           opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    std::map<std::string, std::string> oracle;
    fill_buf(&t, 200, &oracle);
    ASSERT_TRUE(t.snapshot(nullptr).ok()); // clean + resident, touch order == snapshot's DFS walk

    // Re-touch the very first key's leaf so it becomes the *most* recently
    // touched -- recency ranking should keep it resident longer than leaves
    // nothing has re-read since the snapshot walk.
    std::string v;
    uint64_t    s;
    ASSERT_TRUE(t.get(Slice(make_key(0)), &s, &v));

    int reads_before = store.reads.load();
    // Aggressive budget: keep only a single resident leaf.
    size_t evicted = t.evict_clean_leaves(1);
    EXPECT_GT(evicted, 0U);

    // The just-touched leaf must still be resident: no fresh demand-load.
    ASSERT_TRUE(t.get(Slice(make_key(0)), &s, &v));
    EXPECT_EQ(v, oracle[make_key(0)]);
    EXPECT_EQ(store.reads.load(), reads_before) << "recently-touched leaf should not have been evicted";

    // A leaf nothing re-touched should have been evicted and demand-loads on
    // next access.
    ASSERT_TRUE(t.get(Slice(make_key(150)), &s, &v));
    EXPECT_EQ(v, oracle[make_key(150)]);
    EXPECT_GT(store.reads.load(), reads_before) << "a colder leaf should have been evicted and reloaded";
}

// plan-tree #17 D3: evict_clean_inner is a genuinely separate pass/budget
// from evict_clean_leaves -- never touches a leaf, ranked independently.
TEST(Eviction, EvictCleanInnerNeverTouchesLeaves)
{
    CountingPageStore store(1);
    Options           opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 80; // tiny leaves -> many of them
    opt.inner_max_keys   = 4;  // low fanout -> multiple inner bases, not just the root
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    std::map<std::string, std::string> oracle;
    fill_buf(&t, 300, &oracle); // enough keys for several inner levels
    ASSERT_TRUE(t.snapshot(nullptr).ok());

    int reads_before = store.reads.load();
    // Aggressive inner budget: keep at most 1 inner base resident. Leaves
    // must be entirely unaffected -- every value stays readable, and (since
    // evict_clean_inner never touches leaves) any leaf reads below only
    // reflect inner-base reloads, never a leaf-base one.
    size_t evicted = t.evict_clean_inner(1);
    EXPECT_GT(evicted, 0U) << "300 keys should span more than one inner base";

    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t.get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
    EXPECT_GT(store.reads.load(), reads_before) << "evicted inner bases should have demand-loaded again";

    // A second pass with the budget already satisfied evicts nothing more
    // (mirrors EvictIsIdempotentAndSkipsDirty's leaf-side analog) -- reload
    // the whole oracle first so every inner base on these paths is resident
    // and clean again.
    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t.get(Slice(kv.first), &s, &v));
    }
    EXPECT_EQ(t.evict_clean_inner(300), 0U) << "budget already satisfied for a tree this small";
}

// plan-tree #17 D3's whole reason for a *separate* pass: recency-ranking
// leaves and inner bases together would let some unrelated leaf's ranking
// evict a just-touched key's own ancestor chain, forcing extra reads on the
// very next access -- see evict_clean_inner_locked's doc comment
// (crow-tree.cpp). With disjoint passes that can never happen: touching a
// leaf's whole ancestor chain keeps it resident against evict_clean_inner
// alone, in preference to a colder, unrelated chain -- exactly like
// RecentlyTouchedLeafSurvivesEvictionOverColderOnes already proves for
// evict_clean_leaves alone.
//
// Deliberately measures key(0)'s own ancestor-chain depth (`depth0` below)
// rather than assuming a fixed tree shape/depth from `inner_max_keys` and
// key count: evict_clean_inner(0) evicts *every* resident inner base and
// returns the count, so re-touching key(0) alone and evicting again with
// budget 0 yields exactly how many inner bases sit on its path -- whatever
// that number turns out to be for this tree.
TEST(Eviction, RecentlyTouchedAncestorChainSurvivesInnerEvictionOverColderOnes)
{
    CountingPageStore store(1);
    Options           opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 80;
    opt.inner_max_keys   = 4;
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    std::map<std::string, std::string> oracle;
    fill_buf(&t, 300, &oracle);
    ASSERT_TRUE(t.snapshot(nullptr).ok());

    std::string v;
    uint64_t    s;

    // Unload every inner base, then reload only key(0)'s own path by
    // touching it once -- the resident inner-base set is now exactly that
    // path, so evicting everything again (budget 0) measures its depth.
    (void)t.evict_clean_inner(0);
    ASSERT_TRUE(t.get(Slice(make_key(0)), &s, &v));
    size_t depth0 = t.evict_clean_inner(0);
    ASSERT_GT(depth0, 0U) << "key(0) must have at least one inner ancestor (the root)";

    // Reload a different, unrelated key's path first (older ticks), then
    // key(0)'s path again (newest ticks) -- the resident set is now the
    // union of both paths (any shared ancestor, e.g. the root, counted once
    // and stamped with key(0)'s newer touch).
    ASSERT_TRUE(t.get(Slice(make_key(250)), &s, &v));
    ASSERT_TRUE(t.get(Slice(make_key(0)), &s, &v));

    int reads_before = store.reads.load();
    // Budget == key(0)'s own path depth: only key(250)'s ancestors that
    // aren't shared with key(0)'s path (strictly older ticks) should be
    // evicted.
    size_t evicted = t.evict_clean_inner(depth0);

    // key(0)'s whole path must still be fully resident: no fresh demand-load
    // at all.
    ASSERT_TRUE(t.get(Slice(make_key(0)), &s, &v));
    EXPECT_EQ(v, oracle[make_key(0)]);
    EXPECT_EQ(store.reads.load(), reads_before)
        << "recently-touched leaf's ancestor chain should not have been evicted";

    // key(250)'s path shares nothing but possibly the (now-preserved) root
    // with key(0)'s -- if the two diverge below the root at all (very
    // likely with inner_max_keys this low and keys this far apart), at
    // least one of its ancestors was colder than key(0)'s and got evicted.
    if (evicted > 0) {
        ASSERT_TRUE(t.get(Slice(make_key(250)), &s, &v));
        EXPECT_EQ(v, oracle[make_key(250)]);
        EXPECT_GT(store.reads.load(), reads_before) << "a colder inner base should have been evicted and reloaded";
    }
}

// Regression: free_subtree's root->children walk (used by ~Crowtree and
// install_snapshot(_native) to drop the live tree) bails out the moment it
// reaches an *unloaded* slot -- previously always safe, since only a leaf
// (no descendants) could ever be independently evicted, but not once
// evict_clean_inner can unload an *inner* ancestor while its leaf
// descendants stay fully resident underneath it: a root-rooted walk that
// bails on the unloaded root would never reach (or free) those leaves at
// all. Fixed by free_all_resident_pages's segment-scan (crow-tree.cpp) --
// this exercises it via install_snapshot's retire=true path (checkable
// without a sanitizer, via BufferPool::Stats::used, unlike ~Crowtree's own
// teardown path, which needs ASan's leak detector to observe from outside).
TEST(Eviction, InstallSnapshotReclaimsResidentLeavesEvenWhenInnerAncestorWasEvicted)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 80;
    opt.inner_max_keys   = 4;
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    std::map<std::string, std::string> oracle;
    fill_buf(&t, 300, &oracle);
    ASSERT_TRUE(t.snapshot(nullptr).ok());

    // Evict every inner base -- every leaf remains fully resident underneath
    // a now-entirely-unloaded inner chain, including the root itself.
    size_t inner_evicted = t.evict_clean_inner(0);
    EXPECT_GT(inner_evicted, 0U);

    uint32_t used_before = t.buffer_pool()->stats().used;
    EXPECT_GT(used_before, 0U) << "every leaf should still be resident (never evicted)";

    // Replaces the whole live tree -- must reclaim every still-resident
    // leaf, not just the (already-unloaded) inner ancestors.
    ASSERT_TRUE(t.install_snapshot({}, 0).ok());

    // No reader guards are open, so the epoch manager reclaims synchronously
    // (same assumption the leaf-eviction tests above rely on): only the
    // freshly-built empty root should remain resident.
    EXPECT_EQ(t.buffer_pool()->stats().used, 1U)
        << "install_snapshot must reclaim the old tree's still-resident leaves "
           "even though their ancestor chain was unloaded";
}
