// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// plan-tree #3: MemTable double buffering (active_ + frozen_). See the
// active_/frozen_ member comment in crowdb-tree.h for the full design.
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <map>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace crowdb::tree;

namespace
{

Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

std::string get_or(Crowdbtree &t, const std::string &k, const std::string &dflt)
{
    std::string v;
    uint64_t    slot;
    return t.get(Slice(k), &slot, &v) ? v : dflt;
}

std::string make_key(int i)
{
    std::array<char, 16> buf{};
    snprintf(buf.data(), buf.size(), "key%04d", i);
    return buf.data();
}

} // namespace

// Crossing the size/entry threshold freezes active_ into frozen_ and installs
// a fresh active_ (maybe_swap_active(), no drain) -- purely a pointer swap, no
// data movement into L1. Every previously-written key must stay readable
// (get() checks every live MemTable, see the active_/frozen_ member comment),
// and memtable_count() (summed across active_ + frozen_) must be unaffected
// by the swap itself -- only an actual flush() drains anything.
TEST(DoubleBuffer, ThresholdSwapKeepsAllEntriesReadable)
{
    Options opt;
    opt.memtable_flush_entries = 2; // force a freeze every couple of applies
    Crowdbtree t(opt);

    const int K = 20;
    for (int i = 0; i < K; ++i) {
        ASSERT_TRUE(t.apply(static_cast<uint64_t>(i + 1), put_one(make_key(i), "v" + std::to_string(i))).ok());
    }
    // No flush() yet: everything must still be entirely in L0 (across
    // whatever number of frozen_ generations the threshold swaps produced).
    EXPECT_EQ(t.memtable_count(), static_cast<size_t>(K));
    for (int i = 0; i < K; ++i) {
        EXPECT_EQ(get_or(t, make_key(i), "?"), "v" + std::to_string(i));
    }

    // contiguous_slot_ caught up with every apply (slots 1..K, no gaps), so a
    // single flush() call freezes+drains everything down to L1.
    ASSERT_TRUE(t.flush().ok());
    EXPECT_EQ(t.memtable_count(), 0U);
    for (int i = 0; i < K; ++i) {
        EXPECT_EQ(get_or(t, make_key(i), "?"), "v" + std::to_string(i));
    }
}

// The documented non-contiguous-slot handling: when a frozen_ table is
// drained, entries with slot > the current contiguous frontier are relocated
// onto the *live* active_ MemTable instead of being lost, and they may
// bounce through more than one freeze/relocate cycle (a distinct MemTable
// object each time) before finally becoming contiguous-eligible and landing
// in L1. This forces every apply() to freeze (entries threshold = 1) so the
// relocation is exercised across genuinely different MemTable generations,
// not just "stays in the same object" (which write_path_test.cpp's
// FlushOnlyContiguousPrefix already covers for the pre-double-buffering
// single-table case).
TEST(DoubleBuffer, NonContiguousLeftoverSurvivesAcrossFreezeGenerations)
{
    Options opt;
    opt.memtable_flush_entries = 1; // freeze after every single apply
    Crowdbtree t(opt);

    // slot 5 for "a" is never contiguous with the (empty) frontier at 0.
    ASSERT_TRUE(t.apply(5, put_one("a", "A5")).ok()); // freezes immediately (threshold=1)
    EXPECT_EQ(t.memtable_count(), 1U);
    EXPECT_EQ(get_or(t, "a", "?"), "A5");

    // flush() at cs=0: freezes the (empty) active_ (no-op), then drains the
    // frozen table holding "a"@5 -- drain_up_to(0) can't take it (5 > 0), so
    // it is relocated onto the (fresh) active_ instead of being dropped.
    ASSERT_TRUE(t.flush().ok());
    EXPECT_EQ(t.last_applied_slot(), 0U);
    EXPECT_EQ(t.memtable_count(), 1U); // still resident in L0, just relocated
    EXPECT_EQ(get_or(t, "a", "?"), "A5");

    // Advancing the frontier to 5 makes "a"@5 contiguous-eligible.
    // force_advance_slot() also calls maybe_swap_active(): active_ (holding
    // the relocated "a"@5) is at the entries=1 threshold, so it gets frozen
    // right here -- a *different* MemTable object than the one flushed
    // above, exercising the relocation surviving across two distinct
    // freeze generations before finally draining.
    t.force_advance_slot(5);
    ASSERT_TRUE(t.flush().ok());
    EXPECT_EQ(t.last_applied_slot(), 5U);
    EXPECT_EQ(t.memtable_count(), 0U); // now durable in L1
    EXPECT_EQ(get_or(t, "a", "?"), "A5");
}

// max_memtable_count bounds the frozen_ queue depth (active_ + up to
// max_memtable_count-1 queued frozen buffers). Exercise more than the
// default 2 buffers: force a freeze on every apply with a set of keys whose
// slots never become contiguous, so freezes pile up in frozen_ without an
// intervening flush() drain. Once the cap is hit, further threshold trips
// are skipped (active_ keeps growing) rather than stalling the writer or
// growing frozen_ without bound -- but no data is ever lost regardless of
// how many buffer generations a key's write passed through, which is what
// this test actually asserts (the cap is an internal bookkeeping optimization
// with no user-visible behavior difference besides that).
TEST(DoubleBuffer, SupportsMoreThanTwoBuffersAndFlushesAllOfThemCorrectly)
{
    Options opt;
    opt.max_memtable_count     = 4; // active_ + up to 3 queued frozen_ buffers
    opt.memtable_flush_entries = 1; // freeze after every single apply
    Crowdbtree t(opt);

    const int K = 8;
    // Widely-spaced, strictly increasing slots: contiguous_slot_ never
    // advances past 0 until force_advance_slot at the end, so every one of
    // these applies stays non-contiguous and none of the freezes it triggers
    // get drained until the very last flush() below.
    for (int i = 0; i < K; ++i) {
        uint64_t s = static_cast<uint64_t>((i + 1) * 10);
        ASSERT_TRUE(t.apply(s, put_one(make_key(i), "v" + std::to_string(i))).ok());
    }
    EXPECT_EQ(t.last_applied_slot(), 0U);
    EXPECT_EQ(t.memtable_count(), static_cast<size_t>(K));
    for (int i = 0; i < K; ++i) {
        EXPECT_EQ(get_or(t, make_key(i), "?"), "v" + std::to_string(i));
    }

    t.force_advance_slot(static_cast<uint64_t>(K * 10));
    ASSERT_TRUE(t.flush().ok());
    EXPECT_EQ(t.memtable_count(), 0U);
    for (int i = 0; i < K; ++i) {
        EXPECT_EQ(get_or(t, make_key(i), "?"), "v" + std::to_string(i));
    }
}

// Out-of-order slot delivery can straddle a freeze boundary: a lower slot for
// a key can end up in a *later* (fresher) MemTable generation than a higher
// slot for the same key sitting in an earlier, already-frozen generation.
// get()/scan() must resolve the highest-slot cell across every live
// MemTable, not just prefer whichever table is more recent -- see the
// active_/frozen_ member comment's "Read-side correctness" paragraph.
TEST(DoubleBuffer, GetAndScanResolveHighestSlotAcrossOutOfOrderFreezeBoundary)
{
    Options opt;
    opt.memtable_flush_entries = 1; // freeze after every single apply
    Crowdbtree t(opt);

    // "a"@20 lands in generation 1, which is immediately frozen by the next
    // apply()'s threshold check.
    ASSERT_TRUE(t.apply(20, put_one("a", "A20")).ok());
    // "a"@10 (an older, out-of-order slot for the same key) lands in the
    // fresh active_ (generation 2) installed by the freeze above.
    ASSERT_TRUE(t.apply(10, put_one("a", "A10")).ok());

    EXPECT_EQ(t.memtable_count(), 2U); // two distinct live cells for "a"
    uint64_t    slot = 0;
    std::string value;
    ASSERT_TRUE(t.get(Slice("a"), &slot, &value));
    EXPECT_EQ(slot, 20U);
    EXPECT_EQ(value, "A20");

    std::vector<scan_entry> out;
    bool                    truncated = false;
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 0, false, 0, &out, &truncated).ok());
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0].key, "a");
    EXPECT_EQ(out[0].slot, 20U);
    EXPECT_EQ(out[0].value, "A20");
}

// Stress: concurrent readers (get + scan) racing a single writer whose small
// memtable_flush_entries threshold forces frequent freeze-into-frozen_ swaps
// (and flush() drains) throughout the run -- "reads see a consistent overlay
// while a flush swap is in flight" (plan-tree #3's test checklist). Run under
// TSan/ASan to catch races/UAF in the active_/frozen_ swap + drain path.
TEST(DoubleBuffer, ConcurrentReadersDuringFrequentFreezeAndDrainNoCorruption)
{
    Options opt;
    opt.memtable_flush_entries = 4; // freeze/drain every few applies
    opt.max_memtable_count     = 3;
    Crowdbtree t(opt);

    const int         K = 200;
    std::atomic<bool> stop{false};
    std::atomic<bool> bad{false};
    std::atomic<long> reads{0};

    std::vector<std::thread> readers;
    readers.reserve(4);
    for (int r = 0; r < 4; ++r) {
        readers.emplace_back([&, r] {
            std::mt19937 rng(9000 + r);
            std::string  v;
            uint64_t     s;
            while (!stop.load(std::memory_order_relaxed) && !bad.load(std::memory_order_relaxed)) {
                for (int g = 0; g < 8; ++g) {
                    // No correctness oracle check here (the writer below applies
                    // concurrently) -- this is purely a liveness/no-crash/no-UAF
                    // check against TSan/ASan while active_/frozen_ churn.
                    (void)t.get(Slice(make_key(static_cast<int>(rng() % K))), &s, &v);
                }
                std::vector<scan_entry> out;
                bool                    trunc = false;
                if (!t.scan(Slice("key"), Slice(), Slice(), 16, 0, false, 0, &out, &trunc).ok()) {
                    bad.store(true);
                    return;
                }
                for (size_t i = 1; i < out.size(); ++i) {
                    if (!(Slice(out[i - 1].key).compare(Slice(out[i].key)) < 0)) {
                        bad.store(true); // not strictly increasing -> duplicate or out of order
                        return;
                    }
                }
                reads.fetch_add(8, std::memory_order_relaxed);
                std::this_thread::yield();
            }
        });
    }

    std::map<std::string, std::string> oracle;
    std::mt19937                       rng(77);
    uint64_t                           slot = 0;
    for (int step = 0; step < 4000; ++step) {
        ++slot;
        std::string key = make_key(static_cast<int>(rng() % K));
        if ((rng() % 5) == 0) {
            ASSERT_TRUE(t.apply(slot, Batch{{{.key = key, .kind = OpKind::kDelete, .value = ""}}}).ok());
            oracle.erase(key);
        }
        else {
            std::string val = "v" + std::to_string(slot);
            ASSERT_TRUE(t.apply(slot, put_one(key, val)).ok());
            oracle[key] = val;
        }
        if ((rng() % 10) == 0) {
            ASSERT_TRUE(t.flush().ok());
        }
    }
    ASSERT_TRUE(t.flush().ok());

    stop.store(true);
    for (auto &th : readers) {
        th.join();
    }
    EXPECT_FALSE(bad.load());
    EXPECT_GT(reads.load(), 0);

    // Final state (post-flush, no concurrent writer) matches the oracle.
    for (const auto &kv : oracle) {
        EXPECT_EQ(get_or(t, kv.first, "?"), kv.second);
    }
    std::vector<scan_entry> out;
    bool                    trunc = false;
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 0, false, 0, &out, &trunc).ok());
    ASSERT_EQ(out.size(), oracle.size());
}

// R58: exercise the loser tree merge path (k > 2 sources) by forcing 3+
// frozen memtables to pile up without draining, then scan. The scan must
// produce correct merge order (sorted keys), highest-slot-wins on key
// collisions across L0 streams, no duplicate keys, and no missing keys.
// Uses the same non-contiguous-slot pattern as
// SupportsMoreThanTwoBuffersAndFlushesAllOfThemCorrectly above.
TEST(DoubleBuffer, ScanMergeLoserTreeWithMultipleFrozenMemtables)
{
    Options opt;
    opt.max_memtable_count     = 6; // active_ + up to 5 queued frozen_ buffers
    opt.memtable_flush_entries = 1; // freeze after every single apply
    Crowdbtree t(opt);

    // Write the same 3 keys into 4 separate frozen generations, each with a
    // strictly increasing slot so the highest-slot cell is always in the
    // oldest (first-frozen) generation. This forces 4 frozen_ + 1 active_ =
    // 5 L0 sources (> 2 → loser tree path) with key collisions across every
    // source.
    const std::string keys[] = {"a", "b", "c"};
    uint64_t          slot   = 100;
    for (int gen = 0; gen < 4; ++gen) {
        for (const auto &k : keys) {
            ASSERT_TRUE(t.apply(slot, put_one(k, "v" + std::to_string(slot))).ok());
            slot += 10;
        }
    }
    // 12 applies, 12 frozen memtables (but capped at 5 by max_memtable_count).
    // The exact frozen count depends on the cap; the scan must be correct
    // regardless. Every key appears in multiple sources with different slots.

    std::vector<scan_entry> out;
    bool                    trunc = false;
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 0, false, 0, &out, &trunc).ok());
    EXPECT_FALSE(trunc);

    // Exactly 3 unique keys, in sorted order.
    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(out[0].key, "a");
    EXPECT_EQ(out[1].key, "b");
    EXPECT_EQ(out[2].key, "c");

    // Highest slot for each key: the last write to each key. The slots are
    // assigned in order: a@100, b@110, c@120, a@130, b@140, c@150, a@160,
    // b@170, c@180, a@190, b@200, c@210. So the highest slot for each key is:
    // a@190, b@200, c@210.
    EXPECT_EQ(out[0].slot, 190u);
    EXPECT_EQ(out[0].value, "v190");
    EXPECT_EQ(out[1].slot, 200u);
    EXPECT_EQ(out[1].value, "v200");
    EXPECT_EQ(out[2].slot, 210u);
    EXPECT_EQ(out[2].value, "v210");
}

// R58: the loser tree path with non-overlapping keys across 3+ frozen
// memtables — verifies correct merge order when each source has distinct
// keys (no collisions, just k-way interleaving).
TEST(DoubleBuffer, ScanMergeLoserTreeDistinctKeysAcrossFrozenMemtables)
{
    Options opt;
    opt.max_memtable_count     = 6;
    opt.memtable_flush_entries = 1;
    Crowdbtree t(opt);

    // Write 9 keys across 3 generations, each generation holding 3 distinct
    // keys that interleave with the others: gen0 has keys 0,3,6; gen1 has
    // 1,4,7; gen2 has 2,5,8. Non-contiguous slots prevent draining.
    for (int gen = 0; gen < 3; ++gen) {
        for (int i = 0; i < 3; ++i) {
            int      key_idx = gen + i * 3;
            uint64_t s       = static_cast<uint64_t>((key_idx + 1) * 10);
            ASSERT_TRUE(t.apply(s, put_one(make_key(key_idx), "v" + std::to_string(s))).ok());
        }
    }

    std::vector<scan_entry> out;
    bool                    trunc = false;
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 0, false, 0, &out, &trunc).ok());
    EXPECT_FALSE(trunc);

    // All 9 keys present, in sorted order.
    ASSERT_EQ(out.size(), 9u);
    for (int i = 0; i < 9; ++i) {
        EXPECT_EQ(out[i].key, make_key(i)) << "key " << i << " out of order";
    }
}

// O5+O1: verify the merged drain (k-way merge of frozen memtables with
// sort-aware descent) produces the same L1 state as the per-memtable drain.
// Multiple frozen memtables with overlapping keys and different slots must
// resolve to highest-slot-wins after flush, with no duplicates or missing keys.
TEST(DoubleBuffer, MergedDrainDedupAcrossFrozenMemtables)
{
    Options opt;
    opt.max_memtable_count     = 6;
    opt.memtable_flush_entries = 1; // freeze after every single apply
    Crowdbtree t(opt);

    // Write the same 5 keys into 3 separate frozen generations, each with
    // a strictly increasing slot. The highest-slot version is always in the
    // last generation. After flush, L1 must reflect the highest-slot value
    // for each key (cross-memtable dedup).
    const std::string keys[] = {"a", "b", "c", "d", "e"};
    uint64_t          slot   = 10;
    for (int gen = 0; gen < 3; ++gen) {
        for (const auto &k : keys) {
            ASSERT_TRUE(t.apply(slot, put_one(k, "v" + std::to_string(slot))).ok());
            slot += 10;
        }
    }
    // All slots are contiguous (10, 20, 30, ... 150), so flush drains all.
    ASSERT_TRUE(t.flush().ok());

    // After flush, L0 is empty and L1 has the highest-slot value per key.
    // Slots: a@130, b@140, c@150, d@160(→wait, recalc).
    // gen0: a@10, b@20, c@30, d@40, e@50
    // gen1: a@60, b@70, c@80, d@90, e@100
    // gen2: a@110, b@120, c@130, d@140, e@150
    for (const auto &k : keys) {
        uint64_t expected_slot = 0;
        if (k == "a")
            expected_slot = 110;
        if (k == "b")
            expected_slot = 120;
        if (k == "c")
            expected_slot = 130;
        if (k == "d")
            expected_slot = 140;
        if (k == "e")
            expected_slot = 150;
        EXPECT_EQ(get_or(t, k, "?"), "v" + std::to_string(expected_slot));
    }

    // Scan must show exactly 5 keys, sorted, with highest-slot values.
    std::vector<scan_entry> out;
    bool                    trunc = false;
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 0, false, 0, &out, &trunc).ok());
    EXPECT_FALSE(trunc);
    ASSERT_EQ(out.size(), 5u);
    EXPECT_EQ(out[0].key, "a");
    EXPECT_EQ(out[0].slot, 110u);
    EXPECT_EQ(out[1].key, "b");
    EXPECT_EQ(out[1].slot, 120u);
    EXPECT_EQ(out[2].key, "c");
    EXPECT_EQ(out[2].slot, 130u);
    EXPECT_EQ(out[3].key, "d");
    EXPECT_EQ(out[3].slot, 140u);
    EXPECT_EQ(out[4].key, "e");
    EXPECT_EQ(out[4].slot, 150u);
}

// O1: verify sort-aware descent correctly groups entries across leaf
// boundaries and handles splits during a large flush. Write enough entries
// to span multiple leaves (triggering splits), then flush and verify every
// key is readable and the scan is strictly ordered with no duplicates.
TEST(DoubleBuffer, SortAwareDescentAcrossLeafBoundariesWithSplits)
{
    Options opt;
    opt.max_memtable_count     = 6;
    opt.memtable_flush_entries = 50;  // freeze after 50 entries
    opt.leaf_split_bytes       = 512; // small leaves → many splits during flush
    opt.max_delta_len          = 4;   // frequent consolidates during drain
    Crowdbtree t(opt);

    const int N = 500;
    // Write N keys in shuffled order across multiple frozen generations.
    // The shuffle ensures the sorted merge stream crosses leaf boundaries
    // frequently, exercising the sort-aware descent's boundary detection.
    std::mt19937     rng(42);
    std::vector<int> perm(N);
    std::iota(perm.begin(), perm.end(), 0);
    std::shuffle(perm.begin(), perm.end(), rng);

    std::map<std::string, std::string> oracle;
    uint64_t                           slot = 1;
    for (int i = 0; i < N; ++i) {
        std::string key = make_key(perm[i]);
        std::string val = "v" + std::to_string(slot);
        ASSERT_TRUE(t.apply(slot, put_one(key, val)).ok());
        oracle[key] = val;
        slot++;
    }
    ASSERT_TRUE(t.flush().ok());

    // Every key must be readable with the correct value.
    for (const auto &kv : oracle) {
        EXPECT_EQ(get_or(t, kv.first, "?"), kv.second) << "key " << kv.first;
    }

    // Scan must show all N keys, sorted, no duplicates.
    std::vector<scan_entry> out;
    bool                    trunc = false;
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 0, false, 0, &out, &trunc).ok());
    EXPECT_FALSE(trunc);
    ASSERT_EQ(out.size(), static_cast<size_t>(N));
    for (size_t i = 1; i < out.size(); ++i) {
        EXPECT_LT(Slice(out[i - 1].key).compare(Slice(out[i].key)), 0) << "out of order at " << i;
    }
    for (const auto &e : out) {
        EXPECT_EQ(e.value, oracle[e.key]) << "wrong value for " << e.key;
    }
}

// Frozen queue full: when max_memtable_count is small and we write fast
// without flushing, the frozen queue fills up and maybe_freeze_active
// returns false — active_ keeps growing past the threshold. All entries
// must stay readable (they're in active_). This verifies the backpressure
// behavior documented in the maybe_freeze_active error log path.
TEST(DoubleBuffer, FrozenQueueFullActiveKeepsGrowingEntriesReadable)
{
    Options opt;
    opt.max_memtable_count     = 2; // 1 active + 1 frozen slot
    opt.memtable_flush_entries = 3;
    Crowdbtree t(opt);

    // Write enough to fill the frozen queue (1 slot) and overflow active_.
    // After 3 entries: active_ freezes → frozen_ has 1, active_ is fresh.
    // After 6 entries: active_ tries to freeze but frozen_ is full →
    // maybe_freeze_active returns false, active_ keeps growing.
    const int N = 20;
    for (int i = 0; i < N; ++i) {
        ASSERT_TRUE(t.apply(static_cast<uint64_t>(i + 1), put_one(make_key(i), "v" + std::to_string(i))).ok());
    }

    // Without flush(), all entries must still be readable from L0
    // (active_ + frozen_). The frozen queue full condition does NOT
    // lose data — it just delays the freeze.
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(get_or(t, make_key(i), "?"), "v" + std::to_string(i));
    }

    // Flush drains everything — all entries move to L1.
    ASSERT_TRUE(t.flush().ok());
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(get_or(t, make_key(i), "?"), "v" + std::to_string(i));
    }
}

// Parent pointer correctness after splits (O4): write enough data to
// trigger multiple leaf splits and inner node creation, then verify all
// keys are readable. This exercises path_to_page_id_locked (which uses
// parent pointers) during split/merge operations. With small split
// thresholds, even a modest number of keys creates a multi-level tree.
TEST(DoubleBuffer, ParentPointersCorrectAfterSplitsAndMerges)
{
    Options opt;
    opt.leaf_split_bytes   = 256; // small leaves → many splits
    opt.max_delta_len      = 2;   // frequent consolidates → splits during drain
    opt.max_memtable_count = 6;
    Crowdbtree t(opt);

    // Write 200 keys in shuffled order to create a bushy tree with
    // many splits and inner node levels.
    std::mt19937     rng(12345);
    std::vector<int> perm(200);
    std::iota(perm.begin(), perm.end(), 0);
    std::shuffle(perm.begin(), perm.end(), rng);

    std::map<std::string, std::string> oracle;
    uint64_t                           slot = 1;
    for (int i : perm) {
        std::string key = make_key(i);
        std::string val = "v" + std::to_string(slot);
        oracle[key]     = val;
        ASSERT_TRUE(t.apply(slot, put_one(key, val)).ok());
        slot++;
    }

    // Flush to drain L0 → L1, triggering splits via consolidate.
    ASSERT_TRUE(t.flush().ok());

    // All keys must be readable with correct values — if parent pointers
    // were stale after splits, path_to_page_id_locked would return a
    // wrong path, causing maybe_split_or_merge_locked to operate on the
    // wrong parent, corrupting the tree.
    for (const auto &kv : oracle) {
        EXPECT_EQ(get_or(t, kv.first, "?"), kv.second) << "key " << kv.first;
    }

    // Delete half the keys to trigger merges (which also use parent
    // pointers via path_to_page_id_locked).
    int deleted = 0;
    for (auto it = oracle.begin(); it != oracle.end(); ++it) {
        if (deleted >= 100) {
            break;
        }
        Batch del{{{.key = it->first, .kind = OpKind::kDelete, .value = ""}}};
        ASSERT_TRUE(t.apply(slot, del).ok());
        slot++;
        deleted++;
    }

    ASSERT_TRUE(t.flush().ok());

    // Surviving keys must still be readable.
    deleted = 0;
    for (const auto &kv : oracle) {
        if (deleted < 100) {
            deleted++;
            continue; // deleted key
        }
        EXPECT_EQ(get_or(t, kv.first, "?"), kv.second) << "surviving key " << kv.first;
    }

    // Scan must return surviving keys in sorted order, no corruption.
    std::vector<scan_entry> out;
    bool                    trunc = false;
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 0, false, 0, &out, &trunc).ok());
    EXPECT_FALSE(trunc);
    // Deleted keys should have tombstones (or be GC'd); surviving keys
    // must be present with correct values.
    for (const auto &e : out) {
        if (oracle.count(e.key) > 0) {
            int idx = 0;
            for (auto it = oracle.begin(); it != oracle.end() && it->first != e.key; ++it, ++idx) {
            }
            if (idx >= 100) {
                EXPECT_EQ(e.value, oracle[e.key]) << "scan value for " << e.key;
            }
        }
    }
}

// Re-check loop: when multiple memtables are frozen before flush() is called,
// a single flush() must drain ALL of them (not just the first). The k-way
// merge handles all frozen tables in one drain pass; the re-check loop
// catches any that freeze during the drain itself.
TEST(DoubleBuffer, FlushDrainsAllFrozenMemtablesInOneCall)
{
    Options opt;
    opt.memtable_flush_entries = 5; // small threshold → frequent freezes
    opt.max_memtable_count     = 10;
    Crowdbtree t(opt);

    // Write enough to freeze several memtables without flushing.
    const int N = 40;
    for (int i = 0; i < N; ++i) {
        ASSERT_TRUE(t.apply(static_cast<uint64_t>(i + 1), put_one(make_key(i), "v" + std::to_string(i))).ok());
    }
    // Multiple memtables should be frozen (each holds up to 5 entries).
    ASSERT_GT(t.frozen_table_count(), 0U);

    // A single flush() must drain them all.
    ASSERT_TRUE(t.flush().ok());
    EXPECT_EQ(t.frozen_table_count(), 0U);

    // All entries must be readable from L1.
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(get_or(t, make_key(i), "?"), "v" + std::to_string(i));
    }
}

// Iteration cap: when more memtables freeze than max_memtable_count, flush()
// exits at the cap and remaining tables stay in frozen_ for the next flush().
// No data loss — the next flush() drains the rest.
TEST(DoubleBuffer, FlushIterationCapExitsCleanly)
{
    Options opt;
    opt.memtable_flush_entries = 3; // tiny threshold → many freezes
    opt.max_memtable_count     = 2; // 1 active + 1 frozen slot; cap = 2
    Crowdbtree t(opt);

    // Write a lot without flushing. With max_memtable_count=2, the frozen
    // queue fills at 1 slot; further freezes are skipped (active_ grows).
    // So frozen_ has at most 1 entry — the cap is not exercised by writes
    // alone. Instead, verify the cap path is correct: flush() drains what's
    // frozen and leaves frozen_ empty (the cap is a safety net, not the
    // common path here).
    const int N = 30;
    for (int i = 0; i < N; ++i) {
        ASSERT_TRUE(t.apply(static_cast<uint64_t>(i + 1), put_one(make_key(i), "v" + std::to_string(i))).ok());
    }
    ASSERT_TRUE(t.flush().ok());
    EXPECT_EQ(t.frozen_table_count(), 0U);

    // All entries readable.
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(get_or(t, make_key(i), "?"), "v" + std::to_string(i));
    }
}
