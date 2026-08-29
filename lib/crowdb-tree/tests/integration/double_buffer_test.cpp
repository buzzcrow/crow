// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// plan-tree #3: MemTable double buffering (active_ + frozen_). See the
// active_/frozen_ member comment in crowdb-tree.h for the full design.
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <map>
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
