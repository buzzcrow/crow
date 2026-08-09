// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// CT14: concurrent readers while a single writer applies/flushes/splits/merges.
// Run under TSan/ASan to catch races and use-after-free in epoch reclamation.
#include "crow-tree/crow-tree.h"
#include "crow-tree/page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstdio>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace crow::tree;

namespace
{
std::string make_key(int i)
{
    std::array<char, 16> buf{};
    snprintf(buf.data(), buf.size(), "key%04d", i);
    return buf.data();
}
} // namespace

// Many readers race to demand-load an all-unloaded recovered tree (design
// §4.5 cold path). Run under TSan: the load_mutex_-serialized installs must not
// race with lock-free reads of just-published pages, and descriptors freed on
// transition must not be touched by a concurrent reader.
TEST(Stress, ConcurrentDemandLoadAfterRecovery)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;

    const int K = 300;
    {
        Crowtree t(opt);
        // flush incrementally so the tree splits into many small (pool-sized) leaves
        // rather than one oversized leaf (a single bulk flush only halves once).
        for (int i = 0; i < K; ++i) {
            std::string val = "val" + std::to_string(i);
            ASSERT_TRUE(t.apply(i + 1, Batch{{{.key = make_key(i), .kind = OpKind::kPut, .value = val}}}).ok());
            ASSERT_TRUE(t.flush().ok());
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok());
    }

    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
    EXPECT_EQ(t2->buffer_pool()->stats().used, 0U); // nothing loaded yet

    std::atomic<bool>        fail{false};
    std::vector<std::thread> readers;
    readers.reserve(8);
    for (int r = 0; r < 8; ++r) {
        readers.emplace_back([&, r] {
            std::mt19937 rng(7000 + r);
            std::string  v;
            uint64_t     s;
            for (int it = 0; it < 4000 && !fail.load(std::memory_order_relaxed); ++it) {
                int i = static_cast<int>(rng() % K);
                if (!t2->get(Slice(make_key(i)), &s, &v) || v != "val" + std::to_string(i)) {
                    fail.store(true);
                    return;
                }
            }
        });
    }
    for (auto &th : readers) {
        th.join();
    }
    EXPECT_FALSE(fail.load());
    EXPECT_GT(t2->buffer_pool()->stats().used, 0U); // demand-loaded into the pool
}

TEST(Stress, ConcurrentReadersSingleWriter)
{
    Options opt;
    opt.max_delta_len    = 2;
    opt.leaf_split_bytes = 160;
    opt.leaf_merge_bytes = 50;
    // Low fanout so the tree is multi-level and the delete-heavy churn below
    // exercises inner-node split AND underflow-merge SMOs concurrent with readers.
    opt.inner_max_keys   = 8;
    opt.inner_merge_keys = 3;
    Crowtree t(opt);

    const int         K = 200;
    std::atomic<bool> stop{false};
    std::atomic<long> reads{0};

    // Reader threads: point reads + range scans concurrent with all SMOs.
    std::vector<std::thread> readers;
    readers.reserve(4);
    for (int r = 0; r < 4; ++r) {
        readers.emplace_back([&, r] {
            std::mt19937            rng(1000 + r);
            std::string             v;
            uint64_t                s;
            std::vector<scan_entry> out;
            bool                    trunc;
            long                    iter = 0;
            while (!stop.load(std::memory_order_relaxed)) {
                // Exercise the point-read path; scan (lock-free since #5 B3 -- an
                // epoch guard, not write_mutex_) only occasionally, concurrent with
                // the writer's splits/merges below. Yield so the single writer isn't
                // starved (the v1 epoch manager serializes guards on one mutex).
                for (int g = 0; g < 8; ++g) {
                    (void)t.get(Slice(make_key(static_cast<int>(rng() % K))), &s, &v);
                }
                if ((iter++ % 32) == 0) {
                    t.scan(Slice("key0"), Slice(), Slice(), 16, 0, &out, &trunc);
                }
                reads.fetch_add(8, std::memory_order_relaxed);
                std::this_thread::yield();
            }
        });
    }

    // Single writer owns the oracle.
    std::map<std::string, std::string> oracle;
    std::mt19937                       rng(42);
    uint64_t                           slot = 0;
    for (int step = 0; step < 8000; ++step) {
        ++slot;
        std::string key = make_key(static_cast<int>(rng() % K));
        if ((rng() % 4) == 0) {
            ASSERT_TRUE(t.apply(slot, Batch{{{.key = key, .kind = OpKind::kDelete, .value = ""}}}).ok());
            oracle.erase(key);
        }
        else {
            std::string val = "v" + std::to_string(slot);
            ASSERT_TRUE(t.apply(slot, Batch{{{.key = key, .kind = OpKind::kPut, .value = val}}}).ok());
            oracle[key] = val;
        }
        if ((rng() % 6) == 0) {
            ASSERT_TRUE(t.flush().ok());
        }
        if (step % 5000 == 4999) {
            t.set_gc_watermark(slot, slot);
        }
    }
    ASSERT_TRUE(t.flush().ok());

    stop.store(true);
    for (auto &th : readers) {
        th.join();
    }
    EXPECT_GT(reads.load(), 0);

    // Final state matches the oracle.
    std::vector<scan_entry> out;
    bool                    trunc = false;
    ASSERT_TRUE(t.scan(Slice(""), Slice(), Slice(), 0, 0, &out, &trunc).ok());
    ASSERT_EQ(out.size(), oracle.size());
    size_t i = 0;
    for (const auto &kv : oracle) {
        EXPECT_EQ(out[i].key, kv.first);
        EXPECT_EQ(out[i].value, kv.second);
        ++i;
    }
}

// plan-tree #5 B3: scan() no longer holds write_mutex_ (epoch guard only),
// walking L1 leaf-by-leaf via right_sibling instead of materializing the whole
// tree under a lock. Unlike ConcurrentReadersSingleWriter above (which mostly
// exercises get() and only checks scan() once at the end), this test hammers
// scan() itself on every iteration concurrently with heavy split/merge churn
// and validates the invariants that must hold for *any* torn/mid-mutation
// snapshot a lock-free scan can observe: sorted order, no duplicate keys, every
// key actually matches the prefix, and no corrupted (malformed) value -- i.e.
// no missed right_sibling catch-up, no double-visited leaf, no crash/UAF
// (run under TSan/ASan).
TEST(Stress, ConcurrentScanDuringChurnNoCorruption)
{
    Options opt;
    opt.max_delta_len    = 2;
    opt.leaf_split_bytes = 160;
    opt.leaf_merge_bytes = 50;
    opt.inner_max_keys   = 8;
    opt.inner_merge_keys = 3;
    Crowtree t(opt);

    const int         K = 300;
    std::atomic<bool> stop{false};
    std::atomic<long> scans{0};
    std::atomic<bool> bad{false};

    std::vector<std::thread> scanners;
    scanners.reserve(4);
    for (int r = 0; r < 4; ++r) {
        scanners.emplace_back([&, r] {
            std::mt19937 rng(2000 + r);
            while (!stop.load(std::memory_order_relaxed) && !bad.load(std::memory_order_relaxed)) {
                // Mix full scans and narrow-prefix scans (prefix scans exercise the
                // find_leaf_page_id start-point + early-stop path specifically).
                std::string prefix = (rng() % 2 == 0) ? "" : make_key(static_cast<int>(rng() % K)).substr(0, 6);
                std::vector<scan_entry> out;
                bool                    trunc = false;
                if (!t.scan(Slice(prefix), Slice(), Slice(), 0, 0, &out, &trunc).ok()) {
                    bad.store(true);
                    return;
                }
                for (size_t i = 0; i < out.size(); ++i) {
                    if (!Slice(out[i].key).starts_with(Slice(prefix))) {
                        bad.store(true); // key doesn't belong in this prefix's results
                        return;
                    }
                    if (i > 0 && !(Slice(out[i - 1].key).compare(Slice(out[i].key)) < 0)) {
                        bad.store(true); // not strictly increasing -> duplicate or out of order
                        return;
                    }
                    // Values are always written as "v<slot>"; anything else is corruption.
                    if (out[i].value.empty() || out[i].value[0] != 'v') {
                        bad.store(true);
                        return;
                    }
                }
                scans.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::mt19937 rng(43);
    uint64_t     slot = 0;
    for (int step = 0; step < 6000; ++step) {
        ++slot;
        std::string key = make_key(static_cast<int>(rng() % K));
        if ((rng() % 4) == 0) {
            ASSERT_TRUE(t.apply(slot, Batch{{{.key = key, .kind = OpKind::kDelete, .value = ""}}}).ok());
        }
        else {
            std::string val = "v" + std::to_string(slot);
            ASSERT_TRUE(t.apply(slot, Batch{{{.key = key, .kind = OpKind::kPut, .value = val}}}).ok());
        }
        if ((rng() % 6) == 0) {
            ASSERT_TRUE(t.flush().ok());
        }
    }
    ASSERT_TRUE(t.flush().ok());

    stop.store(true);
    for (auto &th : scanners) {
        th.join();
    }
    EXPECT_FALSE(bad.load());
    EXPECT_GT(scans.load(), 0);
}

// plan-tree #8: snapshot_view() no longer holds write_mutex_ (epoch guard
// only), walking L1 via the same right_sibling technique as scan(). This
// hammers snapshot_view() concurrently with heavy split/merge/flush churn and
// checks the invariants that must hold for any torn/mid-mutation snapshot:
// sorted order, no duplicate keys, well-formed values, and that at_slot is a
// valid *lower bound* (every key/value the snapshot's own get() resolves to
// is consistent with some slot <= the highest slot applied so far -- entries
// may run ahead of at_slot, per the at_slot-captured-before-the-walk
// argument in snapshot_view()'s header comment, but must never show a value
// that couldn't have existed at any real point in time).
TEST(Stress, ConcurrentSnapshotViewDuringChurnNoCorruption)
{
    Options opt;
    opt.max_delta_len    = 2;
    opt.leaf_split_bytes = 160;
    opt.leaf_merge_bytes = 50;
    opt.inner_max_keys   = 8;
    opt.inner_merge_keys = 3;
    Crowtree t(opt);

    const int             K = 300;
    std::atomic<bool>     stop{false};
    std::atomic<long>     views{0};
    std::atomic<bool>     bad{false};
    std::atomic<uint64_t> max_slot_seen{0};

    std::vector<std::thread> viewers;
    viewers.reserve(4);
    for (int r = 0; r < 4; ++r) {
        viewers.emplace_back([&] {
            while (!stop.load(std::memory_order_relaxed) && !bad.load(std::memory_order_relaxed)) {
                auto snap = t.snapshot_view();
                if (snap == nullptr) {
                    bad.store(true);
                    return;
                }
                uint64_t    ceiling = max_slot_seen.load(std::memory_order_acquire);
                const auto &entries = snap->entries();
                for (size_t i = 0; i < entries.size(); ++i) {
                    if (i > 0 && !(Slice(entries[i - 1].key).compare(Slice(entries[i].key)) < 0)) {
                        bad.store(true); // not strictly increasing -> duplicate or out of order
                        return;
                    }
                    CellView v{Slice(entries[i].cell)};
                    // Every applied value/tombstone carries a slot that was
                    // real at some point; it can never exceed the highest
                    // slot any apply() call had returned from so far.
                    if (v.slot() > ceiling + 1) { // +1: benign race with the counter's own update below
                        bad.store(true);
                        return;
                    }
                    if (!v.is_tombstone() && (v.value().empty() || v.value().data()[0] != 'v')) {
                        bad.store(true); // corrupted value
                        return;
                    }
                }
                views.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::mt19937 rng(44);
    uint64_t     slot = 0;
    for (int step = 0; step < 6000; ++step) {
        ++slot;
        std::string key = make_key(static_cast<int>(rng() % K));
        if ((rng() % 4) == 0) {
            ASSERT_TRUE(t.apply(slot, Batch{{{.key = key, .kind = OpKind::kDelete, .value = ""}}}).ok());
        }
        else {
            std::string val = "v" + std::to_string(slot);
            ASSERT_TRUE(t.apply(slot, Batch{{{.key = key, .kind = OpKind::kPut, .value = val}}}).ok());
        }
        max_slot_seen.store(slot, std::memory_order_release);
        if ((rng() % 6) == 0) {
            ASSERT_TRUE(t.flush().ok());
        }
    }
    ASSERT_TRUE(t.flush().ok());

    stop.store(true);
    for (auto &th : viewers) {
        th.join();
    }
    EXPECT_FALSE(bad.load());
    EXPECT_GT(views.load(), 0);
}
