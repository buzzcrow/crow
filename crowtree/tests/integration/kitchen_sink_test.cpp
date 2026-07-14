// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Combined stress: compression + overflow + in-frame deltas + a small buffer
// pool (forces eviction) + periodic snapshots, validated against an in-mem
// oracle live and after reopen. Plus a focused test that an overflow chain whose
// pages were evicted is still fully retired on overwrite (no leak; ASan covers).
#include "crowtree/crowtree.h"
#include "crowtree/page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <map>
#include <memory>
#include <random>
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
    std::array<char, 16> b{};
    snprintf(b.data(), b.size(), "k%04d", i);
    return b.data();
}

std::string make_val(size_t n, uint32_t seed)
{
    std::mt19937 rng(seed);
    std::string  s(n, 0);
    for (auto &c : s) {
        c = static_cast<char>('a' + (rng() % 26));
    }
    return s;
}
} // namespace

TEST(KitchenSink, AllFeaturesRandomizedReopen)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store        = &store;
    opt.compression       = compress_algo::kLz4;
    opt.frame_bytes       = 4096;
    opt.buffer_pool_bytes = static_cast<size_t>(64) * 1024; // ~16 frames -> eviction under pressure
    opt.max_inline_value  = 80;                             // mix inline + overflow
    opt.inframe_delta     = true;
    opt.max_inframe_delta = 6;
    opt.max_delta_len     = 3;
    opt.leaf_split_bytes  = 1024;

    std::map<std::string, std::string> oracle;
    std::mt19937                       rng(20260701);
    uint64_t                           slot = 0;
    {
        Crowtree t(opt);
        for (int round = 0; round < 1500; ++round) {
            int         k   = static_cast<int>(rng() % 80);
            std::string key = make_key(k);
            ++slot;
            if ((rng() % 9) == 0) {
                ASSERT_TRUE(t.apply(slot, del_one(key)).ok());
                oracle.erase(key);
            }
            else {
                // ~1/3 large (overflow), else small (inline / in-frame delta).
                size_t      n = ((rng() % 3) == 0) ? (200 + static_cast<size_t>(rng() % 9000))
                                                   : (1 + static_cast<size_t>(rng() % 60));
                std::string v = make_val(n, static_cast<uint32_t>(slot));
                ASSERT_TRUE(t.apply(slot, put_one(key, v)).ok());
                oracle[key] = v;
            }
            if (round % 4 == 0) {
                ASSERT_TRUE(t.flush().ok());
            }
            if (round % 250 == 0) {
                ASSERT_TRUE(t.snapshot(nullptr).ok());
            }
        }
        ASSERT_TRUE(t.flush().ok());
        ASSERT_TRUE(t.snapshot(nullptr).ok());
        EXPECT_FALSE(t.io_failed());

        // Live parity.
        for (int k = 0; k < 80; ++k) {
            std::string v;
            uint64_t    s;
            bool        found = t.get(Slice(make_key(k)), &s, &v);
            auto        it    = oracle.find(make_key(k));
            if (it == oracle.end()) {
                EXPECT_FALSE(found) << "unexpected " << make_key(k);
            }
            else {
                ASSERT_TRUE(found) << "missing " << make_key(k);
                EXPECT_EQ(v, it->second) << "mismatch " << make_key(k);
            }
        }
    }

    // Reopen parity (lazy recovery + demand-load + decompress + overflow assemble).
    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing after reopen " << kv.first;
        EXPECT_EQ(v, kv.second) << "reopen mismatch " << kv.first;
    }
    EXPECT_FALSE(t2->io_failed());
}

TEST(KitchenSink, OverwriteEvictedOverflowChainNoLeak)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.frame_bytes      = 4096;
    opt.max_inline_value = 64;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 1024;
    Crowtree t(opt);

    const std::string k    = make_key(1);
    uint64_t          slot = 0;
    ++slot;
    ASSERT_TRUE(t.apply(slot, put_one(k, make_val(12000, 1))).ok()); // ~3 overflow frames
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot(nullptr).ok());

    // Evict everything: the old overflow chain's pages become unloaded.
    (void)t.evict_clean_leaves(0);

    // Overwrite the key: consolidation supersedes the old (evicted) overflow chain,
    // which retire_overflow_chain_locked must demand-load to fully retire.
    ++slot;
    ASSERT_TRUE(t.apply(slot, put_one(k, make_val(9000, 2))).ok());
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot(nullptr).ok());

    std::string v;
    uint64_t    s;
    ASSERT_TRUE(t.get(Slice(k), &s, &v));
    EXPECT_EQ(v, make_val(9000, 2));
    EXPECT_FALSE(t.io_failed());
}
