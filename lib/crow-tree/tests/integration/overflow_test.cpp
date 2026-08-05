// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// PT11: overflow pages. Large values spill out of leaves into fixed-size
// overflow frame chains; leaves keep small pointer cells. Covers multi-frame
// chains, get/scan/delete, reopen, eviction-reload, overwrite (chain retire),
// and parity vs an in-mem oracle.
#include "crow-tree/crow-tree.h"
#include "crow-tree/page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <vector>

using namespace crow::tree;

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

// Deterministic value of `n` bytes seeded by `seed` (so reopen comparisons are
// exact and content varies across keys).
std::string big_val(size_t n, uint32_t seed)
{
    std::mt19937 rng(seed);
    std::string  s;
    s.resize(n);
    for (auto &c : s) {
        c = static_cast<char>('A' + static_cast<int>(rng() % 26));
    }
    return s;
}

Options overflow_opts(PageStore *store)
{
    Options opt;
    opt.page_store       = store;
    opt.frame_bytes      = 4096; // chunk cap ~4024 -> easy multi-frame chains
    opt.max_inline_value = 64;   // spill anything bigger than 64 bytes
    opt.max_delta_len    = 1;    // consolidate (and thus spill) on each flush
    opt.leaf_split_bytes = 1024; // force multiple leaves
    return opt;
}
} // namespace

TEST(Overflow, PutGetScanReopenMultiFrame)
{
    MemPageStore store(1);
    Options      opt = overflow_opts(&store);

    // Sizes spanning chain boundaries: <1 chunk, exactly 1, just over 1, several,
    // and a multi-MiB value.
    std::vector<size_t>                sizes = {100, 4024, 4025, 10000, 1U << 20};
    std::map<std::string, std::string> oracle;
    {
        Crowtree t(opt);
        uint64_t slot = 0;
        for (size_t i = 0; i < sizes.size(); ++i) {
            ++slot;
            std::string v = big_val(sizes[i], static_cast<uint32_t>(i + 1));
            ASSERT_TRUE(t.apply(slot, put_one(make_key(static_cast<int>(i)), v)).ok());
            ASSERT_TRUE(t.flush().ok());
            oracle[make_key(static_cast<int>(i))] = v;
        }
        // Some small inline values interleaved.
        for (int i = 100; i < 130; ++i) {
            ++slot;
            ASSERT_TRUE(t.apply(slot, put_one(make_key(i), "small" + std::to_string(i))).ok());
            ASSERT_TRUE(t.flush().ok());
            oracle[make_key(i)] = "small" + std::to_string(i);
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok());

        for (const auto &kv : oracle) {
            std::string v;
            uint64_t    s;
            ASSERT_TRUE(t.get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
            EXPECT_EQ(v, kv.second) << "value mismatch " << kv.first;
        }
        // scan returns materialized (assembled) values.
        std::vector<scan_entry> out;
        bool                    trunc = false;
        ASSERT_TRUE(t.scan(Slice(), Slice(), 0, &out, &trunc).ok());
        EXPECT_EQ(out.size(), oracle.size());
        for (const auto &e : out) {
            EXPECT_EQ(e.value, oracle[e.key]);
        }
    }

    // Reopen: overflow chains demand-load through resident on first access.
    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing after reopen " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
}

TEST(Overflow, EvictionReload)
{
    MemPageStore store(1);
    Options      opt = overflow_opts(&store);
    Crowtree     t(opt);

    std::map<std::string, std::string> oracle;
    uint64_t                           slot = 0;
    for (int i = 0; i < 20; ++i) {
        ++slot;
        std::string v = big_val(6000, static_cast<uint32_t>(i + 1)); // ~2 chunks each
        ASSERT_TRUE(t.apply(slot, put_one(make_key(i), v)).ok());
        ASSERT_TRUE(t.flush().ok());
        oracle[make_key(i)] = v;
    }
    ASSERT_TRUE(t.snapshot(nullptr).ok());

    (void)t.evict_clean_leaves(0); // drop all clean leaves; reads must reload them
    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t.get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
}

TEST(Overflow, OverwriteAndDeleteRetiresChains)
{
    MemPageStore store(1);
    Options      opt = overflow_opts(&store);
    {
        Crowtree          t(opt);
        uint64_t          slot = 0;
        const std::string k    = make_key(1);

        std::string v1 = big_val(8000, 1);
        ++slot;
        ASSERT_TRUE(t.apply(slot, put_one(k, v1)).ok());
        ASSERT_TRUE(t.flush().ok());
        ASSERT_TRUE(t.snapshot(nullptr).ok());

        // Overwrite large -> large (old overflow chain superseded + retired).
        std::string v2 = big_val(12000, 2);
        ++slot;
        ASSERT_TRUE(t.apply(slot, put_one(k, v2)).ok());
        ASSERT_TRUE(t.flush().ok());
        {
            std::string v;
            uint64_t    s;
            ASSERT_TRUE(t.get(Slice(k), &s, &v));
            EXPECT_EQ(v, v2);
        }

        // Overwrite large -> small (chain retired, value goes inline).
        ++slot;
        ASSERT_TRUE(t.apply(slot, put_one(k, "tiny")).ok());
        ASSERT_TRUE(t.flush().ok());
        {
            std::string v;
            uint64_t    s;
            ASSERT_TRUE(t.get(Slice(k), &s, &v));
            EXPECT_EQ(v, "tiny");
        }

        // Put large again, then delete it.
        std::string v3 = big_val(9000, 3);
        ++slot;
        ASSERT_TRUE(t.apply(slot, put_one(k, v3)).ok());
        ASSERT_TRUE(t.flush().ok());
        ++slot;
        ASSERT_TRUE(t.apply(slot, del_one(k)).ok());
        ASSERT_TRUE(t.flush().ok());
        ASSERT_TRUE(t.snapshot(nullptr).ok());
        std::string v;
        uint64_t    s;
        EXPECT_FALSE(t.get(Slice(k), &s, &v));
    }
    // Reopen sees the deleted key gone.
    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
    std::string v;
    uint64_t    s;
    EXPECT_FALSE(t2->get(Slice(make_key(1)), &s, &v));
}

TEST(Overflow, ParityVsOracle)
{
    // Pure in-memory engine; overflow chains live in the pool/heap.
    Options opt;
    opt.frame_bytes      = 4096;
    opt.max_inline_value = 48;
    opt.max_delta_len    = 2;
    opt.leaf_split_bytes = 1024;
    Crowtree t(opt);

    std::map<std::string, std::string> oracle;
    std::mt19937                       rng(99);
    uint64_t                           slot = 0;
    for (int round = 0; round < 400; ++round) {
        int         k   = static_cast<int>(rng() % 40);
        std::string key = make_key(k);
        ++slot;
        if ((rng() % 7) == 0) {
            ASSERT_TRUE(t.apply(slot, del_one(key)).ok());
            oracle.erase(key);
        }
        else {
            size_t n =
                ((rng() % 3) == 0) ? (200 + static_cast<size_t>(rng() % 9000)) : (1 + static_cast<size_t>(rng() % 40));
            std::string v = big_val(n, static_cast<uint32_t>(slot));
            ASSERT_TRUE(t.apply(slot, put_one(key, v)).ok());
            oracle[key] = v;
        }
        if (round % 5 == 0) {
            ASSERT_TRUE(t.flush().ok());
        }
    }
    ASSERT_TRUE(t.flush().ok());

    for (int k = 0; k < 40; ++k) {
        std::string key = make_key(k);
        std::string v;
        uint64_t    s;
        bool        found = t.get(Slice(key), &s, &v);
        auto        it    = oracle.find(key);
        if (it == oracle.end()) {
            EXPECT_FALSE(found) << "unexpected " << key;
        }
        else {
            ASSERT_TRUE(found) << "missing " << key;
            EXPECT_EQ(v, it->second) << "mismatch " << key;
        }
    }
}
