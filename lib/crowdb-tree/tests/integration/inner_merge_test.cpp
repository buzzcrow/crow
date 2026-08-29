// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Inner-node underflow merge: a delete-heavy workload must collapse the upper
// tree (merge underfull inner pages, dropping height) while preserving data,
// across reopen, and stay parity-correct vs an in-mem oracle.
#include "crowdb-tree/crowdb-tree.h"
#include "crowdb-tree/page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <map>
#include <memory>
#include <random>
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
    std::array<char, 16> b{};
    snprintf(b.data(), b.size(), "key%06d", i);
    return b.data();
}

Options tall_tree_opts(PageStore *s)
{
    Options o;
    o.page_store       = s;
    o.frame_bytes      = 4096;
    o.max_delta_len    = 1;   // consolidate every flush
    o.leaf_split_bytes = 160; // tiny leaves -> many of them
    o.leaf_merge_bytes = 60;
    o.inner_max_keys   = 8; // low fanout -> tall tree
    o.inner_merge_keys = 3; // merge inner pages below 3 separators
    return o;
}
} // namespace

TEST(InnerMerge, DeleteHeavyCollapsesTreeAndReopens)
{
    MemPageStore store(1);
    Options      opt = tall_tree_opts(&store);

    const int                          N = 600;
    std::map<std::string, std::string> oracle;
    int                                tall_height = 0;
    {
        Crowdbtree t(opt);
        uint64_t slot = 0;
        for (int i = 0; i < N; ++i) {
            ++slot;
            std::string v = "v" + std::to_string(i);
            ASSERT_TRUE(t.apply(slot, put_one(make_key(i), v)).ok());
            ASSERT_TRUE(t.flush().ok());
            oracle[make_key(i)] = v;
        }
        tall_height = t.height();
        ASSERT_GE(tall_height, 3); // genuinely multi-level

        // Allow tombstone GC so deleted leaves shrink + merge away.
        t.set_gc_watermark(1000000, 1000000);
        // Delete all but a sparse handful, driving leaf merges -> inner underflow.
        for (int i = 0; i < N; ++i) {
            if (i % 50 == 0) {
                continue; // keep ~12 keys
            }
            ++slot;
            ASSERT_TRUE(t.apply(slot, del_one(make_key(i))).ok());
            ASSERT_TRUE(t.flush().ok());
            oracle.erase(make_key(i));
        }

        int collapsed_height = t.height();
        EXPECT_LT(collapsed_height, tall_height) << "tree did not collapse";

        // Surviving keys read correctly; deleted keys are gone.
        for (int i = 0; i < N; ++i) {
            std::string v;
            uint64_t    s;
            bool        found = t.get(Slice(make_key(i)), &s, &v);
            if (i % 50 == 0) {
                ASSERT_TRUE(found) << "missing " << make_key(i);
                EXPECT_EQ(v, "v" + std::to_string(i));
            }
            else {
                EXPECT_FALSE(found) << "resurrected " << make_key(i);
            }
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok());
        EXPECT_FALSE(t.io_failed());
    }

    // Reopen: the collapsed tree round-trips.
    std::unique_ptr<Crowdbtree> t2;
    ASSERT_TRUE(Crowdbtree::open(opt, &t2).ok());
    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing after reopen " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
    // Spot-check a deleted key stays gone.
    std::string v;
    uint64_t    s;
    EXPECT_FALSE(t2->get(Slice(make_key(1)), &s, &v));
}

TEST(InnerMerge, RandomizedInsertDeleteParity)
{
    Options opt; // pure in-memory
    opt.frame_bytes      = 4096;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 200;
    opt.leaf_merge_bytes = 70;
    opt.inner_max_keys   = 8;
    opt.inner_merge_keys = 3;
    Crowdbtree t(opt);

    std::map<std::string, std::string> oracle;
    std::mt19937                       rng(424242);
    uint64_t                           slot = 0;
    // build up, then churn with a delete bias to force inner merges.
    for (int round = 0; round < 4000; ++round) {
        int         k   = static_cast<int>(rng() % 800);
        std::string key = make_key(k);
        ++slot;
        bool del = (round > 1500) ? (rng() % 3 != 0) : (rng() % 5 == 0);
        if (del) {
            ASSERT_TRUE(t.apply(slot, del_one(key)).ok());
            oracle.erase(key);
        }
        else {
            std::string val = "val" + std::to_string(slot);
            ASSERT_TRUE(t.apply(slot, put_one(key, val)).ok());
            oracle[key] = val;
        }
        if (round % 3 == 0) {
            ASSERT_TRUE(t.flush().ok());
        }
        if (round % 500 == 499) {
            t.set_gc_watermark(slot, slot);
        }
    }
    ASSERT_TRUE(t.flush().ok());

    for (int k = 0; k < 800; ++k) {
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
