// PT12: in-frame delta region (opt-in). Verifies correctness (reads overlay
// in-frame deltas, fold at cap, reopen-equals, parity vs oracle) and a small
// microbenchmark vs plain COW-rebuild (both must agree).
#include "crowtree/crowtree.h"
#include "crowtree/page_store.h"

#include <gtest/gtest.h>

#include <chrono>
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
} // namespace

TEST(InFrameDelta, ReadOverlayAndFoldReopen)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store        = &store;
    opt.frame_bytes       = 4096;
    opt.inframe_delta     = true;
    opt.max_inframe_delta = 4;    // small cap -> folds frequently
    opt.leaf_split_bytes  = 1024; // keep multiple leaves

    std::map<std::string, std::string> oracle;
    uint64_t                           slot = 0;
    {
        Crowtree t(opt);
        // Seed a base, then stream small single-key flushes (the in-frame fast path).
        for (int i = 0; i < 60; ++i) {
            ++slot;
            std::string v = "v" + std::to_string(i);
            ASSERT_TRUE(t.apply(slot, put_one(make_key(i), v)).ok());
            ASSERT_TRUE(t.flush().ok());
            oracle[make_key(i)] = v;
        }
        // Overwrites + deletes (exercise overlay shadowing + tombstone deltas).
        for (int i = 0; i < 60; i += 3) {
            ++slot;
            std::string v = "up" + std::to_string(i);
            ASSERT_TRUE(t.apply(slot, put_one(make_key(i), v)).ok());
            ASSERT_TRUE(t.flush().ok());
            oracle[make_key(i)] = v;
        }
        for (int i = 1; i < 60; i += 7) {
            ++slot;
            ASSERT_TRUE(t.apply(slot, del_one(make_key(i))).ok());
            ASSERT_TRUE(t.flush().ok());
            oracle.erase(make_key(i));
        }

        for (const auto &kv : oracle) {
            std::string v;
            uint64_t    s;
            ASSERT_TRUE(t.get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
            EXPECT_EQ(v, kv.second);
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok());
    }

    // Reopen: any in-frame deltas persisted in leaf frames overlay on read.
    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing after reopen " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
    // Deleted keys stay gone.
    std::string v;
    uint64_t    s;
    EXPECT_FALSE(t2->get(Slice(make_key(1)), &s, &v));
}

TEST(InFrameDelta, ParityWithDefaultMode)
{
    // The same op stream must produce identical state with in-frame deltas on and
    // off (in-frame deltas are a pure performance variant).
    auto run = [](bool inframe) {
        Options opt;
        opt.frame_bytes       = 4096;
        opt.inframe_delta     = inframe;
        opt.max_inframe_delta = 6;
        opt.leaf_split_bytes  = 1024;
        Crowtree     t(opt);
        std::mt19937 rng(2024);
        uint64_t     slot = 0;
        for (int r = 0; r < 600; ++r) {
            int k = static_cast<int>(rng() % 50);
            ++slot;
            if (rng() % 8 == 0) {
                EXPECT_TRUE(t.apply(slot, del_one(make_key(k))).ok());
            }
            else {
                EXPECT_TRUE(t.apply(slot, put_one(make_key(k), "val" + std::to_string(slot))).ok());
            }
            if (r % 3 == 0) {
                EXPECT_TRUE(t.flush().ok());
            }
        }
        EXPECT_TRUE(t.flush().ok());
        return t.snapshot_view();
    };
    auto a = run(false);
    auto b = run(true);
    EXPECT_TRUE(a->compare(*b).empty());
    EXPECT_EQ(a->size(), b->size());
}

TEST(InFrameDelta, MicrobenchVsCowRebuild)
{
    // Tiny single-key flushes: in-frame deltas avoid a full sorted rebuild each
    // time. We assert both modes agree and report timings (not a hard perf gate).
    auto bench = [](bool inframe) {
        Options opt;
        opt.frame_bytes       = 8192;
        opt.inframe_delta     = inframe;
        opt.max_inframe_delta = 16;
        opt.leaf_split_bytes  = 1U << 20; // single leaf: isolate the overlay vs rebuild cost
        Crowtree t(opt);
        uint64_t slot = 0;
        // Pre-populate a sizeable base so a rebuild is non-trivial.
        for (int i = 0; i < 300; ++i) {
            ++slot;
            EXPECT_TRUE(t.apply(slot, put_one(make_key(i), "seed")).ok());
        }
        EXPECT_TRUE(t.flush().ok());
        auto t0 = std::chrono::steady_clock::now();
        for (int r = 0; r < 2000; ++r) {
            ++slot;
            EXPECT_TRUE(t.apply(slot, put_one(make_key(r % 300), "v" + std::to_string(slot))).ok());
            EXPECT_TRUE(t.flush().ok()); // one tiny group per flush
        }
        auto   t1 = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        return std::make_pair(ms, t.snapshot_view());
    };
    auto [cow_ms, cow] = bench(false);
    auto [inf_ms, inf] = bench(true);
    EXPECT_TRUE(cow->compare(*inf).empty());
    fprintf(stderr, "[inframe-bench] cow-rebuild=%.1fms in-frame=%.1fms\n", cow_ms, inf_ms);
    SUCCEED();
}
