// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// CT14: randomized parity against a std::map oracle (in-order + out-of-order),
// exercising flush / consolidate / split / merge.
#include "crow-tree/crow-tree.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdio>
#include <map>
#include <random>
#include <string>
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

// compare the engine's live scan to the oracle (live key -> value).
void expect_parity(Crowtree &t, const std::map<std::string, std::string> &oracle)
{
    std::vector<scan_entry> out;
    bool                    trunc = false;
    ASSERT_TRUE(t.scan(Slice(""), Slice(), 0, 0, &out, &trunc).ok());
    ASSERT_FALSE(trunc);
    ASSERT_EQ(out.size(), oracle.size());
    size_t i = 0;
    for (const auto &kv : oracle) {
        EXPECT_EQ(out[i].key, kv.first);
        EXPECT_EQ(out[i].value, kv.second);
        ++i;
    }
}

Options small_tree()
{
    Options opt;
    opt.max_delta_len    = 2;
    opt.leaf_split_bytes = 160;
    opt.leaf_merge_bytes = 50;
    return opt;
}

} // namespace

TEST(Parity, InOrderRandomOpsWithPeriodicCompare)
{
    Crowtree                           t(small_tree());
    std::map<std::string, std::string> oracle;
    std::mt19937                       rng(2024);

    const int K    = 120;
    uint64_t  slot = 0;
    for (int step = 0; step < 20000; ++step) {
        ++slot;
        // Occasionally a multi-key batch at one slot (intra-batch last-wins).
        if (rng() % 10 == 0) {
            Batch                                               b;
            int                                                 n = 1 + static_cast<int>(rng() % 4);
            std::map<std::string, std::pair<bool, std::string>> batch_last; // key -> (isDelete, val)
            for (int o = 0; o < n; ++o) {
                std::string key = make_key(static_cast<int>(rng() % K));
                if ((rng() % 4) == 0) {
                    b.ops.push_back({.key = key, .kind = OpKind::kDelete, .value = ""});
                    batch_last[key] = {true, ""};
                }
                else {
                    std::string val = "v" + std::to_string(slot) + "_" + std::to_string(o);
                    b.ops.push_back({.key = key, .kind = OpKind::kPut, .value = val});
                    batch_last[key] = {false, val};
                }
            }
            ASSERT_TRUE(t.apply(slot, b).ok());
            for (auto &kv : batch_last) {
                if (kv.second.first) {
                    oracle.erase(kv.first);
                }
                else {
                    oracle[kv.first] = kv.second.second;
                }
            }
        }
        else {
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
        }

        if (rng() % 5 == 0) {
            ASSERT_TRUE(t.flush().ok());
        }
        if (step % 2000 == 1999) {
            ASSERT_TRUE(t.flush().ok());
            t.set_gc_watermark(slot, slot);
            expect_parity(t, oracle);
        }
    }
    ASSERT_TRUE(t.flush().ok());
    expect_parity(t, oracle);
}

TEST(Parity, OutOfOrderConvergesToHighestSlot)
{
    Crowtree t(small_tree());

    struct Op
    {
        uint64_t    slot;
        std::string key;
        bool        del;
        std::string value;
    };

    std::vector<Op> ops;
    std::mt19937    rng(777);
    const int       K = 100;
    const int       M = 8000;
    for (int i = 0; i < M; ++i) {
        uint64_t    slot = i + 1;
        std::string key  = make_key(static_cast<int>(rng() % K));
        bool        del  = ((rng() % 4) == 0);
        ops.push_back({.slot = slot, .key = key, .del = del, .value = del ? "" : ("v" + std::to_string(slot))});
    }

    // Oracle: highest-slot op per key wins.
    std::map<std::string, Op> winner;
    for (const auto &op : ops) {
        auto it = winner.find(op.key);
        if (it == winner.end() || op.slot > it->second.slot) {
            winner[op.key] = op;
        }
    }
    std::map<std::string, std::string> oracle;
    for (const auto &kv : winner) {
        if (!kv.second.del) {
            oracle[kv.first] = kv.second.value;
        }
    }

    // apply in shuffled order; contiguous stays 0 (nothing flushes), so all live
    // in L0 with highest-slot-wins. scan must already match the oracle.
    std::vector<Op> shuffled = ops;
    std::shuffle(shuffled.begin(), shuffled.end(), rng); // NOLINT(cert-msc50-cpp)
    for (const auto &op : shuffled) {
        Batch b{{{.key = op.key, .kind = op.del ? OpKind::kDelete : OpKind::kPut, .value = op.value}}};
        ASSERT_TRUE(t.apply(op.slot, b).ok());
    }
    expect_parity(t, oracle); // pre-flush (L0 only)

    // Now make everything contiguous and flush; state must still match.
    t.force_advance_slot(M);
    ASSERT_TRUE(t.flush().ok());
    EXPECT_EQ(t.last_applied_slot(), static_cast<uint64_t>(M));
    expect_parity(t, oracle);
}
