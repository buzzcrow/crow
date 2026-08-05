// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// R6 perf gate: sync get/scan microbenchmark. Verifies the per-page
// refcount field (pin_state_ on PageBase) does not regress the sync hot
// path (which uses EBR only, no refcount). The gate catches accidental
// cacheline-padding regression from the new atomic field on PageBase.
//
// Build:
//   pixi run -- cmake -S crow-tree -B crow-tree/build-bench -DCROW_TREE_BENCH=ON \
//     -DCMAKE_BUILD_TYPE=Release
//   cmake --build crow-tree/build-bench -j
//   ./crow-tree/build-bench/crowtree_bench --benchmark_filter=ReadPath
#include "crow-tree/crow-tree.h"
#include "crow-tree/page_store.h"

#include <benchmark/benchmark.h>

#include <array>
#include <cstdio>
#include <string>
#include <vector>

using namespace crow::tree;

namespace
{
std::string make_key(int i)
{
    std::array<char, 16> buf{};
    snprintf(buf.data(), buf.size(), "k%05d", i);
    return buf.data();
}

// Build a tree with N keys, flushed + snapshotted (all pages resident + clean).
// Returns the tree and the keys for iteration.
void build_tree(benchmark::State &state, std::unique_ptr<Crowtree> *t, std::vector<std::string> *keys_out)
{
    int     n     = static_cast<int>(state.range(0));
    auto    store = std::make_shared<MemPageStore>(1);
    Options opt;
    opt.page_store       = store.get();
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    *t                   = std::make_unique<Crowtree>(opt);
    keys_out->clear();
    keys_out->reserve(static_cast<size_t>(n));
    for (int i = 0; i < n; ++i) {
        std::string k = make_key(i);
        std::string v = "val" + std::to_string(i);
        (*t)->apply(i + 1, Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}});
        keys_out->push_back(std::move(k));
    }
    (*t)->flush();
    (*t)->snapshot(nullptr);
}
} // namespace

// Sync point get on a resident L1 hit — the hot path R6 must not regress.
static void BM_ReadPath_GetHit(benchmark::State &state)
{
    std::unique_ptr<Crowtree> t;
    std::vector<std::string>  keys;
    build_tree(state, &t, &keys);
    for (auto _ : state) {
        uint64_t    s;
        std::string v;
        for (const auto &k : keys) {
            benchmark::DoNotOptimize(t->get(Slice(k), &s, &v));
        }
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(keys.size()));
}

BENCHMARK(BM_ReadPath_GetHit)->Arg(1000)->Arg(10000);

// Sync scan over the whole keyspace — the other hot path R6 must not regress.
static void BM_ReadPath_Scan(benchmark::State &state)
{
    std::unique_ptr<Crowtree> t;
    std::vector<std::string>  keys;
    build_tree(state, &t, &keys);
    for (auto _ : state) {
        std::vector<scan_entry> out;
        bool                    truncated = false;
        t->scan(Slice(), Slice(), 1000000, &out, &truncated);
        benchmark::DoNotOptimize(out);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(keys.size()));
}

BENCHMARK(BM_ReadPath_Scan)->Arg(1000)->Arg(10000);
