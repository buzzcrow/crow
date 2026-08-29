// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// MemTable ordered-container microbenchmark (plan-tree #9 / Q2).
//
// Compares the containers considered for the MemTable (L0) against the workload
// that matters: point-get (hit/miss), insert-fill, and ordered drain (what
// flush() does). Candidates:
//   - std::map           (the pre-#9 red-black tree baseline)
//   - absl::btree_map    (the current MemTable choice, #9)
//   - folly::ConcurrentSkipList (optional; built when folly is found)
//
// Build (opt-in, needs Google Benchmark):
//   pixi run -- cmake -S crowdb-tree -B crowdb-tree/build-bench -DCROWDB_TREE_BENCH=ON \
//     -DCMAKE_BUILD_TYPE=Release
//   cmake --build crowdb-tree/build-bench -j
//   ./crowdb-tree/build-bench/crowtree_bench
//
// The MemTable stores key -> encoded cell; here we model that as fixed-size 16 B
// keys -> ~48 B values, uniformly random (so tree/skiplist ordering is exercised
// without key-locality helping any one structure).
#include <absl/container/btree_map.h>
#include <benchmark/benchmark.h>

#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <random>
#include <string>
#include <vector>

#ifdef CROWDB_TREE_BENCH_FOLLY
#    include <folly/ConcurrentSkipList.h>
#endif

namespace
{

// Deterministic random 16-byte keys (seeded), generated once per size.
const std::vector<std::string> &keys(size_t n)
{
    static std::map<size_t, std::vector<std::string>> cache;
    auto                                              it = cache.find(n);
    if (it != cache.end()) {
        return it->second;
    }
    std::mt19937_64          rng(0x9E3779B97F4A7C15ull ^ n);
    std::vector<std::string> ks;
    ks.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        uint64_t    a = rng();
        uint64_t    b = rng();
        std::string s(16, '\0');
        std::memcpy(s.data(), &a, 8);
        std::memcpy(s.data() + 8, &b, 8);
        ks.push_back(std::move(s));
    }
    auto &out = cache.emplace(n, std::move(ks)).first->second;
    return out;
}

// Keys guaranteed absent from keys(n) (flip the high bit of the seed space).
std::vector<std::string> miss_keys(size_t n)
{
    std::mt19937_64          rng(0xD1B54A32D192ED03ull ^ n);
    std::vector<std::string> ks;
    ks.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        uint64_t    a = rng() | (1ull << 63);
        uint64_t    b = rng();
        std::string s(16, '\0');
        std::memcpy(s.data(), &a, 8);
        std::memcpy(s.data() + 8, &b, 8);
        ks.push_back(std::move(s));
    }
    return ks;
}

const std::string kVal(48, 'v');

// ── std::map / absl::btree_map share the ordered-map API ──────────

template <class MapT> void fill(MapT &m, const std::vector<std::string> &ks)
{
    for (const auto &k : ks) {
        m.emplace(k, kVal);
    }
}

template <class MapT> void BM_Insert(benchmark::State &state)
{
    const auto &ks = keys(static_cast<size_t>(state.range(0)));
    for (auto _ : state) {
        MapT m;
        fill(m, ks);
        benchmark::DoNotOptimize(&m);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

template <class MapT> void BM_GetHit(benchmark::State &state)
{
    const auto &ks = keys(static_cast<size_t>(state.range(0)));
    MapT        m;
    fill(m, ks);
    for (auto _ : state) {
        for (const auto &k : ks) {
            auto it = m.find(k);
            benchmark::DoNotOptimize(it);
        }
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

template <class MapT> void BM_GetMiss(benchmark::State &state)
{
    const auto &ks     = keys(static_cast<size_t>(state.range(0)));
    auto        absent = miss_keys(static_cast<size_t>(state.range(0)));
    MapT        m;
    fill(m, ks);
    for (auto _ : state) {
        for (const auto &k : absent) {
            auto it = m.find(k);
            benchmark::DoNotOptimize(it);
        }
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

template <class MapT> void BM_OrderedScan(benchmark::State &state)
{
    const auto &ks = keys(static_cast<size_t>(state.range(0)));
    MapT        m;
    fill(m, ks);
    for (auto _ : state) {
        size_t bytes = 0;
        for (const auto &kv : m) {
            bytes += kv.first.size();
            benchmark::DoNotOptimize(kv.first.data());
        }
        benchmark::DoNotOptimize(bytes);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

using StdMap  = std::map<std::string, std::string, std::less<>>;
using AbslMap = absl::btree_map<std::string, std::string, std::less<>>;

} // namespace

#define REGISTER_MAP(fn, MapT) BENCHMARK_TEMPLATE(fn, MapT)->Arg(1000)->Arg(100000)

REGISTER_MAP(BM_Insert, StdMap);
REGISTER_MAP(BM_Insert, AbslMap);
REGISTER_MAP(BM_GetHit, StdMap);
REGISTER_MAP(BM_GetHit, AbslMap);
REGISTER_MAP(BM_GetMiss, StdMap);
REGISTER_MAP(BM_GetMiss, AbslMap);
REGISTER_MAP(BM_OrderedScan, StdMap);
REGISTER_MAP(BM_OrderedScan, AbslMap);

// ── folly::ConcurrentSkipList (optional) ──────────────────────────
#ifdef CROWDB_TREE_BENCH_FOLLY
namespace
{

struct Entry
{
    std::string key;
    std::string val;
};

struct EntryLess
{
    bool operator()(const Entry &a, const Entry &b) const
    {
        return a.key < b.key;
    }
};

using CSL = folly::ConcurrentSkipList<Entry, EntryLess>;

void BM_Insert_Folly(benchmark::State &state)
{
    const auto &ks = keys(static_cast<size_t>(state.range(0)));
    for (auto _ : state) {
        auto          sl = CSL::createInstance(/*height=*/1);
        CSL::Accessor acc(sl);
        for (const auto &k : ks) {
            acc.insert(Entry{k, kVal});
        }
        benchmark::DoNotOptimize(&acc);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_GetHit_Folly(benchmark::State &state)
{
    const auto   &ks = keys(static_cast<size_t>(state.range(0)));
    auto          sl = CSL::createInstance(1);
    CSL::Accessor acc(sl);
    for (const auto &k : ks) {
        acc.insert(Entry{k, kVal});
    }
    for (auto _ : state) {
        for (const auto &k : ks) {
            auto it = acc.find(Entry{k, std::string()});
            benchmark::DoNotOptimize(it);
        }
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_GetMiss_Folly(benchmark::State &state)
{
    const auto   &ks     = keys(static_cast<size_t>(state.range(0)));
    auto          absent = miss_keys(static_cast<size_t>(state.range(0)));
    auto          sl     = CSL::createInstance(1);
    CSL::Accessor acc(sl);
    for (const auto &k : ks) {
        acc.insert(Entry{k, kVal});
    }
    for (auto _ : state) {
        for (const auto &k : absent) {
            auto it = acc.find(Entry{k, std::string()});
            benchmark::DoNotOptimize(it);
        }
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_OrderedScan_Folly(benchmark::State &state)
{
    const auto   &ks = keys(static_cast<size_t>(state.range(0)));
    auto          sl = CSL::createInstance(1);
    CSL::Accessor acc(sl);
    for (const auto &k : ks) {
        acc.insert(Entry{k, kVal});
    }
    for (auto _ : state) {
        size_t bytes = 0;
        for (auto it = acc.begin(); it != acc.end(); ++it) {
            bytes += it->key.size();
            benchmark::DoNotOptimize(it->key.data());
        }
        benchmark::DoNotOptimize(bytes);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

} // namespace

BENCHMARK(BM_Insert_Folly)->Arg(1000)->Arg(100000);
BENCHMARK(BM_GetHit_Folly)->Arg(1000)->Arg(100000);
BENCHMARK(BM_GetMiss_Folly)->Arg(1000)->Arg(100000);
BENCHMARK(BM_OrderedScan_Folly)->Arg(1000)->Arg(100000);
#endif // CROWDB_TREE_BENCH_FOLLY

BENCHMARK_MAIN();
