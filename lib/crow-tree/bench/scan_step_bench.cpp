// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Scan per-step profile: isolates where scan time goes for 64B vs 1KiB
// values, to identify the 1KiB anomaly's root cause. Builds a flushed L1-only
// tree (no L0) at each value size, runs N scans, and prints per-step
// avg/max via Crowtree::scan_profile(). An L0 variant leaves entries
// unflushed to measure the MemTable::snapshot() copy cost directly.
//
// Build:
//   pixi run -- cmake -S lib/crow-tree -B lib/crow-tree/build-bench \
//     -DCROW_TREE_BENCH=ON -DCMAKE_BUILD_TYPE=Release
//   pixi run -- cmake --build lib/crow-tree/build-bench -j
//   ./lib/crow-tree/build-bench/scan_step_profile
#include "crow-tree/crow-tree.h"
#include "crow-tree/page_store.h"

#include <cstdio>
#include <string>
#include <vector>

using namespace crow::tree;

namespace
{
std::string make_key(int i)
{
    std::string s(21, '\0');
    snprintf(s.data(), s.size(), "k%020d", i);
    s.resize(strlen(s.c_str()));
    return s;
}

std::string make_value(int value_size, int i)
{
    std::string v(static_cast<size_t>(value_size), 'v');
    // Vary the tail so values are not identical (avoids any dedup shortcut).
    if (v.size() >= 12) {
        snprintf(v.data() + v.size() - 12, 12, "%011d", i);
    }
    return v;
}

struct Setup
{
    std::unique_ptr<Crowtree>     tree;
    std::shared_ptr<MemPageStore> store;
};

// Build a tree with `n` keys at `value_size`. If `flush`, drain everything
// into L1 so L0 is empty -- isolates the L1 scan path. `flush_only` does a
// flush without snapshot (leaves retain delta chains, matching production);
// otherwise snapshot() consolidates leaves into clean bases. If not flush,
// leave all entries in L0 (active memtable) -- isolates the L0 snapshot cost.
// `flush_every` (>0) flushes incrementally every that many keys, mimicking the
// production maintenance loop's periodic flush (shaping the tree differently
// from a single end-of-load flush).
Setup build_tree(int n, int value_size, bool flush, bool flush_only = false, int flush_every = 0)
{
    Setup s;
    s.store = std::make_shared<MemPageStore>(1);
    Options opt;
    opt.page_store       = s.store.get();
    opt.frame_bytes      = 64 * 1024;
    opt.leaf_split_bytes = 64 * 1024;
    s.tree               = std::make_unique<Crowtree>(opt);
    s.tree->init_metrics("s.0.g.0");
    for (int i = 0; i < n; ++i) {
        std::string k = make_key(i);
        std::string v = make_value(value_size, i);
        s.tree->apply(i + 1, Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}});
        if (flush_every > 0 && flush && (i + 1) % flush_every == 0) {
            s.tree->flush();
        }
    }
    if (flush) {
        s.tree->flush();
        if (!flush_only) {
            s.tree->snapshot(nullptr);
        }
    }
    return s;
}

void print_profile(const char *label, const ScanProfile &p)
{
    auto us = [](uint64_t ns) { return static_cast<double>(ns) / 1000.0; };
    std::printf("  %-14s count=%llu entries=%llu total_avg=%.1fus\n", label, static_cast<unsigned long long>(p.count),
                static_cast<unsigned long long>(p.entries), us(p.total.avg_ns));
    std::printf("    %-18s avg=%7.1fus  max=%8.1fus  sum=%9.1fus\n", "l0_snapshot", us(p.l0_snapshot.avg_ns),
                us(p.l0_snapshot.max_ns), us(p.l0_snapshot.sum_ns));
    std::printf("    %-18s avg=%7.1fus  max=%8.1fus  sum=%9.1fus\n", "l0_skip", us(p.l0_skip.avg_ns),
                us(p.l0_skip.max_ns), us(p.l0_skip.sum_ns));
    std::printf("    %-18s avg=%7.1fus  max=%8.1fus  sum=%9.1fus\n", "l1_descent", us(p.l1_descent.avg_ns),
                us(p.l1_descent.max_ns), us(p.l1_descent.sum_ns));
    std::printf("    %-18s avg=%7.1fus  max=%8.1fus  sum=%9.1fus\n", "l1_resolve", us(p.l1_resolve.avg_ns),
                us(p.l1_resolve.max_ns), us(p.l1_resolve.sum_ns));
    std::printf("    %-18s avg=%7.1fus  max=%8.1fus  sum=%9.1fus\n", "merge", us(p.merge.avg_ns), us(p.merge.max_ns),
                us(p.merge.sum_ns));
}

// Run `iters` scans (limit, from start) and print the per-step profile.
void run_scenario(const char *label, int n, int value_size, bool flush, size_t limit, int iters,
                  bool flush_only = false, int flush_every = 0)
{
    Setup s = build_tree(n, value_size, flush, flush_only, flush_every);
    (void)s.tree->scan_profile(); // reset window
    std::vector<scan_entry> out;
    bool                    truncated = false;
    for (int i = 0; i < iters; ++i) {
        out.clear();
        truncated = false;
        s.tree->scan(Slice(), Slice(), limit, &out, &truncated);
    }
    ScanProfile p = s.tree->scan_profile();
    std::printf("\n[%s] n=%d val=%dB limit=%zu L0=%s iters=%d\n", label, n, value_size, limit, flush ? "empty" : "full",
                iters);
    print_profile("per-scan", p);
    if (p.count > 0) {
        std::printf("    leaves-touched-est: l1_resolve_avg / total_avg = %.1f%%\n",
                    100.0 * static_cast<double>(p.l1_resolve.sum_ns) / static_cast<double>(p.total.sum_ns));
        std::printf("    merge-est:           merge_avg / total_avg    = %.1f%%\n",
                    100.0 * static_cast<double>(p.merge.sum_ns) / static_cast<double>(p.total.sum_ns));
        std::printf("    l0-snapshot-est:     l0_snapshot_avg/total    = %.1f%%\n",
                    100.0 * static_cast<double>(p.l0_snapshot.sum_ns) / static_cast<double>(p.total.sum_ns));
    }
}
} // namespace

int main()
{
    constexpr int N     = 100000;
    constexpr int ITERS = 2000;

    std::printf("=== Scan per-step profile ===\n");
    std::printf("Isolates L1-only path (flushed) vs L0-full path, 64B vs 1KiB.\n");

    // L1-only (flushed): isolates the L1 B+tree scan path. If 64B ~= 1KiB
    // here, the L1 path is NOT the anomaly's cause.
    run_scenario("L1-only 64B", N, 64, true, 1000, ITERS);
    run_scenario("L1-only 1KiB", N, 1024, true, 1000, ITERS);

    // Flush-only (no snapshot): leaves retain delta chains (base + BatchDelta
    // per flush), matching the production tree shape where the maintenance
    // loop flushes periodically but snapshot() does not run before measurement.
    run_scenario("L1-flushonly 64B", N, 64, true, 1000, ITERS, /*flush_only=*/true);
    run_scenario("L1-flushonly 1KiB", N, 1024, true, 1000, ITERS, true);

    // Incremental flush (mimics the production maintenance loop's periodic
    // flush every ~30k keys): shapes the tree differently from a single
    // end-of-load flush. Confirms whether tree shape explains the production
    // 64B l1_resolve cost (3985us) vs the single-flush microbench (1170us).
    run_scenario("L1-incremental 64B", N, 64, true, 1000, ITERS, true, 30000);
    run_scenario("L1-incremental 1KiB", N, 1024, true, 1000, ITERS, true, 30000);

    // L0-full (unflushed): all entries in the active memtable. Measures the
    // MemTable::snapshot() O(N_l0) copy cost directly.
    run_scenario("L0-full 64B", N, 64, false, 1000, ITERS);
    run_scenario("L0-full 1KiB", N, 1024, false, 1000, ITERS);

    // Vary limit on the L1-only path to see per-entry vs per-scan scaling.
    run_scenario("L1-only 64B limit=10", N, 64, true, 10, ITERS);
    run_scenario("L1-only 1KiB limit=10", N, 1024, true, 10, ITERS);
    run_scenario("L1-only 64B limit=10000", N, 64, true, 10000, ITERS);
    run_scenario("L1-only 1KiB limit=10000", N, 1024, true, 10000, ITERS);

    std::printf("\n=== done ===\n");
    return 0;
}
