// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Scan per-step profile: isolates where scan time goes for 64B vs 1KiB
// values, to identify the 1KiB anomaly's root cause. Builds a flushed L1-only
// tree (no L0) at each value size, runs N scans, and prints per-step
// avg/max via Crowdbtree::scan_profile(). An L0 variant leaves entries
// unflushed to measure the L0 cursor cost directly.
//
// R50 Gate 2: concurrent write+scan scenarios measure scan latency under a
// non-empty L0 at scan time — sustained writes with a periodic flush
// (production-like) and a flush backlog (upper bound). See
// doc/backlog/R50-epoch-protected-memtable.md Gate 2.
//
// Build:
//   pixi run -- cmake -S lib/crowdb-tree -B lib/crowdb-tree/build-bench \
//     -DCROWDB_TREE_BENCH=ON -DCMAKE_BUILD_TYPE=Release
//   pixi run -- cmake --build lib/crowdb-tree/build-bench -j
//   ./lib/crowdb-tree/build-bench/scan_step_profile
#include "crowdb-tree/crowdb-tree.h"
#include "crowdb-tree/page_store.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

using namespace crowdb::tree;

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
    std::unique_ptr<Crowdbtree>   tree;
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
    s.tree               = std::make_unique<Crowdbtree>(opt);
    s.tree->init_metrics("s.0.g.0", "");
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
    std::printf("    %-18s avg=%7.1fus  max=%8.1fus  sum=%9.1fus\n", "l0", us(p.l0.avg_ns), us(p.l0.max_ns),
                us(p.l0.sum_ns));
    std::printf("    %-18s avg=%7.1fus  max=%8.1fus  sum=%9.1fus\n", "l1", us(p.l1.avg_ns), us(p.l1.max_ns),
                us(p.l1.sum_ns));
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
        s.tree->scan(Slice(), Slice(), Slice(), limit, 0, false, 0, &out, &truncated);
    }
    ScanProfile p = s.tree->scan_profile();
    std::printf("\n[%s] n=%d val=%dB limit=%zu L0=%s iters=%d\n", label, n, value_size, limit, flush ? "empty" : "full",
                iters);
    print_profile("per-scan", p);
    if (p.count > 0) {
        std::printf("    l0-est:              l0_avg / total_avg        = %.1f%%\n",
                    100.0 * static_cast<double>(p.l0.sum_ns) / static_cast<double>(p.total.sum_ns));
        std::printf("    l1-est:              l1_avg / total_avg        = %.1f%%\n",
                    100.0 * static_cast<double>(p.l1.sum_ns) / static_cast<double>(p.total.sum_ns));
        std::printf("    merge-est:           merge_avg / total_avg    = %.1f%%\n",
                    100.0 * static_cast<double>(p.merge.sum_ns) / static_cast<double>(p.total.sum_ns));
    }
}

// R50 Gate 2: concurrent write+scan. Pre-populates L1 (flush+snapshot),
// then runs a writer thread applying upserts at monotonically increasing
// slots while the main thread scans for `duration_secs`. An optional flush
// thread calls flush() every `flush_every_ms` (mimicking the production
// maintenance tick). `max_memtable_count` controls how many frozen tables
// can queue before active_ grows past its threshold. Measures
// scan_profile() over the whole scan window — the average scan latency
// reflects the steady-state L0 size across flush cycles.
//
// `prefill_l0` (>0) writes that many entries without flushing before
// starting the concurrent phase, so the flush-backlog scenario starts with
// a non-empty L0.
void run_concurrent(const char *label, int n_prepop, int value_size, size_t limit, int duration_secs,
                    int flush_every_ms, uint32_t max_memtable_count, int prefill_l0)
{
    Setup s;
    s.store = std::make_shared<MemPageStore>(1);
    Options opt;
    opt.page_store         = s.store.get();
    opt.frame_bytes        = 64 * 1024;
    opt.leaf_split_bytes   = 64 * 1024;
    opt.max_memtable_count = max_memtable_count;
    s.tree                 = std::make_unique<Crowdbtree>(opt);
    s.tree->init_metrics("s.0.g.0", "");

    // Pre-populate L1: n_prepop keys at slots 1..n_prepop, then flush+snapshot.
    for (int i = 0; i < n_prepop; ++i) {
        s.tree->apply(i + 1, Batch{{{.key = make_key(i), .kind = OpKind::kPut, .value = make_value(value_size, i)}}});
    }
    s.tree->flush();
    s.tree->snapshot(nullptr);

    // Optional L0 pre-fill: write prefill_l0 overwrites without flushing.
    uint64_t next_slot = static_cast<uint64_t>(n_prepop) + 1;
    for (int i = 0; i < prefill_l0; ++i) {
        int key_id = i % n_prepop;
        s.tree->apply(
            next_slot,
            Batch{{{.key = make_key(key_id), .kind = OpKind::kPut, .value = make_value(value_size, key_id)}}});
        ++next_slot;
    }

    std::atomic<bool>     stop{false};
    std::atomic<uint64_t> writes_done{0};

    // Writer thread: continuously apply upserts to random keys in [0, n_prepop).
    std::thread writer([&] {
        std::string v = make_value(value_size, 0);
        while (!stop.load(std::memory_order_relaxed)) {
            int key_id =
                static_cast<int>(writes_done.fetch_add(1, std::memory_order_relaxed) % static_cast<uint64_t>(n_prepop));
            s.tree->apply(next_slot, Batch{{{.key = make_key(key_id), .kind = OpKind::kPut, .value = v}}});
            ++next_slot;
        }
    });

    // Optional flush thread: call flush() every flush_every_ms.
    std::thread           flusher;
    std::atomic<uint64_t> flushes_done{0};
    if (flush_every_ms > 0) {
        flusher = std::thread([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(flush_every_ms));
                if (stop.load(std::memory_order_relaxed))
                    break;
                s.tree->flush();
                flushes_done.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    // Scanner: run scans for duration_secs, measure scan_profile.
    (void)s.tree->scan_profile(); // reset window
    std::vector<scan_entry> out;
    bool                    truncated  = false;
    auto                    t0         = std::chrono::steady_clock::now();
    uint64_t                scan_count = 0;
    while (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - t0).count() <
           static_cast<long long>(duration_secs)) {
        out.clear();
        truncated = false;
        s.tree->scan(Slice(), Slice(), Slice(), limit, 0, false, 0, &out, &truncated);
        ++scan_count;
    }
    ScanProfile p = s.tree->scan_profile();

    stop.store(true, std::memory_order_relaxed);
    writer.join();
    if (flusher.joinable())
        flusher.join();

    uint64_t wd        = writes_done.load(std::memory_order_relaxed);
    uint64_t fd        = flushes_done.load(std::memory_order_relaxed);
    double   write_kps = static_cast<double>(wd) / 1000.0 / static_cast<double>(duration_secs);
    std::printf("\n[%s] n_prepop=%d val=%dB limit=%zu dur=%ds flush=%dms max_mt=%u prefill=%d\n", label, n_prepop,
                value_size, limit, duration_secs, flush_every_ms, max_memtable_count, prefill_l0);
    std::printf("  scans=%llu writes=%llu (%.1fk/s) flushes=%llu\n", static_cast<unsigned long long>(scan_count),
                static_cast<unsigned long long>(wd), write_kps, static_cast<unsigned long long>(fd));
    print_profile("per-scan", p);
    if (p.count > 0) {
        std::printf("    l0-est:              l0_avg/total              = %.1f%%\n",
                    100.0 * static_cast<double>(p.l0.sum_ns) / static_cast<double>(p.total.sum_ns));
        std::printf("    l1-est:              l1_avg/total              = %.1f%%\n",
                    100.0 * static_cast<double>(p.l1.sum_ns) / static_cast<double>(p.total.sum_ns));
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
    // 64B l1 cost (3985us) vs the single-flush microbench (1170us).
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

    // R50 Gate 2: concurrent write+scan with a non-empty L0 at scan time.
    // Production-like: 1 writer at max rate + flush every 3s (mimics the
    // maintenance tick), default max_memtable_count=2. Measures the
    // steady-state scan latency averaged over ~3 flush cycles (10s).
    std::printf("\n=== R50 Gate 2: concurrent write+scan ===\n");
    std::printf("Measures scan latency under sustained writes with non-empty L0.\n");
    run_concurrent("concurrent_64B_flush3s", N, 64, 1000, 10, 3000, 2, 0);
    run_concurrent("concurrent_1KiB_flush3s", N, 1024, 1000, 10, 3000, 2, 0);

    // Flush-backlog upper bound: pre-fill 100k L0 entries, then writer +
    // scanner with no flush, max_memtable_count=8 so frozen_ can accumulate.
    run_concurrent("concurrent_64B_noflush", N, 64, 1000, 10, 0, 8, N);
    run_concurrent("concurrent_1KiB_noflush", N, 1024, 1000, 10, 0, 8, N);

    std::printf("\n=== done ===\n");
    return 0;
}
