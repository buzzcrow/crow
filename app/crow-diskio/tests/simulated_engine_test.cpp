// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// SimulatedEngine + SimulatedDisk tests: error injection, latency injection,
// error_rate=0.5 distribution, fixed latency.
#include "disk/mem_disk.h"
#include "disk/simulated_disk.h"
#include "disk/types.h"
#include "engine/dummy/dummy_engine.h"
#include "engine/simulated/simulated_engine.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

namespace
{
std::shared_ptr<crow::diskio::SimulatedDisk> make_sim_disk(crow::diskio::DiskId id, crow::diskio::DiskProperties props)
{
    std::vector<crow::diskio::Zone> zones;
    zones.push_back({0, 0, 1 << 24});
    auto mem = std::make_shared<crow::diskio::MemDisk>(id, std::move(zones), 4096);
    return std::make_shared<crow::diskio::SimulatedDisk>(mem, props);
}
} // namespace

TEST(SimulatedEngine, ErrorRateOneAllErrors)
{
    auto                          disk  = make_sim_disk({1, 1}, {0, 0, 1.0});
    auto                          dummy = std::make_unique<crow::diskio::DummyEngine>();
    crow::diskio::SimulatedEngine engine(std::move(dummy));

    std::vector<uint8_t> data(4096, 0xAB);
    std::atomic<int>     got_res{0};
    engine.submit_write(disk.get(), 0, data.data(), data.size(),
                        [&](int res) { got_res.store(res, std::memory_order_relaxed); });
    // Error injection is async (detached thread); wait briefly.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(got_res.load(), -EIO);
}

TEST(SimulatedEngine, ErrorRateZeroNoErrors)
{
    auto                          disk  = make_sim_disk({2, 2}, {0, 0, 0.0});
    auto                          dummy = std::make_unique<crow::diskio::DummyEngine>();
    crow::diskio::SimulatedEngine engine(std::move(dummy));

    std::vector<uint8_t> data(4096, 0xAB);
    std::atomic<int>     got_res{-1};
    engine.submit_write(disk.get(), 0, data.data(), data.size(),
                        [&](int res) { got_res.store(res, std::memory_order_relaxed); });
    // No latency, no error — should complete immediately.
    EXPECT_EQ(got_res.load(), 4096);
}

TEST(SimulatedEngine, LatencyInjectionDelaysCompletion)
{
    auto                          disk  = make_sim_disk({3, 3}, {50, 50, 0.0});
    auto                          dummy = std::make_unique<crow::diskio::DummyEngine>();
    crow::diskio::SimulatedEngine engine(std::move(dummy));

    std::vector<uint8_t> data(4096, 0xAB);
    std::atomic<bool>    done{false};
    auto                 start = std::chrono::steady_clock::now();
    engine.submit_write(disk.get(), 0, data.data(), data.size(),
                        [&](int) { done.store(true, std::memory_order_release); });
    // Wait for completion.
    while (!done.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    // Should have taken at least ~50ms (latency injection).
    EXPECT_GE(elapsed, 40); // allow small scheduling jitter
}

TEST(SimulatedEngine, FixedLatencyDegenerateCase)
{
    auto                          disk  = make_sim_disk({4, 4}, {10, 10, 0.0});
    auto                          dummy = std::make_unique<crow::diskio::DummyEngine>();
    crow::diskio::SimulatedEngine engine(std::move(dummy));

    std::vector<uint8_t> data(4096, 0xAB);
    std::atomic<bool>    done{false};
    auto                 start = std::chrono::steady_clock::now();
    engine.submit_write(disk.get(), 0, data.data(), data.size(),
                        [&](int) { done.store(true, std::memory_order_release); });
    while (!done.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    EXPECT_GE(elapsed, 8); // ~10ms with jitter
}

TEST(SimulatedEngine, ErrorRateHalfApproximatelyHalfErrors)
{
    auto                          disk  = make_sim_disk({5, 5}, {0, 0, 0.5});
    auto                          dummy = std::make_unique<crow::diskio::DummyEngine>();
    crow::diskio::SimulatedEngine engine(std::move(dummy));

    constexpr int    kOps = 1000;
    std::atomic<int> errors{0};
    std::atomic<int> success{0};
    for (int i = 0; i < kOps; ++i) {
        engine.submit_write(disk.get(), 0, nullptr, 0, [&](int res) {
            if (res < 0) {
                errors.fetch_add(1, std::memory_order_relaxed);
            }
            else {
                success.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    // Wait for all to complete (error injection uses detached threads).
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    int total = errors.load() + success.load();
    EXPECT_EQ(total, kOps);
    // With error_rate=0.5 and 1000 ops, expect ~500 errors (within tolerance).
    EXPECT_NEAR(errors.load(), 500, 100); // ±100 tolerance
}

TEST(SimulatedEngine, ReadRoundTripWithNoErrors)
{
    auto                          disk  = make_sim_disk({6, 6}, {0, 0, 0.0});
    auto                          dummy = std::make_unique<crow::diskio::DummyEngine>();
    crow::diskio::SimulatedEngine engine(std::move(dummy));

    std::vector<uint8_t> out(4096, 0);
    std::atomic<int>     got_res{-1};
    engine.submit_read(disk.get(), 0, out.data(), out.size(),
                       [&](int res) { got_res.store(res, std::memory_order_relaxed); });
    EXPECT_EQ(got_res.load(), 4096);
    // Content should be deterministic (from MemDisk pattern).
    std::vector<uint8_t> out2(4096, 0);
    engine.submit_read(disk.get(), 0, out2.data(), out2.size(), [&](int) {});
    EXPECT_EQ(out, out2);
}
