// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// NullDisk + MemDisk + DummyDiskEngine tests: drop-write, pattern read,
// store-and-read-back, fault injection (error rate, latency).
//
// All tests use BlockingEngine as the inner engine (no real disk I/O —
// memfd backing). The full blocking pwrite/pread path executes.
#include "disk/disk_properties.h"
#include "disk/mem_disk.h"
#include "disk/null_disk.h"
#include "disk/types.h"
#include "engine/blocking/blocking_engine.h"
#include "engine/dummy/dummy_engine.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

namespace
{
std::shared_ptr<crowdb::diskio::BlockingEngine> make_engine()
{
    return std::make_shared<crowdb::diskio::BlockingEngine>(2);
}

std::vector<crowdb::diskio::Zone> make_zones()
{
    std::vector<crowdb::diskio::Zone> zones;
    zones.push_back({0, 0, 1 << 24});
    return zones;
}

template <typename Pred> bool wait_for(Pred pred, int max_iters = 200, int sleep_ms = 5)
{
    for (int i = 0; i < max_iters; ++i) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    }
    return pred();
}
} // namespace

// ── NullDisk tests ──────────────────────────────────────────────────

TEST(NullDisk, WriteReturnsSuccess)
{
    auto engine = make_engine();
    auto disk   = std::make_shared<crowdb::diskio::NullDisk>(crowdb::diskio::DiskId{1, 1}, engine, make_zones());
    std::vector<uint8_t> data(4096, 0xAB);
    std::atomic<int>     res{-1};
    disk->engine()->submit_write(disk.get(), 0, data.data(), data.size(),
                                 [&](int r) { res.store(r, std::memory_order_relaxed); });
    wait_for([&] { return res.load() != -1; });
    EXPECT_EQ(res.load(), static_cast<int>(data.size()));
}

TEST(NullDisk, ReadReturnsDeterministicContent)
{
    auto engine = make_engine();
    auto disk   = std::make_shared<crowdb::diskio::NullDisk>(crowdb::diskio::DiskId{2, 2}, engine, make_zones());
    std::vector<uint8_t> out1(4096, 0);
    std::vector<uint8_t> out2(4096, 0);
    std::atomic<int>     res1{-1};
    std::atomic<int>     res2{-1};
    disk->engine()->submit_read(disk.get(), 0, out1.data(), out1.size(), 0,
                                [&](int r) { res1.store(r, std::memory_order_relaxed); });
    wait_for([&] { return res1.load() != -1; });
    disk->engine()->submit_read(disk.get(), 0, out2.data(), out2.size(), 0,
                                [&](int r) { res2.store(r, std::memory_order_relaxed); });
    wait_for([&] { return res2.load() != -1; });
    EXPECT_EQ(res1.load(), 4096);
    EXPECT_EQ(res2.load(), 4096);
    EXPECT_EQ(out1, out2);
}

TEST(NullDisk, DifferentDiskIdsProduceDifferentContent)
{
    auto engine = make_engine();
    auto disk1  = std::make_shared<crowdb::diskio::NullDisk>(crowdb::diskio::DiskId{3, 3}, engine, make_zones());
    auto disk2  = std::make_shared<crowdb::diskio::NullDisk>(crowdb::diskio::DiskId{4, 4}, engine, make_zones());
    std::vector<uint8_t> out1(4096, 0);
    std::vector<uint8_t> out2(4096, 0);
    std::atomic<bool>    done1{false};
    std::atomic<bool>    done2{false};
    disk1->engine()->submit_read(disk1.get(), 0, out1.data(), out1.size(), 0, [&](int) { done1.store(true); });
    disk2->engine()->submit_read(disk2.get(), 0, out2.data(), out2.size(), 0, [&](int) { done2.store(true); });
    wait_for([&] { return done1.load() && done2.load(); });
    EXPECT_NE(out1, out2);
}

TEST(NullDisk, FsyncIsNoOp)
{
    auto             engine = make_engine();
    auto             disk = std::make_shared<crowdb::diskio::NullDisk>(crowdb::diskio::DiskId{5, 5}, engine, make_zones());
    std::atomic<int> res{-1};
    disk->engine()->submit_fsync(disk.get(), [&](int r) { res.store(r, std::memory_order_relaxed); });
    wait_for([&] { return res.load() != -1; });
    EXPECT_EQ(res.load(), 0);
}

// ── MemDisk tests ───────────────────────────────────────────────────

TEST(MemDisk, WriteAndReadBack)
{
    auto engine = make_engine();
    auto disk   = std::make_shared<crowdb::diskio::MemDisk>(crowdb::diskio::DiskId{6, 6}, engine, make_zones());
    std::vector<uint8_t> data(4096);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<uint8_t>(i & 0xFF);
    }
    std::atomic<int> write_res{-1};
    disk->engine()->submit_write(disk.get(), 0, data.data(), data.size(),
                                 [&](int r) { write_res.store(r, std::memory_order_relaxed); });
    wait_for([&] { return write_res.load() != -1; });
    ASSERT_EQ(write_res.load(), 4096);

    std::vector<uint8_t> out(4096, 0);
    std::atomic<int>     read_res{-1};
    disk->engine()->submit_read(disk.get(), 0, out.data(), out.size(), 0,
                                [&](int r) { read_res.store(r, std::memory_order_relaxed); });
    wait_for([&] { return read_res.load() != -1; });
    EXPECT_EQ(read_res.load(), 4096);
    EXPECT_EQ(out, data);
}

TEST(MemDisk, FsyncSucceeds)
{
    auto             engine = make_engine();
    auto             disk   = std::make_shared<crowdb::diskio::MemDisk>(crowdb::diskio::DiskId{7, 7}, engine, make_zones());
    std::atomic<int> res{-1};
    disk->engine()->submit_fsync(disk.get(), [&](int r) { res.store(r, std::memory_order_relaxed); });
    wait_for([&] { return res.load() != -1; });
    EXPECT_EQ(res.load(), 0);
}

// ── Fault injection tests (merged from SimulatedEngine) ─────────────

TEST(DummyDisk, ErrorRateOneAllErrors)
{
    auto                         engine = make_engine();
    crowdb::diskio::DiskProperties props{0, 0, 1.0};
    auto disk = std::make_shared<crowdb::diskio::NullDisk>(crowdb::diskio::DiskId{8, 8}, engine, make_zones(), props);
    std::vector<uint8_t> data(4096, 0xAB);
    std::atomic<int>     res{0};
    disk->engine()->submit_write(disk.get(), 0, data.data(), data.size(),
                                 [&](int r) { res.store(r, std::memory_order_relaxed); });
    wait_for([&] { return res.load() != 0; }, 100);
    EXPECT_EQ(res.load(), -EIO);
}

TEST(DummyDisk, ErrorRateZeroNoErrors)
{
    auto                         engine = make_engine();
    crowdb::diskio::DiskProperties props{0, 0, 0.0};
    auto disk = std::make_shared<crowdb::diskio::NullDisk>(crowdb::diskio::DiskId{9, 9}, engine, make_zones(), props);
    std::vector<uint8_t> data(4096, 0xAB);
    std::atomic<int>     res{-1};
    disk->engine()->submit_write(disk.get(), 0, data.data(), data.size(),
                                 [&](int r) { res.store(r, std::memory_order_relaxed); });
    wait_for([&] { return res.load() != -1; });
    EXPECT_EQ(res.load(), 4096);
}

TEST(DummyDisk, LatencyInjectionDelaysCompletion)
{
    auto                         engine = make_engine();
    crowdb::diskio::DiskProperties props{50, 50, 0.0};
    auto disk = std::make_shared<crowdb::diskio::NullDisk>(crowdb::diskio::DiskId{10, 10}, engine, make_zones(), props);
    std::vector<uint8_t> data(4096, 0xAB);
    std::atomic<bool>    done{false};
    auto                 start = std::chrono::steady_clock::now();
    disk->engine()->submit_write(disk.get(), 0, data.data(), data.size(),
                                 [&](int) { done.store(true, std::memory_order_release); });
    wait_for([&] { return done.load(std::memory_order_acquire); }, 200, 5);
    auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    EXPECT_GE(elapsed, 40);
}

TEST(DummyDisk, ErrorRateHalfApproximatelyHalfErrors)
{
    auto                         engine = make_engine();
    crowdb::diskio::DiskProperties props{0, 0, 0.5};
    auto disk = std::make_shared<crowdb::diskio::NullDisk>(crowdb::diskio::DiskId{11, 11}, engine, make_zones(), props);
    constexpr int    kOps = 1000;
    std::atomic<int> errors{0};
    std::atomic<int> success{0};
    for (int i = 0; i < kOps; ++i) {
        disk->engine()->submit_write(disk.get(), 0, nullptr, 0, [&](int res) {
            if (res < 0) {
                errors.fetch_add(1, std::memory_order_relaxed);
            }
            else {
                success.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    wait_for([&] { return errors.load() + success.load() == kOps; }, 500, 10);
    int total = errors.load() + success.load();
    EXPECT_EQ(total, kOps);
    EXPECT_NEAR(errors.load(), 500, 100);
}
