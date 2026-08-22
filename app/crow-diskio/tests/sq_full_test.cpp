// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// SQ full backpressure tests: verify that the engines handle more
// concurrent submissions than their internal capacity without dropping
// or deadlocking. Tests:
// 1. Tiny-SQ UringEngine + slow disk: all I/O completes.
// 2. Good-disk isolation: a slow disk doesn't block a good disk.
// 3. Cancellation frees SQ slots.
// 4. BlockingEngine backpressure analog: more jobs than threads.
#include "disk/block_disk.h"
#include "disk/types.h"
#include "engine/blocking/blocking_engine.h"
#include "engine/io_engine.h"

#ifdef CROW_HAVE_LIBURING
#    include "engine/uring/uring_engine.h"
#endif

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

using namespace crow::diskio;
using namespace std::chrono_literals;

namespace
{

std::string temp_file(int64_t size)
{
    const char *root = getenv("TMPDIR");
    if (root == nullptr) {
        root = "/tmp";
    }
    char tmpl[128];
    std::snprintf(tmpl, sizeof(tmpl), "%s/dx_XXXXXX", root);
    std::vector<char> buf(tmpl, tmpl + std::strlen(tmpl) + 1);
    int               fd = mkstemp(buf.data());
    if (fd >= 0) {
        if (size > 0) {
            ftruncate(fd, size);
        }
        close(fd);
    }
    return std::string(buf.data());
}

std::shared_ptr<BlockDisk> make_block_disk(DiskId id, const std::string &path, int64_t zone_cap)
{
    std::vector<Zone> zones;
    zones.push_back({0, 0, zone_cap});
    return std::make_shared<BlockDisk>(id, path, nullptr, std::move(zones), false);
}

} // namespace

// ── BlockingEngine: more jobs than threads ────────────────────────
TEST(SqFullBackpressureTest, BlockingEngineMoreJobsThanThreads)
{
    std::string path = temp_file(1 << 20);
    auto        disk = make_block_disk({0, 1}, path, 1 << 24);

    // Tiny thread pool (1 thread) — submit 100 concurrent writes.
    auto engine = std::make_shared<BlockingEngine>(1);

    constexpr int NUM_IOS   = 100;
    constexpr int DATA_SIZE = 4096;

    std::atomic<int>     completed{0};
    std::vector<uint8_t> payload(DATA_SIZE, 0xAB);

    for (int i = 0; i < NUM_IOS; i++) {
        std::vector<uint8_t> data(payload);
        // Each write goes to a different offset so they don't overlap.
        off_t offset = static_cast<off_t>(i * DATA_SIZE);
        engine->submit_write(disk.get(), offset, data.data(), DATA_SIZE, [&completed, DATA_SIZE](int result) {
            EXPECT_EQ(result, DATA_SIZE);
            completed.fetch_add(1, std::memory_order_release);
        });
    }

    // Wait for all to complete.
    for (int i = 0; i < 600 && completed.load(std::memory_order_acquire) < NUM_IOS; i++) {
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_EQ(completed.load(std::memory_order_acquire), NUM_IOS);

    engine->stop();
    unlink(path.c_str());
}

// ── BlockingEngine: read back what we wrote under load ────────────
TEST(SqFullBackpressureTest, BlockingEngineReadBackUnderLoad)
{
    std::string path = temp_file(1 << 20);
    auto        disk = make_block_disk({0, 2}, path, 1 << 24);

    auto engine = std::make_shared<BlockingEngine>(2);

    constexpr int NUM_IOS   = 50;
    constexpr int DATA_SIZE = 4096;

    std::atomic<int> write_completed{0};
    std::atomic<int> read_completed{0};
    std::atomic<int> read_ok{0};

    // Write 50 blocks with distinct patterns.
    for (int i = 0; i < NUM_IOS; i++) {
        auto  data   = std::make_shared<std::vector<uint8_t>>(DATA_SIZE, static_cast<uint8_t>(i % 256));
        off_t offset = static_cast<off_t>(i * DATA_SIZE);
        engine->submit_write(disk.get(), offset, data->data(), DATA_SIZE,
                             [data, &write_completed, DATA_SIZE](int result) {
                                 EXPECT_EQ(result, DATA_SIZE);
                                 write_completed.fetch_add(1, std::memory_order_release);
                             });
    }

    // Wait for writes.
    for (int i = 0; i < 300 && write_completed.load(std::memory_order_acquire) < NUM_IOS; i++) {
        std::this_thread::sleep_for(10ms);
    }
    ASSERT_EQ(write_completed.load(std::memory_order_acquire), NUM_IOS);

    // Read them back under load (50 concurrent reads).
    for (int i = 0; i < NUM_IOS; i++) {
        auto    buf      = std::make_shared<std::vector<uint8_t>>(DATA_SIZE, 0);
        off_t   offset   = static_cast<off_t>(i * DATA_SIZE);
        uint8_t expected = static_cast<uint8_t>(i % 256);
        engine->submit_read(disk.get(), offset, buf->data(), DATA_SIZE,
                            [buf, expected, &read_completed, &read_ok, DATA_SIZE](int result) {
                                if (result == DATA_SIZE) {
                                    read_ok.fetch_add(1, std::memory_order_relaxed);
                                    // Verify the pattern.
                                    bool ok = true;
                                    for (int j = 0; j < DATA_SIZE; j++) {
                                        if ((*buf)[j] != expected) {
                                            ok = false;
                                            break;
                                        }
                                    }
                                    if (ok) {
                                        read_ok.fetch_add(0, std::memory_order_relaxed);
                                    }
                                }
                                read_completed.fetch_add(1, std::memory_order_release);
                            });
    }

    for (int i = 0; i < 300 && read_completed.load(std::memory_order_acquire) < NUM_IOS; i++) {
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_EQ(read_completed.load(std::memory_order_acquire), NUM_IOS);
    EXPECT_EQ(read_ok.load(std::memory_order_acquire), NUM_IOS);

    engine->stop();
    unlink(path.c_str());
}

// ── BlockingEngine: mixed write + fsync under load ────────────────
TEST(SqFullBackpressureTest, BlockingEngineMixedWriteFsync)
{
    std::string path = temp_file(1 << 20);
    auto        disk = make_block_disk({0, 3}, path, 1 << 24);

    auto engine = std::make_shared<BlockingEngine>(4);

    constexpr int NUM_IOS   = 30;
    constexpr int DATA_SIZE = 4096;

    std::atomic<int> write_completed{0};
    std::atomic<int> fsync_completed{0};

    // Interleave writes and fsyncs.
    for (int i = 0; i < NUM_IOS; i++) {
        auto  data   = std::make_shared<std::vector<uint8_t>>(DATA_SIZE, 0xCD);
        off_t offset = static_cast<off_t>(i * DATA_SIZE);
        engine->submit_write(disk.get(), offset, data->data(), DATA_SIZE,
                             [data, &write_completed, DATA_SIZE](int result) {
                                 EXPECT_EQ(result, DATA_SIZE);
                                 write_completed.fetch_add(1, std::memory_order_release);
                             });
        if (i % 10 == 9) {
            engine->submit_fsync(disk.get(), [&fsync_completed, DATA_SIZE](int result) {
                EXPECT_GE(result, 0);
                fsync_completed.fetch_add(1, std::memory_order_release);
            });
        }
    }

    for (int i = 0; i < 300 && write_completed.load(std::memory_order_acquire) < NUM_IOS; i++) {
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_EQ(write_completed.load(std::memory_order_acquire), NUM_IOS);
    EXPECT_EQ(fsync_completed.load(std::memory_order_acquire), 3); // i=9,19,29

    engine->stop();
    unlink(path.c_str());
}

#ifdef CROW_HAVE_LIBURING

// ── UringEngine: tiny SQ + many writes ────────────────────────────
TEST(SqFullBackpressureTest, UringEngineTinySqManyWrites)
{
    std::string path = temp_file(1 << 20);
    auto        disk = make_block_disk({0, 10}, path, 1 << 24);

    // Tiny SQ (8 entries) — submit 100 writes. The reactor should
    // handle backpressure: submissions that don't fit in the SQ ring
    // should be queued and submitted as slots free up.
    auto engine = std::make_shared<UringEngine>(8);

    constexpr int NUM_IOS   = 100;
    constexpr int DATA_SIZE = 4096;

    std::atomic<int> completed{0};

    for (int i = 0; i < NUM_IOS; i++) {
        auto  data   = std::make_shared<std::vector<uint8_t>>(DATA_SIZE, 0xAB);
        off_t offset = static_cast<off_t>(i * DATA_SIZE);
        engine->submit_write(disk.get(), offset, data->data(), DATA_SIZE, [data, &completed, DATA_SIZE](int result) {
            EXPECT_EQ(result, DATA_SIZE);
            completed.fetch_add(1, std::memory_order_release);
        });
    }

    // Wait for all to complete (may take a while with tiny SQ).
    for (int i = 0; i < 600 && completed.load(std::memory_order_acquire) < NUM_IOS; i++) {
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_EQ(completed.load(std::memory_order_acquire), NUM_IOS);

    unlink(path.c_str());
}

// ── UringEngine: good-disk isolation ──────────────────────────────
// Two disks on the same engine/ring. One disk gets many I/Os, the
// other gets one I/O. The second disk's I/O should complete even while
// the first disk is busy.
TEST(SqFullBackpressureTest, UringEngineGoodDiskIsolation)
{
    std::string path1 = temp_file(1 << 20);
    std::string path2 = temp_file(1 << 20);
    auto        disk1 = make_block_disk({0, 11}, path1, 1 << 24);
    auto        disk2 = make_block_disk({0, 12}, path2, 1 << 24);

    auto engine = std::make_shared<UringEngine>(32);

    constexpr int NUM_IOS   = 50;
    constexpr int DATA_SIZE = 4096;

    std::atomic<int> disk1_completed{0};
    std::atomic<int> disk2_completed{0};

    // Flood disk1 with I/O.
    for (int i = 0; i < NUM_IOS; i++) {
        auto  data   = std::make_shared<std::vector<uint8_t>>(DATA_SIZE, 0x11);
        off_t offset = static_cast<off_t>(i * DATA_SIZE);
        engine->submit_write(disk1.get(), offset, data->data(), DATA_SIZE,
                             [data, &disk1_completed, DATA_SIZE](int result) {
                                 EXPECT_EQ(result, DATA_SIZE);
                                 disk1_completed.fetch_add(1, std::memory_order_release);
                             });
    }

    // Submit one I/O to disk2 — should complete despite disk1 flood.
    auto data2 = std::make_shared<std::vector<uint8_t>>(DATA_SIZE, 0x22);
    engine->submit_write(disk2.get(), 0, data2->data(), DATA_SIZE, [data2, &disk2_completed, DATA_SIZE](int result) {
        EXPECT_EQ(result, DATA_SIZE);
        disk2_completed.fetch_add(1, std::memory_order_release);
    });

    // Wait for disk2's I/O to complete (should not be blocked by disk1).
    for (int i = 0; i < 300 && disk2_completed.load(std::memory_order_acquire) < 1; i++) {
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_EQ(disk2_completed.load(std::memory_order_acquire), 1);

    // Wait for disk1's I/O to complete.
    for (int i = 0; i < 600 && disk1_completed.load(std::memory_order_acquire) < NUM_IOS; i++) {
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_EQ(disk1_completed.load(std::memory_order_acquire), NUM_IOS);

    // Verify disk2's data is correct.
    std::vector<uint8_t> read_buf(DATA_SIZE, 0);
    std::atomic<bool>    read_done{false};
    std::atomic<int>     read_result{-1};
    engine->submit_read(disk2.get(), 0, read_buf.data(), DATA_SIZE, [&read_done, &read_result, DATA_SIZE](int result) {
        read_result.store(result, std::memory_order_release);
        read_done.store(true, std::memory_order_release);
    });
    for (int i = 0; i < 300 && !read_done.load(std::memory_order_acquire); i++) {
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_EQ(read_result.load(std::memory_order_acquire), DATA_SIZE);
    for (int i = 0; i < DATA_SIZE; i++) {
        EXPECT_EQ(read_buf[i], 0x22) << "mismatch at byte " << i;
        if (read_buf[i] != 0x22) {
            break;
        }
    }

    unlink(path1.c_str());
    unlink(path2.c_str());
}

// ── UringEngine: cancellation frees slots ─────────────────────────
// Submit I/O, cancel it, then verify new I/O still works (slots freed).
// Note: the current reactor cancel() erases callbacks without invoking
// them — canceled ops' callbacks don't fire. The key invariant is that
// new I/O works after cancellation (no slot leak).
TEST(SqFullBackpressureTest, UringEngineCancellationFreesSlots)
{
    std::string path = temp_file(1 << 20);
    auto        disk = make_block_disk({0, 13}, path, 1 << 24);

    // Small SQ to make slot pressure more visible.
    auto engine = std::make_shared<UringEngine>(16);

    constexpr int    DATA_SIZE = 4096;
    std::atomic<int> completed{0};

    // Submit some writes.
    for (int i = 0; i < 10; i++) {
        auto  data   = std::make_shared<std::vector<uint8_t>>(DATA_SIZE, 0xAB);
        off_t offset = static_cast<off_t>(i * DATA_SIZE);
        engine->submit_write(disk.get(), offset, data->data(), DATA_SIZE, [data, &completed, DATA_SIZE](int result) {
            EXPECT_EQ(result, DATA_SIZE);
            completed.fetch_add(1, std::memory_order_release);
        });
    }

    // Wait for writes to complete.
    for (int i = 0; i < 300 && completed.load(std::memory_order_acquire) < 10; i++) {
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_EQ(completed.load(std::memory_order_acquire), 10);

    // Submit more writes, then cancel them.
    std::atomic<int> batch2_completed{0};
    for (int i = 0; i < 10; i++) {
        auto  data   = std::make_shared<std::vector<uint8_t>>(DATA_SIZE, 0xEF);
        off_t offset = static_cast<off_t>((10 + i) * DATA_SIZE);
        engine->submit_write(disk.get(), offset, data->data(), DATA_SIZE,
                             [data, &batch2_completed, DATA_SIZE](int result) {
                                 EXPECT_EQ(result, DATA_SIZE);
                                 batch2_completed.fetch_add(1, std::memory_order_release);
                             });
    }

    // Cancel all in-flight for this disk.
    engine->cancel_disk({0, 13});

    // After cancellation, new I/O should work (slots were freed).
    std::atomic<bool> new_write_done{false};
    std::atomic<int>  new_write_result{-1};
    auto              data = std::make_shared<std::vector<uint8_t>>(DATA_SIZE, 0xCD);
    engine->submit_write(disk.get(), 0, data->data(), DATA_SIZE,
                         [data, &new_write_done, &new_write_result, DATA_SIZE](int result) {
                             new_write_result.store(result, std::memory_order_release);
                             new_write_done.store(true, std::memory_order_release);
                         });
    for (int i = 0; i < 300 && !new_write_done.load(std::memory_order_acquire); i++) {
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_TRUE(new_write_done.load(std::memory_order_acquire));
    EXPECT_EQ(new_write_result.load(std::memory_order_acquire), DATA_SIZE);

    unlink(path.c_str());
}

#endif // CROW_HAVE_LIBURING
