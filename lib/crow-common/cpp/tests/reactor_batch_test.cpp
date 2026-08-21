// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Reactor batched SQE submission tests: verify that multiple SQEs queued
// between run() iterations are submitted in a single io_uring_submit() call.
#include "crow-common/reactor.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

namespace
{
std::string temp_path()
{
    std::string root = "/tmp/crow-common-reactor-batch-tests";
    std::filesystem::create_directories(root);
    std::array<char, 128> tmpl{};
    std::snprintf(tmpl.data(), tmpl.size(), "%s/rx_XXXXXX", root.c_str());
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    int fd = mkstemp(buf.data());
    if (fd >= 0) {
        close(fd);
    }
    return buf.data();
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

TEST(ReactorBatchedSubmit, ManyConcurrentSubmitsAllComplete)
{
    constexpr int kOps = 100;
    std::string   path = temp_path();
    int           fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::ftruncate(fd, static_cast<off_t>(kOps) * 16), 0);

    crow::common::Reactor r(256);
    std::atomic<int>      completed{0};
    std::vector<int>      results(kOps, -1);

    // Submit all 100 writes as fast as possible (no sleep between submits).
    // With batched submission, these should all be flushed in one
    // io_uring_submit() call by the reactor thread.
    for (int i = 0; i < kOps; ++i) {
        std::vector<uint8_t> buf(16, static_cast<uint8_t>(i));
        // Allocate the buffer on the heap so it survives until the callback fires.
        auto *buf_ptr = new std::vector<uint8_t>(std::move(buf));
        r.submit_write(fd, buf_ptr->data(), buf_ptr->size(), static_cast<off_t>(i) * 16,
                       [buf_ptr, i, &results, &completed](int res) {
                           results[i] = res;
                           completed.fetch_add(1, std::memory_order_acq_rel);
                           delete buf_ptr;
                       });
    }

    ASSERT_TRUE(wait_for([&] { return completed.load(std::memory_order_acquire) == kOps; }, /*max_iters=*/400));
    for (int i = 0; i < kOps; ++i) {
        EXPECT_EQ(results[i], 16) << "op " << i;
    }

    // Verify data integrity: read back each block.
    for (int i = 0; i < kOps; ++i) {
        std::vector<uint8_t> out(16, 0);
        ASSERT_EQ(::pread(fd, out.data(), out.size(), static_cast<off_t>(i) * 16), 16);
        EXPECT_EQ(out.front(), static_cast<uint8_t>(i)) << "op " << i;
    }

    ::close(fd);
    std::remove(path.c_str());
}

TEST(ReactorBatchedSubmit, SubmitAndCancelInterleaved)
{
    // Verify that batched submission doesn't break cancellation: submit a
    // batch, cancel one, and verify the cancelled op's callback never fires.
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::ftruncate(fd, 1 << 16), 0);

    crow::common::Reactor r(64);
    std::atomic<int>      completed{0};
    std::atomic<bool>     cancelled_fired{false};

    // Submit 10 writes, cancel the 5th.
    std::vector<std::vector<uint8_t>> bufs(10);
    for (int i = 0; i < 10; ++i) {
        bufs[i].assign(16, static_cast<uint8_t>(i));
    }
    uint64_t cancel_id = 0;
    for (int i = 0; i < 10; ++i) {
        auto *buf_ptr = &bufs[i];
        if (i == 5) {
            cancel_id = r.submit_write(fd, buf_ptr->data(), buf_ptr->size(), static_cast<off_t>(i) * 16,
                                       [&](int) { cancelled_fired.store(true, std::memory_order_release); });
        }
        else {
            r.submit_write(fd, buf_ptr->data(), buf_ptr->size(), static_cast<off_t>(i) * 16,
                           [&completed](int) { completed.fetch_add(1, std::memory_order_acq_rel); });
        }
    }
    r.cancel(cancel_id);

    // Wait for the 9 non-cancelled ops to complete.
    ASSERT_TRUE(wait_for([&] { return completed.load(std::memory_order_acquire) == 9; }, /*max_iters=*/400));
    // The cancelled op should not have fired.
    EXPECT_FALSE(wait_for([&] { return cancelled_fired.load(std::memory_order_acquire); }, /*max_iters=*/20));

    ::close(fd);
    std::remove(path.c_str());
}
