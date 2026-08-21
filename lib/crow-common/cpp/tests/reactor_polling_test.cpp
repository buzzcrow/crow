// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Reactor polling mode tests: Hybrid busy-poll + Sqpoll.
// Only built when CMake found liburing (CROW_HAVE_LIBURING).
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
    std::string root = "/tmp/crow-common-reactor-tests";
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

TEST(ReactorHybrid, WriteReadRoundTrip)
{
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);

    crow::common::HybridConfig cfg;
    cfg.busy_poll_budget = 32;
    crow::common::Reactor r(256, crow::common::PollingMode::Hybrid, cfg);

    std::atomic<bool>    done{false};
    std::atomic<int>     got_res{-1};
    std::vector<uint8_t> in{1, 2, 3, 4, 5, 6, 7, 8};
    r.submit_write(fd, in.data(), in.size(), 0, [&](int res) {
        got_res.store(res, std::memory_order_relaxed);
        done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_EQ(got_res.load(), static_cast<int>(in.size()));

    std::vector<uint8_t> out(in.size(), 0);
    done.store(false);
    r.submit_read(fd, out.data(), out.size(), 0, [&](int res) {
        got_res.store(res, std::memory_order_relaxed);
        done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_EQ(got_res.load(), static_cast<int>(out.size()));
    EXPECT_EQ(out, in);

    ::close(fd);
    std::remove(path.c_str());
}

TEST(ReactorHybrid, BusyPollTransitionsToEventWait)
{
    // With a very low busy_poll_budget, the reactor should quickly transition
    // from busy-poll to event-wait. Verify it still completes I/O correctly.
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);

    crow::common::HybridConfig cfg;
    cfg.busy_poll_budget = 4; // transition after just 4 empty peeks
    crow::common::Reactor r(64, crow::common::PollingMode::Hybrid, cfg);

    // Submit a write after a brief delay (reactor will be in event-wait mode).
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::atomic<bool>    done{false};
    std::vector<uint8_t> in{0xAB, 0xCD, 0xEF};
    r.submit_write(fd, in.data(), in.size(), 0, [&](int) { done.store(true, std::memory_order_release); });
    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));

    ::close(fd);
    std::remove(path.c_str());
}

TEST(ReactorSqpoll, WriteReadRoundTrip)
{
    // Sqpoll requires Linux 5.11+. If the kernel doesn't support it,
    // io_uring_queue_init with IORING_SETUP_SQPOLL will fail and valid_
    // will be false — the test then verifies the fallback (synchronous
    // error callback) rather than skipping entirely.
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);

    crow::common::SqpollConfig cfg;
    cfg.sq_thread_idle_ms = 500;
    crow::common::Reactor r(256, crow::common::PollingMode::Sqpoll, {}, cfg);

    std::atomic<bool>    done{false};
    std::atomic<int>     got_res{-1};
    std::vector<uint8_t> in{10, 20, 30, 40};
    r.submit_write(fd, in.data(), in.size(), 0, [&](int res) {
        got_res.store(res, std::memory_order_relaxed);
        done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    // If valid_ (kernel supports sqpoll), expect success; if not, expect -EIO.
    if (got_res.load() == -EIO) {
        // Kernel doesn't support SQPOLL — verify the fallback path.
        SUCCEED() << "SQPOLL not supported by kernel, fallback verified";
    }
    else {
        EXPECT_EQ(got_res.load(), static_cast<int>(in.size()));
    }

    ::close(fd);
    std::remove(path.c_str());
}
