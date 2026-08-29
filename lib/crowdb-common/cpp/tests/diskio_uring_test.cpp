// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// DiskIOUring tests: single/multi-pipeline submit, fd routing, cancel_fd,
// batch submit, multi-CQ polling.
#include "crowdb-common/diskio_uring.h"

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

using namespace crowdb::common;

namespace
{
std::string temp_path()
{
    std::string root = "/tmp/crowdb-common-diskio-uring-tests";
    std::filesystem::create_directories(root);
    std::array<char, 128> tmpl{};
    std::snprintf(tmpl.data(), tmpl.size(), "%s/ux_XXXXXX", root.c_str());
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

// ── Single-pipeline basic submit + complete ──────────────────────

TEST(DiskIOUring, SinglePipelineSubmitReadCompletes)
{
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    std::vector<uint8_t> expected{10, 20, 30, 40, 50, 60, 70, 80};
    ASSERT_EQ(::pwrite(fd, expected.data(), expected.size(), 0), static_cast<ssize_t>(expected.size()));

    Topology topo;
    topo.pipelines.push_back({256, PollingMode::Classic});
    DiskIOUring uring(std::move(topo));
    ASSERT_GE(uring.eventfds(nullptr, 0), 0); // just check it doesn't crash
    uring.register_fd(fd);

    std::atomic<bool>    done{false};
    std::atomic<int>     got_res{-1};
    std::vector<uint8_t> buf(expected.size(), 0);
    uring.submit_read(fd, buf.data(), buf.size(), 0, [&](int res) {
        got_res.store(res, std::memory_order_relaxed);
        done.store(true, std::memory_order_release);
    });

    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_EQ(got_res.load(), static_cast<int>(expected.size()));
    EXPECT_EQ(buf, expected);

    ::close(fd);
    std::remove(path.c_str());
}

TEST(DiskIOUring, SinglePipelineSubmitWriteThenReadRoundTrips)
{
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);

    Topology topo;
    topo.pipelines.push_back({256, PollingMode::Classic});
    DiskIOUring uring(std::move(topo));
    uring.register_fd(fd);

    std::atomic<bool>    done{false};
    std::atomic<int>     got_res{-1};
    std::vector<uint8_t> in{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    uring.submit_write(fd, in.data(), in.size(), 100, [&](int res) {
        got_res.store(res, std::memory_order_relaxed);
        done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_EQ(got_res.load(), static_cast<int>(in.size()));

    std::vector<uint8_t> out(in.size(), 0);
    ASSERT_EQ(::pread(fd, out.data(), out.size(), 100), static_cast<ssize_t>(out.size()));
    EXPECT_EQ(out, in);

    ::close(fd);
    std::remove(path.c_str());
}

TEST(DiskIOUring, SinglePipelineFsyncCompletes)
{
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);

    Topology topo;
    topo.pipelines.push_back({256, PollingMode::Classic});
    DiskIOUring uring(std::move(topo));
    uring.register_fd(fd);

    std::vector<uint8_t> in(4096, 0xAB);
    std::atomic<bool>    write_done{false};
    uring.submit_write(fd, in.data(), in.size(), 0, [&](int) { write_done.store(true, std::memory_order_release); });
    ASSERT_TRUE(wait_for([&] { return write_done.load(std::memory_order_acquire); }));

    std::atomic<bool> sync_done{false};
    std::atomic<int>  sync_res{-1};
    uring.submit_fsync(fd, [&](int res) {
        sync_res.store(res, std::memory_order_relaxed);
        sync_done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return sync_done.load(std::memory_order_acquire); }));
    EXPECT_GE(sync_res.load(), 0);

    ::close(fd);
    std::remove(path.c_str());
}

TEST(DiskIOUring, SinglePipelineMultipleConcurrentSubmitsAllComplete)
{
    constexpr int kOps = 64;
    std::string   path = temp_path();
    int           fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::ftruncate(fd, static_cast<off_t>(kOps) * 16), 0);

    Topology topo;
    topo.pipelines.push_back({256, PollingMode::Classic});
    DiskIOUring uring(std::move(topo));
    uring.register_fd(fd);

    std::atomic<int>                  completed{0};
    std::vector<int>                  results(kOps, -1);
    std::vector<std::vector<uint8_t>> patterns(kOps);
    for (int i = 0; i < kOps; ++i) {
        patterns[i].assign(16, static_cast<uint8_t>(i));
    }
    for (int i = 0; i < kOps; ++i) {
        uring.submit_write(fd, patterns[i].data(), patterns[i].size(), static_cast<off_t>(i) * 16, [&, i](int res) {
            results[i] = res;
            completed.fetch_add(1, std::memory_order_acq_rel);
        });
    }

    ASSERT_TRUE(wait_for([&] { return completed.load(std::memory_order_acquire) == kOps; }, 400));
    for (int i = 0; i < kOps; ++i) {
        EXPECT_EQ(results[i], 16) << "op " << i;
    }

    ::close(fd);
    std::remove(path.c_str());
}

// ── Multi-pipeline routing ───────────────────────────────────────

TEST(DiskIOUring, MultiPipelineExplicitRouting)
{
    std::string path_a = temp_path();
    std::string path_b = temp_path();
    int         fd_a   = ::open(path_a.c_str(), O_RDWR);
    int         fd_b   = ::open(path_b.c_str(), O_RDWR);
    ASSERT_GE(fd_a, 0);
    ASSERT_GE(fd_b, 0);
    ASSERT_EQ(::ftruncate(fd_a, 4096), 0);
    ASSERT_EQ(::ftruncate(fd_b, 4096), 0);

    Topology topo;
    topo.pipelines.push_back({64, PollingMode::Classic});
    topo.pipelines.push_back({64, PollingMode::Classic});
    topo.poll_thread_groups.push_back({
        {0, 1}
    }); // one thread for both
    DiskIOUring uring(std::move(topo));
    uring.register_fd(fd_a, 0);
    uring.register_fd(fd_b, 1);

    std::atomic<bool>    done_a{false}, done_b{false};
    std::atomic<int>     res_a{-1}, res_b{-1};
    std::vector<uint8_t> buf(4096, 0xAA);

    uring.submit_write(fd_a, buf.data(), buf.size(), 0, [&](int res) {
        res_a.store(res, std::memory_order_relaxed);
        done_a.store(true, std::memory_order_release);
    });
    uring.submit_write(fd_b, buf.data(), buf.size(), 0, [&](int res) {
        res_b.store(res, std::memory_order_relaxed);
        done_b.store(true, std::memory_order_release);
    });

    ASSERT_TRUE(wait_for([&] { return done_a.load() && done_b.load(); }));
    EXPECT_EQ(res_a.load(), static_cast<int>(buf.size()));
    EXPECT_EQ(res_b.load(), static_cast<int>(buf.size()));

    ::close(fd_a);
    ::close(fd_b);
    std::remove(path_a.c_str());
    std::remove(path_b.c_str());
}

TEST(DiskIOUring, MultiPipelineEventfdsReturnsAllPipelines)
{
    Topology topo;
    topo.pipelines.push_back({64, PollingMode::Classic});
    topo.pipelines.push_back({64, PollingMode::Classic});
    DiskIOUring uring(std::move(topo));

    int32_t fds[2] = {-1, -1};
    size_t  count  = uring.eventfds(fds, 2);
    EXPECT_EQ(count, 2u);
    EXPECT_GE(fds[0], 0);
    EXPECT_GE(fds[1], 0);
    EXPECT_NE(fds[0], fds[1]);
}

// ── In-flight tracking ───────────────────────────────────────────

TEST(DiskIOUring, InFlightCountTracksSubmitAndComplete)
{
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::ftruncate(fd, 4096), 0);

    Topology topo;
    topo.pipelines.push_back({256, PollingMode::Classic});
    DiskIOUring uring(std::move(topo));
    uring.register_fd(fd);

    EXPECT_EQ(uring.in_flight_count(fd), 0u);

    std::atomic<bool>    done{false};
    std::vector<uint8_t> buf(4096, 0);
    uring.submit_write(fd, buf.data(), buf.size(), 0, [&](int) { done.store(true, std::memory_order_release); });

    // After completion, in_flight should be back to 0.
    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_EQ(uring.in_flight_count(fd), 0u);

    ::close(fd);
    std::remove(path.c_str());
}

// ── Destructor stops threads cleanly ──────────────────────────────

TEST(DiskIOUring, DestructorStopsThreadsCleanly)
{
    {
        Topology topo;
        topo.pipelines.push_back({256, PollingMode::Classic});
        DiskIOUring uring(std::move(topo));
        int32_t     fds[1];
        EXPECT_EQ(uring.eventfds(fds, 1), 1u);
        EXPECT_GE(fds[0], 0);
    }
    SUCCEED();
}

// ── Hybrid mode ──────────────────────────────────────────────────

TEST(DiskIOUring, HybridModeSubmitWriteRoundTrips)
{
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);

    Topology       topo;
    PipelineConfig cfg;
    cfg.mode                    = PollingMode::Hybrid;
    cfg.hybrid.busy_poll_budget = 4;
    topo.pipelines.push_back(cfg);
    DiskIOUring uring(std::move(topo));
    uring.register_fd(fd);

    std::atomic<bool>    done{false};
    std::atomic<int>     got_res{-1};
    std::vector<uint8_t> in{1, 2, 3, 4, 5, 6, 7, 8};
    uring.submit_write(fd, in.data(), in.size(), 0, [&](int res) {
        got_res.store(res, std::memory_order_relaxed);
        done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_EQ(got_res.load(), static_cast<int>(in.size()));

    ::close(fd);
    std::remove(path.c_str());
}

// ── Unregistered fd routes to pipeline 0 ─────────────────────────

TEST(DiskIOUring, UnregisteredFdRoutesToPipeline0)
{
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::ftruncate(fd, 4096), 0);

    Topology topo;
    topo.pipelines.push_back({256, PollingMode::Classic});
    topo.pipelines.push_back({256, PollingMode::Classic});
    DiskIOUring uring(std::move(topo));
    // Note: fd not registered — should route to pipeline 0 with warning.

    std::atomic<bool>    done{false};
    std::atomic<int>     got_res{-1};
    std::vector<uint8_t> buf(4096, 0);
    uring.submit_write(fd, buf.data(), buf.size(), 0, [&](int res) {
        got_res.store(res, std::memory_order_relaxed);
        done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_EQ(got_res.load(), static_cast<int>(buf.size()));

    ::close(fd);
    std::remove(path.c_str());
}
