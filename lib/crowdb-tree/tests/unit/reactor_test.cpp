// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// plan-tree #11: DiskIOUring (io_uring event loop) + BlockAsyncPageStore.
// Only built when CMake found liburing (see CMakeLists.txt's
// CROWDB_HAVE_LIBURING gate) -- io_uring is Linux-only.
#include "crowdb-common/diskio_uring.h"
#include "crowdb-tree/async_page_store.h"
#include "crowdb-tree/block_page_store.h"
#include "test_tmp.h"

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
#include <utility>
#include <vector>

using namespace crowdb::tree;
using crowdb::common::DiskIOUring;
using crowdb::common::PipelineConfig;
using crowdb::common::PollingMode;
using crowdb::common::Topology;

namespace
{
std::string temp_path()
{
    std::string root = crowdb::tree_test::test_tmp_root();
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

// Build a single-pipeline DiskIOUring (the common case for tree tests).
DiskIOUring make_uring()
{
    Topology       topo;
    PipelineConfig cfg;
    cfg.entries = 256;
    cfg.mode    = PollingMode::Hybrid;
    topo.pipelines.push_back(cfg);
    return DiskIOUring(std::move(topo));
}

// Bounded poll for a background-thread-set flag, matching the style already
// used in other tests: no condvar wiring for the test's own synchronization,
// just a short sleep loop with a generous overall deadline.
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

TEST(DiskIOUring, SubmitReadCompletesViaCallback)
{
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    std::vector<uint8_t> expected{10, 20, 30, 40, 50, 60, 70, 80};
    ASSERT_EQ(::pwrite(fd, expected.data(), expected.size(), 0), static_cast<ssize_t>(expected.size()));

    DiskIOUring uring = make_uring();
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

TEST(DiskIOUring, SubmitWriteThenReadRoundTrips)
{
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);

    DiskIOUring uring = make_uring();
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

    // Confirms the poll thread actually performed the write (not just
    // invoked the callback with a fabricated success) -- read back with a
    // plain pread, bypassing the uring entirely.
    std::vector<uint8_t> out(in.size(), 0);
    ASSERT_EQ(::pread(fd, out.data(), out.size(), 100), static_cast<ssize_t>(out.size()));
    EXPECT_EQ(out, in);

    ::close(fd);
    std::remove(path.c_str());
}

TEST(DiskIOUring, MultipleConcurrentSubmitsAllComplete)
{
    constexpr int kOps = 64;
    std::string   path = temp_path();
    int           fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::ftruncate(fd, static_cast<off_t>(kOps) * 16), 0);

    DiskIOUring uring = make_uring();
    uring.register_fd(fd);
    std::atomic<int> completed{0};
    std::vector<int> results(kOps, -1);
    // Each op writes a distinct 16-byte pattern at a distinct offset so a
    // wrong callback<->op mapping (e.g. always invoking op 0's callback)
    // shows up as either a missing completion or wrong bytes on read-back.
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

    ASSERT_TRUE(wait_for([&] { return completed.load(std::memory_order_acquire) == kOps; }, /*max_iters=*/400));
    for (int i = 0; i < kOps; ++i) {
        EXPECT_EQ(results[i], 16) << "op " << i;
    }

    for (int i = 0; i < kOps; ++i) {
        std::vector<uint8_t> out(16, 0);
        ASSERT_EQ(::pread(fd, out.data(), out.size(), static_cast<off_t>(i) * 16), 16);
        EXPECT_EQ(out, patterns[i]) << "op " << i;
    }

    ::close(fd);
    std::remove(path.c_str());
}

TEST(DiskIOUring, DestructorStopsThreadCleanly)
{
    {
        DiskIOUring uring = make_uring();
        int32_t     efd   = -1;
        EXPECT_EQ(uring.eventfds(&efd, 1), 1u);
        EXPECT_GE(efd, 0);
    }
    SUCCEED();
}

// ── BlockAsyncPageStore (async twin of BlockPageStore) ───────────

TEST(BlockAsyncPageStore, WriteThenReadRoundTrips)
{
    std::string                     path  = temp_path();
    DiskIOUring                     uring = make_uring();
    std::unique_ptr<BlockPageStore> bs;
    ASSERT_TRUE(BlockPageStore::open(path, 4096, &bs).ok());
    // Register the store's fd with the uring.
    for (int fd : bs->all_extent_fds()) {
        uring.register_fd(fd);
    }
    BlockAsyncPageStore s(bs.get(), &uring);

    // O_DIRECT requires aligned offset/length/buffer; use 4096-aligned I/O.
    std::vector<uint8_t> in(4096, 0);
    for (size_t i = 0; i < in.size(); ++i) {
        in[i] = static_cast<uint8_t>(i & 0xFF);
    }
    std::atomic<bool> write_done{false};
    Status            write_status;
    s.submit_write(0, in.data(), in.size(), [&](Status st) {
        write_status = std::move(st);
        write_done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return write_done.load(std::memory_order_acquire); }));
    EXPECT_TRUE(write_status.ok()) << write_status.to_string();

    std::vector<uint8_t> out(in.size(), 0);
    std::atomic<bool>    read_done{false};
    Status               read_status;
    s.submit_read(0, out.data(), out.size(), [&](Status st) {
        read_status = std::move(st);
        read_done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return read_done.load(std::memory_order_acquire); }));
    EXPECT_TRUE(read_status.ok()) << read_status.to_string();
    EXPECT_EQ(out, in);

    std::remove(path.c_str());
}

TEST(BlockAsyncPageStore, ReadPastEndSurfacesAsError)
{
    std::string                     path  = temp_path();
    DiskIOUring                     uring = make_uring();
    std::unique_ptr<BlockPageStore> bs;
    ASSERT_TRUE(BlockPageStore::open(path, 4096, &bs).ok());
    for (int fd : bs->all_extent_fds()) {
        uring.register_fd(fd);
    }
    BlockAsyncPageStore s(bs.get(), &uring);

    // Read from an offset far past the file end (aligned to keep O_DIRECT happy
    // for the offset, but there's no data there).
    std::vector<uint8_t> out(4096, 0);
    std::atomic<bool>    done{false};
    Status               status;
    s.submit_read(1 << 20, out.data(), out.size(), [&](Status st) {
        status = std::move(st);
        done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_FALSE(status.ok());

    std::remove(path.c_str());
}

TEST(BlockAsyncPageStore, FsyncCompletes)
{
    std::string                     path  = temp_path();
    DiskIOUring                     uring = make_uring();
    std::unique_ptr<BlockPageStore> bs;
    ASSERT_TRUE(BlockPageStore::open(path, 4096, &bs).ok());
    for (int fd : bs->all_extent_fds()) {
        uring.register_fd(fd);
    }
    BlockAsyncPageStore s(bs.get(), &uring);

    // Write something first so the single-medium fd is dirty
    std::vector<uint8_t> in(4096, 0xAB);
    std::atomic<bool>    write_done{false};
    s.submit_write(0, in.data(), in.size(), [&](Status) { write_done.store(true, std::memory_order_release); });
    ASSERT_TRUE(wait_for([&] { return write_done.load(std::memory_order_acquire); }));

    std::atomic<bool> done{false};
    Status            status;
    Status            submit_status = s.submit_fsync([&](Status st) {
        status = std::move(st);
        done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(submit_status.ok());
    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_TRUE(status.ok()) << status.to_string();

    std::remove(path.c_str());
}

TEST(BlockAsyncPageStore, AllExtentFdsReturnsAllLiveFds)
{
    std::string                     path = temp_path();
    std::unique_ptr<BlockPageStore> bs;
    ASSERT_TRUE(BlockPageStore::open(path, 4096, &bs).ok());
    std::vector<int> fds = bs->all_extent_fds();
    // Single-medium mode: exactly one fd.
    EXPECT_EQ(fds.size(), 1u);
    EXPECT_GE(fds[0], 0);
    std::remove(path.c_str());
}
