// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// BlockingEngine tests: write/read/fsync round-trip, concurrent writes,
// null disk error handling.
#include "disk/disk.h"
#include "disk/types.h"
#include "engine/blocking/blocking_engine.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

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
    std::string root = "/tmp/crow-diskio-blocking-tests";
    std::filesystem::create_directories(root);
    char tmpl[128];
    std::snprintf(tmpl, sizeof(tmpl), "%s/dx_XXXXXX", root.c_str());
    std::vector<char> buf(tmpl, tmpl + std::strlen(tmpl) + 1);
    int               fd = mkstemp(buf.data());
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

class TestDisk : public crow::diskio::Disk
{
  public:
    TestDisk(crow::diskio::DiskId id, const std::string &path, crow::diskio::IoEngine *engine)
        : id_(id),
          fd_(::open(path.c_str(), O_RDWR)),
          engine_(engine)
    {
        zones_.push_back({0, 0, 1 << 24});
    }

    ~TestDisk() override
    {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    crow::diskio::DiskType type() const override
    {
        return crow::diskio::DiskType::Block;
    }

    int fd() const override
    {
        return fd_;
    }

    bool is_o_direct() const override
    {
        return false;
    }

    size_t block_size() const override
    {
        return 1;
    }

    crow::diskio::IoEngine *engine() override
    {
        return engine_;
    }

    crow::diskio::DiskId id() const override
    {
        return id_;
    }

    crow::diskio::Zone *find_zone(uint32_t zone_index) override
    {
        for (auto &z : zones_) {
            if (z.zone_index == zone_index) {
                return &z;
            }
        }
        return nullptr;
    }

  private:
    crow::diskio::DiskId    id_;
    int                     fd_;
    crow::diskio::IoEngine *engine_;
};
} // namespace

TEST(BlockingEngine, WriteReadRoundTrip)
{
    std::string path = temp_path();
    ASSERT_EQ(::truncate(path.c_str(), 1 << 16), 0);

    crow::diskio::BlockingEngine engine(4);
    TestDisk                     disk({1, 1}, path, &engine);

    std::vector<uint8_t> in(4096);
    for (size_t i = 0; i < in.size(); ++i) {
        in[i] = static_cast<uint8_t>(i & 0xFF);
    }
    std::atomic<bool> done{false};
    std::atomic<int>  got_res{-1};
    engine.submit_write(&disk, 0, in.data(), in.size(), [&](int res) {
        got_res.store(res, std::memory_order_relaxed);
        done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_EQ(got_res.load(), static_cast<int>(in.size()));

    std::vector<uint8_t> out(in.size(), 0);
    done.store(false);
    engine.submit_read(&disk, 0, out.data(), out.size(), 0, [&](int res) {
        got_res.store(res, std::memory_order_relaxed);
        done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_EQ(got_res.load(), static_cast<int>(out.size()));
    EXPECT_EQ(out, in);

    engine.stop();
    std::remove(path.c_str());
}

TEST(BlockingEngine, FsyncAfterWrite)
{
    std::string path = temp_path();
    ASSERT_EQ(::truncate(path.c_str(), 4096), 0);

    crow::diskio::BlockingEngine engine(2);
    TestDisk                     disk({2, 2}, path, &engine);

    std::vector<uint8_t> in(4096, 0xAB);
    std::atomic<bool>    write_done{false};
    engine.submit_write(&disk, 0, in.data(), in.size(),
                        [&](int) { write_done.store(true, std::memory_order_release); });
    ASSERT_TRUE(wait_for([&] { return write_done.load(std::memory_order_acquire); }));

    std::atomic<bool> sync_done{false};
    std::atomic<int>  sync_res{-1};
    engine.submit_fsync(&disk, [&](int res) {
        sync_res.store(res, std::memory_order_relaxed);
        sync_done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return sync_done.load(std::memory_order_acquire); }));
    EXPECT_GE(sync_res.load(), 0);

    engine.stop();
    std::remove(path.c_str());
}

TEST(BlockingEngine, ConcurrentWritesAllComplete)
{
    std::string path = temp_path();
    ASSERT_EQ(::truncate(path.c_str(), 100 * 4096), 0);

    crow::diskio::BlockingEngine engine(4);
    TestDisk                     disk({3, 3}, path, &engine);

    constexpr int        kOps = 50;
    std::atomic<int>     completed{0};
    std::vector<uint8_t> buf(4096, 0);
    for (int i = 0; i < kOps; ++i) {
        buf[0]  = static_cast<uint8_t>(i);
        auto *b = new std::vector<uint8_t>(buf);
        engine.submit_write(&disk, static_cast<off_t>(i) * 4096, b->data(), b->size(), [b, &completed](int) {
            completed.fetch_add(1, std::memory_order_acq_rel);
            delete b;
        });
    }
    ASSERT_TRUE(wait_for([&] { return completed.load(std::memory_order_acquire) == kOps; }, 400));

    for (int i = 0; i < kOps; ++i) {
        std::vector<uint8_t> out(4096, 0);
        ASSERT_EQ(::pread(disk.fd(), out.data(), out.size(), static_cast<off_t>(i) * 4096), 4096);
        EXPECT_EQ(out[0], static_cast<uint8_t>(i)) << "op " << i;
    }

    engine.stop();
    std::remove(path.c_str());
}

TEST(BlockingEngine, NullDiskReturnsError)
{
    crow::diskio::BlockingEngine engine(2);
    std::atomic<int>             got_res{0};
    engine.submit_write(nullptr, 0, nullptr, 0, [&](int res) { got_res.store(res); });
    EXPECT_EQ(got_res.load(), -EBADF);
    engine.stop();
}

TEST(BlockingEngine, StopJoinsAllThreads)
{
    std::string path = temp_path();
    ASSERT_EQ(::truncate(path.c_str(), 4096), 0);

    {
        crow::diskio::BlockingEngine engine(4);
        TestDisk                     disk({4, 4}, path, &engine);
        std::vector<uint8_t>         in(4096, 0xCD);
        std::atomic<bool>            done{false};
        engine.submit_write(&disk, 0, in.data(), in.size(), [&](int) { done.store(true, std::memory_order_release); });
        ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    } // engine destructor calls stop()

    std::remove(path.c_str());
    SUCCEED() << "Destructor stopped and joined all threads cleanly";
}
