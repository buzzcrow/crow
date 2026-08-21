// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// DiskSet + FileDisk tests: add/find_disk, unknown disk returns nullptr,
// shutdown clears the map.
#include "disk/disk_set.h"
#include "disk/file_disk.h"
#include "disk/mem_disk.h"
#include "disk/types.h"
#include "engine/blocking/blocking_engine.h"
#include "engine/dummy/dummy_engine.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace
{
std::string temp_path()
{
    std::string root = "/tmp/crow-diskio-diskset-tests";
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
} // namespace

TEST(DiskSet, AddAndFindDisk)
{
    std::string path = temp_path();
    ASSERT_EQ(::truncate(path.c_str(), 4096), 0);

    auto                            engine = std::make_unique<crow::diskio::BlockingEngine>(2);
    std::vector<crow::diskio::Zone> zones;
    zones.push_back({0, 0, 1 << 24});
    auto disk =
        std::make_shared<crow::diskio::FileDisk>(crow::diskio::DiskId{1, 1}, path, std::move(engine), std::move(zones));

    crow::diskio::DiskSet set;
    set.add(disk);
    EXPECT_EQ(set.size(), 1u);

    auto found = set.find_disk({1, 1});
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->id(), (crow::diskio::DiskId{1, 1}));
    EXPECT_EQ(found->type(), crow::diskio::DiskType::File);

    set.shutdown();
    EXPECT_EQ(set.size(), 0u);
    std::remove(path.c_str());
}

TEST(DiskSet, UnknownDiskReturnsNullptr)
{
    crow::diskio::DiskSet set;
    EXPECT_EQ(set.find_disk({99, 99}), nullptr);
    EXPECT_EQ(set.size(), 0u);
}

TEST(DiskSet, MultipleDisksAllFindable)
{
    std::string path1 = temp_path();
    std::string path2 = temp_path();
    ASSERT_EQ(::truncate(path1.c_str(), 4096), 0);
    ASSERT_EQ(::truncate(path2.c_str(), 4096), 0);

    auto                            engine1 = std::make_unique<crow::diskio::BlockingEngine>(2);
    auto                            engine2 = std::make_unique<crow::diskio::BlockingEngine>(2);
    std::vector<crow::diskio::Zone> zones;
    zones.push_back({0, 0, 1 << 24});

    auto disk1 = std::make_shared<crow::diskio::FileDisk>(crow::diskio::DiskId{1, 1}, path1, std::move(engine1), zones);
    auto disk2 = std::make_shared<crow::diskio::FileDisk>(crow::diskio::DiskId{2, 2}, path2, std::move(engine2), zones);

    crow::diskio::DiskSet set;
    set.add(disk1);
    set.add(disk2);
    EXPECT_EQ(set.size(), 2u);

    EXPECT_NE(set.find_disk({1, 1}), nullptr);
    EXPECT_NE(set.find_disk({2, 2}), nullptr);
    EXPECT_EQ(set.find_disk({3, 3}), nullptr);

    set.shutdown();
    std::remove(path1.c_str());
    std::remove(path2.c_str());
}

TEST(FileDisk, WriteReadRoundTripViaEngine)
{
    std::string path = temp_path();
    ASSERT_EQ(::truncate(path.c_str(), 1 << 16), 0);

    auto                            engine     = std::make_unique<crow::diskio::BlockingEngine>(4);
    auto                           *engine_ptr = engine.get();
    std::vector<crow::diskio::Zone> zones;
    zones.push_back({0, 0, 1 << 24});
    auto disk =
        std::make_shared<crow::diskio::FileDisk>(crow::diskio::DiskId{5, 5}, path, std::move(engine), std::move(zones));

    std::vector<uint8_t> in(4096);
    for (size_t i = 0; i < in.size(); ++i) {
        in[i] = static_cast<uint8_t>(i & 0xFF);
    }
    std::atomic<bool> done{false};
    std::atomic<int>  got_res{-1};
    engine_ptr->submit_write(disk.get(), 0, in.data(), in.size(), [&](int res) {
        got_res.store(res, std::memory_order_relaxed);
        done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_EQ(got_res.load(), 4096);

    std::vector<uint8_t> out(4096, 0);
    done.store(false);
    engine_ptr->submit_read(disk.get(), 0, out.data(), out.size(), [&](int res) {
        got_res.store(res, std::memory_order_relaxed);
        done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_EQ(got_res.load(), 4096);
    EXPECT_EQ(out, in);

    std::remove(path.c_str());
}

TEST(FileDisk, FindZoneReturnsCorrectZone)
{
    std::string path = temp_path();
    ASSERT_EQ(::truncate(path.c_str(), 4096), 0);

    auto                            engine = std::make_unique<crow::diskio::BlockingEngine>(1);
    std::vector<crow::diskio::Zone> zones;
    zones.push_back({0, 0, 4096});
    zones.push_back({1, 4096, 4096});
    zones.push_back({2, 8192, 4096});

    auto disk =
        std::make_shared<crow::diskio::FileDisk>(crow::diskio::DiskId{6, 6}, path, std::move(engine), std::move(zones));

    auto *z0 = disk->find_zone(0);
    auto *z1 = disk->find_zone(1);
    auto *z2 = disk->find_zone(2);
    auto *z3 = disk->find_zone(3);

    ASSERT_NE(z0, nullptr);
    ASSERT_NE(z1, nullptr);
    ASSERT_NE(z2, nullptr);
    EXPECT_EQ(z3, nullptr);
    EXPECT_EQ(z0->base_offset, 0);
    EXPECT_EQ(z1->base_offset, 4096);
    EXPECT_EQ(z2->base_offset, 8192);

    std::remove(path.c_str());
}
