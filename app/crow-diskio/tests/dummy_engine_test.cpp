// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// DummyEngine + MemDisk tests: drop-write, deterministic read, wrap-around,
// logical_object_offset, repeated reads, fsync no-op.
#include "disk/mem_disk.h"
#include "disk/types.h"
#include "engine/dummy/dummy_engine.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <optional>
#include <vector>

namespace
{
crow::diskio::MemDisk make_test_disk(crow::diskio::DiskId id, size_t max_read_size)
{
    std::vector<crow::diskio::Zone> zones;
    zones.push_back({0, 0, 1 << 24});
    return crow::diskio::MemDisk(id, std::move(zones), max_read_size);
}
} // namespace

TEST(DummyEngine, WriteIsDroppedAndReturnsSuccess)
{
    auto                      disk = make_test_disk({1, 1}, 4096);
    crow::diskio::DummyEngine engine;
    std::vector<uint8_t>      data(4096, 0xAB);
    int                       res = -1;
    engine.submit_write(&disk, 0, data.data(), data.size(), [&](int r) { res = r; });
    EXPECT_EQ(res, static_cast<int>(data.size()));
}

TEST(DummyEngine, ReadReturnsDeterministicContent)
{
    auto                      disk = make_test_disk({2, 2}, 4096);
    crow::diskio::DummyEngine engine;
    std::vector<uint8_t>      out1(4096, 0);
    std::vector<uint8_t>      out2(4096, 0);
    int                       res1 = -1;
    int                       res2 = -1;
    engine.submit_read(&disk, 0, out1.data(), out1.size(), [&](int r) { res1 = r; });
    engine.submit_read(&disk, 0, out2.data(), out2.size(), [&](int r) { res2 = r; });
    EXPECT_EQ(res1, 4096);
    EXPECT_EQ(res2, 4096);
    EXPECT_EQ(out1, out2); // deterministic: same range = same bytes
}

TEST(DummyEngine, ReadWithLogicalObjectOffsetProducesDifferentContent)
{
    auto                      disk = make_test_disk({3, 3}, 4096);
    crow::diskio::DummyEngine engine1(std::make_optional<uint64_t>(0));
    crow::diskio::DummyEngine engine2(std::make_optional<uint64_t>(1));
    std::vector<uint8_t>      out1(4096, 0);
    std::vector<uint8_t>      out2(4096, 0);
    engine1.submit_read(&disk, 0, out1.data(), out1.size(), [&](int) {});
    engine2.submit_read(&disk, 0, out2.data(), out2.size(), [&](int) {});
    // Different logical_object_offset should produce different content.
    EXPECT_NE(out1, out2);
}

TEST(DummyEngine, ReadWrapAroundProducesValidContent)
{
    // Small pattern_len (4096) with read at offset beyond pattern_len.
    auto                      disk = make_test_disk({4, 4}, 4096);
    crow::diskio::DummyEngine engine;
    std::vector<uint8_t>      out(4096, 0);
    int                       res = -1;
    // Read at offset 5000 — wraps around the 4096-byte pattern.
    engine.submit_read(&disk, 5000, out.data(), out.size(), [&](int r) { res = r; });
    EXPECT_EQ(res, 4096);
    // Verify the content is deterministic: read the same range again.
    std::vector<uint8_t> out2(4096, 0);
    engine.submit_read(&disk, 5000, out2.data(), out2.size(), [&](int) {});
    EXPECT_EQ(out, out2);
}

TEST(DummyEngine, FsyncIsNoOp)
{
    auto                      disk = make_test_disk({5, 5}, 4096);
    crow::diskio::DummyEngine engine;
    int                       res = -1;
    engine.submit_fsync(&disk, [&](int r) { res = r; });
    EXPECT_EQ(res, 0);
}

TEST(DummyEngine, NullDiskReadReturnsError)
{
    crow::diskio::DummyEngine engine;
    int                       res = 0;
    engine.submit_read(nullptr, 0, nullptr, 0, [&](int r) { res = r; });
    EXPECT_EQ(res, -EBADF);
}

TEST(DummyEngine, LargeReadWithWrapAround)
{
    // max_read_size = 4096, read 8192 bytes — should wrap around twice.
    auto                      disk = make_test_disk({6, 6}, 4096);
    crow::diskio::DummyEngine engine;
    std::vector<uint8_t>      out(8192, 0);
    int                       res = -1;
    engine.submit_read(&disk, 0, out.data(), out.size(), [&](int r) { res = r; });
    EXPECT_EQ(res, 8192);
    // First 4096 bytes should equal bytes 4096..8191 (pattern repeats).
    EXPECT_EQ(std::memcmp(out.data(), out.data() + 4096, 4096), 0);
}
