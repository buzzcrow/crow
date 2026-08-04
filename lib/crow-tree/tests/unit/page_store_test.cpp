// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// PT1: PageStore backend tests (MemPageStore + BlockPageStore).
#include "crow-tree/block_page_store.h"
#include "crow-tree/page_store.h"
#include "test_tmp.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

using namespace crow::tree;

namespace
{
std::string temp_path()
{
    std::string root = crow::tree_test::test_tmp_root();
    std::filesystem::create_directories(root);
    std::array<char, 128> tmpl{};
    std::snprintf(tmpl.data(), tmpl.size(), "%s/ps_XXXXXX", root.c_str());
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    int fd = mkstemp(buf.data());
    if (fd >= 0) {
        close(fd);
    }
    return buf.data();
}
} // namespace

TEST(PageStore, MemRoundTrip)
{
    MemPageStore         s(1);
    std::vector<uint8_t> in{1, 2, 3, 4, 5};
    ASSERT_TRUE(s.write_at(100, in.data(), in.size()).ok());
    std::vector<uint8_t> out(in.size(), 0);
    ASSERT_TRUE(s.read_at(100, out.data(), out.size()).ok());
    EXPECT_EQ(in, out);
    EXPECT_GE(s.size(), 105U);
}

TEST(PageStore, MemReadPastEndFails)
{
    MemPageStore           s(1);
    std::array<uint8_t, 4> b{};
    EXPECT_FALSE(s.read_at(0, b.data(), b.size()).ok());
}

TEST(PageStore, MemOverwrite)
{
    MemPageStore         s(1);
    std::vector<uint8_t> a{9, 9, 9};
    ASSERT_TRUE(s.write_at(0, a.data(), a.size()).ok());
    std::vector<uint8_t> b{1, 2};
    ASSERT_TRUE(s.write_at(0, b.data(), b.size()).ok());
    std::vector<uint8_t> out(3, 0);
    ASSERT_TRUE(s.read_at(0, out.data(), out.size()).ok());
    EXPECT_EQ(out[0], 1);
    EXPECT_EQ(out[1], 2);
    EXPECT_EQ(out[2], 9);
}

TEST(PageStore, FileRoundTripAcrossReopen)
{
    std::string dir = temp_path();
    std::remove(dir.c_str());
    ASSERT_EQ(::mkdir(dir.c_str(), 0755), 0);
    std::vector<uint8_t> in{10, 20, 30, 40};
    {
        std::unique_ptr<BlockPageStore> s;
        ASSERT_TRUE(BlockPageStore::open_blocks(dir, 0, 0, 4096, 1, &s).ok());
        ASSERT_TRUE(s->write_at(8, in.data(), in.size()).ok());
        ASSERT_TRUE(s->sync().ok());
        EXPECT_EQ(s->iu_size(), 1U);
    }
    {
        std::unique_ptr<BlockPageStore> s;
        ASSERT_TRUE(BlockPageStore::open_blocks(dir, 0, 0, 4096, 1, &s).ok());
        std::vector<uint8_t> out(in.size(), 0);
        ASSERT_TRUE(s->read_at(8, out.data(), out.size()).ok());
        EXPECT_EQ(in, out);
    }
}

// plan-tree #22: BlockPageStore (O_DIRECT). Backed by a regular
// pre-allocated file in these tests (no real block device available), which
// exercises the exact same O_DIRECT alignment path a raw device would.
TEST(PageStore, BlockDeviceAlignedRoundTripAcrossReopen)
{
    std::string path = temp_path();
    // Heap-allocated: not guaranteed aligned, but offset/len are IU-aligned
    // (4096) here, so only the buffer-address alignment check can differ
    // across runs -- either path (direct or bounced) must round-trip.
    std::vector<uint8_t> in(4096, 0xAB);
    {
        std::unique_ptr<BlockPageStore> s;
        ASSERT_TRUE(BlockPageStore::open(path, 4096, &s).ok());
        EXPECT_FALSE(s->is_block_device());
        ASSERT_TRUE(s->write_at(4096, in.data(), in.size()).ok());
        ASSERT_TRUE(s->sync().ok());
        EXPECT_EQ(s->iu_size(), 4096U);
    }
    {
        std::unique_ptr<BlockPageStore> s;
        ASSERT_TRUE(BlockPageStore::open(path, 4096, &s).ok());
        std::vector<uint8_t> out(in.size(), 0);
        ASSERT_TRUE(s->read_at(4096, out.data(), out.size()).ok());
        EXPECT_EQ(in, out);
    }
    std::remove(path.c_str());
}

TEST(PageStore, BlockDeviceUnalignedWriteReadBounces)
{
    std::string                     path = temp_path();
    std::unique_ptr<BlockPageStore> s;
    ASSERT_TRUE(BlockPageStore::open(path, 4096, &s).ok());

    // Offset 10, length 7: neither aligned to the 4096 IU -- must bounce
    // through the bounce-buffer read-modify-write path.
    std::vector<uint8_t> in{1, 2, 3, 4, 5, 6, 7};
    ASSERT_TRUE(s->write_at(10, in.data(), in.size()).ok());

    std::vector<uint8_t> out(in.size(), 0);
    ASSERT_TRUE(s->read_at(10, out.data(), out.size()).ok());
    EXPECT_EQ(in, out);
    std::remove(path.c_str());
}

TEST(PageStore, BlockDeviceUnalignedWritePreservesSurroundingBytes)
{
    std::string                     path = temp_path();
    std::unique_ptr<BlockPageStore> s;
    ASSERT_TRUE(BlockPageStore::open(path, 4096, &s).ok());

    // Fill one full IU with a known pattern (aligned write).
    std::vector<uint8_t> block(4096, 0xFF);
    ASSERT_TRUE(s->write_at(0, block.data(), block.size()).ok());

    // Overwrite a small unaligned sub-range in the middle -- the
    // read-modify-write bounce path must preserve every byte outside
    // [100, 105) in this IU, not zero-fill the whole span.
    std::vector<uint8_t> patch{9, 9, 9, 9, 9};
    ASSERT_TRUE(s->write_at(100, patch.data(), patch.size()).ok());

    std::vector<uint8_t> out(block.size(), 0);
    ASSERT_TRUE(s->read_at(0, out.data(), out.size()).ok());
    for (size_t i = 0; i < out.size(); ++i) {
        if (i >= 100 && i < 105) {
            EXPECT_EQ(out[i], 9U) << "at " << i;
        }
        else {
            EXPECT_EQ(out[i], 0xFF) << "at " << i;
        }
    }
    std::remove(path.c_str());
}

TEST(PageStore, BlockDeviceReadPastEndFails)
{
    std::string                     path = temp_path();
    std::unique_ptr<BlockPageStore> s;
    ASSERT_TRUE(BlockPageStore::open(path, 4096, &s).ok());
    std::vector<uint8_t> out(16, 0);
    EXPECT_FALSE(s->read_at(0, out.data(), out.size()).ok());
    std::remove(path.c_str());
}

// plan-tree #14e: FaultyPageStore fault-injection wrapper.
TEST(FaultyPageStore, PassesThroughWithNoFaultArmed)
{
    MemPageStore    inner(1);
    FaultyPageStore s(&inner);

    std::vector<uint8_t> in{1, 2, 3, 4};
    ASSERT_TRUE(s.write_at(0, in.data(), in.size()).ok());
    ASSERT_TRUE(s.sync().ok());
    std::vector<uint8_t> out(in.size(), 0);
    ASSERT_TRUE(s.read_at(0, out.data(), out.size()).ok());
    EXPECT_EQ(in, out);
    EXPECT_EQ(s.write_count(), 1);
    EXPECT_EQ(s.sync_count(), 1);
}

TEST(FaultyPageStore, DropWriteNeverReachesInner)
{
    MemPageStore    inner(1);
    FaultyPageStore s(&inner);

    std::vector<uint8_t> a{1, 2, 3};
    ASSERT_TRUE(s.write_at(0, a.data(), a.size()).ok()); // write #0: unarmed, lands

    s.arm_write_fault(1, FaultyPageStore::Fault::kDrop);
    std::vector<uint8_t> b{9, 9, 9};
    ASSERT_TRUE(s.write_at(0, b.data(), b.size()).ok()); // write #1: dropped, reports Ok anyway

    std::vector<uint8_t> out(3, 0);
    ASSERT_TRUE(inner.read_at(0, out.data(), out.size()).ok());
    EXPECT_EQ(out, a) << "dropped write must never reach inner_";

    // One-shot: a third write is unarmed again.
    std::vector<uint8_t> c{5, 5, 5};
    ASSERT_TRUE(s.write_at(0, c.data(), c.size()).ok());
    ASSERT_TRUE(inner.read_at(0, out.data(), out.size()).ok());
    EXPECT_EQ(out, c);
}

TEST(FaultyPageStore, TearWriteTruncatesToTearLen)
{
    MemPageStore    inner(1);
    FaultyPageStore s(&inner);
    inner.write_at(0, std::vector<uint8_t>(6, 0xAA).data(), 6); // pre-fill

    s.arm_write_fault(0, FaultyPageStore::Fault::kTear, /*tear_len=*/3);
    std::vector<uint8_t> in{1, 2, 3, 4, 5, 6};
    ASSERT_TRUE(s.write_at(0, in.data(), in.size()).ok());

    std::vector<uint8_t> out(6, 0);
    ASSERT_TRUE(inner.read_at(0, out.data(), out.size()).ok());
    EXPECT_EQ(out[0], 1);
    EXPECT_EQ(out[1], 2);
    EXPECT_EQ(out[2], 3);
    // Bytes past tear_len keep whatever was there before (the pre-fill) --
    // a torn write, not a truncated-then-zero-filled one.
    EXPECT_EQ(out[3], 0xAA);
    EXPECT_EQ(out[4], 0xAA);
    EXPECT_EQ(out[5], 0xAA);
}

TEST(FaultyPageStore, FailWriteReturnsErrorWithoutTouchingInner)
{
    MemPageStore    inner(1);
    FaultyPageStore s(&inner);
    s.arm_write_fault(0, FaultyPageStore::Fault::kFail);

    std::vector<uint8_t> in{1, 2, 3};
    EXPECT_FALSE(s.write_at(0, in.data(), in.size()).ok());
    EXPECT_EQ(inner.size(), 0U);
}

TEST(FaultyPageStore, FailSyncReturnsError)
{
    MemPageStore    inner(1);
    FaultyPageStore s(&inner);
    ASSERT_TRUE(s.sync().ok()); // sync #0: unarmed
    s.arm_sync_fault(1, FaultyPageStore::Fault::kFail);
    EXPECT_FALSE(s.sync().ok()); // sync #1: armed, fails
    EXPECT_TRUE(s.sync().ok());  // sync #2: one-shot, back to normal
}

// Task 2: BlockPageStore::open_mem() — MemoryMedium with IU=1.
TEST(BlockPageStoreMem, RoundTrip)
{
    std::unique_ptr<BlockPageStore> s;
    ASSERT_TRUE(BlockPageStore::open_mem(1, &s).ok());
    EXPECT_EQ(s->iu_size(), 1U);
    EXPECT_FALSE(s->is_block_device());

    std::vector<uint8_t> in{1, 2, 3, 4, 5};
    ASSERT_TRUE(s->write_at(100, in.data(), in.size()).ok());
    std::vector<uint8_t> out(in.size(), 0);
    ASSERT_TRUE(s->read_at(100, out.data(), out.size()).ok());
    EXPECT_EQ(in, out);
    EXPECT_GE(s->size(), 105U);
}

TEST(BlockPageStoreMem, Overwrite)
{
    std::unique_ptr<BlockPageStore> s;
    ASSERT_TRUE(BlockPageStore::open_mem(1, &s).ok());

    std::vector<uint8_t> a{9, 9, 9};
    ASSERT_TRUE(s->write_at(0, a.data(), a.size()).ok());
    std::vector<uint8_t> b{1, 2};
    ASSERT_TRUE(s->write_at(0, b.data(), b.size()).ok());
    std::vector<uint8_t> out(3, 0);
    ASSERT_TRUE(s->read_at(0, out.data(), out.size()).ok());
    EXPECT_EQ(out[0], 1);
    EXPECT_EQ(out[1], 2);
    EXPECT_EQ(out[2], 9);
}

TEST(BlockPageStoreMem, ReadPastEndFails)
{
    std::unique_ptr<BlockPageStore> s;
    ASSERT_TRUE(BlockPageStore::open_mem(1, &s).ok());
    std::array<uint8_t, 4> b{};
    EXPECT_FALSE(s->read_at(0, b.data(), b.size()).ok());
}

TEST(BlockPageStoreMem, SyncIsNoOp)
{
    std::unique_ptr<BlockPageStore> s;
    ASSERT_TRUE(BlockPageStore::open_mem(1, &s).ok());
    EXPECT_TRUE(s->sync().ok());
}

TEST(BlockPageStoreMem, ContentVerification)
{
    std::unique_ptr<BlockPageStore> s;
    ASSERT_TRUE(BlockPageStore::open_mem(1, &s).ok());

    std::vector<uint8_t> in{0xAA, 0xBB, 0xCC};
    ASSERT_TRUE(s->write_at(50, in.data(), in.size()).ok());

    auto *mem = dynamic_cast<MemoryMedium *>(s->medium());
    ASSERT_NE(mem, nullptr);
    const auto &data = mem->data();
    EXPECT_EQ(data.size(), 53U);
    EXPECT_EQ(data[50], 0xAA);
    EXPECT_EQ(data[51], 0xBB);
    EXPECT_EQ(data[52], 0xCC);
}

// Task 3: Array-of-blocks tests
namespace
{
std::string temp_dir()
{
    std::string root = crow::tree_test::test_tmp_root();
    std::filesystem::create_directories(root);
    std::array<char, 128> tmpl{};
    std::snprintf(tmpl.data(), tmpl.size(), "%s/blk_XXXXXX", root.c_str());
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    char *d = mkdtemp(buf.data());
    if (d == nullptr) {
        return root + "/blk_fallback";
    }
    return d;
}
} // namespace

TEST(BlockArray, WriteWithinFirstBlock)
{
    std::string                     dir = temp_dir();
    std::unique_ptr<BlockPageStore> s;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir, 1, 0, 4096, 1, &s).ok());
    EXPECT_EQ(s->num_extents(), 1U);
    EXPECT_EQ(s->block_size(), 4096U);

    std::vector<uint8_t> in{1, 2, 3, 4, 5};
    ASSERT_TRUE(s->write_at(100, in.data(), in.size()).ok());
    std::vector<uint8_t> out(in.size(), 0);
    ASSERT_TRUE(s->read_at(100, out.data(), out.size()).ok());
    EXPECT_EQ(in, out);
    EXPECT_EQ(s->num_extents(), 1U);
}

TEST(BlockArray, WriteExceedsOneBlockCreatesSecond)
{
    std::string                     dir = temp_dir();
    std::unique_ptr<BlockPageStore> s;
    constexpr uint64_t              blk = 4096;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir, 1, 0, blk, 1, &s).ok());

    // Write 100 bytes starting at offset blk-50 → spans into block 1
    std::vector<uint8_t> in(100, 0x42);
    ASSERT_TRUE(s->write_at(blk - 50, in.data(), in.size()).ok());
    EXPECT_EQ(s->num_extents(), 2U);

    std::vector<uint8_t> out(in.size(), 0);
    ASSERT_TRUE(s->read_at(blk - 50, out.data(), out.size()).ok());
    EXPECT_EQ(in, out);
}

TEST(BlockArray, Write20MiBWith8MiBBlocks)
{
    std::string                     dir = temp_dir();
    std::unique_ptr<BlockPageStore> s;
    constexpr uint64_t              blk = 8 * 1024 * 1024;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir, 0, 0, blk, 1, &s).ok());

    // Write 20 MiB of data
    std::vector<uint8_t> in(20 * 1024 * 1024, 0xAB);
    ASSERT_TRUE(s->write_at(0, in.data(), in.size()).ok());
    EXPECT_EQ(s->num_extents(), 3U); // 8+8+4 MiB → 3 blocks

    std::vector<uint8_t> out(in.size(), 0);
    ASSERT_TRUE(s->read_at(0, out.data(), out.size()).ok());
    EXPECT_EQ(in, out);
}

TEST(BlockArray, ReopenAfterWrites)
{
    std::string          dir = temp_dir();
    constexpr uint64_t   blk = 4096;
    std::vector<uint8_t> in(100, 0x77);

    {
        std::unique_ptr<BlockPageStore> s;
        ASSERT_TRUE(BlockPageStore::open_blocks(dir, 2, 3, blk, 1, &s).ok());
        ASSERT_TRUE(s->write_at(0, in.data(), in.size()).ok());
        ASSERT_TRUE(s->write_at(blk + 10, in.data(), in.size()).ok());
        ASSERT_TRUE(s->sync().ok());
        EXPECT_EQ(s->num_extents(), 2U);
    }
    {
        std::unique_ptr<BlockPageStore> s;
        ASSERT_TRUE(BlockPageStore::open_blocks(dir, 2, 3, blk, 1, &s).ok());
        EXPECT_EQ(s->num_extents(), 2U);

        std::vector<uint8_t> out(in.size(), 0);
        ASSERT_TRUE(s->read_at(0, out.data(), out.size()).ok());
        EXPECT_EQ(in, out);
        ASSERT_TRUE(s->read_at(blk + 10, out.data(), out.size()).ok());
        EXPECT_EQ(in, out);
    }
}

TEST(BlockArray, DumpUtility)
{
    std::string                     dir = temp_dir();
    std::unique_ptr<BlockPageStore> s;
    ASSERT_TRUE(BlockPageStore::open_blocks(dir, 0, 0, 4096, 1, &s).ok());

    std::vector<uint8_t> in{0xDE, 0xAD, 0xBE, 0xEF};
    ASSERT_TRUE(s->write_at(0, in.data(), in.size()).ok());
    ASSERT_TRUE(s->sync().ok());

    std::string dump;
    ASSERT_TRUE(dump_block_file(dir + "/0-0.blk-0000", 1, &dump).ok());
    EXPECT_NE(dump.find("Block file"), std::string::npos);
    EXPECT_NE(dump.find("de ad be ef"), std::string::npos);
}
