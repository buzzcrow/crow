// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// PT9: IU block alignment (9.1-9.3) + debug store/codec on real frames (9.5).
#include "crowdb-tree/crowdb-tree.h"
#include "crowdb-tree/debug_codec.h"
#include "crowdb-tree/page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <map>
#include <memory>
#include <string>
#include <vector>

using namespace crowdb::tree;

namespace
{
Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

std::string make_key(int i)
{
    std::array<char, 16> b{};
    snprintf(b.data(), b.size(), "key%05d", i);
    return b.data();
}

void fill_buf(Crowdbtree *t, int K, std::map<std::string, std::string> *oracle)
{
    for (int i = 0; i < K; ++i) {
        std::string v = "val" + std::to_string(i);
        ASSERT_TRUE(t->apply(i + 1, put_one(make_key(i), v)).ok());
        ASSERT_TRUE(t->flush().ok());
        (*oracle)[make_key(i)] = v;
    }
}
} // namespace

TEST(Alignment, Iu4096CheckpointReopenEquals)
{
    MemPageStore store(4096); // aligned block device
    Options      opt;
    opt.page_store       = &store;
    opt.frame_bytes      = 4096; // frame_bytes % iu == 0
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 200; // multi-level tree

    std::map<std::string, std::string> oracle;
    {
        Crowdbtree t(opt);
        fill_buf(&t, 200, &oracle);
        ASSERT_GT(t.height(), 1);
        ASSERT_TRUE(t.snapshot(nullptr).ok());
        // Every durable extent is IU-aligned + IU-sized, so the file is a 4 KiB
        // multiple.
        EXPECT_EQ(store.size() % 4096U, 0U);
    }

    std::unique_ptr<Crowdbtree> t2;
    ASSERT_TRUE(Crowdbtree::open(opt, &t2).ok());
    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
}

TEST(Alignment, Iu4096AllocatorReuseStaysAligned)
{
    MemPageStore store(4096);
    Options      opt;
    opt.page_store       = &store;
    opt.frame_bytes      = 4096;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 200;

    std::map<std::string, std::string> oracle;
    Crowdbtree                           t(opt);
    fill_buf(&t, 80, &oracle);
    ASSERT_TRUE(t.snapshot(nullptr).ok());
    uint64_t early = store.size();

    // Rewrite the same keys repeatedly; aligned gaps are reused so the file stays
    // a 4 KiB multiple and roughly flat.
    uint64_t slot = 100000;
    for (int round = 0; round < 30; ++round) {
        for (int i : {1, 2, 3, 4, 5}) {
            ASSERT_TRUE(t.apply(slot, put_one(make_key(i), "r" + std::to_string(round))).ok());
            ASSERT_TRUE(t.flush().ok());
            ++slot;
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok());
        EXPECT_EQ(store.size() % 4096U, 0U);
    }
    EXPECT_LE(store.size(), early + (static_cast<uint64_t>(16) * 4096U));
}

TEST(Alignment, RejectsFrameNotIuAligned)
{
    // The only geometry constraint now is frame_bytes % iu == 0 (the superblock
    // slot is IU-rounded, so any IU is supported).
    MemPageStore store(512);
    Options      opt;
    opt.page_store  = &store;
    opt.frame_bytes = 4097; // not a multiple of 512
    std::unique_ptr<Crowdbtree> t;
    EXPECT_EQ(Crowdbtree::open(opt, &t).code(), Code::kInvalidArgument);
}

// Larger-than-4096 IU (e.g. 16 KiB SSD): the superblock slot grows to the IU,
// so snapshot/reopen round-trips with IU-sized, IU-aligned extents.
TEST(Alignment, LargeIu16KCheckpointReopen)
{
    MemPageStore store(16384);
    Options      opt;
    opt.page_store       = &store;
    opt.frame_bytes      = 16384; // frame_bytes % iu == 0
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 200; // multi-level tree

    std::map<std::string, std::string> oracle;
    {
        Crowdbtree t(opt);
        fill_buf(&t, 150, &oracle);
        ASSERT_GT(t.height(), 1);
        ASSERT_TRUE(t.snapshot(nullptr).ok());
        EXPECT_EQ(store.size() % 16384U, 0U); // every extent IU-aligned + IU-sized
        // Two superblock slots of 16 KiB precede the page region.
        EXPECT_GE(store.size(), 2U * 16384U);
    }
    std::unique_ptr<Crowdbtree> t2;
    ASSERT_TRUE(Crowdbtree::open(opt, &t2).ok());
    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
}

// A non-power-of-two IU that does not divide 4096 (previously rejected) now
// works because the superblock slot is rounded up to the IU.
TEST(Alignment, NonPowerOfTwoIuRoundTrip)
{
    MemPageStore store(5000);
    Options      opt;
    opt.page_store       = &store;
    opt.frame_bytes      = 10000; // 10000 % 5000 == 0
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 300;
    std::map<std::string, std::string> oracle;
    {
        Crowdbtree t(opt);
        fill_buf(&t, 80, &oracle);
        ASSERT_TRUE(t.snapshot(nullptr).ok());
        EXPECT_EQ(store.size() % 5000U, 0U);
    }
    std::unique_ptr<Crowdbtree> t2;
    ASSERT_TRUE(Crowdbtree::open(opt, &t2).ok());
    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
}

TEST(Alignment, DebugStoreTransparentRoundTrip)
{
    MemPageStore   inner(1);
    DebugPageStore dbg(&inner);
    Options        opt;
    opt.page_store       = &dbg;
    opt.frame_bytes      = 4096;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 200;

    std::map<std::string, std::string> oracle;
    {
        Crowdbtree t(opt);
        fill_buf(&t, 120, &oracle);
        ASSERT_TRUE(t.snapshot(nullptr).ok());
        EXPECT_GT(dbg.writes(), 0U);
    }
    std::unique_ptr<Crowdbtree> t2;
    ASSERT_TRUE(Crowdbtree::open(opt, &t2).ok());
    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
}
