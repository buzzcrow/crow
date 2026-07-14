// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Crash / torn-write recovery across the full feature set (compression, overflow,
// IU alignment). Verifies two-generation fallback (a corrupted newest snapshot
// falls back intact to the previous committed image) and that demand-load
// corruption of the committed image is surfaced via the latched io_failed flag.
#include "crowtree/block_page_store.h"
#include "crowtree/crowtree.h"
#include "crowtree/page_store.h"
#include "test_tmp.h"

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <cstdio>
#include <map>
#include <memory>
#include <string>
#include <vector>

using namespace crowtree;

namespace
{
Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

std::string make_key(int i)
{
    std::array<char, 16> b{};
    snprintf(b.data(), b.size(), "k%05d", i);
    return b.data();
}

std::string big_val(size_t n, char c)
{
    return std::string(n, c); // NOLINT(modernize-return-braced-init-list)
}

constexpr uint64_t kSbBytes = 4096;
} // namespace

// plan-tree #14e: FaultyPageStore-driven crash injection (declarative, in
// place of hand-computed byte offsets). A write failing mid-snapshot must
// leave the previous generation fully recoverable -- snapshot() itself
// reports the failure, and no anchor for the failed generation is ever
// written.
TEST(CrashRecovery, FaultInjectedWriteFailureLeavesPreviousGenerationIntact)
{
    MemPageStore    mem(1);
    FaultyPageStore store(&mem);
    Options         opt;
    opt.page_store = &store;

    std::unique_ptr<Crowtree> t;
    ASSERT_TRUE(Crowtree::open(opt, &t).ok());
    for (int i = 0; i < 20; ++i) {
        ASSERT_TRUE(t->apply(i + 1, put_one(make_key(i), "gen1-" + std::to_string(i))).ok());
    }
    ASSERT_TRUE(t->flush().ok());
    ASSERT_TRUE(t->snapshot(nullptr).ok()); // gen1 commits

    int gen1_writes = store.write_count();

    for (int i = 0; i < 20; ++i) {
        ASSERT_TRUE(t->apply(100 + i, put_one(make_key(i), "gen2-" + std::to_string(i))).ok());
    }
    t->force_advance_slot(119); // slots 100..119 are non-contiguous with gen1's 1..20
    ASSERT_TRUE(t->flush().ok());
    // Fail the very first byte this snapshot generation tries to write.
    store.arm_write_fault(gen1_writes, FaultyPageStore::Fault::kFail);
    EXPECT_FALSE(t->snapshot(nullptr).ok()); // gen2 must fail, not silently succeed

    // gen2's anchor was never written -- gen1 is still the last committed
    // generation. Reopen (fault already disarmed -- one-shot) and confirm.
    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
    for (int i = 0; i < 20; ++i) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(make_key(i)), &s, &v));
        EXPECT_EQ(v, "gen1-" + std::to_string(i));
    }
}

// A dirty mapping-table segment's image is silently lost (as if the write
// never landed pre-crash) but every *later* write in this generation --
// including the anchor itself -- still succeeds and commits normally. This
// is the #14c/#14d-specific corruption class the recovery spec
// calls out: "a segment or directory image failing CRC while its anchor was
// committed indicates media corruption -> fail the node out" (§9), as
// opposed to the anchor itself being torn (already covered by the
// byte-corruption tests above, and by construction unaffected by #14c/#14d
// since the anchor keeps the same A/B slot geometry).
TEST(CrashRecovery, DroppedSegmentImageFailsReopenEvenThoughAnchorCommitted)
{
    MemPageStore    mem(1);
    FaultyPageStore store(&mem);
    Options         opt;
    opt.page_store = &store;

    std::unique_ptr<Crowtree> t;
    ASSERT_TRUE(Crowtree::open(opt, &t).ok());
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(t->apply(i + 1, put_one(make_key(i), "gen1-" + std::to_string(i))).ok());
    }
    ASSERT_TRUE(t->flush().ok());
    ASSERT_TRUE(t->snapshot(nullptr).ok()); // gen1 commits; small tree -> exactly 1 segment

    // A single-key edit (no split) dirties exactly one page and one segment;
    // this generation's write sequence is [page write, segment write,
    // directory write]. Drop write #1 (the segment image) specifically.
    int      gen1_writes = store.write_count();
    uint64_t slot        = 100;
    ASSERT_TRUE(t->apply(slot, put_one(make_key(0), "gen2-updated")).ok());
    t->force_advance_slot(slot); // slot 100 is non-contiguous with gen1's 1..5
    ASSERT_TRUE(t->flush().ok());
    store.arm_write_fault(gen1_writes + 1, FaultyPageStore::Fault::kDrop);
    ASSERT_TRUE(t->snapshot(nullptr).ok()) << "the drop is silent -- snapshot() itself must still report success";
    ASSERT_EQ(t->last_snapshot_segments_written(), 1U) << "expected exactly the touched segment to be re-imaged";

    // The anchor now points at a segment image address whose bytes were
    // never actually written -- reopen must detect this as corruption, not
    // silently fall back or (worse) serve wrong data.
    std::unique_ptr<Crowtree> t2;
    EXPECT_FALSE(Crowtree::open(opt, &t2).ok());
}

// ckpt1 (large overflow values + compression) commits; a second snapshot then
// commits new values; corrupting the newest superblock must fall back to ckpt1's
// fully-intact image, including the multi-frame overflow values.
TEST(CrashRecovery, TwoGenerationFallbackWithOverflowAndCompression)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.compression      = compress_algo::kLz4;
    opt.frame_bytes      = 4096;
    opt.max_inline_value = 64; // force overflow chains
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 512;

    std::map<std::string, std::string> gen1;
    {
        Crowtree t(opt);
        uint64_t slot = 0;
        for (int i = 0; i < 20; ++i) {
            ++slot;
            std::string v = big_val(5000 + i, static_cast<char>('A' + (i % 26))); // ~2 frames
            ASSERT_TRUE(t.apply(slot, put_one(make_key(i), v)).ok());
            ASSERT_TRUE(t.flush().ok());
            gen1[make_key(i)] = v;
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok()); // seq 1 -> slot 0

        // Second generation: overwrite a few keys, then snapshot (seq 2 -> slot B).
        for (int i = 0; i < 20; i += 5) {
            ++slot;
            ASSERT_TRUE(t.apply(slot, put_one(make_key(i), big_val(7000, 'Z'))).ok());
            ASSERT_TRUE(t.flush().ok());
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok()); // seq 2 -> slot kSbBytes
    }

    // Simulate a crash that left the newest superblock unreadable.
    std::vector<uint8_t> garbage(kSbBytes, 0x5a);
    ASSERT_TRUE(store.write_at(kSbBytes, garbage.data(), garbage.size()).ok());

    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
    // Falls back to gen1: every key (incl. overflow values) matches the first ckpt.
    for (const auto &kv : gen1) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second) << "value mismatch " << kv.first;
    }
    EXPECT_FALSE(t2->io_failed()); // fallback image is intact, no media fault
}

// Same two-generation fallback on an aligned (iu=4096) block device.
TEST(CrashRecovery, AlignedTwoGenerationFallback)
{
    MemPageStore store(4096);
    Options      opt;
    opt.page_store       = &store;
    opt.frame_bytes      = 4096;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 256;

    std::map<std::string, std::string> gen1;
    {
        Crowtree t(opt);
        uint64_t slot = 0;
        for (int i = 0; i < 60; ++i) {
            ++slot;
            std::string v = "v" + std::to_string(i);
            ASSERT_TRUE(t.apply(slot, put_one(make_key(i), v)).ok());
            ASSERT_TRUE(t.flush().ok());
            gen1[make_key(i)] = v;
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok());
        for (int i = 0; i < 60; i += 7) {
            ++slot;
            ASSERT_TRUE(t.apply(slot, put_one(make_key(i), "new")).ok());
            ASSERT_TRUE(t.flush().ok());
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok());
    }
    std::vector<uint8_t> garbage(kSbBytes, 0xcd);
    ASSERT_TRUE(store.write_at(kSbBytes, garbage.data(), garbage.size()).ok());

    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
    for (const auto &kv : gen1) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
}

// Corrupting a page body in the *committed* image (valid superblock) is a media
// fault: the read degrades to a miss but io_failed() latches true.
TEST(CrashRecovery, DemandLoadCorruptionLatched)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.frame_bytes      = 4096;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 256; // multi-page so a single corruption is localized
    {
        Crowtree t(opt);
        for (int i = 0; i < 60; ++i) {
            ASSERT_TRUE(t.apply(i + 1, put_one(make_key(i), "v" + std::to_string(i))).ok());
            ASSERT_TRUE(t.flush().ok());
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok());
    }
    // Corrupt deep in the page region (well past both superblock slots).
    uint64_t off = (2 * kSbBytes) + 200;
    uint8_t  b   = 0;
    ASSERT_TRUE(store.read_at(off, &b, 1).ok());
    b ^= 0xff;
    ASSERT_TRUE(store.write_at(off, &b, 1).ok());

    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
    EXPECT_FALSE(t2->io_failed()); // nothing loaded yet (lazy recovery)

    // Read every key: the corrupted page demand-load fails CRC and latches.
    for (int i = 0; i < 60; ++i) {
        std::string v;
        uint64_t    s;
        (void)t2->get(Slice(make_key(i)), &s, &v); // some reads succeed, the corrupt one misses
    }
    EXPECT_TRUE(t2->io_failed());

    t2->clear_io_error();
    EXPECT_FALSE(t2->io_failed());
}

// File-backed (real fsync) crash: gen2's snapshot commits, but a crash tears
// its final superblock write (the engine writes pages+manifest, syncs, THEN the
// superblock — so a torn superblock is the realistic mid-commit crash). Reopen
// must fall back to gen1's committed image, fully intact, on a real file.
TEST(CrashRecovery, FileTornSuperblockFallsBack)
{
    crowtree_test::TempDir tmp("crash_");
    ASSERT_FALSE(tmp.path.empty());
    std::string path = tmp.path;

    std::map<std::string, std::string> gen1;
    {
        std::unique_ptr<BlockPageStore> store;
        ASSERT_TRUE(BlockPageStore::open_blocks(path, 0, 0, 8 * 1024 * 1024, 1, &store).ok());
        Options opt;
        opt.page_store       = store.get();
        opt.frame_bytes      = 4096;
        opt.compression      = compress_algo::kLz4;
        opt.max_delta_len    = 1;
        opt.leaf_split_bytes = 256;
        Crowtree t(opt);
        for (int i = 0; i < 80; ++i) {
            ASSERT_TRUE(t.apply(i + 1, put_one(make_key(i), "v" + std::to_string(i))).ok());
            ASSERT_TRUE(t.flush().ok());
            gen1[make_key(i)] = "v" + std::to_string(i);
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok()); // gen1: seq 1 -> superblock slot 0

        for (int i = 0; i < 80; i += 4) {
            ASSERT_TRUE(t.apply(1000 + i, put_one(make_key(i), "GEN2")).ok());
            ASSERT_TRUE(t.flush().ok());
        }
        ASSERT_TRUE(t.snapshot(nullptr).ok()); // gen2: seq 2 -> superblock slot 4096
    }

    // Tear the gen2 superblock (slot B at offset 4096) as a crash would.
    {
        std::unique_ptr<BlockPageStore> store;
        ASSERT_TRUE(BlockPageStore::open_blocks(path, 0, 0, 8 * 1024 * 1024, 1, &store).ok());
        std::vector<uint8_t> garbage(4096, 0x77);
        ASSERT_TRUE(store->write_at(4096, garbage.data(), garbage.size()).ok());
        ASSERT_TRUE(store->sync().ok());
    }

    {
        std::unique_ptr<BlockPageStore> store;
        ASSERT_TRUE(BlockPageStore::open_blocks(path, 0, 0, 8 * 1024 * 1024, 1, &store).ok());
        Options opt;
        opt.page_store  = store.get();
        opt.frame_bytes = 4096;
        opt.compression = compress_algo::kLz4;
        std::unique_ptr<Crowtree> t;
        ASSERT_TRUE(Crowtree::open(opt, &t).ok());
        for (const auto &kv : gen1) {
            std::string v;
            uint64_t    s;
            ASSERT_TRUE(t->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
            EXPECT_EQ(v, kv.second); // gen1 value, not GEN2 (gen2 never committed)
        }
        EXPECT_FALSE(t->io_failed());
    }
}
