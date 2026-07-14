// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// PT3-PT5: snapshot + recovery + durable round-trip integration tests.
#include "crowtree/block_page_store.h"
#include "crowtree/crowtree.h"
#include "crowtree/page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace crowtree;

namespace
{
Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

Batch del_one(const std::string &k)
{
    return Batch{{{.key = k, .kind = OpKind::kDelete, .value = ""}}};
}

std::string make_key(int i)
{
    std::array<char, 16> buf{};
    snprintf(buf.data(), buf.size(), "key%05d", i);
    return buf.data();
}

// Superblock slot size from persist.cc (each A/B slot is 4 KiB).
constexpr uint64_t kSbBytes = 4096;

// plan-tree #8: blocks the *first* write_at() call until release() is
// called, then passes every write through unblocked. Lets a test park
// snapshot()'s I/O phase mid-flight and assert something else (apply()/
// flush()) can still make progress concurrently -- which is only possible
// if write_mutex_ was already released before this call.
class BlockingPageStore : public PageStore
{
  public:
    explicit BlockingPageStore(PageStore *inner) : inner_(inner)
    {
    }

    Status write_at(uint64_t off, const uint8_t *buf, size_t len) override
    {
        {
            std::unique_lock<std::mutex> lk(mu_);
            if (!first_write_seen_) {
                first_write_seen_ = true;
                entered_.notify_all();
                cv_.wait(lk, [this] { return released_; });
            }
        }
        return inner_->write_at(off, buf, len);
    }

    Status read_at(uint64_t off, uint8_t *buf, size_t len) const override
    {
        return inner_->read_at(off, buf, len);
    }

    Status sync() override
    {
        return inner_->sync();
    }

    [[nodiscard]] uint64_t size() const override
    {
        return inner_->size();
    }

    [[nodiscard]] uint32_t iu_size() const override
    {
        return inner_->iu_size();
    }

    // Blocks the calling thread until the first write_at() call has entered
    // (and is itself now blocked awaiting release()).
    void wait_until_blocked()
    {
        std::unique_lock<std::mutex> lk(mu_);
        entered_.wait(lk, [this] { return first_write_seen_; });
    }

    void release()
    {
        std::lock_guard<std::mutex> lk(mu_);
        released_ = true;
        cv_.notify_all();
    }

  private:
    PageStore              *inner_;
    mutable std::mutex      mu_;
    std::condition_variable entered_;
    std::condition_variable cv_;
    bool                    first_write_seen_ = false;
    bool                    released_         = false;
};
} // namespace

TEST(Persist, LazyRecoveryDemandLoadsOnAccess)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;   // consolidate into base frames
    opt.leaf_split_bytes = 200; // multi-level tree -> many pages
    opt.frame_bytes      = 4096;
    std::map<std::string, std::string> oracle;
    {
        Crowtree t(opt);
        for (int i = 0; i < 80; ++i) {
            ASSERT_TRUE(t.apply(i + 1, put_one(make_key(i), "val" + std::to_string(i))).ok());
            oracle[make_key(i)] = "val" + std::to_string(i);
        }
        ASSERT_TRUE(t.flush().ok());
        ASSERT_TRUE(t.snapshot(nullptr).ok());
    }

    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
    // Lazy: recovery only recorded page_id->addr tags; nothing is resident yet.
    ASSERT_NE(t2->buffer_pool(), nullptr);
    EXPECT_EQ(t2->buffer_pool()->stats().used, 0U);

    // First access demand-loads the pages along the descent path into the pool.
    std::string v;
    uint64_t    s;
    ASSERT_TRUE(t2->get(Slice(make_key(0)), &s, &v));
    EXPECT_EQ(v, "val0");
    EXPECT_GT(t2->buffer_pool()->stats().used, 0U);

    // Every key reads back correctly through demand load.
    for (const auto &kv : oracle) {
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
}

TEST(Persist, CheckpointThenReopenRestoresKeys)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store = &store;

    {
        Crowtree t(opt);
        for (int i = 0; i < 50; ++i) {
            ASSERT_TRUE(t.apply(i + 1, put_one(make_key(i), "v" + std::to_string(i))).ok());
        }
        ASSERT_TRUE(t.flush().ok());
        uint64_t durable = 0;
        ASSERT_TRUE(t.snapshot(&durable).ok());
        EXPECT_EQ(durable, 50U);
    }

    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
    EXPECT_EQ(t2->last_applied_slot(), 50U);
    for (int i = 0; i < 50; ++i) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(make_key(i)), &s, &v)) << "missing " << make_key(i);
        EXPECT_EQ(v, "v" + std::to_string(i));
    }
}

// clear() must wipe every key and reset watermarks back
// to a fresh empty tree, in-memory only (no persist() call here) -- proving
// the wipe itself, independent of durability.
TEST(Persist, ClearWipesLiveTree)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store = &store;
    Crowtree     t(opt);

    for (int i = 0; i < 20; ++i) {
        ASSERT_TRUE(t.apply(i + 1, put_one(make_key(i), "v" + std::to_string(i))).ok());
    }
    ASSERT_TRUE(t.flush().ok());
    EXPECT_EQ(t.last_applied_slot(), 20U);

    ASSERT_TRUE(t.clear().ok());

    EXPECT_EQ(t.last_applied_slot(), 0U);
    EXPECT_EQ(t.contiguous_slot(), 0U);
    for (int i = 0; i < 20; ++i) {
        std::string v;
        uint64_t    s;
        EXPECT_FALSE(t.get(Slice(make_key(i)), &s, &v)) << "key " << make_key(i) << " should be gone after clear()";
    }

    // The wiped tree is a genuinely fresh tree, not just "empty of these
    // particular keys": re-applying a slot number already seen before the
    // wipe must succeed (received_slots_/max_seen_slot_ reset too), and a
    // brand-new key is visible immediately.
    ASSERT_TRUE(t.apply(1, put_one("after-clear", "x")).ok());
    std::string v;
    uint64_t    s;
    ASSERT_TRUE(t.get(Slice("after-clear"), &s, &v));
    EXPECT_EQ(v, "x");
    EXPECT_EQ(s, 1U);
}

// clear() alone is not durable (matching install_snapshot's own contract) --
// only an explicit flush()+snapshot() after it persists the wipe. This is
// the crash-safety-relevant half of G3: a close + reopen after that persist
// must not resurrect the pre-clear keys.
TEST(Persist, ClearThenSnapshotReopenIsEmpty)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store = &store;

    {
        Crowtree t(opt);
        for (int i = 0; i < 20; ++i) {
            ASSERT_TRUE(t.apply(i + 1, put_one(make_key(i), "v" + std::to_string(i))).ok());
        }
        ASSERT_TRUE(t.flush().ok());
        uint64_t durable = 0;
        ASSERT_TRUE(t.snapshot(&durable).ok());
        EXPECT_EQ(durable, 20U);

        ASSERT_TRUE(t.clear().ok());
        durable = 123; // sentinel; snapshot() on an empty tree must overwrite it with 0
        ASSERT_TRUE(t.snapshot(&durable).ok());
        EXPECT_EQ(durable, 0U);
    }

    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
    EXPECT_EQ(t2->last_applied_slot(), 0U);
    for (int i = 0; i < 20; ++i) {
        std::string v;
        uint64_t    s;
        EXPECT_FALSE(t2->get(Slice(make_key(i)), &s, &v))
            << "key " << make_key(i) << " must not survive clear() + snapshot() + reopen";
    }
}

TEST(Persist, MultiLevelTreeSurvivesAndComparesEqual)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 200; // force a multi-level tree

    std::shared_ptr<Snapshot> before;
    {
        Crowtree t(opt);
        for (int i = 0; i < 300; ++i) {
            ASSERT_TRUE(t.apply(i + 1, put_one(make_key(i), "payload-" + std::to_string(i))).ok());
            ASSERT_TRUE(t.flush().ok());
        }
        ASSERT_GT(t.height(), 1);
        ASSERT_TRUE(t.snapshot().ok());
        before = t.snapshot_view();
    }

    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
    EXPECT_GT(t2->height(), 1);
    auto after = t2->snapshot_view();
    EXPECT_TRUE(before->compare(*after).empty());
    EXPECT_EQ(before->size(), after->size());
}

TEST(Persist, ReapplyOldSlotsIsNoOp)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store = &store;
    {
        Crowtree t(opt);
        ASSERT_TRUE(t.apply(5, put_one("a", "A5")).ok());
        t.force_advance_slot(5);
        ASSERT_TRUE(t.flush().ok());
        ASSERT_TRUE(t.snapshot().ok());
    }
    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
    // Re-applying a slot <= last_applied_slot must not regress the value.
    ASSERT_TRUE(t2->apply(3, put_one("a", "STALE")).ok());
    ASSERT_TRUE(t2->flush().ok());
    std::string v;
    uint64_t    s;
    ASSERT_TRUE(t2->get(Slice("a"), &s, &v));
    EXPECT_EQ(v, "A5");
    EXPECT_EQ(s, 5U);
}

TEST(Persist, FreshOpenWithNoSuperblock)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store = &store;
    std::unique_ptr<Crowtree> t;
    ASSERT_TRUE(Crowtree::open(opt, &t).ok());
    EXPECT_EQ(t->last_applied_slot(), 0U);
    std::string v;
    uint64_t    s;
    EXPECT_FALSE(t->get(Slice("nope"), &s, &v));
    // Usable after a fresh open.
    ASSERT_TRUE(t->apply(1, put_one("x", "X")).ok());
    ASSERT_TRUE(t->flush().ok());
    ASSERT_TRUE(t->get(Slice("x"), &s, &v));
    EXPECT_EQ(v, "X");
}

TEST(Persist, CorruptNewestSuperblockFallsBackToPrevious)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store = &store;
    {
        Crowtree t(opt);
        ASSERT_TRUE(t.apply(1, put_one("a", "first")).ok());
        ASSERT_TRUE(t.flush().ok());
        ASSERT_TRUE(t.snapshot().ok()); // seq 1 -> slot 0

        ASSERT_TRUE(t.apply(2, put_one("a", "second")).ok());
        ASSERT_TRUE(t.flush().ok());
        ASSERT_TRUE(t.snapshot().ok()); // seq 2 -> slot kSbBytes
    }
    // Corrupt the newest superblock slot (seq 2 lives at the second slot).
    std::vector<uint8_t> garbage(kSbBytes, 0xab);
    ASSERT_TRUE(store.write_at(kSbBytes, garbage.data(), garbage.size()).ok());

    std::unique_ptr<Crowtree> t2;
    ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
    // Recovery falls back to seq 1.
    EXPECT_EQ(t2->last_applied_slot(), 1U);
    std::string v;
    uint64_t    s;
    ASSERT_TRUE(t2->get(Slice("a"), &s, &v));
    EXPECT_EQ(v, "first");
}

TEST(Persist, FileBackendRoundTrip)
{
    std::array<char, 29> tmpl{"/tmp/crowtree_persist_XXXXXX"};
    int                  fd = mkstemp(tmpl.data());
    ASSERT_GE(fd, 0);
    close(fd);
    std::string path(tmpl.data());

    std::map<std::string, std::string> oracle;
    {
        std::unique_ptr<FilePageStore> store;
        ASSERT_TRUE(FilePageStore::open(path, 4096, &store).ok());
        Options opt;
        opt.page_store       = store.get();
        opt.leaf_split_bytes = 256;
        Crowtree     t(opt);
        std::mt19937 rng(7);
        uint64_t     slot = 0;
        for (int i = 0; i < 200; ++i) {
            ++slot;
            std::string k = make_key(static_cast<int>(rng() % 120));
            if ((rng() % 5) == 0) {
                ASSERT_TRUE(t.apply(slot, del_one(k)).ok());
                oracle.erase(k);
            }
            else {
                std::string val = "v" + std::to_string(slot);
                ASSERT_TRUE(t.apply(slot, put_one(k, val)).ok());
                oracle[k] = val;
            }
            if (i % 9 == 0) {
                ASSERT_TRUE(t.flush().ok());
            }
        }
        ASSERT_TRUE(t.flush().ok());
        ASSERT_TRUE(t.snapshot().ok());
    }

    // Reopen the file in a brand-new store + engine.
    {
        std::unique_ptr<FilePageStore> store;
        ASSERT_TRUE(FilePageStore::open(path, 4096, &store).ok());
        Options opt;
        opt.page_store = store.get();
        std::unique_ptr<Crowtree> t;
        ASSERT_TRUE(Crowtree::open(opt, &t).ok());
        for (auto &kv : oracle) {
            std::string v;
            uint64_t    s;
            ASSERT_TRUE(t->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
            EXPECT_EQ(v, kv.second);
        }
        // The snapshot retains tombstones (gc_floor = 0), so compare live entries.
        auto   snap = t->snapshot_view();
        size_t live = 0;
        for (const auto &e : snap->entries()) {
            if (!CellView{Slice(e.cell)}.is_tombstone()) {
                ++live;
            }
        }
        EXPECT_EQ(live, oracle.size());
    }
    std::remove(path.c_str());
}

// plan-tree #22: same round-trip as FileBackendRoundTrip, but against the
// O_DIRECT BlockPageStore -- proves a real Crowtree engine's actual
// snapshot/recovery write/read pattern (superblocks, manifest, page
// frames -- whatever offsets/lengths persist.cpp happens to use) round-trips
// correctly through BlockPageStore's alignment handling, not just the
// synthetic offsets exercised directly in page_store_test.cpp.
TEST(Persist, BlockDeviceBackendRoundTrip)
{
    std::array<char, 29> tmpl{"/tmp/crowtree_persist_XXXXXX"};
    int                  fd = mkstemp(tmpl.data());
    ASSERT_GE(fd, 0);
    close(fd);
    std::string path(tmpl.data());

    std::map<std::string, std::string> oracle;
    {
        std::unique_ptr<BlockPageStore> store;
        ASSERT_TRUE(BlockPageStore::open(path, 4096, &store).ok());
        Options opt;
        opt.page_store       = store.get();
        opt.leaf_split_bytes = 256;
        Crowtree     t(opt);
        std::mt19937 rng(7);
        uint64_t     slot = 0;
        for (int i = 0; i < 200; ++i) {
            ++slot;
            std::string k = make_key(static_cast<int>(rng() % 120));
            if ((rng() % 5) == 0) {
                ASSERT_TRUE(t.apply(slot, del_one(k)).ok());
                oracle.erase(k);
            }
            else {
                std::string val = "v" + std::to_string(slot);
                ASSERT_TRUE(t.apply(slot, put_one(k, val)).ok());
                oracle[k] = val;
            }
            if (i % 9 == 0) {
                ASSERT_TRUE(t.flush().ok());
            }
        }
        ASSERT_TRUE(t.flush().ok());
        ASSERT_TRUE(t.snapshot().ok());
    }

    // Reopen the file in a brand-new store + engine.
    {
        std::unique_ptr<BlockPageStore> store;
        ASSERT_TRUE(BlockPageStore::open(path, 4096, &store).ok());
        Options opt;
        opt.page_store = store.get();
        std::unique_ptr<Crowtree> t;
        ASSERT_TRUE(Crowtree::open(opt, &t).ok());
        for (auto &kv : oracle) {
            std::string v;
            uint64_t    s;
            ASSERT_TRUE(t->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
            EXPECT_EQ(v, kv.second);
        }
        auto   snap = t->snapshot_view();
        size_t live = 0;
        for (const auto &e : snap->entries()) {
            if (!CellView{Slice(e.cell)}.is_tombstone()) {
                ++live;
            }
        }
        EXPECT_EQ(live, oracle.size());
    }
    std::remove(path.c_str());
}

// plan-tree #8: write_mutex_ must be released before snapshot()'s I/O phase
// (prepare_snapshot_locked's CPU-only walk holds it; the actual
// write_at/sync calls must not). Park the first write_at() call mid-flight
// on a background thread, then prove apply()+flush() -- flush() needs
// write_mutex_ -- still completes on the main thread without waiting for
// the parked write to unblock. If write_mutex_ were still held across the
// I/O phase, flush() would deadlock against this test's own timeout.
TEST(Persist, WriteMutexNotHeldDuringSnapshotIo)
{
    MemPageStore      mem(1);
    BlockingPageStore store(&mem);
    Options           opt;
    opt.page_store = &store;

    Crowtree t(opt);
    ASSERT_TRUE(t.apply(1, put_one("a", "1")).ok());
    ASSERT_TRUE(t.flush().ok());

    std::thread snap_thread([&] {
        Status s = t.snapshot(nullptr);
        EXPECT_TRUE(s.ok());
    });
    store.wait_until_blocked(); // snapshot()'s I/O phase is now parked

    // If write_mutex_ were still held here, this would block until
    // snap_thread's I/O unblocks and commit_prepared_snapshot() releases it
    // -- defeating the whole point of the async-writable persistence phase.
    ASSERT_TRUE(t.apply(2, put_one("b", "2")).ok());
    ASSERT_TRUE(t.flush().ok());
    std::string v;
    uint64_t    s;
    EXPECT_TRUE(t.get(Slice("b"), &s, &v));
    EXPECT_EQ(v, "2");

    store.release();
    snap_thread.join();
}

// Task 6: Array-of-blocks BlockPageStore integration test. Snapshot with
// enough data to span multiple block files, reopen, recover, and verify all
// data is intact. Uses small block_size (8 KiB) to force multi-block with
// modest data volume.
TEST(Persist, ArrayOfBlocksSnapshotReopenRecover)
{
    std::array<char, 32> tmpl{"/tmp/crowtree_blkarr_XXXXXX"};
    char                *d = mkdtemp(tmpl.data());
    ASSERT_NE(d, nullptr);
    std::string dir(d);

    constexpr uint64_t blk = 8 * 1024; // 8 KiB blocks
    std::map<std::string, std::string> oracle;
    {
        std::unique_ptr<BlockPageStore> store;
        ASSERT_TRUE(BlockPageStore::open_blocks(dir, 0, 0, blk, 1, &store).ok());
        Options opt;
        opt.page_store       = store.get();
        opt.leaf_split_bytes = 256;
        Crowtree     t(opt);
        std::mt19937 rng(42);
        uint64_t     slot = 0;
        for (int i = 0; i < 300; ++i) {
            ++slot;
            std::string k = make_key(static_cast<int>(rng() % 200));
            if ((rng() % 5) == 0) {
                ASSERT_TRUE(t.apply(slot, del_one(k)).ok());
                oracle.erase(k);
            } else {
                std::string val = "v" + std::to_string(slot);
                ASSERT_TRUE(t.apply(slot, put_one(k, val)).ok());
                oracle[k] = val;
            }
            if (i % 10 == 0) {
                ASSERT_TRUE(t.flush().ok());
            }
        }
        ASSERT_TRUE(t.flush().ok());
        ASSERT_TRUE(t.snapshot().ok());
        // Verify multiple blocks were created
        EXPECT_GE(store->num_extents(), 2U);
    }

    // Reopen with a fresh store + engine, verify all data
    {
        std::unique_ptr<BlockPageStore> store;
        ASSERT_TRUE(BlockPageStore::open_blocks(dir, 0, 0, blk, 1, &store).ok());
        Options opt;
        opt.page_store = store.get();
        std::unique_ptr<Crowtree> t;
        ASSERT_TRUE(Crowtree::open(opt, &t).ok());
        for (auto &kv : oracle) {
            std::string v;
            uint64_t    s;
            ASSERT_TRUE(t->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
            EXPECT_EQ(v, kv.second);
        }
        auto   snap = t->snapshot_view();
        size_t live = 0;
        for (const auto &e : snap->entries()) {
            if (!CellView{Slice(e.cell)}.is_tombstone()) {
                ++live;
            }
        }
        EXPECT_EQ(live, oracle.size());
    }
}

// Task 6: Array-of-blocks with dump utility content verification.
TEST(Persist, ArrayOfBlocksDumpVerification)
{
    std::array<char, 32> tmpl{"/tmp/crowtree_blkdump_XXXXXX"};
    char                *d = mkdtemp(tmpl.data());
    ASSERT_NE(d, nullptr);
    std::string dir(d);

    constexpr uint64_t blk = 8 * 1024;
    {
        std::unique_ptr<BlockPageStore> store;
        ASSERT_TRUE(BlockPageStore::open_blocks(dir, 0, 0, blk, 1, &store).ok());
        Options opt;
        opt.page_store = store.get();
        opt.leaf_split_bytes = 256;
        Crowtree t(opt);
        ASSERT_TRUE(t.apply(1, put_one("a", "1")).ok());
        ASSERT_TRUE(t.apply(2, put_one("b", "2")).ok());
        ASSERT_TRUE(t.flush().ok());
        ASSERT_TRUE(t.snapshot().ok());
    }

    // Verify block 0 exists and dump contains anchor data
    std::string dump;
    ASSERT_TRUE(dump_block_file(dir + "/0-0.blk-0000", 1, &dump).ok());
    EXPECT_NE(dump.find("Block file"), std::string::npos);
    // The anchor magic bytes (0x41435443 = 'CTCA') should appear in the hex dump
    EXPECT_NE(dump.find("43 54 43 41"), std::string::npos);
}
