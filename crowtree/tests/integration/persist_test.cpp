// PT3-PT5: snapshot + recovery + durable round-trip integration tests.
#include "crowtree/crowtree.h"
#include "crowtree/page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <map>
#include <memory>
#include <random>
#include <string>
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
    int  fd     = mkstemp(tmpl.data());
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
