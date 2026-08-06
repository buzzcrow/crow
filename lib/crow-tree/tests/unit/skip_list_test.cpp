// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// R50: ConcurrentSkipList unit tests.
#include "crow-tree/skip_list.h"

#include <gtest/gtest.h>

#include <atomic>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace crow::tree;

namespace
{
CellVersion *make_cv(uint64_t slot, uint8_t flags, const std::string &value)
{
    auto *cv  = new CellVersion{};
    cv->slot  = slot;
    cv->flags = flags;
    if ((flags & kFlagTombstone) != 0) {
        cv->cell = encode_cell_buf(slot, OpKind::kDelete);
    }
    else {
        cv->cell = encode_cell_buf(slot, OpKind::kPut, Slice(value));
    }
    return cv;
}

CellVersion *make_cv_simple(uint64_t slot, const std::string &value)
{
    return make_cv(slot, 0, value);
}

// Helper: upsert, return the old version (or nullptr). On rejection, deletes cv.
CellVersion *do_upsert(ConcurrentSkipList &sl, Slice key, CellVersion *cv)
{
    CellVersion *old = nullptr;
    if (!sl.upsert(key, cv, &old)) {
        delete cv; // rejected — caller cleans up
        return nullptr;
    }
    return old;
}
} // namespace

TEST(SkipList, BasicInsertFind)
{
    ConcurrentSkipList sl;
    EXPECT_TRUE(sl.empty());
    EXPECT_EQ(sl.count(), 0U);

    CellVersion *a = make_cv_simple(1, "va");
    EXPECT_EQ(do_upsert(sl, "a", a), nullptr); // new insert
    EXPECT_EQ(sl.count(), 1U);
    EXPECT_FALSE(sl.empty());

    const CellVersion *found = sl.find("a");
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->slot, 1U);
    EXPECT_EQ(CellView{found->cell.slice()}.value().to_string(), "va");

    EXPECT_EQ(sl.find("b"), nullptr);
}

TEST(SkipList, OrderedIteration)
{
    ConcurrentSkipList sl;
    (void)do_upsert(sl, "c", make_cv_simple(1, "vc"));
    (void)do_upsert(sl, "a", make_cv_simple(2, "va"));
    (void)do_upsert(sl, "b", make_cv_simple(3, "vb"));

    auto                     cur = sl.cursor(Slice());
    std::vector<std::string> keys;
    while (cur.valid()) {
        keys.push_back(cur.key().to_string());
        cur.advance();
    }
    ASSERT_EQ(keys.size(), 3U);
    EXPECT_EQ(keys[0], "a");
    EXPECT_EQ(keys[1], "b");
    EXPECT_EQ(keys[2], "c");
}

TEST(SkipList, CursorStartAfter)
{
    ConcurrentSkipList sl;
    (void)do_upsert(sl, "a", make_cv_simple(1, "va"));
    (void)do_upsert(sl, "b", make_cv_simple(2, "vb"));
    (void)do_upsert(sl, "c", make_cv_simple(3, "vc"));
    (void)do_upsert(sl, "d", make_cv_simple(4, "vd"));

    auto                     cur = sl.cursor(Slice("b"));
    std::vector<std::string> keys;
    while (cur.valid()) {
        keys.push_back(cur.key().to_string());
        cur.advance();
    }
    ASSERT_EQ(keys.size(), 2U);
    EXPECT_EQ(keys[0], "c");
    EXPECT_EQ(keys[1], "d");
}

TEST(SkipList, OverwriteHighestSlotWins)
{
    ConcurrentSkipList sl;
    (void)do_upsert(sl, "k", make_cv_simple(5, "v5"));
    CellVersion *old = do_upsert(sl, "k", make_cv_simple(8, "v8"));
    ASSERT_NE(old, nullptr);
    EXPECT_EQ(old->slot, 5U);
    delete old;

    // Lower slot rejected.
    CellVersion *v3   = make_cv_simple(3, "v3");
    CellVersion *old2 = nullptr;
    EXPECT_FALSE(sl.upsert("k", v3, &old2));
    EXPECT_EQ(old2, nullptr);
    delete v3; // rejected — caller cleans up

    const CellVersion *found = sl.find("k");
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->slot, 8U);
    EXPECT_EQ(CellView{found->cell.slice()}.value().to_string(), "v8");
}

TEST(SkipList, OverwriteReturnsOld)
{
    ConcurrentSkipList sl;
    CellVersion       *v1 = make_cv_simple(1, "v1");
    EXPECT_EQ(do_upsert(sl, "k", v1), nullptr);

    CellVersion *v2  = make_cv_simple(2, "v2");
    CellVersion *old = do_upsert(sl, "k", v2);
    ASSERT_NE(old, nullptr);
    EXPECT_EQ(old, v1);
    delete old;

    CellVersion *v3   = make_cv_simple(1, "v3");
    CellVersion *old2 = nullptr;
    EXPECT_FALSE(sl.upsert("k", v3, &old2));
    delete v3;
}

TEST(SkipList, DrainUpTo)
{
    ConcurrentSkipList sl;
    (void)do_upsert(sl, "a", make_cv_simple(1, "va"));
    (void)do_upsert(sl, "b", make_cv_simple(5, "vb"));
    (void)do_upsert(sl, "c", make_cv_simple(3, "vc"));
    (void)do_upsert(sl, "d", make_cv_simple(9, "vd"));

    auto drained = sl.drain_up_to(5);
    ASSERT_EQ(drained.size(), 3U);
    EXPECT_EQ(drained[0].key, "a");
    EXPECT_EQ(drained[0].slot, 1U);
    EXPECT_EQ(drained[1].key, "b");
    EXPECT_EQ(drained[1].slot, 5U);
    EXPECT_EQ(drained[2].key, "c");
    EXPECT_EQ(drained[2].slot, 3U);

    for (auto &e : drained) {
        delete e.cv;
        ConcurrentSkipList::free_node(e.node);
    }

    EXPECT_EQ(sl.count(), 1U);
    EXPECT_NE(sl.find("d"), nullptr);
    EXPECT_EQ(sl.find("a"), nullptr);
}

TEST(SkipList, DrainAll)
{
    ConcurrentSkipList sl;
    (void)do_upsert(sl, "x", make_cv_simple(1, "vx"));
    (void)do_upsert(sl, "y", make_cv_simple(2, "vy"));

    auto drained = sl.drain_all();
    ASSERT_EQ(drained.size(), 2U);
    for (auto &e : drained) {
        delete e.cv;
        ConcurrentSkipList::free_node(e.node);
    }
    EXPECT_TRUE(sl.empty());
}

TEST(SkipList, HotKeyCollapse)
{
    ConcurrentSkipList sl;
    for (uint64_t s = 1; s <= 1000; ++s) {
        CellVersion *cv  = make_cv_simple(s, "v" + std::to_string(s));
        CellVersion *old = do_upsert(sl, "hot", cv);
        delete old;
    }
    EXPECT_EQ(sl.count(), 1U);
    const CellVersion *found = sl.find("hot");
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->slot, 1000U);
}

TEST(SkipList, TombstoneOverwrite)
{
    ConcurrentSkipList sl;
    CellVersion       *v1 = make_cv_simple(1, "v");
    (void)do_upsert(sl, "k", v1);

    CellVersion *del = make_cv(2, kFlagTombstone, "");
    CellVersion *old = do_upsert(sl, "k", del);
    ASSERT_NE(old, nullptr);
    EXPECT_EQ(old, v1);
    delete old;

    const CellVersion *found = sl.find("k");
    ASSERT_NE(found, nullptr);
    EXPECT_TRUE(CellView{found->cell.slice()}.is_tombstone());
}

TEST(SkipList, ConcurrentInsertAndIterate)
{
    ConcurrentSkipList       sl;
    std::atomic<bool>        stop{false};
    std::vector<std::thread> writers;
    writers.reserve(4);
    for (int w = 0; w < 4; ++w) {
        writers.emplace_back([&, w] {
            for (uint64_t s = 1; s <= 1000; ++s) {
                std::string key = "key" + std::to_string(w) + "_" + std::to_string(s);
                (void)do_upsert(sl, key, make_cv_simple(s, "v"));
            }
        });
    }

    std::thread reader([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            auto cur = sl.cursor(Slice());
            while (cur.valid()) {
                (void)cur.key();
                (void)cur.cell_version();
                cur.advance();
            }
        }
    });

    for (auto &t : writers) {
        t.join();
    }
    stop.store(true);
    reader.join();

    EXPECT_EQ(sl.count(), 4000U);
}

TEST(SkipList, ConcurrentOverwriteAndIterate)
{
    // 4 writers all writing to the SAME key — tests versioned overwrite
    // under concurrent iteration. Old versions are collected (not freed
    // during the test) because the reader may still hold a pointer to
    // them; in the real engine they are epoch-retired. Under TSAN this
    // catches any memory-ordering bug in the acquire/release protocol.
    ConcurrentSkipList         sl;
    std::atomic<bool>          stop{false};
    std::vector<CellVersion *> old_versions;
    std::mutex                 old_mu;

    (void)do_upsert(sl, "hot", make_cv_simple(0, "init"));

    std::vector<std::thread> writers;
    writers.reserve(4);
    for (int w = 0; w < 4; ++w) {
        writers.emplace_back([&, w] {
            for (uint64_t s = 1; s <= 2000; ++s) {
                CellVersion *cv  = make_cv_simple(s, "v" + std::to_string(w));
                CellVersion *old = do_upsert(sl, "hot", cv);
                if (old != nullptr) {
                    std::lock_guard lk(old_mu);
                    old_versions.push_back(old);
                }
            }
        });
    }

    std::thread reader([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            auto cur = sl.cursor(Slice());
            while (cur.valid()) {
                const CellVersion *cv = cur.cell_version();
                if (cv != nullptr) {
                    (void)cv->slot;
                }
                cur.advance();
            }
        }
    });

    for (auto &t : writers) {
        t.join();
    }
    stop.store(true);
    reader.join();

    for (CellVersion *cv : old_versions) {
        delete cv;
    }

    EXPECT_EQ(sl.count(), 1U);
    const CellVersion *found = sl.find("hot");
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->slot, 2000U);
}
