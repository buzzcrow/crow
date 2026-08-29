// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// CT7: MemTable (L0) tests — R50 epoch-protected skip-list version.
#include "crowdb-tree/cell.h"
#include "crowdb-tree/epoch.h"
#include "crowdb-tree/memtable.h"

#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <thread>
#include <vector>

using namespace crowdb::tree;

namespace
{
bool put_op(MemTable &mt, const std::string &k, uint64_t slot, const std::string &v)
{
    std::string cell = encode_cell(slot, OpKind::kPut, Slice(v));
    return mt.upsert(Slice(k), slot, Slice(cell));
}

// Helper: materialize a CellVersion into a full [header][value] std::string,
// matching the old MemTable::get(key, &cell) contract.
std::string materialize_cv(const CellVersion *cv)
{
    if (cv->cell.ownership() != buffer::mode::kExternal) {
        return std::string(reinterpret_cast<const char *>(cv->cell.data()), cv->cell.size());
    }
    size_t      vlen = cv->cell.size();
    std::string out(kCellHeaderSize + vlen, '\0');
    auto       *p = reinterpret_cast<uint8_t *>(out.data());
    for (int i = 0; i < 8; ++i) {
        p[i] = static_cast<uint8_t>((cv->slot >> (8 * i)) & 0xff);
    }
    p[8] = cv->flags;
    if (vlen > 0) {
        std::memcpy(p + kCellHeaderSize, cv->cell.data(), vlen);
    }
    return out;
}

bool get_cell(const MemTable &mt, Slice key, std::string *out_cell)
{
    const CellVersion *cv = mt.find(key);
    if (cv == nullptr) {
        return false;
    }
    *out_cell = materialize_cv(cv);
    return true;
}

bool get_cell_guarded(const MemTable &mt, EpochManager &epoch, Slice key, std::string *out_cell)
{
    EpochManager::Guard guard = epoch.enter();
    const CellVersion  *cv    = mt.find(key);
    if (cv == nullptr) {
        return false;
    }
    *out_cell = materialize_cv(cv);
    return true;
}

uint64_t slot_of(const std::string &cell)
{
    return CellView{Slice(cell)}.slot();
}

std::string val_of(const std::string &cell)
{
    return CellView{Slice(cell)}.value().to_string();
}
} // namespace

TEST(MemTable, HighestSlotWins)
{
    MemTable mt;
    EXPECT_TRUE(put_op(mt, "k", 5, "v5"));
    EXPECT_FALSE(put_op(mt, "k", 3, "v3"));
    EXPECT_TRUE(put_op(mt, "k", 8, "v8"));
    EXPECT_FALSE(put_op(mt, "k", 8, "v8b"));
    std::string cell;
    ASSERT_TRUE(get_cell(mt, "k", &cell));
    EXPECT_EQ(slot_of(cell), 8U);
    EXPECT_EQ(val_of(cell), "v8");
    EXPECT_EQ(mt.count(), 1U);
}

TEST(MemTable, DurableFloorDrops)
{
    MemTable mt;
    mt.set_durable_floor(10);
    EXPECT_FALSE(put_op(mt, "k", 5, "old"));
    EXPECT_FALSE(put_op(mt, "k", 10, "eq"));
    EXPECT_TRUE(put_op(mt, "k", 11, "new"));
    EXPECT_EQ(mt.count(), 1U);
}

TEST(MemTable, DrainUpToPrefix)
{
    MemTable mt;
    put_op(mt, "a", 1, "x");
    put_op(mt, "b", 5, "y");
    put_op(mt, "c", 3, "z");
    put_op(mt, "d", 9, "w");
    auto drained = mt.drain_up_to(5);
    ASSERT_EQ(drained.size(), 3U);
    EXPECT_EQ(drained[0].key, "a");
    EXPECT_EQ(drained[1].key, "b");
    EXPECT_EQ(drained[2].key, "c");
    EXPECT_EQ(mt.count(), 1U);
    std::string cell;
    EXPECT_FALSE(get_cell(mt, "a", &cell));
    EXPECT_TRUE(get_cell(mt, "d", &cell));
    EXPECT_EQ(slot_of(cell), 9U);
}

TEST(MemTable, SnapshotOrdered)
{
    MemTable mt;
    put_op(mt, "c", 1, "x");
    put_op(mt, "a", 2, "y");
    put_op(mt, "b", 3, "z");
    auto snap = mt.snapshot();
    ASSERT_EQ(snap.size(), 3U);
    EXPECT_EQ(snap[0].key, "a");
    EXPECT_EQ(snap[1].key, "b");
    EXPECT_EQ(snap[2].key, "c");
    EXPECT_EQ(snap[0].slot, 2U);
}

TEST(MemTable, BytesAccounting)
{
    MemTable mt;
    EXPECT_EQ(mt.approx_bytes(), 0U);
    put_op(mt, "key", 1, "value");
    size_t b1 = mt.approx_bytes();
    EXPECT_GT(b1, 0U);
    put_op(mt, "key", 2, "value-longer");
    EXPECT_NE(mt.approx_bytes(), 0U);
    (void)mt.drain_up_to(100);
    EXPECT_EQ(mt.approx_bytes(), 0U);
}

TEST(MemTable, HotKeyCollapse)
{
    MemTable mt;
    for (uint64_t s = 1; s <= 1000; ++s) {
        put_op(mt, "hot", s, "v" + std::to_string(s));
    }
    EXPECT_EQ(mt.count(), 1U);
    std::string cell;
    ASSERT_TRUE(get_cell(mt, "hot", &cell));
    EXPECT_EQ(slot_of(cell), 1000U);
}

TEST(MemTable, Tombstone)
{
    MemTable mt;
    put_op(mt, "k", 1, "v");
    std::string del = encode_cell(2, OpKind::kDelete);
    EXPECT_TRUE(mt.upsert(Slice("k"), 2, Slice(del)));
    std::string cell;
    ASSERT_TRUE(get_cell(mt, "k", &cell));
    EXPECT_TRUE(CellView{Slice(cell)}.is_tombstone());
}

TEST(MemTable, ConcurrentUpsertAndGet)
{
    EpochManager             epoch;
    MemTable                 mt(0, &epoch);
    std::atomic<bool>        stop{false};
    std::vector<std::thread> writers;
    writers.reserve(4);
    for (int w = 0; w < 4; ++w) {
        writers.emplace_back([&, w] {
            for (uint64_t s = 1; s <= 2000; ++s) {
                put_op(mt, "key" + std::to_string(w), s, "v");
            }
        });
    }
    std::thread reader([&] {
        std::string cell;
        while (!stop.load(std::memory_order_relaxed)) {
            (void)get_cell_guarded(mt, epoch, "key0", &cell);
        }
    });
    for (auto &t : writers) {
        t.join();
    }
    stop.store(true);
    reader.join();
    for (int w = 0; w < 4; ++w) {
        std::string cell;
        ASSERT_TRUE(get_cell_guarded(mt, epoch, "key" + std::to_string(w), &cell));
        EXPECT_EQ(slot_of(cell), 2000U);
    }
}
