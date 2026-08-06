// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// R30: zero-copy apply path tests — kExternal buffer mode, split-cell
// MemTable, and Crowtree::apply_external round-trip + flush + read.
#include "crow-tree/buffer.h"
#include "crow-tree/cell.h"
#include "crow-tree/crow-tree.h"
#include "crow-tree/memtable.h"
#include "crow-tree/page_store.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

using namespace crow::tree;

namespace
{
void count_drop(void *ctx)
{
    static_cast<std::atomic<int> *>(ctx)->fetch_add(1, std::memory_order_relaxed);
}

std::unique_ptr<Crowtree> open_tree(MemPageStore &store)
{
    Options opt;
    opt.page_store = &store;
    std::unique_ptr<Crowtree> t;
    EXPECT_TRUE(Crowtree::open(opt, &t).ok());
    return t;
}

// R50: materialize a CellVersion into a full [header][value] std::string,
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
} // namespace

// ── kExternal buffer mode ──────────────────────────────────────────

TEST(ExternalBuffer, ConstructAndDropCallsDropFn)
{
    std::atomic<int>     drops{0};
    std::vector<uint8_t> data(64, 0xAB);
    {
        buffer b = buffer::wrap_external(data.data(), data.size(), &drops, count_drop);
        EXPECT_EQ(b.size(), 64u);
        EXPECT_EQ(b.ownership(), buffer::mode::kExternal);
        EXPECT_EQ(b.data(), data.data());
        EXPECT_EQ(drops.load(), 0);
    }
    EXPECT_EQ(drops.load(), 1);
}

TEST(ExternalBuffer, MoveTransfersOwnershipNoDrop)
{
    std::atomic<int>     drops{0};
    std::vector<uint8_t> data(32, 0xCD);
    {
        buffer b1 = buffer::wrap_external(data.data(), data.size(), &drops, count_drop);
        buffer b2 = std::move(b1); // move: no drop_fn call
        EXPECT_EQ(drops.load(), 0);
        EXPECT_EQ(b2.size(), 32u);
        EXPECT_EQ(b2.ownership(), buffer::mode::kExternal);
    }
    EXPECT_EQ(drops.load(), 1); // b2 destroyed -> drop_fn once
}

TEST(ExternalBuffer, CloneDeepCopiesIntoOwned)
{
    std::atomic<int>     drops{0};
    std::vector<uint8_t> data(128, 0x42);
    buffer               b = buffer::wrap_external(data.data(), data.size(), &drops, count_drop);
    buffer               c = b.clone();
    EXPECT_EQ(c.ownership(), buffer::mode::kOwned);
    EXPECT_EQ(c.size(), 128u);
    EXPECT_EQ(std::memcmp(c.data(), data.data(), 128), 0);
    EXPECT_EQ(drops.load(), 0); // original still alive
    // Destroy both: clone frees its owned copy; original calls drop_fn.
}

TEST(ExternalBuffer, SliceAndCompareWorkOnBorrowedBytes)
{
    std::atomic<int>     drops{0};
    std::vector<uint8_t> data(16, 0x77);
    buffer               b = buffer::wrap_external(data.data(), data.size(), &drops, count_drop);
    EXPECT_EQ(b.slice().size(), 16u);
    EXPECT_EQ(std::memcmp(b.slice().data(), data.data(), 16), 0);
}

// ── Split-cell MemTable ────────────────────────────────────────────

TEST(MemTableExternal, SplitPutGetRoundTrip)
{
    std::atomic<int>     drops{0};
    std::vector<uint8_t> val(256, 0x7E);
    MemTable             mt;
    buffer               vbuf = buffer::wrap_external(val.data(), val.size(), &drops, count_drop);
    EXPECT_TRUE(mt.upsert_external("k", 5, 0, std::move(vbuf)));
    std::string cell;
    EXPECT_TRUE(get_cell(mt, "k", &cell));
    EXPECT_EQ(cell.size(), 9u + 256u); // [9-byte header][256-byte value]
    CellView cv{Slice(cell)};
    EXPECT_EQ(cv.slot(), 5u);
    EXPECT_FALSE(cv.is_tombstone());
    EXPECT_EQ(cv.value().size(), 256u);
    EXPECT_EQ(std::memcmp(cv.value().data(), val.data(), 256), 0);
    EXPECT_EQ(drops.load(), 0); // value still borrowed in the memtable
}

TEST(MemTableExternal, SplitPutDestroyReleasesRef)
{
    std::atomic<int>     drops{0};
    std::vector<uint8_t> val(256, 0x7E);
    {
        MemTable mt;
        buffer   vbuf = buffer::wrap_external(val.data(), val.size(), &drops, count_drop);
        mt.upsert_external("k", 5, 0, std::move(vbuf));
        EXPECT_EQ(drops.load(), 0);
    }
    EXPECT_EQ(drops.load(), 1); // memtable destroyed -> split entry freed
}

TEST(MemTableExternal, SplitHighestSlotWins)
{
    std::atomic<int>     drops1{0};
    std::atomic<int>     drops2{0};
    std::vector<uint8_t> v1(64, 0xA1);
    std::vector<uint8_t> v2(64, 0xA2);
    MemTable             mt;
    mt.upsert_external("k", 3, 0, buffer::wrap_external(v1.data(), v1.size(), &drops1, count_drop));
    EXPECT_EQ(drops1.load(), 0);
    // Lower slot rejected; incoming buffer freed immediately.
    EXPECT_FALSE(mt.upsert_external("k", 2, 0, buffer::wrap_external(v2.data(), v2.size(), &drops2, count_drop)));
    EXPECT_EQ(drops2.load(), 1);
    EXPECT_EQ(drops1.load(), 0);
    // Higher slot wins; old entry freed.
    EXPECT_TRUE(mt.upsert_external("k", 7, 0, buffer::wrap_external(v2.data(), v2.size(), &drops2, count_drop)));
    EXPECT_EQ(drops1.load(), 1);
    std::string cell;
    EXPECT_TRUE(get_cell(mt, "k", &cell));
    CellView cv{Slice(cell)};
    EXPECT_EQ(cv.slot(), 7u);
    EXPECT_EQ(static_cast<uint8_t>(cv.value().data()[0]), 0xA2u);
}

TEST(MemTableExternal, SplitDrainMaterializesContiguous)
{
    std::atomic<int>     drops{0};
    std::vector<uint8_t> val(512, 0x5A);
    MemTable             mt;
    mt.upsert_external("k", 10, 0, buffer::wrap_external(val.data(), val.size(), &drops, count_drop));
    auto drained = mt.drain_up_to(10);
    ASSERT_EQ(drained.size(), 1u);
    EXPECT_EQ(drained[0].key, "k");
    EXPECT_EQ(drained[0].slot, 10u);
    CellView cv{Slice(drained[0].cell.data(), drained[0].cell.size())};
    EXPECT_EQ(cv.slot(), 10u);
    EXPECT_EQ(cv.value().size(), 512u);
    EXPECT_EQ(std::memcmp(cv.value().data(), val.data(), 512), 0);
    EXPECT_EQ(drops.load(), 1); // drained -> external buffer freed
}

TEST(MemTableExternal, SplitDeleteRoundTrip)
{
    MemTable mt;
    EXPECT_TRUE(mt.upsert_external("k", 3, kFlagTombstone, buffer::alloc(0)));
    std::string cell;
    EXPECT_TRUE(get_cell(mt, "k", &cell));
    CellView cv{Slice(cell)};
    EXPECT_EQ(cv.slot(), 3u);
    EXPECT_TRUE(cv.is_tombstone());
    EXPECT_EQ(cv.value().size(), 0u);
}

TEST(MemTableExternal, SplitAndContiguousCoexist)
{
    std::atomic<int>     drops{0};
    std::vector<uint8_t> ext_val(128, 0x33);
    MemTable             mt;
    mt.upsert_external("ext", 1, 0, buffer::wrap_external(ext_val.data(), ext_val.size(), &drops, count_drop));
    mt.upsert("con", 1, encode_cell_buf(1, OpKind::kPut, Slice("cv", 2)));
    std::string ext_cell;
    std::string con_cell;
    EXPECT_TRUE(get_cell(mt, "ext", &ext_cell));
    EXPECT_TRUE(get_cell(mt, "con", &con_cell));
    CellView ev{Slice(ext_cell)};
    CellView cv{Slice(con_cell)};
    EXPECT_EQ(ev.slot(), 1u);
    EXPECT_EQ(cv.slot(), 1u);
    EXPECT_EQ(ev.value().size(), 128u);
    EXPECT_EQ(cv.value().size(), 2u);
}

// ── Crowtree::apply_external end-to-end ────────────────────────────

TEST(ApplyExternal, RoundTripReadBeforeFlush)
{
    std::atomic<int>     drops{0};
    std::vector<uint8_t> big(4096, 0xF0);
    MemPageStore         store(1);
    auto                 t = open_tree(store);
    ASSERT_NE(t, nullptr);
    std::vector<Crowtree::external_op> ops;
    ops.push_back({"k1", 0, buffer::wrap_external(big.data(), big.size(), &drops, count_drop)});
    EXPECT_TRUE(t->apply_external(1, std::move(ops)).ok());
    uint64_t    slot;
    std::string value;
    EXPECT_TRUE(t->get("k1", &slot, &value));
    EXPECT_EQ(slot, 1u);
    EXPECT_EQ(value.size(), 4096u);
    EXPECT_EQ(std::memcmp(value.data(), big.data(), 4096), 0);
    EXPECT_EQ(drops.load(), 0); // still in memtable
}

TEST(ApplyExternal, RoundTripReadAfterFlush)
{
    std::atomic<int>     drops{0};
    std::vector<uint8_t> big(8192, 0xE1);
    MemPageStore         store(1);
    auto                 t = open_tree(store);
    ASSERT_NE(t, nullptr);
    std::vector<Crowtree::external_op> ops;
    ops.push_back({"k2", 0, buffer::wrap_external(big.data(), big.size(), &drops, count_drop)});
    EXPECT_TRUE(t->apply_external(1, std::move(ops)).ok());
    EXPECT_TRUE(t->flush().ok()); // drain -> materialize -> drop_fn fires
    EXPECT_EQ(drops.load(), 1);
    uint64_t    slot;
    std::string value;
    EXPECT_TRUE(t->get("k2", &slot, &value));
    EXPECT_EQ(slot, 1u);
    EXPECT_EQ(value.size(), 8192u);
    EXPECT_EQ(std::memcmp(value.data(), big.data(), 8192), 0);
}

TEST(ApplyExternal, MultiKeyBatchAtomicity)
{
    std::atomic<int>     drops{0};
    std::vector<uint8_t> v1(256, 0x01);
    std::vector<uint8_t> v2(256, 0x02);
    std::vector<uint8_t> v3(256, 0x03);
    MemPageStore         store(1);
    auto                 t = open_tree(store);
    ASSERT_NE(t, nullptr);
    std::vector<Crowtree::external_op> ops;
    ops.push_back({"a", 0, buffer::wrap_external(v1.data(), v1.size(), &drops, count_drop)});
    ops.push_back({"b", 0, buffer::wrap_external(v2.data(), v2.size(), &drops, count_drop)});
    ops.push_back({"c", 0, buffer::wrap_external(v3.data(), v3.size(), &drops, count_drop)});
    EXPECT_TRUE(t->apply_external(1, std::move(ops)).ok());
    for (const auto &k : {"a", "b", "c"}) {
        uint64_t    slot;
        std::string value;
        EXPECT_TRUE(t->get(k, &slot, &value));
        EXPECT_EQ(slot, 1u);
        EXPECT_EQ(value.size(), 256u);
    }
    EXPECT_EQ(drops.load(), 0); // all still in memtable
}

TEST(ApplyExternal, IntraBatchLastKeyWins)
{
    std::atomic<int>     drops{0};
    std::vector<uint8_t> v1(64, 0xAA);
    std::vector<uint8_t> v2(64, 0xBB);
    MemPageStore         store(1);
    auto                 t = open_tree(store);
    ASSERT_NE(t, nullptr);
    std::vector<Crowtree::external_op> ops;
    ops.push_back({"k", 0, buffer::wrap_external(v1.data(), v1.size(), &drops, count_drop)});
    ops.push_back({"k", 0, buffer::wrap_external(v2.data(), v2.size(), &drops, count_drop)});
    EXPECT_TRUE(t->apply_external(1, std::move(ops)).ok());
    EXPECT_EQ(drops.load(), 1); // v1 freed (last-key-wins), v2 retained
    uint64_t    slot;
    std::string value;
    EXPECT_TRUE(t->get("k", &slot, &value));
    EXPECT_EQ(static_cast<uint8_t>(value.data()[0]), 0xBBu);
}

TEST(ApplyExternal, DeleteViaExternalOp)
{
    MemPageStore store(1);
    auto         t = open_tree(store);
    ASSERT_NE(t, nullptr);
    // First put a value via the legacy (contiguous) path, then delete via
    // external. Verifies the two paths interoperate correctly.
    std::vector<Crowtree::encoded_op> put_ops;
    put_ops.push_back({"k", encode_cell_buf(1, OpKind::kPut, Slice("v1", 2))});
    EXPECT_TRUE(t->apply_encoded(1, std::move(put_ops)).ok());
    std::vector<Crowtree::external_op> del_ops;
    del_ops.push_back({"k", kFlagTombstone, buffer::alloc(0)});
    EXPECT_TRUE(t->apply_external(2, std::move(del_ops)).ok());
    uint64_t    slot;
    std::string value;
    EXPECT_FALSE(t->get("k", &slot, &value)); // tombstone -> not found
}
