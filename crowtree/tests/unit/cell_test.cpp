// CT2: slot-aware value cell encode/decode tests.
#include "crowtree/cell.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <string>

using namespace crowtree;

TEST(Cell, PutRoundTrip)
{
    std::string enc = encode_cell(42, OpKind::kPut, Slice("hello"));
    CellView    v{Slice(enc)};
    ASSERT_TRUE(v.valid());
    EXPECT_EQ(v.slot(), 42U);
    EXPECT_FALSE(v.is_tombstone());
    EXPECT_EQ(v.kind(), OpKind::kPut);
    EXPECT_EQ(v.value().to_string(), "hello");
}

TEST(Cell, Tombstone)
{
    std::string enc = encode_cell(7, OpKind::kDelete);
    CellView    v{Slice(enc)};
    EXPECT_EQ(v.slot(), 7U);
    EXPECT_TRUE(v.is_tombstone());
    EXPECT_EQ(v.kind(), OpKind::kDelete);
    EXPECT_TRUE(v.value().empty());
    EXPECT_EQ(enc.size(), kCellHeaderSize);
}

TEST(Cell, EmptyValuePut)
{
    std::string enc = encode_cell(1, OpKind::kPut, Slice());
    CellView    v{Slice(enc)};
    EXPECT_FALSE(v.is_tombstone());
    EXPECT_TRUE(v.value().empty());
}

TEST(Cell, SlotBoundaries)
{
    std::array<uint64_t, 7> slots = {0, 1, 255, 256, 65535, 1ULL << 32, ~0ULL};
    for (uint64_t s : slots) {
        std::string enc = encode_cell(s, OpKind::kPut, Slice("x"));
        CellView    v{Slice(enc)};
        EXPECT_EQ(v.slot(), s);
        EXPECT_EQ(v.value().to_string(), "x");
    }
}

TEST(Cell, BinaryValueWithNuls)
{
    const std::array<uint8_t, 4> raw = {0x00, 0xff, 0x00, 0x10};
    std::string   enc   = encode_cell(9, OpKind::kPut, Slice(raw.data(), raw.size()));
    CellView      v{Slice(enc)};
    EXPECT_EQ(v.value().size(), 4U);
    EXPECT_EQ(static_cast<uint8_t>(v.value().data()[1]), 0xff);
}

TEST(Cell, ReservedBitsIgnoredForTombstone)
{
    std::string enc = encode_cell(3, OpKind::kPut, Slice("v"));
    // Manually set a reserved bit (bit2); tombstone bit stays 0.
    enc[8] = static_cast<char>(enc[8] | 0x4);
    CellView v{Slice(enc)};
    EXPECT_FALSE(v.is_tombstone());
}

TEST(Cell, OverflowPointerRoundTrip)
{
    std::string enc = encode_overflow_cell(/*slot=*/77, /*head_page_id=*/1234, /*total_len=*/9000);
    CellView    v{Slice(enc)};
    EXPECT_EQ(enc.size(), kOverflowCellSize);
    EXPECT_EQ(v.slot(), 77U);
    EXPECT_TRUE(v.is_overflow());
    EXPECT_FALSE(v.is_tombstone());
    EXPECT_EQ(v.overflow_head(), 1234U);
    EXPECT_EQ(v.overflow_len(), 9000U);
    // value() must not return the pointer bytes as a value.
    EXPECT_TRUE(v.value().empty());
}

TEST(Cell, HighestSlotWins)
{
    std::string a = encode_cell(10, OpKind::kPut, Slice("new"));
    std::string b = encode_cell(5, OpKind::kPut, Slice("old"));
    EXPECT_TRUE(cell_wins(CellView{Slice(a)}, CellView{Slice(b)}));
    EXPECT_FALSE(cell_wins(CellView{Slice(b)}, CellView{Slice(a)}));
    // Equal slot: a wins (treated as same/idempotent write).
    std::string c = encode_cell(10, OpKind::kPut, Slice("dup"));
    EXPECT_TRUE(cell_wins(CellView{Slice(a)}, CellView{Slice(c)}));
}
