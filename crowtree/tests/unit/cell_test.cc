// CT2: slot-aware value cell encode/decode tests.
#include <gtest/gtest.h>

#include <string>

#include "crowtree/cell.h"

using namespace crowtree;

TEST(Cell, PutRoundTrip) {
  std::string enc = EncodeCell(42, OpKind::kPut, Slice("hello"));
  CellView v{Slice(enc)};
  ASSERT_TRUE(v.valid());
  EXPECT_EQ(v.slot(), 42u);
  EXPECT_FALSE(v.is_tombstone());
  EXPECT_EQ(v.kind(), OpKind::kPut);
  EXPECT_EQ(v.value().ToString(), "hello");
}

TEST(Cell, Tombstone) {
  std::string enc = EncodeCell(7, OpKind::kDelete);
  CellView v{Slice(enc)};
  EXPECT_EQ(v.slot(), 7u);
  EXPECT_TRUE(v.is_tombstone());
  EXPECT_EQ(v.kind(), OpKind::kDelete);
  EXPECT_TRUE(v.value().empty());
  EXPECT_EQ(enc.size(), kCellHeaderSize);
}

TEST(Cell, EmptyValuePut) {
  std::string enc = EncodeCell(1, OpKind::kPut, Slice());
  CellView v{Slice(enc)};
  EXPECT_FALSE(v.is_tombstone());
  EXPECT_TRUE(v.value().empty());
}

TEST(Cell, SlotBoundaries) {
  uint64_t slots[] = {0, 1, 255, 256, 65535, 1ull << 32, ~0ull};
  for (uint64_t s : slots) {
    std::string enc = EncodeCell(s, OpKind::kPut, Slice("x"));
    CellView v{Slice(enc)};
    EXPECT_EQ(v.slot(), s);
    EXPECT_EQ(v.value().ToString(), "x");
  }
}

TEST(Cell, BinaryValueWithNuls) {
  const uint8_t raw[] = {0x00, 0xff, 0x00, 0x10};
  std::string enc = EncodeCell(9, OpKind::kPut, Slice(raw, sizeof(raw)));
  CellView v{Slice(enc)};
  EXPECT_EQ(v.value().size(), 4u);
  EXPECT_EQ(static_cast<uint8_t>(v.value().data()[1]), 0xff);
}

TEST(Cell, ReservedBitsIgnoredForTombstone) {
  std::string enc = EncodeCell(3, OpKind::kPut, Slice("v"));
  // Manually set a reserved bit (bit2); tombstone bit stays 0.
  enc[8] = static_cast<char>(enc[8] | 0x4);
  CellView v{Slice(enc)};
  EXPECT_FALSE(v.is_tombstone());
}

TEST(Cell, HighestSlotWins) {
  std::string a = EncodeCell(10, OpKind::kPut, Slice("new"));
  std::string b = EncodeCell(5, OpKind::kPut, Slice("old"));
  EXPECT_TRUE(CellWins(CellView{Slice(a)}, CellView{Slice(b)}));
  EXPECT_FALSE(CellWins(CellView{Slice(b)}, CellView{Slice(a)}));
  // Equal slot: a wins (treated as same/idempotent write).
  std::string c = EncodeCell(10, OpKind::kPut, Slice("dup"));
  EXPECT_TRUE(CellWins(CellView{Slice(a)}, CellView{Slice(c)}));
}
