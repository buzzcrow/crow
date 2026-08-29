// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// #5 B2a: buffer-based cell encoders are byte-identical to the std::string ones,
// and decode correctly through CellView.
#include "crowdb-tree/cell.h"

#include <gtest/gtest.h>

#include <string>

using namespace crowdb::tree;

namespace
{
std::string as_str(const buffer &b)
{
    return {reinterpret_cast<const char *>(b.data()), b.size()};
}
} // namespace

TEST(CellBuffer, PutMatchesStringEncoder)
{
    buffer b = encode_cell_buf(42, OpKind::kPut, Slice("hello world"));
    EXPECT_EQ(as_str(b), encode_cell(42, OpKind::kPut, Slice("hello world")));
    EXPECT_EQ(b.header_reserve(), kCellHeaderSize);

    CellView v{b.slice()};
    ASSERT_TRUE(v.valid());
    EXPECT_EQ(v.slot(), 42U);
    EXPECT_FALSE(v.is_tombstone());
    EXPECT_FALSE(v.is_overflow());
    EXPECT_EQ(v.value().to_string(), "hello world");
}

TEST(CellBuffer, EmptyValuePut)
{
    buffer b = encode_cell_buf(7, OpKind::kPut, Slice());
    EXPECT_EQ(as_str(b), encode_cell(7, OpKind::kPut, Slice()));
    EXPECT_EQ(b.size(), kCellHeaderSize);
    CellView v{b.slice()};
    EXPECT_EQ(v.slot(), 7U);
    EXPECT_EQ(v.value().size(), 0U);
}

TEST(CellBuffer, TombstoneCarriesNoValue)
{
    // A delete cell drops the value bytes even if one is passed.
    buffer b = encode_cell_buf(9, OpKind::kDelete, Slice("ignored"));
    EXPECT_EQ(as_str(b), encode_cell(9, OpKind::kDelete, Slice("ignored")));
    EXPECT_EQ(b.size(), kCellHeaderSize);
    CellView v{b.slice()};
    EXPECT_TRUE(v.is_tombstone());
    EXPECT_EQ(v.kind(), OpKind::kDelete);
}

TEST(CellBuffer, OverflowMatchesStringEncoder)
{
    buffer b = encode_overflow_cell_buf(100, /*head_page_id=*/2222, /*total_len=*/999999);
    EXPECT_EQ(as_str(b), encode_overflow_cell(100, 2222, 999999));
    EXPECT_EQ(b.size(), kOverflowCellSize);
    CellView v{b.slice()};
    EXPECT_TRUE(v.is_overflow());
    EXPECT_EQ(v.slot(), 100U);
    EXPECT_EQ(v.overflow_head(), 2222U);
    EXPECT_EQ(v.overflow_len(), 999999U);
}

TEST(CellBuffer, LargeValueRoundTrip)
{
    std::string big(4096, 'x');
    buffer      b = encode_cell_buf(123456789, OpKind::kPut, Slice(big));
    EXPECT_EQ(as_str(b), encode_cell(123456789, OpKind::kPut, Slice(big)));
    CellView v{b.slice()};
    EXPECT_EQ(v.slot(), 123456789U);
    EXPECT_EQ(v.value().to_string(), big);
}
