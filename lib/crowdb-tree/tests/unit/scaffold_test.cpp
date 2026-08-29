// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// CT1: scaffold smoke tests for Slice, Status, Options.
#include "crowdb-tree/options.h"
#include "crowdb-tree/slice.h"
#include "crowdb-tree/status.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <string>

using namespace crowdb::tree;

TEST(Slice, BasicAndCompare)
{
    Slice       a("apple");
    std::string apple = "apple"; // lvalue: Slice is a non-owning view
    Slice       b(apple);
    EXPECT_EQ(a, b);
    EXPECT_EQ(a.size(), 5U);
    EXPECT_EQ(a.to_string(), "apple");

    EXPECT_LT(Slice("apple"), Slice("banana"));
    EXPECT_GT(Slice("banana"), Slice("apple"));
    // Prefix is smaller than the longer string with same prefix.
    EXPECT_LT(Slice("app"), Slice("apple"));
    EXPECT_TRUE(Slice("apple").starts_with(Slice("app")));
    EXPECT_FALSE(Slice("apple").starts_with(Slice("banana")));
}

TEST(Slice, EmptyAndBinary)
{
    Slice e;
    EXPECT_TRUE(e.empty());
    const std::array<uint8_t, 4> raw = {0x00, 0x01, 0x00, 0xff};
    Slice                        s(raw.data(), raw.size());
    EXPECT_EQ(s.size(), 4U);
    EXPECT_EQ(s.to_string().size(), 4U);
    EXPECT_EQ(static_cast<uint8_t>(s.data()[3]), 0xff);
}

TEST(Status, Codes)
{
    EXPECT_TRUE(Status::Ok().ok());
    EXPECT_FALSE(Status::not_found("x").ok());
    EXPECT_EQ(Status::not_found().code(), Code::kNotFound);
    EXPECT_EQ(Status::invalid_argument("bad").message(), "bad");
    EXPECT_NE(Status::corruption().to_string().find("-3"), std::string::npos);
}

TEST(Options, Defaults)
{
    Options o;
    EXPECT_EQ(o.max_delta_len, 8U);
    EXPECT_EQ(o.max_delta_bytes, 256U * 1024U);
    EXPECT_GT(o.leaf_split_bytes, o.leaf_merge_bytes);
}
