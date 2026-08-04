// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// PT10: page compression (LZ4 default, identity fallback).
#include "crow-tree/cell.h"
#include "crow-tree/compressor.h"
#include "crow-tree/frame_page.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

using namespace crow::tree;

namespace
{
constexpr uint32_t kPb = 4096;

std::string key(int i)
{
    std::array<char, 16> b{};
    snprintf(b.data(), b.size(), "k%05d", i);
    return b.data();
}

// A highly-compressible leaf frame (repetitive values).
std::vector<uint8_t> compressible_frame()
{
    std::vector<uint8_t> f(kPb);
    LeafFrameBuilder     b(f.data(), kPb);
    for (int i = 0; i < 40; ++i) {
        std::string cell = encode_cell(i, OpKind::kPut, Slice(std::string(20, 'a')));
        b.try_append_sorted(Slice(key(i)), Slice(cell));
    }
    b.finish(1, kInvalidPageId);
    return f;
}
} // namespace

TEST(PageCompression, RoundTripPreservesFrame)
{
    auto                 frame = compressible_frame();
    std::vector<uint8_t> blob;
    encode_durable_page(frame.data(), kPb, compress_algo::kLz4, &blob);

    std::vector<uint8_t> back(kPb);
    ASSERT_TRUE(decode_durable_page(blob.data(), blob.size(), back.data(), kPb).ok());
    EXPECT_EQ(frame, back);
    ASSERT_TRUE(frame_validate(back.data(), kPb));
}

TEST(PageCompression, CompressibleShrinksWhenLz4Available)
{
    auto                 frame = compressible_frame();
    std::vector<uint8_t> blob;
    encode_durable_page(frame.data(), kPb, compress_algo::kLz4, &blob);
    if (lz4_available()) {
        EXPECT_LT(blob.size(), static_cast<size_t>(kPb)); // actually compressed
    }
    else {
        EXPECT_GE(blob.size(), static_cast<size_t>(kPb)); // stored raw
    }
}

TEST(PageCompression, IncompressibleStoredRaw)
{
    std::vector<uint8_t> frame(kPb);
    std::mt19937         rng(1234);
    for (auto &b : frame) {
        b = static_cast<uint8_t>(rng());
    }
    std::vector<uint8_t> blob;
    encode_durable_page(frame.data(), kPb, compress_algo::kLz4, &blob);
    // Random data does not shrink -> stored raw (algo byte 0 = kNone).
    EXPECT_EQ(blob[0], static_cast<uint8_t>(compress_algo::kNone));
    std::vector<uint8_t> back(kPb);
    ASSERT_TRUE(decode_durable_page(blob.data(), blob.size(), back.data(), kPb).ok());
    EXPECT_EQ(frame, back);
}

TEST(PageCompression, NonePreferStoresRaw)
{
    auto                 frame = compressible_frame();
    std::vector<uint8_t> blob;
    encode_durable_page(frame.data(), kPb, compress_algo::kNone, &blob);
    EXPECT_EQ(blob[0], static_cast<uint8_t>(compress_algo::kNone));
    std::vector<uint8_t> back(kPb);
    ASSERT_TRUE(decode_durable_page(blob.data(), blob.size(), back.data(), kPb).ok());
    EXPECT_EQ(frame, back);
}

TEST(PageCompression, CrcTamperRejected)
{
    auto                 frame = compressible_frame();
    std::vector<uint8_t> blob;
    encode_durable_page(frame.data(), kPb, compress_algo::kLz4, &blob);
    blob[kDurableBlobHeader + 1] ^= 0xff; // flip a stored byte
    std::vector<uint8_t> back(kPb);
    EXPECT_EQ(decode_durable_page(blob.data(), blob.size(), back.data(), kPb).code(), Code::kCorruption);
}

TEST(PageCompression, ShortBlobRejected)
{
    std::array<uint8_t, 4> b = {0};
    std::vector<uint8_t>   back(kPb);
    EXPECT_EQ(decode_durable_page(b.data(), b.size(), back.data(), kPb).code(), Code::kInvalidArgument);
}
