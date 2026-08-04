// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// #5 B1: buffer abstraction (owned/borrowed, move-only, header reserve, clone).
#include "crow-tree/buffer.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <string>
#include <utility>

using namespace crow::tree;

namespace
{
std::string str(const buffer &b)
{
    return {reinterpret_cast<const char *>(b.data()), b.size()};
}
} // namespace

TEST(Buffer, EmptyDefault)
{
    buffer b;
    EXPECT_TRUE(b.empty());
    EXPECT_EQ(b.size(), 0U);
    EXPECT_TRUE(b.owned());
    EXPECT_EQ(b.data(), nullptr);
}

TEST(Buffer, AllocWriteRead)
{
    buffer b = buffer::alloc(5);
    ASSERT_EQ(b.size(), 5U);
    ASSERT_NE(b.data(), nullptr);
    EXPECT_TRUE(b.owned());
    std::memcpy(b.data(), "hello", 5); // NOLINT(bugprone-not-null-terminated-result)
    EXPECT_GE(b.capacity(), 5U);
}

TEST(Buffer, HeaderReserveLayout)
{
    // alloc(value=4, header=9): total 13, value region starts at data()+9.
    buffer b = buffer::alloc(/*capacity=*/4, /*header_reserve=*/9);
    ASSERT_EQ(b.size(), 13U);
    EXPECT_EQ(b.header_reserve(), 9U);
    // Write header prefix + value after it — one contiguous block.
    std::memset(b.header(0), 0xAB, 9);
    std::memcpy(b.data() + b.header_reserve(), "vals", 4);
    EXPECT_EQ(b.header(0), b.data());
    EXPECT_EQ(static_cast<uint8_t>(b.data()[0]), 0xABU);
    EXPECT_EQ(std::string(reinterpret_cast<const char *>(b.data() + 9), 4), "vals");
}

TEST(Buffer, SetSizeShrinks)
{
    buffer b = buffer::alloc(10);
    std::memcpy(b.data(), "abcdefghij", 10);
    b.set_size(3);
    EXPECT_EQ(b.size(), 3U);
    EXPECT_EQ(str(b), "abc");
    EXPECT_GE(b.capacity(), 10U);
}

TEST(Buffer, MoveTransfersOwnership)
{
    // Use a heap-sized (> kInlineCap) buffer so the moved heap pointer is stable
    // (inline buffers relocate their bytes — covered by MoveOfInlineRelocatesBytes).
    const std::string payload(buffer::kInlineCap + 8, 'q');
    buffer            a = buffer::alloc(payload.size());
    std::memcpy(a.data(), payload.data(), payload.size());
    ASSERT_FALSE(a.inlined());
    const uint8_t *p = a.data();

    buffer b = std::move(a);
    EXPECT_EQ(b.data(), p); // heap pointer moved, no copy
    EXPECT_EQ(b.size(), payload.size());
    EXPECT_EQ(a.data(), nullptr); // NOLINT(bugprone-use-after-move,clang-analyzer-cplusplus.Move) — asserting reset
    EXPECT_TRUE(a.empty());

    buffer c;
    c = std::move(b);
    EXPECT_EQ(c.data(), p);
    EXPECT_EQ(str(c), payload);
    EXPECT_EQ(b.data(), nullptr); // NOLINT(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
}

TEST(Buffer, CloneIsIndependentDeepCopy)
{
    buffer a = buffer::alloc(/*capacity=*/4, /*header_reserve=*/2); // total size 6
    ASSERT_EQ(a.size(), 6U);
    std::memcpy(a.data(), "HHVVVV", 6); // 2 header bytes + 4 value bytes
    buffer c = a.clone();
    ASSERT_NE(c.data(), a.data());
    EXPECT_EQ(c.size(), a.size());
    EXPECT_EQ(c.header_reserve(), a.header_reserve());
    EXPECT_EQ(str(c), "HHVVVV");
    // Mutating the clone does not touch the original.
    c.data()[0] = 'Z';
    EXPECT_EQ(a.data()[0], 'H');
}

TEST(Buffer, WrapIsBorrowedAndNeverFrees)
{
    // A borrowed buffer must not free external memory. If it did, freeing the same
    // pointer below would be a double-free that ASan traps.
    auto *ext = static_cast<uint8_t *>(std::malloc(4));
    std::memcpy(ext, "data", 4); // NOLINT(bugprone-not-null-terminated-result)
    {
        buffer b = buffer::wrap(ext, 4);
        EXPECT_FALSE(b.owned());
        EXPECT_EQ(b.ownership(), buffer::mode::kBorrowed);
        EXPECT_EQ(str(b), "data");
    } // b destroyed: must NOT free ext
    std::free(ext); // the sole free
}

TEST(Buffer, MoveFromTakesOwnership)
{
    auto *p = static_cast<uint8_t *>(std::malloc(3));
    std::memcpy(p, "abc", 3); // NOLINT(bugprone-not-null-terminated-result)
    {
        buffer b = buffer::move_from(p, 3, 3);
        EXPECT_TRUE(b.owned());
        EXPECT_EQ(str(b), "abc");
    } // b frees p (ASan verifies exactly one free)
}

TEST(Buffer, OrderingAndEquality)
{
    buffer a = buffer::alloc(3);
    std::memcpy(a.data(), "abc", 3);
    buffer b = buffer::alloc(3);
    std::memcpy(b.data(), "abd", 3);
    buffer c = buffer::alloc(2);
    std::memcpy(c.data(), "ab", 2);

    EXPECT_TRUE(a < b); // 'c' < 'd'
    EXPECT_FALSE(b < a);
    EXPECT_TRUE(c < a); // shorter prefix orders first
    EXPECT_FALSE(a == b);
    buffer a2 = a.clone();
    EXPECT_TRUE(a == a2);
}

TEST(Buffer, SliceView)
{
    buffer b = buffer::alloc(3);
    std::memcpy(b.data(), "abc", 3);
    Slice s = b.slice();
    EXPECT_EQ(s.size(), 3U);
    EXPECT_EQ(s.to_string(), "abc");
}

// ── SBO (small-buffer optimization) ───────────────────────────────

TEST(Buffer, SmallIsInlineNoHeap)
{
    buffer b = buffer::alloc(buffer::kInlineCap); // exactly fits inline
    EXPECT_TRUE(b.inlined());
    EXPECT_TRUE(b.owned());
    std::memset(b.data(), 'a', b.size());
    EXPECT_EQ(str(b), std::string(buffer::kInlineCap, 'a'));
}

TEST(Buffer, LargeGoesToHeap)
{
    buffer b = buffer::alloc(buffer::kInlineCap + 1); // one past inline
    EXPECT_FALSE(b.inlined());
    EXPECT_TRUE(b.owned());
    std::memset(b.data(), 'z', b.size());
    EXPECT_EQ(b.size(), buffer::kInlineCap + 1);
}

TEST(Buffer, InlineWithHeaderReserveBoundary)
{
    // 9-byte header + 15-byte value = 24 = kInlineCap -> still inline.
    buffer b = buffer::alloc(/*capacity=*/15, /*header_reserve=*/9);
    EXPECT_EQ(b.size(), 24U);
    EXPECT_TRUE(b.inlined());
    // one more byte tips it to heap.
    buffer h = buffer::alloc(/*capacity=*/16, /*header_reserve=*/9);
    EXPECT_EQ(h.size(), 25U);
    EXPECT_FALSE(h.inlined());
}

TEST(Buffer, MoveOfInlineRelocatesBytes)
{
    buffer a = buffer::alloc(5);
    std::memcpy(a.data(), "hello", 5); // NOLINT(bugprone-not-null-terminated-result)

    buffer b = std::move(a);
    EXPECT_TRUE(b.inlined());
    EXPECT_EQ(str(b), "hello"); // content relocated into b's own inline storage
    EXPECT_TRUE(a.empty());     // NOLINT(bugprone-use-after-move)

    // Move-assign path too.
    buffer c;
    c = std::move(b);
    EXPECT_TRUE(c.inlined());
    EXPECT_EQ(str(c), "hello");
    EXPECT_TRUE(b.empty()); // NOLINT(bugprone-use-after-move)
}

TEST(Buffer, CloneOfInlineIsInlineAndIndependent)
{
    buffer a = buffer::alloc(4, /*header_reserve=*/2); // size 6, inline
    std::memcpy(a.data(), "HHVVVV", 6);
    buffer c = a.clone();
    EXPECT_TRUE(c.inlined());
    EXPECT_EQ(c.header_reserve(), 2U);
    EXPECT_EQ(str(c), "HHVVVV");
    c.data()[0] = 'Z';
    EXPECT_EQ(a.data()[0], 'H'); // independent storage
}
