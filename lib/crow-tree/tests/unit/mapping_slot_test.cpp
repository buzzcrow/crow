// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// #14a: packed 64-bit mapping slot-word encode/decode helpers.
#include "crow-tree/mapping_slot.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>

using namespace crow::tree;
using namespace crow::tree::slot_word;

TEST(MappingSlot, EmptyClassification)
{
    EXPECT_TRUE(is_empty(kEmpty));
    EXPECT_FALSE(is_unloaded(kEmpty));
    EXPECT_FALSE(is_resident(kEmpty));
}

TEST(MappingSlot, UnloadedRoundTrip)
{
    struct Case
    {
        uint64_t iu_index;
        uint32_t iu_count;
    };

    const std::array<Case, 8> cases = {
        {
         {.iu_index = 0, .iu_count = 0},
         {.iu_index = 0, .iu_count = 1},
         {.iu_index = 1, .iu_count = 0},
         {.iu_index = 1, .iu_count = 1},
         {.iu_index = 12345, .iu_count = 7},
         {.iu_index = kMaxIuIndex, .iu_count = 1},
         {.iu_index = 5, .iu_count = kMaxIuCount},
         {.iu_index = kMaxIuIndex, .iu_count = kMaxIuCount},
         }
    };
    for (const auto &c : cases) {
        ASSERT_TRUE(fits_unloaded(c.iu_index, c.iu_count));
        uint64_t w = pack_unloaded(c.iu_index, c.iu_count);
        EXPECT_TRUE(is_unloaded(w)) << c.iu_index << "/" << c.iu_count;
        EXPECT_FALSE(is_empty(w));
        EXPECT_FALSE(is_resident(w));
        EXPECT_EQ(unloaded_iu_index(w), c.iu_index);
        EXPECT_EQ(unloaded_iu_count(w), c.iu_count);
    }
}

TEST(MappingSlot, UnloadedFieldsDoNotOverlap)
{
    // Max in one field must not disturb the other.
    uint64_t a = pack_unloaded(kMaxIuIndex, 0);
    EXPECT_EQ(unloaded_iu_index(a), kMaxIuIndex);
    EXPECT_EQ(unloaded_iu_count(a), 0U);

    uint64_t b = pack_unloaded(0, kMaxIuCount);
    EXPECT_EQ(unloaded_iu_index(b), 0U);
    EXPECT_EQ(unloaded_iu_count(b), kMaxIuCount);

    // Tag bit is always set for an unloaded word.
    EXPECT_EQ(a & kUnloadedTag, kUnloadedTag);
    EXPECT_EQ(b & kUnloadedTag, kUnloadedTag);
}

TEST(MappingSlot, FitsBoundaries)
{
    EXPECT_TRUE(fits_unloaded(kMaxIuIndex, kMaxIuCount));
    EXPECT_FALSE(fits_unloaded(kMaxIuIndex + 1, 0));
    EXPECT_FALSE(fits_unloaded(0, kMaxIuCount + 1));
}

TEST(MappingSlot, ResidentPointerRoundTrip)
{
    struct alignas(8) Dummy
    {
        uint64_t x;
    } d{0};

    auto    *p = reinterpret_cast<PageBase *>(&d); // aligned; never dereferenced
    uint64_t w = pack_resident(p);
    EXPECT_TRUE(is_resident(w));
    EXPECT_FALSE(is_empty(w));
    EXPECT_FALSE(is_unloaded(w)); // 8-byte aligned => low bit clear
    EXPECT_EQ(resident_ptr(w), p);
}
