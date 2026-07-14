// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// plan-tree #15: apply() rejects oversized keys with kInvalidArgument.
#include "crowtree/crowtree.h"

#include <gtest/gtest.h>

#include <string>

using namespace crowtree;

namespace
{

Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

} // namespace

TEST(OversizedKey, RejectedAtApplyWithDefaultLimit)
{
    Options opt;
    opt.frame_bytes = 4096; // default limit = frame_bytes / 2 = 2048
    Crowtree t(opt);

    const std::string big((opt.frame_bytes / 2) + 1, 'x');
    Status            s = t.apply(1, put_one(big, "v"));
    EXPECT_EQ(s.code(), Code::kInvalidArgument);

    // The rejected write left no state behind (all-or-nothing).
    EXPECT_EQ(t.memtable_count(), 0U);
    std::string v;
    uint64_t    slot;
    EXPECT_FALSE(t.get(Slice(big), &slot, &v));
}

TEST(OversizedKey, KeyAtLimitAccepted)
{
    Options opt;
    opt.frame_bytes = 4096; // limit = 2048
    Crowtree t(opt);

    const std::string ok_key(opt.frame_bytes / 2, 'y'); // exactly at the limit
    ASSERT_TRUE(t.apply(1, put_one(ok_key, "v")).ok());
    ASSERT_TRUE(t.flush().ok());
    std::string v;
    uint64_t    slot;
    EXPECT_TRUE(t.get(Slice(ok_key), &slot, &v));
    EXPECT_EQ(v, "v");
}

TEST(OversizedKey, ConfigurableLimit)
{
    Options opt;
    opt.max_key_size = 8; // explicit override
    Crowtree t(opt);

    EXPECT_EQ(t.apply(1, put_one("012345678", "v")).code(), Code::kInvalidArgument); // 9 > 8
    EXPECT_TRUE(t.apply(2, put_one("01234567", "v")).ok());                          // 8 == 8
}

TEST(OversizedKey, BatchRejectedAtomicallyIfAnyKeyTooLarge)
{
    Options opt;
    opt.max_key_size = 8;
    Crowtree t(opt);

    const std::string big(9, 'z');
    Batch             b{
                    {{.key = "small", .kind = OpKind::kPut, .value = "a"}, {.key = big, .kind = OpKind::kPut, .value = "b"}}
    };
    EXPECT_EQ(t.apply(1, b).code(), Code::kInvalidArgument);
    // No op from the rejected batch landed, not even the small one.
    EXPECT_EQ(t.memtable_count(), 0U);
}

TEST(OversizedKey, PutAndDelConvenienceRespectLimit)
{
    Options opt;
    opt.max_key_size = 4;
    Crowtree t(opt);

    EXPECT_EQ(t.put(Slice("abcde"), Slice("v")).code(), Code::kInvalidArgument);
    EXPECT_EQ(t.del(Slice("abcde")).code(), Code::kInvalidArgument);
    EXPECT_TRUE(t.put(Slice("abcd"), Slice("v")).ok());
}
