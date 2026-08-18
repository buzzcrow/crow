// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/buffer.h"

#include <gtest/gtest.h>

#include <cstring>

using crow::rpc::Buffer;
using crow::rpc::BufferType;
using crow::rpc::SystemBufferPool;

TEST(BufferTest, AllocReturnsValidBuffer)
{
    SystemBufferPool pool;
    Buffer          *buf = pool.alloc(1024);
    ASSERT_NE(buf, nullptr);
    EXPECT_GE(buf->capacity, 1024u);
    EXPECT_EQ(buf->len, 0u);
    EXPECT_EQ(buf->type, BufferType::System);
    // ref == 1 after alloc
    EXPECT_EQ(buf->ref->load(), 1);
    buf->release();
}

TEST(BufferTest, WriteSetsLenAndBytes)
{
    SystemBufferPool pool;
    Buffer          *buf = pool.alloc(1024);
    ASSERT_NE(buf, nullptr);

    const uint8_t data[] = {1, 2, 3, 4, 5};
    buf->write(data, 5);
    EXPECT_EQ(buf->len, 5u);
    EXPECT_EQ(std::memcmp(buf->data, data, 5), 0);
    buf->release();
}

TEST(BufferTest, RefCloneAndReleaseRecycles)
{
    SystemBufferPool pool;
    Buffer          *buf = pool.alloc(512);
    ASSERT_NE(buf, nullptr);
    uint8_t *original_ptr = buf->data;

    Buffer *ref1 = buf->ref_clone();
    EXPECT_EQ(buf->ref->load(), 2);
    EXPECT_EQ(ref1->data, original_ptr);

    // Release one — ref == 1, buffer not recycled yet
    ref1->release();
    EXPECT_EQ(buf->ref->load(), 1);

    // Release the other — ref == 0, buffer recycled to pool
    buf->release();

    // Next alloc should reuse the recycled buffer (same data pointer)
    Buffer *reused = pool.alloc(512);
    ASSERT_NE(reused, nullptr);
    EXPECT_EQ(reused->data, original_ptr);
    reused->release();
}

TEST(BufferTest, PoolExhaustedReturnsNull)
{
    SystemBufferPool pool(2); // max 2 buffers
    Buffer          *a = pool.alloc(64);
    Buffer          *b = pool.alloc(64);
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(pool.alloc(64), nullptr); // exhausted
    a->release();
    b->release();
}

TEST(BufferTest, DifferentCapacitiesReuseSameBucket)
{
    SystemBufferPool pool;
    Buffer          *buf = pool.alloc(200); // bucketed to 256
    ASSERT_NE(buf, nullptr);
    uint8_t *ptr256 = buf->data;
    EXPECT_GE(buf->capacity, 256u);
    buf->release();

    // A 130-byte request also buckets to 256 → should reuse
    Buffer *reused = pool.alloc(130);
    ASSERT_NE(reused, nullptr);
    EXPECT_EQ(reused->data, ptr256);
    reused->release();
}
