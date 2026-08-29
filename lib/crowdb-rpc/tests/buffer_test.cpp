// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/buffer.h"

#include <gtest/gtest.h>

#include <cstring>

using crowdb::rpc::Buffer;
using crowdb::rpc::BufferType;
using crowdb::rpc::SystemBufferPool;

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

TEST(BufferTest, RefCloneAndRelease)
{
    SystemBufferPool pool;
    Buffer          *buf = pool.alloc(512);
    ASSERT_NE(buf, nullptr);
    uint8_t *original_ptr = buf->data;

    Buffer *ref1 = buf->ref_clone();
    EXPECT_EQ(buf->ref->load(), 2);
    EXPECT_EQ(ref1->data, original_ptr);

    // Release one — ref == 1, buffer not freed yet
    ref1->release();
    EXPECT_EQ(buf->ref->load(), 1);

    // Release the other — ref == 0, buffer freed (direct delete, no pool reuse)
    buf->release();
}

TEST(BufferTest, PoolExhaustedReturnsNull)
{
    SystemBufferPool pool(2); // max 2 outstanding
    Buffer          *a = pool.alloc(64);
    Buffer          *b = pool.alloc(64);
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(pool.alloc(64), nullptr); // exhausted
    a->release();                       // frees buffer, outstanding drops
    Buffer *c = pool.alloc(64);         // should succeed now
    ASSERT_NE(c, nullptr);
    b->release();
    c->release();
}

TEST(BufferTest, ExactCapacityNoBucketing)
{
    // Direct allocation — capacity is exactly what was requested (no
    // power-of-2 bucket rounding). This differs from the old pool which
    // rounded up to the next power of 2.
    SystemBufferPool pool;
    Buffer          *buf = pool.alloc(200);
    ASSERT_NE(buf, nullptr);
    EXPECT_EQ(buf->capacity, 200u); // exact, not bucketed to 256
    buf->release();
}
