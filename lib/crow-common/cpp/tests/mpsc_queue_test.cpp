// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-common/mpsc_queue.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

using crow::common::MpscQueue;

// ── Basic single-thread push/drain ────────────────────────────────

TEST(MpscQueueTest, PushDrainSingleThread)
{
    MpscQueue<int> q(256);
    ASSERT_EQ(q.capacity(), 256);

    ASSERT_TRUE(q.try_push(1));
    ASSERT_TRUE(q.try_push(2));
    ASSERT_TRUE(q.try_push(3));
    ASSERT_TRUE(q.has_pending());

    int out[8] = {};
    int n      = q.drain(out, 8);
    ASSERT_EQ(n, 3);
    EXPECT_EQ(out[0], 1);
    EXPECT_EQ(out[1], 2);
    EXPECT_EQ(out[2], 3);
    EXPECT_FALSE(q.has_pending());
}

TEST(MpscQueueTest, DrainEmpty)
{
    MpscQueue<int> q(16);
    int            out[4] = {};
    EXPECT_EQ(q.drain(out, 4), 0);
    EXPECT_FALSE(q.has_pending());
}

TEST(MpscQueueTest, DrainPartialBatch)
{
    MpscQueue<int> q(64);
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(q.try_push(i));
    }

    int out[4] = {};
    EXPECT_EQ(q.drain(out, 4), 4);
    EXPECT_EQ(out[0], 0);
    EXPECT_EQ(out[3], 3);
    EXPECT_TRUE(q.has_pending());

    EXPECT_EQ(q.drain(out, 4), 4);
    EXPECT_EQ(q.drain(out, 4), 2);
    EXPECT_FALSE(q.has_pending());
}

// ── Backpressure ──────────────────────────────────────────────────

TEST(MpscQueueTest, BackpressureWhenFull)
{
    MpscQueue<int> q(4);

    // Fill the queue to capacity.
    for (uint32_t i = 0; i < q.capacity(); i++) {
        ASSERT_TRUE(q.try_push(static_cast<int>(i))) << "push " << i << " failed";
    }
    // Next push should fail — backpressure.
    EXPECT_FALSE(q.try_push(99));

    // Drain one, then one more slot is available.
    int out[1] = {};
    ASSERT_EQ(q.drain(out, 1), 1);
    ASSERT_TRUE(q.try_push(99));
    EXPECT_FALSE(q.try_push(100));
}

// ── Wrap-around ───────────────────────────────────────────────────

TEST(MpscQueueTest, WrapAroundRepeated)
{
    MpscQueue<int> q(4);

    // Push 2, drain 2, repeat — exercises ring wrap-around many times.
    for (int i = 0; i < 30; i++) {
        ASSERT_TRUE(q.try_push(i * 2));
        ASSERT_TRUE(q.try_push(i * 2 + 1));
        int out[2] = {};
        ASSERT_EQ(q.drain(out, 2), 2);
        EXPECT_EQ(out[0], i * 2);
        EXPECT_EQ(out[1], i * 2 + 1);
    }
    EXPECT_FALSE(q.has_pending());
}

// ── Capacity rounding ─────────────────────────────────────────────

TEST(MpscQueueTest, CapacityRoundsUpToPow2)
{
    MpscQueue<int> q3(3);
    EXPECT_EQ(q3.capacity(), 4);

    MpscQueue<int> q5(5);
    EXPECT_EQ(q5.capacity(), 8);

    MpscQueue<int> q256(256);
    EXPECT_EQ(q256.capacity(), 256);
}

// ── Pointer type (matches crow-rpc OutFrame* usage) ───────────────

TEST(MpscQueueTest, PointerElementType)
{
    MpscQueue<int *> q(16);
    int              a = 10;
    int              b = 20;

    ASSERT_TRUE(q.try_push(&a));
    ASSERT_TRUE(q.try_push(&b));

    int *out[2] = {};
    ASSERT_EQ(q.drain(out, 2), 2);
    EXPECT_EQ(out[0], &a);
    EXPECT_EQ(out[1], &b);
    EXPECT_FALSE(q.has_pending());
}

// ── MPSC stress test ──────────────────────────────────────────────
//
// N producer threads each push M ints into the queue. One consumer
// thread drains until it has collected N*M values. Verifies no losses.

TEST(MpscQueueTest, MpscStress)
{
    constexpr int      PRODUCERS = 8;
    constexpr int      PER_PROD  = 2000;
    constexpr int      TOTAL     = PRODUCERS * PER_PROD;
    constexpr uint32_t CAP       = 256;

    MpscQueue<int> q(CAP);

    std::atomic<int> next_value{0};
    std::atomic<int> drained_count{0};

    auto producer_fn = [&]() {
        for (;;) {
            int v = next_value.fetch_add(1, std::memory_order_relaxed);
            if (v >= TOTAL) {
                break;
            }
            while (!q.try_push(v)) {
                std::this_thread::yield();
            }
        }
    };

    auto consumer_fn = [&]() {
        while (drained_count.load(std::memory_order_relaxed) < TOTAL) {
            int out[64];
            int n = q.drain(out, 64);
            if (n > 0) {
                drained_count.fetch_add(n, std::memory_order_relaxed);
            }
            else {
                std::this_thread::yield();
            }
        }
    };

    std::thread              consumer(consumer_fn);
    std::vector<std::thread> producers;
    for (int p = 0; p < PRODUCERS; p++) {
        producers.emplace_back(producer_fn);
    }
    for (auto &t : producers) {
        t.join();
    }
    consumer.join();

    EXPECT_EQ(drained_count.load(), TOTAL);
    EXPECT_FALSE(q.has_pending());
}

// ── MPSC stress with backpressure (small queue) ───────────────────

TEST(MpscQueueTest, MpscStressSmallQueue)
{
    constexpr int      PRODUCERS = 4;
    constexpr int      PER_PROD  = 1000;
    constexpr int      TOTAL     = PRODUCERS * PER_PROD;
    constexpr uint32_t CAP       = 4;

    MpscQueue<int> q(CAP);

    std::atomic<int> next_value{0};
    std::atomic<int> drained_count{0};

    auto producer_fn = [&]() {
        for (;;) {
            int v = next_value.fetch_add(1, std::memory_order_relaxed);
            if (v >= TOTAL) {
                break;
            }
            while (!q.try_push(v)) {
                std::this_thread::yield();
            }
        }
    };

    auto consumer_fn = [&]() {
        while (drained_count.load(std::memory_order_relaxed) < TOTAL) {
            int out[4];
            int n = q.drain(out, 4);
            if (n > 0) {
                drained_count.fetch_add(n, std::memory_order_relaxed);
            }
            else {
                std::this_thread::yield();
            }
        }
    };

    std::thread              consumer(consumer_fn);
    std::vector<std::thread> producers;
    for (int p = 0; p < PRODUCERS; p++) {
        producers.emplace_back(producer_fn);
    }
    for (auto &t : producers) {
        t.join();
    }
    consumer.join();

    EXPECT_EQ(drained_count.load(), TOTAL);
    EXPECT_FALSE(q.has_pending());
}
