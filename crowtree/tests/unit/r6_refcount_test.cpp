// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// R6: per-page refcount state machine tests. Verifies the no-double-free
// protocol between pin() / unpin() / retire_with_pins() on PageBase.
#include "crowtree/page_types.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

using namespace crowtree;

namespace
{
// A minimal PageBase subclass that counts destructions. PageBase itself is
// abstract (virtual dtor), so we need a concrete leaf-like type.
struct CountedPage : PageBase
{
    explicit CountedPage(std::atomic<int> *c) : PageBase(page_type::kLeafBase), freed(c)
    {
    }

    ~CountedPage() override
    {
        freed->fetch_add(1, std::memory_order_relaxed);
    }

    std::atomic<int> *freed;
};
} // namespace

TEST(R6Refcount, RetireWithNoPinsFreesImmediately)
{
    std::atomic<int> freed{0};
    auto            *p = new CountedPage(&freed);
    p->retire_with_pins();
    EXPECT_EQ(freed.load(), 1);
}

TEST(R6Refcount, RetireWithPinsDefersUntilLastUnpin)
{
    std::atomic<int> freed{0};
    auto            *p = new CountedPage(&freed);
    p->pin();
    p->pin();
    p->retire_with_pins(); // EBR drained, but 2 pins outstanding → no free
    EXPECT_EQ(freed.load(), 0);
    p->unpin(); // NOLINT(clang-analyzer-cplusplus.NewDelete) count 2→1, no free
    EXPECT_EQ(freed.load(), 0); // still 1 pin
    p->unpin(); // last unpin frees (count 1→0 + retired bit)
    EXPECT_EQ(freed.load(), 1);
}

TEST(R6Refcount, UnpinBeforeRetireDoesNotFree)
{
    std::atomic<int> freed{0};
    auto            *p = new CountedPage(&freed);
    p->pin();
    p->unpin(); // NOLINT(clang-analyzer-cplusplus.NewDelete) no retired bit → no free
    EXPECT_EQ(freed.load(), 0);
    p->retire_with_pins(); // now retire, count is 0 → free
    EXPECT_EQ(freed.load(), 1);
}

TEST(R6Refcount, ConcurrentPinUnpinRetireNoDoubleFree)
{
    std::atomic<int>  freed{0};
    auto             *p = new CountedPage(&freed);
    std::atomic<bool> stop{false};
    std::atomic<int>  live_pins{0};

    // Pinners: continuously pin then unpin until retired.
    std::vector<std::thread> pinners;
    pinners.reserve(4);
    for (int i = 0; i < 4; ++i) {
        pinners.emplace_back([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                p->pin(); // NOLINT(clang-analyzer-cplusplus.NewDelete)
                live_pins.fetch_add(1, std::memory_order_relaxed);
                // brief hold
                live_pins.fetch_sub(1, std::memory_order_relaxed);
                p->unpin(); // NOLINT(clang-analyzer-cplusplus.NewDelete)
            }
        });
    }

    // Let pinners run briefly, then retire.
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    p->retire_with_pins();
    stop.store(true, std::memory_order_relaxed);
    for (auto &t : pinners) {
        t.join();
    }
    // After all pinners stopped, any pin taken before retire_with_pins() is
    // already unpinned (the pinners' loop is pin→unpin→check stop). Any pin
    // taken after retire_with_pins() can't happen (the slot would be cleared
    // in real use; here we just verify no double-free / no leak). The page is
    // freed exactly once: either by retire_with_pins() (if count hit 0) or by
    // the last unpin.
    EXPECT_EQ(freed.load(), 1);
}
