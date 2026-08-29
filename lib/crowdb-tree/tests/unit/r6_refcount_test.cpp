// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// R6: per-page refcount state machine tests. Verifies the no-double-free
// protocol between pin() / unpin() / retire_with_pins() on PageBase.
#include "crowdb-tree/page_types.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

using namespace crowdb::tree;

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
    p->unpin();                 // NOLINT(clang-analyzer-cplusplus.NewDelete) count 2→1, no free
    EXPECT_EQ(freed.load(), 0); // still 1 pin
    p->unpin();                 // NOLINT(clang-analyzer-cplusplus.NewDelete) last unpin frees (count 1→0 + retired bit)
    EXPECT_EQ(freed.load(), 1);
}

TEST(R6Refcount, UnpinBeforeRetireDoesNotFree)
{
    std::atomic<int> freed{0};
    auto            *p = new CountedPage(&freed);
    p->pin();
    p->unpin(); // NOLINT(clang-analyzer-cplusplus.NewDelete) no retired bit → no free
    EXPECT_EQ(freed.load(), 0);
    p->retire_with_pins(); // NOLINT(clang-analyzer-cplusplus.NewDelete) now retire, count is 0 → free
    EXPECT_EQ(freed.load(), 1);
}

TEST(R6Refcount, ConcurrentPinsThenRetireFreesOnce)
{
    // Test the scenario that matters for R6: multiple threads hold pins
    // concurrently, then all unpin, then retire frees exactly once. This
    // matches the real usage (PinnedSnapshot holds pins, drops them, retire
    // deleter runs). The concurrent pin-after-retire race is prevented by the
    // epoch guard + slot-clear protocol in the real code, not by the refcount
    // alone, so we don't test it here.
    std::atomic<int> freed{0};
    auto            *p = new CountedPage(&freed);

    // 4 threads each pin once (simulating 4 PinnedSnapshots holding the page).
    std::vector<std::thread> pinners;
    pinners.reserve(4);
    std::atomic<int> pins_done{0};
    for (int i = 0; i < 4; ++i) {
        pinners.emplace_back([&] {
            p->pin();
            pins_done.fetch_add(1, std::memory_order_relaxed);
        });
    }
    for (auto &t : pinners) {
        t.join();
    }
    EXPECT_EQ(pins_done.load(), 4);
    EXPECT_EQ(freed.load(), 0); // 4 pins outstanding, not freed

    // Retire with pins outstanding — deleter defers.
    p->retire_with_pins();
    EXPECT_EQ(freed.load(), 0); // still 4 pins outstanding

    // Unpin from 4 different threads (simulating PinnedSnapshots dropping on
    // different threads). The last unpin frees.
    std::vector<std::thread> unpinners;
    unpinners.reserve(4);
    for (int i = 0; i < 4; ++i) {
        unpinners.emplace_back([&] {
            p->unpin(); // NOLINT(clang-analyzer-cplusplus.NewDelete)
        });
    }
    for (auto &t : unpinners) {
        t.join();
    }
    EXPECT_EQ(freed.load(), 1); // last unpin freed the page
}
