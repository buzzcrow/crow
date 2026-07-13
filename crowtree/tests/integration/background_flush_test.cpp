// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Open issue: Options.background_flush /
// flush_interval_ms existed but were never wired up (maybe_flush() only ever
// checked the size thresholds). This exercises the background flush thread
// added to fix that: a low/no-write-rate workload still becomes
// durable-eligible (drained out of the MemTable) on a timer, without an
// explicit flush() call, and the thread must not race with Crowtree::open()'s
// single-threaded recovery mutations.
#include "crowtree/crowtree.h"
#include "crowtree/page_store.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

using namespace crowtree;

namespace
{

Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

} // namespace

TEST(BackgroundFlush, DisabledByDefaultNoAutoFlush)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.flush_interval_ms = 20; // short, but background_flush stays false (default)

    std::unique_ptr<Crowtree> t;
    ASSERT_TRUE(Crowtree::open(opt, &t).ok());
    ASSERT_TRUE(t->apply(1, put_one("a", "1")).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // Nothing should have drained the MemTable: no background thread runs.
    EXPECT_EQ(t->memtable_count(), 1U);
}

TEST(BackgroundFlush, PeriodicFlushDrainsMemTableWithoutExplicitCall)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store        = &store;
    opt.background_flush  = true;
    opt.flush_interval_ms = 20;

    std::unique_ptr<Crowtree> t;
    ASSERT_TRUE(Crowtree::open(opt, &t).ok());
    ASSERT_TRUE(t->apply(1, put_one("a", "1")).ok());
    EXPECT_EQ(t->memtable_count(), 1U);

    // Give the background thread a few intervals to fire; no explicit flush().
    // Poll last_applied_slot() (set at the very end of flush(), after the
    // drained entries are fully folded into L1) rather than memtable_count()
    // (which drops to 0 earlier, mid-flush) to avoid a TOCTOU race in the test
    // itself between "drained from L0" and "durable in L1".
    bool settled = false;
    for (int i = 0; i < 50 && !settled; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        settled = (t->last_applied_slot() >= 1U);
    }
    EXPECT_TRUE(settled) << "background flush thread never advanced last_applied_slot";
    EXPECT_EQ(t->memtable_count(), 0U);

    uint64_t slot = 0;
    std::string value;
    EXPECT_TRUE(t->get("a", &slot, &value));
    EXPECT_EQ(value, "1");
}

TEST(BackgroundFlush, SafeAcrossOpenRecovery)
{
    // Regression guard: the background thread must not start until *after*
    // Crowtree::open()'s recovery mutations finish (recovery touches the tree
    // directly, without write_mutex_). Reopen a store with prior durable state
    // and a very short interval so the thread would fire almost immediately if
    // started too early.
    MemPageStore store(1);
    {
        Options opt;
        opt.page_store = &store;
        std::unique_ptr<Crowtree> t;
        ASSERT_TRUE(Crowtree::open(opt, &t).ok());
        ASSERT_TRUE(t->apply(1, put_one("k", "v")).ok());
        ASSERT_TRUE(t->flush().ok());
        ASSERT_TRUE(t->snapshot(nullptr).ok());
    }

    for (int i = 0; i < 20; ++i) {
        Options opt;
        opt.page_store        = &store;
        opt.background_flush  = true;
        opt.flush_interval_ms = 1; // as aggressive as possible
        std::unique_ptr<Crowtree> t;
        ASSERT_TRUE(Crowtree::open(opt, &t).ok());
        uint64_t slot = 0;
        std::string value;
        EXPECT_TRUE(t->get("k", &slot, &value));
        EXPECT_EQ(value, "v");
        // Destructor must cleanly stop + join the thread on every iteration
        // (ASan/TSan would flag a leaked/racing thread otherwise).
    }
}
