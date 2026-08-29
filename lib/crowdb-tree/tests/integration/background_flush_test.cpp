// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Explicit flush() drains the MemTable into L1 without a background thread.
// The upper-layer maintenance loop (run_pass) is responsible for calling
// flush() periodically.
#include "crowdb-tree/crowdb-tree.h"
#include "crowdb-tree/page_store.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

using namespace crowdb::tree;

namespace
{

Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

} // namespace

TEST(ExplicitFlush, NoAutoFlushWithoutExplicitCall)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store = &store;

    std::unique_ptr<Crowdbtree> t;
    ASSERT_TRUE(Crowdbtree::open(opt, &t).ok());
    ASSERT_TRUE(t->apply(1, put_one("a", "1")).ok());
    // Without an explicit flush(), nothing drains the MemTable.
    EXPECT_EQ(t->memtable_count(), 1U);
    EXPECT_EQ(t->last_applied_slot(), 0U);
}

TEST(ExplicitFlush, FlushDrainsMemTable)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store = &store;

    std::unique_ptr<Crowdbtree> t;
    ASSERT_TRUE(Crowdbtree::open(opt, &t).ok());
    ASSERT_TRUE(t->apply(1, put_one("a", "1")).ok());
    EXPECT_EQ(t->memtable_count(), 1U);

    ASSERT_TRUE(t->flush().ok());
    EXPECT_EQ(t->last_applied_slot(), 1U);
    EXPECT_EQ(t->memtable_count(), 0U);

    uint64_t    slot = 0;
    std::string value;
    EXPECT_TRUE(t->get("a", &slot, &value));
    EXPECT_EQ(value, "1");
}

TEST(ExplicitFlush, SafeAcrossOpenRecovery)
{
    // Reopen a store with prior durable state: recovery must complete before
    // any external access, and the recovered state must be correct.
    MemPageStore store(1);
    {
        Options opt;
        opt.page_store = &store;
        std::unique_ptr<Crowdbtree> t;
        ASSERT_TRUE(Crowdbtree::open(opt, &t).ok());
        ASSERT_TRUE(t->apply(1, put_one("k", "v")).ok());
        ASSERT_TRUE(t->flush().ok());
        ASSERT_TRUE(t->snapshot(nullptr).ok());
    }

    for (int i = 0; i < 20; ++i) {
        Options opt;
        opt.page_store = &store;
        std::unique_ptr<Crowdbtree> t;
        ASSERT_TRUE(Crowdbtree::open(opt, &t).ok());
        uint64_t    slot = 0;
        std::string value;
        EXPECT_TRUE(t->get("k", &slot, &value));
        EXPECT_EQ(value, "v");
    }
}
