// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// CT11: versioned root / snapshot view tests.
#include "crow-tree/crow-tree.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

using namespace crow::tree;

namespace
{
Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}
} // namespace

TEST(Version, FlushBumpsVersionAndTagsSlot)
{
    Crowtree t;
    EXPECT_EQ(t.version(), 0U);
    ASSERT_TRUE(t.apply(1, put_one("a", "A1")).ok());
    ASSERT_TRUE(t.flush().ok());
    EXPECT_EQ(t.version(), 1U);
    ASSERT_TRUE(t.apply(2, put_one("b", "B2")).ok());
    ASSERT_TRUE(t.flush().ok());
    EXPECT_EQ(t.version(), 2U);
}

TEST(Version, SnapshotTagEqualsFlushedSlot)
{
    Crowtree t;
    ASSERT_TRUE(t.apply(7, put_one("a", "A7")).ok());
    t.force_advance_slot(7);
    ASSERT_TRUE(t.flush().ok());
    auto snap = t.snapshot_view();
    EXPECT_EQ(snap->at_slot(), 7U);
}

TEST(Version, SnapshotIsStableWhileWriterChurns)
{
    Crowtree t;
    ASSERT_TRUE(t.apply(1, put_one("a", "A1")).ok());
    ASSERT_TRUE(t.apply(1, put_one("b", "B1")).ok());
    ASSERT_TRUE(t.flush().ok());
    auto snap = t.snapshot_view();
    EXPECT_EQ(snap->at_slot(), 1U);
    ASSERT_EQ(snap->size(), 2U);

    // Mutate the tree heavily after pinning the snapshot.
    for (uint64_t s = 2; s <= 50; ++s) {
        ASSERT_TRUE(t.apply(s, put_one("a", "A" + std::to_string(s))).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    // The pinned snapshot still reflects slot 1.
    uint64_t    slot;
    std::string v;
    ASSERT_TRUE(snap->get(Slice("a"), &slot, &v));
    EXPECT_EQ(v, "A1");
    EXPECT_EQ(slot, 1U);
    EXPECT_EQ(snap->size(), 2U);

    // A fresh snapshot reflects the latest.
    auto snap2 = t.snapshot_view();
    ASSERT_TRUE(snap2->get(Slice("a"), &slot, &v));
    EXPECT_EQ(v, "A50");
}

TEST(Version, SnapshotIncludesTombstonesButGetSkips)
{
    Crowtree t;
    ASSERT_TRUE(t.apply(1, put_one("a", "A1")).ok());
    ASSERT_TRUE(t.flush().ok());
    Batch del{{{.key = "a", .kind = OpKind::kDelete, .value = ""}}};
    ASSERT_TRUE(t.apply(2, del).ok());
    ASSERT_TRUE(t.flush().ok());
    auto snap = t.snapshot_view();
    // iter_all (entries) includes the tombstone...
    EXPECT_EQ(snap->size(), 1U);
    // ...but get skips it.
    uint64_t    slot;
    std::string v;
    EXPECT_FALSE(snap->get(Slice("a"), &slot, &v));
}

TEST(Version, CompareDetectsDiffs)
{
    Crowtree t;
    ASSERT_TRUE(t.apply(1, put_one("a", "A1")).ok());
    ASSERT_TRUE(t.apply(1, put_one("b", "B1")).ok());
    ASSERT_TRUE(t.flush().ok());
    auto s1 = t.snapshot_view();
    EXPECT_TRUE(s1->compare(*s1).empty()); // identical

    ASSERT_TRUE(t.apply(2, put_one("c", "C2")).ok());
    ASSERT_TRUE(t.flush().ok());
    auto s2    = t.snapshot_view();
    auto diffs = s1->compare(*s2);
    ASSERT_EQ(diffs.size(), 1U);
    EXPECT_EQ(diffs[0].key, "c");
    EXPECT_EQ(diffs[0].kind, engine_diff::kOnlyRight);
}

TEST(Version, RefcountLifecycle)
{
    Crowtree t;
    ASSERT_TRUE(t.apply(1, put_one("a", "A1")).ok());
    ASSERT_TRUE(t.flush().ok());
    std::shared_ptr<Snapshot> a = t.snapshot_view();
    EXPECT_EQ(a.use_count(), 1);
    {
        [[maybe_unused]] std::shared_ptr<Snapshot> b = a; // NOLINT(performance-unnecessary-copy-initialization)
        EXPECT_EQ(a.use_count(), 2);
    }
    EXPECT_EQ(a.use_count(), 1);
}
