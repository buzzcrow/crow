// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Phase 2: exercise the async
// C API surface (ct_get_async/ct_flush_async/ct_snapshot_async,
// ct_future_poll/ct_future_free, ct_reactor_eventfd) end to end, not just
// the underlying Crowtree::*_async methods -- these are the four cases
// called out for this file.
//
// Runs against a durable (file-backed) ct_tree so ct_evict_clean_leaves can
// force the demand-load ("slow path") that ct_get_async's retry loop exists
// for. On a build with liburing (CROWTREE_HAVE_LIBURING), ct_open wires a
// real Reactor + BlockAsyncPageStore, so the slow path genuinely completes
// off the Reactor thread; without liburing (or for an in-memory tree) it
// falls back to completing synchronously -- every assertion
// below is written to hold either way (poll-until-done with a bounded
// retry), except where a test specifically distinguishes the two.
#include "crowtree/c_api.h"
#include "test_tmp.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

namespace
{
std::string make_key(int i)
{
    std::array<char, 16> b{};
    snprintf(b.data(), b.size(), "key%05d", i);
    return b.data();
}

ct_status put_flush(ct_tree *t, uint64_t slot, const std::string &k, const std::string &v)
{
    ct_status s = ct_apply_put(t, slot, reinterpret_cast<const uint8_t *>(k.data()), k.size(),
                               reinterpret_cast<const uint8_t *>(v.data()), v.size());
    if (s != 0) {
        return s;
    }
    return ct_flush(t);
}

// Polls `f` until ct_future_poll reports done, sleeping briefly between
// attempts; fails the test (via a fatal EXPECT-style return) if it never
// completes within a generous bound. Returns the operation's ct_status.
ct_status poll_until_done(ct_future *f, int32_t *out_found, uint64_t *out_slot, ct_buf *out_value)
{
    for (int attempt = 0; attempt < 2000; ++attempt) {
        int32_t   done = 0;
        ct_status st   = ct_future_poll(f, &done, out_found, out_slot, out_value);
        if (done != 0) {
            return st;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ADD_FAILURE() << "future never completed";
    return static_cast<ct_status>(-1);
}

} // namespace

// Case 1: a resident L1 hit's future is already done on
// the very first ct_future_poll call -- no reactor round trip needed.
TEST(AsyncGet, FastPathHitCompletesSynchronously)
{
    ct_options opt = {};
    ct_tree   *t   = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);
    ASSERT_EQ(put_flush(t, 1, make_key(0), "hello"), 0);

    std::string k = make_key(0);
    ct_future  *f = ct_get_async(t, reinterpret_cast<const uint8_t *>(k.data()), k.size());
    ASSERT_NE(f, nullptr);

    int32_t   done  = 0;
    int32_t   found = 0;
    uint64_t  slot  = 0;
    ct_buf    val   = {};
    ct_status st    = ct_future_poll(f, &done, &found, &slot, &val);
    ASSERT_EQ(st, 0);
    EXPECT_EQ(done, 1) << "L1-resident hit must complete on the first poll";
    EXPECT_EQ(found, 1);
    EXPECT_EQ(std::string(reinterpret_cast<char *>(val.data), val.len), "hello");
    // zero-copy fast path: `val` may
    // now borrow directly from a resident frame -- passing it to
    // ct_free_buf would be a bad-free (ASan-caught: not malloc()-ed). The
    // future itself, not the buffer, is what must be released, and only
    // *after* the borrowed bytes have been read.
    ct_future_free(f);

    ct_close(t);
}

// Regression for the above: ct_future_poll on a resolved kGet future must
// NOT free it (unlike flush_async/snapshot_async) -- polling it again
// before the caller's own ct_future_free is safe and idempotent, since the
// borrowed value's backing frame is kept alive by the future's own epoch
// guard the whole time.
TEST(AsyncGet, FastPathValueSurvivesRepeatedPollsUntilExplicitFree)
{
    ct_options opt = {};
    ct_tree   *t   = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);
    ASSERT_EQ(put_flush(t, 1, make_key(0), "hello"), 0);

    std::string k = make_key(0);
    ct_future  *f = ct_get_async(t, reinterpret_cast<const uint8_t *>(k.data()), k.size());
    ASSERT_NE(f, nullptr);

    for (int i = 0; i < 3; ++i) {
        int32_t   done  = 0;
        int32_t   found = 0;
        uint64_t  slot  = 0;
        ct_buf    val   = {};
        ct_status st    = ct_future_poll(f, &done, &found, &slot, &val);
        ASSERT_EQ(st, 0);
        EXPECT_EQ(done, 1);
        EXPECT_EQ(found, 1);
        EXPECT_EQ(slot, 1U);
        EXPECT_EQ(std::string(reinterpret_cast<char *>(val.data), val.len), "hello");
    }
    ct_future_free(f);

    ct_close(t);
}

// Case 2: evict the key's leaf, forcing ct_get_async
// onto the miss path; the future eventually completes (via the Reactor
// thread on a liburing build, or synchronously as a fallback) with the
// correct value.
TEST(AsyncGet, MissAfterEvictionCompletesViaReactor)
{
    crowtree_test::TempDir tmp;
    ct_options             opt = {};
    opt.path                   = tmp.path.c_str();
    opt.backend                = CT_BACKEND_BLOCK;
    opt.iu_size                = 4096;
    opt.frame_bytes            = 4096;
    ct_tree *t                 = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);

    for (int i = 0; i < 20; ++i) {
        ASSERT_EQ(put_flush(t, i + 1, make_key(i), "val" + std::to_string(i)), 0);
    }
    uint64_t durable = 0;
    ASSERT_EQ(ct_snapshot(t, &durable), 0);
    EXPECT_EQ(durable, 20U);

    uint64_t evicted = ct_evict_clean_leaves(t, 0);
    EXPECT_GT(evicted, 0U) << "snapshot should have made every leaf clean and evictable";

    std::string k = make_key(5);
    ct_future  *f = ct_get_async(t, reinterpret_cast<const uint8_t *>(k.data()), k.size());
    ASSERT_NE(f, nullptr);

    int32_t   found = 0;
    uint64_t  slot  = 0;
    ct_buf    val   = {};
    ct_status st    = poll_until_done(f, &found, &slot, &val);
    ASSERT_EQ(st, 0);
    EXPECT_EQ(found, 1);
    EXPECT_EQ(std::string(reinterpret_cast<char *>(val.data), val.len), "val5");
    // A miss resolved via the Reactor always
    // materializes an owned copy (materialize_owned(), crowtree.cpp --
    // never a cross-thread-borrowed frame pointer), but ct_get_async's
    // handle is still only freed via ct_future_free, uniformly for both
    // the fast and slow paths.
    ct_future_free(f);

    // The reactor eventfd is a valid fd whenever a Reactor is wired (durable
    // tree + liburing build); -1 otherwise (design: -1 means "nothing will
    // ever be genuinely pending", still a well-defined answer either way).
    int32_t efd = ct_reactor_eventfd(t);
#ifdef CROWTREE_HAVE_LIBURING
    EXPECT_GE(efd, 0) << "a durable tree on a liburing build should have a real Reactor";
#else
    EXPECT_EQ(efd, -1);
#endif

    ct_close(t);
}

// Case 3: abandoning a still-(possibly-)pending future
// via ct_future_free must not crash or leak, regardless of whether the
// underlying I/O has already completed in the background by the time this
// runs (best-effort cancel). Run under ASan for the strongest
// signal (see the /coding sanitizer pass in this session's plan).
TEST(AsyncGet, FutureFreeBeforeCompletionDoesNotCrashOrLeak)
{
    crowtree_test::TempDir tmp;
    ct_options             opt = {};
    opt.path                   = tmp.path.c_str();
    opt.iu_size                = 4096;
    opt.frame_bytes            = 4096;
    ct_tree *t                 = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);

    for (int i = 0; i < 20; ++i) {
        ASSERT_EQ(put_flush(t, i + 1, make_key(i), "val" + std::to_string(i)), 0);
    }
    ASSERT_EQ(ct_snapshot(t, nullptr), 0);
    ct_evict_clean_leaves(t, 0);

    std::string k = make_key(5);
    ct_future  *f = ct_get_async(t, reinterpret_cast<const uint8_t *>(k.data()), k.size());
    ASSERT_NE(f, nullptr);
    ct_future_free(f); // abandon immediately, whether pending or already done

    // Give any in-flight reactor completion a moment to actually run and
    // discover the future was abandoned, then close the tree -- if
    // ct_future_free left a dangling ct_future_impl* anywhere, this is
    // where ASan would catch the use-after-free.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ct_close(t);
}

// Case 4: flush_async and snapshot_async both resolve
// to the same result their synchronous twins would. snapshot_async does
// genuine I/O ("flush / snapshot ... Always: write dirty pages
// to disk") and is pending on its first poll whenever a Reactor is wired.
// flush_async, in *this* engine, only drains the in-memory MemTable into
// L1 (Crowtree::flush() never touches page_store -- see its doc comment on
// crowtree.h) so it has no I/O to submit and always completes on the first
// poll; this is a deliberate, documented deviation from the design doc's
// literal table for this one case (verified against the real
// Crowtree::flush() implementation, not assumed).
TEST(AsyncFlushSnapshot, FlushCompletesImmediatelySnapshotEventually)
{
    crowtree_test::TempDir tmp;
    ct_options             opt = {};
    opt.path                   = tmp.path.c_str();
    opt.iu_size                = 4096;
    opt.frame_bytes            = 4096;
    ct_tree *t                 = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);

    ASSERT_EQ(ct_apply_put(t, 1, reinterpret_cast<const uint8_t *>("a"), 1, reinterpret_cast<const uint8_t *>("va"), 2),
              0);

    ct_future *ff = ct_flush_async(t);
    ASSERT_NE(ff, nullptr);
    int32_t   fdone = 0;
    ct_status fst   = ct_future_poll(ff, &fdone, nullptr, nullptr, nullptr);
    EXPECT_EQ(fdone, 1) << "flush_async never has genuine I/O to wait on in this engine";
    EXPECT_EQ(fst, 0);

    ct_future *sf = ct_snapshot_async(t);
    ASSERT_NE(sf, nullptr);
    uint64_t  slot = 0;
    ct_status sst  = poll_until_done(sf, nullptr, &slot, nullptr);
    EXPECT_EQ(sst, 0);
    EXPECT_EQ(slot, 1U) << "durable last_applied_slot from the completed snapshot";

    // Matches what the synchronous twins would have reported.
    EXPECT_EQ(ct_last_applied_slot(t), 1U);

    int32_t  found = 0;
    uint64_t gslot = 0;
    ct_buf   val   = {};
    ASSERT_EQ(ct_get(t, reinterpret_cast<const uint8_t *>("a"), 1, &found, &gslot, &val), 0);
    ASSERT_EQ(found, 1);
    EXPECT_EQ(std::string(reinterpret_cast<char *>(val.data), val.len), "va");
    ct_free_buf(&val);

    ct_close(t);
}

// Block-backend async snapshot: exercises BlockAsyncPageStore::submit_write
// + submit_fsync through the Reactor's io_uring on Linux (CROWTREE_HAVE_LIBURING).
// On non-liburing builds, snapshot_async falls back to the sync path — the
// assertions hold either way (poll-until-done).
TEST(AsyncSnapshot, BlockBackendAsyncSnapshotRoundTrip)
{
    crowtree_test::TempDir tmp;
    ct_options             opt = {};
    opt.path                   = tmp.path.c_str();
    opt.backend                = CT_BACKEND_BLOCK;
    opt.iu_size                = 4096;
    opt.frame_bytes            = 4096;
    opt.block_size             = 8 * 1024; // small blocks to force multi-extent
    ct_tree *t                 = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);

    // Write enough data to fill at least one block and exercise the write path.
    for (int i = 0; i < 30; ++i) {
        ASSERT_EQ(put_flush(t, i + 1, make_key(i), "val" + std::to_string(i)), 0);
    }

    // Async snapshot — on liburing builds this chains submit_write → ... →
    // submit_fsync → anchor write → submit_fsync → commit, all via io_uring.
    ct_future *sf = ct_snapshot_async(t);
    ASSERT_NE(sf, nullptr);
    uint64_t  slot = 0;
    ct_status sst  = poll_until_done(sf, nullptr, &slot, nullptr);
    EXPECT_EQ(sst, 0);
    EXPECT_EQ(slot, 30U);

    // sf is already freed by ct_future_poll (snapshot futures are freed on
    // done — see c_api.cpp line 646). Do NOT call ct_future_free here.

    // Verify data is readable after async snapshot.
    for (int i = 0; i < 30; ++i) {
        std::string k     = make_key(i);
        int32_t     found = 0;
        uint64_t    gslot = 0;
        ct_buf      val   = {};
        ASSERT_EQ(ct_get(t, reinterpret_cast<const uint8_t *>(k.data()), k.size(), &found, &gslot, &val), 0);
        ASSERT_EQ(found, 1);
        EXPECT_EQ(std::string(reinterpret_cast<char *>(val.data), val.len), "val" + std::to_string(i));
        ct_free_buf(&val);
    }

    // Reopen and verify durability.
    ct_close(t);
    t = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);
    for (int i = 0; i < 30; ++i) {
        std::string k     = make_key(i);
        int32_t     found = 0;
        uint64_t    gslot = 0;
        ct_buf      val   = {};
        ASSERT_EQ(ct_get(t, reinterpret_cast<const uint8_t *>(k.data()), k.size(), &found, &gslot, &val), 0);
        ASSERT_EQ(found, 1);
        EXPECT_EQ(std::string(reinterpret_cast<char *>(val.data), val.len), "val" + std::to_string(i));
        ct_free_buf(&val);
    }

    ct_close(t);
}
