// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// ct_scan_async, scan()'s async twin.
// scan() calls resident() per leaf (and for the initial root->leaf descent),
// which blocks synchronously on page_store->read_at() for a cold/unloaded
// page -- the same cost ct_get_async/KVFuture::Pending already exists to
// avoid for point reads. Mirrors async_get_test.cpp's structure/coverage
// (fast path, miss-after-eviction, abandon-before-completion), plus a
// dedicated equivalence check against ct_scan's own packed output.
#include "crow-tree/c_api.h"
#include "test_tmp.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstdio>
#include <map>
#include <string>
#include <thread>
#include <vector>

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

// Mirrors async_get_test.cpp's poll_until_done, adapted to ct_scan_async's
// out-param reuse (out_found carries *truncated, out_slot carries *count).
ct_status poll_scan_until_done(ct_future *f, int32_t *out_truncated, uint64_t *out_count, ct_buf *out_entries)
{
    for (int attempt = 0; attempt < 2000; ++attempt) {
        int32_t   done = 0;
        ct_status st   = ct_future_poll(f, &done, out_truncated, out_count, out_entries);
        if (done != 0) {
            return st;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ADD_FAILURE() << "future never completed";
    return static_cast<ct_status>(-1);
}

// Unpack ct_scan/ct_scan_async's shared wire format:
// [u32 klen][key][u64 slot][u32 vlen][val] * count.
std::map<std::string, std::string> unpack_entries(const ct_buf &buf, uint64_t count)
{
    std::map<std::string, std::string> out;
    const auto                        *p       = reinterpret_cast<const uint8_t *>(buf.data);
    size_t                             pos     = 0;
    auto                               get_u32 = [&]() {
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i) {
            v |= static_cast<uint32_t>(p[pos + i]) << (8 * i);
        }
        pos += 4;
        return v;
    };
    auto get_u64 = [&]() {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) {
            v |= static_cast<uint64_t>(p[pos + i]) << (8 * i);
        }
        pos += 8;
        return v;
    };
    for (uint64_t i = 0; i < count; ++i) {
        uint32_t    klen = get_u32();
        std::string key(reinterpret_cast<const char *>(p + pos), klen);
        pos += klen;
        (void)get_u64(); // slot, unused by these tests
        pos += 1;        // tombstone flag, unused by these tests
        uint32_t    vlen = get_u32();
        std::string val(reinterpret_cast<const char *>(p + pos), vlen);
        pos += vlen;
        out[key] = val;
    }
    return out;
}

} // namespace

// Fast path: every leaf already resident -- the future is done on the very
// first poll, and the packed output matches ct_scan's own synchronous result
// byte for byte.
TEST(AsyncScan, FastPathAllResidentCompletesSynchronously)
{
    ct_options opt = {};
    ct_tree   *t   = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(put_flush(t, i + 1, make_key(i), "v" + std::to_string(i)), 0);
    }

    ct_future *f = ct_scan_async(t, nullptr, 0, nullptr, 0, 0);
    ASSERT_NE(f, nullptr);

    int32_t   done      = 0;
    int32_t   truncated = 0;
    uint64_t  count     = 0;
    ct_buf    entries   = {};
    ct_status st        = ct_future_poll(f, &done, &truncated, &count, &entries);
    ASSERT_EQ(st, 0);
    EXPECT_EQ(done, 1) << "an all-resident scan must complete on the first poll";
    EXPECT_EQ(truncated, 0);
    ASSERT_EQ(count, 10U);

    auto got = unpack_entries(entries, count);
    ct_free_buf(&entries); // kScan is always an owned buffer -- unlike kGet.
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(got[make_key(i)], "v" + std::to_string(i));
    }

    // ct_future_poll already freed a resolved kScan future (mirrors
    // flush/snapshot, not get) -- no ct_future_free follow-up.
    ct_close(t);
}

// Equivalence: ct_scan_async's packed result matches ct_scan's synchronous
// result exactly for the same prefix/limit, including a limit that
// truncates.
TEST(AsyncScan, MatchesSyncScanOutputIncludingTruncation)
{
    ct_options opt = {};
    ct_tree   *t   = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);
    for (int i = 0; i < 30; ++i) {
        ASSERT_EQ(put_flush(t, i + 1, make_key(i), "v" + std::to_string(i)), 0);
    }

    ct_buf   sync_entries = {};
    uint64_t sync_count   = 0;
    int32_t  sync_trunc   = 0;
    ASSERT_EQ(ct_scan(t, nullptr, 0, nullptr, 0, 12, 0, &sync_entries, &sync_count, &sync_trunc), 0);
    auto sync_map = unpack_entries(sync_entries, sync_count);
    ct_free_buf(&sync_entries);

    ct_future *f = ct_scan_async(t, nullptr, 0, nullptr, 0, 12);
    ASSERT_NE(f, nullptr);
    int32_t  async_trunc   = 0;
    uint64_t async_count   = 0;
    ct_buf   async_entries = {};
    ASSERT_EQ(poll_scan_until_done(f, &async_trunc, &async_count, &async_entries), 0);
    auto async_map = unpack_entries(async_entries, async_count);
    ct_free_buf(&async_entries);

    EXPECT_EQ(sync_count, async_count);
    EXPECT_EQ(sync_trunc, async_trunc);
    EXPECT_EQ(sync_trunc, 1) << "limit=12 over 30 keys should truncate";
    EXPECT_EQ(sync_map, async_map);

    ct_close(t);
}

// Miss path: evict every leaf so the scan must demand-load at least one
// cold leaf; the future eventually completes (via the Reactor thread on a
// liburing build, or synchronously as a fallback) with the full, correct
// result -- exercising scan_async_attempt's retry-the-whole-scan loop
// across however many cold leaves this range spans.
TEST(AsyncScan, MissAfterEvictionCompletesViaReactor)
{
    crow::tree_test::TempDir tmp;
    ct_options               opt = {};
    opt.path                     = tmp.path.c_str();
    opt.iu_size                  = 4096;
    opt.frame_bytes              = 4096;
    ct_tree *t                   = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);

    for (int i = 0; i < 40; ++i) {
        ASSERT_EQ(put_flush(t, i + 1, make_key(i), "val" + std::to_string(i)), 0);
    }
    uint64_t durable = 0;
    ASSERT_EQ(ct_snapshot(t, &durable), 0);
    EXPECT_EQ(durable, 40U);

    uint64_t evicted = ct_evict_clean_leaves(t, 0);
    EXPECT_GT(evicted, 0U) << "snapshot should have made every leaf clean and evictable";

    ct_future *f = ct_scan_async(t, nullptr, 0, nullptr, 0, 0);
    ASSERT_NE(f, nullptr);

    int32_t   truncated = 0;
    uint64_t  count     = 0;
    ct_buf    entries   = {};
    ct_status st        = poll_scan_until_done(f, &truncated, &count, &entries);
    ASSERT_EQ(st, 0);
    EXPECT_EQ(truncated, 0);
    ASSERT_EQ(count, 40U);
    auto got = unpack_entries(entries, count);
    ct_free_buf(&entries);
    for (int i = 0; i < 40; ++i) {
        EXPECT_EQ(got[make_key(i)], "val" + std::to_string(i)) << "missing/wrong " << make_key(i);
    }

    ct_close(t);
}

// Abandoning a still-(possibly-)pending scan future via ct_future_free must
// not crash or leak, regardless of whether the underlying I/O has already
// completed in the background by the time this runs (best-effort cancel).
TEST(AsyncScan, FutureFreeBeforeCompletionDoesNotCrashOrLeak)
{
    crow::tree_test::TempDir tmp;
    ct_options               opt = {};
    opt.path                     = tmp.path.c_str();
    opt.iu_size                  = 4096;
    opt.frame_bytes              = 4096;
    ct_tree *t                   = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);

    for (int i = 0; i < 40; ++i) {
        ASSERT_EQ(put_flush(t, i + 1, make_key(i), "val" + std::to_string(i)), 0);
    }
    ASSERT_EQ(ct_snapshot(t, nullptr), 0);
    ct_evict_clean_leaves(t, 0);

    ct_future *f = ct_scan_async(t, nullptr, 0, nullptr, 0, 0);
    ASSERT_NE(f, nullptr);
    ct_future_free(f); // abandon immediately, whether pending or already done

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ct_close(t);
}

// Empty-tree scan_async resolves immediately with zero entries, not an
// infinite retry loop (root_page_id_ == kInvalidPageId is handled the same
// way scan() itself handles it -- no page to ever be "pending").
TEST(AsyncScan, EmptyTreeResolvesImmediately)
{
    ct_options opt = {};
    ct_tree   *t   = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);

    ct_future *f = ct_scan_async(t, nullptr, 0, nullptr, 0, 0);
    ASSERT_NE(f, nullptr);
    int32_t   done  = 0;
    uint64_t  count = 0;
    ct_status st    = ct_future_poll(f, &done, nullptr, &count, nullptr);
    ASSERT_EQ(st, 0);
    EXPECT_EQ(done, 1);
    EXPECT_EQ(count, 0U);

    ct_close(t);
}

// start_after cursor: a scan with a non-empty start_after returns only keys
// strictly greater than the cursor, in order, up to the limit. The descent
// lands on the leaf containing the cursor instead of the first prefix leaf,
// so earlier entries are never visited (the deep-pagination win R37 targets).
TEST(AsyncScan, StartAfterCursorSkipsEarlierEntries)
{
    ct_options opt = {};
    ct_tree   *t   = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);
    for (int i = 0; i < 30; ++i) {
        ASSERT_EQ(put_flush(t, i + 1, make_key(i), "v" + std::to_string(i)), 0);
    }

    // Cursor at key009: only key010..key029 should come back (20 keys).
    std::string cursor = make_key(9);
    ct_future  *f = ct_scan_async(t, nullptr, 0, reinterpret_cast<const uint8_t *>(cursor.data()), cursor.size(), 0);
    ASSERT_NE(f, nullptr);
    int32_t  async_trunc   = 0;
    uint64_t async_count   = 0;
    ct_buf   async_entries = {};
    ASSERT_EQ(poll_scan_until_done(f, &async_trunc, &async_count, &async_entries), 0);
    auto async_map = unpack_entries(async_entries, async_count);
    ct_free_buf(&async_entries);
    EXPECT_EQ(async_trunc, 0);
    EXPECT_EQ(async_count, 20U);
    EXPECT_EQ(async_map.size(), 20U);
    // Every returned key is strictly greater than the cursor.
    for (const auto &kv : async_map) {
        EXPECT_GT(kv.first, cursor);
    }
    // First returned key is the one immediately after the cursor.
    EXPECT_EQ(async_map.begin()->first, make_key(10));

    // Cursor + limit: a page of 5 starting after key009.
    f = ct_scan_async(t, nullptr, 0, reinterpret_cast<const uint8_t *>(cursor.data()), cursor.size(), 5);
    ASSERT_NE(f, nullptr);
    async_trunc   = 0;
    async_count   = 0;
    async_entries = {};
    ASSERT_EQ(poll_scan_until_done(f, &async_trunc, &async_count, &async_entries), 0);
    async_map = unpack_entries(async_entries, async_count);
    ct_free_buf(&async_entries);
    EXPECT_EQ(async_count, 5U);
    EXPECT_EQ(async_trunc, 1) << "20 keys after cursor, limit=5 should truncate";
    EXPECT_EQ(async_map.begin()->first, make_key(10));

    // Cursor past the end: empty result, not truncated.
    std::string past_end = make_key(29);
    f = ct_scan_async(t, nullptr, 0, reinterpret_cast<const uint8_t *>(past_end.data()), past_end.size(), 0);
    ASSERT_NE(f, nullptr);
    async_trunc   = 0;
    async_count   = 0;
    async_entries = {};
    ASSERT_EQ(poll_scan_until_done(f, &async_trunc, &async_count, &async_entries), 0);
    ct_free_buf(&async_entries);
    EXPECT_EQ(async_count, 0U);
    EXPECT_EQ(async_trunc, 0);

    ct_close(t);
}

// Equivalence: for a given start_after + limit, ct_scan_async's packed result
// matches ct_scan's synchronous result exactly (same keys, same truncation).
TEST(AsyncScan, StartAfterMatchesSyncScan)
{
    ct_options opt = {};
    ct_tree   *t   = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);
    for (int i = 0; i < 40; ++i) {
        ASSERT_EQ(put_flush(t, i + 1, make_key(i), "v" + std::to_string(i)), 0);
    }

    std::string cursor = make_key(19);
    // Sync scan with the same cursor + limit.
    ct_buf   sync_entries = {};
    uint64_t sync_count   = 0;
    int32_t  sync_trunc   = 0;
    ASSERT_EQ(ct_scan(t, nullptr, 0, reinterpret_cast<const uint8_t *>(cursor.data()), cursor.size(), 7, 0,
                      &sync_entries, &sync_count, &sync_trunc),
              0);
    auto sync_map = unpack_entries(sync_entries, sync_count);
    ct_free_buf(&sync_entries);

    // Async scan with the same cursor + limit.
    ct_future *f = ct_scan_async(t, nullptr, 0, reinterpret_cast<const uint8_t *>(cursor.data()), cursor.size(), 7);
    ASSERT_NE(f, nullptr);
    int32_t  async_trunc   = 0;
    uint64_t async_count   = 0;
    ct_buf   async_entries = {};
    ASSERT_EQ(poll_scan_until_done(f, &async_trunc, &async_count, &async_entries), 0);
    auto async_map = unpack_entries(async_entries, async_count);
    ct_free_buf(&async_entries);

    EXPECT_EQ(sync_count, async_count);
    EXPECT_EQ(sync_trunc, async_trunc);
    EXPECT_EQ(sync_trunc, 1) << "20 keys after cursor, limit=7 should truncate";
    EXPECT_EQ(sync_map, async_map);

    ct_close(t);
}
