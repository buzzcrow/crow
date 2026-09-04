// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// PT6b: buffer pool manager tests.
#include "crowdb-tree/buffer_pool.h"
#include "crowdb-tree/cell.h"
#include "crowdb-tree/frame_page.h"
#include "crowdb-tree/page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <condition_variable>
#include <cstdio>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace crowdb::tree;

namespace
{
constexpr uint32_t kPb = 256;

std::string key(int i)
{
    std::array<char, 16> b{};
    snprintf(b.data(), b.size(), "k%05d", i);
    return b.data();
}

// build a one-entry leaf into a frame and mark it dirty.
void build_leaf(BufferPool *pool, FrameRef *ref, uint64_t page_id, int k)
{
    LeafFrameBuilder b(ref->bytes(), kPb);
    std::string      cell = encode_cell(static_cast<uint64_t>(k), OpKind::kPut, Slice("v"));
    ASSERT_TRUE(b.try_append_sorted(Slice(key(k)), Slice(cell)));
    b.finish(page_id, kInvalidPageId);
    pool->mark_dirty(page_id);
}

PageAddr addr(int i)
{
    return static_cast<PageAddr>(i) * kPb;
}

class BlockingReadStore final : public PageStore
{
  public:
    Status write_at(uint64_t off, const uint8_t *buf, size_t len) override
    {
        return inner_.write_at(off, buf, len);
    }

    Status read_at(uint64_t off, uint8_t *buf, size_t len) const override
    {
        if (off == blocked_addr_) {
            std::unique_lock lk(mu_);
            read_started_ = true;
            cv_.notify_all();
            cv_.wait(lk, [this] { return allow_read_; });
        }
        return inner_.read_at(off, buf, len);
    }

    Status sync() override
    {
        return Status::Ok();
    }

    [[nodiscard]] uint64_t size() const override
    {
        return inner_.size();
    }

    [[nodiscard]] uint32_t iu_size() const override
    {
        return 1;
    }

    void block(PageAddr target)
    {
        blocked_addr_ = target;
    }

    void wait_until_blocked() const
    {
        std::unique_lock lk(mu_);
        cv_.wait(lk, [this] { return read_started_; });
    }

    void release_read()
    {
        std::scoped_lock lk(mu_);
        allow_read_ = true;
        cv_.notify_all();
    }

  private:
    mutable MemPageStore            inner_{1};
    mutable std::mutex              mu_;
    mutable std::condition_variable cv_;
    PageAddr                        blocked_addr_ = kNoAddr;
    mutable bool                    read_started_ = false;
    bool                            allow_read_   = false;
};
} // namespace

TEST(BufferPool, MissLoadsFromStoreAfterEviction)
{
    MemPageStore store(1);
    BufferPool   pool(static_cast<size_t>(2) * kPb, kPb, &store); // 2 frames

    // Write 4 pages through the pool; with 2 frames, evictions write dirty pages
    // back to the store.
    for (int i = 0; i < 4; ++i) {
        FrameRef r;
        ASSERT_TRUE(pool.pin_new(i, addr(i), &r).ok());
        build_leaf(&pool, &r, i, i);
        r.release();
    }
    ASSERT_TRUE(pool.flush_dirty().ok());

    // pin page_id 0 again: it was evicted, so this is a miss that reloads from store.
    FrameRef r;
    ASSERT_TRUE(pool.pin(0, addr(0), &r).ok());
    LeafFrameView v(r.bytes(), kPb);
    ASSERT_EQ(v.count(), 1U);
    EXPECT_EQ(v.key(0).to_string(), key(0));
    EXPECT_EQ(v.self_page_id(), 0U);

    auto s = pool.stats();
    EXPECT_GT(s.misses, 0U);
    EXPECT_GT(s.evictions, 0U);
    EXPECT_GT(s.writebacks, 0U);
}

TEST(BufferPool, HitDoesNotMiss)
{
    MemPageStore store(1);
    BufferPool   pool(static_cast<size_t>(4) * kPb, kPb, &store);
    {
        FrameRef r;
        ASSERT_TRUE(pool.pin_new(5, addr(5), &r).ok());
        build_leaf(&pool, &r, 5, 5);
    } // released
    auto     before = pool.stats();
    FrameRef r;
    ASSERT_TRUE(pool.pin(5, addr(5), &r).ok()); // still resident -> hit
    auto after = pool.stats();
    EXPECT_EQ(after.misses, before.misses);
    EXPECT_GT(after.hits, before.hits);
}

TEST(BufferPool, UnrelatedHitContinuesWhileMissReadIsBlocked)
{
    BlockingReadStore store;
    BufferPool        pool(static_cast<size_t>(2) * kPb, kPb, &store);
    {
        FrameRef resident;
        ASSERT_TRUE(pool.pin_new(1, addr(1), &resident).ok());
        build_leaf(&pool, &resident, 1, 1);
    }
    ASSERT_TRUE(pool.flush_dirty().ok());
    std::vector<uint8_t> durable(kPb);
    LeafFrameBuilder     builder(durable.data(), kPb);
    std::string          cell = encode_cell(2, OpKind::kPut, Slice("v"));
    ASSERT_TRUE(builder.try_append_sorted(Slice(key(2)), Slice(cell)));
    builder.finish(2, kInvalidPageId);
    ASSERT_TRUE(store.write_at(addr(2), durable.data(), durable.size()).ok());

    store.block(addr(2));
    auto miss = std::async(std::launch::async, [&] {
        FrameRef loaded;
        return pool.pin(2, addr(2), &loaded);
    });
    store.wait_until_blocked();

    auto hit = std::async(std::launch::async, [&] {
        FrameRef resident;
        return pool.pin(1, addr(1), &resident);
    });
    EXPECT_EQ(hit.wait_for(std::chrono::milliseconds(100)), std::future_status::ready);
    EXPECT_TRUE(hit.get().ok());

    store.release_read();
    EXPECT_TRUE(miss.get().ok());
}

TEST(BufferPool, PinnedFramesAreNotEvicted)
{
    MemPageStore store(1);
    BufferPool   pool(static_cast<size_t>(2) * kPb, kPb, &store); // 2 frames
    FrameRef     a;
    ASSERT_TRUE(pool.pin_new(0, addr(0), &a).ok());
    FrameRef b;
    ASSERT_TRUE(pool.pin_new(1, addr(1), &b).ok());
    FrameRef c;
    Status   s = pool.pin_new(2, addr(2), &c);
    EXPECT_FALSE(s.ok());
    EXPECT_FALSE(c.valid());
    // release one; now it succeeds.
    a.release();
    EXPECT_TRUE(pool.pin_new(2, addr(2), &c).ok());
}

TEST(BufferPool, DirtyFlushWritesBack)
{
    MemPageStore store(1);
    BufferPool   pool(static_cast<size_t>(4) * kPb, kPb, &store);
    {
        FrameRef r;
        ASSERT_TRUE(pool.pin_new(9, addr(9), &r).ok());
        build_leaf(&pool, &r, 9, 9);
    }
    ASSERT_TRUE(pool.flush_dirty().ok());
    std::vector<uint8_t> buf(kPb);
    ASSERT_TRUE(store.read_at(addr(9), buf.data(), buf.size()).ok());
    ASSERT_TRUE(frame_validate(buf.data(), kPb));
    EXPECT_EQ(LeafFrameView(buf.data(), kPb).key(0).to_string(), key(9));
}

TEST(BufferPool, CorruptPageRejectedOnLoad)
{
    MemPageStore store(1);
    BufferPool   pool(static_cast<size_t>(2) * kPb, kPb, &store);
    for (int i = 0; i < 4; ++i) {
        FrameRef r;
        ASSERT_TRUE(pool.pin_new(i, addr(i), &r).ok());
        build_leaf(&pool, &r, i, i);
        r.release();
    }
    ASSERT_TRUE(pool.flush_dirty().ok());
    // Corrupt page_id 0's durable image, then force a reload.
    std::vector<uint8_t> garbage(kPb, 0x5a);
    ASSERT_TRUE(store.write_at(addr(0), garbage.data(), garbage.size()).ok());
    FrameRef r;
    EXPECT_EQ(pool.pin(0, addr(0), &r).code(), Code::kCorruption);
}

TEST(BufferPool, AcquireFrameIsAnonymousDirtyAndResident)
{
    BufferPool pool(static_cast<size_t>(4) * kPb, kPb, nullptr); // no store: anon frames only
    uint32_t   idx   = 0;
    uint8_t   *bytes = nullptr;
    ASSERT_TRUE(pool.acquire_frame(&idx, &bytes).ok());
    ASSERT_NE(bytes, nullptr);
    // build a leaf directly into the acquired frame and read it back zero-copy.
    LeafFrameBuilder b(bytes, kPb);
    std::string      cell = encode_cell(7, OpKind::kPut, Slice("v"));
    ASSERT_TRUE(b.try_append_sorted(Slice(key(7)), Slice(cell)));
    b.finish(99, kInvalidPageId);
    EXPECT_EQ(LeafFrameView(bytes, kPb).key(0).to_string(), key(7));

    auto s = pool.stats();
    EXPECT_EQ(s.used, 1U);  // one frame held
    EXPECT_EQ(s.dirty, 1U); // anonymous == not yet durable

    pool.release_frame(idx);
    EXPECT_EQ(pool.stats().used, 0U);
}

TEST(BufferPool, AcquireFrameFailsWhenFull)
{
    BufferPool pool(static_cast<size_t>(2) * kPb, kPb, nullptr); // 2 frames
    uint32_t   i1 = 0;
    uint32_t   i2 = 0;
    uint32_t   i3 = 0;
    uint8_t   *b  = nullptr;
    ASSERT_TRUE(pool.acquire_frame(&i1, &b).ok());
    ASSERT_TRUE(pool.acquire_frame(&i2, &b).ok());
    // Both frames pinned-resident: a third acquire must fail (caller falls back
    // to a heap buffer).
    EXPECT_FALSE(pool.acquire_frame(&i3, &b).ok());
    pool.release_frame(i1);
    EXPECT_TRUE(pool.acquire_frame(&i3, &b).ok()); // freed slot reused
}

TEST(BufferPool, ManyPagesThroughSmallPool)
{
    MemPageStore store(1);
    BufferPool   pool(static_cast<size_t>(3) * kPb, kPb, &store); // 3 frames, many pages
    const int    N = 200;
    for (int i = 0; i < N; ++i) {
        FrameRef r;
        ASSERT_TRUE(pool.pin_new(i, addr(i), &r).ok());
        build_leaf(&pool, &r, i, i);
        r.release();
    }
    ASSERT_TRUE(pool.flush_dirty().ok());
    // Every page reloads correctly through the small pool (page table churn).
    for (int i = 0; i < N; ++i) {
        FrameRef r;
        ASSERT_TRUE(pool.pin(i, addr(i), &r).ok());
        EXPECT_EQ(LeafFrameView(r.bytes(), kPb).key(0).to_string(), key(i));
    }
}
