// PT6b: buffer pool manager tests.
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "crowtree/buffer_pool.h"
#include "crowtree/cell.h"
#include "crowtree/frame_page.h"
#include "crowtree/page_store.h"

using namespace crowtree;

namespace {
constexpr uint32_t kPb = 256;
std::string Key(int i) {
  char b[16];
  snprintf(b, sizeof(b), "k%05d", i);
  return b;
}
// Build a one-entry leaf into a frame and mark it dirty.
void BuildLeaf(BufferPool* pool, FrameRef* ref, uint64_t pid, int k) {
  LeafFrameBuilder b(ref->bytes(), kPb);
  std::string cell = EncodeCell(static_cast<uint64_t>(k), OpKind::kPut, Slice("v"));
  ASSERT_TRUE(b.TryAppendSorted(Slice(Key(k)), Slice(cell)));
  b.Finish(pid, kInvalidPID);
  pool->MarkDirty(pid);
}
PageAddr Addr(int i) { return static_cast<PageAddr>(i) * kPb; }
}  // namespace

TEST(BufferPool, MissLoadsFromStoreAfterEviction) {
  MemPageStore store(1);
  BufferPool pool(2 * kPb, kPb, &store);  // 2 frames

  // Write 4 pages through the pool; with 2 frames, evictions write dirty pages
  // back to the store.
  for (int i = 0; i < 4; ++i) {
    FrameRef r;
    ASSERT_TRUE(pool.PinNew(i, Addr(i), &r).ok());
    BuildLeaf(&pool, &r, i, i);
    r.Release();
  }
  ASSERT_TRUE(pool.FlushDirty().ok());

  // Pin pid 0 again: it was evicted, so this is a miss that reloads from store.
  FrameRef r;
  ASSERT_TRUE(pool.Pin(0, Addr(0), &r).ok());
  LeafFrameView v(r.bytes(), kPb);
  ASSERT_EQ(v.count(), 1u);
  EXPECT_EQ(v.key(0).ToString(), Key(0));
  EXPECT_EQ(v.self_pid(), 0u);

  auto s = pool.stats();
  EXPECT_GT(s.misses, 0u);
  EXPECT_GT(s.evictions, 0u);
  EXPECT_GT(s.writebacks, 0u);
}

TEST(BufferPool, HitDoesNotMiss) {
  MemPageStore store(1);
  BufferPool pool(4 * kPb, kPb, &store);
  {
    FrameRef r;
    ASSERT_TRUE(pool.PinNew(5, Addr(5), &r).ok());
    BuildLeaf(&pool, &r, 5, 5);
  }  // released
  auto before = pool.stats();
  FrameRef r;
  ASSERT_TRUE(pool.Pin(5, Addr(5), &r).ok());  // still resident -> hit
  auto after = pool.stats();
  EXPECT_EQ(after.misses, before.misses);
  EXPECT_GT(after.hits, before.hits);
}

TEST(BufferPool, PinnedFramesAreNotEvicted) {
  MemPageStore store(1);
  BufferPool pool(2 * kPb, kPb, &store);  // 2 frames
  FrameRef a, b;
  ASSERT_TRUE(pool.PinNew(0, Addr(0), &a).ok());
  ASSERT_TRUE(pool.PinNew(1, Addr(1), &b).ok());
  // Both frames pinned; a third pin has no victim.
  FrameRef c;
  Status s = pool.PinNew(2, Addr(2), &c);
  EXPECT_FALSE(s.ok());
  EXPECT_FALSE(c.valid());
  // Release one; now it succeeds.
  a.Release();
  EXPECT_TRUE(pool.PinNew(2, Addr(2), &c).ok());
}

TEST(BufferPool, DirtyFlushWritesBack) {
  MemPageStore store(1);
  BufferPool pool(4 * kPb, kPb, &store);
  {
    FrameRef r;
    ASSERT_TRUE(pool.PinNew(9, Addr(9), &r).ok());
    BuildLeaf(&pool, &r, 9, 9);
  }
  ASSERT_TRUE(pool.FlushDirty().ok());
  std::vector<uint8_t> buf(kPb);
  ASSERT_TRUE(store.ReadAt(Addr(9), buf.data(), buf.size()).ok());
  ASSERT_TRUE(FrameValidate(buf.data(), kPb));
  EXPECT_EQ(LeafFrameView(buf.data(), kPb).key(0).ToString(), Key(9));
}

TEST(BufferPool, CorruptPageRejectedOnLoad) {
  MemPageStore store(1);
  BufferPool pool(2 * kPb, kPb, &store);
  for (int i = 0; i < 4; ++i) {
    FrameRef r;
    ASSERT_TRUE(pool.PinNew(i, Addr(i), &r).ok());
    BuildLeaf(&pool, &r, i, i);
    r.Release();
  }
  ASSERT_TRUE(pool.FlushDirty().ok());
  // Corrupt pid 0's durable image, then force a reload.
  std::vector<uint8_t> garbage(kPb, 0x5a);
  ASSERT_TRUE(store.WriteAt(Addr(0), garbage.data(), garbage.size()).ok());
  FrameRef r;
  EXPECT_EQ(pool.Pin(0, Addr(0), &r).code(), Code::kCorruption);
}

TEST(BufferPool, AcquireFrameIsAnonymousDirtyAndResident) {
  BufferPool pool(4 * kPb, kPb, nullptr);  // no store: anon frames only
  uint32_t idx = 0;
  uint8_t* bytes = nullptr;
  ASSERT_TRUE(pool.AcquireFrame(&idx, &bytes).ok());
  ASSERT_NE(bytes, nullptr);
  // Build a leaf directly into the acquired frame and read it back zero-copy.
  LeafFrameBuilder b(bytes, kPb);
  std::string cell = EncodeCell(7, OpKind::kPut, Slice("v"));
  ASSERT_TRUE(b.TryAppendSorted(Slice(Key(7)), Slice(cell)));
  b.Finish(99, kInvalidPID);
  EXPECT_EQ(LeafFrameView(bytes, kPb).key(0).ToString(), Key(7));

  auto s = pool.stats();
  EXPECT_EQ(s.used, 1u);  // one frame held
  EXPECT_EQ(s.dirty, 1u); // anonymous == not yet durable

  pool.ReleaseFrame(idx);
  EXPECT_EQ(pool.stats().used, 0u);
}

TEST(BufferPool, AcquireFrameFailsWhenFull) {
  BufferPool pool(2 * kPb, kPb, nullptr);  // 2 frames
  uint32_t i1 = 0, i2 = 0, i3 = 0;
  uint8_t* b = nullptr;
  ASSERT_TRUE(pool.AcquireFrame(&i1, &b).ok());
  ASSERT_TRUE(pool.AcquireFrame(&i2, &b).ok());
  // Both frames pinned-resident: a third acquire must fail (caller falls back
  // to a heap buffer).
  EXPECT_FALSE(pool.AcquireFrame(&i3, &b).ok());
  pool.ReleaseFrame(i1);
  EXPECT_TRUE(pool.AcquireFrame(&i3, &b).ok());  // freed slot reused
}

TEST(BufferPool, ManyPagesThroughSmallPool) {
  MemPageStore store(1);
  BufferPool pool(3 * kPb, kPb, &store);  // 3 frames, many pages
  const int N = 200;
  for (int i = 0; i < N; ++i) {
    FrameRef r;
    ASSERT_TRUE(pool.PinNew(i, Addr(i), &r).ok());
    BuildLeaf(&pool, &r, i, i);
    r.Release();
  }
  ASSERT_TRUE(pool.FlushDirty().ok());
  // Every page reloads correctly through the small pool (page table churn).
  for (int i = 0; i < N; ++i) {
    FrameRef r;
    ASSERT_TRUE(pool.Pin(i, Addr(i), &r).ok());
    EXPECT_EQ(LeafFrameView(r.bytes(), kPb).key(0).ToString(), Key(i));
  }
}
