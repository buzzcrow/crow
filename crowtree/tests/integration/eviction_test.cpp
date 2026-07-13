// PT6c-5.4: writer-driven eviction of clean resident bases (design §4.6). An
// evicted leaf re-tags its mapping slot `unloaded` and epoch-retires the page;
// the next access demand-loads it. Run under TSan for the eviction-vs-reader
// race (epoch-deferred frame reuse).
#include "crowtree/crowtree.h"
#include "crowtree/page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace crowtree;

namespace
{
Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

std::string make_key(int i)
{
    std::array<char, 16> buf{};
    snprintf(buf.data(), buf.size(), "key%05d", i);
    return buf.data();
}

void fill_buf(Crowtree *t, int K, std::map<std::string, std::string> *oracle)
{
    for (int i = 0; i < K; ++i) {
        std::string v = "val" + std::to_string(i);
        ASSERT_TRUE(t->apply(i + 1, put_one(make_key(i), v)).ok());
        ASSERT_TRUE(t->flush().ok());
        (*oracle)[make_key(i)] = v;
    }
}

// Counts read_at calls so a test can observe whether a specific page was
// demand-loaded (evicted, then reloaded) vs. still resident (plan-tree #17
// recency-ranked eviction).
class CountingPageStore : public MemPageStore
{
  public:
    explicit CountingPageStore(uint32_t iu_size = 1) : MemPageStore(iu_size)
    {
    }

    Status read_at(uint64_t off, uint8_t *buf, size_t len) const override
    {
        ++reads;
        return MemPageStore::read_at(off, buf, len);
    }

    mutable std::atomic<int> reads{0};
};
} // namespace

TEST(Eviction, EvictedLeavesFreeMemoryAndReloadCorrectly)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    std::map<std::string, std::string> oracle;
    fill_buf(&t, 200, &oracle);
    ASSERT_TRUE(t.snapshot(nullptr).ok()); // all reachable pages now clean

    uint32_t before  = t.buffer_pool()->stats().used;
    size_t   evicted = t.evict_clean_leaves(2); // keep at most 2 resident leaves
    EXPECT_GT(evicted, 0U);

    // No reader guards are open, so the epoch manager reclaims the retired pages
    // synchronously and their frames return to the pool: residency drops.
    uint32_t after = t.buffer_pool()->stats().used;
    EXPECT_LT(after, before);

    // Every value is still readable — evicted leaves demand-load on access.
    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t.get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
}

TEST(Eviction, EvictIsIdempotentAndSkipsDirty)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    std::map<std::string, std::string> oracle;
    fill_buf(&t, 120, &oracle);
    // No snapshot yet: every built leaf is dirty (no durable addr) -> nothing
    // is evictable.
    EXPECT_EQ(t.evict_clean_leaves(0), 0U);

    ASSERT_TRUE(t.snapshot(nullptr).ok()); // pages become clean
    size_t first = t.evict_clean_leaves(1);
    EXPECT_GT(first, 0U);
    // A second pass with everything already unloaded evicts nothing more.
    EXPECT_EQ(t.evict_clean_leaves(1), 0U);

    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t.get(Slice(kv.first), &s, &v));
        EXPECT_EQ(v, kv.second);
    }
}

TEST(Eviction, ConcurrentReadersWhileEvicting)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    const int                          K = 250;
    std::map<std::string, std::string> oracle;
    fill_buf(&t, K, &oracle);
    ASSERT_TRUE(t.snapshot(nullptr).ok());

    std::atomic<bool>        stop{false};
    std::atomic<bool>        fail{false};
    std::vector<std::thread> readers;
    readers.reserve(6);
    for (int r = 0; r < 6; ++r) {
        readers.emplace_back([&, r] {
            std::mt19937 rng(9000 + r);
            std::string  v;
            uint64_t     s;
            while (!stop.load(std::memory_order_relaxed)) {
                int i = static_cast<int>(rng() % K);
                if (!t.get(Slice(make_key(i)), &s, &v) || v != "val" + std::to_string(i)) {
                    fail.store(true);
                    return;
                }
            }
        });
    }

    // Churn: repeatedly evict almost everything while readers demand-load it back.
    for (int it = 0; it < 400; ++it) {
        (void)t.evict_clean_leaves(2);
        std::this_thread::yield();
    }
    stop.store(true);
    for (auto &th : readers) {
        th.join();
    }
    EXPECT_FALSE(fail.load());
}

// plan-tree #17: evict_clean_leaves ranks its candidates by real access
// recency (PageBase::last_touch_tick, stamped on every resident() touch)
// instead of arbitrary DFS order.
TEST(Eviction, RecentlyTouchedLeafSurvivesEvictionOverColderOnes)
{
    CountingPageStore store(1);
    Options           opt;
    opt.page_store       = &store;
    opt.max_delta_len    = 1;
    opt.leaf_split_bytes = 160;
    opt.frame_bytes      = 4096;
    Crowtree t(opt);

    std::map<std::string, std::string> oracle;
    fill_buf(&t, 200, &oracle);
    ASSERT_TRUE(t.snapshot(nullptr).ok()); // clean + resident, touch order == snapshot's DFS walk

    // Re-touch the very first key's leaf so it becomes the *most* recently
    // touched -- recency ranking should keep it resident longer than leaves
    // nothing has re-read since the snapshot walk.
    std::string v;
    uint64_t    s;
    ASSERT_TRUE(t.get(Slice(make_key(0)), &s, &v));

    int reads_before = store.reads.load();
    // Aggressive budget: keep only a single resident leaf.
    size_t evicted = t.evict_clean_leaves(1);
    EXPECT_GT(evicted, 0U);

    // The just-touched leaf must still be resident: no fresh demand-load.
    ASSERT_TRUE(t.get(Slice(make_key(0)), &s, &v));
    EXPECT_EQ(v, oracle[make_key(0)]);
    EXPECT_EQ(store.reads.load(), reads_before) << "recently-touched leaf should not have been evicted";

    // A leaf nothing re-touched should have been evicted and demand-loads on
    // next access.
    ASSERT_TRUE(t.get(Slice(make_key(150)), &s, &v));
    EXPECT_EQ(v, oracle[make_key(150)]);
    EXPECT_GT(store.reads.load(), reads_before) << "a colder leaf should have been evicted and reloaded";
}
