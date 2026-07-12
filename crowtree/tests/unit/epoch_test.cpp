// CT6: epoch-based reclamation tests.
#include "crowtree/epoch.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

using namespace crowtree;

namespace
{
struct Tracked
{
    std::atomic<int> *counter;

    ~Tracked()
    {
        counter->fetch_add(1, std::memory_order_relaxed);
    }
};
} // namespace

TEST(Epoch, RetireFreesWhenNoGuard)
{
    EpochManager     em;
    std::atomic<int> freed{0};
    auto            *t = new Tracked{&freed};
    em.retire_object(t);
    // No active guard: reclamation should free it.
    em.try_reclaim();
    EXPECT_EQ(freed.load(), 1);
    EXPECT_EQ(em.pending_retired(), 0U);
}

TEST(Epoch, GuardDelaysReclamation)
{
    EpochManager     em;
    std::atomic<int> freed{0};
    {
        EpochManager::Guard g = em.enter();
        auto               *t = new Tracked{&freed};
        em.retire_object(t);
        // The guard predates... actually the guard was opened before retire, so it
        // could still reference t -> must NOT be freed yet.
        em.try_reclaim();
        EXPECT_EQ(freed.load(), 0);
        EXPECT_EQ(em.pending_retired(), 1U);
    }
    // Guard dropped: reclamation is writer-driven (#12) — exit() only clears the
    // reader's slot, so the next try_reclaim() (or retire()) frees t.
    em.try_reclaim();
    EXPECT_EQ(freed.load(), 1);
    EXPECT_EQ(em.pending_retired(), 0U);
}

TEST(Epoch, NewGuardAfterRetireDoesNotBlock)
{
    EpochManager     em;
    std::atomic<int> freed{0};
    auto            *t = new Tracked{&freed};
    em.retire_object(t); // bumps epoch
    // A guard entered AFTER the retire cannot reference t, so reclaim frees it.
    {
        EpochManager::Guard g = em.enter();
        em.try_reclaim();
        EXPECT_EQ(freed.load(), 1);
    }
}

TEST(Epoch, MultipleGuardsHoldUntilAllExit)
{
    // Two *threads*: thread A opens g1 before the retire (so it must block
    // reclamation); the main thread opens g2 after the retire (so it must not).
    // (Per-thread EBR gives each thread one epoch, so two independent readers must
    // live on two threads — nested guards on one thread share a slot by design.)
    EpochManager      em;
    std::atomic<int>  freed{0};
    std::atomic<bool> g1_open{false};
    std::atomic<bool> release_g1{false};
    std::atomic<bool> g1_released{false};

    std::thread a([&] {
        {
            EpochManager::Guard g1 = em.enter(); // epoch 1
            g1_open.store(true);
            while (!release_g1.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
        } // g1 released here
        g1_released.store(true, std::memory_order_release);
    });

    while (!g1_open.load()) {
        std::this_thread::yield();
    }

    auto *t = new Tracked{&freed};
    em.retire_object(t); // epoch -> 2; g1 (epoch 1) must block

    EpochManager::Guard g2 = em.enter(); // main thread, epoch 2 (after retire)
    em.try_reclaim();
    EXPECT_EQ(freed.load(), 0); // g1 still open, blocks reclamation

    release_g1.store(true, std::memory_order_release);
    while (!g1_released.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    a.join();

    em.try_reclaim();
    EXPECT_EQ(freed.load(), 1); // g2 entered after retire, doesn't block
    (void)g2;
}

TEST(Epoch, DestructorFreesPending)
{
    std::atomic<int> freed{0};
    {
        EpochManager        em;
        EpochManager::Guard g = em.enter();
        em.retire_object(new Tracked{&freed});
        // Leave a guard "open" conceptually; em destructs and must free pending.
    }
    EXPECT_EQ(freed.load(), 1);
}

TEST(Epoch, ConcurrentReadersAndRetire)
{
    EpochManager             em;
    std::atomic<int>         freed{0};
    std::atomic<bool>        stop{false};
    std::vector<std::thread> readers;
    readers.reserve(4);
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                EpochManager::Guard g = em.enter();
                // simulate a brief read
            }
        });
    }
    for (int i = 0; i < 5000; ++i) {
        em.retire_object(new Tracked{&freed});
    }
    stop.store(true);
    for (auto &t : readers) {
        t.join();
    }
    em.try_reclaim();
    EXPECT_EQ(freed.load(), 5000);
    EXPECT_EQ(em.pending_retired(), 0U);
}

// The real safety property: readers dereference a shared object while the writer
// swaps it out and retires it. EBR must keep every retired node alive until all
// guards that could have loaded it exit — a UAF here trips ASan/TSan.
TEST(Epoch, ConcurrentReadersDerefRetiredNoUAF)
{
    EpochManager em;

    struct Node
    {
        int magic;
    };

    constexpr int         kMagic = 0x5A5A;
    std::atomic<Node *>   slot{new Node{kMagic}};
    std::atomic<bool>     stop{false};
    std::atomic<uint64_t> reads{0};

    std::vector<std::thread> readers;
    readers.reserve(4);
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                EpochManager::Guard g = em.enter();
                Node               *n = slot.load(std::memory_order_acquire);
                // Under the guard n cannot be freed; a stale/freed read would fail here.
                EXPECT_EQ(n->magic, kMagic);
                reads.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (int i = 0; i < 20000; ++i) {
        Node *fresh = new Node{kMagic};
        Node *old   = slot.exchange(fresh, std::memory_order_acq_rel);
        em.retire_object(old);
    }
    stop.store(true);
    for (auto &t : readers) {
        t.join();
    }
    em.try_reclaim();
    delete slot.load(); // final live node
    EXPECT_GT(reads.load(), 0U);
}
