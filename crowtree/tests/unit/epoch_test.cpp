// CT6: epoch-based reclamation tests.
#include "crowtree/epoch.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

using namespace crowtree;

namespace {
struct Tracked {
  std::atomic<int>* counter;
  ~Tracked() { counter->fetch_add(1, std::memory_order_relaxed); }
};
}  // namespace

TEST(Epoch, RetireFreesWhenNoGuard) {
  EpochManager em;
  std::atomic<int> freed{0};
  auto* t = new Tracked{&freed};
  em.retire_object(t);
  // No active guard: reclamation should free it.
  em.try_reclaim();
  EXPECT_EQ(freed.load(), 1);
  EXPECT_EQ(em.pending_retired(), 0u);
}

TEST(Epoch, GuardDelaysReclamation) {
  EpochManager em;
  std::atomic<int> freed{0};
  {
    EpochManager::Guard g = em.enter();
    auto* t = new Tracked{&freed};
    em.retire_object(t);
    // The guard predates... actually the guard was opened before retire, so it
    // could still reference t -> must NOT be freed yet.
    em.try_reclaim();
    EXPECT_EQ(freed.load(), 0);
    EXPECT_EQ(em.pending_retired(), 1u);
  }
  // Guard dropped: reclamation on exit frees t.
  EXPECT_EQ(freed.load(), 1);
  EXPECT_EQ(em.pending_retired(), 0u);
}

TEST(Epoch, NewGuardAfterRetireDoesNotBlock) {
  EpochManager em;
  std::atomic<int> freed{0};
  auto* t = new Tracked{&freed};
  em.retire_object(t);  // bumps epoch
  // A guard entered AFTER the retire cannot reference t, so reclaim frees it.
  {
    EpochManager::Guard g = em.enter();
    em.try_reclaim();
    EXPECT_EQ(freed.load(), 1);
  }
}

TEST(Epoch, MultipleGuardsHoldUntilAllExit) {
  EpochManager em;
  std::atomic<int> freed{0};
  EpochManager::Guard g1 = em.enter();
  auto* t = new Tracked{&freed};
  em.retire_object(t);
  EpochManager::Guard g2 = em.enter();
  em.try_reclaim();
  EXPECT_EQ(freed.load(), 0);  // g1 still open
  {
    EpochManager::Guard moved = std::move(g1);
  }  // drop g1
  em.try_reclaim();
  EXPECT_EQ(freed.load(), 1);  // g2 entered after retire, doesn't block
  (void)g2;
}

TEST(Epoch, DestructorFreesPending) {
  std::atomic<int> freed{0};
  {
    EpochManager em;
    EpochManager::Guard g = em.enter();
    em.retire_object(new Tracked{&freed});
    // Leave a guard "open" conceptually; em destructs and must free pending.
  }
  EXPECT_EQ(freed.load(), 1);
}

TEST(Epoch, ConcurrentReadersAndRetire) {
  EpochManager em;
  std::atomic<int> freed{0};
  std::atomic<bool> stop{false};
  std::vector<std::thread> readers;
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
  for (auto& t : readers) {
    t.join();
  }
  em.try_reclaim();
  EXPECT_EQ(freed.load(), 5000);
  EXPECT_EQ(em.pending_retired(), 0u);
}
