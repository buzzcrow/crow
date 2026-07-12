// Epoch-based reclamation.
//
// Readers take a Guard for the duration of a lock-free page walk. The writer
// retire()s pages it replaces; a retired page is freed only once no Guard that
// could still reference it remains open.
//
// v1 uses a mutex-protected implementation (correct and TSan-clean). The
// lock-free fast-path enter/exit from the design is a later optimization; see
// the plan's implementation log.
#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <vector>

namespace crowtree {

class EpochManager {
 public:
  using Deleter = std::function<void(void*)>;

  // RAII reader guard. Holds an epoch open until destroyed.
  class Guard {
   public:
    Guard() : mgr_(nullptr), epoch_(0) {}
    Guard(EpochManager* m, uint64_t e) : mgr_(m), epoch_(e) {}
    Guard(Guard&& o) noexcept : mgr_(o.mgr_), epoch_(o.epoch_) { o.mgr_ = nullptr; }
    Guard& operator=(Guard&& o) noexcept {
      if (this != &o)
      {
        release();
        mgr_ = o.mgr_;
        epoch_ = o.epoch_;
        o.mgr_ = nullptr;
      }
      return *this;
    }
    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;
    ~Guard() { release(); }

   private:
    void release();
    EpochManager* mgr_;
    uint64_t epoch_;
  };

  EpochManager() = default;
  ~EpochManager();

  EpochManager(const EpochManager&) = delete;
  EpochManager& operator=(const EpochManager&) = delete;

  // open a reader guard at the current epoch.
  Guard enter();

  // Defer deletion of `ptr` until no guard opened at-or-before now remains.
  void retire(void* ptr, Deleter deleter);

  // Convenience: retire a typed pointer with `delete`.
  template <class T>
  void retire_object(T* p) {
    retire(p, [](void* x) { delete static_cast<T*>(x); });
  }

  // Force a reclamation sweep. Returns the number of objects freed.
  size_t try_reclaim();

  // Diagnostics.
  size_t pending_retired();
  size_t active_guards();

 private:
  friend class Guard;
  void exit_epoch(uint64_t epoch);
  size_t reclaim_locked();  // caller holds mu_

  struct Retired {
    uint64_t epoch;
    void* ptr;
    Deleter deleter;
  };

  std::mutex mu_;
  uint64_t global_epoch_ = 1;
  std::map<uint64_t, uint32_t> active_;  // epoch -> open guard count
  std::vector<Retired> retired_;
};

}  // namespace crowtree
