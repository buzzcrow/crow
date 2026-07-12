#include "crowtree/epoch.h"

namespace crowtree {

void EpochManager::Guard::release() {
  if (mgr_ != nullptr)
  {
    mgr_->exit_epoch(epoch_);
    mgr_ = nullptr;
  }
}

EpochManager::~EpochManager() {
  // Free anything still pending; by destruction time no guards must remain.
  std::lock_guard<std::mutex> lk(mu_);
  for (auto& r : retired_)
  {
    r.deleter(r.ptr);
  }
  retired_.clear();
}

EpochManager::Guard EpochManager::enter() {
  std::lock_guard<std::mutex> lk(mu_);
  uint64_t e = global_epoch_;
  ++active_[e];
  return Guard(this, e);
}

void EpochManager::exit_epoch(uint64_t epoch) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = active_.find(epoch);
  if (it != active_.end())
  {
    if (--(it->second) == 0)
    {
      active_.erase(it);
    }
  }
  reclaim_locked();
}

void EpochManager::retire(void* ptr, Deleter deleter) {
  std::lock_guard<std::mutex> lk(mu_);
  // New retirements belong to the current epoch; bump so a fresh guard cannot
  // claim to predate this retirement.
  retired_.push_back(Retired{global_epoch_, ptr, std::move(deleter)});
  ++global_epoch_;
  reclaim_locked();
}

size_t EpochManager::reclaim_locked() {
  // The oldest epoch any open guard might still be reading.
  uint64_t min_active = active_.empty() ? global_epoch_ : active_.begin()->first;
  size_t freed = 0;
  std::vector<Retired> keep;
  keep.reserve(retired_.size());
  for (auto& r : retired_)
  {
    if (r.epoch < min_active)
    {
      r.deleter(r.ptr);
      ++freed;
    } else
    {
      keep.push_back(std::move(r));
    }
  }
  retired_.swap(keep);
  return freed;
}

size_t EpochManager::try_reclaim() {
  std::lock_guard<std::mutex> lk(mu_);
  return reclaim_locked();
}

size_t EpochManager::pending_retired() {
  std::lock_guard<std::mutex> lk(mu_);
  return retired_.size();
}

size_t EpochManager::active_guards() {
  std::lock_guard<std::mutex> lk(mu_);
  size_t n = 0;
  for (auto& kv : active_)
  {
    n += kv.second;
  }
  return n;
}

}  // namespace crowtree
