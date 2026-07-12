#include "crowtree/epoch.h"

namespace crowtree {

void EpochManager::Guard::Release() {
  if (mgr_ != nullptr) {
    mgr_->ExitEpoch(epoch_);
    mgr_ = nullptr;
  }
}

EpochManager::~EpochManager() {
  // Free anything still pending; by destruction time no guards must remain.
  std::lock_guard<std::mutex> lk(mu_);
  for (auto& r : retired_) r.deleter(r.ptr);
  retired_.clear();
}

EpochManager::Guard EpochManager::Enter() {
  std::lock_guard<std::mutex> lk(mu_);
  uint64_t e = global_epoch_;
  ++active_[e];
  return Guard(this, e);
}

void EpochManager::ExitEpoch(uint64_t epoch) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = active_.find(epoch);
  if (it != active_.end()) {
    if (--(it->second) == 0) active_.erase(it);
  }
  ReclaimLocked();
}

void EpochManager::Retire(void* ptr, Deleter deleter) {
  std::lock_guard<std::mutex> lk(mu_);
  // New retirements belong to the current epoch; bump so a fresh guard cannot
  // claim to predate this retirement.
  retired_.push_back(Retired{global_epoch_, ptr, std::move(deleter)});
  ++global_epoch_;
  ReclaimLocked();
}

size_t EpochManager::ReclaimLocked() {
  // The oldest epoch any open guard might still be reading.
  uint64_t min_active = active_.empty() ? global_epoch_ : active_.begin()->first;
  size_t freed = 0;
  std::vector<Retired> keep;
  keep.reserve(retired_.size());
  for (auto& r : retired_) {
    if (r.epoch < min_active) {
      r.deleter(r.ptr);
      ++freed;
    } else {
      keep.push_back(std::move(r));
    }
  }
  retired_.swap(keep);
  return freed;
}

size_t EpochManager::TryReclaim() {
  std::lock_guard<std::mutex> lk(mu_);
  return ReclaimLocked();
}

size_t EpochManager::PendingRetired() {
  std::lock_guard<std::mutex> lk(mu_);
  return retired_.size();
}

size_t EpochManager::ActiveGuards() {
  std::lock_guard<std::mutex> lk(mu_);
  size_t n = 0;
  for (auto& kv : active_) n += kv.second;
  return n;
}

}  // namespace crowtree
