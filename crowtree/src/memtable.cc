#include "crowtree/memtable.h"

namespace crowtree {

bool MemTable::Upsert(Slice key, uint64_t slot, Slice cell_payload) {
  std::lock_guard<std::mutex> lk(mu_);
  if (slot <= durable_floor_) return false;  // already durable in L1
  std::string_view kv = key.ToView();
  auto it = map_.find(kv);
  if (it != map_.end()) {
    uint64_t existing = CellView{Slice(it->second)}.slot();
    if (slot <= existing) return false;  // highest-slot-wins: keep existing
    bytes_ -= it->first.size() + it->second.size();
    it->second.assign(cell_payload.data(), cell_payload.size());
    bytes_ += it->first.size() + it->second.size();
    return true;
  }
  std::string k(key.data(), key.size());
  std::string c(cell_payload.data(), cell_payload.size());
  bytes_ += k.size() + c.size();
  map_.emplace(std::move(k), std::move(c));
  return true;
}

void MemTable::SetDurableFloor(uint64_t slot) {
  std::lock_guard<std::mutex> lk(mu_);
  if (slot > durable_floor_) durable_floor_ = slot;
}

uint64_t MemTable::durable_floor() const {
  std::lock_guard<std::mutex> lk(mu_);
  return durable_floor_;
}

bool MemTable::Get(Slice key, std::string* out_cell) const {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = map_.find(key.ToView());
  if (it == map_.end()) return false;
  out_cell->assign(it->second);
  return true;
}

std::vector<MemEntry> MemTable::DrainUpTo(uint64_t cs) {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<MemEntry> out;
  for (auto it = map_.begin(); it != map_.end();) {
    uint64_t slot = CellView{Slice(it->second)}.slot();
    if (slot <= cs) {
      out.push_back(MemEntry{it->first, it->second, slot});
      bytes_ -= it->first.size() + it->second.size();
      it = map_.erase(it);
    } else {
      ++it;
    }
  }
  return out;
}

std::vector<MemEntry> MemTable::Snapshot() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<MemEntry> out;
  out.reserve(map_.size());
  for (auto& kv : map_) {
    out.push_back(MemEntry{kv.first, kv.second, CellView{Slice(kv.second)}.slot()});
  }
  return out;
}

size_t MemTable::ApproxBytes() const {
  std::lock_guard<std::mutex> lk(mu_);
  return bytes_;
}

size_t MemTable::Count() const {
  std::lock_guard<std::mutex> lk(mu_);
  return map_.size();
}

bool MemTable::Empty() const {
  std::lock_guard<std::mutex> lk(mu_);
  return map_.empty();
}

}  // namespace crowtree
