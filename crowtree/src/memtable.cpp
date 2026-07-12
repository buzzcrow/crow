#include "crowtree/memtable.h"

namespace crowtree {

bool MemTable::upsert(Slice key, uint64_t slot, Slice cell_payload) {
  std::lock_guard<std::mutex> lk(mu_);
  // Already durable in L1; reject unless restore explicitly allows old slots.
  if (slot <= durable_floor_ && !allow_old_slots_)
  {
    return false;
  }
  std::string_view kv = key.to_view();
  auto it = map_.find(kv);
  if (it != map_.end())
  {
    uint64_t existing = CellView{Slice(it->second)}.slot();
    if (slot <= existing)
    {
      return false;  // highest-slot-wins: keep existing
    }
    bytes_ -= it->first.size() + it->second.size();
    it->second.assign(cell_payload.data(), cell_payload.size());
    bytes_ += it->first.size() + it->second.size();
    if (slot < min_slot_)
    {
      min_slot_ = slot;
    }
    if (slot > max_slot_)
    {
      max_slot_ = slot;
    }
    return true;
  }
  std::string k(key.data(), key.size());
  std::string c(cell_payload.data(), cell_payload.size());
  bytes_ += k.size() + c.size();
  map_.emplace(std::move(k), std::move(c));
  if (slot < min_slot_)
  {
    min_slot_ = slot;
  }
  if (slot > max_slot_)
  {
    max_slot_ = slot;
  }
  return true;
}

bool MemTable::upsert(std::string&& key, uint64_t slot, std::string&& cell_payload) {
  std::lock_guard<std::mutex> lk(mu_);
  if (slot <= durable_floor_ && !allow_old_slots_)
  {
    return false;
  }
  std::string_view kv(key);
  auto it = map_.find(kv);
  if (it != map_.end())
  {
    uint64_t existing = CellView{Slice(it->second)}.slot();
    if (slot <= existing)
    {
      return false;  // highest-slot-wins: keep existing
    }
    bytes_ -= it->first.size() + it->second.size();
    it->second = std::move(cell_payload);
    bytes_ += it->first.size() + it->second.size();
    if (slot < min_slot_)
    {
      min_slot_ = slot;
    }
    if (slot > max_slot_)
    {
      max_slot_ = slot;
    }
    return true;
  }
  bytes_ += key.size() + cell_payload.size();
  if (slot < min_slot_)
  {
    min_slot_ = slot;
  }
  if (slot > max_slot_)
  {
    max_slot_ = slot;
  }
  map_.emplace(std::move(key), std::move(cell_payload));
  return true;
}

void MemTable::set_allow_old_slots(bool v) {
  std::lock_guard<std::mutex> lk(mu_);
  allow_old_slots_ = v;
}

MemTable::slot_range_t MemTable::slot_range() const {
  std::lock_guard<std::mutex> lk(mu_);
  if (map_.empty())
  {
    return slot_range_t{};
  }
  return slot_range_t{min_slot_, max_slot_, false};
}

void MemTable::set_durable_floor(uint64_t slot) {
  std::lock_guard<std::mutex> lk(mu_);
  if (slot > durable_floor_)
  {
    durable_floor_ = slot;
  }
}

uint64_t MemTable::durable_floor() const {
  std::lock_guard<std::mutex> lk(mu_);
  return durable_floor_;
}

void MemTable::reset() {
  std::lock_guard<std::mutex> lk(mu_);
  map_.clear();
  bytes_ = 0;
  durable_floor_ = 0;
  allow_old_slots_ = false;
  min_slot_ = UINT64_MAX;
  max_slot_ = 0;
}

bool MemTable::get(Slice key, std::string* out_cell) const {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = map_.find(key.to_view());
  if (it == map_.end())
  {
    return false;
  }
  out_cell->assign(it->second);
  return true;
}

std::vector<mem_entry> MemTable::drain_up_to(uint64_t cs) {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<mem_entry> out;
  for (auto it = map_.begin(); it != map_.end();)
  {
    uint64_t slot = CellView{Slice(it->second)}.slot();
    if (slot <= cs)
    {
      out.push_back(mem_entry{it->first, it->second, slot});
      bytes_ -= it->first.size() + it->second.size();
      it = map_.erase(it);
    } else
    {
      ++it;
    }
  }
  if (map_.empty())
  {  // no entries left: clear the slot-range hint
    min_slot_ = UINT64_MAX;
    max_slot_ = 0;
  }
  return out;
}

std::vector<mem_entry> MemTable::snapshot() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<mem_entry> out;
  out.reserve(map_.size());
  for (auto& kv : map_)
  {
    out.push_back(mem_entry{kv.first, kv.second, CellView{Slice(kv.second)}.slot()});
  }
  return out;
}

size_t MemTable::approx_bytes() const {
  std::lock_guard<std::mutex> lk(mu_);
  return bytes_;
}

size_t MemTable::count() const {
  std::lock_guard<std::mutex> lk(mu_);
  return map_.size();
}

bool MemTable::empty() const {
  std::lock_guard<std::mutex> lk(mu_);
  return map_.empty();
}

}  // namespace crowtree
