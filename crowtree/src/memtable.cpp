#include "crowtree/memtable.h"

#include <cstring>
#include <utility>

namespace crowtree {

namespace {
// Copy a byte range into a fresh owned buffer (SBO-inline for small ranges).
buffer buf_of(Slice s) {
  buffer b = buffer::alloc(s.size());
  if (s.size() > 0)
  {
    std::memcpy(b.data(), s.data(), s.size());
  }
  return b;
}
}  // namespace

bool MemTable::upsert(Slice key, uint64_t slot, Slice cell_payload) {
  return upsert(key, slot, buf_of(cell_payload));
}

bool MemTable::upsert(Slice key, uint64_t slot, buffer&& cell_payload) {
  std::lock_guard<std::mutex> lk(mu_);
  // Already durable in L1; reject unless restore explicitly allows old slots.
  if (slot <= durable_floor_ && !allow_old_slots_)
  {
    return false;
  }
  auto it = map_.find(key.to_view());  // heterogeneous lookup, no temp key
  if (it != map_.end())
  {
    uint64_t existing = CellView{it->second.slice()}.slot();
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
  std::string k(key.data(), key.size());
  bytes_ += k.size() + cell_payload.size();
  if (slot < min_slot_)
  {
    min_slot_ = slot;
  }
  if (slot > max_slot_)
  {
    max_slot_ = slot;
  }
  // string key (copyable, relocatable) + move-only buffer value: try_emplace
  // constructs both in place without materializing a movable pair.
  map_.try_emplace(std::move(k), std::move(cell_payload));
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
  auto it = map_.find(key);  // heterogeneous lookup by Slice (buffer_less)
  if (it == map_.end())
  {
    return false;
  }
  out_cell->assign(reinterpret_cast<const char*>(it->second.data()), it->second.size());
  return true;
}

std::vector<mem_entry> MemTable::drain_up_to(uint64_t cs) {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<mem_entry> out;
  for (auto it = map_.begin(); it != map_.end();)
  {
    uint64_t slot = CellView{it->second.slice()}.slot();
    if (slot <= cs)
    {
      // Copy the (small, SSO) key; move the cell buffer out before erase.
      bytes_ -= it->first.size() + it->second.size();
      out.push_back(mem_entry{it->first, std::move(it->second), slot});
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
    out.push_back(mem_entry{kv.first, kv.second.clone(), CellView{kv.second.slice()}.slot()});
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
