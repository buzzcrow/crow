// Copyright 2026-present buzzcrow <buzzcrow@126.com>

#include "crow-tree/memtable.h"

#include <algorithm>
#include <cstring>
#include <utility>

namespace crow::tree
{

namespace
{
// Copy a byte range into a fresh owned buffer (SBO-inline for small ranges).
buffer buf_of(Slice s)
{
    buffer b = buffer::alloc(s.size());
    if (!s.empty()) {
        std::memcpy(b.data(), s.data(), s.size());
    }
    return b;
}
} // namespace

bool MemTable::upsert(Slice key, uint64_t slot, Slice cell_payload)
{
    return upsert(key, slot, buf_of(cell_payload));
}

bool MemTable::upsert(Slice key, uint64_t slot, buffer &&cell_payload)
{
    std::lock_guard<std::mutex> lk(mu_);
    // Already durable in L1; reject unless restore explicitly allows old slots.
    if (slot <= durable_floor_ && !allow_old_slots_) {
        return false;
    }
    // Decode slot/flags from the contiguous cell so cell_entry has the fields
    // populated for fast highest-slot-wins checks (no CellView re-parse later).
    CellView   cv{cell_payload.slice()};
    cell_entry entry;
    entry.slot  = cv.valid() ? cv.slot() : slot;
    entry.flags = cv.valid() ? cv.flags() : 0;
    entry.cell  = std::move(cell_payload);

    auto it = map_.find(key.to_view()); // heterogeneous lookup, no temp key
    if (it != map_.end()) {
        if (slot <= it->second.slot) {
            return false; // highest-slot-wins: keep existing
        }
        bytes_ -= it->first.size() + it->second.cell.size();
        it->second = std::move(entry); // old entry freed (drop_fn fires if split)
        bytes_ += it->first.size() + it->second.cell.size();
        min_slot_ = std::min(min_slot_, slot);
        max_slot_ = std::max(max_slot_, slot);
        return true;
    }
    std::string k(key.data(), key.size());
    bytes_ += k.size() + entry.cell.size();
    min_slot_ = std::min(min_slot_, slot);
    max_slot_ = std::max(max_slot_, slot);
    // string key (copyable, relocatable) + move-only cell_entry value:
    // try_emplace constructs both in place without materializing a movable pair.
    map_.try_emplace(std::move(k), std::move(entry));
    return true;
}

bool MemTable::upsert_external(Slice key, uint64_t slot, uint8_t flags, buffer &&value)
{
    std::lock_guard<std::mutex> lk(mu_);
    if (slot <= durable_floor_ && !allow_old_slots_) {
        return false;
    }
    cell_entry entry;
    entry.slot  = slot;
    entry.flags = flags;
    // For Delete (tombstone), the caller may pass an empty kOwned buffer
    // (buffer::alloc(0)). Tag it as kExternal so materialize() knows to build
    // the 9-byte header from slot/flags (a kOwned size-0 buffer would be
    // mistaken for a contiguous cell and cloned as-empty). No drop_fn needed.
    if ((flags & kFlagTombstone) != 0 && value.ownership() != buffer::mode::kExternal) {
        value = buffer::wrap_external(nullptr, 0, nullptr, nullptr);
    }
    entry.cell = std::move(value); // kExternal (borrowed) for Put; empty kExternal for Delete

    auto it = map_.find(key.to_view());
    if (it != map_.end()) {
        if (slot <= it->second.slot) {
            return false; // highest-slot-wins: keep existing (incoming value freed)
        }
        bytes_ -= it->first.size() + it->second.cell.size();
        it->second = std::move(entry); // old entry freed (drop_fn fires if split)
        bytes_ += it->first.size() + it->second.cell.size();
        min_slot_ = std::min(min_slot_, slot);
        max_slot_ = std::max(max_slot_, slot);
        return true;
    }
    std::string k(key.data(), key.size());
    bytes_ += k.size() + entry.cell.size();
    min_slot_ = std::min(min_slot_, slot);
    max_slot_ = std::max(max_slot_, slot);
    map_.try_emplace(std::move(k), std::move(entry));
    return true;
}

void MemTable::set_allow_old_slots(bool v)
{
    std::lock_guard<std::mutex> lk(mu_);
    allow_old_slots_ = v;
}

MemTable::slot_range_t MemTable::slot_range() const
{
    std::lock_guard<std::mutex> lk(mu_);
    if (map_.empty()) {
        return slot_range_t{};
    }
    return {.min = min_slot_, .max = max_slot_, .empty = false};
}

void MemTable::set_durable_floor(uint64_t slot)
{
    std::lock_guard<std::mutex> lk(mu_);
    durable_floor_ = std::max(durable_floor_, slot);
}

uint64_t MemTable::durable_floor() const
{
    std::lock_guard<std::mutex> lk(mu_);
    return durable_floor_;
}

void MemTable::reset()
{
    std::lock_guard<std::mutex> lk(mu_);
    map_.clear(); // drop_fn fires for every split entry (releases Rust refs)
    bytes_           = 0;
    durable_floor_   = 0;
    allow_old_slots_ = false;
    min_slot_        = UINT64_MAX;
    max_slot_        = 0;
}

bool MemTable::get(Slice key, std::string *out_cell) const
{
    std::lock_guard<std::mutex> lk(mu_);
    auto                        it = map_.find(key); // heterogeneous lookup by Slice
    if (it == map_.end()) {
        return false;
    }
    const cell_entry &e = it->second;
    if (e.cell.ownership() != buffer::mode::kExternal) {
        // contiguous: copy the full [header][value] cell (same as pre-R30).
        out_cell->assign(reinterpret_cast<const char *>(e.cell.data()), e.cell.size());
        return true;
    }
    // split: write [header][value] directly into the output string (one copy,
    // same as the contiguous path's assign + 9 bytes of header).
    size_t vlen = e.cell.size();
    out_cell->resize(kCellHeaderSize + vlen);
    auto *p = reinterpret_cast<uint8_t *>(out_cell->data());
    for (int i = 0; i < 8; ++i) {
        p[i] = static_cast<uint8_t>((e.slot >> (8 * i)) & 0xff);
    }
    p[8] = e.flags;
    if (vlen > 0) {
        std::memcpy(p + kCellHeaderSize, e.cell.data(), vlen);
    }
    return true;
}

std::vector<mem_entry> MemTable::drain_up_to(uint64_t cs)
{
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<mem_entry>      out;
    for (auto it = map_.begin(); it != map_.end();) {
        if (it->second.slot <= cs) {
            // Copy the (small, SSO) key; materialize+move the cell buffer out
            // before erase. Split cells copy the value here (off the apply hot
            // path — this runs on the Flusher thread); contiguous cells move.
            bytes_ -= it->first.size() + it->second.cell.size();
            out.push_back({.key = it->first, .cell = it->second.materialize_move(), .slot = it->second.slot});
            it = map_.erase(it); // drop_fn fires for split entries (Rust ref released)
        }
        else {
            ++it;
        }
    }
    if (map_.empty()) { // no entries left: clear the slot-range hint
        min_slot_ = UINT64_MAX;
        max_slot_ = 0;
    }
    return out;
}

std::vector<mem_entry> MemTable::snapshot() const
{
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<mem_entry>      out;
    out.reserve(map_.size());
    for (const auto &kv : map_) {
        out.push_back({.key = kv.first, .cell = kv.second.materialize(), .slot = kv.second.slot});
    }
    return out;
} // NOLINT(clang-analyzer-unix.Malloc)

size_t MemTable::approx_bytes() const
{
    std::lock_guard<std::mutex> lk(mu_);
    return bytes_;
}

size_t MemTable::count() const
{
    std::lock_guard<std::mutex> lk(mu_);
    return map_.size();
}

bool MemTable::empty() const
{
    std::lock_guard<std::mutex> lk(mu_);
    return map_.empty();
}

} // namespace crow::tree
