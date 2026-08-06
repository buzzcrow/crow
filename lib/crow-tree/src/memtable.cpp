// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-tree/memtable.h"

#include <algorithm>
#include <cstring>
#include <utility>

namespace crow::tree
{

namespace
{
buffer buf_of(Slice s)
{
    buffer b = buffer::alloc(s.size());
    if (!s.empty()) {
        std::memcpy(b.data(), s.data(), s.size());
    }
    return b;
}

// Materialize a contiguous [header][value] cell from a CellVersion (R30 split
// cell support). Contiguous: clone. Split (kExternal): build header from
// slot/flags + copy value.
buffer materialize_cell(const CellVersion *cv)
{
    if (cv->cell.ownership() != buffer::mode::kExternal) {
        return cv->cell.clone();
    }
    size_t   vlen = cv->cell.size();
    buffer   b    = buffer::alloc(vlen, kCellHeaderSize);
    uint8_t *p    = b.data();
    for (int i = 0; i < 8; ++i) {
        p[i] = static_cast<uint8_t>((cv->slot >> (8 * i)) & 0xff);
    }
    p[8] = cv->flags;
    if (vlen > 0) {
        std::memcpy(b.data() + kCellHeaderSize, cv->cell.data(), vlen);
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
    if (slot <= durable_floor_.load(std::memory_order_relaxed) && !allow_old_slots_.load(std::memory_order_relaxed)) {
        return false;
    }
    CellView     cv{cell_payload.slice()};
    uint64_t     entry_slot = cv.valid() ? cv.slot() : slot;
    uint8_t      flags      = cv.valid() ? cv.flags() : 0;
    CellVersion *ver        = make_version(entry_slot, flags, std::move(cell_payload));

    CellVersion *old = nullptr;
    if (!list_.upsert(key, ver, &old)) {
        delete ver; // rejected — caller cleans up
        return false;
    }
    list_.add_bytes(key.size() + ver->cell.size());
    if (old != nullptr) {
        list_.sub_bytes(key.size() + old->cell.size());
    }
    retire_version(old);
    update_slot_range(entry_slot);
    return true;
}

bool MemTable::upsert_external(Slice key, uint64_t slot, uint8_t flags, buffer &&value)
{
    if (slot <= durable_floor_.load(std::memory_order_relaxed) && !allow_old_slots_.load(std::memory_order_relaxed)) {
        return false;
    }
    // upsert_external stores the raw value (no 9-byte cell header); the
    // header is reconstructed from slot/flags at read time. Tag non-kExternal
    // buffers as kExternal so get_view/scan treat them as split cells.
    if (value.ownership() != buffer::mode::kExternal) {
        if (!value.empty()) {
            auto *heap = new buffer(std::move(value));
            value      = buffer::wrap_external(heap->data(), heap->size(), heap,
                                               [](void *ctx) { delete static_cast<buffer *>(ctx); });
        }
        else {
            value = buffer::wrap_external(nullptr, 0, nullptr, nullptr);
        }
    }
    CellVersion *ver = make_version(slot, flags, std::move(value));

    CellVersion *old = nullptr;
    if (!list_.upsert(key, ver, &old)) {
        delete ver;
        return false;
    }
    list_.add_bytes(key.size() + ver->cell.size());
    if (old != nullptr) {
        list_.sub_bytes(key.size() + old->cell.size());
    }
    retire_version(old);
    update_slot_range(slot);
    return true;
}

void MemTable::set_durable_floor(uint64_t slot)
{
    uint64_t cur = durable_floor_.load(std::memory_order_relaxed);
    while (slot > cur && !durable_floor_.compare_exchange_weak(cur, slot, std::memory_order_relaxed)) {
        // cur is refreshed by CAS failure
    }
}

void MemTable::reset()
{
    auto drained = list_.drain_all();
    for (auto &e : drained) {
        size_t entry_bytes = e.key.size() + e.cv->cell.size();
        list_.sub_bytes(entry_bytes);
        retire_version(e.cv);
        retire_node(e.node);
    }
    durable_floor_.store(0, std::memory_order_relaxed);
    allow_old_slots_.store(false, std::memory_order_relaxed);
    reset_slot_range();
}

std::vector<mem_entry> MemTable::drain_up_to(uint64_t cs)
{
    auto                   drained = list_.drain_up_to(cs);
    std::vector<mem_entry> out;
    out.reserve(drained.size());
    for (auto &e : drained) {
        size_t entry_bytes = e.key.size() + e.cv->cell.size();
        buffer cell        = materialize_cell(e.cv);
        out.push_back({.key = std::move(e.key), .cell = std::move(cell), .slot = e.slot});
        list_.sub_bytes(entry_bytes);
        retire_version(e.cv);
        retire_node(e.node);
    }
    if (list_.empty()) {
        reset_slot_range();
    }
    return out;
}

std::vector<mem_entry> MemTable::snapshot() const
{
    std::vector<mem_entry> out;
    auto                   cur = list_.cursor(Slice());
    while (cur.valid()) {
        const CellVersion *cv = cur.cell_version();
        if (cv != nullptr) {
            out.push_back({.key = cur.key().to_string(), .cell = materialize_cell(cv), .slot = cv->slot});
        }
        cur.advance();
    }
    return out;
} // NOLINT(clang-analyzer-unix.Malloc)

} // namespace crow::tree
