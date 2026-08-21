// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "disk/mem_disk.h"

#include <algorithm>
#include <cstring>

namespace crow::diskio
{

namespace
{
// xorshift64 PRNG for deterministic pattern generation.
uint64_t xorshift64(uint64_t &state)
{
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    return state;
}

uint64_t hash_seed(DiskId id)
{
    return id.high ^ (id.low * 0x9E3779B97F4A7C15ULL);
}
} // namespace

MemDisk::MemDisk(DiskId id, std::vector<Zone> zones, size_t max_read_size)
    : id_(id),
      pattern_len_(std::max<size_t>(max_read_size, 4096))
{
    zones_ = std::move(zones);
    pattern_buf_.resize(pattern_len_ * 2);
    generate_pattern(hash_seed(id));
}

void MemDisk::generate_pattern(uint64_t seed)
{
    uint64_t state = seed;
    for (size_t i = 0; i < pattern_buf_.size(); i += sizeof(uint64_t)) {
        uint64_t val = xorshift64(state);
        std::memcpy(&pattern_buf_[i], &val, std::min(sizeof(uint64_t), pattern_buf_.size() - i));
    }
}

Zone *MemDisk::find_zone(uint32_t zone_index)
{
    for (auto &z : zones_) {
        if (z.zone_index == zone_index) {
            return &z;
        }
    }
    return nullptr;
}

int MemDisk::read(off_t phys_offset, uint8_t *buf, size_t size, std::optional<uint64_t> logical_object_offset)
{
    if (size == 0) {
        return 0;
    }
    // Compute the pattern index: mix phys_offset with logical_object_offset
    // when present, so different logical objects at the same physical offset
    // produce different content.
    uint64_t idx;
    if (logical_object_offset.has_value()) {
        // Mix logical_object_offset into the index using a multiplier that's
        // coprime with pattern_len_ (which is a power of 2). Any odd multiplier
        // is coprime with a power of 2. Use a large prime for good dispersion.
        idx =
            (static_cast<uint64_t>(phys_offset) + logical_object_offset.value() * 0x9E3779B97F4A7C15ULL) % pattern_len_;
    }
    else {
        idx = static_cast<uint64_t>(phys_offset) % pattern_len_;
    }
    // memcpy with wrap-around. pattern_buf_ is 2x pattern_len_, so a single
    // memcpy handles any read up to pattern_len_ without a second copy.
    size_t to_copy = std::min(size, pattern_len_);
    std::memcpy(buf, pattern_buf_.data() + idx, to_copy);
    if (to_copy < size) {
        // Wrap-around: copy the remaining bytes from the start.
        std::memcpy(buf + to_copy, pattern_buf_.data(), size - to_copy);
    }
    return static_cast<int>(size);
}

} // namespace crow::diskio
