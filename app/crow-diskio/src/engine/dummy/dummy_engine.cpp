// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "engine/dummy/dummy_engine.h"

#include "disk/disk.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <random>
#include <thread>

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

DummyDiskEngine::DummyDiskEngine(std::shared_ptr<IoEngine> inner, bool hack_reads, std::optional<DiskProperties> props)
    : inner_(std::move(inner)),
      hack_reads_(hack_reads),
      props_(props)
{
}

void DummyDiskEngine::fill_pattern(DiskId disk_id, uint64_t test_pattern_offset, uint8_t *buf, size_t size)
{
    if (size == 0) {
        return;
    }
    // Generate deterministic pattern on the fly.
    uint64_t state = hash_seed(disk_id);
    // Advance the PRNG to the start of this offset (8 bytes at a time).
    uint64_t skip = test_pattern_offset / sizeof(uint64_t);
    for (uint64_t i = 0; i < skip; ++i) {
        xorshift64(state);
    }
    size_t pos = 0;
    while (pos < size) {
        uint64_t val = xorshift64(state);
        size_t   n   = std::min(sizeof(uint64_t), size - pos);
        std::memcpy(buf + pos, &val, n);
        pos += n;
    }
}

uint32_t DummyDiskEngine::draw_latency() const
{
    if (!props_.has_value() || props_->latency_max_ms == 0) {
        return 0;
    }
    static thread_local std::mt19937 rng{std::random_device{}()};
    uint32_t                         lo = props_->latency_min_ms;
    uint32_t                         hi = props_->latency_max_ms;
    if (lo >= hi) {
        return lo;
    }
    std::uniform_int_distribution<uint32_t> dist(lo, hi);
    return dist(rng);
}

bool DummyDiskEngine::draw_error() const
{
    if (!props_.has_value() || props_->error_rate <= 0.0) {
        return false;
    }
    static thread_local std::mt19937       rng{std::random_device{}()};
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    return dist(rng) < props_->error_rate;
}

void DummyDiskEngine::submit_write(Disk *disk, off_t phys_offset, const uint8_t *data, size_t size,
                                   std::function<void(int)> on_complete)
{
    if (draw_error()) {
        uint32_t latency_ms = draw_latency();
        std::thread([latency_ms, cb = std::move(on_complete)]() {
            if (latency_ms > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(latency_ms));
            }
            if (cb) {
                cb(-EIO);
            }
        }).detach();
        return;
    }
    uint32_t latency_ms = draw_latency();
    auto     wrapped    = [latency_ms, cb = std::move(on_complete)](int res) {
        if (latency_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(latency_ms));
        }
        if (cb) {
            cb(res);
        }
    };
    inner_->submit_write(disk, phys_offset, data, size, std::move(wrapped));
}

void DummyDiskEngine::submit_read(Disk *disk, off_t phys_offset, uint8_t *buf, size_t size,
                                  uint64_t test_pattern_offset, std::function<void(int)> on_complete)
{
    if (draw_error()) {
        uint32_t latency_ms = draw_latency();
        std::thread([latency_ms, cb = std::move(on_complete)]() {
            if (latency_ms > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(latency_ms));
            }
            if (cb) {
                cb(-EIO);
            }
        }).detach();
        return;
    }
    DiskId   did        = (disk != nullptr) ? disk->id() : DiskId{};
    uint32_t latency_ms = draw_latency();
    auto     wrapped    = [this, did, test_pattern_offset, buf, latency_ms, cb = std::move(on_complete)](int res) {
        if (latency_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(latency_ms));
        }
        if (res > 0 && hack_reads_) {
            fill_pattern(did, test_pattern_offset, buf, static_cast<size_t>(res));
        }
        if (cb) {
            cb(res);
        }
    };
    inner_->submit_read(disk, phys_offset, buf, size, test_pattern_offset, std::move(wrapped));
}

void DummyDiskEngine::submit_fsync(Disk *disk, std::function<void(int)> on_complete)
{
    if (draw_error()) {
        uint32_t latency_ms = draw_latency();
        std::thread([latency_ms, cb = std::move(on_complete)]() {
            if (latency_ms > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(latency_ms));
            }
            if (cb) {
                cb(-EIO);
            }
        }).detach();
        return;
    }
    uint32_t latency_ms = draw_latency();
    auto     wrapped    = [latency_ms, cb = std::move(on_complete)](int res) {
        if (latency_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(latency_ms));
        }
        if (cb) {
            cb(res);
        }
    };
    inner_->submit_fsync(disk, std::move(wrapped));
}

} // namespace crow::diskio
