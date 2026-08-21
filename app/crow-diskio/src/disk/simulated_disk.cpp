// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "disk/simulated_disk.h"

namespace crow::diskio
{

SimulatedDisk::SimulatedDisk(std::shared_ptr<Disk> inner, DiskProperties props)
    : inner_(std::move(inner)),
      props_(props)
{
    // Seed RNG from disk_id for reproducibility.
    uint64_t seed = inner_->id().high ^ inner_->id().low;
    if (seed == 0) {
        seed = 1;
    }
    rng_.seed(static_cast<std::mt19937::result_type>(seed));
}

uint32_t SimulatedDisk::draw_latency()
{
    if (props_.latency_min_ms >= props_.latency_max_ms) {
        return props_.latency_min_ms;
    }
    std::uniform_int_distribution<uint32_t> dist(props_.latency_min_ms, props_.latency_max_ms);
    return dist(rng_);
}

bool SimulatedDisk::draw_error()
{
    if (props_.error_rate <= 0.0) {
        return false;
    }
    return real_dist_(rng_) < props_.error_rate;
}

} // namespace crow::diskio
