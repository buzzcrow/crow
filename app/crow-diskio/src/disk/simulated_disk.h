// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// SimulatedDisk: wraps a real or mem disk + DiskProperties (latency, error
// rate). Used for fault-injection tests. SimulatedEngine wraps another
// IoEngine and injects per-I/O random latency and errors.
#pragma once

#include "disk/disk.h"
#include "disk/types.h"

#include <cstdint>
#include <memory>
#include <random>

namespace crow::diskio
{

struct DiskProperties
{
    uint32_t latency_min_ms = 0;
    uint32_t latency_max_ms = 0;
    double   error_rate     = 0.0; // 0.0 = no errors, 1.0 = all errors
};

class SimulatedDisk : public Disk
{
  public:
    SimulatedDisk(std::shared_ptr<Disk> inner, DiskProperties props);

    DiskType type() const override
    {
        return DiskType::Simulated;
    }

    int fd() const override
    {
        return inner_->fd();
    }

    bool is_o_direct() const override
    {
        return inner_->is_o_direct();
    }

    size_t block_size() const override
    {
        return inner_->block_size();
    }

    IoEngine *engine() override
    {
        return inner_->engine();
    }

    DiskId id() const override
    {
        return inner_->id();
    }

    Zone *find_zone(uint32_t zone_index) override
    {
        return inner_->find_zone(zone_index);
    }

    DiskProperties properties() const
    {
        return props_;
    }

    // Expose the wrapped disk so the SimulatedEngine can delegate I/O to
    // the inner engine with the inner disk (type-correct dispatch).
    std::shared_ptr<Disk> inner() const
    {
        return inner_;
    }

    // Draw a random latency from [latency_min_ms, latency_max_ms].
    uint32_t draw_latency();
    // Draw a random double; if < error_rate, inject an error.
    bool draw_error();

  private:
    std::shared_ptr<Disk>                  inner_;
    DiskProperties                         props_;
    std::mt19937                           rng_;
    std::uniform_real_distribution<double> real_dist_{0.0, 1.0};
};

} // namespace crow::diskio
