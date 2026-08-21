// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// SimulatedEngine: wraps another IoEngine and injects per-I/O random
// latency and errors based on the SimulatedDisk's DiskProperties.
#pragma once

#include "disk/types.h"
#include "engine/io_engine.h"

#include <cerrno>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>

namespace crow::diskio
{

class SimulatedDisk;

class SimulatedEngine : public IoEngine
{
  public:
    explicit SimulatedEngine(std::unique_ptr<IoEngine> inner) : inner_(std::move(inner))
    {
    }

    void submit_write(Disk *disk, off_t phys_offset, const uint8_t *data, size_t size,
                      std::function<void(int)> on_complete) override;
    void submit_read(Disk *disk, off_t phys_offset, uint8_t *buf, size_t size,
                     std::function<void(int)> on_complete) override;
    void submit_fsync(Disk *disk, std::function<void(int)> on_complete) override;

  private:
    std::unique_ptr<IoEngine> inner_;

    // Inject latency + error for a given disk. If error is injected, invokes
    // on_complete(-EIO) after the latency delay. Otherwise, delegates to
    // inner_->submit_* with a wrapped callback that delays by the latency.
    void inject_write(Disk *disk, off_t phys_offset, const uint8_t *data, size_t size,
                      std::function<void(int)> on_complete);
    void inject_read(Disk *disk, off_t phys_offset, uint8_t *buf, size_t size, std::function<void(int)> on_complete);
    void inject_fsync(Disk *disk, std::function<void(int)> on_complete);
};

} // namespace crow::diskio
