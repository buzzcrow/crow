// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// DummyEngine: IoEngine for MemDisk. Writes are dropped (immediate success);
// reads return deterministic content from MemDisk's pattern buffer.
// Used for throughput benches that measure RPC + engine overhead at
// memory speed.
#pragma once

#include "disk/types.h"
#include "engine/io_engine.h"

#include <cstdint>
#include <functional>
#include <optional>

namespace crow::diskio
{

class MemDisk;

class DummyEngine : public IoEngine
{
  public:
    explicit DummyEngine(std::optional<uint64_t> logical_object_offset = std::nullopt)
        : logical_offset_(logical_object_offset)
    {
    }

    void submit_write(Disk *disk, off_t phys_offset, const uint8_t *data, size_t size,
                      std::function<void(int)> on_complete) override;
    void submit_read(Disk *disk, off_t phys_offset, uint8_t *buf, size_t size,
                     std::function<void(int)> on_complete) override;
    void submit_fsync(Disk *disk, std::function<void(int)> on_complete) override;

  private:
    std::optional<uint64_t> logical_offset_;
};

} // namespace crow::diskio
