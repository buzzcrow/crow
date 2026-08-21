// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// MemDisk: drop-write + rule-based read. No storage; reads return
// deterministic content from a cached pattern buffer. Used for throughput
// benches that measure the full RPC + engine path without real disk limits.
#pragma once

#include "disk/disk.h"
#include "disk/types.h"

#include <cstdint>
#include <optional>
#include <vector>

namespace crow::diskio
{

class MemDisk : public Disk
{
  public:
    MemDisk(DiskId id, std::vector<Zone> zones, size_t max_read_size);

    DiskType type() const override
    {
        return DiskType::Mem;
    }

    int fd() const override
    {
        return -1;
    }

    bool is_o_direct() const override
    {
        return false;
    }

    size_t block_size() const override
    {
        return 1;
    }

    IoEngine *engine() override
    {
        return engine_.get();
    }

    DiskId id() const override
    {
        return id_;
    }

    Zone *find_zone(uint32_t zone_index) override;

    // Read: memcpy from pattern_buf_ with wrap-around.
    // Write: drop (no-op), return success.
    int read(off_t phys_offset, uint8_t *buf, size_t size, std::optional<uint64_t> logical_object_offset);

    int write(off_t /*phys_offset*/, const uint8_t * /*data*/, size_t size)
    {
        return static_cast<int>(size); // drop-write: immediate success
    }

  private:
    void generate_pattern(uint64_t seed);

    DiskId               id_;
    size_t               pattern_len_;
    std::vector<uint8_t> pattern_buf_; // size = 2 * max_read_size
};

} // namespace crow::diskio
