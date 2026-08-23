// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// MemDisk: dummy block device backed by memfd_create. Unlike NullDisk,
// MemDisk stores written data and reads it back — for end-to-end
// correctness tests that verify I/O data integrity.
//
// The full uring or blocking I/O path executes (real pwrite/pread on
// the memfd). No real disk I/O — tmpfs is RAM.
//
// Optional DiskProperties enable fault injection (latency, error rate).
// When fault injection is active, a DummyDiskEngine wrapper is used;
// otherwise the shared engine is used directly (no read-content hack).
#pragma once

#include "disk/disk.h"
#include "disk/disk_properties.h"
#include "disk/types.h"
#include "engine/io_engine.h"

#include <memory>
#include <optional>
#include <vector>

namespace crow::diskio
{

class MemDisk : public Disk
{
  public:
    MemDisk(DiskId id, std::shared_ptr<IoEngine> engine, std::vector<Zone> zones,
            std::optional<DiskProperties> props = std::nullopt);
    ~MemDisk() override;

    DiskType type() const override
    {
        return DiskType::Mem;
    }

    int fd() const override
    {
        return fd_;
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

  private:
    DiskId                    id_;
    int                       fd_;
    std::shared_ptr<IoEngine> engine_;
};

} // namespace crow::diskio
