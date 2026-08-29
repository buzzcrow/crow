// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// NullDisk: dummy block device backed by memfd_create. The full uring
// or blocking I/O path executes (real pwrite/pread on the memfd), but:
// - Writes: data goes to tmpfs memory (discarded — not read back).
// - Reads: the inner engine preads from the memfd, then the wrapper
//   engine overwrites the buffer with deterministic pattern data.
//
// Used for benchmark tests: measures uring/blocking overhead without
// real disk I/O or storage capacity limits. Default disk type when no
// real block device is configured.
//
// Optional DiskProperties enable fault injection (latency, error rate).
#pragma once

#include "disk/disk.h"
#include "disk/disk_properties.h"
#include "disk/types.h"
#include "engine/io_engine.h"

#include <memory>
#include <optional>
#include <vector>

namespace crowdb::diskio
{

class NullDisk : public Disk
{
  public:
    NullDisk(DiskId id, std::shared_ptr<IoEngine> engine, std::vector<Zone> zones,
             std::optional<DiskProperties> props = std::nullopt);
    ~NullDisk() override;

    DiskType type() const override
    {
        return DiskType::Null;
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
        return wrapper_.get();
    }

    DiskId id() const override
    {
        return id_;
    }

    Zone *find_zone(uint32_t zone_index) override;

  private:
    DiskId                            id_;
    int                               fd_;
    std::shared_ptr<IoEngine>         wrapper_;
};

} // namespace crowdb::diskio
