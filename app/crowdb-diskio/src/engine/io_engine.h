// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// IoEngine: virtual base for async I/O backends.
// Implementations: UringEngine (Linux io_uring), BlockingEngine (thread pool),
// DummyEngine (mem disk), SimulatedEngine (fault injection).
#pragma once

#include "disk/types.h"

#include <cstdint>
#include <functional>

namespace crowdb::diskio
{

class Disk;

class IoEngine
{
  public:
    virtual ~IoEngine() = default;

    // Submit a write/read/fsync. `on_complete` is invoked exactly once with
    // the raw result: >=0 bytes transferred, <0 negative -errno.
    // `test_pattern_offset` is used by NullDisk for deterministic content
    // generation (testing only); real engines ignore it.
    virtual void submit_write(Disk *disk, off_t phys_offset, const uint8_t *data, size_t size,
                              std::function<void(int)> on_complete)             = 0;
    virtual void submit_read(Disk *disk, off_t phys_offset, uint8_t *buf, size_t size, uint64_t test_pattern_offset,
                             std::function<void(int)> on_complete)              = 0;
    virtual void submit_fsync(Disk *disk, std::function<void(int)> on_complete) = 0;

    // Cancel all in-flight I/O for a disk (bad-disk isolation).
    virtual void cancel_disk(DiskId /*disk_id*/)
    {
    }
};

} // namespace crowdb::diskio
