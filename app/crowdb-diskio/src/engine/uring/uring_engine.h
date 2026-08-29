// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// UringEngine: IoEngine backed by crowdb::common::DiskIOUring (io_uring).
// Linux-only (CROWDB_HAVE_LIBURING). Wraps the uring engine's submit_read/
// write/fsync with O_DIRECT alignment validation. Per-disk in-flight
// tracking and cancel are handled by DiskIOUring (cancel_fd).
#pragma once

#include "disk/types.h"
#include "engine/io_engine.h"

#ifdef CROWDB_HAVE_LIBURING
#    include "crowdb-common/diskio_uring.h"
#endif

#include <cstdint>
#include <functional>
#include <memory>

namespace crowdb::diskio
{

#ifdef CROWDB_HAVE_LIBURING

class UringEngine : public IoEngine
{
  public:
    explicit UringEngine(unsigned ring_entries = 256);
    UringEngine(unsigned ring_entries, crowdb::common::PollingMode mode, crowdb::common::HybridConfig hybrid = {},
                crowdb::common::SqpollConfig sqpoll = {});
    ~UringEngine() override = default;

    void submit_write(Disk *disk, off_t phys_offset, const uint8_t *data, size_t size,
                      std::function<void(int)> on_complete) override;
    void submit_read(Disk *disk, off_t phys_offset, uint8_t *buf, size_t size, uint64_t test_pattern_offset,
                     std::function<void(int)> on_complete) override;
    void submit_fsync(Disk *disk, std::function<void(int)> on_complete) override;
    void cancel_disk(DiskId disk_id) override;

    // Access the underlying DiskIOUring (for fd registration by the server).
    crowdb::common::DiskIOUring &uring()
    {
        return *uring_;
    }

  private:
    std::unique_ptr<crowdb::common::DiskIOUring> uring_;
};

#endif // CROWDB_HAVE_LIBURING

} // namespace crowdb::diskio
