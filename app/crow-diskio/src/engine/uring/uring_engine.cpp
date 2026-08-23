// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "engine/uring/uring_engine.h"

#ifdef CROW_HAVE_LIBURING

#    include "disk/disk.h"

#    include <cerrno>
#    include <utility>

namespace crow::diskio
{

UringEngine::UringEngine(unsigned ring_entries)
{
    crow::common::Topology       topo;
    crow::common::PipelineConfig cfg;
    cfg.entries = ring_entries;
    cfg.mode    = crow::common::PollingMode::Hybrid;
    topo.pipelines.push_back(cfg);
    uring_ = std::make_unique<crow::common::DiskIOUring>(std::move(topo));
}

UringEngine::UringEngine(unsigned ring_entries, crow::common::PollingMode mode, crow::common::HybridConfig hybrid,
                         crow::common::SqpollConfig sqpoll)
{
    crow::common::Topology       topo;
    crow::common::PipelineConfig cfg;
    cfg.entries = ring_entries;
    cfg.mode    = mode;
    cfg.hybrid  = hybrid;
    cfg.sqpoll  = sqpoll;
    topo.pipelines.push_back(cfg);
    uring_ = std::make_unique<crow::common::DiskIOUring>(std::move(topo));
}

void UringEngine::submit_write(Disk *disk, off_t phys_offset, const uint8_t *data, size_t size,
                               std::function<void(int)> on_complete)
{
    if (disk == nullptr || disk->fd() < 0) {
        if (on_complete) {
            on_complete(-EBADF);
        }
        return;
    }
    if (disk->is_o_direct() && disk->block_size() > 0) {
        if ((size % disk->block_size()) != 0 || (static_cast<size_t>(phys_offset) % disk->block_size()) != 0) {
            if (on_complete) {
                on_complete(-EINVAL);
            }
            return;
        }
    }
    uring_->submit_write(disk->fd(), data, size, phys_offset, std::move(on_complete));
}

void UringEngine::submit_read(Disk *disk, off_t phys_offset, uint8_t *buf, size_t size,
                              uint64_t /*test_pattern_offset*/, std::function<void(int)> on_complete)
{
    if (disk == nullptr || disk->fd() < 0) {
        if (on_complete) {
            on_complete(-EBADF);
        }
        return;
    }
    if (disk->is_o_direct() && disk->block_size() > 0) {
        if ((size % disk->block_size()) != 0 || (static_cast<size_t>(phys_offset) % disk->block_size()) != 0) {
            if (on_complete) {
                on_complete(-EINVAL);
            }
            return;
        }
    }
    uring_->submit_read(disk->fd(), buf, size, phys_offset, std::move(on_complete));
}

void UringEngine::submit_fsync(Disk *disk, std::function<void(int)> on_complete)
{
    if (disk == nullptr || disk->fd() < 0) {
        if (on_complete) {
            on_complete(-EBADF);
        }
        return;
    }
    uring_->submit_fsync(disk->fd(), std::move(on_complete));
}

void UringEngine::cancel_disk(DiskId /*disk_id*/)
{
    // cancel_disk is called with a DiskId, but DiskIOUring::cancel_fd takes
    // an fd. The caller (diskio server) should call uring().cancel_fd(fd)
    // directly for bad-disk cancellation. This override is a no-op — the
    // server-level monitor handles bad-disk detection and cancellation.
}

} // namespace crow::diskio

#endif // CROW_HAVE_LIBURING
