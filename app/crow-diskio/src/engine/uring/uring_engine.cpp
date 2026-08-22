// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "engine/uring/uring_engine.h"

#ifdef CROW_HAVE_LIBURING

#    include "disk/disk.h"

#    include <cerrno>
#    include <memory>

namespace crow::diskio
{

UringEngine::UringEngine(unsigned ring_entries) : reactor_(ring_entries, crow::common::PollingMode::Hybrid)
{
}

UringEngine::UringEngine(unsigned ring_entries, crow::common::PollingMode mode, crow::common::HybridConfig hybrid,
                         crow::common::SqpollConfig sqpoll)
    : reactor_(ring_entries, mode, hybrid, sqpoll)
{
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
    DiskId                   did        = disk->id();
    auto                     op_id_ptr  = std::make_shared<uint64_t>(0);
    std::function<void(int)> wrapped_cb = [this, did, op_id_ptr, cb = std::move(on_complete)](int res) {
        {
            auto                       &s = shard(did);
            std::lock_guard<std::mutex> lk(s.mu);
            auto                        it = s.ops.find(did);
            if (it != s.ops.end()) {
                it->second.erase(*op_id_ptr);
            }
        }
        if (cb) {
            cb(res);
        }
    };
    uint64_t op_id = reactor_.submit_write(disk->fd(), data, size, phys_offset, std::move(wrapped_cb));
    *op_id_ptr     = op_id;
    if (op_id != 0) {
        auto                       &s = shard(did);
        std::lock_guard<std::mutex> lk(s.mu);
        s.ops[did].insert(op_id);
    }
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
    DiskId                   did        = disk->id();
    auto                     op_id_ptr  = std::make_shared<uint64_t>(0);
    std::function<void(int)> wrapped_cb = [this, did, op_id_ptr, cb = std::move(on_complete)](int res) {
        {
            auto                       &s = shard(did);
            std::lock_guard<std::mutex> lk(s.mu);
            auto                        it = s.ops.find(did);
            if (it != s.ops.end()) {
                it->second.erase(*op_id_ptr);
            }
        }
        if (cb) {
            cb(res);
        }
    };
    uint64_t op_id = reactor_.submit_read(disk->fd(), buf, size, phys_offset, std::move(wrapped_cb));
    *op_id_ptr     = op_id;
    if (op_id != 0) {
        auto                       &s = shard(did);
        std::lock_guard<std::mutex> lk(s.mu);
        s.ops[did].insert(op_id);
    }
}

void UringEngine::submit_fsync(Disk *disk, std::function<void(int)> on_complete)
{
    if (disk == nullptr || disk->fd() < 0) {
        if (on_complete) {
            on_complete(-EBADF);
        }
        return;
    }
    reactor_.submit_fsync(disk->fd(), std::move(on_complete));
}

void UringEngine::cancel_disk(DiskId disk_id)
{
    std::unordered_set<uint64_t> ops;
    {
        auto                       &s = shard(disk_id);
        std::lock_guard<std::mutex> lk(s.mu);
        auto                        it = s.ops.find(disk_id);
        if (it != s.ops.end()) {
            ops = std::move(it->second);
            s.ops.erase(it);
        }
    }
    for (uint64_t op_id : ops) {
        reactor_.cancel(op_id);
    }
}

size_t UringEngine::in_flight_count(DiskId disk_id)
{
    auto                       &s = shard(disk_id);
    std::lock_guard<std::mutex> lk(s.mu);
    auto                        it = s.ops.find(disk_id);
    if (it == s.ops.end()) {
        return 0;
    }
    return it->second.size();
}

} // namespace crow::diskio

#endif // CROW_HAVE_LIBURING
