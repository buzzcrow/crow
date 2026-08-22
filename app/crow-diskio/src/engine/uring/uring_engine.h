// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// UringEngine: IoEngine backed by crow::common::Reactor (io_uring).
// Linux-only (CROW_HAVE_LIBURING). Wraps the reactor's submit_read/write/
// fsync with per-disk in-flight tracking for bad-disk cancellation.
#pragma once

#include "disk/types.h"
#include "engine/io_engine.h"

#ifdef CROW_HAVE_LIBURING
#    include "crow-common/reactor.h"
#endif

#include <array>
#include <cstdint>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace crow::diskio
{

#ifdef CROW_HAVE_LIBURING

class UringEngine : public IoEngine
{
  public:
    explicit UringEngine(unsigned ring_entries = 256);
    UringEngine(unsigned ring_entries, crow::common::PollingMode mode, crow::common::HybridConfig hybrid = {},
                crow::common::SqpollConfig sqpoll = {});
    ~UringEngine() override = default;

    void submit_write(Disk *disk, off_t phys_offset, const uint8_t *data, size_t size,
                      std::function<void(int)> on_complete) override;
    void submit_read(Disk *disk, off_t phys_offset, uint8_t *buf, size_t size, uint64_t test_pattern_offset,
                     std::function<void(int)> on_complete) override;
    void submit_fsync(Disk *disk, std::function<void(int)> on_complete) override;
    void cancel_disk(DiskId disk_id) override;

    // For testing: number of in-flight ops for a disk.
    size_t in_flight_count(DiskId disk_id);

  private:
    // Sharded by DiskId hash so writes to different disks don't contend.
    // Each shard owns its own map, so inserts on different shards never race
    // on shared bucket/rehash state. One shard lock covers a disk's whole
    // entry, keeping cancel_disk atomic.
    static constexpr size_t kInFlightShards = 16;

    struct InFlightShard
    {
        std::mutex                                                           mu;
        std::unordered_map<DiskId, std::unordered_set<uint64_t>, DiskIdHash> ops;
    };

    InFlightShard &shard(DiskId d)
    {
        return shards_[DiskIdHash{}(d) % kInFlightShards];
    }

    crow::common::Reactor                      reactor_;
    std::array<InFlightShard, kInFlightShards> shards_;
};

#endif // CROW_HAVE_LIBURING

} // namespace crow::diskio
