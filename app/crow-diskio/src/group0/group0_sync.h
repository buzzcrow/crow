// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Group0Sync: periodic group-0 synchronization for crow-diskio.
//
// When kv_seeds are configured, Group0Sync connects to group-0 via
// the crow-kv-client FFI C ABI, fetches the disk list for this node's
// disk-group, reconciles the DiskSet, and heartbeats to the service
// registry.
//
// Instead of a dedicated thread, Group0Sync uses a ScheduledExecutor
// (from crow-rpc). The main loop calls executor.run_due_tasks() every
// ~100ms; Group0Sync schedules periodic sync tasks on it. The FFI
// callbacks (which run on the FFI's tokio runtime thread) schedule
// follow-up tasks back on the executor.
#pragma once

#include "crow-rpc/scheduled_executor.h"
#include "disk/disk_set.h"
#include "disk/types.h"

#include <memory>
#include <string>
#include <vector>

namespace crow::diskio
{

struct Group0SyncConfig
{
    std::vector<std::string> kv_seeds;
    uint64_t                 instance_id      = 0;
    uint64_t                 rack_id          = 0;
    uint64_t                 node_id          = 0;
    uint64_t                 dg_id            = 0;
    uint32_t                 sync_interval_ms = 5000;
    std::string              grpc_endpoint; // e.g. "127.0.0.1:50051"
    bool                     auto_discover_disks = false;
};

class Group0Sync
{
  public:
    Group0Sync(Group0SyncConfig cfg, std::shared_ptr<DiskSet> disk_set, std::shared_ptr<IoEngine> engine,
               crow::rpc::ScheduledExecutor &executor);
    ~Group0Sync();

    Group0Sync(const Group0Sync &)            = delete;
    Group0Sync &operator=(const Group0Sync &) = delete;
    Group0Sync(Group0Sync &&)                 = delete;
    Group0Sync &operator=(Group0Sync &&)      = delete;

    // Start syncing: schedules the first sync task on the executor.
    // The executor must be polled (run_due_tasks) by the caller's loop.
    void start();

    // Stop syncing: cancels pending tasks. Does not block (no thread).
    void stop();

  private:
    void do_sync();
    void schedule_next_sync();
    void fetch_disks_from_group0();
    void reconcile_disks(const std::string &json);
    void heartbeat();

    Group0SyncConfig                     cfg_;
    std::shared_ptr<DiskSet>             disk_set_;
    std::shared_ptr<IoEngine>            engine_;
    crow::rpc::ScheduledExecutor        &executor_;
    crow::rpc::ScheduledExecutor::TaskId sync_task_id_{0};

    // FFI handles (opaque pointers from crow-kv-client FFI).
    void *hw_client_  = nullptr;
    void *svc_client_ = nullptr;
};

} // namespace crow::diskio
