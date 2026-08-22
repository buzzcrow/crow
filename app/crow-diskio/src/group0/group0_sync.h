// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Group0Sync: periodic group-0 synchronization for crow-diskio.
//
// When kv_seeds are configured, Group0Sync connects to group-0 via
// the crow-kv-client-ffi C ABI, fetches the disk list for this node's
// disk-group, and heartbeats to the service registry. Runs in a
// background thread with a configurable interval.
//
// The disk list fetch is async (callback-based via the FFI). The
// sync loop waits for each callback before proceeding.
#pragma once

#include "disk/disk_set.h"
#include "disk/types.h"

#include <atomic>
#include <memory>
#include <string>
#include <thread>
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
    Group0Sync(Group0SyncConfig cfg, std::shared_ptr<DiskSet> disk_set, std::shared_ptr<IoEngine> engine);
    ~Group0Sync();

    Group0Sync(const Group0Sync &)            = delete;
    Group0Sync &operator=(const Group0Sync &) = delete;
    Group0Sync(Group0Sync &&)                 = delete;
    Group0Sync &operator=(Group0Sync &&)      = delete;

    // Start the background sync thread. Returns immediately.
    void start();

    // Stop the sync thread (blocks until the thread exits).
    void stop();

  private:
    void run_loop();
    void do_sync();
    void fetch_disks_from_group0();
    void heartbeat();

    Group0SyncConfig          cfg_;
    std::shared_ptr<DiskSet>  disk_set_;
    std::shared_ptr<IoEngine> engine_;
    std::atomic<bool>         running_{false};
    std::thread               thread_;

    // FFI handles (opaque pointers from crow-kv-client-ffi).
    void *hw_client_  = nullptr;
    void *svc_client_ = nullptr;
};

} // namespace crow::diskio
