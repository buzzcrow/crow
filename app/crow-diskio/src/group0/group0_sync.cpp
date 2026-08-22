// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "group0/group0_sync.h"

#include "crow-kv-client/c_api.h"
#include "disk/block_disk.h"
#include "disk/mem_disk.h"
#include "disk/null_disk.h"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>

// Minimal JSON parser for the FFI responses. We only need to extract
// disk_id (high/low), device_path, capacity, zone_size, unit_size,
// and zone_count from the DiskValue. Using a simple string search
// since we control the JSON format from the FFI side.

namespace crow::diskio
{

// ── Callback context for async FFI ops ────────────────────────────

struct SyncCallbackCtx
{
    std::atomic<int>  status{-1};
    std::string       json_result;
    std::atomic<bool> done{false};
};

static void on_ffi_complete(int status, const char *result_json, void *user_data)
{
    auto *ctx = static_cast<SyncCallbackCtx *>(user_data);
    ctx->status.store(status, std::memory_order_relaxed);
    if (result_json != nullptr) {
        ctx->json_result = result_json;
    }
    ctx->done.store(true, std::memory_order_release);
}

static bool wait_for_ctx(SyncCallbackCtx &ctx, uint32_t timeout_ms = 10000)
{
    for (uint32_t i = 0; i < timeout_ms / 10; ++i) {
        if (ctx.done.load(std::memory_order_acquire)) {
            return ctx.status.load(std::memory_order_relaxed) == 0;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

// ── Group0Sync ────────────────────────────────────────────────────

Group0Sync::Group0Sync(Group0SyncConfig cfg, std::shared_ptr<DiskSet> disk_set, std::shared_ptr<IoEngine> engine)
    : cfg_(std::move(cfg)),
      disk_set_(std::move(disk_set)),
      engine_(std::move(engine))
{
}

Group0Sync::~Group0Sync()
{
    stop();
    if (hw_client_ != nullptr) {
        crow_hw_client_destroy(hw_client_);
    }
    if (svc_client_ != nullptr) {
        crow_svc_client_destroy(svc_client_);
    }
}

void Group0Sync::start()
{
    // Create FFI clients.
    std::vector<const char *> seeds;
    for (const auto &s : cfg_.kv_seeds) {
        seeds.push_back(s.c_str());
    }
    hw_client_  = crow_hw_client_create(seeds.data(), seeds.size());
    svc_client_ = crow_svc_client_create(seeds.data(), seeds.size());

    if (hw_client_ == nullptr || svc_client_ == nullptr) {
        std::fprintf(stderr, "warning: failed to create group-0 clients, sync disabled\n");
        return;
    }

    running_.store(true);
    thread_ = std::thread([this] { run_loop(); });
}

void Group0Sync::stop()
{
    running_.store(false);
    if (thread_.joinable()) {
        thread_.join();
    }
}

void Group0Sync::run_loop()
{
    // Initial sync (blocking).
    do_sync();

    // Periodic sync.
    while (running_.load()) {
        for (uint32_t i = 0; i < cfg_.sync_interval_ms / 100 && running_.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (!running_.load()) {
            break;
        }
        do_sync();
    }
}

void Group0Sync::do_sync()
{
    if (cfg_.auto_discover_disks) {
        fetch_disks_from_group0();
    }
    heartbeat();
}

void Group0Sync::fetch_disks_from_group0()
{
    if (hw_client_ == nullptr) {
        return;
    }

    SyncCallbackCtx ctx;
    crow_hw_list_disks_in_group(hw_client_, cfg_.rack_id, cfg_.node_id, cfg_.dg_id, on_ffi_complete, &ctx);
    if (!wait_for_ctx(ctx)) {
        std::fprintf(stderr, "warning: group-0 list_disks timed out\n");
        return;
    }
    if (ctx.status.load() != 0) {
        std::fprintf(stderr, "warning: group-0 list_disks failed\n");
        return;
    }

    // Parse the JSON response and reconcile the disk set.
    // The JSON is an array of {"disk_id": {"high": N, "low": N}, "value": {...}}.
    // For now, log the result. Full reconciliation (adding/removing disks
    // from the DiskSet based on group-0 state) will be implemented in a
    // follow-up — it requires parsing the JSON and creating BlockDisk/
    // NullDisk instances from the DiskValue fields.
    std::printf("group-0: fetched disk list for dg=%llu (%zu bytes)\n", static_cast<unsigned long long>(cfg_.dg_id),
                ctx.json_result.size());
}

void Group0Sync::heartbeat()
{
    if (svc_client_ == nullptr) {
        return;
    }

    // Build owned dg_ids JSON array.
    std::string dg_ids_json = "[" + std::to_string(cfg_.dg_id) + "]";

    SyncCallbackCtx ctx;
    crow_svc_heartbeat_diskio(svc_client_, cfg_.instance_id, cfg_.grpc_endpoint.c_str(), dg_ids_json.c_str(), "[]",
                              on_ffi_complete, &ctx);
    if (!wait_for_ctx(ctx)) {
        std::fprintf(stderr, "warning: group-0 heartbeat timed out\n");
        return;
    }
    if (ctx.status.load() != 0) {
        std::fprintf(stderr, "warning: group-0 heartbeat failed\n");
        return;
    }
}

} // namespace crow::diskio
