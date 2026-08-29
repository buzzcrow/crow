// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// crowdb-diskio: the disk I/O server binary.
//
// Parses config from CLI args, auto-detects the I/O engine (io_uring
// on Linux with liburing, blocking thread-pool otherwise), creates
// DiskSet + IoEngine, registers diskio RPC handlers, and serves until
// SIGTERM/SIGINT.
//
// Usage: crowdb-diskio --port <port> [--bind <addr>]
//   [--dummy-disk null|mem] [--disk <hex_id>:<path>[:<capacity>]]...
//
// Engine is auto-detected — no --engine flag. Disks with an empty path
// are dummy disks (NullDisk by default, MemDisk with --dummy-disk mem).

#include "crowdb-kv-client/c_api.h"
#include "crowdb-rpc/scheduled_executor.h"
#include "crowdb-rpc/server/server.h"
#include "crowdb-rpc/transport/socket_transport.h"
#include "dio_config.h"
#include "disk/block_disk.h"
#include "disk/disk_set.h"
#include "disk/mem_disk.h"
#include "disk/null_disk.h"
#include "engine/blocking/blocking_engine.h"
#include "group0/group0_sync.h"
#include "rpc/dio_server.h"

#include <atomic>
#include <csignal>
#include <cstdio>
#include <thread>

#ifdef CROWDB_HAVE_LIBURING
#    include "engine/uring/uring_engine.h"
#endif

static std::atomic<bool> g_running{true};

static void on_signal(int)
{
    g_running.store(false);
}

// Auto-detect the IoEngine: try UringEngine first (Linux with liburing),
// fall back to BlockingEngine. Returns nullptr on error.
static std::shared_ptr<crowdb::diskio::IoEngine> create_engine(const crowdb::diskio::DioConfig &cfg)
{
    using namespace crowdb::diskio;
#ifdef CROWDB_HAVE_LIBURING
    try {
        auto engine = std::make_shared<UringEngine>(cfg.sq_entries);
        return engine;
    }
    catch (const std::exception &e) {
        std::fprintf(stderr, "warning: uring engine creation failed (%s), falling back to blocking\n", e.what());
    }
#endif
    return std::make_shared<BlockingEngine>(cfg.thread_pool_size);
}

// Build the DiskSet from config. Disks with a non-empty path are
// BlockDisk (O_DIRECT block device); disks with an empty path are
// dummy disks (NullDisk or MemDisk per config). For UringEngine, real
// block device fds are registered with the uring for fd→pipeline routing.
static std::shared_ptr<crowdb::diskio::DiskSet> build_disk_set(const crowdb::diskio::DioConfig          &cfg,
                                                             std::shared_ptr<crowdb::diskio::IoEngine> engine)
{
    using namespace crowdb::diskio;
    auto disk_set = std::make_shared<DiskSet>();
#ifdef CROWDB_HAVE_LIBURING
    // If the engine is a UringEngine, register each real disk's fd with it.
    auto uring_engine = std::dynamic_pointer_cast<UringEngine>(engine);
#endif
    for (const auto &entry : cfg.disks) {
        if (entry.path.empty()) {
            // Dummy disk (NullDisk or MemDisk).
            auto zones = std::vector<Zone>(entry.zones);
            if (cfg.dummy_disk_type == DummyDiskType::Mem) {
                auto disk = std::make_shared<MemDisk>(entry.id, engine, std::move(zones), cfg.dummy_props);
                disk_set->add(disk);
            }
            else {
                auto disk = std::make_shared<NullDisk>(entry.id, engine, std::move(zones), cfg.dummy_props);
                disk_set->add(disk);
            }
        }
        else {
            // Real block device.
            auto disk =
                std::make_shared<BlockDisk>(entry.id, entry.path, engine, std::vector<Zone>(entry.zones), cfg.o_direct);
#ifdef CROWDB_HAVE_LIBURING
            // Register the disk's fd with the uring for fd→pipeline routing.
            if (uring_engine != nullptr && disk->fd() >= 0) {
                uring_engine->uring().register_fd(disk->fd());
            }
#endif
            disk_set->add(disk);
        }
    }
    return disk_set;
}

int main(int argc, char *argv[])
{
    using namespace crowdb::diskio;

    // Disable stdout/stderr buffering so output is immediately visible
    // when captured via pipes/files (e.g. in integration tests).
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::setvbuf(stderr, nullptr, _IONBF, 0);

    DioConfig   cfg;
    std::string err;
    if (!DioConfig::parse_args(argc, argv, cfg, err)) {
        std::fprintf(stderr, "error: %s\n", err.c_str());
        return 1;
    }
    if (!cfg.validate(err)) {
        std::fprintf(stderr, "error: %s\n", err.c_str());
        return 1;
    }

    std::signal(SIGTERM, on_signal);
    std::signal(SIGINT, on_signal);

    // Auto-detect and create the engine.
    auto engine = create_engine(cfg);
    if (engine == nullptr) {
        return 1;
    }

    // Build the disk set (disks share the engine).
    auto disk_set = build_disk_set(cfg, engine);
    if (disk_set == nullptr) {
        return 1;
    }

    // Create + start the RPC server.
    crowdb::rpc::RpcServer server;
    if (!server.listen(cfg.bind_address, cfg.listen_port)) {
        std::fprintf(stderr, "error: failed to listen on %s:%d\n", cfg.bind_address.c_str(), cfg.listen_port);
        return 1;
    }
    int actual_port = server.listen_port();
    std::printf("crowdb-diskio listening on %s:%d (%zu disks)\n", cfg.bind_address.c_str(), actual_port,
                disk_set->size());
    std::fflush(stdout);

    auto *transport  = server.transport();
    auto  dio_server = std::make_unique<DiskioServer>(disk_set, transport);
    dio_server->register_handlers(server);

    server.start();

    // Scheduled executor for periodic tasks (group-0 sync, etc.).
    // The main loop polls run_due_tasks() every ~100ms.
    crowdb::rpc::ScheduledExecutor scheduler;

    // Start group-0 sync if kv_seeds are configured.
    std::unique_ptr<Group0Sync> group0_sync;
    if (!cfg.kv_seeds.empty()) {
        Group0SyncConfig g0_cfg;
        g0_cfg.kv_seeds            = cfg.kv_seeds;
        g0_cfg.instance_id         = cfg.instance_id;
        g0_cfg.rack_id             = cfg.rack_id;
        g0_cfg.node_id             = cfg.node_id;
        g0_cfg.dg_id               = cfg.dg_id;
        g0_cfg.sync_interval_ms    = cfg.sync_interval_ms;
        g0_cfg.rpc_endpoint       = cfg.bind_address + ":" + std::to_string(actual_port);
        g0_cfg.auto_discover_disks = cfg.auto_discover_disks;
        g0_cfg.dummy_disk_type     = cfg.dummy_disk_type;
        g0_cfg.dummy_props         = cfg.dummy_props;
        group0_sync                = std::make_unique<Group0Sync>(std::move(g0_cfg), disk_set, engine, scheduler);
        group0_sync->start();
        std::printf("group-0 sync started (interval=%ums, dg=%llu)\n", cfg.sync_interval_ms,
                    static_cast<unsigned long long>(cfg.dg_id));
    }

    // Run until signaled. Poll the scheduler every 100ms.
    while (g_running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        scheduler.run_due_tasks();
    }

    if (group0_sync != nullptr) {
        group0_sync->stop();
    }
    server.stop();
    disk_set->shutdown();

    // Stop the engine (BlockingEngine has a stop() method).
    if (auto *be = dynamic_cast<BlockingEngine *>(engine.get())) {
        be->stop();
    }

    // Shut down the FFI tokio runtime (if it was initialized).
    if (!cfg.kv_seeds.empty()) {
        crowdb_kv_ffi_shutdown();
    }

    return 0;
}
