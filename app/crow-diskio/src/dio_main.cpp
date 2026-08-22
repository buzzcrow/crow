// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// crow-diskio: the disk I/O server binary.
//
// Parses config from CLI args, auto-detects the I/O engine (io_uring
// on Linux with liburing, blocking thread-pool otherwise), creates
// DiskSet + IoEngine, registers diskio RPC handlers, and serves until
// SIGTERM/SIGINT.
//
// Usage: crow-diskio --port <port> [--bind <addr>]
//   [--dummy-disk null|mem] [--disk <hex_id>:<path>[:<capacity>]]...
//
// Engine is auto-detected — no --engine flag. Disks with an empty path
// are dummy disks (NullDisk by default, MemDisk with --dummy-disk mem).

#include "crow-rpc/server/server.h"
#include "crow-rpc/transport/socket_transport.h"
#include "dio_config.h"
#include "disk/block_disk.h"
#include "disk/disk_set.h"
#include "disk/mem_disk.h"
#include "disk/null_disk.h"
#include "engine/blocking/blocking_engine.h"
#include "rpc/dio_server.h"

#include <atomic>
#include <csignal>
#include <cstdio>
#include <thread>

#ifdef CROW_HAVE_LIBURING
#    include "engine/uring/uring_engine.h"
#endif

static std::atomic<bool> g_running{true};

static void on_signal(int)
{
    g_running.store(false);
}

// Auto-detect the IoEngine: try UringEngine first (Linux with liburing),
// fall back to BlockingEngine. Returns nullptr on error.
static std::shared_ptr<crow::diskio::IoEngine> create_engine(const crow::diskio::DioConfig &cfg)
{
    using namespace crow::diskio;
#ifdef CROW_HAVE_LIBURING
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
// dummy disks (NullDisk or MemDisk per config).
static std::shared_ptr<crow::diskio::DiskSet> build_disk_set(const crow::diskio::DioConfig          &cfg,
                                                             std::shared_ptr<crow::diskio::IoEngine> engine)
{
    using namespace crow::diskio;
    auto disk_set = std::make_shared<DiskSet>();
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
            auto disk = std::make_shared<BlockDisk>(entry.id, entry.path, engine, std::vector<Zone>(entry.zones),
                                                    cfg.o_direct);
            disk_set->add(disk);
        }
    }
    return disk_set;
}

int main(int argc, char *argv[])
{
    using namespace crow::diskio;

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
    crow::rpc::RpcServer server;
    if (!server.listen(cfg.bind_address, cfg.listen_port)) {
        std::fprintf(stderr, "error: failed to listen on %s:%d\n", cfg.bind_address.c_str(), cfg.listen_port);
        return 1;
    }
    int actual_port = server.listen_port();
    std::printf("crow-diskio listening on %s:%d (%zu disks)\n", cfg.bind_address.c_str(), actual_port,
                disk_set->size());
    std::fflush(stdout);

    auto *transport  = server.transport();
    auto  dio_server = std::make_unique<DiskioServer>(disk_set, transport);
    dio_server->register_handlers(server);

    server.start();

    // Run until signaled.
    while (g_running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    server.stop();
    disk_set->shutdown();

    // Stop the engine (BlockingEngine has a stop() method).
    if (auto *be = dynamic_cast<BlockingEngine *>(engine.get())) {
        be->stop();
    }

    return 0;
}
