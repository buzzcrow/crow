// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// crow-diskio: the disk I/O server binary.
//
// Parses config from CLI args, creates DiskSet + IoEngine, registers
// diskio RPC handlers, and serves until SIGTERM/SIGINT.
//
// Usage: crow-diskio --port <port> [--bind <addr>] [--engine ...]
//   [--disk <hex_id>:<path>[:<capacity>]]...

#include "crow-rpc/server/server.h"
#include "crow-rpc/transport/socket_transport.h"
#include "dio_config.h"
#include "disk/disk_set.h"
#include "disk/file_disk.h"
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

// Create the IoEngine based on config. Returns nullptr on error.
static std::shared_ptr<crow::diskio::IoEngine> create_engine(const crow::diskio::DioConfig &cfg)
{
    using namespace crow::diskio;
    EngineType type = cfg.engine;
    if (type == EngineType::Auto) {
#ifdef CROW_HAVE_LIBURING
        type = EngineType::Uring;
#else
        type = EngineType::Blocking;
#endif
    }

    switch (type) {
    case EngineType::Uring:
#ifdef CROW_HAVE_LIBURING
        return std::make_shared<UringEngine>(cfg.sq_entries);
#else
        std::fprintf(stderr, "error: uring engine not available (built without liburing)\n");
        return nullptr;
#endif
    case EngineType::Blocking:
        return std::make_shared<BlockingEngine>(cfg.thread_pool_size);
    case EngineType::Dummy:
        // DummyEngine is for testing; not wired in the binary.
        std::fprintf(stderr, "error: dummy engine not supported in production binary\n");
        return nullptr;
    case EngineType::Simulated:
        std::fprintf(stderr, "error: simulated engine not supported in production binary\n");
        return nullptr;
    default:
        return nullptr;
    }
}

// Build the DiskSet from config: open each disk file and add to the set.
static std::shared_ptr<crow::diskio::DiskSet> build_disk_set(const crow::diskio::DioConfig &cfg,
                                                             crow::diskio::IoEngine * /*engine*/)
{
    using namespace crow::diskio;
    auto disk_set = std::make_shared<DiskSet>();
    for (const auto &entry : cfg.disks) {
        // The binary uses FileDisk (regular files). BlockDisk requires
        // O_DIRECT block devices — not wired in the minimal binary.
        auto disk = std::make_shared<FileDisk>(entry.id, entry.path, nullptr, std::vector<Zone>(entry.zones));
        disk_set->add(disk);
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

    // Create the engine.
    auto engine = create_engine(cfg);
    if (engine == nullptr) {
        return 1;
    }

    // Build the disk set.
    auto disk_set = build_disk_set(cfg, engine.get());
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
    auto  dio_server = std::make_unique<DiskioServer>(disk_set, engine, transport);
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
