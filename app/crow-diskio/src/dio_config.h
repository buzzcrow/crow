// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// DioConfig: configuration for the crow-diskio server.
// Parsed from CLI args; validated before startup.
#pragma once

#include "disk/types.h"

#include <cstdint>
#include <string>
#include <vector>

namespace crow::diskio
{

// Engine type selection.
enum class EngineType {
    Auto,      // uring on Linux+liburing, blocking otherwise
    Uring,     // io_uring (Linux only)
    Blocking,  // thread-pool pwrite/pread
    Dummy,     // in-memory drop-write
    Simulated, // fault-injection wrapper
};

// A disk entry from config: path + zone layout.
struct DiskEntry
{
    DiskId            id;
    std::string       path;
    std::vector<Zone> zones;
};

// Server configuration.
struct DioConfig
{
    // RPC listen.
    std::string bind_address = "127.0.0.1";
    int         listen_port  = 0;

    // Node identity.
    uint64_t node_id_high = 0;
    uint64_t node_id_low  = 0;

    // Engine.
    EngineType engine           = EngineType::Auto;
    uint32_t   thread_pool_size = 4;

    // Reactor / uring tuning.
    uint32_t sq_entries        = 256;
    uint32_t busy_poll_budget  = 16;
    uint32_t sq_thread_idle    = 1000; // ms
    uint32_t linked_timeout_ms = 30000;

    // O_DIRECT for block devices.
    bool o_direct = true;

    // Disk list.
    std::vector<DiskEntry> disks;

    // Parse CLI args. Returns true on success, false on error (msg in err).
    // Static so dio_main can call it without a pre-built config.
    static bool parse_args(int argc, char *argv[], DioConfig &out, std::string &err);

    // Validate the parsed config. Returns true on success.
    bool validate(std::string &err) const;
};

// Parse an engine type string ("auto", "uring", "blocking", "dummy", "simulated").
// Returns true on success.
bool parse_engine_type(const std::string &s, EngineType &out);

} // namespace crow::diskio
