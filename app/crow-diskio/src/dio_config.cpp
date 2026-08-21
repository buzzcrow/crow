// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "dio_config.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace crow::diskio
{

bool parse_engine_type(const std::string &s, EngineType &out)
{
    if (s == "auto") {
        out = EngineType::Auto;
    }
    else if (s == "uring") {
        out = EngineType::Uring;
    }
    else if (s == "blocking") {
        out = EngineType::Blocking;
    }
    else if (s == "dummy") {
        out = EngineType::Dummy;
    }
    else if (s == "simulated") {
        out = EngineType::Simulated;
    }
    else {
        return false;
    }
    return true;
}

// Parse a hex DiskId from "high:low" or a single hex value (low only).
static bool parse_disk_id(const char *s, DiskId &out)
{
    out.high          = 0;
    out.low           = 0;
    const char *colon = std::strchr(s, ':');
    if (colon != nullptr) {
        char *end = nullptr;
        out.high  = std::strtoull(s, &end, 16);
        if (end != colon) {
            return false;
        }
        out.low = std::strtoull(colon + 1, &end, 16);
        if (*end != '\0') {
            return false;
        }
    }
    else {
        char *end = nullptr;
        out.low   = std::strtoull(s, &end, 16);
        if (*end != '\0') {
            return false;
        }
    }
    return true;
}

// Parse a u32 from argv.
static bool parse_u32(const char *s, uint32_t &out)
{
    char *end = nullptr;
    long  v   = std::strtol(s, &end, 10);
    if (end == s || *end != '\0' || v < 0 || v > 0xFFFFFFFF) {
        return false;
    }
    out = static_cast<uint32_t>(v);
    return true;
}

// Parse a u64 from argv.
static bool parse_u64(const char *s, uint64_t &out)
{
    char              *end = nullptr;
    unsigned long long v   = std::strtoull(s, &end, 10);
    if (end == s || *end != '\0') {
        return false;
    }
    out = static_cast<uint64_t>(v);
    return true;
}

bool DioConfig::parse_args(int argc, char *argv[], DioConfig &out, std::string &err)
{
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--bind" && i + 1 < argc) {
            out.bind_address = argv[++i];
        }
        else if (arg == "--port" && i + 1 < argc) {
            uint32_t p;
            if (!parse_u32(argv[++i], p)) {
                err = "invalid --port value";
                return false;
            }
            out.listen_port = static_cast<int>(p);
        }
        else if (arg == "--node-id" && i + 1 < argc) {
            if (!parse_u64(argv[++i], out.node_id_low)) {
                err = "invalid --node-id value";
                return false;
            }
        }
        else if (arg == "--engine" && i + 1 < argc) {
            if (!parse_engine_type(argv[++i], out.engine)) {
                err = "invalid --engine value (auto|uring|blocking|dummy|simulated)";
                return false;
            }
        }
        else if (arg == "--threads" && i + 1 < argc) {
            if (!parse_u32(argv[++i], out.thread_pool_size)) {
                err = "invalid --threads value";
                return false;
            }
        }
        else if (arg == "--sq-entries" && i + 1 < argc) {
            if (!parse_u32(argv[++i], out.sq_entries)) {
                err = "invalid --sq-entries value";
                return false;
            }
        }
        else if (arg == "--no-o-direct") {
            out.o_direct = false;
        }
        else if (arg == "--disk" && i + 1 < argc) {
            // Format: --disk <hex_id>:<path>[:<zone_capacity>]
            // Multiple --disk args allowed.
            std::string spec = argv[++i];
            // Split on ':' — id:path[:capacity]
            size_t first_colon = spec.find(':');
            if (first_colon == std::string::npos) {
                err = "--disk expects <hex_id>:<path>[:<capacity>]";
                return false;
            }
            std::string id_str = spec.substr(0, first_colon);
            std::string rest   = spec.substr(first_colon + 1);

            DiskEntry entry;
            if (!parse_disk_id(id_str.c_str(), entry.id)) {
                err = "invalid disk id in --disk";
                return false;
            }

            // rest = path[:capacity]
            size_t path_colon = rest.find(':');
            if (path_colon == std::string::npos) {
                entry.path = rest;
                Zone z;
                z.zone_index  = 0;
                z.base_offset = 0;
                z.capacity    = 1LL << 40; // default 1 TiB
                entry.zones.push_back(z);
            }
            else {
                entry.path          = rest.substr(0, path_colon);
                std::string cap_str = rest.substr(path_colon + 1);
                char       *end     = nullptr;
                int64_t     cap     = std::strtoll(cap_str.c_str(), &end, 10);
                if (*end != '\0' || cap <= 0) {
                    err = "invalid zone capacity in --disk";
                    return false;
                }
                Zone z;
                z.zone_index  = 0;
                z.base_offset = 0;
                z.capacity    = cap;
                entry.zones.push_back(z);
            }
            out.disks.push_back(std::move(entry));
        }
        else if (arg == "--help" || arg == "-h") {
            std::printf("usage: crow-diskio --port <port> [--bind <addr>] "
                        "[--engine auto|uring|blocking|dummy|simulated] "
                        "[--threads N] [--sq-entries N] [--no-o-direct] "
                        "[--disk <hex_id>:<path>[:<capacity>]]...\n");
            std::exit(0);
        }
        else {
            err = "unknown argument: " + arg;
            return false;
        }
    }
    return true;
}

bool DioConfig::validate(std::string &err) const
{
    if (listen_port < 0 || listen_port > 65535) {
        err = "invalid listen port";
        return false;
    }
    if (thread_pool_size == 0) {
        err = "thread_pool_size must be > 0";
        return false;
    }
    if (sq_entries == 0) {
        err = "sq_entries must be > 0";
        return false;
    }
    for (const auto &d : disks) {
        if (d.path.empty()) {
            err = "disk path is empty";
            return false;
        }
        if (d.id.is_zero()) {
            err = "disk id is zero";
            return false;
        }
        if (d.zones.empty()) {
            err = "disk has no zones";
            return false;
        }
    }
    return true;
}

} // namespace crow::diskio
