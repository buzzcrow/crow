// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// DiskProperties: optional fault-injection parameters for dummy disks
// (NullDisk, MemDisk). When set, the dummy disk's wrapper engine injects
// per-I/O random latency and errors.
#pragma once

#include <cstdint>

namespace crow::diskio
{

struct DiskProperties
{
    uint32_t latency_min_ms = 0;
    uint32_t latency_max_ms = 0;
    double   error_rate     = 0.0; // 0.0 = no errors, 1.0 = all errors

    bool has_fault_injection() const
    {
        return latency_max_ms > 0 || error_rate > 0.0;
    }
};

} // namespace crow::diskio
