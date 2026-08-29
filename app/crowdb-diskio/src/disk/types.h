// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Core types for the diskio server: DiskId, Zone, IoRetCode.
#pragma once

#include <cstdint>
#include <cstring>
#include <functional>
#include <string>

namespace crowdb::diskio
{

// 128-bit disk identifier (matches crowdb-protocol's DiskId proto).
struct DiskId
{
    uint64_t high = 0;
    uint64_t low  = 0;

    bool operator==(const DiskId &o) const
    {
        return high == o.high && low == o.low;
    }

    bool operator!=(const DiskId &o) const
    {
        return !(*this == o);
    }

    bool is_zero() const
    {
        return high == 0 && low == 0;
    }

    std::string to_hex() const
    {
        char buf[33];
        std::snprintf(buf, sizeof(buf), "%016lx%016lx", static_cast<unsigned long>(high),
                      static_cast<unsigned long>(low));
        return buf;
    }
};

// DiskId hash for use in unordered_map.
struct DiskIdHash
{
    size_t operator()(const DiskId &id) const
    {
        return std::hash<uint64_t>{}(id.high) ^ (std::hash<uint64_t>{}(id.low) << 1);
    }
};

// Zone record: a contiguous region on a disk.
struct Zone
{
    uint32_t zone_index  = 0;
    off_t    base_offset = 0; // physical offset of zone start on disk
    int64_t  capacity    = 0;
    // state is not tracked by diskio (see design doc §3.4)
};

// Return codes for I/O operations (mirrors FBDiskIoRetCode in diskio.fbs).
enum class IoRetCode : int16_t {
    Success          = 0,
    DiskNotExist     = 1,
    ZoneNotExist     = 2,
    IoError          = 3,
    PartialWrite     = 4,
    InvalidAlignment = 5,
    ConnectionError  = 6,
};

} // namespace crowdb::diskio
