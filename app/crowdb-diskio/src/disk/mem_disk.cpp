// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "disk/mem_disk.h"

#include "engine/dummy/dummy_engine.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace crowdb::diskio
{

namespace
{
int create_memfd(int64_t capacity)
{
#ifdef __linux__
    int fd = ::memfd_create("crowdb-mem-disk", 0);
    if (fd < 0) {
        return -1;
    }
    if (::ftruncate(fd, capacity) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
#else
    (void)capacity;
    return -1;
#endif
}
} // namespace

MemDisk::MemDisk(DiskId id, std::shared_ptr<IoEngine> engine, std::vector<Zone> zones,
                 std::optional<DiskProperties> props)
    : id_(id),
      fd_(create_memfd(zones.empty() ? 4096 : zones[0].capacity))
{
    if (props.has_value() && props->has_fault_injection()) {
        // Wrap with fault injection (no read-content hack — MemDisk
        // returns actual stored data).
        engine_ = std::make_shared<DummyDiskEngine>(std::move(engine), false, props);
    }
    else {
        engine_ = std::move(engine);
    }
    zones_ = std::move(zones);
}

MemDisk::~MemDisk()
{
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

Zone *MemDisk::find_zone(uint32_t zone_index)
{
    for (auto &z : zones_) {
        if (z.zone_index == zone_index) {
            return &z;
        }
    }
    return nullptr;
}

} // namespace crowdb::diskio
