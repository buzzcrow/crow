// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "disk/null_disk.h"

#include "engine/dummy/dummy_engine.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace crow::diskio
{

namespace
{
// Create a memfd and ftruncate it to a small size. The actual I/O
// goes through the kernel (pwrite/pread on tmpfs), exercising the
// full uring/blocking path. Content is discarded — NullDisk reads
// return pattern data via the wrapper engine, not the memfd content.
int create_memfd(int64_t capacity)
{
#ifdef __linux__
    int fd = ::memfd_create("crow-null-disk", 0);
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

NullDisk::NullDisk(DiskId id, std::shared_ptr<IoEngine> engine, std::vector<Zone> zones,
                   std::optional<DiskProperties> props)
    : id_(id),
      fd_(create_memfd(zones.empty() ? 4096 : zones[0].capacity))
{
    // Wrap the shared engine with read-content hack + optional fault injection.
    wrapper_ = std::make_shared<DummyDiskEngine>(std::move(engine), true, props);
    zones_   = std::move(zones);
}

NullDisk::~NullDisk()
{
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

Zone *NullDisk::find_zone(uint32_t zone_index)
{
    for (auto &z : zones_) {
        if (z.zone_index == zone_index) {
            return &z;
        }
    }
    return nullptr;
}

} // namespace crow::diskio
