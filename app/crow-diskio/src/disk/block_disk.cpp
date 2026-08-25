// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "disk/block_disk.h"

#include <fcntl.h>
#include <sys/ioctl.h>
#include <unistd.h>

#ifdef __linux__
#    include <linux/fs.h>
#endif

namespace crow::diskio
{

namespace
{
size_t query_block_size(int fd)
{
#ifdef __linux__
    int blk_size = 0;
    if (ioctl(fd, BLKSSZGET, &blk_size) == 0 && blk_size > 0) {
        return static_cast<size_t>(blk_size);
    }
#else
    (void)fd;
#endif
    return 512; // default
}
} // namespace

BlockDisk::BlockDisk(DiskId id, const std::string &path, std::shared_ptr<IoEngine> engine, std::vector<Zone> zones,
                     bool o_direct)
    : id_(id),
      o_direct_(o_direct),
      engine_(std::move(engine))
{
    int flags = O_RDWR;
    if (o_direct) {
#ifdef O_DIRECT
        flags |= O_DIRECT;
#endif
    }
    fd_         = ::open(path.c_str(), flags);
    block_size_ = (fd_ >= 0) ? query_block_size(fd_) : 512;
    zones_      = std::move(zones);
}

BlockDisk::~BlockDisk()
{
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

Zone *BlockDisk::find_zone(uint32_t zone_index)
{
    for (auto &z : zones_) {
        if (z.zone_index == zone_index) {
            return &z;
        }
    }
    return nullptr;
}

} // namespace crow::diskio
