// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "disk/file_disk.h"

#include <fcntl.h>
#include <unistd.h>

namespace crow::diskio
{

FileDisk::FileDisk(DiskId id, const std::string &path, std::shared_ptr<IoEngine> engine, std::vector<Zone> zones)
    : id_(id),
      fd_(::open(path.c_str(), O_RDWR)),
      engine_(std::move(engine))
{
    zones_ = std::move(zones);
}

FileDisk::~FileDisk()
{
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

Zone *FileDisk::find_zone(uint32_t zone_index)
{
    for (auto &z : zones_) {
        if (z.zone_index == zone_index) {
            return &z;
        }
    }
    return nullptr;
}

} // namespace crow::diskio
