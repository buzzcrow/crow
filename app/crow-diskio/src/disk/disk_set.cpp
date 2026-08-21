// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "disk/disk_set.h"

namespace crow::diskio
{

DiskSet::~DiskSet()
{
    shutdown();
}

void DiskSet::add(std::shared_ptr<Disk> disk)
{
    if (disk) {
        disk_map_[disk->id()] = std::move(disk);
    }
}

std::shared_ptr<Disk> DiskSet::find_disk(DiskId disk_id) const
{
    auto it = disk_map_.find(disk_id);
    if (it == disk_map_.end()) {
        return nullptr;
    }
    return it->second;
}

void DiskSet::shutdown()
{
    disk_map_.clear();
}

} // namespace crow::diskio
