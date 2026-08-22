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
    if (!disk) {
        return;
    }
    auto current        = disk_map_.load(std::memory_order_acquire);
    auto next           = std::make_shared<DiskMap>(*current);
    (*next)[disk->id()] = std::move(disk);
    disk_map_.store(std::move(next), std::memory_order_release);
}

bool DiskSet::remove_disk(DiskId disk_id)
{
    auto current = disk_map_.load(std::memory_order_acquire);
    if (current->find(disk_id) == current->end()) {
        return false;
    }
    auto next    = std::make_shared<DiskMap>(*current);
    bool removed = next->erase(disk_id) > 0;
    disk_map_.store(std::move(next), std::memory_order_release);
    return removed;
}

std::shared_ptr<Disk> DiskSet::find_disk(DiskId disk_id) const
{
    auto map = disk_map_.load(std::memory_order_acquire);
    auto it  = map->find(disk_id);
    if (it == map->end()) {
        return nullptr;
    }
    return it->second;
}

void DiskSet::shutdown()
{
    disk_map_.store(std::make_shared<const DiskMap>(), std::memory_order_release);
}

size_t DiskSet::size() const
{
    auto map = disk_map_.load(std::memory_order_acquire);
    return map->size();
}

std::vector<DiskId> DiskSet::disk_ids() const
{
    auto                map = disk_map_.load(std::memory_order_acquire);
    std::vector<DiskId> ids;
    ids.reserve(map->size());
    for (const auto &entry : *map) {
        ids.push_back(entry.first);
    }
    return ids;
}

} // namespace crow::diskio
