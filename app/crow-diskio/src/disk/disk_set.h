// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// DiskSet: holds the node's disk map (DiskId -> shared_ptr<Disk>).
// The RPC handler resolves disk_id to a disk handle via find_disk().
#pragma once

#include "disk/disk.h"
#include "disk/types.h"

#include <memory>
#include <unordered_map>

namespace crow::diskio
{

class DiskSet
{
  public:
    DiskSet() = default;
    ~DiskSet();

    // Take ownership of a disk (added at startup).
    void add(std::shared_ptr<Disk> disk);

    // Resolve a disk by ID. Returns nullptr if not found.
    std::shared_ptr<Disk> find_disk(DiskId disk_id) const;

    // Close all disks and stop their engines.
    void shutdown();

    // Number of disks in the set.
    size_t size() const
    {
        return disk_map_.size();
    }

  private:
    std::unordered_map<DiskId, std::shared_ptr<Disk>, DiskIdHash> disk_map_;
};

} // namespace crow::diskio
