// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// DiskSet: holds the node's disk map (DiskId -> shared_ptr<Disk>).
// The RPC handler resolves disk_id to a disk handle via find_disk().
// Lock-free reads: find_disk() loads an atomic shared_ptr snapshot and
// does a hash lookup with no mutex. Writes (add/remove/shutdown) are
// copy-on-write: load → copy → modify → store. Writes are O(n) but rare
// (startup, group-0 sync); the hot path is lock-free.
#pragma once

#include "disk/atomic_shared_ptr.h"
#include "disk/disk.h"
#include "disk/types.h"

#include <memory>
#include <unordered_map>
#include <vector>

namespace crowdb::diskio
{

class DiskSet
{
  public:
    DiskSet() : disk_map_(std::make_shared<const DiskMap>())
    {
    }


    ~DiskSet();

    // Take ownership of a disk (added at startup or via group-0 sync).
    void add(std::shared_ptr<Disk> disk);

    // Remove a disk from the set. The shared_ptr is erased from the
    // map; in-flight IO holding a copy of the shared_ptr completes
    // safely. New requests for this disk_id will return DiskNotExist.
    // Returns true if the disk was found and removed.
    bool remove_disk(DiskId disk_id);

    // Resolve a disk by ID. Returns nullptr if not found. Lock-free.
    std::shared_ptr<Disk> find_disk(DiskId disk_id) const;

    // Close all disks and stop their engines.
    void shutdown();

    // Number of disks in the set.
    size_t size() const;

    // Return all disk IDs currently in the set.
    std::vector<DiskId> disk_ids() const;

  private:
    using DiskMap = std::unordered_map<DiskId, std::shared_ptr<Disk>, DiskIdHash>;

    // Copy-on-write snapshot. Readers load a shared_ptr and do a lock-free
    // lookup; writers copy, modify, and store. The old snapshot stays alive
    // (via refcount) for any in-flight reader during a swap.
    AtomicSharedPtr<const DiskMap> disk_map_;
};

} // namespace crowdb::diskio
