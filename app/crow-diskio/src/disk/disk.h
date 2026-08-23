// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Disk virtual base: per-disk handle. Each disk owns (or shares) an
// IoEngine instance via its subclass. Subclasses: BlockDisk (O_DIRECT
// block device), NullDisk (memfd, drop-write + pattern read), MemDisk
// (memfd, store + read-back).
#pragma once

#include "disk/types.h"
#include "engine/io_engine.h"

#include <memory>
#include <vector>

namespace crow::diskio
{

enum class DiskType {
    Block,
    Null,
    Mem,
};

class Disk
{
  public:
    virtual ~Disk() = default;

    virtual DiskType  type() const                   = 0;
    virtual int       fd() const                     = 0;
    virtual bool      is_o_direct() const            = 0;
    virtual size_t    block_size() const             = 0;
    virtual IoEngine *engine()                       = 0;
    virtual DiskId    id() const                     = 0;
    virtual Zone     *find_zone(uint32_t zone_index) = 0;

  protected:
    std::vector<Zone> zones_;
};

} // namespace crow::diskio
