// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// BlockDisk: opens a Linux block device with O_DIRECT | O_RDWR.
// block_size() = logical block size (from BLKSSZGET ioctl, or default 512).
// Linux-only.
#pragma once

#include "disk/disk.h"
#include "disk/types.h"

#include <memory>
#include <string>
#include <vector>

namespace crow::diskio
{

class BlockDisk : public Disk
{
  public:
    BlockDisk(DiskId id, const std::string &path, std::unique_ptr<IoEngine> engine, std::vector<Zone> zones,
              bool o_direct);
    ~BlockDisk() override;

    DiskType type() const override
    {
        return DiskType::Block;
    }

    int fd() const override
    {
        return fd_;
    }

    bool is_o_direct() const override
    {
        return o_direct_;
    }

    size_t block_size() const override
    {
        return block_size_;
    }

    IoEngine *engine() override
    {
        return engine_.get();
    }

    DiskId id() const override
    {
        return id_;
    }

    Zone *find_zone(uint32_t zone_index) override;

  private:
    DiskId                    id_;
    int                       fd_;
    bool                      o_direct_;
    size_t                    block_size_;
    std::unique_ptr<IoEngine> engine_;
};

} // namespace crow::diskio
