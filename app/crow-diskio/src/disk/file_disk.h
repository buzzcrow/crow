// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// FileDisk: opens a regular file with O_RDWR (no O_DIRECT by default).
// Cross-platform. block_size() = 1 (no alignment requirement).
#pragma once

#include "disk/disk.h"
#include "disk/types.h"

#include <memory>
#include <string>
#include <vector>

namespace crow::diskio
{

class FileDisk : public Disk
{
  public:
    FileDisk(DiskId id, const std::string &path, std::unique_ptr<IoEngine> engine, std::vector<Zone> zones);
    ~FileDisk() override;

    DiskType type() const override
    {
        return DiskType::File;
    }

    int fd() const override
    {
        return fd_;
    }

    bool is_o_direct() const override
    {
        return false;
    }

    size_t block_size() const override
    {
        return 1;
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
    std::unique_ptr<IoEngine> engine_;
};

} // namespace crow::diskio
