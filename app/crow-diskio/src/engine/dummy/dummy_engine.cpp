// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "engine/dummy/dummy_engine.h"

#include "disk/mem_disk.h"

namespace crow::diskio
{

void DummyEngine::submit_write(Disk * /*disk*/, off_t /*phys_offset*/, const uint8_t * /*data*/, size_t size,
                               std::function<void(int)> on_complete)
{
    // Drop-write: immediate success.
    if (on_complete) {
        on_complete(static_cast<int>(size));
    }
}

void DummyEngine::submit_read(Disk *disk, off_t phys_offset, uint8_t *buf, size_t size,
                              std::function<void(int)> on_complete)
{
    if (disk == nullptr) {
        if (on_complete) {
            on_complete(-EBADF);
        }
        return;
    }
    auto *mem = static_cast<MemDisk *>(disk);
    int   ret = mem->read(phys_offset, buf, size, logical_offset_);
    if (on_complete) {
        on_complete(ret);
    }
}

void DummyEngine::submit_fsync(Disk * /*disk*/, std::function<void(int)> on_complete)
{
    // No-op: MemDisk has no durability requirement.
    if (on_complete) {
        on_complete(0);
    }
}

} // namespace crow::diskio
