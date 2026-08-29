// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/page_store.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#ifdef __APPLE__
#    include <sys/stat.h>
#endif

namespace crowdb::tree
{

// ── MemPageStore ──────────────────────────────────────────────────

Status MemPageStore::write_at(uint64_t off, const uint8_t *buf, size_t len)
{
    std::lock_guard<std::mutex> lk(mu_);
    if (off + len > data_.size()) {
        data_.resize(off + len, 0);
    }
    std::memcpy(data_.data() + off, buf, len);
    return Status::Ok();
}

Status MemPageStore::read_at(uint64_t off, uint8_t *buf, size_t len) const
{
    std::lock_guard<std::mutex> lk(mu_);
    if (off + len > data_.size()) {
        return Status::io_error("MemPageStore: read past end");
    }
    std::memcpy(buf, data_.data() + off, len);
    return Status::Ok();
}

uint64_t MemPageStore::size() const
{
    std::lock_guard<std::mutex> lk(mu_);
    return data_.size();
}

} // namespace crowdb::tree
