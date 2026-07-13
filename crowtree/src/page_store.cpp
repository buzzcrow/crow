// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crowtree/page_store.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#ifdef __APPLE__
#    include <sys/stat.h>
#endif

namespace crowtree
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

// ── FilePageStore ─────────────────────────────────────────────────

FilePageStore::~FilePageStore()
{
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

Status FilePageStore::open(const std::string &path, uint32_t iu_size, std::unique_ptr<FilePageStore> *out)
{
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        return Status::io_error(std::string("open: ") + std::strerror(errno));
    }
    out->reset(new FilePageStore(fd, iu_size == 0 ? 4096 : iu_size));
    return Status::Ok();
}

Status FilePageStore::write_at(uint64_t off, const uint8_t *buf, size_t len)
{
    size_t done = 0;
    while (done < len) {
        ssize_t n = ::pwrite(fd_, buf + done, len - done, static_cast<off_t>(off + done));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return Status::io_error(std::string("pwrite: ") + std::strerror(errno));
        }
        done += static_cast<size_t>(n);
    }
    return Status::Ok();
}

Status FilePageStore::read_at(uint64_t off, uint8_t *buf, size_t len) const
{
    size_t done = 0;
    while (done < len) {
        ssize_t n = ::pread(fd_, buf + done, len - done, static_cast<off_t>(off + done));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return Status::io_error(std::string("pread: ") + std::strerror(errno));
        }
        if (n == 0) {
            return Status::io_error("FilePageStore: read past end");
        }
        done += static_cast<size_t>(n);
    }
    return Status::Ok();
}

Status FilePageStore::sync()
{
#ifdef __APPLE__
    if (::fsync(fd_) < 0) {
        return Status::io_error(std::string("fsync: ") + std::strerror(errno));
    }
#else
    if (::fdatasync(fd_) < 0) {
        return Status::io_error(std::string("fdatasync: ") + std::strerror(errno));
    }
#endif
    return Status::Ok();
}

uint64_t FilePageStore::size() const
{
    off_t end = ::lseek(fd_, 0, SEEK_END);
    return end < 0 ? 0 : static_cast<uint64_t>(end);
}

} // namespace crowtree
