// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crowtree/async_page_store.h"
#include "crowtree/reactor.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <utility>

namespace crowtree
{

namespace
{
// Maps a Reactor callback's raw CQE `res` (>=0 bytes transferred, <0
// -errno) to a Status, treating a short read/write (res >= 0 but < the
// requested `len`) as an io_error too -- Phase 1 is a single-shot op per
// submit_read/write call, unlike the synchronous stores' internal
// retry-until-full-length loop (page_store.cpp's FilePageStore::read_at/
// write_at); a later phase can add that retry if real usage needs it.
Status result_to_status(int res, size_t len, const char *op)
{
    if (res < 0) {
        return Status::io_error(std::string(op) + ": " + std::strerror(-res));
    }
    if (static_cast<size_t>(res) < len) {
        return Status::io_error(std::string("short ") + op + " (" + std::to_string(res) + " of " + std::to_string(len) +
                                " bytes)");
    }
    return Status::Ok();
}
} // namespace

FileAsyncPageStore::~FileAsyncPageStore()
{
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

Status FileAsyncPageStore::open(const std::string &path, uint32_t iu_size, Reactor *reactor,
                                std::unique_ptr<FileAsyncPageStore> *out)
{
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        return Status::io_error(std::string("open: ") + std::strerror(errno));
    }
    out->reset(new FileAsyncPageStore(fd, iu_size == 0 ? 4096 : iu_size, reactor));
    return Status::Ok();
}

uint64_t FileAsyncPageStore::submit_read(PageAddr addr, void *buf, size_t len, std::function<void(Status)> on_complete)
{
    return reactor_->submit_read(fd_, buf, len, static_cast<off_t>(addr), [len, cb = std::move(on_complete)](int res) {
        if (cb) {
            cb(result_to_status(res, len, "read"));
        }
    });
}

uint64_t FileAsyncPageStore::submit_write(PageAddr addr, const void *buf, size_t len,
                                          std::function<void(Status)> on_complete)
{
    return reactor_->submit_write(fd_, buf, len, static_cast<off_t>(addr), [len, cb = std::move(on_complete)](int res) {
        if (cb) {
            cb(result_to_status(res, len, "write"));
        }
    });
}

Status FileAsyncPageStore::submit_fsync(std::function<void(Status)> on_complete)
{
    if (fd_ < 0) {
        return Status::invalid_argument("FileAsyncPageStore: no backing fd");
    }
    reactor_->submit_fsync(fd_, [cb = std::move(on_complete)](int res) {
        if (cb) {
            cb(res < 0 ? Status::io_error(std::string("fsync: ") + std::strerror(-res)) : Status::Ok());
        }
    });
    return Status::Ok();
}

void FileAsyncPageStore::cancel(uint64_t op_id)
{
    reactor_->cancel(op_id);
}

} // namespace crowtree
