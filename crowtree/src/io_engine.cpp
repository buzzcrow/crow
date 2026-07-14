// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crowtree/io_engine.h"

#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>

namespace crowtree
{

static Status do_pread(int fd, void *buf, size_t len, off_t offset)
{
    size_t done = 0;
    while (done < len) {
        ssize_t n = ::pread(fd, static_cast<uint8_t *>(buf) + done, len - done, offset + static_cast<off_t>(done));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return Status::io_error(std::string("pread: ") + std::strerror(errno));
        }
        if (n == 0) {
            return Status::io_error("pread: unexpected EOF");
        }
        done += static_cast<size_t>(n);
    }
    return Status::Ok();
}

static Status do_pwrite(int fd, const void *buf, size_t len, off_t offset)
{
    size_t done = 0;
    while (done < len) {
        ssize_t n =
            ::pwrite(fd, static_cast<const uint8_t *>(buf) + done, len - done, offset + static_cast<off_t>(done));
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

static Status do_fsync(int fd)
{
#if defined(__APPLE__)
    if (::fsync(fd) < 0) {
        return Status::io_error(std::string("fsync: ") + std::strerror(errno));
    }
#else
    if (::fdatasync(fd) < 0) {
        return Status::io_error(std::string("fdatasync: ") + std::strerror(errno));
    }
#endif
    return Status::Ok();
}

void DirectIoEngine::submit_read(int fd, void *buf, size_t len, off_t offset, std::function<void(Status)> cb)
{
    cb(do_pread(fd, buf, len, offset));
}

void DirectIoEngine::submit_write(int fd, const void *buf, size_t len, off_t offset, std::function<void(Status)> cb)
{
    cb(do_pwrite(fd, buf, len, offset));
}

void DirectIoEngine::submit_fsync(int fd, std::function<void(Status)> cb)
{
    cb(do_fsync(fd));
}

} // namespace crowtree
