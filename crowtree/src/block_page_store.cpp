// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crowtree/block_page_store.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#if defined(__linux__)
#    include <linux/fs.h>
#    include <sys/ioctl.h>
#endif

namespace crowtree
{

namespace
{
// O_DIRECT's buffer-address alignment requirement is filesystem/device
// specific but 4096 (a common page/sector size) satisfies virtually every
// real device; safe to over-align a bit beyond iu_size_ when iu_size_ is
// smaller (e.g. a caller-supplied 512 on a regular file).
constexpr size_t kDirectBufAlign = 4096;

size_t align_up(size_t v, size_t a)
{
    return (v + a - 1) / a * a;
}

// RAII aligned scratch buffer (posix_memalign), used for the bounce path
// when a caller's offset/length/buffer isn't already O_DIRECT-aligned.
class AlignedBuffer
{
  public:
    AlignedBuffer(size_t len, size_t align)
    {
        size_t a = align > kDirectBufAlign ? align : kDirectBufAlign;
        if (::posix_memalign(reinterpret_cast<void **>(&ptr_), a, len == 0 ? a : len) != 0) {
            ptr_ = nullptr;
        }
        len_ = ptr_ != nullptr ? len : 0;
    }

    ~AlignedBuffer()
    {
        std::free(ptr_);
    }

    AlignedBuffer(const AlignedBuffer &)            = delete;
    AlignedBuffer &operator=(const AlignedBuffer &) = delete;

    [[nodiscard]] uint8_t *data() const
    {
        return ptr_;
    }

    [[nodiscard]] bool ok() const
    {
        return ptr_ != nullptr;
    }

  private:
    uint8_t *ptr_ = nullptr;
    size_t   len_ = 0;
};
} // namespace

// ── MemoryMedium ───────────────────────────────────────────────────

Status MemoryMedium::pwrite_at(uint64_t off, const uint8_t *buf, size_t len)
{
    if (len == 0) {
        return Status::Ok();
    }
    std::lock_guard<std::mutex> lk(mu_);
    if (off + len > data_.size()) {
        data_.resize(off + len, 0);
    }
    std::memcpy(data_.data() + off, buf, len);
    return Status::Ok();
}

Status MemoryMedium::pread_partial(uint64_t off, uint8_t *buf, size_t len, size_t *out_read) const
{
    std::lock_guard<std::mutex> lk(mu_);
    if (off >= data_.size()) {
        *out_read = 0;
        return Status::Ok();
    }
    size_t avail = data_.size() - off;
    size_t n     = len < avail ? len : avail;
    std::memcpy(buf, data_.data() + off, n);
    *out_read = n;
    return Status::Ok();
}

Status MemoryMedium::fsync()
{
    return Status::Ok();
}

uint64_t MemoryMedium::size() const
{
    std::lock_guard<std::mutex> lk(mu_);
    return data_.size();
}

// ── FileMedium ─────────────────────────────────────────────────────

FileMedium::~FileMedium()
{
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

Status FileMedium::open(const std::string &path, bool o_direct, std::unique_ptr<FileMedium> *out)
{
#if defined(__APPLE__)
    (void)o_direct; // macOS has no O_DIRECT; use F_NOCACHE instead
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        return Status::io_error(std::string("open: ") + std::strerror(errno));
    }
    if (::fcntl(fd, F_NOCACHE, 1) < 0) {
        ::close(fd);
        return Status::io_error(std::string("fcntl(F_NOCACHE): ") + std::strerror(errno));
    }
#else
    int flags = O_RDWR | O_CREAT;
    if (o_direct) {
        flags |= O_DIRECT;
    }
    int fd = ::open(path.c_str(), flags, 0644);
    if (fd < 0) {
        return Status::io_error(std::string("open: ") + std::strerror(errno));
    }
#endif
    out->reset(new FileMedium(fd));
    return Status::Ok();
}

Status FileMedium::pwrite_at(uint64_t off, const uint8_t *buf, size_t len)
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

Status FileMedium::pread_partial(uint64_t off, uint8_t *buf, size_t len, size_t *out_read) const
{
    size_t done = 0;
    while (done < len) {
        ssize_t n = ::pread(fd_, buf + done, len - done, static_cast<off_t>(off + done));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            *out_read = done;
            return Status::io_error(std::string("pread: ") + std::strerror(errno));
        }
        if (n == 0) {
            break; // EOF: not an error here, caller decides how to treat the shortfall
        }
        done += static_cast<size_t>(n);
    }
    *out_read = done;
    return Status::Ok();
}

Status FileMedium::fsync()
{
#if defined(__APPLE__)
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

uint64_t FileMedium::size() const
{
    off_t end = ::lseek(fd_, 0, SEEK_END);
    return end < 0 ? 0 : static_cast<uint64_t>(end);
}

// ── BlockPageStore ─────────────────────────────────────────────────

BlockPageStore::~BlockPageStore() = default;

int BlockPageStore::fd() const
{
    auto *fm = dynamic_cast<FileMedium *>(medium_.get());
    return fm ? fm->fd() : -1;
}

Status BlockPageStore::open(const std::string &path, uint32_t iu_size, std::unique_ptr<BlockPageStore> *out)
{
    std::unique_ptr<FileMedium> medium;
    Status                       s = FileMedium::open(path, true, &medium);
    if (!s.ok()) {
        return s;
    }

    struct stat st{};
    if (::fstat(medium->fd(), &st) < 0) {
        return Status::io_error(std::string("fstat: ") + std::strerror(errno));
    }
    bool     is_block     = S_ISBLK(st.st_mode);
    uint32_t effective_iu = iu_size == 0 ? 4096 : iu_size;
    uint64_t probed_bytes = 0;

#if defined(__linux__)
    if (is_block) {
        uint64_t bytes = 0;
        if (::ioctl(medium->fd(), BLKGETSIZE64, &bytes) == 0) {
            probed_bytes = bytes;
        }
        uint32_t sector_size = 0;
        if (::ioctl(medium->fd(), BLKSSZGET, &sector_size) == 0 && sector_size > 0) {
            effective_iu = sector_size;
        }
    }
#endif

    auto *store      = new BlockPageStore(std::move(medium), effective_iu, is_block);
    store->capacity_ = probed_bytes;
    out->reset(store);
    return Status::Ok();
}

Status BlockPageStore::open_mem(uint32_t iu_size, std::unique_ptr<BlockPageStore> *out)
{
    auto medium = std::make_unique<MemoryMedium>();
    uint32_t effective_iu = iu_size == 0 ? 1 : iu_size;
    out->reset(new BlockPageStore(std::move(medium), effective_iu, false));
    return Status::Ok();
}

Status BlockPageStore::write_at(uint64_t off, const uint8_t *buf, size_t len)
{
    if (len == 0) {
        return Status::Ok();
    }

    // IU=1 (byte-aligned): no alignment checks, no bounce buffer.
    if (iu_size_ <= 1) {
        return medium_->pwrite_at(off, buf, len);
    }

    bool aligned =
        (off % iu_size_ == 0) && (len % iu_size_ == 0) && (reinterpret_cast<uintptr_t>(buf) % kDirectBufAlign == 0);
    if (aligned) {
        return medium_->pwrite_at(off, buf, len);
    }

    // Bounce through an IU-aligned scratch span covering [off, off+len):
    // read-modify-write, since O_DIRECT can't write a sub-IU or misaligned
    // range directly.
    uint64_t      start = (off / iu_size_) * iu_size_;
    uint64_t      end   = align_up(off + len, iu_size_);
    size_t        span  = end - start;
    AlignedBuffer scratch(span, iu_size_);
    if (!scratch.ok()) {
        return Status::io_error("BlockPageStore: aligned scratch allocation failed");
    }
    size_t got = 0;
    Status rs  = medium_->pread_partial(start, scratch.data(), span, &got);
    if (!rs.ok()) {
        return rs;
    }
    if (got < span) {
        // Genuinely new territory past the current end (growing the
        // store) rather than a read failure -- zero-fill the unread tail.
        std::memset(scratch.data() + got, 0, span - got);
    }
    std::memcpy(scratch.data() + (off - start), buf, len);
    return medium_->pwrite_at(start, scratch.data(), span);
}

Status BlockPageStore::read_at(uint64_t off, uint8_t *buf, size_t len) const
{
    if (len == 0) {
        return Status::Ok();
    }

    // IU=1 (byte-aligned): no alignment checks, no bounce buffer.
    if (iu_size_ <= 1) {
        return medium_->pread_at(off, buf, len);
    }

    bool aligned =
        (off % iu_size_ == 0) && (len % iu_size_ == 0) && (reinterpret_cast<uintptr_t>(buf) % kDirectBufAlign == 0);
    if (aligned) {
        return medium_->pread_at(off, buf, len);
    }

    uint64_t      start = (off / iu_size_) * iu_size_;
    uint64_t      end   = align_up(off + len, iu_size_);
    size_t        span  = end - start;
    AlignedBuffer scratch(span, iu_size_);
    if (!scratch.ok()) {
        return Status::io_error("BlockPageStore: aligned scratch allocation failed");
    }
    size_t got = 0;
    Status rs  = medium_->pread_partial(start, scratch.data(), span, &got);
    if (!rs.ok()) {
        return rs;
    }
    if (got < (off - start) + len) {
        return Status::io_error("BlockPageStore: read past end");
    }
    std::memcpy(buf, scratch.data() + (off - start), len);
    return Status::Ok();
}

Status BlockPageStore::sync()
{
    return medium_->fsync();
}

uint64_t BlockPageStore::size() const
{
    if (is_block_device_ && capacity_ > 0) {
        return capacity_;
    }
    return medium_->size();
}

} // namespace crowtree
