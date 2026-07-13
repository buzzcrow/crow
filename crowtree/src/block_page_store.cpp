#include "crowtree/block_page_store.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
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

BlockPageStore::~BlockPageStore()
{
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

Status BlockPageStore::open(const std::string &path, uint32_t iu_size, std::unique_ptr<BlockPageStore> *out)
{
#if defined(__APPLE__)
    // macOS has no O_DIRECT open flag; F_NOCACHE after open is the
    // equivalent "bypass the page cache" request.
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        return Status::io_error(std::string("open: ") + std::strerror(errno));
    }
    if (::fcntl(fd, F_NOCACHE, 1) < 0) {
        ::close(fd);
        return Status::io_error(std::string("fcntl(F_NOCACHE): ") + std::strerror(errno));
    }
#else
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0644);
    if (fd < 0) {
        return Status::io_error(std::string("open(O_DIRECT): ") + std::strerror(errno));
    }
#endif

    struct stat st{};
    if (::fstat(fd, &st) < 0) {
        ::close(fd);
        return Status::io_error(std::string("fstat: ") + std::strerror(errno));
    }
    bool     is_block     = S_ISBLK(st.st_mode);
    uint32_t effective_iu = iu_size == 0 ? 4096 : iu_size;
    uint64_t probed_bytes = 0;

#if defined(__linux__)
    if (is_block) {
        uint64_t bytes = 0;
        if (::ioctl(fd, BLKGETSIZE64, &bytes) == 0) {
            probed_bytes = bytes;
        }
        uint32_t sector_size = 0;
        if (::ioctl(fd, BLKSSZGET, &sector_size) == 0 && sector_size > 0) {
            effective_iu = sector_size;
        }
    }
#endif

    auto *store      = new BlockPageStore(fd, effective_iu, is_block);
    store->capacity_ = probed_bytes;
    out->reset(store);
    return Status::Ok();
}

Status BlockPageStore::raw_pwrite(uint64_t off, const uint8_t *buf, size_t len) const
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

Status BlockPageStore::raw_pread_partial(uint64_t off, uint8_t *buf, size_t len, size_t *out_read) const
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

Status BlockPageStore::write_at(uint64_t off, const uint8_t *buf, size_t len)
{
    if (len == 0) {
        return Status::Ok();
    }
    bool aligned =
        (off % iu_size_ == 0) && (len % iu_size_ == 0) && (reinterpret_cast<uintptr_t>(buf) % kDirectBufAlign == 0);
    if (aligned) {
        return raw_pwrite(off, buf, len);
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
    Status rs  = raw_pread_partial(start, scratch.data(), span, &got);
    if (!rs.ok()) {
        return rs;
    }
    if (got < span) {
        // Genuinely new territory past the current end (growing the
        // store) rather than a read failure -- zero-fill the unread tail.
        std::memset(scratch.data() + got, 0, span - got);
    }
    std::memcpy(scratch.data() + (off - start), buf, len);
    return raw_pwrite(start, scratch.data(), span);
}

Status BlockPageStore::read_at(uint64_t off, uint8_t *buf, size_t len) const
{
    if (len == 0) {
        return Status::Ok();
    }
    bool aligned =
        (off % iu_size_ == 0) && (len % iu_size_ == 0) && (reinterpret_cast<uintptr_t>(buf) % kDirectBufAlign == 0);
    if (aligned) {
        size_t got = 0;
        Status rs  = raw_pread_partial(off, buf, len, &got);
        if (!rs.ok()) {
            return rs;
        }
        if (got < len) {
            return Status::io_error("BlockPageStore: read past end");
        }
        return Status::Ok();
    }

    uint64_t      start = (off / iu_size_) * iu_size_;
    uint64_t      end   = align_up(off + len, iu_size_);
    size_t        span  = end - start;
    AlignedBuffer scratch(span, iu_size_);
    if (!scratch.ok()) {
        return Status::io_error("BlockPageStore: aligned scratch allocation failed");
    }
    size_t got = 0;
    Status rs  = raw_pread_partial(start, scratch.data(), span, &got);
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

uint64_t BlockPageStore::size() const
{
    if (is_block_device_ && capacity_ > 0) {
        return capacity_;
    }
    off_t end = ::lseek(fd_, 0, SEEK_END);
    return end < 0 ? 0 : static_cast<uint64_t>(end);
}

} // namespace crowtree
