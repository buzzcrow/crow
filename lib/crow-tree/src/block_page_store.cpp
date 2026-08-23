// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-tree/block_page_store.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <sstream>

#if defined(__linux__)
#    include <linux/fs.h>
#    include <sys/ioctl.h>
#endif

namespace crow::tree
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
    Status                      s = FileMedium::open(path, true, &medium);
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
    auto     medium       = std::make_unique<MemoryMedium>();
    uint32_t effective_iu = iu_size == 0 ? 1 : iu_size;
    out->reset(new BlockPageStore(std::move(medium), effective_iu, false));
    return Status::Ok();
}

namespace
{
// Parse a block filename like "1-3.blk-0007" → (store_id=1, group_id=3, idx=7).
// Returns false if the filename doesn't match the pattern.
bool parse_block_filename(const std::string &name, uint32_t &out_store, uint32_t &out_part, uint32_t &out_idx)
{
    auto dot = name.find('.');
    if (dot == std::string::npos) {
        return false;
    }
    if (name.substr(dot + 1, 4) != "blk-") {
        return false;
    }
    auto dash = name.find('-');
    if (dash == std::string::npos || dash >= dot) {
        return false;
    }
    try {
        out_store = static_cast<uint32_t>(std::stoul(name.substr(0, dash)));
        out_part  = static_cast<uint32_t>(std::stoul(name.substr(dash + 1, dot - dash - 1)));
        out_idx   = static_cast<uint32_t>(std::stoul(name.substr(dot + 5)));
    }
    catch (...) {
        return false;
    }
    return true;
}
} // namespace

Status BlockPageStore::open_blocks(const std::string &dir, uint32_t store_id, uint32_t group_id, uint64_t block_size,
                                   uint32_t iu_size, std::unique_ptr<BlockPageStore> *out)
{
    if (block_size == 0) {
        return Status::invalid_argument("open_blocks: block_size must be > 0");
    }

    auto *store = new BlockPageStore(dir, store_id, group_id, block_size, iu_size == 0 ? 4096 : iu_size);

    // Scan directory for existing block files matching {store_id}-{group_id}.blk-*
    DIR *d = ::opendir(dir.c_str());
    if (d != nullptr) {
        struct dirent *ent;
        while ((ent = ::readdir(d)) != nullptr) {
            uint32_t s_id = 0, p_id = 0, idx = 0;
            if (!parse_block_filename(ent->d_name, s_id, p_id, idx)) {
                continue;
            }
            if (s_id != store_id || p_id != group_id) {
                continue;
            }
            // Open the existing block file
            std::string                 path = dir + "/" + ent->d_name;
            std::unique_ptr<FileMedium> fm;
            Status                      s = FileMedium::open(path, store->iu_size_ > 1, &fm);
            if (!s.ok()) {
                ::closedir(d);
                delete store;
                return s;
            }
            BlockExtent ext;
            ext.medium      = std::move(fm);
            ext.base_offset = static_cast<uint64_t>(idx) * block_size;
            ext.used        = ext.medium->size();
            ext.dirty       = false;
            store->extents_.push_back(std::move(ext));
        }
        ::closedir(d);
    }

    // Sort extents by base_offset (which is idx * block_size)
    std::sort(store->extents_.begin(), store->extents_.end(),
              [](const BlockExtent &a, const BlockExtent &b) { return a.base_offset < b.base_offset; });

    // Insert deleted placeholder extents for missing block indices (deleted
    // blocks leave gaps in the index sequence — must preserve offset-to-extent mapping).
    if (!store->extents_.empty()) {
        std::vector<BlockExtent> dense;
        uint32_t                 expected_idx = 0;
        for (auto &ext : store->extents_) {
            uint32_t ext_idx = static_cast<uint32_t>(ext.base_offset / block_size);
            while (expected_idx < ext_idx) {
                BlockExtent gap;
                gap.base_offset = static_cast<uint64_t>(expected_idx) * block_size;
                gap.deleted     = true;
                dense.push_back(std::move(gap));
                ++expected_idx;
            }
            dense.push_back(std::move(ext));
            ++expected_idx;
        }
        store->extents_ = std::move(dense);
    }

    // If no blocks exist, allocate the first one
    if (store->extents_.empty()) {
        Status s = store->allocate_new_block();
        if (!s.ok()) {
            delete store;
            return s;
        }
    }

    out->reset(store);
    return Status::Ok();
}

Status BlockPageStore::allocate_new_block()
{
    uint32_t idx = static_cast<uint32_t>(extents_.size());
    char     name[64];
    std::snprintf(name, sizeof(name), "%u-%u.blk-%04u", store_id_, group_id_, idx);
    std::string path = dir_ + "/" + name;

    std::unique_ptr<FileMedium> fm;
    Status                      s = FileMedium::open(path, iu_size_ > 1, &fm);
    if (!s.ok()) {
        return s;
    }

    BlockExtent ext;
    ext.medium      = std::move(fm);
    ext.base_offset = static_cast<uint64_t>(idx) * block_size_;
    ext.used        = 0;
    ext.dirty       = false;
    extents_.push_back(std::move(ext));
    return Status::Ok();
}

Status BlockPageStore::delete_block(uint32_t block_idx)
{
    if (block_idx >= extents_.size()) {
        return Status::invalid_argument("delete_block: block index out of range");
    }

    auto &ext = extents_[block_idx];
    if (ext.deleted) {
        return Status::Ok(); // already deleted
    }

    char name[64];
    std::snprintf(name, sizeof(name), "%u-%u.blk-%04u", store_id_, group_id_, block_idx);
    std::string path = dir_ + "/" + name;

    if (::unlink(path.c_str()) < 0 && errno != ENOENT) {
        return Status::io_error(std::string("unlink: ") + std::strerror(errno));
    }

    ext.medium.reset();
    ext.deleted = true;
    return Status::Ok();
}

Status BlockPageStore::write_at_extents(uint64_t off, const uint8_t *buf, size_t len)
{
    // Ensure enough extents are allocated
    uint64_t end = off + len;
    while (extents_.back().base_offset + block_size_ < end) {
        Status s = allocate_new_block();
        if (!s.ok()) {
            return s;
        }
    }

    // Split write across extent boundaries
    size_t   done = 0;
    uint64_t cur  = off;
    while (done < len) {
        // Find the extent containing `cur`
        uint64_t extent_idx = cur / block_size_;
        if (extent_idx >= extents_.size()) {
            return Status::io_error("BlockPageStore: extent index out of range");
        }
        auto &ext = extents_[extent_idx];
        if (ext.deleted) {
            return Status::io_error("BlockPageStore: write to deleted block");
        }
        uint64_t local = cur - ext.base_offset;
        uint64_t avail = block_size_ - local;
        size_t   chunk = std::min(static_cast<uint64_t>(len - done), avail);

        // For IU=1, write directly. For IU>1, use the bounce path via medium.
        Status s;
        if (iu_size_ <= 1) {
            s = ext.medium->pwrite_at(local, buf + done, chunk);
        }
        else {
            // Use the same alignment logic as single-medium mode
            bool aligned = (local % iu_size_ == 0) && (chunk % iu_size_ == 0) &&
                           (reinterpret_cast<uintptr_t>(buf + done) % kDirectBufAlign == 0);
            if (aligned) {
                s = ext.medium->pwrite_at(local, buf + done, chunk);
            }
            else {
                uint64_t      start = (local / iu_size_) * iu_size_;
                uint64_t      e     = align_up(local + chunk, iu_size_);
                size_t        span  = e - start;
                AlignedBuffer scratch(span, iu_size_);
                if (!scratch.ok()) {
                    return Status::io_error("BlockPageStore: aligned scratch allocation failed");
                }
                size_t got = 0;
                s          = ext.medium->pread_partial(start, scratch.data(), span, &got);
                if (s.ok()) {
                    if (got < span) {
                        std::memset(scratch.data() + got, 0, span - got);
                    }
                    std::memcpy(scratch.data() + (local - start), buf + done, chunk);
                    s = ext.medium->pwrite_at(start, scratch.data(), span);
                }
            }
        }
        if (!s.ok()) {
            return s;
        }
        ext.used  = std::max(ext.used, local + chunk);
        ext.dirty = true;
        done += chunk;
        cur += chunk;
    }
    return Status::Ok();
}

Status BlockPageStore::read_at_extents(uint64_t off, uint8_t *buf, size_t len) const
{
    size_t   done = 0;
    uint64_t cur  = off;
    while (done < len) {
        uint64_t extent_idx = cur / block_size_;
        if (extent_idx >= extents_.size()) {
            return Status::io_error("BlockPageStore: read past end (no extent)");
        }
        const auto &ext = extents_[extent_idx];
        if (ext.deleted) {
            return Status::io_error("BlockPageStore: read from deleted block");
        }
        uint64_t local = cur - ext.base_offset;
        uint64_t avail = block_size_ - local;
        size_t   chunk = std::min(static_cast<uint64_t>(len - done), avail);

        Status s;
        if (iu_size_ <= 1) {
            s = ext.medium->pread_at(local, buf + done, chunk);
        }
        else {
            bool aligned = (local % iu_size_ == 0) && (chunk % iu_size_ == 0) &&
                           (reinterpret_cast<uintptr_t>(buf + done) % kDirectBufAlign == 0);
            if (aligned) {
                s = ext.medium->pread_at(local, buf + done, chunk);
            }
            else {
                uint64_t      start = (local / iu_size_) * iu_size_;
                uint64_t      e     = align_up(local + chunk, iu_size_);
                size_t        span  = e - start;
                AlignedBuffer scratch(span, iu_size_);
                if (!scratch.ok()) {
                    return Status::io_error("BlockPageStore: aligned scratch allocation failed");
                }
                size_t got = 0;
                s          = ext.medium->pread_partial(start, scratch.data(), span, &got);
                if (s.ok()) {
                    if (got < (local - start) + chunk) {
                        return Status::io_error("BlockPageStore: read past end");
                    }
                    std::memcpy(buf + done, scratch.data() + (local - start), chunk);
                }
            }
        }
        if (!s.ok()) {
            return s;
        }
        done += chunk;
        cur += chunk;
    }
    return Status::Ok();
}

Status BlockPageStore::write_at(uint64_t off, const uint8_t *buf, size_t len)
{
    if (len == 0) {
        return Status::Ok();
    }

    // Array-of-blocks mode
    if (!extents_.empty()) {
        return write_at_extents(off, buf, len);
    }

    // Single-medium mode
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

    // Array-of-blocks mode
    if (!extents_.empty()) {
        return read_at_extents(off, buf, len);
    }

    // Single-medium mode
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
    // CT_SYNC_SKIP: no fsync at all (tests/CI only)
    if (sync_mode_ == SyncMode::kSkip) {
        return Status::Ok();
    }
    // CT_SYNC_FULL and CT_SYNC_BATCH both fsync here. In BATCH mode,
    // persist.cpp's snapshot() calls sync() only at the two barrier points
    // (before and after the anchor write), so the batching is already
    // handled by the caller not calling sync() per-page.
    // Array-of-blocks mode: fsync all dirty extents
    if (!extents_.empty()) {
        for (auto &ext : extents_) {
            if (ext.deleted || !ext.dirty) {
                continue;
            }
            Status s = ext.medium->fsync();
            if (!s.ok()) {
                return s;
            }
            ext.dirty = false;
        }
        return Status::Ok();
    }

    // Single-medium mode
    return medium_->fsync();
}

uint64_t BlockPageStore::size() const
{
    // Array-of-blocks mode: logical high-water mark (skip deleted blocks)
    if (!extents_.empty()) {
        uint64_t total = 0;
        for (const auto &ext : extents_) {
            if (!ext.deleted) {
                total = std::max(total, ext.base_offset + ext.used);
            }
        }
        return total;
    }

    // Single-medium mode
    if (is_block_device_ && capacity_ > 0) {
        return capacity_;
    }
    if (medium_ == nullptr) {
        return 0;
    }
    return medium_->size();
}

// ── Dump utility ───────────────────────────────────────────────────

Status dump_block_file(const std::string &path, uint32_t iu_size, std::string *out)
{
    std::unique_ptr<FileMedium> fm;
    Status                      s = FileMedium::open(path, false, &fm);
    if (!s.ok()) {
        return s;
    }

    uint64_t           file_size = fm->size();
    std::ostringstream oss;
    oss << "=== Block file: " << path << " ===\n";
    oss << "Size: " << file_size << " bytes, IU: " << iu_size << "\n\n";

    // Dump first 512 bytes as hex (anchor region)
    size_t               dump_len = std::min(static_cast<uint64_t>(512), file_size);
    std::vector<uint8_t> buf(dump_len, 0);
    size_t               got = 0;
    s                        = fm->pread_partial(0, buf.data(), dump_len, &got);
    if (!s.ok()) {
        return s;
    }

    oss << "--- First " << got << " bytes (anchor region) ---\n";
    for (size_t i = 0; i < got; i += 16) {
        oss << std::setfill('0') << std::hex << std::setw(8) << i << ": ";
        for (size_t j = 0; j < 16 && i + j < got; ++j) {
            oss << std::setw(2) << static_cast<unsigned>(buf[i + j]) << ' ';
        }
        oss << '\n';
    }
    oss << std::dec;

    *out = oss.str();
    return Status::Ok();
}

int BlockPageStore::fd_for_offset(uint64_t off, uint64_t *out_local) const
{
    if (extents_.empty()) {
        // Single-medium mode: only works if the medium is a FileMedium
        auto *fm = dynamic_cast<FileMedium *>(medium_.get());
        if (fm == nullptr) {
            return -1;
        }
        *out_local = off;
        return fm->fd();
    }
    // Array-of-blocks mode
    uint64_t extent_idx = off / block_size_;
    if (extent_idx >= extents_.size()) {
        return -1;
    }
    const auto &ext = extents_[extent_idx];
    if (ext.deleted) {
        return -1;
    }
    auto *fm = dynamic_cast<FileMedium *>(ext.medium.get());
    if (fm == nullptr) {
        return -1;
    }
    *out_local = off - ext.base_offset;
    return fm->fd();
}

Status BlockPageStore::ensure_extents(uint64_t off, size_t len)
{
    if (extents_.empty()) {
        // Single-medium mode: no block allocation needed
        return Status::Ok();
    }
    // Allocate blocks until the range [off, off+len) is covered
    uint64_t end = off + len;
    while (extents_.back().base_offset + block_size_ < end) {
        Status s = allocate_new_block();
        if (!s.ok()) {
            return s;
        }
    }
    // Mark covered extents as dirty
    uint64_t cur = off;
    while (cur < end) {
        uint64_t extent_idx = cur / block_size_;
        if (extent_idx < extents_.size()) {
            extents_[extent_idx].dirty = true;
        }
        uint64_t next_boundary = (extent_idx + 1) * block_size_;
        cur                    = std::min(end, next_boundary);
    }
    return Status::Ok();
}

std::vector<int> BlockPageStore::dirty_fds()
{
    std::vector<int> fds;
    if (sync_mode_ == SyncMode::kSkip) {
        return fds;
    }
    if (extents_.empty()) {
        // Single-medium mode
        auto *fm = dynamic_cast<FileMedium *>(medium_.get());
        if (fm != nullptr && fm->fd() >= 0) {
            fds.push_back(fm->fd());
        }
        return fds;
    }
    // Array-of-blocks mode: collect dirty, non-deleted extent fds
    for (auto &ext : extents_) {
        if (ext.deleted || !ext.dirty) {
            continue;
        }
        auto *fm = dynamic_cast<FileMedium *>(ext.medium.get());
        if (fm != nullptr && fm->fd() >= 0) {
            fds.push_back(fm->fd());
            ext.dirty = false;
        }
    }
    return fds;
}

std::vector<int> BlockPageStore::all_extent_fds() const
{
    std::vector<int> fds;
    if (extents_.empty()) {
        // Single-medium mode
        auto *fm = dynamic_cast<FileMedium *>(medium_.get());
        if (fm != nullptr && fm->fd() >= 0) {
            fds.push_back(fm->fd());
        }
        return fds;
    }
    // Array-of-blocks mode: collect all non-deleted extent fds
    for (const auto &ext : extents_) {
        if (ext.deleted) {
            continue;
        }
        auto *fm = dynamic_cast<FileMedium *>(ext.medium.get());
        if (fm != nullptr && fm->fd() >= 0) {
            fds.push_back(fm->fd());
        }
    }
    return fds;
}

} // namespace crow::tree
