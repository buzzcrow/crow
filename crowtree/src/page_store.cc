#include "crowtree/page_store.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace crowtree {

// ── MemPageStore ──────────────────────────────────────────────────

Status MemPageStore::WriteAt(uint64_t off, const uint8_t* buf, size_t len) {
  std::lock_guard<std::mutex> lk(mu_);
  if (off + len > data_.size()) data_.resize(off + len, 0);
  std::memcpy(data_.data() + off, buf, len);
  return Status::Ok();
}

Status MemPageStore::ReadAt(uint64_t off, uint8_t* buf, size_t len) const {
  std::lock_guard<std::mutex> lk(mu_);
  if (off + len > data_.size()) {
    return Status::IoError("MemPageStore: read past end");
  }
  std::memcpy(buf, data_.data() + off, len);
  return Status::Ok();
}

uint64_t MemPageStore::size() const {
  std::lock_guard<std::mutex> lk(mu_);
  return data_.size();
}

// ── FilePageStore ─────────────────────────────────────────────────

FilePageStore::~FilePageStore() {
  if (fd_ >= 0) ::close(fd_);
}

Status FilePageStore::Open(const std::string& path, uint32_t iu_size,
                           std::unique_ptr<FilePageStore>* out) {
  int fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    return Status::IoError(std::string("open: ") + std::strerror(errno));
  }
  out->reset(new FilePageStore(fd, iu_size == 0 ? 4096 : iu_size));
  return Status::Ok();
}

Status FilePageStore::WriteAt(uint64_t off, const uint8_t* buf, size_t len) {
  size_t done = 0;
  while (done < len) {
    ssize_t n = ::pwrite(fd_, buf + done, len - done, off + done);
    if (n < 0) {
      if (errno == EINTR) continue;
      return Status::IoError(std::string("pwrite: ") + std::strerror(errno));
    }
    done += static_cast<size_t>(n);
  }
  return Status::Ok();
}

Status FilePageStore::ReadAt(uint64_t off, uint8_t* buf, size_t len) const {
  size_t done = 0;
  while (done < len) {
    ssize_t n = ::pread(fd_, buf + done, len - done, off + done);
    if (n < 0) {
      if (errno == EINTR) continue;
      return Status::IoError(std::string("pread: ") + std::strerror(errno));
    }
    if (n == 0) return Status::IoError("FilePageStore: read past end");
    done += static_cast<size_t>(n);
  }
  return Status::Ok();
}

Status FilePageStore::Sync() {
  if (::fdatasync(fd_) < 0) {
    return Status::IoError(std::string("fdatasync: ") + std::strerror(errno));
  }
  return Status::Ok();
}

uint64_t FilePageStore::size() const {
  off_t end = ::lseek(fd_, 0, SEEK_END);
  return end < 0 ? 0 : static_cast<uint64_t>(end);
}

}  // namespace crowtree
