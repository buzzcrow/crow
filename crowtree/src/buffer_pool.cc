#include "crowtree/buffer_pool.h"

#include "crowtree/frame_page.h"

#include <cstring>
#include <utility>

namespace crowtree {

namespace {
size_t NextPow2(size_t v) {
  size_t p = 1;
  while (p < v) p <<= 1;
  return p;
}
}  // namespace

// ── FrameRef ──────────────────────────────────────────────────────

FrameRef::~FrameRef() { Release(); }

FrameRef& FrameRef::operator=(FrameRef&& o) noexcept {
  if (this != &o) {
    Release();
    pool_ = o.pool_;
    idx_ = o.idx_;
    bytes_ = o.bytes_;
    pid_ = o.pid_;
    o.pool_ = nullptr;
    o.bytes_ = nullptr;
    o.pid_ = kInvalidPID;
  }
  return *this;
}

void FrameRef::Release() {
  if (pool_ != nullptr) {
    pool_->Unpin(idx_);
    pool_ = nullptr;
    bytes_ = nullptr;
    pid_ = kInvalidPID;
  }
}

// ── BufferPool ────────────────────────────────────────────────────

BufferPool::BufferPool(size_t capacity_bytes, uint32_t page_bytes, PageStore* store)
    : page_bytes_(page_bytes), store_(store) {
  num_frames_ = static_cast<uint32_t>(capacity_bytes / page_bytes);
  if (num_frames_ == 0) num_frames_ = 1;
  arena_.assign(size_t(num_frames_) * page_bytes_, 0);
  frames_.resize(num_frames_);
  stats_.num_frames = num_frames_;

  size_t ht_cap = NextPow2(size_t(num_frames_) * 2);
  if (ht_cap < 2) ht_cap = 2;
  ht_key_.assign(ht_cap, kInvalidPID);
  ht_val_.assign(ht_cap, 0);
  ht_mask_ = ht_cap - 1;
}

void BufferPool::HtInsert(uint64_t pid, uint32_t idx) {
  size_t i = pid & ht_mask_;
  while (ht_key_[i] != kInvalidPID) i = (i + 1) & ht_mask_;
  ht_key_[i] = pid;
  ht_val_[i] = idx;
}

int64_t BufferPool::HtFind(uint64_t pid) const {
  size_t i = pid & ht_mask_;
  while (ht_key_[i] != kInvalidPID) {
    if (ht_key_[i] == pid) return static_cast<int64_t>(ht_val_[i]);
    i = (i + 1) & ht_mask_;
  }
  return -1;
}

void BufferPool::HtErase(uint64_t pid) {
  size_t i = pid & ht_mask_;
  while (ht_key_[i] != kInvalidPID && ht_key_[i] != pid) i = (i + 1) & ht_mask_;
  if (ht_key_[i] == kInvalidPID) return;
  // Backward-shift deletion to keep probe chains intact.
  size_t j = i;
  while (true) {
    ht_key_[i] = kInvalidPID;
    size_t k;
    do {
      j = (j + 1) & ht_mask_;
      if (ht_key_[j] == kInvalidPID) return;
      k = ht_key_[j] & ht_mask_;
    } while ((i <= j) ? (i < k && k <= j) : (i < k || k <= j));
    ht_key_[i] = ht_key_[j];
    ht_val_[i] = ht_val_[j];
    i = j;
  }
}

Status BufferPool::WriteBack(uint32_t idx) {
  FrameMeta& m = frames_[idx];
  Status s = store_->WriteAt(m.addr, FrameBytes(idx), page_bytes_);
  if (!s.ok()) return s;
  m.dirty = false;
  ++stats_.writebacks;
  return Status::Ok();
}

int64_t BufferPool::AcquireVictim() {
  // Two full sweeps: first honoring the ref bit, second forcing a clean victim.
  for (uint32_t scanned = 0; scanned < num_frames_ * 2; ++scanned) {
    uint32_t idx = clock_hand_;
    clock_hand_ = (clock_hand_ + 1) % num_frames_;
    FrameMeta& m = frames_[idx];
    if (m.pin > 0) continue;
    if (m.pid == kInvalidPID) return idx;  // empty slot
    if (m.ref != 0) {
      m.ref = 0;
      continue;
    }
    if (m.dirty) {
      if (!WriteBack(idx).ok()) continue;
    }
    HtErase(m.pid);
    m.pid = kInvalidPID;
    ++stats_.evictions;
    return idx;
  }
  return -1;  // everything pinned
}

Status BufferPool::Pin(uint64_t pid, PageAddr addr, FrameRef* out) {
  std::lock_guard<std::mutex> lk(mu_);
  int64_t hit = HtFind(pid);
  if (hit >= 0) {
    uint32_t idx = static_cast<uint32_t>(hit);
    FrameMeta& m = frames_[idx];
    ++m.pin;
    m.ref = 1;
    ++stats_.hits;
    *out = FrameRef(this, idx, FrameBytes(idx), pid);
    return Status::Ok();
  }
  ++stats_.misses;
  int64_t v = AcquireVictim();
  if (v < 0) return Status::Internal("BufferPool: no evictable frame (all pinned)");
  uint32_t idx = static_cast<uint32_t>(v);
  Status rs = store_->ReadAt(addr, FrameBytes(idx), page_bytes_);
  if (!rs.ok()) return rs;
  if (!FrameValidate(FrameBytes(idx), page_bytes_)) {
    return Status::Corruption("BufferPool: page CRC on load");
  }
  FrameMeta& m = frames_[idx];
  m.pid = pid;
  m.addr = addr;
  m.pin = 1;
  m.ref = 1;
  m.dirty = false;
  HtInsert(pid, idx);
  *out = FrameRef(this, idx, FrameBytes(idx), pid);
  return Status::Ok();
}

Status BufferPool::PinNew(uint64_t pid, PageAddr addr, FrameRef* out) {
  std::lock_guard<std::mutex> lk(mu_);
  if (HtFind(pid) >= 0) return Status::InvalidArgument("BufferPool: pid already resident");
  int64_t v = AcquireVictim();
  if (v < 0) return Status::Internal("BufferPool: no evictable frame (all pinned)");
  uint32_t idx = static_cast<uint32_t>(v);
  std::memset(FrameBytes(idx), 0, page_bytes_);
  FrameMeta& m = frames_[idx];
  m.pid = pid;
  m.addr = addr;
  m.pin = 1;
  m.ref = 1;
  m.dirty = false;
  HtInsert(pid, idx);
  *out = FrameRef(this, idx, FrameBytes(idx), pid);
  return Status::Ok();
}

Status BufferPool::AcquireFrame(uint32_t* out_idx, uint8_t** out_bytes) {
  std::lock_guard<std::mutex> lk(mu_);
  int64_t v = AcquireVictim();
  if (v < 0) return Status::Internal("BufferPool: no evictable frame (all pinned)");
  uint32_t idx = static_cast<uint32_t>(v);
  std::memset(FrameBytes(idx), 0, page_bytes_);
  FrameMeta& m = frames_[idx];
  m.pid = kInvalidPID;  // anonymous: not findable by pid
  m.addr = kNoAddr;
  m.pin = 1;  // pinned-resident until ReleaseFrame (never evicted)
  m.ref = 1;
  m.dirty = true;  // not yet durable
  *out_idx = idx;
  *out_bytes = FrameBytes(idx);
  return Status::Ok();
}

void BufferPool::ReleaseFrame(uint32_t idx) {
  std::lock_guard<std::mutex> lk(mu_);
  FrameMeta& m = frames_[idx];
  if (m.pid != kInvalidPID && HtFind(m.pid) == static_cast<int64_t>(idx)) {
    HtErase(m.pid);
  }
  m.pid = kInvalidPID;
  m.addr = 0;
  m.pin = 0;
  m.ref = 0;
  m.dirty = false;  // empty + evictable again
}

void BufferPool::Unpin(uint32_t idx) {
  std::lock_guard<std::mutex> lk(mu_);
  if (frames_[idx].pin > 0) --frames_[idx].pin;
}

void BufferPool::MarkDirty(uint64_t pid) {
  std::lock_guard<std::mutex> lk(mu_);
  int64_t hit = HtFind(pid);
  if (hit >= 0) frames_[static_cast<uint32_t>(hit)].dirty = true;
}

Status BufferPool::FlushDirty() {
  std::lock_guard<std::mutex> lk(mu_);
  for (uint32_t idx = 0; idx < num_frames_; ++idx) {
    if (frames_[idx].pid != kInvalidPID && frames_[idx].dirty) {
      Status s = WriteBack(idx);
      if (!s.ok()) return s;
    }
  }
  return Status::Ok();
}

BufferPool::Stats BufferPool::stats() const {
  std::lock_guard<std::mutex> lk(mu_);
  Stats s = stats_;
  s.resident = 0;
  s.dirty = 0;
  s.used = 0;
  for (const FrameMeta& m : frames_) {
    if (m.pid != kInvalidPID) ++s.resident;
    if (m.dirty) ++s.dirty;
    if (m.pin > 0 || m.pid != kInvalidPID) ++s.used;
  }
  return s;
}

}  // namespace crowtree
