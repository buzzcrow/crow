#include "crowtree/buffer_pool.h"

#include "crowtree/frame_page.h"

#include <cstring>
#include <utility>

namespace crowtree {

namespace {
size_t next_pow2(size_t v) {
  size_t p = 1;
  while (p < v)
  {
    p <<= 1;
  }
  return p;
}
}  // namespace

// ── FrameRef ──────────────────────────────────────────────────────

FrameRef::~FrameRef() { release(); }

FrameRef& FrameRef::operator=(FrameRef&& o) noexcept {
  if (this != &o)
  {
    release();
    pool_ = o.pool_;
    idx_ = o.idx_;
    bytes_ = o.bytes_;
    page_id_ = o.page_id_;
    o.pool_ = nullptr;
    o.bytes_ = nullptr;
    o.page_id_ = kInvalidPageId;
  }
  return *this;
}

void FrameRef::release() {
  if (pool_ != nullptr)
  {
    pool_->unpin(idx_);
    pool_ = nullptr;
    bytes_ = nullptr;
    page_id_ = kInvalidPageId;
  }
}

// ── BufferPool ────────────────────────────────────────────────────

BufferPool::BufferPool(size_t capacity_bytes, uint32_t page_bytes, PageStore* store)
    : page_bytes_(page_bytes), store_(store) {
  num_frames_ = static_cast<uint32_t>(capacity_bytes / page_bytes);
  if (num_frames_ == 0)
  {
    num_frames_ = 1;
  }
  arena_.assign(size_t(num_frames_) * page_bytes_, 0);
  frames_.resize(num_frames_);
  stats_.num_frames = num_frames_;

  size_t ht_cap = next_pow2(size_t(num_frames_) * 2);
  if (ht_cap < 2)
  {
    ht_cap = 2;
  }
  ht_key_.assign(ht_cap, kInvalidPageId);
  ht_val_.assign(ht_cap, 0);
  ht_mask_ = ht_cap - 1;
}

void BufferPool::ht_insert(uint64_t page_id, uint32_t idx) {
  size_t i = page_id & ht_mask_;
  while (ht_key_[i] != kInvalidPageId)
  {
    i = (i + 1) & ht_mask_;
  }
  ht_key_[i] = page_id;
  ht_val_[i] = idx;
}

int64_t BufferPool::ht_find(uint64_t page_id) const {
  size_t i = page_id & ht_mask_;
  while (ht_key_[i] != kInvalidPageId)
  {
    if (ht_key_[i] == page_id)
    {
      return static_cast<int64_t>(ht_val_[i]);
    }
    i = (i + 1) & ht_mask_;
  }
  return -1;
}

void BufferPool::ht_erase(uint64_t page_id) {
  size_t i = page_id & ht_mask_;
  while (ht_key_[i] != kInvalidPageId && ht_key_[i] != page_id)
  {
    i = (i + 1) & ht_mask_;
  }
  if (ht_key_[i] == kInvalidPageId)
  {
    return;
  }
  // Backward-shift deletion to keep probe chains intact.
  size_t j = i;
  while (true)
  {
    ht_key_[i] = kInvalidPageId;
    size_t k;
    do
    {
      j = (j + 1) & ht_mask_;
      if (ht_key_[j] == kInvalidPageId)
      {
        return;
      }
      k = ht_key_[j] & ht_mask_;
    } while ((i <= j) ? (i < k && k <= j) : (i < k || k <= j));
    ht_key_[i] = ht_key_[j];
    ht_val_[i] = ht_val_[j];
    i = j;
  }
}

Status BufferPool::write_back(uint32_t idx) {
  FrameMeta& m = frames_[idx];
  Status s = store_->write_at(m.addr, frame_bytes(idx), page_bytes_);
  if (!s.ok())
  {
    return s;
  }
  m.dirty = false;
  ++stats_.writebacks;
  return Status::Ok();
}

int64_t BufferPool::acquire_victim() {
  // Two full sweeps: first honoring the ref bit, second forcing a clean victim.
  for (uint32_t scanned = 0; scanned < num_frames_ * 2; ++scanned)
  {
    uint32_t idx = clock_hand_;
    clock_hand_ = (clock_hand_ + 1) % num_frames_;
    FrameMeta& m = frames_[idx];
    if (m.pin > 0)
    {
      continue;
    }
    if (m.page_id == kInvalidPageId)
    {
      return idx;  // empty slot
    }
    if (m.ref != 0)
    {
      m.ref = 0;
      continue;
    }
    if (m.dirty)
    {
      if (!write_back(idx).ok())
      {
        continue;
      }
    }
    ht_erase(m.page_id);
    m.page_id = kInvalidPageId;
    ++stats_.evictions;
    return idx;
  }
  return -1;  // everything pinned
}

Status BufferPool::pin(uint64_t page_id, PageAddr addr, FrameRef* out) {
  std::lock_guard<std::mutex> lk(mu_);
  int64_t hit = ht_find(page_id);
  if (hit >= 0)
  {
    uint32_t idx = static_cast<uint32_t>(hit);
    FrameMeta& m = frames_[idx];
    ++m.pin;
    m.ref = 1;
    ++stats_.hits;
    *out = FrameRef(this, idx, frame_bytes(idx), page_id);
    return Status::Ok();
  }
  ++stats_.misses;
  int64_t v = acquire_victim();
  if (v < 0)
  {
    return Status::internal_error("BufferPool: no evictable frame (all pinned)");
  }
  uint32_t idx = static_cast<uint32_t>(v);
  Status rs = store_->read_at(addr, frame_bytes(idx), page_bytes_);
  if (!rs.ok())
  {
    return rs;
  }
  if (!frame_validate(frame_bytes(idx), page_bytes_))
  {
    return Status::corruption("BufferPool: page CRC on load");
  }
  FrameMeta& m = frames_[idx];
  m.page_id = page_id;
  m.addr = addr;
  m.pin = 1;
  m.ref = 1;
  m.dirty = false;
  ht_insert(page_id, idx);
  *out = FrameRef(this, idx, frame_bytes(idx), page_id);
  return Status::Ok();
}

Status BufferPool::pin_new(uint64_t page_id, PageAddr addr, FrameRef* out) {
  std::lock_guard<std::mutex> lk(mu_);
  if (ht_find(page_id) >= 0)
  {
    return Status::invalid_argument("BufferPool: page_id already resident");
  }
  int64_t v = acquire_victim();
  if (v < 0)
  {
    return Status::internal_error("BufferPool: no evictable frame (all pinned)");
  }
  uint32_t idx = static_cast<uint32_t>(v);
  std::memset(frame_bytes(idx), 0, page_bytes_);
  FrameMeta& m = frames_[idx];
  m.page_id = page_id;
  m.addr = addr;
  m.pin = 1;
  m.ref = 1;
  m.dirty = false;
  ht_insert(page_id, idx);
  *out = FrameRef(this, idx, frame_bytes(idx), page_id);
  return Status::Ok();
}

Status BufferPool::acquire_frame(uint32_t* out_idx, uint8_t** out_bytes) {
  std::lock_guard<std::mutex> lk(mu_);
  int64_t v = acquire_victim();
  if (v < 0)
  {
    return Status::internal_error("BufferPool: no evictable frame (all pinned)");
  }
  uint32_t idx = static_cast<uint32_t>(v);
  std::memset(frame_bytes(idx), 0, page_bytes_);
  FrameMeta& m = frames_[idx];
  m.page_id = kInvalidPageId;  // anonymous: not findable by page_id
  m.addr = kNoAddr;
  m.pin = 1;  // pinned-resident until release_frame (never evicted)
  m.ref = 1;
  m.dirty = true;  // not yet durable
  *out_idx = idx;
  *out_bytes = frame_bytes(idx);
  return Status::Ok();
}

void BufferPool::release_frame(uint32_t idx) {
  std::lock_guard<std::mutex> lk(mu_);
  FrameMeta& m = frames_[idx];
  if (m.page_id != kInvalidPageId && ht_find(m.page_id) == static_cast<int64_t>(idx))
  {
    ht_erase(m.page_id);
  }
  m.page_id = kInvalidPageId;
  m.addr = 0;
  m.pin = 0;
  m.ref = 0;
  m.dirty = false;  // empty + evictable again
}

void BufferPool::unpin(uint32_t idx) {
  std::lock_guard<std::mutex> lk(mu_);
  if (frames_[idx].pin > 0)
  {
    --frames_[idx].pin;
  }
}

void BufferPool::mark_dirty(uint64_t page_id) {
  std::lock_guard<std::mutex> lk(mu_);
  int64_t hit = ht_find(page_id);
  if (hit >= 0)
  {
    frames_[static_cast<uint32_t>(hit)].dirty = true;
  }
}

Status BufferPool::flush_dirty() {
  std::lock_guard<std::mutex> lk(mu_);
  for (uint32_t idx = 0; idx < num_frames_; ++idx)
  {
    if (frames_[idx].page_id != kInvalidPageId && frames_[idx].dirty)
    {
      Status s = write_back(idx);
      if (!s.ok())
      {
        return s;
      }
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
  for (const FrameMeta& m : frames_)
  {
    if (m.page_id != kInvalidPageId)
    {
      ++s.resident;
    }
    if (m.dirty)
    {
      ++s.dirty;
    }
    if (m.pin > 0 || m.page_id != kInvalidPageId)
    {
      ++s.used;
    }
  }
  return s;
}

}  // namespace crowtree
