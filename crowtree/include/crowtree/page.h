// Pages (design-crowtree-core.md §3).
//
// The core in-memory engine represents pages as C++ objects (not the byte-packed
// on-disk offset-array layout; that lives in the persistence plan). Semantics
// match the design: leaves hold sorted (key, cell) entries with a right-sibling
// link; inner pages hold separator keys + child PIDs. Delta records (CT8) link
// in front of a LeafBase via the chain fields in PageBase.
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "crowtree/buffer_pool.h"
#include "crowtree/cell.h"
#include "crowtree/frame_page.h"
#include "crowtree/page_types.h"
#include "crowtree/slice.h"

namespace crowtree {

// Backing bytes for a base page (design §4.5). Either a buffer-pool frame (the
// page co-owns the pool via shared_ptr so the frame is valid even when the page
// is freed late by the env-level epoch manager) or a heap buffer (used by
// standalone/unit construction, recovery, oversized pages, or pool exhaustion).
struct FrameStore {
  std::shared_ptr<BufferPool> pool;  // non-null => pool-backed
  uint32_t frame_idx = 0;
  std::vector<uint8_t> owned;        // used iff pool == nullptr
  uint8_t* ptr = nullptr;
  uint32_t page_bytes = 0;

  FrameStore() = default;
  FrameStore(const FrameStore&) = delete;
  FrameStore& operator=(const FrameStore&) = delete;
  ~FrameStore() {
    if (pool) pool->ReleaseFrame(frame_idx);
  }

  // Allocate writable backing for a `need`-byte page. Uses a fixed pool frame
  // when one is available and large enough; otherwise a tight heap buffer.
  uint8_t* Alloc(size_t need, const std::shared_ptr<BufferPool>& p, uint32_t frame_bytes) {
    if (p && need <= frame_bytes) {
      uint32_t idx = 0;
      uint8_t* bytes = nullptr;
      if (p->AcquireFrame(&idx, &bytes).ok()) {
        pool = p;
        frame_idx = idx;
        ptr = bytes;
        page_bytes = frame_bytes;
        return ptr;
      }
    }
    uint32_t pb = static_cast<uint32_t>((need < 128 ? 128 : need + 7) & ~size_t(7));
    owned.assign(pb, 0);
    ptr = owned.data();
    page_bytes = pb;
    return ptr;
  }

  // Wrap a copy of an existing frame image (heap-backed; recovery path).
  uint8_t* AdoptCopy(const uint8_t* buf, uint32_t n) {
    owned.assign(buf, buf + n);
    ptr = owned.data();
    page_bytes = n;
    return ptr;
  }
};

// Immutable, sorted leaf base page, backed by a zero-copy frame (the on-disk
// byte layout; see frame_page.h). Accessors read directly from the frame; the
// returned Slices point into it and stay valid for the page's lifetime.
class LeafBase : public PageBase {
 public:
  LeafBase() : PageBase(PageType::kLeafBase) {}

  // Build from already key-sorted, deduplicated entries.
  static LeafBase* Build(std::vector<LeafEntry> sorted_entries,
                         uint64_t right_sibling = kInvalidPID,
                         const std::shared_ptr<BufferPool>& pool = nullptr,
                         uint32_t frame_bytes = 0) {
    auto* p = new LeafBase();
    size_t need = kFrameHeaderSize + kFrameTrailerSize +
                  sorted_entries.size() * kLeafSlotSize;
    for (const auto& e : sorted_entries) need += e.key.size() + e.cell.size();
    uint8_t* dst = p->fs_.Alloc(need, pool, frame_bytes);
    LeafFrameBuilder b(dst, p->fs_.page_bytes);
    for (const auto& e : sorted_entries) b.TryAppendSorted(Slice(e.key), Slice(e.cell));
    b.Finish(p->pid, right_sibling);
    return p;
  }

  // Wrap a copy of an existing frame image (e.g. read from durable storage).
  static LeafBase* FromFrameCopy(const uint8_t* buf, uint32_t page_bytes) {
    auto* p = new LeafBase();
    p->fs_.AdoptCopy(buf, page_bytes);
    return p;
  }

  LeafFrameView view() const { return LeafFrameView(fs_.ptr, fs_.page_bytes); }
  const uint8_t* frame() const { return fs_.ptr; }
  uint32_t page_bytes() const { return fs_.page_bytes; }

  size_t count() const { return view().count(); }
  bool empty() const { return count() == 0; }
  uint64_t right_sibling() const { return view().right_sibling(); }
  void set_right_sibling(uint64_t pid) {
    FramePutU64(fs_.ptr, fh::kRightSibling, pid);
    FrameRestampCrc(fs_.ptr, fs_.page_bytes);
  }

  // Zero-copy accessors.
  Slice key(size_t i) const { return view().key(static_cast<uint32_t>(i)); }
  Slice cell(size_t i) const { return view().cell(static_cast<uint32_t>(i)); }
  // Materializing accessors (compatibility; copy out of the frame).
  LeafEntry entry(size_t i) const {
    LeafFrameView v = view();
    return LeafEntry{v.key(static_cast<uint32_t>(i)).ToString(),
                     v.cell(static_cast<uint32_t>(i)).ToString()};
  }
  std::vector<LeafEntry> entries() const {
    LeafFrameView v = view();
    std::vector<LeafEntry> out;
    out.reserve(v.count());
    for (uint32_t i = 0; i < v.count(); ++i) {
      out.push_back(LeafEntry{v.key(i).ToString(), v.cell(i).ToString()});
    }
    return out;
  }

  Slice low_key() const { return count() == 0 ? Slice() : view().key(0); }
  Slice high_key() const {
    uint32_t n = view().count();
    return n == 0 ? Slice() : view().key(n - 1);
  }

  size_t data_bytes() const { return view().data_bytes(); }
  int Find(Slice key) const { return view().Find(key); }
  bool Lookup(Slice key, CellView* out) const { return view().Lookup(key, out); }
  size_t LowerBound(Slice key) const { return view().LowerBound(key); }

 private:
  FrameStore fs_;
};

// Immutable inner (index) page. Holds `n` child PIDs and `n-1` separator keys.
// children_[i] covers keys k with separators_[i-1] <= k < separators_[i]
// (with -inf / +inf at the ends). Inner pages carry no values and are rebuilt
// eagerly on change (no delta chain) in the in-memory core.
class InnerBase : public PageBase {
 public:
  InnerBase() : PageBase(PageType::kInnerBase) {}

  static InnerBase* Build(std::vector<std::string> separators,
                          std::vector<uint64_t> children,
                          const std::shared_ptr<BufferPool>& pool = nullptr,
                          uint32_t frame_bytes = 0) {
    auto* p = new InnerBase();
    size_t need = kFrameHeaderSize + kFrameTrailerSize +
                  children.size() * sizeof(uint64_t) +
                  separators.size() * kInnerSlotSize;
    for (const auto& s : separators) need += s.size();
    uint8_t* dst = p->fs_.Alloc(need, pool, frame_bytes);
    std::vector<Slice> sep_slices;
    sep_slices.reserve(separators.size());
    for (const auto& s : separators) sep_slices.push_back(Slice(s));
    InnerFrameBuild(dst, p->fs_.page_bytes, p->pid, children, sep_slices);
    return p;
  }

  // Wrap a copy of an existing frame image (e.g. read from durable storage).
  static InnerBase* FromFrameCopy(const uint8_t* buf, uint32_t page_bytes) {
    auto* p = new InnerBase();
    p->fs_.AdoptCopy(buf, page_bytes);
    return p;
  }

  InnerFrameView view() const { return InnerFrameView(fs_.ptr, fs_.page_bytes); }
  const uint8_t* frame() const { return fs_.ptr; }
  uint32_t page_bytes() const { return fs_.page_bytes; }

  size_t num_children() const { return view().num_children(); }
  size_t num_separators() const { return view().num_separators(); }
  uint64_t child_at(size_t i) const { return view().child_at(static_cast<uint32_t>(i)); }
  std::string separator_at(size_t i) const {
    return view().separator_at(static_cast<uint32_t>(i)).ToString();
  }
  // Materializing accessors (compatibility; copy out of the frame).
  std::vector<std::string> separators() const {
    InnerFrameView v = view();
    std::vector<std::string> out;
    out.reserve(v.num_separators());
    for (uint32_t i = 0; i < v.num_separators(); ++i) out.push_back(v.separator_at(i).ToString());
    return out;
  }
  std::vector<uint64_t> children() const {
    InnerFrameView v = view();
    std::vector<uint64_t> out;
    out.reserve(v.num_children());
    for (uint32_t i = 0; i < v.num_children(); ++i) out.push_back(v.child_at(i));
    return out;
  }

  size_t ChildIndexFor(Slice key) const { return view().ChildIndexFor(key); }
  uint64_t ChildFor(Slice key) const { return view().ChildFor(key); }

 private:
  FrameStore fs_;
};

}  // namespace crowtree
