// buffer: a move-only byte container that is either OWNED (frees on destruction)
// or BORROWED (a non-owning view whose lifetime is guaranteed elsewhere — e.g. a
// resident B+tree frame held alive by an epoch guard).
//
// Layout of an OWNED allocation created by alloc(capacity, header_reserve):
//
//   [ header_reserve bytes ][ capacity bytes ]
//   ^data()                 ^data() + header_reserve
//
// The whole block is one contiguous buffer, so an encoded cell ([slot][flags]
// written into the reserved header, value written after it) needs no second
// allocation. data()/size()/slice() span the whole used range; header(off)
// addresses the reserved prefix.
//
// SBO: an owned buffer whose total length is <= kInlineCap is stored
// INLINE in the object with no malloc (mirrors std::string's SSO), so replacing
// std::string on the write path never regresses the small-key/small-value case.
// Because inline bytes live in the object, data() is COMPUTED (not a cached
// pointer) and moves relocate the inline bytes.
#pragma once

#include "crowtree/slice.h"

#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>

namespace crowtree
{

class buffer
{
  public:
    enum class mode : uint8_t {
        kOwned,    // allocated (inline SBO or malloc seam); frees on destruction
        kBorrowed, // view into external memory; never frees
    };

    // Owned buffers whose total length is <= kInlineCap are stored inline (no
    // malloc), mirroring std::string's SSO. 24 B holds a 9-byte cell
    // header + up to 15 B of value inline — the common small-value case.
    static constexpr size_t kInlineCap = 24;

    // Empty owned buffer (null, size 0).
    buffer() = default;

    // Owned allocation of `header_reserve + capacity` bytes (inline when it fits
    // kInlineCap, else heap). `size()` starts at the full length; shrink with
    // set_size(). A zero-length request yields an empty owned buffer.
    static buffer alloc(size_t capacity, size_t header_reserve = 0)
    {
        size_t total = capacity + header_reserve;
        buffer b;
        b.header_reserve_ = header_reserve;
        if (total == 0) {
            return b; // empty
        }
        b.mode_ = mode::kOwned;
        b.size_ = total;
        if (total <= kInlineCap) {
            b.inline_active_ = true;
            b.capacity_      = kInlineCap;
        }
        else {
            b.heap_     = allocate(total);
            b.capacity_ = total;
        }
        return b;
    }

    // Owned deep copy of an external byte view (inline when it fits).
    static buffer copy_of(Slice s)
    {
        buffer b = alloc(s.size());
        if (!s.empty()) {
            std::memcpy(b.data(), s.bytes(), s.size());
        }
        return b;
    }

    // Borrowed view over external bytes (read path; caller guarantees the lifetime).
    static buffer wrap(const uint8_t *data, size_t len)
    {
        buffer b;
        b.mode_     = mode::kBorrowed;
        b.heap_     = const_cast<uint8_t *>(data); // never written/freed through
        b.size_     = len;
        b.capacity_ = len;
        return b;
    }

    // Take ownership of a raw heap pointer already allocated by the matching
    // allocator (the alloc() seam, i.e. std::malloc). Always heap (not inline).
    static buffer move_from(uint8_t *data, size_t len, size_t cap)
    {
        buffer b;
        b.mode_     = mode::kOwned;
        b.heap_     = data;
        b.size_     = len;
        b.capacity_ = cap;
        return b;
    }

    buffer(buffer &&o) noexcept
    {
        adopt(o);
    }

    buffer &operator=(buffer &&o) noexcept
    {
        if (this != &o) {
            free_if_owned();
            adopt(o);
        }
        return *this;
    }

    buffer(const buffer &)            = delete;
    buffer &operator=(const buffer &) = delete;

    ~buffer()
    {
        free_if_owned();
    }

    // Explicit deep copy into a fresh OWNED buffer (copies the used [data,size)
    // range, preserving the header_reserve marker; inline when it fits).
    [[nodiscard]] buffer clone() const
    {
        buffer c          = alloc(size_, 0);
        c.header_reserve_ = header_reserve_;
        if (size_ > 0) {
            std::memcpy(c.data(), data(), size_);
        }
        return c;
    }

    // data() is COMPUTED (inline bytes live in the object; not a cached pointer).
    [[nodiscard]] uint8_t *data()
    {
        return inline_active_ ? inbuf_.data() : heap_;
    }

    [[nodiscard]] const uint8_t *data() const
    {
        return inline_active_ ? inbuf_.data() : heap_;
    }

    [[nodiscard]] size_t size() const
    {
        return size_;
    }

    [[nodiscard]] size_t capacity() const
    {
        return capacity_;
    }

    [[nodiscard]] size_t header_reserve() const
    {
        return header_reserve_;
    }

    [[nodiscard]] Slice slice() const
    {
        return {data(), size_};
    }

    // Implicit view conversion so a buffer can be passed anywhere a Slice (byte
    // view) is expected — keeps call sites that read a buffer's bytes uniform with
    // std::string. Never extends the buffer's lifetime; the view dangles once freed.
    operator Slice() const
    {
        return {data(), size_};
    } // NOLINT(google-explicit-constructor)

    // Pointer into the reserved header prefix ([0, header_reserve)).
    [[nodiscard]] uint8_t *header(size_t off)
    {
        return data() + off;
    }

    [[nodiscard]] const uint8_t *header(size_t off) const
    {
        return data() + off;
    }

    void set_size(size_t len)
    {
        size_ = len;
    }

    [[nodiscard]] bool owned() const
    {
        return mode_ == mode::kOwned;
    }

    [[nodiscard]] bool inlined() const
    {
        return inline_active_;
    } // diagnostic / tests

    [[nodiscard]] mode ownership() const
    {
        return mode_;
    }

    [[nodiscard]] bool empty() const
    {
        return size_ == 0;
    }

    // Byte-order (memcmp) comparison so buffer can key an absl::btree_map (OQ2/OQ3).
    bool operator<(const buffer &o) const
    {
        return slice().compare(o.slice()) < 0;
    }

    bool operator==(const buffer &o) const
    {
        return slice().compare(o.slice()) == 0;
    }

  private:
    // Allocator seam. Step 1 = glibc malloc; a size-classed pool or
    // RDMA-pinned allocator slots in here later with no call-site changes. Only used
    // for owned buffers larger than kInlineCap.
    static uint8_t *allocate(size_t n)
    {
        return static_cast<uint8_t *>(std::malloc(n));
    }

    static void deallocate(uint8_t *p)
    {
        std::free(p);
    }

    // Take over o's state (move); relocate inline bytes since they live in-object.
    void adopt(buffer &o)
    {
        size_           = o.size_;
        capacity_       = o.capacity_;
        header_reserve_ = o.header_reserve_;
        mode_           = o.mode_;
        inline_active_  = o.inline_active_;
        if (inline_active_) {
            std::memcpy(inbuf_.data(), o.inbuf_.data(), size_); // only the used bytes
            heap_ = nullptr;
        }
        else {
            heap_ = o.heap_;
        }
        o.release_fields();
    }

    void free_if_owned()
    {
        if (mode_ == mode::kOwned && !inline_active_ && heap_ != nullptr) {
            deallocate(heap_);
        }
        release_fields();
    }

    void release_fields()
    {
        heap_           = nullptr;
        size_           = 0;
        capacity_       = 0;
        header_reserve_ = 0;
        inline_active_  = false;
        mode_           = mode::kOwned;
    }

    uint8_t                        *heap_           = nullptr; // owned-heap or borrowed external ptr (null when inline)
    size_t                          size_           = 0;       // used length
    size_t                          capacity_       = 0;       // usable capacity (kInlineCap when inline)
    size_t                          header_reserve_ = 0;       // reserved prefix length (owned cells)
    bool                            inline_active_  = false;   // owned && stored in inbuf_
    mode                            mode_           = mode::kOwned;
    std::array<uint8_t, kInlineCap> inbuf_{}; // SBO storage; valid iff inline_active_
};

} // namespace crowtree
