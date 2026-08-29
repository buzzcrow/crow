// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-tree/slice.h"

#include <cstdint>
#include <cstring>
#include <utility>

namespace crowdb::tree
{

// Growing byte buffer for scan's packed wire format, using std::malloc /
// std::realloc so the raw pointer can be transferred across the FFI and
// freed by ct_free_buf's std::free (R57: zero-copy scan result staging).
// The wire format per entry is:
//   [u32 klen][key][u64 slot][u8 tombstone][u32 vlen][value]
class ScanPackedBuf
{
  public:
    ScanPackedBuf() = default;

    ~ScanPackedBuf()
    {
        std::free(data_);
    }

    ScanPackedBuf(const ScanPackedBuf &)            = delete;
    ScanPackedBuf &operator=(const ScanPackedBuf &) = delete;

    ScanPackedBuf(ScanPackedBuf &&o) noexcept : data_(o.data_), size_(o.size_), cap_(o.cap_)
    {
        o.data_ = nullptr;
        o.size_ = 0;
        o.cap_  = 0;
    }

    ScanPackedBuf &operator=(ScanPackedBuf &&o) noexcept
    {
        if (this != &o) {
            std::free(data_);
            data_   = o.data_;
            size_   = o.size_;
            cap_    = o.cap_;
            o.data_ = nullptr;
            o.size_ = 0;
            o.cap_  = 0;
        }
        return *this;
    }

    void reserve(size_t n) // NOLINT(readability-convert-member-functions-to-static) modifies cap_ via grow
    {
        if (n <= cap_) {
            return;
        }
        grow(n);
    }

    void pack_u32(uint32_t v) // NOLINT(readability-convert-member-functions-to-static) modifies data_/size_ via ensure
    {
        ensure(4);
        for (int i = 0; i < 4; ++i) {
            data_[size_++] = static_cast<uint8_t>((v >> (8 * i)) & 0xff);
        }
    }

    void pack_u64(uint64_t v) // NOLINT(readability-convert-member-functions-to-static) modifies data_/size_ via ensure
    {
        ensure(8);
        for (int i = 0; i < 8; ++i) {
            data_[size_++] = static_cast<uint8_t>((v >> (8 * i)) & 0xff);
        }
    }

    void push_back(uint8_t b)
    {
        ensure(1);
        data_[size_++] = b;
    }

    void append(const char *p,
                size_t      n) // NOLINT(readability-convert-member-functions-to-static) modifies data_/size_ via ensure
    {
        if (n == 0) {
            return;
        }
        ensure(n);
        std::memcpy(data_ + size_, p, n);
        size_ += n;
    }

    void append(const uint8_t *p, size_t n)
    {
        append(reinterpret_cast<const char *>(p), n);
    }

    void append(Slice s)
    {
        append(s.data(), s.size());
    }

    [[nodiscard]] size_t size() const
    {
        return size_;
    }

    [[nodiscard]] const uint8_t *data() const
    {
        return data_;
    }

    // Transfer ownership of the internal buffer to the caller. The
    // ScanPackedBuf no longer owns it and will not free it. The caller
    // must std::free the returned pointer.
    [[nodiscard]] uint8_t *release()
    {
        uint8_t *p = data_;
        data_      = nullptr;
        size_      = 0;
        cap_       = 0;
        return p;
    }

  private:
    void ensure(size_t need) // NOLINT(readability-convert-member-functions-to-static) modifies data_/cap_ via grow
    {
        if (size_ + need > cap_) {
            grow(size_ + need);
        }
    }

    void grow(size_t min_cap)
    {
        size_t new_cap = (cap_ == 0) ? 256 : cap_ * 2;
        while (new_cap < min_cap) {
            new_cap *= 2;
        }
        auto *p = static_cast<uint8_t *>(std::realloc(data_, new_cap));
        data_   = p;
        cap_    = new_cap;
    }

    uint8_t *data_ = nullptr;
    size_t   size_ = 0;
    size_t   cap_  = 0;
};

} // namespace crowdb::tree
