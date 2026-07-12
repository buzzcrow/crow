// Slice: a non-owning view over a contiguous byte range, ordered
// lexicographically. Keys and values cross the engine as Slices.
#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace crowtree {

class Slice {
 public:
  Slice() : data_(nullptr), size_(0) {}
  Slice(const char* d, size_t n) : data_(d), size_(n) {}
  Slice(const uint8_t* d, size_t n)
      : data_(reinterpret_cast<const char*>(d)), size_(n) {}
  Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}
  Slice(std::string_view s) : data_(s.data()), size_(s.size()) {}
  Slice(const char* s) : data_(s), size_(std::strlen(s)) {}

  const char* data() const { return data_; }
  const uint8_t* bytes() const { return reinterpret_cast<const uint8_t*>(data_); }
  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

  std::string ToString() const { return std::string(data_, size_); }
  std::string_view ToView() const { return std::string_view(data_, size_); }

  // Lexicographic comparison: <0, 0, >0.
  int compare(const Slice& o) const {
    size_t n = size_ < o.size_ ? size_ : o.size_;
    int r = n == 0 ? 0 : std::memcmp(data_, o.data_, n);
    if (r != 0) return r;
    if (size_ < o.size_) return -1;
    if (size_ > o.size_) return 1;
    return 0;
  }

  bool starts_with(const Slice& prefix) const {
    return size_ >= prefix.size_ &&
           std::memcmp(data_, prefix.data_, prefix.size_) == 0;
  }

 private:
  const char* data_;
  size_t size_;
};

inline bool operator==(const Slice& a, const Slice& b) { return a.compare(b) == 0; }
inline bool operator!=(const Slice& a, const Slice& b) { return a.compare(b) != 0; }
inline bool operator<(const Slice& a, const Slice& b) { return a.compare(b) < 0; }
inline bool operator<=(const Slice& a, const Slice& b) { return a.compare(b) <= 0; }
inline bool operator>(const Slice& a, const Slice& b) { return a.compare(b) > 0; }
inline bool operator>=(const Slice& a, const Slice& b) { return a.compare(b) >= 0; }

}  // namespace crowtree
