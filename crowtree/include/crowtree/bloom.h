// A small Bloom filter for leaf pages (design-crowtree-core.md §3): a fast
// negative check before scanning a leaf's entries. Double-hashing scheme.
#pragma once

#include "crowtree/slice.h"

#include <cmath>
#include <cstdint>
#include <vector>

namespace crowtree {

// FNV-1a 64-bit hash.
inline uint64_t Fnv1a64(Slice s) {
  uint64_t h = 1469598103934665603ull;
  const uint8_t* p = s.bytes();
  for (size_t i = 0; i < s.size(); ++i) {
    h ^= p[i];
    h *= 1099511628211ull;
  }
  return h;
}

class BloomFilter {
 public:
  // Size for `n_keys` at `bits_per_key` bits each.
  void Init(size_t n_keys, uint32_t bits_per_key = 10) {
    uint64_t bits = static_cast<uint64_t>(n_keys) * bits_per_key;
    if (bits < 64) bits = 64;
    size_t words = static_cast<size_t>((bits + 63) / 64);
    words_.assign(words, 0);
    num_bits_ = static_cast<uint64_t>(words) * 64;
    k_ = static_cast<uint32_t>(bits_per_key * 0.69);  // ln2
    if (k_ < 1) k_ = 1;
    if (k_ > 16) k_ = 16;
  }

  void Add(Slice key) {
    uint64_t h = Fnv1a64(key);
    uint32_t h1 = static_cast<uint32_t>(h);
    uint32_t h2 = static_cast<uint32_t>(h >> 32) | 1;  // odd, non-zero
    for (uint32_t i = 0; i < k_; ++i) {
      uint64_t bit = (static_cast<uint64_t>(h1) + static_cast<uint64_t>(i) * h2) % num_bits_;
      words_[bit / 64] |= (1ull << (bit % 64));
    }
  }

  bool MaybeContains(Slice key) const {
    if (num_bits_ == 0) return true;  // empty filter: never a false negative
    uint64_t h = Fnv1a64(key);
    uint32_t h1 = static_cast<uint32_t>(h);
    uint32_t h2 = static_cast<uint32_t>(h >> 32) | 1;
    for (uint32_t i = 0; i < k_; ++i) {
      uint64_t bit = (static_cast<uint64_t>(h1) + static_cast<uint64_t>(i) * h2) % num_bits_;
      if ((words_[bit / 64] & (1ull << (bit % 64))) == 0) return false;
    }
    return true;
  }

 private:
  std::vector<uint64_t> words_;
  uint64_t num_bits_ = 0;
  uint32_t k_ = 1;
};

}  // namespace crowtree
