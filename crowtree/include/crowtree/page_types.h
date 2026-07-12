// Shared page primitives (design-crowtree-core.md §3): the page-type tag, the
// common chain header, and the leaf entry. Split out of page.h so the frame
// format (frame_page.h) and the page classes (page.h) can both depend on these
// without a circular include.
#pragma once

#include <cstdint>
#include <string>

namespace crowtree {

inline constexpr uint64_t kInvalidPID = ~0ull;

enum class PageType : uint8_t {
  kLeafBase = 1,
  kInnerBase = 2,
  kBatchDelta = 3,
};

inline bool IsBase(PageType t) {
  return t == PageType::kLeafBase || t == PageType::kInnerBase;
}

// Common header for every node reachable through the mapping table. Delta nodes
// chain in front of a base via `next`; bases have next == nullptr.
struct PageBase {
  explicit PageBase(PageType t) : type(t) {}
  virtual ~PageBase() = default;

  PageType type;
  PageBase* next = nullptr;    // chain link: delta -> ... -> base
  uint32_t delta_len = 0;      // # of deltas above the base (0 on a base)
  size_t chain_bytes = 0;      // approx bytes of this node + below (triggers)
  uint64_t pid = kInvalidPID;  // logical page id (set on install)
};

// One leaf entry: key + encoded slot-aware cell payload (see cell.h).
struct LeafEntry {
  std::string key;
  std::string cell;  // encoded CellView payload
};

}  // namespace crowtree
