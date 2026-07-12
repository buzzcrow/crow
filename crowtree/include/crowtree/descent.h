// Tree descent (design-crowtree-core.md §3, §11): walk inner pages from the
// root PID down to the leaf PID whose key range contains `key`. Leaf chains
// (deltas) are resolved by the caller; descent only follows base inner pages.
#pragma once

#include "crowtree/mapping_table.h"
#include "crowtree/page.h"
#include "crowtree/slice.h"

namespace crowtree {

// Returns the leaf PID that should contain `key`, or kInvalidPID if the tree is
// empty / malformed. `resolve(pid)` maps a PID to its resident chain head,
// demand-loading an unloaded slot (design §4.5); it returns a real PageBase* or
// nullptr. `max_depth` guards against accidental cycles.
template <class Resolve>
inline uint64_t FindLeafPID(Resolve&& resolve, uint64_t root_pid, Slice key,
                            int max_depth = 64) {
  uint64_t pid = root_pid;
  for (int d = 0; d < max_depth; ++d) {
    PageBase* page = resolve(pid);
    if (page == nullptr) return kInvalidPID;
    // A leaf's mapping slot may point at a delta chain head; the chain shares the
    // leaf's PID, so the PID is already the answer once we reach a leaf level.
    PageBase* node = page;
    while (node != nullptr && node->type == PageType::kBatchDelta) node = node->next;
    if (node == nullptr) return kInvalidPID;
    if (node->type == PageType::kLeafBase) return pid;
    // Inner page: descend.
    auto* inner = static_cast<InnerBase*>(node);
    pid = inner->ChildFor(key);
  }
  return kInvalidPID;
}

}  // namespace crowtree
