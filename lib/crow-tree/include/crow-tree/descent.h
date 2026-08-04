// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Tree descent: walk inner pages from the
// root PID down to the leaf PID whose key range contains `key`. Leaf chains
// (deltas) are resolved by the caller; descent only follows base inner pages.
#pragma once

#include "crow-tree/mapping_table.h"
#include "crow-tree/page.h"
#include "crow-tree/slice.h"

namespace crow::tree
{

// Returns the leaf PID that should contain `key`, or kInvalidPageId if the tree is
// empty / malformed. `resolve(page_id)` maps a PID to its resident chain head,
// demand-loading an unloaded slot; it returns a real PageBase* or
// nullptr. `max_depth` guards against accidental cycles.
template <class Resolve>
[[nodiscard]] inline uint64_t find_leaf_page_id(Resolve &&resolve, uint64_t root_page_id, Slice key, int max_depth = 64)
{
    uint64_t page_id = root_page_id;
    for (int d = 0; d < max_depth; ++d) {
        PageBase *page = resolve(page_id);
        if (page == nullptr) {
            return kInvalidPageId;
        }
        // A leaf's mapping slot may point at a delta chain head; the chain shares the
        // leaf's PID, so the PID is already the answer once we reach a leaf level.
        PageBase *node = page;
        while (node != nullptr && node->type == page_type::kBatchDelta) {
            node = node->next;
        }
        if (node == nullptr) {
            return kInvalidPageId;
        }
        if (node->type == page_type::kLeafBase) {
            return page_id;
        }
        // Inner page: descend.
        auto *inner = static_cast<InnerBase *>(node);
        page_id     = inner->child_for(key);
    }
    return kInvalidPageId;
}

} // namespace crow::tree
