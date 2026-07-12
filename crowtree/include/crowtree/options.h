// Options: tunables for consolidation, flush triggers, and page split/merge.
// Defaults follow design-crowtree-core.md.
#pragma once

#include <cstddef>
#include <cstdint>

namespace crowtree {

struct Options {
  // ── Consolidation (core doc §7) ──
  // Fold a leaf's delta chain into a fresh base when either threshold trips.
  uint32_t max_delta_len = 8;
  size_t max_delta_bytes = 256 * 1024;  // 256 KiB

  // ── Leaf split / merge (core doc §8) ──
  // Split when a consolidated leaf exceeds leaf_split_bytes; merge when it
  // drops below leaf_merge_bytes. Hysteresis: merge threshold is well below
  // split to avoid oscillation.
  size_t leaf_split_bytes = 64 * 1024;  // 64 KiB target page size
  size_t leaf_merge_bytes = 16 * 1024;  // split/4

  // Inner-page fanout bound (separator count) before an inner split.
  uint32_t inner_max_keys = 256;

  // ── MemTable flush triggers (core doc §6.2) ──
  size_t memtable_flush_bytes = 4 * 1024 * 1024;  // 4 MiB
  uint32_t memtable_flush_entries = 100000;
  uint64_t flush_interval_ms = 200;  // time-based flush

  // Run the flusher on a background thread. Tests often set this false and
  // drive flush() synchronously for determinism.
  bool background_flush = false;
};

}  // namespace crowtree
