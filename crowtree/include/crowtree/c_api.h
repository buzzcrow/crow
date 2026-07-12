// Stable C ABI for crowtree.
//
// A thin, exception-free C surface over the C++ engine so Rust (and any C
// caller) can drive it across the FFI boundary. All functions return ct_status
// (0 = ok, negative = error code matching crowtree::Code). Owned byte buffers
// returned to the caller (ct_buf) must be freed with ct_free_buf.
//
// v1 is synchronous (the engine's PageStore is blocking); the Rust adapter
// bridges this onto async via spawn_blocking.
#ifndef CROWTREE_C_API_H
#define CROWTREE_C_API_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int32_t ct_status;  // 0 = ok; negative mirrors crowtree::Code

// Opaque handles.
typedef struct ct_tree ct_tree;
typedef struct ct_view ct_view;
typedef struct ct_iter ct_iter;
typedef struct ct_export ct_export;
typedef struct ct_import ct_import;

// Owned byte buffer handed back to the caller; free with ct_free_buf.
typedef struct {
  uint8_t* data;
  size_t len;
} ct_buf;

void ct_free_buf(ct_buf* buf);

// Backend / engine configuration. `path` null or empty selects an in-memory
// store. Zero numeric fields take engine defaults.
typedef struct {
  const char* path;            // durable file path; null/empty => in-memory
  uint32_t iu_size;            // 0 => default (1 for mem, 4096 for file)
  uint32_t frame_bytes;        // 0 => default
  uint64_t buffer_pool_bytes;  // 0 => default
  uint8_t compression;         // 0 = none, 1 = lz4
  uint64_t max_inline_value;   // 0 => default (frame_bytes/4)
} ct_options;

// ── Lifecycle + durability ────────────────────────────────────────
ct_status ct_open(const ct_options* opt, ct_tree** out);
void ct_close(ct_tree* t);
ct_status ct_snapshot(ct_tree* t, uint64_t* out_last_applied);
uint64_t ct_last_applied_slot(const ct_tree* t);
void ct_set_gc_watermark(ct_tree* t, uint64_t safe_slot);
ct_status ct_collect_garbage(ct_tree* t);  // durable GC runs via snapshot
// Latched media-fault flag: 1 if a demand-load hit an I/O error or CRC mismatch
// on a committed page (the read degraded to a miss). ct_clear_io_error resets it.
int32_t ct_io_failed(const ct_tree* t);
void ct_clear_io_error(ct_tree* t);

// ── Data path ─────────────────────────────────────────────────────
ct_status ct_apply_put(ct_tree* t, uint64_t slot, const uint8_t* key, size_t klen,
                       const uint8_t* val, size_t vlen);
ct_status ct_apply_delete(ct_tree* t, uint64_t slot, const uint8_t* key, size_t klen);
void ct_force_advance_slot(ct_tree* t, uint64_t slot);

// Convenience: auto-assign the next slot and apply (single-writer only).
ct_status ct_put(ct_tree* t, const uint8_t* key, size_t klen, const uint8_t* val, size_t vlen);
ct_status ct_del(ct_tree* t, const uint8_t* key, size_t klen);

ct_status ct_flush(ct_tree* t);

// Point read. *found is 0/1; on found, *slot and *value (owned) are set.
ct_status ct_get(ct_tree* t, const uint8_t* key, size_t klen, int32_t* found, uint64_t* slot,
                 ct_buf* value);

// Range scan over `prefix` (empty = whole keyspace), up to `limit` (0 = all).
// `out_entries` is a packed owned buffer of records:
//   [u32 klen][key bytes][u64 slot][u32 vlen][value bytes] * count
// `out_count` receives the number of records; *truncated is set if more matched.
ct_status ct_scan(ct_tree* t, const uint8_t* prefix, size_t plen, size_t limit, ct_buf* out_entries,
                  uint64_t* out_count, int32_t* truncated);

// ── Consistent view (compare / iterate) ───────────────────────────
ct_status ct_snapshot_view(ct_tree* t, ct_view** out);
uint64_t ct_view_at_slot(const ct_view* v);
ct_status ct_view_iter(ct_view* v, ct_iter** out);
// Advance the iterator. *valid is 0 at end; otherwise key/value (owned) + slot +
// kind (0 put, 1 delete/tombstone) are set.
ct_status ct_iter_next(ct_iter* it, ct_buf* key, uint64_t* slot, uint8_t* kind, ct_buf* value,
                       int32_t* valid);
void ct_iter_release(ct_iter* it);
void ct_view_release(ct_view* v);

// ── Snapshot export / import (portable stream) ────────────────────
ct_status ct_snapshot_export_begin(ct_tree* t, ct_export** out);
ct_status ct_snapshot_export_next(ct_export* e, ct_buf* chunk, int32_t* done);
void ct_snapshot_export_end(ct_export* e);

ct_status ct_snapshot_import_begin(ct_tree* t, ct_import** out);
ct_status ct_snapshot_import_feed(ct_import* im, const uint8_t* chunk, size_t len);
ct_status ct_snapshot_import_finish(ct_import* im, uint64_t* out_at_slot);
void ct_snapshot_import_end(ct_import* im);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // CROWTREE_C_API_H
