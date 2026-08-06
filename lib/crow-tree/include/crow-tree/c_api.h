// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Stable C ABI for crow-tree.
//
// A thin, exception-free C surface over the C++ engine so Rust (and any C
// caller) can drive it across the FFI boundary. All functions return ct_status
// (0 = ok, negative = error code matching crow::tree::Code). Owned byte buffers
// returned to the caller (ct_buf) must be freed with ct_free_buf.
//
// v1 is synchronous (the engine's PageStore is blocking); the Rust adapter
// bridges this onto async via spawn_blocking.
#ifndef CROW_TREE_C_API_H
#define CROW_TREE_C_API_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

using ct_status = int32_t; // 0 = ok; negative mirrors crow::tree::Code

// Opaque handles.
using ct_tree         = struct ct_tree;
using ct_view         = struct ct_view;
using ct_iter         = struct ct_iter;
using ct_export       = struct ct_export;
using ct_import       = struct ct_import;
using ct_write_handle = struct ct_write_handle;

// Owned byte buffer handed back to the caller; free with ct_free_buf.
using ct_buf = struct
{
    uint8_t *data;
    size_t   len;
};

void ct_free_buf(ct_buf *buf);

// Result of an explicit ct_collect_garbage sweep (plan-tree #21); mirrors
// crow::tree::GcStats.
using ct_gc_stats = struct
{
    uint64_t tombstones_dropped;
    uint64_t pages_freed;
    uint64_t bytes_freed;
};

// Batched diagnostics snapshot; mirrors crow::tree::EngineStats. Every field
// is O(1) (an already-tracked atomic counter or BufferPool::stats()), so
// ct_get_stats is safe to poll periodically (metrics scrape / console
// panel refresh) without touching the tree itself.
using ct_stats = struct
{
    uint64_t last_applied_slot;
    uint64_t contiguous_slot;
    uint64_t gc_watermark;
    int32_t  io_failed; // 0/1
    uint64_t snapshot_pages_written;
    uint64_t snapshot_pages_total;
    uint64_t snapshot_segments_written;
    uint64_t buffer_pool_hits;
    uint64_t buffer_pool_misses;
    uint64_t buffer_pool_evictions;
    uint64_t buffer_pool_writebacks;
    uint32_t buffer_pool_resident;
    uint32_t buffer_pool_dirty;
    uint32_t buffer_pool_used;
    uint32_t buffer_pool_num_frames;
    // MemTable (L0) / flush / L1 cumulative counters (monotonic since open).
    uint64_t mt_upsert_total;
    uint64_t mt_get_total;
    uint64_t mt_get_hit_total;
    uint64_t flush_drain_total;
    uint64_t flush_entries_total;
    uint64_t l1_get_total;
    uint64_t l1_get_hit_total;
};

// Backend selection for durable storage.
enum ct_backend : uint8_t {
    CT_BACKEND_FILE      = 0, // FilePageStore (file-based, no alignment)
    CT_BACKEND_BLOCK     = 1, // BlockPageStore (block device, 4K aligned, O_DIRECT)
    CT_BACKEND_MEM_BLOCK = 2, // MemPageStore (in-memory block device, no alignment)
};

// Durability barrier policy.
enum ct_sync_mode : uint8_t {
    CT_SYNC_FULL  = 0, // fdatasync after every flush (default)
    CT_SYNC_SKIP  = 1, // no fsync (tests/CI)
    CT_SYNC_BATCH = 2, // fsync once per snapshot commit
};

// Backend / engine configuration. `path` null or empty selects an in-memory
// store. Zero numeric fields take engine defaults.
using ct_options = struct
{
    const char       *path;              // durable file path; null/empty => in-memory
    uint32_t          iu_size;           // 0 => default (1 for mem, 4096 for file)
    uint32_t          frame_bytes;       // 0 => default
    uint64_t          buffer_pool_bytes; // 0 => default
    uint8_t           compression;       // 0 = none, 1 = lz4
    uint64_t          max_inline_value;  // 0 => default (frame_bytes/4)
    enum ct_backend   backend;           // default CT_BACKEND_BLOCK; ignored for in-memory
    uint64_t          block_size;        // 0 => default 64 MiB; ignored for file/mem-block
    uint32_t          store_id;          // default 0; block file naming
    uint32_t          group_id;          // default 0; maps to PxGroupId in CrowKV
    enum ct_sync_mode sync_mode;         // default CT_SYNC_FULL
    const char       *log_dir;           // null/empty => no C++ file logging
    const char       *log_level;         // null/empty => "info"
    const char       *log_file_prefix;   // null/empty => "crow-tree"; filename prefix
    size_t            log_max_file_mb;   // 0 => default 30
    size_t            log_max_files;     // 0 => default 5
};

// ── Lifecycle + durability ────────────────────────────────────────
ct_status ct_open(const ct_options *opt, ct_tree **out);
void      ct_close(ct_tree *t);

// Process-global logging control (not bound to any ct_tree instance).
// Call ct_init_logging once at process startup (before any ct_open),
// ct_flush_logging to push buffered messages to disk, and
// ct_shutdown_logging at process exit (after all ct_close calls) to
// flush + join the async logger thread. All three are safe to call
// when logging was never initialized (no-op).
void      ct_init_logging(const char *log_dir, const char *level, size_t max_file_mb, size_t max_files,
                          const char *file_prefix);
void      ct_flush_logging();
void      ct_shutdown_logging();
ct_status ct_snapshot(ct_tree *t, uint64_t *out_last_applied);
uint64_t  ct_last_applied_slot(const ct_tree *t);
// gc_slot = min(snapshot_slot, safe_slot); see crow::tree::set_gc_watermark.
void ct_set_gc_watermark(ct_tree *t, uint64_t snapshot_slot, uint64_t safe_slot);
// Explicit in-memory tombstone-retention sweep (crow::tree::collect_garbage);
// does NOT persist -- call ct_snapshot separately for durable GC of dead
// on-disk extents. out_stats may be null.
ct_status ct_collect_garbage(ct_tree *t, ct_gc_stats *out_stats);
// Latched media-fault flag: 1 if a demand-load hit an I/O error or CRC mismatch
// on a committed page (the read degraded to a miss). ct_clear_io_error resets it.
int32_t ct_io_failed(const ct_tree *t);
void    ct_clear_io_error(ct_tree *t);

// Wipe every key/value back to a fresh, empty tree (crow::tree::Crowtree::clear;
// the same wipe ct_snapshot_import_finish performs before loading imported
// entries, exposed standalone for a caller with nothing to load afterward).
// Not durable by itself -- call ct_snapshot separately to persist the wipe to
// a file-backed store.
ct_status ct_clear(ct_tree *t);

// Batched diagnostics snapshot (crow::tree::Crowtree::stats; see ct_stats's
// own doc comment). `out` must be non-null; a no-op (out left untouched)
// if `t` is null.
void ct_get_stats(const ct_tree *t, ct_stats *out);

// Flush C++ metrics into a formatted string (for FFI return to Rust).
// Returns a malloc'd null-terminated string; caller must ct_free_string it.
// Returns nullptr if t is null or no metrics registry is configured.
// `width` overrides per-section max name length (0 = use internal max).
char *ct_flush_metrics_str(ct_tree *t, double window_secs, const char *timestamp, size_t width);

// Extended flush with negotiated column widths (count_w, tps_w).
char *ct_flush_metrics_str_ext(ct_tree *t, double window_secs, const char *timestamp, size_t width, size_t count_w,
                               size_t tps_w);

// Return the current max metric name length from the C++ registry.
size_t ct_max_name_len(const ct_tree *t);

// Shared column widths for cross-language alignment.
// count_w and tps_w are the 2nd and 3rd column widths.
// NOLINTNEXTLINE(modernize-use-using)
typedef struct
{
    size_t count_w;
    size_t tps_w;
} ct_column_widths;

// Negotiate column widths: caller passes its preferred widths,
// C++ returns its preferred widths in *out. Both sides then use max.
void ct_negotiate_widths(const ct_tree *t, ct_column_widths input, ct_column_widths *out);

// Free a string returned by ct_flush_metrics_str.
void ct_free_string(char *s);

// Evict clean, delta-free resident leaf bases down to at most
// `max_resident_leaves` (crow::tree::Crowtree::evict_clean_leaves).
// Test/ops hook -- forces the demand-load path a subsequent ct_get/
// ct_get_async will have to take. Returns the number of leaves evicted.
uint64_t ct_evict_clean_leaves(ct_tree *t, uint64_t max_resident_leaves);

// #17 D3: same contract, but for resident *inner* bases, down
// to at most `max_resident_inner` (crow::tree::Crowtree::evict_clean_inner) --
// a genuinely separate ranked budget/pass from ct_evict_clean_leaves, never
// evicting a leaf. Returns the number of inner bases evicted.
uint64_t ct_evict_clean_inner(ct_tree *t, uint64_t max_resident_inner);

// ── Data path ─────────────────────────────────────────────────────
ct_status ct_apply_put(ct_tree *t, uint64_t slot, const uint8_t *key, size_t klen, const uint8_t *val, size_t vlen);
ct_status ct_apply_delete(ct_tree *t, uint64_t slot, const uint8_t *key, size_t klen);
void      ct_force_advance_slot(ct_tree *t, uint64_t slot);

// ── Zero-copy write path (R3) ──────────────────────────────────────
//
// Handle-based alloc-then-apply: the caller writes key and value bytes
// directly into crow-tree-owned memory, then ct_apply_put_owned consumes
// the handle with zero value memcpy (the cell header is written at apply
// time; the value region was filled by the caller). The cell header
// layout (kCellHeaderSize) is internal — the C API never exposes it.
//
// Lifecycle: ct_alloc → write key/val → ct_apply_put_owned (consumes)
//            ct_alloc → ct_free_handle (error/cancel path)
//
// For small values (≤ 15 B) the cell buffer is inline (SBO, no malloc) —
// same as ct_apply_put's internal path, no regression. For small keys
// (≤ ~15 B) the key string is inline (std::string SSO) — same as today.

// Writable pointers returned by ct_alloc.
using ct_write_ptrs = struct
{
    uint8_t *key; // [0, key_len) — caller fills key bytes
    uint8_t *val; // [0, val_len) — caller fills value bytes
};

// Allocate crow-tree-owned memory for a key + value write. `t` may be null
// (allocates without tree validation); if non-null, key_len is validated
// against the tree's max_key_size. Returns an opaque handle plus writable
// pointers via `out_ptrs`. The pointers are valid until ct_apply_put_owned
// or ct_free_handle is called.
ct_status ct_alloc(ct_tree *t, size_t key_len, size_t val_len, ct_write_handle **out_handle, ct_write_ptrs *out_ptrs);

// Apply a pre-allocated key+value at `slot` (zero value memcpy). Writes
// the cell header (slot + put flags) into the pre-allocated cell, then
// moves key+cell into apply_encoded. Consumes and frees the handle.
ct_status ct_apply_put_owned(ct_tree *t, uint64_t slot, ct_write_handle *handle);

// Free a handle that was never applied (error/cancel path). No-op if null.
void ct_free_handle(ct_write_handle *handle);

// Apply a multi-key batch atomically at `slot` (single call into
// Crowtree::apply, so it's atomic to readers -- unlike looping ct_apply_put/
// ct_apply_delete per key, which would let a reader observe a partially
// applied batch). `ops` is a packed buffer of `count` records:
//   [u8 kind (0=put,1=delete)][u32 klen][key bytes][u32 vlen][value bytes]
// `vlen`/value bytes are 0-length for a delete record.
ct_status ct_apply_batch(ct_tree *t, uint64_t slot, const uint8_t *ops, size_t ops_len, uint64_t count);

// One key/value reference for ct_apply_batch_slices — non-owning pointers
// into the caller's buffers (must outlive the call). kind: 0 = put, 1 = delete.
using ct_kv_ref = struct
{
    const uint8_t *key;
    size_t         key_len;
    const uint8_t *value; // null/zero-len for delete
    size_t         value_len;
    uint8_t        kind;
};

// Same semantics as ct_apply_batch but accepts an array of ct_kv_ref structs
// instead of a packed buffer — eliminates the Rust-side packing copy.
ct_status ct_apply_batch_slices(ct_tree *t, uint64_t slot, const ct_kv_ref *ops, uint64_t count);

// One zero-copy key/value op for ct_apply_batch_external (R30). `value` is a
// non-owning pointer into Rust-owned memory (a `bytes::Bytes` slice); crow-tree
// borrows it via a kExternal buffer and calls `drop_fn(bytes_ref)` when the
// buffer is freed (at MemTable drain/overwrite). `bytes_ref` is an opaque
// Rust handle (e.g. a boxed `Arc<Bytes>`); `drop_fn` decrements the Rust
// refcount. kind: 0 = put, 1 = delete (value/value_len/bytes_ref/drop_fn are
// unused for delete).
using ct_ext_op = struct
{
    const uint8_t *key;
    size_t         key_len;
    const uint8_t *value;     // borrowed from Rust Bytes (put only)
    size_t         value_len; // 0 for delete
    uint8_t        kind;      // 0 = put, 1 = delete
    void          *bytes_ref; // opaque Rust handle (put only); NULL for delete
    void (*drop_fn)(void *);  // Rust drop callback (put only); NULL for delete
};

// Zero-copy apply (R30): same semantics as ct_apply_batch_slices (atomic
// multi-key apply, highest-slot-wins, oversized-key rejection) but the value
// bytes are borrowed from Rust-owned memory instead of copied at the FFI
// boundary. The value memcpy is deferred to MemTable drain (flush, off the
// apply critical path). Ownership of every `bytes_ref` transfers to crow-tree;
// `drop_fn` is called exactly once per op when the borrowed buffer is freed.
ct_status ct_apply_batch_external(ct_tree *t, uint64_t slot, const ct_ext_op *ops, uint64_t count);

// Convenience: auto-assign the next slot and apply (single-writer only).
ct_status ct_put(ct_tree *t, const uint8_t *key, size_t klen, const uint8_t *val, size_t vlen);
ct_status ct_del(ct_tree *t, const uint8_t *key, size_t klen);

ct_status ct_flush(ct_tree *t);

// Point read. *found is 0/1; on found, *slot and *value (owned) are set.
ct_status ct_get(ct_tree *t, const uint8_t *key, size_t klen, int32_t *found, uint64_t *slot, ct_buf *value);

// ── Async data path ──
//
// Opaque completion handle returned by the ct_*_async calls below; poll it
// with ct_future_poll. For ct_flush_async/ct_snapshot_async, a future that
// ct_future_poll reports done (*done=1) is freed by that same call -- do not
// also call ct_future_free on it. For ct_get_async specifically, done=1 does
// *not* free it (zero-copy fast path:
// *out_value may borrow bytes from a resident frame, kept alive by an epoch
// guard this ct_future owns) -- the caller must always follow up with
// ct_future_free once done reading *out_value, for both a found and a
// not-found/errored result. ct_future_free is otherwise only for abandoning
// a still-pending future early (e.g. the Rust Future was dropped/cancelled
// before completion); calling it twice, or calling it after ct_future_poll
// already freed a flush/snapshot future, is undefined behavior.
using ct_future = struct ct_future;

// Fast path (no demand-load needed): the returned future already reports
// done=1 on the very first ct_future_poll call. Slow path (page must be
// loaded): the future stays pending until the tree's Reactor completes the
// I/O (or, with no Reactor wired -- e.g. an in-memory tree, or a build
// without liburing -- until it falls back to a synchronous load; still
// correct, just not genuinely async). Returns null only if
// `t` is itself null.
ct_future *ct_get_async(ct_tree *t, const uint8_t *key, size_t klen);

// flush()/snapshot() twins. flush_async's future never has genuine I/O to
// wait on (flush only touches the in-memory L1, never page_store) so it
// always completes synchronously, same as the fast path above; it exists
// so Rust has one uniform ct_future-based shape for all three ops.
// snapshot_async's future, on completion, carries the durable
// last_applied_slot in ct_future_poll's *out_slot (mirrors ct_snapshot's
// own out param); *out_found and *out_value are unused for both.
ct_future *ct_flush_async(ct_tree *t);
ct_future *ct_snapshot_async(ct_tree *t);

// scan() twin. Unlike ct_get_async, a single
// pending scan may need more than one page load (one per cold leaf, plus
// possibly the initial root->leaf descent) -- each is resolved by retrying
// the whole scan from scratch (still correct: every retry either resolves
// or permanently loads one more page, so it always terminates; see
// Crowtree::scan_async's doc comment). On completion (ct_future_poll),
// *out_value carries the same packed record format as ct_scan
// (`[u32 klen][key][u64 slot][u8 tombstone][u32 vlen][val]*`, always a *malloc'd*, owned
// buffer -- pass it to ct_free_buf, no zero-copy borrow attempted here,
// unlike ct_get_async), *out_slot carries the entry count (mirrors
// ct_scan's *out_count), and *out_found carries the truncated flag (0/1,
// mirrors ct_scan's *truncated). No borrowed state, so ct_future_poll frees
// `f` immediately once done, same as flush/snapshot -- do not also call
// ct_future_free.
ct_future *ct_scan_async(ct_tree *t, const uint8_t *prefix, size_t plen, const uint8_t *start_after, size_t salen,
                         size_t limit, size_t byte_budget);

// Non-blocking poll.
// *done == 0: still pending; f remains valid, poll again later (e.g. after
//   the Rust side's AsyncFd wakes on ct_reactor_eventfd()).
// *done == 1: the returned ct_status is the underlying operation's result
//   (mirrors what ct_get/ct_flush/ct_snapshot/ct_scan would have returned).
//   - ct_flush_async/ct_snapshot_async/ct_scan_async: f has been freed by
//     this call (do not also call ct_future_free). *out_slot carries
//     snapshot_async's durable last_applied_slot, or scan_async's entry
//     count; *out_found carries scan_async's truncated flag; *out_value
//     carries scan_async's packed, owned entries buffer (pass to
//     ct_free_buf) -- otherwise untouched.
//   - ct_get_async: f is NOT freed by this call -- see ct_future's own doc
//     comment above. *out_found is 0/1 and, if found, *out_slot and
//     *out_value are set; *out_value may be a *borrowed* pointer into a
//     resident frame (do not pass it to ct_free_buf) valid only until the
//     caller's next ct_future_free call, which must always follow. A
//     not-found/errored get leaves *out_slot/*out_value untouched (still
//     requires ct_future_free).
//   Any of out_found/out_slot/out_value may be null if the caller doesn't
//   need them.
ct_status ct_future_poll(ct_future *f, int32_t *done, int32_t *out_found, uint64_t *out_slot, ct_buf *out_value);

// Best-effort cancel + free of a still-pending future: the
// underlying I/O (if any is in flight) is not actually interrupted -- it
// runs to completion in the background and its result is simply discarded
// -- but `f` itself is safe to drop immediately; a no-op if f is null. Also
// how the caller *must* release a ct_get_async future once done reading
// out_value -- see ct_future's own doc comment above. Never call this on a
// ct_flush_async/ct_snapshot_async future already resolved by
// ct_future_poll (ct_get_async futures are the one exception: always call
// this after a resolved poll, never before).
void ct_future_free(ct_future *f);

// The tree's Reactor eventfd, for the Rust side to register with
// tokio::io::AsyncFd: it becomes readable after the Reactor
// dispatches a batch of completions, so re-polling every pending future at
// that point will observe any that just finished. Returns -1 if this tree
// has no Reactor wired (in-memory tree, or a build without liburing) --
// ct_*_async calls still work in that case, they just always complete
// synchronously (nothing to wait on). Reactor-owned; do not close it.
int32_t ct_reactor_eventfd(const ct_tree *t);

// Range scan over `prefix` (empty = whole keyspace), up to `limit` (0 = all).
// `start_after` (null or salen = 0 = start from beginning) is an exclusive
// lower bound: only keys strictly greater than `start_after` are returned,
// enabling cursor-based pagination without over-fetching the prefix range.
// When `include_tombstones` is 1, tombstone entries are included in results.
// `out_entries` is a packed owned buffer of records:
//   [u32 klen][key bytes][u64 slot][u8 tombstone][u32 vlen][value bytes] * count
// `out_count` receives the number of records; *truncated is set if more matched.
ct_status ct_scan(ct_tree *t, const uint8_t *prefix, size_t plen, const uint8_t *start_after, size_t salen,
                  size_t limit, size_t byte_budget, int include_tombstones, ct_buf *out_entries, uint64_t *out_count,
                  int32_t *truncated);

// ── Consistent view (compare / iterate) ───────────────────────────
ct_status ct_snapshot_view(ct_tree *t, ct_view **out);
uint64_t  ct_view_at_slot(const ct_view *v);
ct_status ct_view_iter(ct_view *v, ct_iter **out);
// Advance the iterator. *valid is 0 at end; otherwise key/value (owned) + slot +
// kind (0 put, 1 delete/tombstone) are set.
ct_status ct_iter_next(ct_iter *it, ct_buf *key, uint64_t *slot, uint8_t *kind, ct_buf *value, int32_t *valid);
void      ct_iter_release(ct_iter *it);
void      ct_view_release(ct_view *v);

// ── Snapshot export / import (portable stream) ────────────────────
ct_status ct_snapshot_export_begin(ct_tree *t, ct_export **out);
ct_status ct_snapshot_export_next(ct_export *e, ct_buf *chunk, int32_t *done);
void      ct_snapshot_export_end(ct_export *e);

ct_status ct_snapshot_import_begin(ct_tree *t, ct_import **out);
ct_status ct_snapshot_import_feed(ct_import *im, const uint8_t *chunk, size_t len);
ct_status ct_snapshot_import_finish(ct_import *im, uint64_t *out_at_slot);
void      ct_snapshot_import_end(ct_import *im);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // CROW_TREE_C_API_H
