// Snapshot export / import.
//
// The streaming byte form is the primitive: it feeds the network snapshot
// service for new-member install without ever touching local disk. Dumping to a
// `.ctsnap` file is just streaming into a file writer.
//
// v1 ships the **portable** format only: a versioned header, then key-sorted
// `(klen,key,slot,kind,vlen,value)` tuples (including tombstones), then a
// whole-stream CRC32C. It is deterministic and engine-independent, so an export
// re-imports identically on any backend / page size and is comparable against
// the in-mem oracle. The native frame-dump format is deferred.
//
// Key work: portable stream encode/decode, chunked export, CRC-checked import,
// file convenience wrappers.
#pragma once

#include "crowtree/slice.h"
#include "crowtree/status.h"

#include <cstdint>
#include <memory>
#include <string>

namespace crowtree
{

class Crowtree;

enum class snapshot_format : uint8_t {
    kPortable = 0, // v1 default: portable tuple stream
    kNative   = 1, // deferred: raw frame images + remapped manifest
};

// default_env export chunk size (design §9.1: fixed ≤1 MiB chunks).
inline constexpr size_t kSnapshotChunkBytes = 1U << 20;

// A resumable, chunked export over an immutable point-in-time view. The whole
// stream is materialized once at begin (deterministic), then sliced into chunks
// of at most `chunk_bytes`, so chunk boundaries are stable across exports.
class SnapshotExport
{
  public:
    SnapshotExport(std::string stream, size_t chunk_bytes, uint64_t at_slot)
        : stream_(std::move(stream)),
          chunk_bytes_(chunk_bytes == 0 ? kSnapshotChunkBytes : chunk_bytes),
          at_slot_(at_slot)
    {
    }

    // Copy the next chunk into *out (<= chunk_bytes). Sets *done=true once the
    // final chunk has been returned (an empty stream returns one empty final
    // chunk).
    Status next_chunk(std::string *out, bool *done);

    [[nodiscard]] uint64_t at_slot() const
    {
        return at_slot_;
    }

    [[nodiscard]] size_t total_bytes() const
    {
        return stream_.size();
    }

  private:
    std::string stream_;
    size_t      pos_ = 0;
    size_t      chunk_bytes_;
    uint64_t    at_slot_;
};

// Begin a snapshot export of `tree`: exports the current durable view (at the
// engine's last_applied_slot, recorded in the stream header). Both kPortable
// and kNative (plan-tree #16) are supported. (Historical/arbitrary-slot
// export is deferred until path-copy COW RootVersions exist; there is no
// slot selector.)
Status snapshot_export_begin(Crowtree &tree, snapshot_format fmt, size_t chunk_bytes,
                             std::unique_ptr<SnapshotExport> *out);

// Convenience: dump a whole snapshot to a `.ctsnap` file (loops next_chunk).
Status snapshot_dump_to_file(Crowtree &tree, snapshot_format fmt, const std::string &path);

// Accumulates a portable stream and atomically installs it into `tree` on
// finish (verifying the whole-stream CRC first).
class SnapshotImport
{
  public:
    explicit SnapshotImport(Crowtree &tree) : tree_(tree)
    {
    }

    // feed the next chunk of bytes (order matters; chunks are concatenated).
    Status feed(Slice chunk);

    // Parse + verify the accumulated stream, then replace the engine state.
    // Returns the snapshot's slot via *out_at_slot (if non-null).
    Status finish(uint64_t *out_at_slot);

  private:
    Status      finish_native(const uint8_t *p, size_t len, uint64_t *out_at_slot);
    Crowtree   &tree_;
    std::string buf_;
};

// Convenience: load a `.ctsnap` file into `tree`.
Status snapshot_load_from_file(Crowtree &tree, const std::string &path);

} // namespace crowtree
