// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// DummyDiskEngine: wrapper around a real IoEngine (UringEngine or
// BlockingEngine) that provides dummy-disk semantics:
// - Optional read-content hack: after the inner engine completes a
//   pread, overwrites the buffer with deterministic pattern data
//   (for NullDisk benchmarks — the full uring/blocking flow executes
//   but read content is predesigned, not stored data).
// - Optional fault injection: per-I/O random latency and errors based
//   on DiskProperties (merged from the former SimulatedEngine).
//
// The inner engine submits real I/O to the dummy disk's memfd, so the
// full io_uring SQE→CQE round-trip (or blocking pwrite/pread) executes.
#pragma once

#include "disk/disk_properties.h"
#include "disk/types.h"
#include "engine/io_engine.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>

namespace crowdb::diskio
{

class DummyDiskEngine : public IoEngine
{
  public:
    // Construct with a shared inner engine (UringEngine or BlockingEngine).
    // If hack_reads is true, read completions overwrite the buffer with
    // pattern data (NullDisk). DiskProperties enables fault injection.
    DummyDiskEngine(std::shared_ptr<IoEngine> inner, bool hack_reads,
                    std::optional<DiskProperties> props = std::nullopt);

    void submit_write(Disk *disk, off_t phys_offset, const uint8_t *data, size_t size,
                      std::function<void(int)> on_complete) override;
    void submit_read(Disk *disk, off_t phys_offset, uint8_t *buf, size_t size, uint64_t test_pattern_offset,
                     std::function<void(int)> on_complete) override;
    void submit_fsync(Disk *disk, std::function<void(int)> on_complete) override;

  private:
    std::shared_ptr<IoEngine>     inner_;
    bool                          hack_reads_;
    std::optional<DiskProperties> props_;

    // Fill buf with deterministic pattern data for the given disk_id +
    // test_pattern_offset. Used by NullDisk read hack.
    static void fill_pattern(DiskId disk_id, uint64_t test_pattern_offset, uint8_t *buf, size_t size);

    // Draw a random latency from [props_.latency_min_ms, latency_max_ms].
    uint32_t draw_latency() const;
    // Draw a random double; if < error_rate, inject an error.
    bool draw_error() const;
};

} // namespace crowdb::diskio
