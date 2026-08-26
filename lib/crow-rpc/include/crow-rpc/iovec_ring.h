// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Pre-allocated iovec ring buffer for writev scatter-gather.
// Inspired by buzz-cpp's socket_send_queue: iovecs are stored directly
// in a fixed-size ring, so writev reads from the ring without per-call
// allocation. On partial write, the iovec is modified in place (base +=
// count, len -= count) — no re-enqueue, no pending vector.
//
// Each ring slot tracks:
//   - iovec (base + len for writev)
//   - OutFrame* (for buffer release + delete after full send)
//   - header storage (14-byte serialized header, stack-allocated in slot)
//
// Lifecycle:
//   offer(frame) → converts frame to 1-3 iovecs at end_, stores frame*
//   send(fd)     → writev on [begin_, end_), advances begin_ on full send,
//                  modifies iovec in place on partial
//   has_pending() → begin_ != end_
#pragma once

#include "crow-rpc/buffer.h"
#include "crow-rpc/framing.h"
#include "crow-rpc/transport.h"

#include <sys/uio.h>

#include <atomic>
#include <cstdint>

namespace crow::rpc
{
struct TransportStats; // forward declaration — defined in socket_transport.h
}

namespace crow::rpc
{

// Ring buffer capacity in OutFrame slots. Each frame produces up to 3
// iovecs, so the total iovec count is at most 3 * IOVEC_RING_FRAMES.
// IOV_MAX is 1024 on Linux, so 340 frames × 3 = 1020 iovecs (safe).
constexpr uint32_t IOVEC_RING_FRAMES = 340;

class IovecRing
{
  public:
    IovecRing();

    // Convert frame to 1-3 iovecs and store at end_. Returns false if
    // the ring is full (backpressure — caller should retry via EPOLLOUT).
    bool offer(OutFrame *frame);

    // writev all pending iovecs on the ring. Returns:
    //   >0: total bytes written (all sent, ring drained)
    //    0: nothing to send (ring empty)
    //   -1: EAGAIN/EWOULDBLOCK (socket full, partials stay in ring)
    //   -2: hard error (caller should close connection)
    // On partial write, advances begin_ past fully-sent frames and
    // modifies the partial iovec in place.
    ssize_t send(int fd, TransportStats *stats);

    // Release all frames in the ring (on connection close).
    void clear();

    bool has_pending() const
    {
        return end_.load(std::memory_order_relaxed) != begin_.load(std::memory_order_relaxed);
    }

    // Number of frames currently in the ring.
    uint32_t size() const
    {
        return end_.load(std::memory_order_relaxed) - begin_.load(std::memory_order_relaxed);
    }

  private:
    // Per-frame slot: up to 3 iovecs + metadata for cleanup.
    struct Slot
    {
        iovec     iovs[3];
        int       iov_count = 0;
        OutFrame *frame     = nullptr;
        uint8_t   header_buf[HEADER_SIZE];
        // Total bytes across all iovecs (for sent_offset tracking).
        ssize_t total_bytes = 0;
    };

    // Fixed-size ring of slots. begin_/end_ are atomic counters (not
    // indices) — index = counter % IOVEC_RING_FRAMES.
    Slot                  slots_[IOVEC_RING_FRAMES];
    std::atomic<uint32_t> begin_{0};
    std::atomic<uint32_t> end_{0};
};

} // namespace crow::rpc
