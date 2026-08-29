// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Lock-free bounded MPSC (multiple-producer, single-consumer) ring
// buffer. Uses the Vyukov sequence-per-cell scheme for safe publication:
// each cell carries a monotonically increasing sequence number. A
// producer claims a tail index via CAS, fills the cell, then flips the
// sequence so the consumer can observe it. The consumer advances head
// and recycles the cell by setting its sequence to head + capacity.
//
// Capacity is a power of two, fixed at construction. try_push returns
// false when full (backpressure). drain is single-consumer only.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>

namespace crowdb::common
{

template <typename T> class MpscQueue
{
  public:
    explicit MpscQueue(uint32_t capacity = 256)
        : capacity_(round_up_pow2(capacity)),
          mask_(capacity_ - 1),
          cells_(new Cell[capacity_])
    {
        for (uint32_t i = 0; i < capacity_; i++) {
            cells_[i].seq.store(i, std::memory_order_relaxed);
        }
    }

    ~MpscQueue() = default;

    MpscQueue(const MpscQueue &)            = delete;
    MpscQueue &operator=(const MpscQueue &) = delete;
    MpscQueue(MpscQueue &&)                 = delete;
    MpscQueue &operator=(MpscQueue &&)      = delete;

    // Push an element. Returns false if the queue is full (backpressure).
    bool try_push(T value)
    {
        uint64_t tail = tail_.load(std::memory_order_relaxed);
        for (;;) {
            uint64_t head = head_.load(std::memory_order_acquire);
            if (tail - head >= capacity_) {
                return false; // full — backpressure
            }
            if (tail_.compare_exchange_weak(tail, tail + 1, std::memory_order_relaxed)) {
                break;
            }
            // CAS failed — tail moved; retry with updated value.
        }
        auto &cell = cells_[tail & mask_];
        // Wait for the consumer to recycle this slot. In steady state
        // (tail - head < capacity) the slot was already recycled and its
        // seq == tail — this loop is 0 iterations. It only spins if the
        // consumer hasn't yet finished recycling the slot after the
        // capacity check raced with a concurrent drain.
        while (cell.seq.load(std::memory_order_acquire) != tail) {
            std::this_thread::yield();
        }
        cell.value.store(value, std::memory_order_relaxed);
        cell.seq.store(tail + 1, std::memory_order_release);
        return true;
    }

    // Drain up to max elements into out[]. Returns the number drained.
    // Caller owns the returned values. Single-consumer only.
    int drain(T *out, int max)
    {
        uint64_t head = head_.load(std::memory_order_relaxed);
        int      n    = 0;
        while (n < max) {
            auto &cell = cells_[head & mask_];
            if (cell.seq.load(std::memory_order_acquire) != head + 1) {
                break; // empty or producer hasn't filled the slot yet
            }
            out[n++] = cell.value.load(std::memory_order_relaxed);
            cell.value.store(T{}, std::memory_order_relaxed);
            cell.seq.store(head + capacity_, std::memory_order_release);
            head++;
        }
        head_.store(head, std::memory_order_release);
        return n;
    }

    // Conservative pending check: may return true when a producer has
    // claimed a slot but not yet filled it. Safe for wake/disarm decisions.
    bool has_pending() const
    {
        return head_.load(std::memory_order_acquire) != tail_.load(std::memory_order_acquire);
    }

    uint32_t capacity() const
    {
        return capacity_;
    }

  private:
    struct Cell
    {
        std::atomic<T>        value{};
        std::atomic<uint64_t> seq{0};
    };

    static uint32_t round_up_pow2(uint32_t v)
    {
        // Next power of two (v already a power of two → unchanged).
        if (v <= 1) {
            return 2;
        }
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        return v + 1;
    }

    const uint32_t          capacity_;
    const uint32_t          mask_;
    std::unique_ptr<Cell[]> cells_;

    // Producer side (many threads). Padded to avoid false sharing with head.
    alignas(64) std::atomic<uint64_t> tail_{0};
    // Consumer side (one thread).
    alignas(64) std::atomic<uint64_t> head_{0};
};

} // namespace crowdb::common
