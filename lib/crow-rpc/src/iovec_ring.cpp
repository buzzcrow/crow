// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/iovec_ring.h"

#include "crow-common/log.h"
#include "crow-rpc/transport/socket_transport.h" // TransportStats

#include <unistd.h>

#include <cerrno>
#include <climits>
#include <cstring>

#ifndef IOV_MAX
#    define IOV_MAX 1024
#endif

namespace crow::rpc
{

IovecRing::IovecRing()
{
    for (auto &slot : slots_) {
        slot.iov_count   = 0;
        slot.frame       = nullptr;
        slot.total_bytes = 0;
    }
}

bool IovecRing::offer(OutFrame *frame)
{
    uint32_t end = end_.load(std::memory_order_relaxed);
    if (end - begin_.load(std::memory_order_relaxed) >= IOVEC_RING_FRAMES) {
        return false; // ring full
    }

    uint32_t idx  = end % IOVEC_RING_FRAMES;
    Slot    &slot = slots_[idx];

    // Serialize header into the slot's header buffer.
    serialize_header(slot.header_buf, frame->header);

    ssize_t total = HEADER_SIZE;
    int     count = 0;

    // Header iovec (skip if already partially sent past header).
    if (frame->sent_offset < HEADER_SIZE) {
        slot.iovs[count] = {slot.header_buf + frame->sent_offset,
                            static_cast<size_t>(HEADER_SIZE - frame->sent_offset)};
        count++;
    }

    // Control iovec.
    if (frame->control != nullptr && frame->control->len > 0) {
        ssize_t off = static_cast<ssize_t>(frame->sent_offset) - HEADER_SIZE;
        if (off < 0) {
            off = 0;
        }
        ssize_t clen = static_cast<ssize_t>(frame->control->len);
        if (off < clen) {
            slot.iovs[count] = {frame->control->data + off, static_cast<size_t>(clen - off)};
            count++;
        }
    }

    // Data iovec.
    if (frame->data != nullptr && frame->data->len > 0) {
        ssize_t off = static_cast<ssize_t>(frame->sent_offset) - HEADER_SIZE;
        if (frame->control != nullptr) {
            off -= static_cast<ssize_t>(frame->control->len);
        }
        if (off < 0) {
            off = 0;
        }
        ssize_t dlen = static_cast<ssize_t>(frame->data->len);
        if (off < dlen) {
            slot.iovs[count] = {frame->data->data + off, static_cast<size_t>(dlen - off)};
            count++;
        }
    }

    slot.iov_count   = count;
    slot.frame       = frame;
    slot.total_bytes = total + (frame->control ? frame->control->len : 0) + (frame->data ? frame->data->len : 0);

    end_.store(end + 1, std::memory_order_release);
    return true;
}

ssize_t IovecRing::send(int fd, TransportStats *stats)
{
    while (true) {
        uint32_t begin = begin_.load(std::memory_order_relaxed);
        uint32_t end   = end_.load(std::memory_order_relaxed);
        if (begin == end) {
            return 0; // ring empty
        }

        // Collect iovecs from all pending slots, up to IOV_MAX.
        iovec    iovs[IOV_MAX];
        int      iov_count  = 0;
        uint32_t slot_start = begin;
        uint32_t slot_end   = begin;

        for (uint32_t i = begin; i < end && iov_count < IOV_MAX - 3; i++) {
            Slot &slot = slots_[i % IOVEC_RING_FRAMES];
            for (int j = 0; j < slot.iov_count; j++) {
                if (slot.iovs[j].iov_len > 0) {
                    iovs[iov_count++] = slot.iovs[j];
                }
            }
            slot_end = i + 1;
        }

        if (iov_count == 0) {
            // All slots have zero-length iovecs — skip them.
            begin_.store(slot_end, std::memory_order_release);
            continue;
        }

        ssize_t written = ::writev(fd, iovs, iov_count);
        if (stats != nullptr) {
            if (written > 0) {
                stats->writev_bytes.fetch_add(static_cast<uint64_t>(written), std::memory_order_relaxed);
            }
            stats->writev_calls.fetch_add(1, std::memory_order_relaxed);
        }
        if (written < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return -1; // socket full, partials stay in ring
            }
            return -2; // hard error
        }

        // Advance through slots, consuming `written` bytes.
        ssize_t remaining = written;
        while (remaining > 0 && slot_start < slot_end) {
            Slot   &slot           = slots_[slot_start % IOVEC_RING_FRAMES];
            ssize_t slot_remaining = 0;
            for (int j = 0; j < slot.iov_count; j++) {
                slot_remaining += slot.iovs[j].iov_len;
            }

            if (remaining >= slot_remaining) {
                // Full slot sent — release frame, advance begin_.
                remaining -= slot_remaining;
                if (slot.frame != nullptr) {
                    if (slot.frame->control != nullptr) {
                        slot.frame->control->release();
                    }
                    if (slot.frame->data != nullptr) {
                        slot.frame->data->release();
                    }
                    delete slot.frame;
                    slot.frame = nullptr;
                }
                slot.iov_count = 0;
                begin_.store(slot_start + 1, std::memory_order_release);
                slot_start++;
            }
            else {
                // Partial slot — modify iovecs in place.
                for (int j = 0; j < slot.iov_count && remaining > 0; j++) {
                    if (slot.iovs[j].iov_len <= static_cast<size_t>(remaining)) {
                        remaining -= slot.iovs[j].iov_len;
                        slot.iovs[j].iov_len = 0;
                    }
                    else {
                        slot.iovs[j].iov_base = static_cast<uint8_t *>(slot.iovs[j].iov_base) + remaining;
                        slot.iovs[j].iov_len -= remaining;
                        remaining = 0;
                    }
                }
                // Partial — can't advance further, stop.
                break;
            }
        }

        if (slot_start == slot_end) {
            // All slots in this batch were fully sent — loop to check
            // if more slots were offered while we were writing.
            continue;
        }
        // Partial write — stop, partials stay in ring for next send().
        return written;
    }
}

void IovecRing::clear()
{
    uint32_t begin = begin_.load(std::memory_order_relaxed);
    uint32_t end   = end_.load(std::memory_order_relaxed);
    for (uint32_t i = begin; i < end; i++) {
        Slot &slot = slots_[i % IOVEC_RING_FRAMES];
        if (slot.frame != nullptr) {
            if (slot.frame->control != nullptr) {
                slot.frame->control->release();
            }
            if (slot.frame->data != nullptr) {
                slot.frame->data->release();
            }
            delete slot.frame;
            slot.frame = nullptr;
        }
        slot.iov_count = 0;
    }
    begin_.store(end, std::memory_order_relaxed);
}

} // namespace crow::rpc
