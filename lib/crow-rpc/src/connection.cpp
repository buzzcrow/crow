// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/connection.h"

#include "crow-common/log.h"
#include "crow-rpc/transport/socket_transport.h" // TransportStats

#include <sys/uio.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstring>

namespace crow::rpc
{

namespace
{

// Serialize the frame header into hdr_buf and build up to 3 iovecs from
// the frame at its current sent_offset. Returns the number of iovecs.
// hdr_buf must point to HEADER_SIZE bytes of stable storage.
static inline int build_frame_iovecs(OutFrame *frame, uint8_t *hdr_buf, iovec *iovs)
{
    serialize_header(hdr_buf, frame->header);
    ssize_t off   = static_cast<ssize_t>(frame->sent_offset);
    int     count = 0;

    // Header region.
    if (off < HEADER_SIZE) {
        iovs[count++] = {hdr_buf + off, static_cast<size_t>(HEADER_SIZE - off)};
    }
    else {
        off -= HEADER_SIZE;
    }

    // Control region.
    if (frame->control != nullptr && frame->control->len > 0) {
        ssize_t clen = static_cast<ssize_t>(frame->control->len);
        if (off < clen) {
            iovs[count++] = {frame->control->data + off, static_cast<size_t>(clen - off)};
            off           = 0;
        }
        else {
            off -= clen;
        }
    }

    // Data region.
    if (frame->data != nullptr && frame->data->len > 0) {
        ssize_t dlen = static_cast<ssize_t>(frame->data->len);
        if (off < dlen) {
            iovs[count++] = {frame->data->data + off, static_cast<size_t>(dlen - off)};
        }
    }

    return count;
}

// Total bytes in a frame (header + control + data).
static inline ssize_t frame_total(OutFrame *frame)
{
    ssize_t total = HEADER_SIZE;
    if (frame->control != nullptr) {
        total += static_cast<ssize_t>(frame->control->len);
    }
    if (frame->data != nullptr) {
        total += static_cast<ssize_t>(frame->data->len);
    }
    return total;
}

// Release an OutFrame's buffers and delete it.
static inline void release_frame(OutFrame *frame)
{
    if (frame->control != nullptr) {
        frame->control->release();
    }
    if (frame->data != nullptr) {
        frame->data->release();
    }
    delete frame;
}

} // namespace

Connection::Connection(int64_t id, std::string name, BufferPool *pool, uint32_t max_data_size,
                       uint32_t send_queue_capacity)
    : id_(id),
      name_(std::move(name)),
      pool_(pool),
      parser_(max_data_size),
      send_queue_(send_queue_capacity)
{
    parser_.set_pool(pool);
}

bool Connection::enqueue_send(OutFrame *frame)
{
    if (!is_open()) {
        return false;
    }
    return send_queue_.try_push(frame);
}

int Connection::drain_send_queue(OutFrame **out, int max)
{
    return send_queue_.drain(out, max);
}

// ── try_send: unified caller-thread writev ────────────────────────
//
// Drains the MPSC send queue and does writev directly on the caller's
// thread. The in_send_ CAS flag serializes: only one thread does writev
// at a time. Others just offer to the queue and return — the ongoing
// writev will pick up their frames.
//
// On EAGAIN/partial, unsent frames are kept in the persistent iovec
// buffer (pending_[] + pending_iovs_[] + pending_hdrs_[]) — NOT
// re-enqueued to the MPSC queue, which would break order with concurrent
// enqueues. The next try_send sends partials first, then drains the queue.
// The caller arms EPOLLOUT for retry.
//
// Common case (no existing partials): uses stack-local arrays for hot
// L1 cache. Only spills to the persistent buffer on EAGAIN.
bool Connection::try_send(int fd, TransportStats *stats)
{
    if (!is_open()) {
        return false;
    }

    bool expected = false;
    if (!in_send_.compare_exchange_strong(expected, true)) {
        return true; // another thread is sending; our frame is queued
    }

    bool all_sent = true;
retry:
    while (true) {
        int       frame_count = 0;
        OutFrame *frames[BATCH_MAX];
        ssize_t   frame_totals[BATCH_MAX];
        iovec     iovs[3 * BATCH_MAX];
        uint8_t   hdr_bufs[BATCH_MAX][HEADER_SIZE];
        int       iov_count = 0;

        // If we have partials from a previous EAGAIN, send them first.
        if (pending_count_ > 0) {
            for (int i = 0; i < pending_count_ && frame_count < BATCH_MAX; i++) {
                frames[frame_count] = pending_frames_[i];
                frame_totals[frame_count] = frame_total(frames[frame_count]);
                iov_count += build_frame_iovecs(frames[frame_count], hdr_bufs[frame_count],
                                                &iovs[iov_count]);
                frame_count++;
            }
            pending_count_ = 0;
        }

        // Drain the MPSC queue for new frames.
        while (frame_count < BATCH_MAX) {
            OutFrame *tmp[BATCH_MAX];
            int       n = drain_send_queue(tmp, BATCH_MAX - frame_count);
            if (n == 0) {
                break;
            }
            for (int i = 0; i < n; i++) {
                frames[frame_count]     = tmp[i];
                frame_totals[frame_count] = frame_total(tmp[i]);
                iov_count += build_frame_iovecs(tmp[i], hdr_bufs[frame_count], &iovs[iov_count]);
                if (stats != nullptr && tmp[i]->create_nano > 0) {
                    uint64_t now =
                        static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
                    stats->submit_to_writev.record(now - tmp[i]->create_nano);
                }
                frame_count++;
            }
            if (n < BATCH_MAX - frame_count) {
                break;
            }
        }

        if (frame_count == 0) {
            break;
        }

        if (iov_count == 0) {
            for (int i = 0; i < frame_count; i++) {
                release_frame(frames[i]);
                if (stats != nullptr) {
                    stats->frames_sent.fetch_add(1, std::memory_order_relaxed);
                }
            }
            continue;
        }

        if (stats != nullptr) {
            stats->writev_calls.fetch_add(1, std::memory_order_relaxed);
        }

        ssize_t written = ::writev(fd, iovs, iov_count);
        if (stats != nullptr && written > 0) {
            stats->writev_bytes.fetch_add(static_cast<uint64_t>(written), std::memory_order_relaxed);
        }

        if (written < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Socket buffer full. Store unsent frames in pending buffer.
                pending_count_ = 0;
                for (int i = 0; i < frame_count; i++) {
                    pending_frames_[pending_count_++] = frames[i];
                }
                all_sent = false;
                break;
            }
            CR_LOG_WARN("try_send: writev hard error fd={} conn_id={} name={} errno={} ({})", fd,
                        static_cast<long long>(id_), name_, errno, std::strerror(errno));
            for (int i = 0; i < frame_count; i++) {
                release_frame(frames[i]);
            }
            pending_count_ = 0;
            close();
            all_sent = false;
            break;
        }

        // Consume written bytes across frames.
        ssize_t remaining     = written;
        int     first_partial = -1;
        for (int i = 0; i < frame_count && remaining > 0; i++) {
            ssize_t left = frame_totals[i] - frames[i]->sent_offset;
            if (remaining >= left) {
                frames[i]->sent_offset = static_cast<uint32_t>(frame_totals[i]);
                remaining -= left;
            }
            else {
                frames[i]->sent_offset += static_cast<uint32_t>(remaining);
                remaining     = 0;
                first_partial = i;
            }
        }

        if (first_partial == -1) {
            // All fully sent.
            for (int i = 0; i < frame_count; i++) {
                release_frame(frames[i]);
                if (stats != nullptr) {
                    stats->frames_sent.fetch_add(1, std::memory_order_relaxed);
                }
            }
            pending_count_ = 0;
        }
        else {
            // Store partial + unsent frames in pending buffer.
            pending_count_ = 0;
            for (int i = first_partial; i < frame_count; i++) {
                pending_frames_[pending_count_++] = frames[i];
            }
            // Release fully-sent frames before the partial.
            for (int i = 0; i < first_partial; i++) {
                release_frame(frames[i]);
                if (stats != nullptr) {
                    stats->frames_sent.fetch_add(1, std::memory_order_relaxed);
                }
            }
            all_sent = false;
            break;
        }
        // If we drained a full batch, loop to get more.
        if (frame_count < BATCH_MAX) {
            break;
        }
    }

    in_send_.store(false, std::memory_order_release);

    // Race check: if more frames were offered while we were sending,
    // try again (another thread may have missed the in_send_ window).
    if (all_sent && (send_queue_.has_pending() || pending_count_ > 0)) {
        expected = false;
        if (in_send_.compare_exchange_strong(expected, true)) {
            all_sent = true;
            goto retry;
        }
    }
    return all_sent;
}

void Connection::close()
{
    if (!open_.exchange(false, std::memory_order_acq_rel)) {
        return; // already closed
    }
    CR_LOG_INFO("close: conn_id={} name={}", static_cast<long long>(id_), name_);
    // Release pending frames from a previous EAGAIN.
    for (int i = 0; i < pending_count_; i++) {
        release_frame(pending_frames_[i]);
    }
    pending_count_ = 0;
    if (on_close_callback_) {
        on_close_callback_(this);
    }
}

void Connection::on_frame(Frame *frame)
{
    if (on_frame_callback_) {
        on_frame_callback_(frame, this);
    }
}

} // namespace crow::rpc
