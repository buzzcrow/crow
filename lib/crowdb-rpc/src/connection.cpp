// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/connection.h"

#include "crowdb-common/log.h"
#include "crowdb-rpc/rpc_metrics.h"
#include "crowdb-rpc/transport/socket_transport.h" // TransportStats

#include <sys/uio.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstring>

namespace crowdb::rpc
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

    if (off < HEADER_SIZE) {
        iovs[count++] = {hdr_buf + off, static_cast<size_t>(HEADER_SIZE - off)};
    }
    else {
        off -= HEADER_SIZE;
    }

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

// Restore pending frames to the frames array (cold path — partials
// from a previous EAGAIN). Returns the number restored.
int __attribute__((noinline)) restore_pending(OutFrame **pending, int pending_count, OutFrame **frames,
                                              ssize_t *frame_totals, iovec *iovs, uint8_t (*hdr_bufs)[HEADER_SIZE],
                                              int *iov_count)
{
    int n = pending_count;
    if (n > BATCH_MAX) {
        n = BATCH_MAX;
    }
    int fc = 0;
    int ic = 0;
    for (int i = 0; i < n; i++) {
        frames[fc]       = pending[i];
        frame_totals[fc] = frame_total(frames[fc]);
        ic += build_frame_iovecs(frames[fc], hdr_bufs[fc], &iovs[ic]);
        fc++;
    }
    *iov_count = ic;
    return fc;
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
// On EAGAIN/partial, unsent frames are kept in pending_frames_[] (NOT
// re-enqueued to the MPSC queue, which would break order with concurrent
// enqueues). The next try_send sends partials first, then drains the queue.
// The caller arms EPOLLOUT for retry.
bool Connection::try_send(int fd, TransportStats *stats)
{
    bool expected = false;
    if (!in_send_.compare_exchange_strong(expected, true)) {
        return true; // another thread is sending; our frame is queued
    }

    if (!is_open()) {
        OutFrame *discarded[BATCH_MAX];
        int       discarded_count = drain_send_queue(discarded, BATCH_MAX);
        while (discarded_count > 0) {
            for (int i = 0; i < discarded_count; ++i) {
                release_frame(discarded[i]);
            }
            discarded_count = drain_send_queue(discarded, BATCH_MAX);
        }
        in_send_.store(false, std::memory_order_release);
        return false;
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

        // Partials from a previous EAGAIN go first (preserve order).
        if (pending_count_ > 0) {
            frame_count =
                restore_pending(pending_frames_, pending_count_, frames, frame_totals, iovs, hdr_bufs, &iov_count);
            pending_count_ = 0;
        }

        // Drain the MPSC queue for new frames (single call).
        OutFrame *batch[BATCH_MAX];
        int       n = drain_send_queue(batch, BATCH_MAX - frame_count);
        if (stats != nullptr) {
            uint64_t now = now_nanos();
            for (int i = 0; i < n; i++) {
                if (batch[i]->create_nano > 0) {
                    uint64_t delta = now - batch[i]->create_nano;
                    stats->submit_to_writev.record(delta);
                    hist_submit_to_writev().observe(delta);
                }
                // Request payload bandwidth: data bytes per frame (no header).
                uint64_t payload = 0;
                if (batch[i]->data != nullptr) {
                    payload = batch[i]->data->len;
                }
                bw_request_payload().observe(payload);
            }
        }
        for (int i = 0; i < n; i++) {
            frames[frame_count]       = batch[i];
            frame_totals[frame_count] = frame_total(batch[i]);
            iov_count += build_frame_iovecs(batch[i], hdr_bufs[frame_count], &iovs[iov_count]);
            frame_count++;
        }

        if (frame_count == 0) {
            break;
        }

        uint64_t writev_start = now_nanos();
        ssize_t  written      = ::writev(fd, iovs, iov_count);
        uint64_t writev_end   = now_nanos();
        hist_writev().observe(writev_end - writev_start);
        if (written > 0) {
            bw_writev().observe(static_cast<uint64_t>(written));
        }

        if (written < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                pending_count_ = 0;
                for (int i = 0; i < frame_count; i++) {
                    pending_frames_[pending_count_++] = frames[i];
                }
                all_sent = false;
                break;
            }
            cnt_write_error().inc();
            CRB_LOG_WARN("try_send: writev hard error fd={} conn_id={} name={} errno={} ({})", fd,
                        static_cast<long long>(id_), name_, errno, std::strerror(errno));
            for (int i = 0; i < frame_count; i++) {
                release_frame(frames[i]);
            }
            pending_count_ = 0;
            close();
            all_sent = false;
            break;
        }

        // Advance sent_offset across the batch.
        ssize_t remaining = written;
        for (int i = 0; i < frame_count && remaining > 0; i++) {
            ssize_t left = frame_totals[i] - frames[i]->sent_offset;
            if (remaining >= left) {
                frames[i]->sent_offset = static_cast<uint32_t>(frame_totals[i]);
                remaining -= left;
            }
            else {
                frames[i]->sent_offset += static_cast<uint32_t>(remaining);
                remaining = 0;
            }
        }

        // Release fully-sent frames; store partials in pending chain.
        bool has_partial = false;
        pending_count_   = 0;
        for (int i = 0; i < frame_count; i++) {
            if (frames[i]->sent_offset >= frame_totals[i]) {
                release_frame(frames[i]);
            }
            else {
                pending_frames_[pending_count_++] = frames[i];
                has_partial                       = true;
            }
        }
        if (has_partial) {
            all_sent = false;
            break;
        }
        // If we drained a full batch, loop for more.
        if (n < BATCH_MAX) {
            break;
        }
    }

    if (!is_open()) {
        for (int i = 0; i < pending_count_; ++i) {
            release_frame(pending_frames_[i]);
        }
        pending_count_ = 0;
        OutFrame *discarded[BATCH_MAX];
        int       discarded_count = drain_send_queue(discarded, BATCH_MAX);
        while (discarded_count > 0) {
            for (int i = 0; i < discarded_count; ++i) {
                release_frame(discarded[i]);
            }
            discarded_count = drain_send_queue(discarded, BATCH_MAX);
        }
    }
    in_send_.store(false, std::memory_order_release);

    // Race check: if more frames arrived while we were sending, retry.
    if (all_sent && send_queue_.has_pending()) {
        expected = false;
        if (in_send_.compare_exchange_strong(expected, true)) {
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
    CRB_LOG_INFO("close: conn_id={} name={}", static_cast<long long>(id_), name_);
    bool expected = false;
    if (in_send_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        for (int i = 0; i < pending_count_; i++) {
            release_frame(pending_frames_[i]);
        }
        pending_count_ = 0;
        OutFrame *discarded[BATCH_MAX];
        int       discarded_count = drain_send_queue(discarded, BATCH_MAX);
        while (discarded_count > 0) {
            for (int i = 0; i < discarded_count; ++i) {
                release_frame(discarded[i]);
            }
            discarded_count = drain_send_queue(discarded, BATCH_MAX);
        }
        in_send_.store(false, std::memory_order_release);
    }
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

} // namespace crowdb::rpc
