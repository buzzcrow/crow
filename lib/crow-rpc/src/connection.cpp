// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/connection.h"

#include "crow-rpc/transport/socket_transport.h" // TransportStats

#include <sys/uio.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstring>

namespace crow::rpc
{

Connection::Connection(int64_t id, std::string name, BufferPool *pool, uint32_t max_data_size)
    : id_(id),
      name_(std::move(name)),
      pool_(pool),
      parser_(max_data_size)
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

// ── try_send: caller-thread direct writev ────────────
//
// Drains the send queue and does writev directly on the caller's thread.
// The in_send_ flag serializes: only one thread does writev at a time.
// Others just offer to the queue and return — the ongoing writev will
// pick up their frames. If writev returns EAGAIN, frames are re-enqueued
// and the caller must arm write on the I/O worker for retry.
bool Connection::try_send(int fd, TransportStats *stats)
{
    // Acquire the send lock. If another thread is already sending,
    // our frame is in the queue — it will be sent by the ongoing writev.
    bool expected = false;
    if (!in_send_.compare_exchange_strong(expected, true)) {
        return true; // another thread is sending; our frame is queued
    }

    bool all_sent = true;
retry_send:
    while (true) {
        OutFrame *batch[BATCH_MAX];
        int       n = drain_send_queue(batch, BATCH_MAX);
        if (n == 0) {
            break;
        }

        if (stats != nullptr) {
            stats->writev_calls.fetch_add(1, std::memory_order_relaxed);
            uint64_t now = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
            for (int i = 0; i < n; i++) {
                if (batch[i]->create_nano > 0) {
                    stats->submit_to_writev.record(now - batch[i]->create_nano);
                }
            }
        }

        // Compute frame totals and build iovecs.
        ssize_t frame_total[BATCH_MAX];
        for (int i = 0; i < n; i++) {
            ssize_t sz = HEADER_SIZE;
            if (batch[i]->control != nullptr) {
                sz += batch[i]->control->len;
            }
            if (batch[i]->data != nullptr) {
                sz += batch[i]->data->len;
            }
            frame_total[i] = sz;
        }

        iovec   iov[3 * BATCH_MAX];
        int     iov_count = 0;
        uint8_t header_bufs[BATCH_MAX][HEADER_SIZE];

        for (int i = 0; i < n; i++) {
            ssize_t off = batch[i]->sent_offset;
            ssize_t rem = frame_total[i] - off;
            if (rem <= 0) {
                continue;
            }
            // Header region.
            if (off < HEADER_SIZE) {
                serialize_header(header_bufs[i], batch[i]->header);
                iov[iov_count++] = {header_bufs[i] + off, static_cast<size_t>(HEADER_SIZE - off)};
                off              = 0;
            }
            else {
                off -= HEADER_SIZE;
            }
            // Control region.
            if (batch[i]->control != nullptr && batch[i]->control->len > 0) {
                ssize_t clen = static_cast<ssize_t>(batch[i]->control->len);
                if (off < clen) {
                    iov[iov_count++] = {batch[i]->control->data + off, static_cast<size_t>(clen - off)};
                    off              = 0;
                }
                else {
                    off -= clen;
                }
            }
            // Data region.
            if (batch[i]->data != nullptr && batch[i]->data->len > 0) {
                ssize_t dlen = static_cast<ssize_t>(batch[i]->data->len);
                if (off < dlen) {
                    iov[iov_count++] = {batch[i]->data->data + off, static_cast<size_t>(dlen - off)};
                }
            }
        }

        ssize_t written = ::writev(fd, iov, iov_count);
        if (written < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Socket buffer full — re-enqueue and let the I/O worker retry.
                for (int i = 0; i < n; i++) {
                    enqueue_send(batch[i]);
                }
                all_sent = false;
                break;
            }
            // Hard error — close and free frames.
            close();
            for (int i = 0; i < n; i++) {
                if (batch[i]->control != nullptr) {
                    batch[i]->control->release();
                }
                if (batch[i]->data != nullptr) {
                    batch[i]->data->release();
                }
                delete batch[i];
            }
            all_sent = false;
            break;
        }

        // Advance sent_offset across the batch.
        ssize_t remaining = written;
        for (int i = 0; i < n && remaining > 0; i++) {
            ssize_t left = frame_total[i] - batch[i]->sent_offset;
            if (remaining >= left) {
                batch[i]->sent_offset = static_cast<uint32_t>(frame_total[i]);
                remaining -= left;
            }
            else {
                batch[i]->sent_offset += static_cast<uint32_t>(remaining);
                remaining = 0;
            }
        }

        // Release fully-sent frames; re-enqueue partials.
        bool has_partial = false;
        for (int i = 0; i < n; i++) {
            if (batch[i]->sent_offset >= frame_total[i]) {
                if (batch[i]->control != nullptr) {
                    batch[i]->control->release();
                }
                if (batch[i]->data != nullptr) {
                    batch[i]->data->release();
                }
                delete batch[i];
            }
            else {
                enqueue_send(batch[i]);
                has_partial = true;
            }
        }
        if (has_partial) {
            all_sent = false;
            break;
        }
        // If we drained a full batch, loop to get more.
        if (n < BATCH_MAX) {
            break;
        }
    }

    in_send_.store(false, std::memory_order_release);

    // Race check: if more frames were offered while we were sending,
    // try again (another thread may have missed the in_send_ window).
    // Lock-free — the check + CAS is not atomic, but the race is benign:
    // if another thread acquires in_send_ between the check and the CAS,
    // it will drain the frames. If no thread does, the worker's
    // on_writable will pick them up via arm_write.
    if (!all_sent) {
        return false;
    }
    if (send_queue_.has_pending()) {
        expected = false;
        if (in_send_.compare_exchange_strong(expected, true)) {
            // Re-acquired in_send_ — retry to drain frames that arrived
            // in the gap between in_send_.store(false) and this check.
            all_sent = true;
            goto retry_send;
        }
    }
    return true;
}

void Connection::close()
{
    if (!open_.exchange(false, std::memory_order_acq_rel)) {
        return; // already closed
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

} // namespace crow::rpc
