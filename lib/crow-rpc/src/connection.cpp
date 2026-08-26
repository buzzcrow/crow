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

// ── try_send: caller-thread direct writev ────────────
//
// Drains the send queue and does writev directly on the caller's thread.
// The in_send_ flag serializes: only one thread does writev at a time.
// Others just offer to the queue and return — the ongoing writev will
// pick up their frames. If writev returns EAGAIN, frames are re-enqueued
// and the caller must arm write on the I/O worker for retry.
bool Connection::try_send(int fd, TransportStats *stats)
{
    try {
        // Acquire the send lock. If another thread is already sending,
        // our frame is in the queue — it will be sent by the ongoing writev.
        bool expected = false;
        if (!in_send_.compare_exchange_strong(expected, true)) {
            return true; // another thread is sending; our frame is queued
        }

        for (;;) { // retry_send loop — re-enter if race check finds new frames
            // Drain MPSC queue into the iovec ring. The ring already holds
            // partials from a previous EAGAIN (iovecs modified in place).
            // New frames are appended after the partials, preserving order.
            uint32_t frames_offered = 0;
            uint64_t now = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());

            OutFrame *tmp[BATCH_MAX];
            while (true) {
                int n = drain_send_queue(tmp, BATCH_MAX);
                if (n == 0) {
                    break;
                }
                for (int i = 0; i < n; i++) {
                    if (!ring_.offer(tmp[i])) {
                        // Ring full — re-enqueue to MPSC queue for next cycle.
                        enqueue_send(tmp[i]);
                        for (int j = i + 1; j < n; j++) {
                            enqueue_send(tmp[j]);
                        }
                        break;
                    }
                    if (stats != nullptr) {
                        stats->frames_sent.fetch_add(1, std::memory_order_relaxed);
                        if (tmp[i]->create_nano > 0) {
                            stats->submit_to_writev.record(now - tmp[i]->create_nano);
                        }
                    }
                    frames_offered++;
                }
                if (n < BATCH_MAX) {
                    break; // queue drained
                }
            }

            if (frames_offered == 0 && !ring_.has_pending()) {
                in_send_.store(false, std::memory_order_release);
                // Race check: if more frames arrived in the gap.
                if (send_queue_.has_pending()) {
                    expected = false;
                    if (in_send_.compare_exchange_strong(expected, true)) {
                        continue; // retry_send
                    }
                }
                break;
            }

            // writev via the ring — partials stay in the ring on EAGAIN.
            ssize_t result = ring_.send(fd, stats);

            bool all_sent;
            if (result < 0) {
                if (result == -2) {
                    // Hard error — close. The is_open() check below will
                    // clear the ring (close() can't — we hold in_send_).
                    CR_LOG_WARN("try_send: writev hard error fd={} conn_id={} name={} errno={} ({})", fd,
                                static_cast<long long>(id_), name_, errno, std::strerror(errno));
                    close();
                }
                // EAGAIN (-1): partials stay in ring, caller arms EPOLLOUT.
                all_sent = false;
            }
            else {
                all_sent = !ring_.has_pending();
            }

            in_send_.store(false, std::memory_order_release);

            // Race check: if more frames were offered while we were sending,
            // try again (another thread may have missed the in_send_ window).
            if (!all_sent) {
                break; // partial/EAGAIN — caller arms EPOLLOUT
            }
            if (send_queue_.has_pending()) {
                expected = false;
                if (in_send_.compare_exchange_strong(expected, true)) {
                    continue; // retry_send — drain new frames
                }
            }
            break; // all sent, no new frames — done
        }

        return !ring_.has_pending();
    }
    catch (const std::exception &e) {
        in_send_.store(false, std::memory_order_release);
        return false;
    }
    catch (...) {
        in_send_.store(false, std::memory_order_release);
        return false;
    }
}

void Connection::close()
{
    if (!open_.exchange(false, std::memory_order_acq_rel)) {
        return; // already closed
    }
    CR_LOG_INFO("close: conn_id={} name={}", static_cast<long long>(id_), name_);
    ring_.clear();
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
