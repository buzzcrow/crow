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
// hdr_buf must point to HEADER_SIZE bytes of stable storage (not stack).
int build_frame_iovecs(OutFrame *frame, uint8_t *hdr_buf, iovec *iovs)
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

// Total remaining bytes for a frame at its current sent_offset.
ssize_t frame_remaining(OutFrame *frame)
{
    ssize_t total = HEADER_SIZE;
    if (frame->control != nullptr) {
        total += static_cast<ssize_t>(frame->control->len);
    }
    if (frame->data != nullptr) {
        total += static_cast<ssize_t>(frame->data->len);
    }
    return total - static_cast<ssize_t>(frame->sent_offset);
}

// Total bytes in a frame (header + control + data).
ssize_t frame_total(OutFrame *frame)
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
void release_frame(OutFrame *frame)
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

// ── Path A: send_direct — caller-thread batched writev ────────────
//
// Enqueues the frame, then drains the ENTIRE send queue and batches up to
// BATCH_MAX frames into a single writev. The in_send_ CAS flag serializes
// concurrent callers — only one does writev; others just enqueue and return
// (the winner picks up their frames).
//
// On EAGAIN/partial, unsent frames are kept in a pending chain (direct_partial_
// + direct_pending_[]), NOT re-enqueued to the MPSC queue. This preserves
// order: concurrent enqueues would interleave with re-enqueued frames,
// corrupting the byte stream. Next call sends partial + pending first, then
// drains the MPSC queue for new frames.
bool Connection::send_direct(int fd, OutFrame *frame, TransportStats *stats)
{
    // Enqueue first so the winner drains it along with everything else.
    if (!enqueue_send(frame)) {
        CR_LOG_WARN("send_direct: enqueue failed (backpressure) conn_id={} name={}", static_cast<long long>(id_),
                    name_);
        return false;
    }

    // Acquire the send lock. If another thread is already sending, our
    // frame is in the queue — it will be sent by the ongoing writev.
    bool expected = false;
    if (!in_send_.compare_exchange_strong(expected, true)) {
        return true; // another thread is sending; our frame is queued
    }

    bool all_sent = true;
retry_send:
    while (true) {
        // Build frame array: pending chain first (partial + pending[]),
        // then drain the MPSC queue for new frames.
        int       frame_count = 0;
        OutFrame *frames[BATCH_MAX];

        if (direct_partial_ != nullptr) {
            if (frame_remaining(direct_partial_) > 0) {
                frames[frame_count++] = direct_partial_;
            }
            else {
                release_frame(direct_partial_);
                direct_partial_ = nullptr;
            }
        }
        for (int i = 0; i < direct_pending_count_ && frame_count < BATCH_MAX; i++) {
            frames[frame_count++] = direct_pending_[i];
        }
        direct_pending_count_ = 0;

        OutFrame *batch[BATCH_MAX];
        int       n = drain_send_queue(batch, BATCH_MAX - frame_count);
        for (int i = 0; i < n; i++) {
            frames[frame_count++] = batch[i];
        }

        if (frame_count == 0) {
            break;
        }

        if (stats != nullptr) {
            stats->writev_calls.fetch_add(1, std::memory_order_relaxed);
            uint64_t now = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
            for (int i = 0; i < frame_count; i++) {
                if (frames[i]->create_nano > 0) {
                    stats->submit_to_writev.record(now - frames[i]->create_nano);
                }
            }
        }

        // Compute frame totals.
        ssize_t frame_totals[BATCH_MAX];
        for (int i = 0; i < frame_count; i++) {
            frame_totals[i] = frame_total(frames[i]);
        }

        // Build iovecs.
        iovec   iovs[3 * BATCH_MAX];
        int     iov_count = 0;
        uint8_t hdr_bufs[BATCH_MAX][HEADER_SIZE];
        for (int i = 0; i < frame_count; i++) {
            iov_count += build_frame_iovecs(frames[i], hdr_bufs[i], &iovs[iov_count]);
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

        ssize_t written = ::writev(fd, iovs, iov_count);
        if (stats != nullptr && written > 0) {
            stats->writev_bytes.fetch_add(static_cast<uint64_t>(written), std::memory_order_relaxed);
        }

        if (written < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Socket buffer full. Store unsent frames in the pending
                // chain (NOT re-enqueue — preserves order vs concurrent
                // enqueues). frames[0] is the partial (or the first unsent
                // frame if nothing was written). Frames after the partial
                // go to direct_pending_[].
                direct_partial_       = frames[0];
                direct_pending_count_ = 0;
                for (int i = 1; i < frame_count; i++) {
                    direct_pending_[direct_pending_count_++] = frames[i];
                }
                all_sent = false;
                break;
            }
            // Hard error — close, release all frames + pending chain.
            CR_LOG_WARN("send_direct: writev hard error fd={} conn_id={} name={} errno={} ({})", fd,
                        static_cast<long long>(id_), name_, errno, std::strerror(errno));
            for (int i = 0; i < frame_count; i++) {
                release_frame(frames[i]);
            }
            direct_partial_ = nullptr;
            for (int i = 0; i < direct_pending_count_; i++) {
                release_frame(direct_pending_[i]);
            }
            direct_pending_count_ = 0;
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

        // Release fully-sent frames; store partial + unsent in pending chain.
        if (first_partial == -1) {
            // All fully sent.
            for (int i = 0; i < frame_count; i++) {
                release_frame(frames[i]);
                if (stats != nullptr) {
                    stats->frames_sent.fetch_add(1, std::memory_order_relaxed);
                }
            }
            direct_partial_       = nullptr;
            direct_pending_count_ = 0;
        }
        else {
            // frames[first_partial] is the partial; frames after it are unsent.
            direct_partial_       = frames[first_partial];
            direct_pending_count_ = 0;
            for (int i = first_partial + 1; i < frame_count; i++) {
                direct_pending_[direct_pending_count_++] = frames[i];
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
        if (n < BATCH_MAX - (frame_count - n)) {
            break;
        }
    }

    in_send_.store(false, std::memory_order_release);

    // Race check: if more frames were offered while we were sending,
    // try again (another thread may have missed the in_send_ window).
    if (all_sent && (send_queue_.has_pending() || direct_partial_ != nullptr)) {
        expected = false;
        if (in_send_.compare_exchange_strong(expected, true)) {
            all_sent = true;
            goto retry_send;
        }
    }
    return all_sent;
}

// Path A retry: drain the pending chain + queue without enqueuing a new
// frame. Called from on_writable (EPOLLOUT) after a previous EAGAIN.
bool Connection::retry_direct(int fd, TransportStats *stats)
{
    bool expected = false;
    if (!in_send_.compare_exchange_strong(expected, true)) {
        return true; // another thread is sending
    }

    bool all_sent = true;
retry:
    while (true) {
        int       frame_count = 0;
        OutFrame *frames[BATCH_MAX];

        if (direct_partial_ != nullptr) {
            if (frame_remaining(direct_partial_) > 0) {
                frames[frame_count++] = direct_partial_;
            }
            else {
                release_frame(direct_partial_);
                direct_partial_ = nullptr;
            }
        }
        for (int i = 0; i < direct_pending_count_ && frame_count < BATCH_MAX; i++) {
            frames[frame_count++] = direct_pending_[i];
        }
        direct_pending_count_ = 0;

        OutFrame *batch[BATCH_MAX];
        int       n = drain_send_queue(batch, BATCH_MAX - frame_count);
        for (int i = 0; i < n; i++) {
            frames[frame_count++] = batch[i];
        }

        if (frame_count == 0) {
            break;
        }

        if (stats != nullptr) {
            stats->writev_calls.fetch_add(1, std::memory_order_relaxed);
        }

        ssize_t frame_totals[BATCH_MAX];
        for (int i = 0; i < frame_count; i++) {
            frame_totals[i] = frame_total(frames[i]);
        }

        iovec   iovs[3 * BATCH_MAX];
        int     iov_count = 0;
        uint8_t hdr_bufs[BATCH_MAX][HEADER_SIZE];
        for (int i = 0; i < frame_count; i++) {
            iov_count += build_frame_iovecs(frames[i], hdr_bufs[i], &iovs[iov_count]);
        }

        if (iov_count == 0) {
            for (int i = 0; i < frame_count; i++) {
                release_frame(frames[i]);
            }
            continue;
        }

        ssize_t written = ::writev(fd, iovs, iov_count);
        if (stats != nullptr && written > 0) {
            stats->writev_bytes.fetch_add(static_cast<uint64_t>(written), std::memory_order_relaxed);
        }

        if (written < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                direct_partial_       = frames[0];
                direct_pending_count_ = 0;
                for (int i = 1; i < frame_count; i++) {
                    direct_pending_[direct_pending_count_++] = frames[i];
                }
                all_sent = false;
                break;
            }
            CR_LOG_WARN("retry_direct: writev hard error fd={} conn_id={} name={} errno={} ({})", fd,
                        static_cast<long long>(id_), name_, errno, std::strerror(errno));
            for (int i = 0; i < frame_count; i++) {
                release_frame(frames[i]);
            }
            direct_partial_ = nullptr;
            for (int i = 0; i < direct_pending_count_; i++) {
                release_frame(direct_pending_[i]);
            }
            direct_pending_count_ = 0;
            close();
            all_sent = false;
            break;
        }

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
            for (int i = 0; i < frame_count; i++) {
                release_frame(frames[i]);
                if (stats != nullptr) {
                    stats->frames_sent.fetch_add(1, std::memory_order_relaxed);
                }
            }
            direct_partial_       = nullptr;
            direct_pending_count_ = 0;
        }
        else {
            direct_partial_       = frames[first_partial];
            direct_pending_count_ = 0;
            for (int i = first_partial + 1; i < frame_count; i++) {
                direct_pending_[direct_pending_count_++] = frames[i];
            }
            for (int i = 0; i < first_partial; i++) {
                release_frame(frames[i]);
                if (stats != nullptr) {
                    stats->frames_sent.fetch_add(1, std::memory_order_relaxed);
                }
            }
            all_sent = false;
            break;
        }
        if (n < BATCH_MAX - (frame_count - n)) {
            break;
        }
    }

    in_send_.store(false, std::memory_order_release);

    if (all_sent && (send_queue_.has_pending() || direct_partial_ != nullptr)) {
        expected = false;
        if (in_send_.compare_exchange_strong(expected, true)) {
            all_sent = true;
            goto retry;
        }
    }
    return all_sent;
}

// ── Path B: flush_send — worker-thread flush via flat iovec array ──
//
// Drains the MPSC queue into the flat iovec array. Partials from a previous
// EAGAIN are already at the front (flush_iov_count_ > 0 with adjusted
// iov_base/iov_len). New frames are appended after. writev's once, then
// consumes written bytes: fully-sent frames are released and removed from
// the front; a partial frame's sent_offset is advanced and its iovecs are
// rebuilt at the front. in_flush_ CAS ensures only one worker flushes at a
// time — losers skip (their frames are in the MPSC queue; the winner drains
// them). Race check at the end catches frames enqueued during the flush.
bool Connection::flush_send(int fd, TransportStats *stats)
{
    if (!is_open()) {
        return false;
    }

    bool expected = false;
    if (!in_flush_.compare_exchange_strong(expected, true)) {
        return true; // another worker is flushing; our frames are queued
    }

    bool all_sent = true;
retry_flush: {
    uint64_t now = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());

    // Drain the MPSC queue, appending new frames after existing partials.
    while (flush_frame_count_ < BATCH_MAX) {
        OutFrame *tmp[BATCH_MAX];
        int       n = drain_send_queue(tmp, BATCH_MAX - flush_frame_count_);
        if (n == 0) {
            break;
        }
        for (int i = 0; i < n; i++) {
            flush_frames_[flush_frame_count_] = tmp[i];
            int fc = build_frame_iovecs(tmp[i], flush_hdrs_[flush_frame_count_], &flush_iovs_[flush_iov_count_]);
            flush_iov_count_ += fc;
            if (stats != nullptr && tmp[i]->create_nano > 0) {
                stats->submit_to_writev.record(now - tmp[i]->create_nano);
            }
            flush_frame_count_++;
        }
        if (n < BATCH_MAX - flush_frame_count_) {
            break;
        }
    }

    if (flush_iov_count_ == 0) {
        goto done;
    }

    if (stats != nullptr) {
        stats->writev_calls.fetch_add(1, std::memory_order_relaxed);
    }

    ssize_t written = ::writev(fd, flush_iovs_, flush_iov_count_);
    if (stats != nullptr && written > 0) {
        stats->writev_bytes.fetch_add(static_cast<uint64_t>(written), std::memory_order_relaxed);
    }

    if (written < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            all_sent = false;
            goto done; // partials stay, caller arms EPOLLOUT
        }
        CR_LOG_WARN("flush_send: writev hard error fd={} conn_id={} name={} errno={} ({})", fd,
                    static_cast<long long>(id_), name_, errno, std::strerror(errno));
        close();
        all_sent = false;
        goto done;
    }

    // Consume written bytes: release fully-sent frames, compact partials.
    ssize_t remaining = written;
    int     consumed  = 0;

    for (int fi = 0; fi < flush_frame_count_; fi++) {
        OutFrame *frame     = flush_frames_[fi];
        ssize_t   frame_rem = frame_remaining(frame);

        if (remaining >= frame_rem) {
            remaining -= frame_rem;
            frame->sent_offset = static_cast<uint32_t>(frame_total(frame));
            release_frame(frame);
            if (stats != nullptr) {
                stats->frames_sent.fetch_add(1, std::memory_order_relaxed);
            }
            consumed++;
        }
        else if (remaining > 0) {
            frame->sent_offset += static_cast<uint32_t>(remaining);
            remaining         = 0;
            int new_iov_count = 0;
            for (int j = consumed; j < flush_frame_count_; j++) {
                int fc = build_frame_iovecs(flush_frames_[j], flush_hdrs_[j - consumed], &flush_iovs_[new_iov_count]);
                new_iov_count += fc;
                flush_frames_[j - consumed] = flush_frames_[j];
            }
            flush_frame_count_ -= consumed;
            flush_iov_count_ = new_iov_count;
            all_sent         = false;
            goto done;
        }
        else {
            int new_iov_count = 0;
            for (int j = consumed; j < flush_frame_count_; j++) {
                int fc = build_frame_iovecs(flush_frames_[j], flush_hdrs_[j - consumed], &flush_iovs_[new_iov_count]);
                new_iov_count += fc;
                flush_frames_[j - consumed] = flush_frames_[j];
            }
            flush_frame_count_ -= consumed;
            flush_iov_count_ = new_iov_count;
            if (flush_frame_count_ > 0) {
                all_sent = false;
            }
            goto done;
        }
    }

    // All frames fully sent.
    flush_frame_count_ = 0;
    flush_iov_count_   = 0;
}

done:
    in_flush_.store(false, std::memory_order_release);

    // Race check: if more frames were enqueued during the flush, retry.
    if (all_sent && (send_queue_.has_pending() || flush_iov_count_ > 0)) {
        expected = false;
        if (in_flush_.compare_exchange_strong(expected, true)) {
            all_sent = true;
            goto retry_flush;
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
    // Release Path A partial + pending chain.
    if (direct_partial_ != nullptr) {
        release_frame(direct_partial_);
        direct_partial_ = nullptr;
    }
    for (int i = 0; i < direct_pending_count_; i++) {
        release_frame(direct_pending_[i]);
    }
    direct_pending_count_ = 0;
    // Release Path B pending frames.
    for (int i = 0; i < flush_frame_count_; i++) {
        release_frame(flush_frames_[i]);
    }
    flush_iov_count_   = 0;
    flush_frame_count_ = 0;
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
