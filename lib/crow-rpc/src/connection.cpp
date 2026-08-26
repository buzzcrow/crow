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

namespace {

// Serialize the frame header into hdr_buf and build up to 3 iovecs from
// the frame at its current sent_offset. Returns the number of iovecs.
// hdr_buf must point to HEADER_SIZE bytes of stable storage (not stack).
int build_frame_iovecs(OutFrame *frame, uint8_t *hdr_buf, iovec *iovs)
{
    serialize_header(hdr_buf, frame->header);
    ssize_t off = static_cast<ssize_t>(frame->sent_offset);
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
            off = 0;
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

// ── Path A: send_direct — direct writev with mutex ────────────────
//
// Sends a single frame immediately via writev. If a partial frame from a
// previous EAGAIN exists (direct_partial_), its remaining iovecs are
// prepended before the new frame's iovecs so order is preserved. The
// mutex serializes concurrent callers — only one thread does writev at
// a time. On partial write, the partially-sent frame is kept in
// direct_partial_ for the next call.
bool Connection::send_direct(int fd, OutFrame *frame, TransportStats *stats)
{
    std::lock_guard<std::mutex> lock(send_mu_);
    if (!is_open()) {
        release_frame(frame);
        return false;
    }

    uint64_t now = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    if (stats != nullptr && frame->create_nano > 0) {
        stats->submit_to_writev.record(now - frame->create_nano);
    }

    // Build iovecs: partial first (if any), then the new frame.
    iovec    iovs[6]; // up to 3 for partial + 3 for new frame
    int      iov_count = 0;
    uint8_t  hdr_bufs[2][HEADER_SIZE];
    OutFrame *frames[2];
    int      frame_count = 0;

    if (direct_partial_ != nullptr) {
        int n = build_frame_iovecs(direct_partial_, hdr_bufs[0], &iovs[iov_count]);
        iov_count += n;
        frames[frame_count++] = direct_partial_;
    }

    int n = build_frame_iovecs(frame, hdr_bufs[frame_count], &iovs[iov_count]);
    iov_count += n;
    frames[frame_count++] = frame;

    if (iov_count == 0) {
        return true; // nothing to send (both fully sent already)
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
            // Socket buffer full. If no existing partial, the new frame
            // becomes the partial. If there is a partial, we can't hold
            // two — enqueue the new frame back (it will be sent after the
            // partial on the next call via EPOLLOUT retry).
            if (direct_partial_ == nullptr) {
                direct_partial_ = frame;
            }
            else {
                // Enqueue the new frame; it goes after the partial next time.
                // try_push is lock-free so this is safe under the mutex.
                enqueue_send(frame);
            }
            return false;
        }
        // Hard error — close, release both frames.
        CR_LOG_WARN("send_direct: writev hard error fd={} conn_id={} name={} errno={} ({})", fd,
                    static_cast<long long>(id_), name_, errno, std::strerror(errno));
        if (direct_partial_ != nullptr) {
            release_frame(direct_partial_);
            direct_partial_ = nullptr;
        }
        release_frame(frame);
        close();
        return false;
    }

    // Consume written bytes across frames.
    ssize_t remaining = written;
    for (int i = 0; i < frame_count && remaining > 0; i++) {
        ssize_t frame_rem = frame_remaining(frames[i]);
        if (remaining >= frame_rem) {
            // Fully sent — release frame.
            remaining -= frame_rem;
            frames[i]->sent_offset = static_cast<uint32_t>(frame_total(frames[i]));
            release_frame(frames[i]);
            if (i == 0 && direct_partial_ != nullptr) {
                direct_partial_ = nullptr;
            }
            if (stats != nullptr) {
                stats->frames_sent.fetch_add(1, std::memory_order_relaxed);
            }
        }
        else {
            // Partial — advance sent_offset, keep as partial.
            frames[i]->sent_offset += static_cast<uint32_t>(remaining);
            remaining = 0;
            if (i == 0) {
                // Old partial still partial; new frame (i==1) unsent.
                // Keep old partial, enqueue new frame for next time.
                direct_partial_ = frames[0];
                if (frame_count == 2) {
                    enqueue_send(frames[1]);
                }
            }
            else {
                // New frame is partial (no old partial existed).
                direct_partial_ = frames[1];
            }
            return false;
        }
    }

    // All frames fully sent.
    direct_partial_ = nullptr;
    return true;
}

// ── Path B: flush_send — worker-thread flush via flat iovec array ──
//
// Drains the MPSC queue into the flat iovec array. Partials from a previous
// EAGAIN are already at the front (flush_iov_count_ > 0 with adjusted
// iov_base/iov_len). New frames are appended after. writev's once, then
// consumes written bytes: fully-sent frames are released and removed from
// the front; a partial frame's sent_offset is advanced and its iovecs are
// rebuilt at the front. No lock — only the I/O worker calls this.
bool Connection::flush_send(int fd, TransportStats *stats)
{
    if (!is_open()) {
        return false;
    }

    uint64_t now = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());

    // Drain the MPSC queue, appending new frames after existing partials.
    // flush_frame_count_ is the number of frames currently in the array
    // (partials from last time + new ones we're about to add).
    while (flush_frame_count_ < BATCH_MAX) {
        OutFrame *tmp[BATCH_MAX];
        int       n = drain_send_queue(tmp, BATCH_MAX - flush_frame_count_);
        if (n == 0) {
            break;
        }
        for (int i = 0; i < n; i++) {
            flush_frames_[flush_frame_count_] = tmp[i];
            // Build iovecs for this frame into the flush array.
            int fc = build_frame_iovecs(tmp[i], flush_hdrs_[flush_frame_count_],
                                        &flush_iovs_[flush_iov_count_]);
            flush_iov_count_ += fc;
            if (stats != nullptr) {
                if (tmp[i]->create_nano > 0) {
                    stats->submit_to_writev.record(now - tmp[i]->create_nano);
                }
            }
            flush_frame_count_++;
        }
        if (n < BATCH_MAX - flush_frame_count_) {
            break; // queue drained
        }
    }

    if (flush_iov_count_ == 0) {
        return true; // nothing to send
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
            return false; // partials stay in array, caller arms EPOLLOUT
        }
        // Hard error — close, release all pending frames.
        CR_LOG_WARN("flush_send: writev hard error fd={} conn_id={} name={} errno={} ({})", fd,
                    static_cast<long long>(id_), name_, errno, std::strerror(errno));
        close();
        return false;
    }

    // Consume written bytes: walk through frames, release fully-sent ones,
    // advance the partial frame's sent_offset and rebuild its iovecs at front.
    ssize_t remaining = written;
    int     consumed = 0; // fully-sent frame count

    for (int fi = 0; fi < flush_frame_count_; fi++) {
        OutFrame *frame = flush_frames_[fi];
        ssize_t   frame_rem = frame_remaining(frame);

        if (remaining >= frame_rem) {
            // Fully sent — release frame.
            remaining -= frame_rem;
            frame->sent_offset = static_cast<uint32_t>(frame_total(frame));
            release_frame(frame);
            if (stats != nullptr) {
                stats->frames_sent.fetch_add(1, std::memory_order_relaxed);
            }
            consumed++;
        }
        else if (remaining > 0) {
            // Partial — advance sent_offset, keep this frame and remaining.
            frame->sent_offset += static_cast<uint32_t>(remaining);
            remaining = 0;
            // Compact: move this frame and all remaining to front.
            int new_iov_count = 0;
            for (int j = consumed; j < flush_frame_count_; j++) {
                int fc = build_frame_iovecs(flush_frames_[j], flush_hdrs_[j - consumed],
                                            &flush_iovs_[new_iov_count]);
                new_iov_count += fc;
                flush_frames_[j - consumed] = flush_frames_[j];
            }
            flush_frame_count_ -= consumed;
            flush_iov_count_ = new_iov_count;
            return false;
        }
        else {
            // remaining == 0 — this frame and all after it are unsent.
            // Compact: move them to front (no offset changes needed).
            int new_iov_count = 0;
            for (int j = consumed; j < flush_frame_count_; j++) {
                int fc = build_frame_iovecs(flush_frames_[j], flush_hdrs_[j - consumed],
                                            &flush_iovs_[new_iov_count]);
                new_iov_count += fc;
                flush_frames_[j - consumed] = flush_frames_[j];
            }
            flush_frame_count_ -= consumed;
            flush_iov_count_ = new_iov_count;
            return flush_frame_count_ == 0;
        }
    }

    // All frames fully sent.
    flush_frame_count_ = 0;
    flush_iov_count_ = 0;
    return true;
}

void Connection::close()
{
    if (!open_.exchange(false, std::memory_order_acq_rel)) {
        return; // already closed
    }
    CR_LOG_INFO("close: conn_id={} name={}", static_cast<long long>(id_), name_);
    // Release Path A partial.
    if (direct_partial_ != nullptr) {
        release_frame(direct_partial_);
        direct_partial_ = nullptr;
    }
    // Release Path B pending frames.
    for (int i = 0; i < flush_frame_count_; i++) {
        release_frame(flush_frames_[i]);
    }
    flush_iov_count_ = 0;
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
