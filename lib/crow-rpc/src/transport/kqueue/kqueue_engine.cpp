// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/transport/kqueue/kqueue_engine.h"

#include <sys/event.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>

namespace crow::rpc
{

// EVFILT_USER ident — arbitrary, must be unique per kqueue.
static constexpr int NOTIFY_IDENT = 1;

KqueueEngine::KqueueEngine() = default;

KqueueEngine::~KqueueEngine()
{
    shutdown();
}

int KqueueEngine::init()
{
    kq_ = ::kqueue();
    if (kq_ < 0) {
        return -1;
    }

    // Register EVFILT_USER for cross-thread notify.
    struct kevent change;
    EV_SET(&change, NOTIFY_IDENT, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
    if (::kevent(kq_, &change, 1, nullptr, 0, nullptr) < 0) {
        ::close(kq_);
        kq_ = -1;
        return -1;
    }
    notify_ident_ = NOTIFY_IDENT;
    return 0;
}

void KqueueEngine::set_oneshot(bool on)
{
    oneshot_ = on;
}

void KqueueEngine::add_listen_fd(int fd)
{
    struct kevent change;
    EV_SET(&change, fd, EVFILT_READ, EV_ADD, 0, 0, nullptr);
    ::kevent(kq_, &change, 1, nullptr, 0, nullptr);
}

void KqueueEngine::add_connection(int fd, Connection *conn)
{
    {
        std::lock_guard<std::mutex> lock(conn_mu_);
        connections_[fd] = conn;
    }
    // Register read with udata = Connection* so wait() can dispatch
    // without a map lookup. EV_ONESHOT in multi-worker mode prevents
    // races (only one worker wakes per event; re-arm after processing).
    int           flags = EV_ADD | (oneshot_ ? EV_ONESHOT : 0);
    struct kevent change;
    EV_SET(&change, fd, EVFILT_READ, flags, 0, 0, conn);
    ::kevent(kq_, &change, 1, nullptr, 0, nullptr);
}

void KqueueEngine::remove_connection(int fd)
{
    {
        std::lock_guard<std::mutex> lock(conn_mu_);
        connections_.erase(fd);
    }
    // Delete both read and write filters.
    struct kevent changes[2];
    EV_SET(&changes[0], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    EV_SET(&changes[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    ::kevent(kq_, changes, 2, nullptr, 0, nullptr);
}

void KqueueEngine::arm_read(int fd)
{
    // Look up the Connection* for udata (needed for zero-lock dispatch).
    Connection *conn = nullptr;
    {
        std::lock_guard<std::mutex> lock(conn_mu_);
        auto                        it = connections_.find(fd);
        if (it != connections_.end()) {
            conn = it->second;
        }
    }
    // In one-shot mode, EV_ADD re-arms the filter after a one-shot event.
    int           flags = EV_ADD | (oneshot_ ? EV_ONESHOT : 0);
    struct kevent change;
    EV_SET(&change, fd, EVFILT_READ, flags, 0, 0, conn);
    ::kevent(kq_, &change, 1, nullptr, 0, nullptr);
}

void KqueueEngine::arm_write(int fd)
{
    // Level-triggered (no EV_CLEAR): fires whenever the socket is
    // writable. We disarm via disarm_write when the send queue is empty
    // to avoid a busy-loop. udata = Connection* for zero-lock dispatch.
    // In one-shot mode, EV_ONESHOT ensures only one worker processes write.
    Connection *conn = nullptr;
    {
        std::lock_guard<std::mutex> lock(conn_mu_);
        auto                        it = connections_.find(fd);
        if (it != connections_.end()) {
            conn = it->second;
        }
    }
    int           flags = EV_ADD | (oneshot_ ? EV_ONESHOT : 0);
    struct kevent change;
    EV_SET(&change, fd, EVFILT_WRITE, flags, 0, 0, conn);
    ::kevent(kq_, &change, 1, nullptr, 0, nullptr);
}

void KqueueEngine::disarm_write(int fd)
{
    struct kevent change;
    EV_SET(&change, fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    ::kevent(kq_, &change, 1, nullptr, 0, nullptr);
}

void KqueueEngine::notify_worker()
{
    if (kq_ < 0 || notify_ident_ < 0) {
        return;
    }
    // Trigger EVFILT_USER — note identifies the filter.
    struct kevent change;
    EV_SET(&change, notify_ident_, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
    ::kevent(kq_, &change, 1, nullptr, 0, nullptr);
}

void KqueueEngine::set_timer(int timeout_ms)
{
    if (timeout_ms <= 0) {
        // Disable timer.
        if (timer_fd_ >= 0) {
            struct kevent change;
            EV_SET(&change, timer_fd_, EVFILT_TIMER, EV_DELETE, 0, 0, nullptr);
            ::kevent(kq_, &change, 1, nullptr, 0, nullptr);
            timer_fd_ = -1;
        }
        return;
    }
    // Use a fixed ident for the timer (2, after NOTIFY_IDENT=1).
    // kqueue EVFILT_TIMER takes the timeout in the data field; use
    // NOTE_USECONDS for microsecond resolution.
    int           timer_ident = 2;
    struct kevent change;
    EV_SET(&change, timer_ident, EVFILT_TIMER, EV_ADD | EV_ONESHOT, NOTE_USECONDS, timeout_ms * 1000, nullptr);
    ::kevent(kq_, &change, 1, nullptr, 0, nullptr);
    timer_fd_ = timer_ident;
}

int KqueueEngine::wait(EngineEvent *out_events, int max_events, int timeout_ms)
{
    struct kevent events[64];
    int           max = std::min(max_events, 64);

    struct timespec  ts;
    struct timespec *pts = nullptr;
    if (timeout_ms >= 0) {
        ts.tv_sec  = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000L;
        pts        = &ts;
    }

    int n = ::kevent(kq_, nullptr, 0, events, max, pts);
    if (n < 0) {
        if (errno == EINTR) {
            return 0;
        }
        return -1;
    }

    int out = 0;
    for (int i = 0; i < n && out < max_events; i++) {
        const struct kevent &ev = events[i];
        if (ev.filter == EVFILT_USER) {
            out_events[out++] = {SocketEvent::Notify, -1, nullptr};
        }
        else if (ev.filter == EVFILT_TIMER) {
            out_events[out++] = {SocketEvent::Timer, -1, nullptr};
        }
        else if (ev.filter == EVFILT_READ) {
            // udata holds the Connection* (set in add_connection/arm_read).
            // No mutex needed — the kernel passes it back directly.
            auto *conn = static_cast<Connection *>(ev.udata);
            if (conn != nullptr) {
                out_events[out++] = {SocketEvent::Readable, static_cast<int>(ev.ident), conn};
            }
            else {
                // No udata — must be the listen socket.
                out_events[out++] = {SocketEvent::Accept, static_cast<int>(ev.ident), nullptr};
            }
        }
        else if (ev.filter == EVFILT_WRITE) {
            auto *conn        = static_cast<Connection *>(ev.udata);
            out_events[out++] = {SocketEvent::Writable, static_cast<int>(ev.ident), conn};
        }
        // Check for error flags.
        if (ev.flags & EV_ERROR) {
            out_events[out - 1].type = SocketEvent::Error;
        }
    }
    return out;
}

void KqueueEngine::shutdown()
{
    if (kq_ >= 0) {
        ::close(kq_);
        kq_ = -1;
    }
}

} // namespace crow::rpc
