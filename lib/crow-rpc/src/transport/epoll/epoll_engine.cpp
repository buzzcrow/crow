// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/transport/epoll/epoll_engine.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstring>

namespace crow::rpc
{

EpollEngine::EpollEngine() = default;

EpollEngine::~EpollEngine()
{
    shutdown();
}

int EpollEngine::init()
{
    epoll_fd_ = ::epoll_create1(0);
    if (epoll_fd_ < 0) {
        return -1;
    }

    // eventfd for cross-thread notify.
    notify_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (notify_fd_ < 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
        return -1;
    }

    struct epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    ev.events  = EPOLLIN;
    ev.data.fd = notify_fd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, notify_fd_, &ev) < 0) {
        ::close(notify_fd_);
        ::close(epoll_fd_);
        notify_fd_ = -1;
        epoll_fd_  = -1;
        return -1;
    }

    // timerfd for scheduled tasks.
    timer_fd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timer_fd_ >= 0) {
        std::memset(&ev, 0, sizeof(ev));
        ev.events  = EPOLLIN;
        ev.data.fd = timer_fd_;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, timer_fd_, &ev);
    }

    return 0;
}

void EpollEngine::set_oneshot(bool on)
{
    oneshot_ = on;
}

// Bit 0 of data.ptr marks write fds (buzz-cpp pattern). Connection*
// is at least 2-byte aligned, so bit 0 is always 0 for read fds.
static constexpr uintptr_t WRITE_FD_MASK = 1;

// epoll_ctl is kernel-serialized (thread-safe), so no userspace lock
// is needed. Redundant MODs are ~1µs each — cheaper than a mutex.
// For read fds: data.ptr = conn (no mask bit).
// For write fds: data.ptr = conn | WRITE_FD_MASK (bit 0 set).
void EpollEngine::mod_fd(int fd, uint32_t events, Connection *conn)
{
    struct epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    ev.events   = events | (oneshot_ ? static_cast<uint32_t>(EPOLLONESHOT) : 0u);
    ev.data.ptr = conn; // udata = Connection* for zero-lock dispatch
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
}

// MOD for write fds — preserves the WRITE_FD_MASK bit in data.ptr so
// wait() can distinguish write events from read events.
void EpollEngine::mod_fd_write(int fd, uint32_t events, Connection *conn)
{
    struct epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    ev.events   = events | (oneshot_ ? static_cast<uint32_t>(EPOLLONESHOT) : 0u);
    ev.data.ptr = reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(conn) | WRITE_FD_MASK);
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
}

void EpollEngine::add_listen_fd(int fd)
{
    struct epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    ev.events  = EPOLLIN;
    ev.data.fd = fd; // listen socket: use fd (no Connection*)
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
}

void EpollEngine::add_connection(int read_fd, int write_fd, Connection *conn)
{
    {
        std::lock_guard<std::mutex> lock(conn_mu_);
        connections_[read_fd] = conn;
    }
    // Register read fd with EPOLLIN (armed immediately for reading).
    struct epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    ev.events   = EPOLLIN | (oneshot_ ? static_cast<uint32_t>(EPOLLONESHOT) : 0u);
    ev.data.ptr = conn; // udata = Connection* for zero-lock dispatch
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, read_fd, &ev);

    // Register write fd with no events (armed on-demand via arm_write).
    // Bit 0 of data.ptr marks this as a write fd so wait() can distinguish
    // read events from write events on the same underlying socket.
    if (write_fd >= 0 && write_fd != read_fd) {
        std::memset(&ev, 0, sizeof(ev));
        ev.events   = 0; // no events until arm_write
        ev.data.ptr = reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(conn) | WRITE_FD_MASK);
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, write_fd, &ev);
    }
}

void EpollEngine::remove_connection(int read_fd, int write_fd)
{
    {
        std::lock_guard<std::mutex> lock(conn_mu_);
        connections_.erase(read_fd);
    }
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, read_fd, nullptr);
    if (write_fd >= 0 && write_fd != read_fd) {
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, write_fd, nullptr);
    }
}

void EpollEngine::arm_read(int read_fd, Connection *conn)
{
    // MOD read fd with EPOLLIN only — does not touch the write fd.
    mod_fd(read_fd, EPOLLIN, conn);
}

void EpollEngine::arm_write(int write_fd, Connection *conn)
{
    // MOD write fd with EPOLLOUT only — does not re-arm read.
    // Uses mod_fd_write to preserve the WRITE_FD_MASK bit in data.ptr
    // so wait() can distinguish write events from read events.
    mod_fd_write(write_fd, EPOLLOUT, conn);
}

void EpollEngine::disarm_write(int write_fd, Connection *conn)
{
    // MOD write fd with 0 events — disarms write without touching read.
    mod_fd_write(write_fd, 0, conn);
}

void EpollEngine::notify_worker()
{
    if (notify_fd_ >= 0) {
        uint64_t val = 1;
        ::write(notify_fd_, &val, sizeof(val));
    }
}

void EpollEngine::set_timer(int timeout_ms)
{
    if (timer_fd_ < 0) {
        return;
    }
    struct itimerspec its;
    std::memset(&its, 0, sizeof(its));
    if (timeout_ms > 0) {
        its.it_value.tv_sec  = timeout_ms / 1000;
        its.it_value.tv_nsec = (timeout_ms % 1000) * 1000000L;
    }
    ::timerfd_settime(timer_fd_, 0, &its, nullptr);
}

int EpollEngine::wait(EngineEvent *out_events, int max_events, int timeout_ms)
{
    struct epoll_event events[64];
    int                max = std::min(max_events, 64);

    int n = ::epoll_wait(epoll_fd_, events, max, timeout_ms);
    if (n < 0) {
        if (errno == EINTR) {
            return 0;
        }
        return -1;
    }

    int out = 0;
    for (int i = 0; i < n && out < max_events; i++) {
        const struct epoll_event &ev = events[i];

        // Special fds (notify/timer) use data.fd; connection fds use
        // data.ptr = Connection*. The listen socket uses data.fd.
        // We check data.fd first for special fds, then fall through to
        // data.ptr for connections.
        int fd = ev.data.fd;

        if (fd == notify_fd_) {
            uint64_t val;
            ::read(notify_fd_, &val, sizeof(val));
            out_events[out++] = {SocketEvent::Notify, -1, nullptr};
            continue;
        }
        if (fd == timer_fd_) {
            uint64_t val;
            ::read(timer_fd_, &val, sizeof(val));
            out_events[out++] = {SocketEvent::Timer, -1, nullptr};
            continue;
        }

        // Connection fd: data.ptr holds Connection* (read fd) or
        // Connection* | WRITE_FD_MASK (write fd). The listen socket
        // has data.ptr = nullptr (set via data.fd).
        auto  raw_ptr     = reinterpret_cast<uintptr_t>(ev.data.ptr);
        bool  is_write_fd = (raw_ptr & WRITE_FD_MASK) != 0;
        auto *conn        = reinterpret_cast<Connection *>(raw_ptr & ~WRITE_FD_MASK);

        // Recover fd for connection events: data.ptr was set, not data.fd.
        // For read events: use the read fd (transport_handle).
        // For write events: use the write fd (conn->write_fd).
        if (conn != nullptr) {
            fd = is_write_fd ? conn->write_fd : static_cast<int>(conn->transport_handle);
        }

        if (ev.events & (EPOLLERR | EPOLLHUP)) {
            out_events[out++] = {SocketEvent::Error, fd, conn};
        }
        else if (is_write_fd) {
            // Write fd event — only EPOLLOUT is armed on this fd.
            if (ev.events & EPOLLOUT && conn != nullptr) {
                out_events[out++] = {SocketEvent::Writable, fd, conn};
            }
        }
        else {
            // Read fd event — EPOLLIN for connection or listen socket.
            if (ev.events & EPOLLIN) {
                if (conn != nullptr) {
                    out_events[out++] = {SocketEvent::Readable, fd, conn};
                }
                else {
                    out_events[out++] = {SocketEvent::Accept, fd, nullptr};
                }
            }
        }
    }
    return out;
}

void EpollEngine::shutdown()
{
    if (notify_fd_ >= 0) {
        ::close(notify_fd_);
        notify_fd_ = -1;
    }
    if (timer_fd_ >= 0) {
        ::close(timer_fd_);
        timer_fd_ = -1;
    }
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

} // namespace crow::rpc
