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

// epoll_ctl is kernel-serialized (thread-safe), so no userspace lock
// is needed. Redundant MODs are ~1µs each — cheaper than a mutex.
void EpollEngine::mod_fd(int fd, uint32_t events, Connection *conn)
{
    struct epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    ev.events   = events | (oneshot_ ? static_cast<uint32_t>(EPOLLONESHOT) : 0u);
    ev.data.ptr = conn; // udata = Connection* for zero-lock dispatch
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

void EpollEngine::add_connection(int fd, Connection *conn)
{
    {
        std::lock_guard<std::mutex> lock(conn_mu_);
        connections_[fd] = conn;
    }
    struct epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    ev.events   = EPOLLIN | (oneshot_ ? static_cast<uint32_t>(EPOLLONESHOT) : 0u);
    ev.data.ptr = conn; // udata = Connection* for zero-lock dispatch
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
}

void EpollEngine::remove_connection(int fd)
{
    {
        std::lock_guard<std::mutex> lock(conn_mu_);
        connections_.erase(fd);
    }
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
}

void EpollEngine::arm_read(int fd, Connection *conn)
{
    // Always MOD — epoll_ctl is kernel-serialized (thread-safe), so no
    // userspace mask tracking is needed. Redundant MODs when EPOLLIN is
    // already armed are ~1µs each, cheaper than a mutex lock/unlock.
    mod_fd(fd, EPOLLIN, conn);
}

void EpollEngine::arm_write(int fd, Connection *conn)
{
    // MOD with EPOLLIN|EPOLLOUT to arm both. In ONESHOT mode the kernel
    // disarmed everything; in level-triggered mode this may be a redundant
    // MOD if EPOLLOUT is already armed — acceptable (epoll_ctl is cheap).
    mod_fd(fd, EPOLLIN | EPOLLOUT, conn);
}

void EpollEngine::disarm_write(int fd, Connection *conn)
{
    // MOD with EPOLLIN only — removes EPOLLOUT from the registration.
    // In ONESHOT mode this also re-arms read (kernel disarmed both).
    mod_fd(fd, EPOLLIN, conn);
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

        // Connection fd: data.ptr holds Connection* (set in add_connection).
        // The listen socket has data.ptr = nullptr (set via data.fd).
        auto *conn = reinterpret_cast<Connection *>(ev.data.ptr);

        // Recover fd for connection events: data.ptr was set, not data.fd.
        // We need the fd for read/write syscalls. Store it in the event.
        // For listen socket: fd is valid (data.fd was set).
        // For connection: fd is garbage (data.ptr was set). Recover from
        // the connection's transport_handle.
        if (conn != nullptr) {
            fd = static_cast<int>(conn->transport_handle);
        }

        if (ev.events & (EPOLLERR | EPOLLHUP)) {
            out_events[out++] = {SocketEvent::Error, fd, conn};
        }
        else {
            if (ev.events & EPOLLIN) {
                if (conn != nullptr) {
                    out_events[out++] = {SocketEvent::Readable, fd, conn};
                }
                else {
                    out_events[out++] = {SocketEvent::Accept, fd, nullptr};
                }
            }
            if (ev.events & EPOLLOUT && conn != nullptr) {
                out_events[out++] = {SocketEvent::Writable, fd, conn};
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
