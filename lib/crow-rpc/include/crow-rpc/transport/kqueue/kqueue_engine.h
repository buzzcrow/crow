// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/transport/socket_transport.h"

namespace crow::rpc
{

// KqueueEngine: macOS event loop using kqueue.
// - Read: level-triggered (EV_ADD without EV_CLEAR) to match epoll semantics.
// - Write: edge-triggered (EV_ADD | EV_CLEAR) — on_writable drains fully.
// - Notify: EVFILT_USER (or pipe fallback on older macOS).
// - Timer: EVFILT_TIMER.
class KqueueEngine : public SocketEngine
{
  public:
    KqueueEngine();
    ~KqueueEngine() override;

    int  init() override;
    void set_oneshot(bool on) override;

    bool oneshot() const override
    {
        return oneshot_;
    }

    void add_listen_fd(int fd) override;
    void add_connection(int fd, Connection *conn) override;
    void remove_connection(int fd) override;
    void arm_read(int fd) override;
    void arm_write(int fd) override;
    void disarm_write(int fd) override;
    void notify_worker() override;
    void set_timer(int timeout_ms) override;
    int  wait(EngineEvent *out_events, int max_events, int timeout_ms) override;
    void shutdown() override;

  private:
    int  kq_           = -1;
    int  notify_ident_ = -1;    // EVFILT_USER ident for cross-thread notify
    int  timer_fd_     = -1;    // timer ident (EVFILT_TIMER)
    bool oneshot_      = false; // multi-worker safety

    // fd → Connection* map (only used for add/remove; wait() uses udata).
    std::mutex                            conn_mu_;
    std::unordered_map<int, Connection *> connections_;
};

} // namespace crow::rpc
