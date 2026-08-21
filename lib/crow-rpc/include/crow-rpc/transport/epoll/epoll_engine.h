// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/transport/socket_transport.h"

namespace crow::rpc
{

// EpollEngine: Linux event loop using epoll.
// - Level-triggered (not edge-triggered) — simpler correctness model.
// - Read: always armed (EPOLLIN).
// - Write: armed on-demand (EPOLLOUT via MOD), disarmed when idle.
// - No userspace mask tracking — epoll_ctl is kernel-serialized, so
//   arm/disarm always call MOD directly (redundant MODs are ~1µs, cheaper
//   than a mutex). This eliminates mask_mu_ from the hot path.
// - Notify: eventfd.
// - Timer: timerfd.
class EpollEngine : public SocketEngine
{
  public:
    EpollEngine();
    ~EpollEngine() override;

    int  init() override;
    void set_oneshot(bool on) override;

    bool oneshot() const override
    {
        return oneshot_;
    }

    void add_listen_fd(int fd) override;
    void add_connection(int fd, Connection *conn) override;
    void remove_connection(int fd) override;
    void arm_read(int fd, Connection *conn) override;
    void arm_write(int fd, Connection *conn) override;
    void disarm_write(int fd, Connection *conn) override;
    void notify_worker() override;
    void set_timer(int timeout_ms) override;
    int  wait(EngineEvent *out_events, int max_events, int timeout_ms) override;
    void shutdown() override;

  private:
    int  epoll_fd_  = -1;
    int  notify_fd_ = -1;    // eventfd
    int  timer_fd_  = -1;    // timerfd
    bool oneshot_   = false; // multi-worker safety

    // fd → Connection* map (only used for add/remove; wait() uses data.ptr).
    std::mutex                            conn_mu_;
    std::unordered_map<int, Connection *> connections_;

    void mod_fd(int fd, uint32_t events, Connection *conn);
};

} // namespace crow::rpc
