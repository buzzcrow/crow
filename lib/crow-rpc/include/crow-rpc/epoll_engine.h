// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/socket_transport.h"

namespace crow::rpc
{

// EpollEngine: Linux event loop using epoll.
// - Level-triggered (not edge-triggered) — simpler correctness model.
// - Read: always armed (EPOLLIN).
// - Write: armed on-demand (EPOLLOUT add via MOD), disarmed when idle.
// - Notify: eventfd.
// - Timer: timerfd.
class EpollEngine : public SocketEngine
{
  public:
    EpollEngine();
    ~EpollEngine() override;

    int  init() override;
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
    int epoll_fd_  = -1;
    int notify_fd_ = -1; // eventfd
    int timer_fd_  = -1; // timerfd

    // fd → Connection* map (for dispatching events).
    std::mutex                            conn_mu_;
    std::unordered_map<int, Connection *> connections_;

    // Track per-fd event mask for MOD operations.
    std::mutex                        mask_mu_;
    std::unordered_map<int, uint32_t> fd_masks_;

    void mod_fd(int fd, uint32_t events);
};

} // namespace crow::rpc
