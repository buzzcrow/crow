// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/transport.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace crow::rpc
{

// Configuration for ConnectionPool.
struct PoolConfig
{
    uint32_t reconnect_initial_delay_ms = 100;
    uint32_t reconnect_max_delay_ms     = 10000;
    uint32_t reconnect_max_retries      = 0; // 0 = infinite
};

// ConnectionPool manages a set of connections to peer endpoints. Callers
// (consensus replicas, diskio clients) use get() for round-robin selection
// among healthy connections, or get_for(endpoint) to target a specific node.
//
// The pool does not own the transport — the caller creates connections via
// SocketTransport::create_connection and adds them to the pool. Reconnect
// logic is handled by the caller via on_close callbacks (the pool itself
// is a simple vector + round-robin index for v1).
class ConnectionPool
{
  public:
    ConnectionPool() = default;
    explicit ConnectionPool(PoolConfig config);
    ~ConnectionPool() = default;

    // Round-robin among healthy (is_open) connections. Returns nullptr if
    // all connections are down.
    Connection *get();

    // Find a connection by name (endpoint). Round-robin among connections
    // with matching name prefix. Returns nullptr if none found.
    Connection *get_for(const std::string &endpoint);

    // Add a connection to the pool.
    void add(std::shared_ptr<Connection> conn);

    // Remove a connection from the pool.
    void remove(Connection *conn);

    // Close all connections.
    void close_all();

    // Number of connections (healthy + unhealthy).
    size_t size();

    // Number of healthy connections.
    size_t healthy_count();

    const PoolConfig &config() const
    {
        return config_;
    }

  private:
    PoolConfig                               config_;
    std::mutex                               mu_;
    std::vector<std::shared_ptr<Connection>> connections_;
    std::atomic<size_t>                      next_{0};
};

} // namespace crow::rpc
