// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/pool.h"

namespace crow::rpc
{

ConnectionPool::ConnectionPool(PoolConfig config) : config_(config)
{
}

Connection *ConnectionPool::get()
{
    std::lock_guard<std::mutex> lock(mu_);
    if (connections_.empty()) {
        return nullptr;
    }
    size_t n     = connections_.size();
    size_t start = next_.fetch_add(1, std::memory_order_relaxed) % n;
    for (size_t i = 0; i < n; i++) {
        size_t idx = (start + i) % n;
        if (connections_[idx]->is_open()) {
            return connections_[idx].get();
        }
    }
    return nullptr; // all down
}

Connection *ConnectionPool::get_for(const std::string &endpoint)
{
    std::lock_guard<std::mutex> lock(mu_);
    // Find all connections whose name matches the endpoint.
    std::vector<size_t> matches;
    for (size_t i = 0; i < connections_.size(); i++) {
        if (connections_[i]->name() == endpoint && connections_[i]->is_open()) {
            matches.push_back(i);
        }
    }
    if (matches.empty()) {
        return nullptr;
    }
    size_t idx = next_.fetch_add(1, std::memory_order_relaxed) % matches.size();
    return connections_[matches[idx]].get();
}

void ConnectionPool::add(std::shared_ptr<Connection> conn)
{
    std::lock_guard<std::mutex> lock(mu_);
    connections_.push_back(std::move(conn));
}

void ConnectionPool::remove(Connection *conn)
{
    std::lock_guard<std::mutex> lock(mu_);
    for (auto it = connections_.begin(); it != connections_.end(); ++it) {
        if (it->get() == conn) {
            connections_.erase(it);
            return;
        }
    }
}

void ConnectionPool::close_all()
{
    std::lock_guard<std::mutex> lock(mu_);
    for (auto &conn : connections_) {
        conn->close();
    }
    connections_.clear();
}

size_t ConnectionPool::size()
{
    std::lock_guard<std::mutex> lock(mu_);
    return connections_.size();
}

size_t ConnectionPool::healthy_count()
{
    std::lock_guard<std::mutex> lock(mu_);
    size_t                      count = 0;
    for (auto &conn : connections_) {
        if (conn->is_open()) {
            count++;
        }
    }
    return count;
}

} // namespace crow::rpc
