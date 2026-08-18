// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/transport.h"

#include <cassert>

namespace crow::rpc
{

Connection::Connection(int64_t id, std::string name, BufferPool *pool, uint32_t max_data_size)
    : id_(id),
      name_(std::move(name)),
      pool_(pool),
      parser_(max_data_size)
{
}

bool Connection::enqueue_send(OutFrame *frame)
{
    if (!is_open()) {
        return false;
    }
    std::lock_guard<std::mutex> lock(send_mu_);
    if (send_queue_.size() >= send_queue_capacity_) {
        return false; // backpressure
    }
    send_queue_.push_back(frame);
    return true;
}

int Connection::drain_send_queue(OutFrame **out, int max)
{
    std::lock_guard<std::mutex> lock(send_mu_);
    int                         n = 0;
    while (n < max && !send_queue_.empty()) {
        out[n++] = send_queue_.front();
        send_queue_.pop_front();
    }
    return n;
}

void Connection::close()
{
    if (!open_.exchange(false, std::memory_order_acq_rel)) {
        return; // already closed
    }
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
