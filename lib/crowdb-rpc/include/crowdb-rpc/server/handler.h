// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/buffer.h"
#include "crowdb-rpc/connection.h"
#include "crowdb-rpc/framing.h"
#include "crowdb-rpc/transport.h"

#include <functional>
#include <mutex>
#include <unordered_map>

namespace crowdb::rpc
{

// ── Handler dispatch ──────────────────────────────────────────────
//
// The server dispatches received frames to handlers registered by
// msg_type. A handler receives the request Frame + Connection and
// returns an OutFrame* response (nullptr for one-way or async).
//
// The handler runs on the worker thread. Fast handlers (ping, echo)
// return inline; slow handlers return nullptr and submit the response
// later via transport->submit.

// Handler function: receives request frame + connection, returns a
// response OutFrame* (nullptr for one-way / async). The handler owns
// the request frame (must delete it or transfer ownership).
using HandlerFn = std::function<OutFrame *(Frame *request, Connection *conn)>;

// Built-in handlers.
OutFrame *handle_ping(Frame *request, Connection *conn);
OutFrame *handle_unknown(Frame *request, Connection *conn);

// HandlerRegistry: maps msg_type → HandlerFn. Thread-safe.
class HandlerRegistry
{
  public:
    HandlerRegistry() = default;

    void register_handler(uint16_t msg_type, HandlerFn handler)
    {
        std::lock_guard<std::mutex> lock(mu_);
        handlers_[msg_type] = std::move(handler);
    }

    HandlerFn get_handler(uint16_t msg_type)
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto                        it = handlers_.find(msg_type);
        if (it != handlers_.end()) {
            return it->second;
        }
        return nullptr;
    }

    void clear()
    {
        std::lock_guard<std::mutex> lock(mu_);
        handlers_.clear();
    }

  private:
    std::mutex                              mu_;
    std::unordered_map<uint16_t, HandlerFn> handlers_;
};

} // namespace crowdb::rpc
