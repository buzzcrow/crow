// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/buffer.h"
#include "crowdb-rpc/connection.h"
#include "crowdb-rpc/framing.h"
#include "crowdb-rpc/transport.h"

#include <atomic>
#include <functional>
#include <memory>
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
    using HandlerTable = std::unordered_map<uint16_t, HandlerFn>;

    HandlerRegistry() : handlers_(std::make_shared<const HandlerTable>())
    {
    }

    void register_handler(uint16_t msg_type, HandlerFn handler)
    {
        auto current = handlers_.load(std::memory_order_acquire);
        for (;;) {
            auto next                                     = std::make_shared<HandlerTable>(*current);
            (*next)[msg_type]                             = handler;
            std::shared_ptr<const HandlerTable> published = std::move(next);
            if (handlers_.compare_exchange_weak(current, std::move(published), std::memory_order_release,
                                                std::memory_order_acquire)) {
                return;
            }
        }
    }

    HandlerFn get_handler(uint16_t msg_type) const
    {
        auto table = handlers_.load(std::memory_order_acquire);
        auto it    = table->find(msg_type);
        if (it != table->end()) {
            return it->second;
        }
        return nullptr;
    }

    void clear()
    {
        handlers_.store(std::make_shared<const HandlerTable>(), std::memory_order_release);
    }

  private:
    std::atomic<std::shared_ptr<const HandlerTable>> handlers_;
};

} // namespace crowdb::rpc
