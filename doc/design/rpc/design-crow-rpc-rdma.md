<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Design: RPC RDMA Transport

Depends on: [`design-crow-rpc.md`](design-crow-rpc.md) (root — `Transport` interface, `Connection`, `OutFrame`, `Buffer`, `FrameParser`)
Satisfies: `design-crow-rpc.md` §2 (transport interface + I/O loop decisions)

RDMA is the target transport for the diskio data path — kernel bypass,
no syscall per send, pre-registered memory pools. RDMA is Linux-only
(no RNICs on macOS); the build gates it behind `#ifdef __linux__` + a
CMake `CROW_RPC_HAVE_RDMA` flag.

## Table of Contents

- [1. RdmaTransport](#1-rdmatransport)
- [2. CQ Poll Loop](#2-cq-poll-loop)
- [3. Connection Setup](#3-connection-setup)

---

## 1. RdmaTransport

`RdmaTransport` holds an `ibv_context`, protection domain (`ibv_pd`),
and two `RdmaBufferPool`s — one for send, one for recv — both
pre-registered at construction. `submit` builds a send work request
with the control and data buffers' `ibv_mr` handles and posts it via
`ibv_post_send`; the QP sends directly from registered memory — no
kernel involvement, no copy. `register_buffer` copies a System buffer
into `send_pool_` if not already `Registered`; callers on the RDMA path
allocate from `RdmaBufferPool` directly to avoid this.

## 2. CQ Poll Loop

```
Per worker thread:
  cq = ibv_create_cq(context_, depth, ...)
  post initial recv WRs (refill recv queue)

  loop:
    ibv_poll_cq(cq, wcs)
    for each wc:
      if send completion: recycle send buffer; post next send WR if queued
      if recv completion: feed recv buffer to parser → on_frame; post new recv WR
    check for cross-thread submits → post send WRs
```

Same submit/completion/dispatch logic as the socket transport. The I/O
primitive changes (CQ poll vs epoll), the buffer registration changes
(`ibv_reg_mr` vs noop), everything else is shared via the `Transport` /
`Connection` / `FrameParser` interfaces. An RNIC disconnect surfaces as
a CQ error completion → connection closes, reconnect triggers. A full
QP send queue returns `RpcError::SendQueueFull`; an empty recv queue
drops incoming sends (RDMA behavior), so the recv pool depth is sized
to avoid this under normal load. RDMA CM events (connection rejected,
addr unreachable) trigger reconnect with backoff, same as TCP.

## 3. Connection Setup

Connection establishment uses `librdmacm` (RDMA CM). The server
sequence is `rdma_create_id` → `rdma_bind_addr` → `rdma_listen` →
`rdma_get_request` → `rdma_create_qp` → `rdma_accept`; the client
sequence is `rdma_create_id` → `rdma_resolve_addr` →
`rdma_resolve_route` → `rdma_connect` → `rdma_create_qp`. After QP
creation, the worker posts initial receive WRs to refill the recv
queue so incoming sends have a destination buffer.
