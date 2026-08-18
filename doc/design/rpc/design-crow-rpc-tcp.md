<!-- Copyright 2026-present buzzcrow <buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Design: RPC TCP Transport

Depends on: [`design-crow-rpc.md`](design-crow-rpc.md) (root — `Transport` interface, `Connection`, `OutFrame`, `Buffer`, `FrameParser`)
Satisfies: `design-crow-rpc.md` §2 (transport interface + I/O loop decisions)

TCP is the v1 transport. epoll (Linux) and kqueue (macOS) are the two
kernel event interfaces; they differ in API but share the same
event-driven loop structure. A common `SocketTransport` base holds the
shared logic (send queue drain, parser feed, connection management); the
event-dispatch primitives are in `EpollEngine` and `KqueueEngine`
subclasses. The engine subclass tells the base *when* to read/write; the
base does the actual I/O and parsing via `on_readable` and `on_writable`.

## Table of Contents

- [1. Worker Loop](#1-worker-loop)
- [2. Scatter-Gather Send](#2-scatter-gather-send)
- [3. Zero-Copy Receive](#3-zero-copy-receive)
- [4. EpollEngine (Linux)](#4-epollengine-linux)
- [5. KqueueEngine (macOS)](#5-kqueueengine-macos)

---

## 1. Worker Loop

```
Per worker thread:
  init engine (epoll_fd / kqueue_fd)
  register: eventfd/notify-fd, timerfd/kqueue-timer

  loop:
    events = engine.wait(timeout)
    for event in events:
      if event.fd == listen_socket:    // acceptor worker only
        accept → create Connection → assign to worker (round-robin)
                → engine.add_connection(fd, conn) → arm_read(fd)
      elif event.fd == notify_fd:
        drain cross-thread submit queue
        for each pending send: conn->enqueue_send(frame) → arm_write(fd)
      elif event.fd == timer_fd:
        run due scheduled tasks → reset timer to next deadline
      elif event is READABLE:
        on_readable(conn)
      elif event is WRITABLE:
        on_writable(conn)
        if send queue empty: disarm_write(fd)
      elif event is ERROR/HUP:
        conn->close() → remove_connection(fd) → trigger reconnect
```

WRITABLE is armed only when there's data to send — idle connections
don't wake the worker; when the send queue drains, disarm. Cross-thread
submit (from a tokio thread) wakes the worker via notify_fd (eventfd on
Linux, `EVFILT_USER` on macOS) — no locking on the hot path. One timer
per worker (`timerfd` / `EVFILT_TIMER`) serves all scheduled tasks.
Connections are assigned to workers round-robin at accept time; each
connection is owned by one worker, so no cross-worker locking.

## 2. Scatter-Gather Send

`on_writable` drains up to `BATCH_MAX` frames from the send queue, builds
an `iovec` array (3 per frame: header, control, data), and calls
`writev` in one syscall. The kernel reads directly from the pool buffers
— zero-copy. On partial write, `advance_iov` skips the written bytes and
the remaining iovecs stay queued; the next `on_writable` continues.
Fully-sent frames' buffers are released (refcount decrement → pool
recycle). A dropped connection mid-send returns `EPIPE` / `ENOTCONN`;
`on_writable` calls `conn->close()` and reconnect triggers. A worker
thread crash is detected by the engine's main loop, which closes all its
connections, fails pending requests, and triggers reconnect.

## 3. Zero-Copy Receive

`on_readable` loops: gets the next read target from `FrameParser`,
calls `read()` directly into the pool-allocated `Buffer`, feeds the
bytes to `parser.advance()`. When a complete `Frame` is yielded,
dispatches via `conn->on_frame()`. On `EAGAIN`, the loop breaks;
level-triggered epoll wakes again when more data arrives. No scratch
buffer — the `Frame` handed to `on_frame` points at the same bytes the
kernel wrote.

## 4. EpollEngine (Linux)

Level-triggered (not edge-triggered) — simpler correctness model, the
worker re-arms write only when there's data. `EPOLLET` would require
draining the socket to EAGAIN every wake; level-triggered lets us arm
on-demand and disarm when idle. `arm_read` / `arm_write` / `disarm_write`
are `epoll_ctl(EPOLL_CTL_MOD, …)`; `wait` is `epoll_wait`.

## 5. KqueueEngine (macOS)

kqueue uses `EV_CLEAR` (edge-triggered) for write — the API is cleaner
this way on macOS, and `on_writable` already drains the queue fully per
wake. Read uses level-triggered to match epoll semantics. `arm_read` /
`arm_write` / `disarm_write` are `kevent` on `EVFILT_READ` /
`EVFILT_WRITE`; notify is `EVFILT_USER` (pipe fallback on older macOS);
timer is `EVFILT_TIMER`.
