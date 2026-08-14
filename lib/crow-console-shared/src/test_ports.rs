// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::net::TcpListener;

/// Allocate a free port by binding to port 0 and reading the
/// OS-assigned port number. The listener is dropped immediately, so
/// there is a small TOCTOU window — acceptable for tests. This is
/// strictly safer than a counter-based scheme, which can collide with
/// other processes or overflow past 65535.
///
/// # Panics
/// Panics if binding to `127.0.0.1:0` fails (no loopback or no free
/// ports — both effectively impossible on a healthy machine).
#[must_use]
pub fn unique_test_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind 127.0.0.1:0 for test port")
        .local_addr()
        .expect("local_addr for test port")
        .port()
}
