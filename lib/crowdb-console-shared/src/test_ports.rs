// Copyright 2026-present Gian <crow.db@outlook.com>
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

/// Allocate `count` free ports atomically. Binds `count` listeners to
/// port 0 simultaneously, reads all OS-assigned port numbers, then drops
/// the listeners. Because all binds are held at the same time, the OS
/// assigns `count` distinct ports — no port can be reassigned to another
/// bind in the same batch. This eliminates the TOCTOU race between
/// concurrent `unique_test_port()` calls where one call drops a port and
/// another immediately re-binds `:0` and receives the same port.
///
/// Use this instead of calling `unique_test_port()` in a loop when the
/// caller deploys multiple subprocesses concurrently — each subprocess
/// binds its assigned port later, and without batch allocation one
/// subprocess's port can be silently reused by another's port pick.
///
/// # Panics
/// Panics if any bind fails (no loopback or no free ports).
#[must_use]
pub fn unique_test_ports(count: usize) -> Vec<u16> {
    let mut listeners = Vec::with_capacity(count);
    for _ in 0..count {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0 for test port batch");
        listeners.push(listener);
    }
    let ports = listeners
        .iter()
        .map(|l| l.local_addr().expect("local_addr for test port batch").port())
        .collect();
    drop(listeners);
    ports
}

/// Pick a base port where `port`, `port+1`, ..., `port+count-1` are all
/// free. Binds to port 0 to get a random base, then probes the next
/// `count-1` ports while holding the first listener (to prevent the OS
/// from re-allocating the base port). If any probe fails, drops all and
/// retries. Needed for subprocesses that derive multiple ports from a
/// single base (e.g. `crowdb-diskdb` uses `rpc_port`, `rpc_port+1`,
/// `rpc_port+2`).
///
/// # Panics
/// Panics if no contiguous run of `count` free ports is found after 500
/// attempts.
#[must_use]
pub fn unique_test_port_range(count: u16) -> u16 {
    for _ in 0..500 {
        // Bind to port 0 to get a random base, but keep the listener
        // open so the OS doesn't re-allocate it while we probe.
        let Ok(listener) = TcpListener::bind(("127.0.0.1", 0)) else {
            continue;
        };
        let Ok(local) = listener.local_addr() else {
            continue;
        };
        let base = local.port();
        if u32::from(base) + u32::from(count) > 65536 {
            drop(listener);
            continue;
        }
        // Probe the remaining ports while holding the base listener.
        let mut probes: Vec<TcpListener> = Vec::new();
        let mut all_free = true;
        for i in 1..count {
            let port = base + i;
            if let Ok(l) = TcpListener::bind(("127.0.0.1", port)) {
                probes.push(l);
            } else {
                all_free = false;
                break;
            }
        }
        if all_free {
            // All ports are free — drop listeners and return the base.
            drop(listener);
            drop(probes);
            return base;
        }
        drop(listener);
        drop(probes);
    }
    panic!("could not find {count} consecutive free ports after 500 attempts");
}
