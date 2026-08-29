// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Once;

use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

static TEST_SUBSCRIBER_INIT: Once = Once::new();

pub fn init_test_subscriber() {
    TEST_SUBSCRIBER_INIT.call_once(|| {
        // Always init C++ spdlog to test-logs/ so transport/engine logs
        // (e.g. socket_transport.cpp worker teardown) go to files instead
        // of stderr. Error-level messages are mirrored to stderr by the
        // C++ logging init for CI visibility. Tree first, then rpc.
        crowdb_tree_ffi::ct_init_test_logging();
        crowdb_rpc_ffi::init_test_logging();

        if std::env::var("CROWDB_KV_TEST_LOG").is_err() {
            // No debug logging requested — still install a minimal
            // error-level subscriber so Rust-side panics and error
            // spans are visible on stderr (not just the C++ side).
            let _ = tracing_subscriber::registry()
                .with(EnvFilter::new("error"))
                .with(fmt::layer().with_writer(std::io::stderr).with_target(true))
                .try_init();
            return;
        }

        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("warn,crowdb_kv=debug,crowdb_kv_server=debug,crowdb_web=debug,crowdb_console_shared=debug,crowdb_cli=debug"));
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().with_test_writer().with_target(true))
            .try_init();
    });
}
