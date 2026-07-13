// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Once;

use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

static TEST_SUBSCRIBER_INIT: Once = Once::new();

pub fn init_test_subscriber() {
    TEST_SUBSCRIBER_INIT.call_once(|| {
        if std::env::var("CROWKV_TEST_LOG").is_err() {
            return;
        }

        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("warn,crowkv=debug,crowkv_server=debug,crowkv_web=debug,crowkv_console_shared=debug,crowkv_cli=debug"));
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().with_test_writer().with_target(true))
            .try_init();
    });
}
