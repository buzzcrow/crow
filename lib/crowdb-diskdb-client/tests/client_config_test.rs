// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::Duration;

use crowdb_diskdb_client::RetryConfig;

#[test]
fn retry_config_default() {
    let retry = RetryConfig::default();
    assert_eq!(retry.max_retries, 3);
    assert_eq!(retry.initial_backoff, Duration::from_millis(50));
}
