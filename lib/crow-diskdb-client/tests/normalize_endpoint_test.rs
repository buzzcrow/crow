// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use crow_diskdb_client::client::normalize_endpoint;

#[test]
fn normalize_endpoint_adds_scheme() {
    assert_eq!(normalize_endpoint("127.0.0.1:9941"), "http://127.0.0.1:9941");
}

#[test]
fn normalize_endpoint_rewrites_wildcard() {
    assert_eq!(normalize_endpoint("0.0.0.0:9941"), "http://127.0.0.1:9941");
}

#[test]
fn normalize_endpoint_preserves_scheme() {
    assert_eq!(
        normalize_endpoint("http://127.0.0.1:9941"),
        "http://127.0.0.1:9941"
    );
}
