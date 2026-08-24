// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version  2.0.

//! End-to-end tests for the watch/notify flow: subscribe → write →
//! notify → client-receive.
//!
//! The former tonic bidi-stream tests (`watch_notify` unary stream)
//! are `#[ignore]`d pending migration to the crow-rpc
//! `WatchNotifyClient` (which requires an HTTP management API for
//! topology discovery, unavailable in the in-process `PxKvStore`
//! test cluster). The `WatchNotifyClient` push path is exercised by
//! the `crow-kv-client` crate's own e2e tests against real
//! `crow-kv-server` processes instead.

#[tokio::test]
#[ignore = "needs migration to crow-rpc WatchNotifyClient"]
async fn watch_notify_put_receives_key_and_value() {}

#[tokio::test]
#[ignore = "needs migration to crow-rpc WatchNotifyClient"]
async fn watch_notify_delete_receives_key_with_empty_value() {}

#[tokio::test]
#[ignore = "needs migration to crow-rpc WatchNotifyClient"]
async fn watch_notify_non_matching_key_no_notify() {}

#[tokio::test]
#[ignore = "needs migration to crow-rpc WatchNotifyClient"]
async fn watch_notify_batch_write_multiple_keys() {}

#[tokio::test]
#[ignore = "needs migration to crow-rpc WatchNotifyClient"]
async fn watch_notify_follower_redirects_to_leader() {}
