// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

/// Metric name: either a static string (process-global metrics) or an
/// owned `Arc<str>` (dynamic-name metrics like per-peer RPC stats).
#[derive(Debug, Clone)]
pub enum MetricName {
    Static(&'static str),
    Owned(Arc<str>),
}

impl MetricName {
    #[must_use]
    pub fn new_static(name: &'static str) -> Self {
        Self::Static(name)
    }

    #[must_use]
    pub fn new_owned(name: impl Into<String>) -> Self {
        Self::Owned(Arc::from(name.into().as_str()))
    }
}

impl std::ops::Deref for MetricName {
    type Target = str;

    fn deref(&self) -> &str {
        match self {
            Self::Static(s) => s,
            Self::Owned(s) => s,
        }
    }
}

impl PartialEq for MetricName {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Eq for MetricName {}

impl PartialOrd for MetricName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MetricName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (**self).cmp(&**other)
    }
}

impl AsRef<str> for MetricName {
    fn as_ref(&self) -> &str {
        self
    }
}

impl From<&'static str> for MetricName {
    fn from(s: &'static str) -> Self {
        Self::Static(s)
    }
}

impl From<String> for MetricName {
    fn from(s: String) -> Self {
        Self::Owned(Arc::from(s.as_str()))
    }
}
