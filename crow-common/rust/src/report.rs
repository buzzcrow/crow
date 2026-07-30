// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! [`OperationReport`] — aggregated `critical:` errors from a multi-step
//! cascade (e.g. layered shutdown across `PxKvStore` → `PxGroup` →
//! replicas). Layers continue on errors and push messages here so the
//! caller can decide how to surface them.

/// Aggregated outcome of a multi-step operation.
///
/// `errors` contains one entry per `critical:` issue encountered.
/// An empty `errors` means every step completed gracefully.
#[derive(Debug, Default)]
pub struct OperationReport {
    pub errors: Vec<String>,
}

impl OperationReport {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if no `critical:` error was recorded.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
    }

    /// Append a single `critical:` error message.
    pub fn push_error(&mut self, msg: impl Into<String>) {
        self.errors.push(msg.into());
    }

    /// Merge another report's errors into this one.
    pub fn merge(&mut self, other: OperationReport) {
        self.errors.extend(other.errors);
    }
}
