//! Key–value layer: log entry shapes, operation types, and (later) client-facing API.

pub mod types;

pub use types::{LogEntryKind, OpKind, Operation, PxLogEntry};
