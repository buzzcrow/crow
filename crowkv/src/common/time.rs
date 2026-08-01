// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Re-export of the monotonic-time helpers now living in `crow-common`.
//! The module path `crowkv::common::time::*` is preserved so existing
//! call sites compile unchanged; the implementation moved to
//! `crow_common::time` (R12).

pub use crow_common::time::{anchor_ms_to_instant, instant_to_anchor_ms, process_anchor};
