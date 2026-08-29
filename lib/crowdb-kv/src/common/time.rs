// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Re-export of the monotonic-time helpers now living in `crowdb-common`.
//! The module path `crowdb_kv::common::time::*` is preserved so existing
//! call sites compile unchanged; the implementation moved to
//! `crowdb_common::time` (R12).

pub(crate) use crowdb_common::time::{anchor_ms_to_instant, instant_to_anchor_ms};
